//! The clipboard network task.
//!
//! Runs the TLS listener, accepts authorized peers, registers each accepted
//! connection in the pool, forwards received frames to the orchestrator's
//! inbound channel, and writes orchestrator-produced broadcast frames to all
//! live connections (with idle eviction of outgoing conns).

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, ReadHalf};
use tokio::sync::{mpsc, watch};

use crate::protocol::{FrameReader, KIND_PING, KIND_PONG, Message};
use crate::registry::{ConnKind, ConnectionRegistry};
use crate::transport::{ClientConfig, ClientStream, ServerStream, TlsListener, connect};

pub struct NetworkError;

/// Events the network task raises for the owning service. Both a new
/// connection and a failed health check mean the peer's live address should
/// be re-derived, so they collapse into a single `Retrain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkEvent {
    /// Re-run IP "training" (re-derive peer addresses from the live UDP path).
    Retrain,
}

/// Clipboard endpoints for one peer.
///
/// Only one channel per peer is kept live. `active` is the preferred address
/// (the live UDP-path IP when known); the `fallbacks` stay registered so they
/// can be tried if the active address fails, without ever opening a second
/// simultaneous channel to the same peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoints {
    /// Stable peer identity (certificate fingerprint).
    pub id: String,
    /// Preferred address to dial.
    pub active: SocketAddr,
    /// Other registered addresses, tried in order if `active` fails.
    pub fallbacks: Vec<SocketAddr>,
}

impl PeerEndpoints {
    /// Every endpoint, preferred first.
    pub fn all(&self) -> Vec<SocketAddr> {
        std::iter::once(self.active)
            .chain(self.fallbacks.iter().copied())
            .collect()
    }

    fn contains_ip(&self, ip: IpAddr) -> bool {
        self.active.ip() == ip || self.fallbacks.iter().any(|a| a.ip() == ip)
    }

    /// The next endpoint after `addr`, if any (used to rotate to a fallback).
    fn next_after(&self, addr: SocketAddr) -> Option<SocketAddr> {
        let all = self.all();
        let idx = all.iter().position(|a| *a == addr)?;
        all.get(idx + 1).copied()
    }
}

/// Internal signals from a per-peer read loop back to the network loop.
enum HealthEvent {
    /// A ping arrived (from `addr`); reply with a pong.
    Ping(SocketAddr),
    /// A pong arrived (from `addr`); mark the peer alive.
    Pong(SocketAddr),
}

/// Health check cadence: probe every second, consider a peer dead if no pong
/// has arrived within the timeout.
const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(3);
/// Fallback reconnect cadence. Only acts while a known peer has no live
/// channel; it is the backstop for when the health check cannot run yet
/// (initial connect) or just failed.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Run the clipboard TLS server until the broadcast channel closes.
///
/// Always polls the listener for incoming peers, while background tasks
/// lazily establish an outgoing connection to each configured peer. One
/// live channel per peer is enough for bidirectional sync; broadcast frames
/// are written to all live channels.
///
/// Liveness is tracked with a 1s ping/pong health check. Reconnect attempts
/// are the fallback: they only run while a known peer has no live channel
/// (initial connect) or right after a health check fails. Every new
/// connection, and every health failure, raises [`NetworkEvent::Retrain`] so
/// the service re-derives peers from the live UDP address.
pub async fn run_clipboard_server(
    listener: TlsListener,
    client_config: ClientConfig,
    mut peers: watch::Receiver<Vec<PeerEndpoints>>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    mut broadcast_rx: mpsc::Receiver<Vec<u8>>,
    idle_timeout: Duration,
    event_tx: mpsc::Sender<NetworkEvent>,
) {
    let mut registry = ConnectionRegistry::new(idle_timeout);
    // Channel from background connect-tasks back to this loop. Carries the
    // peer id so a failed dial can rotate that peer to its next fallback.
    let (conn_tx, mut conn_rx) = mpsc::channel::<(SocketAddr, String, Option<ClientStream>)>(16);
    // Channel from per-peer read loops reporting that a connection died, so
    // we can drop it from the registry and the `connected` set and let the
    // fallback reconnect re-establish it.
    let (disc_tx, mut disc_rx) = mpsc::channel::<SocketAddr>(16);
    // Channel from per-peer read loops for health frames (ping/pong).
    let (health_tx, mut health_rx) = mpsc::channel::<HealthEvent>(64);
    // Peer target addresses we already have a live outgoing channel to.
    let mut connected: HashSet<SocketAddr> = HashSet::new();
    // Addresses with a connect attempt currently in flight.
    let mut connecting: HashSet<SocketAddr> = HashSet::new();
    // Last seen pong per live channel, keyed by connection address.
    let mut last_seen: HashMap<SocketAddr, Instant> = HashMap::new();
    // Current peer set (from the service), used by the fallback reconnect.
    let mut peers_now: Vec<PeerEndpoints> = peers.borrow().clone();
    // Endpoint currently being used per peer (its active address, or a
    // fallback after the active one failed).
    let mut peer_current: HashMap<String, SocketAddr> =
        peers_now.iter().map(|p| (p.id.clone(), p.active)).collect();

    let mut health = tokio::time::interval(HEALTH_INTERVAL);
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut retry =
        tokio::time::interval_at(tokio::time::Instant::now() + RETRY_INTERVAL, RETRY_INTERVAL);
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Accepting must not run inside `select!`: a tick winning the race would
    // cancel the future mid TLS handshake and drop the connection. Run it in
    // its own task and just receive finished handshakes here.
    let (accept_tx, mut accept_rx) = mpsc::channel::<(ServerStream, SocketAddr, String)>(16);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr, fp)) => {
                    if accept_tx.send((stream, addr, fp)).await.is_err() {
                        break;
                    }
                }
                Err(e) => log::debug!("clipboard accept failed: {e}"),
            }
        }
    });

    // Establish connections for the peers known at startup.
    spawn_connections(
        &client_config,
        &peers_now,
        &peer_current,
        &connected,
        &mut connecting,
        &conn_tx,
    );

    loop {
        tokio::select! {
            _ = health.tick() => {
                let now = Instant::now();
                let ping = Message::Ping.encode(0).unwrap_or_else(|_| vec![KIND_PING]);
                let mut dead = Vec::new();
                for addr in registry.peers() {
                    match last_seen.get(&addr) {
                        Some(t) if now.duration_since(*t) <= HEALTH_TIMEOUT => {
                            // A failed write drops the channel from the
                            // registry, so fold it into `dead` to keep
                            // `connected`/`last_seen` in sync and re-dial.
                            if !registry.write_to(&addr, &ping).await {
                                dead.push(addr);
                            }
                        }
                        _ => dead.push(addr),
                    }
                }
                for addr in &dead {
                    log::warn!("clipboard health check failed for {addr}; dropping and retraining");
                    registry.remove(addr);
                    connected.remove(addr);
                    last_seen.remove(addr);
                    advance_peer(&mut peer_current, &peers_now, *addr);
                }
                if !dead.is_empty() {
                    // Backstop: retrain the IPs, then burst reconnect.
                    let _ = event_tx.try_send(NetworkEvent::Retrain);
                    spawn_connections(
                        &client_config,
                        &peers_now,
                        &peer_current,
                        &connected,
                        &mut connecting,
                        &conn_tx,
                    );
                }
            }
            _ = retry.tick() => {
                // Fallback reconnect only while some known peer has no live
                // endpoint; a healthy, fully-connected set does nothing.
                if peers_now
                    .iter()
                    .any(|p| !p.all().iter().any(|a| connected.contains(a)))
                {
                    spawn_connections(
                        &client_config,
                        &peers_now,
                        &peer_current,
                        &connected,
                        &mut connecting,
                        &conn_tx,
                    );
                }
            }
            changed = peers.changed() => {
                if changed.is_ok() {
                    peers_now = peers.borrow_and_update().clone();
                    log::debug!("clipboard peer set updated: {peers_now:?}");
                    // Prefer the (possibly re-trained) active endpoint again,
                    // but only for peers that have no live channel; otherwise
                    // a healthy fallback connection would be pruned and
                    // re-dialed on every retrain.
                    for p in &peers_now {
                        let has_live = p.all().iter().any(|a| connected.contains(a));
                        if !has_live {
                            peer_current.insert(p.id.clone(), p.active);
                        }
                    }
                    // Prune any channel that is no longer the one live
                    // endpoint for its peer, then top up connections.
                    prune_channels(
                        &mut registry,
                        &mut connected,
                        &mut last_seen,
                        &peers_now,
                        &peer_current,
                    );
                    spawn_connections(
                        &client_config,
                        &peers_now,
                        &peer_current,
                        &connected,
                        &mut connecting,
                        &conn_tx,
                    );
                }
            }
            established = conn_rx.recv() => {
                if let Some((addr, peer_id, stream)) = established {
                    connecting.remove(&addr);
                    match stream {
                        Some(stream) => {
                            if registry.contains(&addr) {
                                continue;
                            }
                            log::info!("clipboard connected to peer {addr}");
                            let (r, w) = tokio::io::split(stream);
                            registry.insert(addr, Box::new(w), ConnKind::Outgoing);
                            connected.insert(addr);
                            last_seen.insert(addr, Instant::now());
                            peer_current.insert(peer_id, addr);
                            let tx = inbound_tx.clone();
                            let dtx = disc_tx.clone();
                            let htx = health_tx.clone();
                            tokio::spawn(read_loop(r, addr, tx, dtx, htx));
                            // Retrain IPs on every connect so peers converge
                            // on the live address.
                            let _ = event_tx.try_send(NetworkEvent::Retrain);
                        }
                        None => {
                            // Dial failed: rotate this peer to its next
                            // registered fallback.
                            advance_peer(&mut peer_current, &peers_now, addr);
                        }
                    }
                }
            }
            health_ev = health_rx.recv() => match health_ev {
                Some(HealthEvent::Pong(addr)) => {
                    last_seen.insert(addr, Instant::now());
                }
                Some(HealthEvent::Ping(addr)) => {
                    let pong = Message::Pong.encode(0).unwrap_or_else(|_| vec![KIND_PONG]);
                    if !registry.write_to(&addr, &pong).await {
                        // Keep the bookkeeping consistent with the registry,
                        // which `write_to` just pruned on the failed write.
                        connected.remove(&addr);
                        last_seen.remove(&addr);
                    }
                }
                None => {}
            },
            disconnected = disc_rx.recv() => {
                if let Some(addr) = disconnected {
                    // Peer closed its side. Drop the stale channel and clear the
                    // "connected" marker; the fallback reconnect re-establishes it.
                    registry.remove(&addr);
                    connected.remove(&addr);
                    last_seen.remove(&addr);
                    advance_peer(&mut peer_current, &peers_now, addr);
                    log::info!("clipboard peer {addr} removed, will reconnect");
                }
            }
            maybe_frame = broadcast_rx.recv() => match maybe_frame {
                Some(frame) => {
                    let (_delivered, removed) = registry.broadcast(&frame).await;
                    let evicted = registry.evict_idle();
                    for addr in removed.into_iter().chain(evicted) {
                        connected.remove(&addr);
                        last_seen.remove(&addr);
                    }
                }
                None => break,
            },
            accepted = accept_rx.recv() => {
                if let Some((stream, addr, fp)) = accepted {
                    log::info!("clipboard peer connected: {fp} @ {addr}");
                    // One channel per peer: drop a duplicate from an IP we
                    // already have a live channel to (any port).
                    if registry.entries().iter().any(|(a, _)| a.ip() == addr.ip()) {
                        log::debug!("clipboard dropping duplicate channel from {addr}");
                        continue;
                    }
                    let (r, w) = tokio::io::split(stream);
                    registry.insert(addr, Box::new(w), ConnKind::Incoming);
                    last_seen.insert(addr, Instant::now());
                    let tx = inbound_tx.clone();
                    let dtx = disc_tx.clone();
                    let htx = health_tx.clone();
                    tokio::spawn(read_loop(r, addr, tx, dtx, htx));
                    let _ = event_tx.try_send(NetworkEvent::Retrain);
                }
            }
        }
    }
}

/// Spawn a background connect task per configured peer that we do not
/// already have a live outgoing channel to and are not already dialing.
///
/// Connect attempts never block the accept loop (which would deadlock two
/// peers connecting to each other), so the listener stays available. The
/// `connecting` set guards against duplicate concurrent dials to the same
/// address (the registry is keyed by peer address, so a duplicate would let
/// one connection's teardown evict the other).
fn spawn_connections(
    client_config: &ClientConfig,
    peers: &[PeerEndpoints],
    peer_current: &HashMap<String, SocketAddr>,
    connected: &HashSet<SocketAddr>,
    connecting: &mut HashSet<SocketAddr>,
    conn_tx: &mpsc::Sender<(SocketAddr, String, Option<ClientStream>)>,
) {
    for peer in peers {
        // One channel per peer: skip if any of its endpoints is already live
        // or being dialed.
        if peer
            .all()
            .iter()
            .any(|a| connected.contains(a) || connecting.contains(a))
        {
            continue;
        }
        let addr = peer_current.get(&peer.id).copied().unwrap_or(peer.active);
        connecting.insert(addr);
        let cfg = client_config.clone();
        let tx = conn_tx.clone();
        let id = peer.id.clone();
        tokio::spawn(async move {
            let stream = match connect(addr, cfg).await {
                Ok(stream) => Some(stream),
                Err(e) => {
                    log::debug!("clipboard connect to {addr} failed: {e}");
                    None
                }
            };
            let _ = tx.send((addr, id, stream)).await;
        });
    }
}

/// Find the peer that owns `ip`.
fn peer_of_ip(peers: &[PeerEndpoints], ip: IpAddr) -> Option<&PeerEndpoints> {
    peers.iter().find(|p| p.contains_ip(ip))
}

/// After a channel to `addr` failed, advance that peer's current endpoint to
/// its next fallback (if any) so the fallback reconnect tries the next IP.
fn advance_peer(
    peer_current: &mut HashMap<String, SocketAddr>,
    peers: &[PeerEndpoints],
    addr: SocketAddr,
) {
    let Some(peer) = peer_of_ip(peers, addr.ip()) else {
        return;
    };
    if peer_current.get(&peer.id).copied() == Some(addr) {
        if let Some(next) = peer.next_after(addr) {
            peer_current.insert(peer.id.clone(), next);
        }
    }
}

/// Reconcile live channels with the current peer set: keep only one outgoing
/// channel per peer (the peer's currently used endpoint), drop outgoing
/// channels whose IP is no longer a known endpoint, and collapse duplicate
/// incoming channels from the same peer IP.
fn prune_channels(
    registry: &mut ConnectionRegistry,
    connected: &mut HashSet<SocketAddr>,
    last_seen: &mut HashMap<SocketAddr, Instant>,
    peers: &[PeerEndpoints],
    peer_current: &HashMap<String, SocketAddr>,
) {
    let mut seen_incoming: HashSet<IpAddr> = HashSet::new();
    let mut to_drop = Vec::new();
    for (addr, kind) in registry.entries() {
        match kind {
            ConnKind::Outgoing => {
                let Some(peer) = peer_of_ip(peers, addr.ip()) else {
                    to_drop.push(addr);
                    continue;
                };
                let current = peer_current.get(&peer.id).copied().unwrap_or(peer.active);
                if addr != current {
                    to_drop.push(addr);
                }
            }
            ConnKind::Incoming => {
                // At most one incoming channel per peer IP.
                if !seen_incoming.insert(addr.ip()) {
                    to_drop.push(addr);
                }
            }
        }
    }
    for addr in to_drop {
        log::debug!("clipboard pruning redundant/stale channel {addr}");
        registry.remove(&addr);
        connected.remove(&addr);
        last_seen.remove(&addr);
    }
}

/// Read frames from one peer connection and forward clipboard frames to the
/// orchestrator. Control frames (ping/pong) are handled here instead: pongs
/// feed the health check and pings are answered via `health_tx`.
async fn read_loop<R>(
    mut reader: ReadHalf<R>,
    addr: SocketAddr,
    tx: mpsc::Sender<Vec<u8>>,
    disc_tx: mpsc::Sender<SocketAddr>,
    health_tx: mpsc::Sender<HealthEvent>,
) where
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; 8192];
    let mut frames = FrameReader::new();
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                log::debug!("clipboard read from {addr} failed: {e}");
                break;
            }
        };
        frames.push(&buf[..n]);
        loop {
            match frames.next_frame() {
                Ok(Some(payload)) => match payload.first() {
                    Some(&KIND_PING) => {
                        let _ = health_tx.send(HealthEvent::Ping(addr)).await;
                    }
                    Some(&KIND_PONG) => {
                        let _ = health_tx.send(HealthEvent::Pong(addr)).await;
                    }
                    _ => {
                        if tx.send(payload).await.is_err() {
                            return;
                        }
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    log::debug!("clipboard frame from {addr} invalid: {e}");
                    // Drop the frame buffer and resync on the next length prefix.
                    frames = FrameReader::new();
                    break;
                }
            }
        }
    }
    // Always notify so the network loop can drop the stale channel and the
    // "connected" marker, allowing the fallback reconnect to re-establish it.
    let _ = disc_tx.send(addr).await;
    log::debug!("clipboard peer {addr} disconnected");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item::ClipboardItem;
    use crate::protocol::Message;
    use crate::transport::client_config;
    use std::collections::HashMap;

    struct TestIdentity {
        identity: crate::transport::Identity,
        fingerprint: String,
    }

    fn test_identity(cn: &str) -> TestIdentity {
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::new(vec![cn.to_string()]).expect("params");
        let cert = params.self_signed(&key).expect("cert");
        let pem = format!("{}{}", cert.pem(), key.serialize_pem());
        let identity = crate::transport::load_identity(&pem).expect("identity");
        let fingerprint = identity.fingerprint();
        TestIdentity {
            identity,
            fingerprint,
        }
    }

    async fn bind_listener(
        identity: &crate::transport::Identity,
        authorized: HashMap<String, String>,
    ) -> crate::transport::TlsListener {
        crate::transport::TlsListener::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            identity,
            authorized,
        )
        .await
        .expect("bind")
    }

    /// Two peer servers, each lazily connecting to the other, deliver a
    /// broadcast sent from either side.
    #[tokio::test(flavor = "multi_thread")]
    async fn peers_deliver_broadcast_both_directions() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();
        let a = test_identity("a");
        let b = test_identity("b");

        let listener_a = bind_listener(
            &a.identity,
            HashMap::from([(b.fingerprint.clone(), "b".into())]),
        )
        .await;
        let listener_b = bind_listener(
            &b.identity,
            HashMap::from([(a.fingerprint.clone(), "a".into())]),
        )
        .await;

        let addr_b = listener_b.local_addr().expect("addr b");

        let (in_a_tx, mut in_a_rx) = mpsc::channel(64);
        let (bc_a_tx, bc_a_rx) = mpsc::channel(64);
        let (in_b_tx, mut in_b_rx) = mpsc::channel(64);
        let (bc_b_tx, bc_b_rx) = mpsc::channel(64);

        let cfg_a = client_config(&a.identity).expect("cfg a");
        let cfg_b = client_config(&b.identity).expect("cfg b");

        // A initiates to B; B only listens/accepts.
        let (peers_a_tx, peers_a_rx) = watch::channel(vec![PeerEndpoints {
            id: "peer-b".to_string(),
            active: addr_b,
            fallbacks: Vec::new(),
        }]);
        let (peers_b_tx, peers_b_rx) = watch::channel::<Vec<PeerEndpoints>>(Vec::new());
        let (ev_a_tx, _ev_a_rx) = mpsc::channel(4);
        let (ev_b_tx, _ev_b_rx) = mpsc::channel(4);

        tokio::spawn(run_clipboard_server(
            listener_a,
            cfg_a,
            peers_a_rx,
            in_a_tx,
            bc_a_rx,
            Duration::from_secs(60),
            ev_a_tx,
        ));
        // B only listens/accepts (single initiated connection, as enforced in
        // production by the fingerprint tiebreak); this keeps the test
        // deterministic with exactly one live channel.
        tokio::spawn(run_clipboard_server(
            listener_b,
            cfg_b,
            peers_b_rx,
            in_b_tx,
            bc_b_rx,
            Duration::from_secs(60),
            ev_b_tx,
        ));
        // Keep the senders alive so the receivers keep yielding the latest value.
        let _ = peers_a_tx;
        let _ = peers_b_tx;

        // Give the connections time to establish.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let item = ClipboardItem::text("hello from A", [0xAA; 8], 1);
        let frame = Message::Announce(item).encode(1024).unwrap();
        let payload = frame[4..].to_vec();

        // Retry until a connection has formed and the frame is delivered,
        // mirroring real usage where a peer copies after both are connected.
        let got = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                bc_a_tx.send(frame.clone()).await.unwrap();
                if let Some(p) = in_b_rx.recv().await {
                    break p;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timeout waiting for B to receive");
        assert_eq!(got, payload);

        // And the reverse direction.
        let item_b = ClipboardItem::text("hello from B", [0xBB; 8], 1);
        let frame_b = Message::Announce(item_b).encode(1024).unwrap();
        let payload_b = frame_b[4..].to_vec();
        let got_b = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                bc_b_tx.send(frame_b.clone()).await.unwrap();
                if let Some(p) = in_a_rx.recv().await {
                    break p;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timeout waiting for A to receive");
        assert_eq!(got_b, payload_b);
    }

    // --- read_loop unit tests using a duplex stream as a test double ---

    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, duplex, split};

    #[tokio::test]
    async fn read_loop_forwards_complete_frame() {
        let (mut w, r) = duplex(4096);
        let (tx, mut rx) = mpsc::channel(16);
        let (dtx, _drx) = mpsc::channel(16);
        let (htx, _hrx) = mpsc::channel(16);
        let addr = SocketAddr::from(([127, 0, 0, 1], 1));

        let item = ClipboardItem::text("hello", [1; 8], 1);
        let frame = Message::Announce(item).encode(1024).unwrap();

        let (r_half, _w_half) = split(r);
        let task = tokio::spawn(read_loop(r_half, addr, tx, dtx, htx));

        w.write_all(&frame).await.unwrap();
        drop(w); // EOF signals the read loop to stop

        let payload = rx.recv().await.unwrap();
        assert_eq!(payload, frame[4..].to_vec());
        let _ = task.await;
    }

    #[tokio::test]
    async fn read_loop_ignores_garbage_and_delivers_following_frame() {
        let (mut w, r) = duplex(4096);
        let (tx, mut rx) = mpsc::channel(16);
        let (dtx, _drx) = mpsc::channel(16);
        let (htx, _hrx) = mpsc::channel(16);
        let addr = SocketAddr::from(([127, 0, 0, 1], 1));

        let item = ClipboardItem::text("still here", [2; 8], 1);
        let good = Message::Announce(item).encode(1024).unwrap();

        let (r_half, _w_half) = split(r);
        let task = tokio::spawn(read_loop(r_half, addr, tx, dtx, htx));

        // A length prefix that claims a 4 GiB payload -> rejected -> resync.
        w.write_all(&[0xff, 0xff, 0xff, 0xff]).await.unwrap();
        w.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        w.write_all(&good).await.unwrap();
        drop(w);

        let payload = rx.recv().await.unwrap();
        assert_eq!(payload, good[4..].to_vec());
        let _ = task.await;
    }
}
