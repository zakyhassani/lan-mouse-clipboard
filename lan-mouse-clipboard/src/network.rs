//! The clipboard network task.
//!
//! Runs the TLS listener, accepts authorized peers, registers each accepted
//! connection in the pool, forwards received frames to the orchestrator's
//! inbound channel, and writes orchestrator-produced broadcast frames to all
//! live connections (with idle eviction of outgoing conns).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, ReadHalf};
use tokio::sync::{mpsc, watch};

use crate::protocol::FrameReader;
use crate::registry::{ConnKind, ConnectionRegistry};
use crate::transport::{ClientConfig, ClientStream, TlsListener, connect};

pub struct NetworkError;

/// Run the clipboard TLS server until the broadcast channel closes.
///
/// Always polls the listener for incoming peers, while background tasks
/// lazily establish an outgoing connection to each configured peer. One
/// live channel per peer is enough for bidirectional sync; broadcast frames
/// are written to all live channels and idle outgoing channels are evicted.
pub async fn run_clipboard_server(
    listener: TlsListener,
    client_config: ClientConfig,
    mut peers: watch::Receiver<Vec<SocketAddr>>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    mut broadcast_rx: mpsc::Receiver<Vec<u8>>,
    idle_timeout: Duration,
) {
    let mut registry = ConnectionRegistry::new(idle_timeout);
    // Channel from background connect-tasks back to this loop.
    let (conn_tx, mut conn_rx) = mpsc::channel::<(SocketAddr, ClientStream)>(16);
    // Channel from per-peer read loops reporting that a connection died, so
    // we can drop it from the registry and the `connected` set and let the
    // reconnect tick re-establish it. Without this, a peer that closes its
    // side leaves us stuck in CLOSE-WAIT holding a stale WriteHalf, and the
    // registry/`connected` set keep claiming a live channel exists.
    let (disc_tx, mut disc_rx) = mpsc::channel::<SocketAddr>(16);
    // Peer target addresses we already have a live outgoing channel to.
    let mut connected: HashSet<SocketAddr> = HashSet::new();

    let mut reconnect = tokio::time::interval(Duration::from_secs(5));
    reconnect.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = reconnect.tick() => {
                // Re-read the peer set each tick so clipboard follows the
                // live UDP-path address of each peer (and drops stale IPs).
                let peers = peers.borrow_and_update().clone();
                spawn_connections(&client_config, &peers, &connected, &conn_tx);
            }
            established = conn_rx.recv() => {
                if let Some((addr, stream)) = established {
                    if registry.contains(&addr) {
                        continue;
                    }
                    log::info!("clipboard connected to peer {addr}");
                    let (r, w) = tokio::io::split(stream);
                    registry.insert(addr, Box::new(w), ConnKind::Outgoing);
                    connected.insert(addr);
                    let tx = inbound_tx.clone();
                    let dtx = disc_tx.clone();
                    tokio::spawn(read_loop(r, addr, tx, dtx));
                }
            }
            disconnected = disc_rx.recv() => {
                if let Some(addr) = disconnected {
                    // Peer closed its side. Drop the stale channel and clear the
                    // "connected" marker so the reconnect tick re-establishes it.
                    registry.remove(&addr);
                    connected.remove(&addr);
                    log::info!("clipboard peer {addr} removed, will reconnect");
                }
            }
            maybe_frame = broadcast_rx.recv() => match maybe_frame {
                Some(frame) => {
                    let (_delivered, removed) = registry.broadcast(&frame).await;
                    let evicted = registry.evict_idle();
                    for addr in removed.into_iter().chain(evicted) {
                        connected.remove(&addr);
                    }
                }
                None => break,
            },
            accepted = listener.accept() => match accepted {
                Ok((stream, addr, fp)) => {
                    log::info!("clipboard peer connected: {fp} @ {addr}");
                    // One channel per peer is enough for bidirectional sync; a
                    // connection from a peer we already connect to is redundant.
                    if registry.contains(&addr) {
                        continue;
                    }
                    let (r, w) = tokio::io::split(stream);
                    registry.insert(addr, Box::new(w), ConnKind::Incoming);
                    let tx = inbound_tx.clone();
                    let dtx = disc_tx.clone();
                    tokio::spawn(read_loop(r, addr, tx, dtx));
                }
                Err(e) => log::debug!("clipboard accept failed: {e}"),
            },
        }
    }
}

/// Spawn a background connect task per configured peer that we do not
/// already have a live outgoing channel to.
///
/// Connect attempts never block the accept loop (which would deadlock two
/// peers connecting to each other), so the listener stays available.
fn spawn_connections(
    client_config: &ClientConfig,
    peers: &[SocketAddr],
    connected: &HashSet<SocketAddr>,
    conn_tx: &mpsc::Sender<(SocketAddr, ClientStream)>,
) {
    for &addr in peers {
        if connected.contains(&addr) {
            continue;
        }
        let cfg = client_config.clone();
        let tx = conn_tx.clone();
        tokio::spawn(async move {
            match connect(addr, cfg).await {
                Ok(stream) => {
                    let _ = tx.send((addr, stream)).await;
                }
                Err(e) => log::debug!("clipboard connect to {addr} failed: {e}"),
            }
        });
    }
}

/// Read frames from one peer connection and forward them to the orchestrator.
async fn read_loop<R>(
    mut reader: ReadHalf<R>,
    addr: SocketAddr,
    tx: mpsc::Sender<Vec<u8>>,
    disc_tx: mpsc::Sender<SocketAddr>,
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
                Ok(Some(payload)) => {
                    if tx.send(payload).await.is_err() {
                        return;
                    }
                }
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
    // "connected" marker, allowing the reconnect tick to re-establish it.
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
        let (peers_a_tx, peers_a_rx) = watch::channel(vec![addr_b]);
        let (peers_b_tx, peers_b_rx) = watch::channel::<Vec<SocketAddr>>(Vec::new());

        tokio::spawn(run_clipboard_server(
            listener_a,
            cfg_a,
            peers_a_rx,
            in_a_tx,
            bc_a_rx,
            Duration::from_secs(60),
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
        let addr = SocketAddr::from(([127, 0, 0, 1], 1));

        let item = ClipboardItem::text("hello", [1; 8], 1);
        let frame = Message::Announce(item).encode(1024).unwrap();

        let (r_half, _w_half) = split(r);
        let task = tokio::spawn(read_loop(r_half, addr, tx, dtx));

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
        let addr = SocketAddr::from(([127, 0, 0, 1], 1));

        let item = ClipboardItem::text("still here", [2; 8], 1);
        let good = Message::Announce(item).encode(1024).unwrap();

        let (r_half, _w_half) = split(r);
        let task = tokio::spawn(read_loop(r_half, addr, tx, dtx));

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
