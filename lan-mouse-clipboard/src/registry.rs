//! Connection registry / pool for clipboard peers.
//!
//! Tracks live connections (both outgoing pooled conns and incoming accepted
//! conns), broadcasts clipboard items to all of them, and evicts idle
//! outgoing connections after a timeout. The incoming listener itself is not
//! part of this registry and is never idle-evicted.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncWrite, AsyncWriteExt};

pub type PeerSink = Box<dyn AsyncWrite + Send + Unpin>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnKind {
    /// A connection we established to a peer (subject to idle eviction).
    Outgoing,
    /// A connection a peer established to us (not idle-evicted).
    Incoming,
}

struct Entry {
    sink: PeerSink,
    kind: ConnKind,
    last_used: Instant,
}

/// Registry of live clipboard connections.
pub struct ConnectionRegistry {
    peers: HashMap<SocketAddr, Entry>,
    idle_timeout: Duration,
}

impl ConnectionRegistry {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            peers: HashMap::new(),
            idle_timeout,
        }
    }

    /// Register or update a connection.
    pub fn insert(&mut self, addr: SocketAddr, sink: PeerSink, kind: ConnKind) {
        self.peers.insert(
            addr,
            Entry {
                sink,
                kind,
                last_used: Instant::now(),
            },
        );
    }

    pub fn remove(&mut self, addr: &SocketAddr) {
        self.peers.remove(addr);
    }

    pub fn contains(&self, addr: &SocketAddr) -> bool {
        self.peers.contains_key(addr)
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn peers(&self) -> Vec<SocketAddr> {
        self.peers.keys().copied().collect()
    }

    /// Broadcast an encoded frame to every live connection.
    ///
    /// Connections that fail to accept the write are removed. Returns the
    /// number of connections the frame was successfully queued to, and the
    /// addresses of connections removed due to a failed write.
    pub async fn broadcast(&mut self, frame: &[u8]) -> (usize, Vec<SocketAddr>) {
        let mut to_remove = Vec::new();
        let mut delivered = 0;
        for (addr, entry) in self.peers.iter_mut() {
            match entry.sink.write_all(frame).await {
                Ok(()) => {
                    delivered += 1;
                    entry.last_used = Instant::now();
                }
                Err(e) => {
                    log::warn!("clipboard send to {addr} failed: {e}");
                    to_remove.push(*addr);
                }
            }
        }
        for addr in &to_remove {
            self.peers.remove(addr);
        }
        (delivered, to_remove)
    }

    /// Evict idle outgoing connections. Incoming conns are never evicted.
    /// Returns the addresses of the evicted outgoing connections.
    pub fn evict_idle(&mut self) -> Vec<SocketAddr> {
        let timeout = self.idle_timeout;
        let now = Instant::now();
        let mut evicted = Vec::new();
        self.peers.retain(|addr, entry| {
            let keep =
                entry.kind == ConnKind::Incoming || now.duration_since(entry.last_used) <= timeout;
            if !keep {
                evicted.push(*addr);
            }
            keep
        });
        evicted
    }

    /// Remove all connections (used on shutdown).
    pub fn clear(&mut self) {
        self.peers.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn duplex() -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        tokio::io::duplex(64)
    }

    #[tokio::test]
    async fn broadcast_reaches_all_peers() {
        let mut reg = ConnectionRegistry::new(Duration::from_secs(60));
        let mut frames = Vec::new();
        for _ in 0..3 {
            let (a, b) = duplex();
            frames.push(b);
            reg.insert(
                SocketAddr::from(([10, 0, 0, 1], 1000 + frames.len() as u16)),
                Box::new(a),
                ConnKind::Incoming,
            );
        }
        let msg = b"hello".to_vec();
        let (delivered, removed) = reg.broadcast(&msg).await;
        assert_eq!(delivered, 3);
        assert!(removed.is_empty());
        for mut f in frames {
            let mut buf = [0u8; 5];
            f.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
        }
    }

    #[tokio::test]
    async fn failed_send_evicts_peer() {
        let mut reg = ConnectionRegistry::new(Duration::from_secs(60));
        // A closed duplex will error on write.
        let (a, _b) = duplex();
        drop(_b);
        let addr = SocketAddr::from(([10, 0, 0, 2], 2000));
        reg.insert(addr, Box::new(a), ConnKind::Incoming);
        let (delivered, removed) = reg.broadcast(b"x").await;
        assert_eq!(delivered, 0);
        assert_eq!(removed, vec![addr]);
        assert!(reg.is_empty());
    }

    #[tokio::test]
    async fn idle_eviction_spares_incoming() {
        let mut reg = ConnectionRegistry::new(Duration::from_millis(50));
        let (a1, _b1) = duplex();
        let addr_out = SocketAddr::from(([10, 0, 0, 3], 3000));
        reg.insert(addr_out, Box::new(a1), ConnKind::Outgoing);
        let (a2, _b2) = duplex();
        let addr_in = SocketAddr::from(([10, 0, 0, 4], 4000));
        reg.insert(addr_in, Box::new(a2), ConnKind::Incoming);

        tokio::time::sleep(Duration::from_millis(120)).await;
        let evicted = reg.evict_idle();
        assert_eq!(evicted, vec![addr_out]);
        assert!(!reg.contains(&addr_out));
        assert!(reg.contains(&addr_in));
    }

    #[tokio::test]
    async fn recent_activity_defers_eviction() {
        let mut reg = ConnectionRegistry::new(Duration::from_millis(50));
        let (a, mut b) = duplex();
        let addr = SocketAddr::from(([10, 0, 0, 5], 5000));
        reg.insert(addr, Box::new(a), ConnKind::Outgoing);

        // Keep sending so last_used stays fresh.
        for _ in 0..6 {
            reg.broadcast(b"keep-alive").await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = &mut b;
        assert!(reg.evict_idle().is_empty());
    }
}
