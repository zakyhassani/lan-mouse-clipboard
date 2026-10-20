//! Pending lazy read requests.
//!
//! A FUSE read misses its chunk cache and asks the peer that offered the file
//! for one range. Requests are matched by an atomic id and bounded by a
//! timeout, and the origin recorded at request time is checked against the
//! authenticated origin of the answering connection.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::protocol::{
    FILE_ERR_UNAVAILABLE, FILE_READ_CHUNK_MAX, FileData, FileError, FileRead, Message,
};

/// How long one lazy read may wait for the sender.
pub const READ_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("invalid read range")]
    InvalidRange,
    #[error("peer is offline")]
    Offline,
    #[error("read timed out")]
    Timeout,
    #[error("peer error: {0}")]
    Peer(String),
    #[error("read failed: {0}")]
    Failed(String),
}

struct Pending {
    origin: [u8; 8],
    offset: u64,
    length: u32,
    tx: oneshot::Sender<Result<Vec<u8>, BrokerError>>,
}

/// Removes a pending request when the read future is dropped (for example when
/// a FUSE read is cancelled), so cancellation cannot leak the map entry.
struct PendingGuard<'a> {
    pending: &'a Mutex<HashMap<u64, Pending>>,
    request_id: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.request_id);
    }
}

/// Tracks in-flight lazy reads and routes them to the owning peer.
pub struct Broker {
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    directed_tx: mpsc::Sender<([u8; 8], Vec<u8>)>,
}

impl Broker {
    pub fn new(directed_tx: mpsc::Sender<([u8; 8], Vec<u8>)>) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            directed_tx,
        }
    }

    fn next_request_id(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            self.next_id.store(2, Ordering::Relaxed);
            return 1;
        }
        id
    }

    /// Ask `origin` for one range of an offered file.
    pub async fn read(
        &self,
        origin: [u8; 8],
        serial: u64,
        entry_id: u32,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, BrokerError> {
        if length > FILE_READ_CHUNK_MAX {
            return Err(BrokerError::InvalidRange);
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let request_id = self.next_request_id();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(
            request_id,
            Pending {
                origin,
                offset,
                length,
                tx,
            },
        );
        let _guard = PendingGuard {
            pending: &self.pending,
            request_id,
        };
        let read = FileRead {
            request_id,
            serial,
            entry_id,
            offset,
            length,
        };
        let frame = match Message::FileRead(read).encode(0) {
            Ok(frame) => frame,
            Err(e) => {
                self.pending.lock().unwrap().remove(&request_id);
                return Err(BrokerError::Failed(e.to_string()));
            }
        };
        // One deadline covers both queuing the request and waiting for the
        // reply. Otherwise a full directed channel could block a FUSE read
        // indefinitely even though a reply would have timed out.
        let deadline = tokio::time::Instant::now() + READ_TIMEOUT;
        match tokio::time::timeout_at(deadline, self.directed_tx.send((origin, frame))).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(BrokerError::Offline),
            Err(_) => return Err(BrokerError::Timeout),
        }
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BrokerError::Failed("read cancelled".into())),
            Err(_) => Err(BrokerError::Timeout),
        }
    }

    /// Complete a read with peer data. The origin must match the request.
    pub fn complete(&self, peer_origin: [u8; 8], data: FileData) {
        // A frame from any other peer must not cancel or poison a pending
        // request. Only the peer the request was sent to may complete it.
        let pending = {
            let mut pending = self.pending.lock().unwrap();
            match pending.get(&data.request_id) {
                Some(entry) if entry.origin == peer_origin => pending.remove(&data.request_id),
                _ => None,
            }
        };
        let Some(pending) = pending else {
            return;
        };
        if data.offset != pending.offset || data.data.len() > pending.length as usize {
            let _ = pending.tx.send(Err(BrokerError::InvalidRange));
            return;
        }
        let _ = pending.tx.send(Ok(data.data));
    }

    /// Fail a read with a peer error. The origin must match the request.
    pub fn fail(&self, peer_origin: [u8; 8], error: FileError) {
        let pending = {
            let mut pending = self.pending.lock().unwrap();
            match pending.get(&error.request_id) {
                Some(entry) if entry.origin == peer_origin => pending.remove(&error.request_id),
                _ => None,
            }
        };
        let Some(pending) = pending else {
            return;
        };
        let code = if error.code == FILE_ERR_UNAVAILABLE {
            BrokerError::Offline
        } else {
            BrokerError::Peer(error.message)
        };
        let _ = pending.tx.send(Err(code));
    }

    /// Drop every pending request (used on shutdown).
    pub fn cancel_all(&self) {
        self.pending.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type DirectedRx = mpsc::Receiver<([u8; 8], Vec<u8>)>;

    fn broker() -> (std::sync::Arc<Broker>, DirectedRx) {
        let (tx, rx) = mpsc::channel(8);
        (std::sync::Arc::new(Broker::new(tx)), rx)
    }

    #[tokio::test]
    async fn sends_read_and_completes_with_data() {
        let (broker, mut directed) = broker();
        let b2 = broker.clone();
        let task = tokio::spawn(async move { b2.read([7; 8], 4, 2, 1024, 4096).await });
        let (origin, frame) = directed.recv().await.unwrap();
        assert_eq!(origin, [7; 8]);
        let Message::FileRead(read) = Message::decode(&frame[4..]).unwrap() else {
            panic!("expected FileRead");
        };
        assert_eq!(read.serial, 4);
        assert_eq!(read.entry_id, 2);
        assert_eq!(read.offset, 1024);
        assert_eq!(read.length, 4096);
        broker.complete(
            [7; 8],
            FileData {
                request_id: read.request_id,
                offset: 1024,
                data: vec![9; 4096],
            },
        );
        assert_eq!(task.await.unwrap().unwrap(), vec![9; 4096]);
    }

    #[tokio::test]
    async fn wrong_peer_cannot_poison_a_pending_read() {
        let (broker, mut directed) = broker();
        let b2 = broker.clone();
        let task = tokio::spawn(async move { b2.read([1; 8], 1, 0, 0, 4).await });
        let (_, frame) = directed.recv().await.unwrap();
        let Message::FileRead(read) = Message::decode(&frame[4..]).unwrap() else {
            panic!("expected FileRead");
        };
        // A different peer guessing the request id must not remove it.
        broker.complete(
            [2; 8],
            FileData {
                request_id: read.request_id,
                offset: 0,
                data: vec![9; 4],
            },
        );
        broker.fail(
            [2; 8],
            FileError {
                request_id: read.request_id,
                code: FILE_ERR_UNAVAILABLE,
                message: "spoof".into(),
            },
        );
        // The legitimate peer can still answer.
        broker.complete(
            [1; 8],
            FileData {
                request_id: read.request_id,
                offset: 0,
                data: vec![7; 4],
            },
        );
        assert_eq!(task.await.unwrap().unwrap(), vec![7; 4]);
    }

    #[tokio::test]
    async fn peer_error_fails_the_read() {
        let (broker, mut directed) = broker();
        let b2 = broker.clone();
        let task = tokio::spawn(async move { b2.read([3; 8], 1, 0, 0, 4).await });
        let (_, frame) = directed.recv().await.unwrap();
        let Message::FileRead(read) = Message::decode(&frame[4..]).unwrap() else {
            panic!("expected FileRead");
        };
        broker.fail(
            [3; 8],
            FileError {
                request_id: read.request_id,
                code: FILE_ERR_UNAVAILABLE,
                message: "gone".into(),
            },
        );
        assert!(matches!(task.await.unwrap(), Err(BrokerError::Offline)));
    }

    #[tokio::test]
    async fn invalid_ranges_are_rejected_before_sending() {
        let (broker, _directed) = broker();
        assert!(matches!(
            broker.read([1; 8], 1, 0, 0, FILE_READ_CHUNK_MAX + 1).await,
            Err(BrokerError::InvalidRange)
        ));
        assert!(broker.read([1; 8], 1, 0, 0, 0).await.unwrap().is_empty());
    }
}
