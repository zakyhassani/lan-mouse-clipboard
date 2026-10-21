//! Local file offers: manifest building and lazy read serving.
//!
//! A single worker builds manifests off the async runtime. The driver stores
//! the resulting offer and broadcasts it; later `FileRead` requests are served
//! from the stored local paths without ever sending a path to the peer.

use std::collections::VecDeque;
use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as WindowsFileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::{Semaphore, mpsc};

use crate::file::manifest::{
    BuiltManifest, Manifest, ManifestError, ManifestLimits, build_manifest, item_paths,
};
use crate::item::ClipboardItem;
use crate::protocol::{
    FILE_ERR_IO, FILE_ERR_NOT_FOUND, FILE_ERR_RANGE, FILE_READ_CHUNK_MAX, FileData, FileError,
    FileRead, Message,
};

/// How many completed offers the sender remembers.
const MAX_OFFERS: usize = 8;
/// Total manifest bytes the sender keeps (in addition to the count cap).
const MAX_OFFER_BYTES: usize = 64 * 1024 * 1024;
/// Concurrent file reads served by one sender.
pub(crate) const MAX_INFLIGHT_READS: usize = 8;

/// A manifest built by the worker, ready to store and broadcast.
#[derive(Debug, Clone)]
pub struct BuiltOffer {
    pub manifest: Manifest,
    pub paths: Vec<PathBuf>,
    pub encoded: Vec<u8>,
}

/// One local clipboard item after the manifest worker has looked at it.
#[derive(Debug, Clone)]
pub struct PreparedItem {
    pub item: ClipboardItem,
    /// `Some` when the item contains at least one valid local file URI.
    pub offer: Option<BuiltOffer>,
}

pub struct BuildRequest {
    pub item: ClipboardItem,
}

/// Handle to the single manifest build worker.
pub struct ManifestWorker {
    tx: mpsc::Sender<BuildRequest>,
}

impl ManifestWorker {
    /// Spawn the worker and return its input handle plus the ordered result
    /// channel consumed by the driver.
    pub fn spawn(limits: ManifestLimits) -> (Self, mpsc::Receiver<PreparedItem>) {
        let (tx, mut rx) = mpsc::channel::<BuildRequest>(16);
        let (ready_tx, ready_rx) = mpsc::channel::<PreparedItem>(16);
        tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                let item = request.item;
                let item_for_worker = item.clone();
                let built = tokio::task::spawn_blocking(move || {
                    let paths = item_paths(&item_for_worker);
                    if paths.is_empty() {
                        return None;
                    }
                    match build_manifest(&paths, limits) {
                        Ok(BuiltManifest { manifest, paths }) => match manifest.encode(limits) {
                            Ok(encoded) => Some(BuiltOffer {
                                manifest,
                                paths,
                                encoded,
                            }),
                            Err(e) => {
                                log::warn!("file offer: manifest encode failed: {e}");
                                None
                            }
                        },
                        Err(ManifestError::NoFiles) => None,
                        Err(e) => {
                            log::warn!("file offer: manifest build failed: {e}");
                            None
                        }
                    }
                })
                .await
                .ok()
                .flatten();
                if ready_tx
                    .send(PreparedItem { item, offer: built })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        (Self { tx }, ready_rx)
    }

    /// Queue a local item for manifest building.
    pub async fn submit(&self, item: ClipboardItem) -> bool {
        self.tx.send(BuildRequest { item }).await.is_ok()
    }
}

struct StoredOffer {
    serial: u64,
    manifest: Manifest,
    paths: Vec<PathBuf>,
    encoded_len: usize,
}

struct StoreInner {
    offers: VecDeque<StoredOffer>,
    total_bytes: usize,
}

/// Completed offers, kept so remote peers can read file ranges lazily.
pub struct OfferStore {
    inner: Mutex<StoreInner>,
    reads: Semaphore,
}

impl Default for OfferStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OfferStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(StoreInner {
                offers: VecDeque::new(),
                total_bytes: 0,
            }),
            reads: Semaphore::new(MAX_INFLIGHT_READS),
        }
    }

    /// Store a completed offer, evicting the oldest when over either cap.
    pub fn insert(&self, serial: u64, offer: &BuiltOffer) {
        let mut inner = self.inner.lock().unwrap();
        inner.offers.push_back(StoredOffer {
            serial,
            manifest: offer.manifest.clone(),
            paths: offer.paths.clone(),
            encoded_len: offer.encoded.len(),
        });
        inner.total_bytes += offer.encoded.len();
        while inner.offers.len() > MAX_OFFERS || inner.total_bytes > MAX_OFFER_BYTES {
            let Some(old) = inner.offers.pop_front() else {
                break;
            };
            inner.total_bytes = inner.total_bytes.saturating_sub(old.encoded_len);
        }
    }

    /// Serve one read request, returning a directed data or error message.
    pub async fn handle_read(self: &Arc<Self>, request: FileRead) -> Message {
        let _permit = match self.reads.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                return Message::FileError(FileError {
                    request_id: request.request_id,
                    code: FILE_ERR_IO,
                    message: "sender is shutting down".into(),
                });
            }
        };
        if request.length > FILE_READ_CHUNK_MAX {
            return error(request.request_id, FILE_ERR_RANGE, "read range too large");
        }
        let (entry, path) = {
            let inner = self.inner.lock().unwrap();
            let Some(offer) = inner
                .offers
                .iter()
                .find(|offer| offer.serial == request.serial)
            else {
                return error(request.request_id, FILE_ERR_NOT_FOUND, "unknown offer");
            };
            let Some(entry) = offer.manifest.entries.get(request.entry_id as usize) else {
                return error(request.request_id, FILE_ERR_NOT_FOUND, "unknown entry");
            };
            if !entry.is_file() {
                return error(request.request_id, FILE_ERR_RANGE, "not a file");
            }
            let Some(path) = offer.paths.get(request.entry_id as usize).cloned() else {
                return error(request.request_id, FILE_ERR_NOT_FOUND, "missing path");
            };
            (entry.clone(), path)
        };
        if request.offset > entry.size
            || request.offset.saturating_add(request.length as u64) > entry.size
        {
            return error(request.request_id, FILE_ERR_RANGE, "read outside file");
        }
        let offset = request.offset;
        let length = request.length as usize;
        let read = tokio::task::spawn_blocking(move || read_at(&path, offset, length)).await;
        match read {
            Ok(Ok(data)) => Message::FileData(FileData {
                request_id: request.request_id,
                offset,
                data,
            }),
            Ok(Err(e)) => error(request.request_id, FILE_ERR_IO, &e.to_string()),
            Err(e) => error(request.request_id, FILE_ERR_IO, &e.to_string()),
        }
    }

    /// Offer count, for tests.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().offers.len()
    }
}

fn error(request_id: u64, code: u8, message: &str) -> Message {
    Message::FileError(FileError {
        request_id,
        code,
        message: message.to_string(),
    })
}

fn read_at(path: &PathBuf, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut data = vec![0u8; length];
    let mut done = 0usize;
    while done < length {
        #[cfg(unix)]
        let n = file.read_at(&mut data[done..], offset + done as u64)?;
        #[cfg(windows)]
        let n = file.seek_read(&mut data[done..], offset + done as u64)?;
        if n == 0 {
            break;
        }
        done += n;
    }
    data.truncate(done);
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::manifest::KIND_FILE;
    use crate::file::manifest::ManifestEntry;

    fn offer(name: &[u8], file: &std::path::Path, data: &[u8]) -> BuiltOffer {
        std::fs::write(file, data).unwrap();
        let entry = ManifestEntry {
            id: 0,
            parent: None,
            kind: KIND_FILE,
            name: name.to_vec(),
            size: data.len() as u64,
            mode: 0o644,
            mtime_sec: 0,
            mtime_nsec: 0,
        };
        let manifest = Manifest {
            entries: vec![entry],
        };
        let encoded = manifest
            .encode(ManifestLimits {
                max_entries: 10,
                max_bytes: 1024 * 1024,
            })
            .unwrap();
        BuiltOffer {
            manifest,
            paths: vec![file.to_path_buf()],
            encoded,
        }
    }

    fn temp_file(tag: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("lan_mouse_sender_{}_{}", tag, std::process::id()));
        path
    }

    #[tokio::test]
    async fn reads_ranges_and_reports_bad_ones() {
        let path = temp_file("read");
        let store = Arc::new(OfferStore::new());
        store.insert(4, &offer(b"f", &path, b"0123456789"));

        let Message::FileData(data) = store
            .handle_read(FileRead {
                request_id: 1,
                serial: 4,
                entry_id: 0,
                offset: 2,
                length: 4,
            })
            .await
        else {
            panic!("expected data");
        };
        assert_eq!(data.data, b"2345");

        let Message::FileError(err) = store
            .handle_read(FileRead {
                request_id: 2,
                serial: 4,
                entry_id: 0,
                offset: 9,
                length: 4,
            })
            .await
        else {
            panic!("expected range error");
        };
        assert_eq!(err.code, FILE_ERR_RANGE);

        let Message::FileError(missing) = store
            .handle_read(FileRead {
                request_id: 3,
                serial: 99,
                entry_id: 0,
                offset: 0,
                length: 1,
            })
            .await
        else {
            panic!("expected missing error");
        };
        assert_eq!(missing.code, FILE_ERR_NOT_FOUND);
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn short_read_at_eof_is_returned_as_is() {
        let path = temp_file("eof");
        let store = Arc::new(OfferStore::new());
        store.insert(1, &offer(b"f", &path, b"abc"));
        let Message::FileData(data) = store
            .handle_read(FileRead {
                request_id: 1,
                serial: 1,
                entry_id: 0,
                offset: 0,
                length: 3,
            })
            .await
        else {
            panic!("expected data");
        };
        assert_eq!(data.data, b"abc");
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn broker_and_sender_roundtrip_chunks() {
        use crate::file::broker::Broker;
        use crate::file::manifest::{ManifestLimits, build_manifest};
        use crate::protocol::Message;

        let data: Vec<u8> = (0..(2 * 1024 * 1024 + 4096))
            .map(|i| (i % 251) as u8)
            .collect();
        let path = temp_file("roundtrip");
        std::fs::write(&path, &data).unwrap();
        let limits = ManifestLimits {
            max_entries: 10,
            max_bytes: 1024 * 1024,
        };
        let built = build_manifest(std::slice::from_ref(&path), limits).unwrap();
        let encoded = built.manifest.encode(limits).unwrap();
        let offer = BuiltOffer {
            manifest: built.manifest,
            paths: built.paths,
            encoded,
        };
        let store = Arc::new(OfferStore::new());
        store.insert(11, &offer);

        let (directed_tx, mut directed_rx) = tokio::sync::mpsc::channel(32);
        let broker = Arc::new(Broker::new(directed_tx));
        let responder_store = store.clone();
        let responder_broker = broker.clone();
        tokio::spawn(async move {
            while let Some((origin, frame)) = directed_rx.recv().await {
                if let Ok(Message::FileRead(read)) = Message::decode(&frame[4..]) {
                    match responder_store.handle_read(read).await {
                        Message::FileData(data) => {
                            responder_broker.complete(origin, data);
                        }
                        Message::FileError(error) => {
                            responder_broker.fail(origin, error);
                        }
                        other => log::warn!("unexpected sender response: {other:?}"),
                    }
                }
            }
        });

        // Two full 1 MiB chunks and the final short chunk.
        let mut got = Vec::new();
        for offset in [0u64, 1024 * 1024, 2 * 1024 * 1024] {
            let want = (data.len() as u64 - offset).min(1024 * 1024) as u32;
            let chunk = broker
                .read([1; 8], 11, 0, offset, want)
                .await
                .expect("chunk read");
            assert_eq!(chunk.len(), want as usize);
            got.extend_from_slice(&chunk);
        }
        assert_eq!(got, data);

        let missing = broker.read([1; 8], 99, 0, 0, 4).await;
        assert!(missing.is_err());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn offer_store_evicts_oldest_beyond_cap() {
        let store = OfferStore::new();
        let path = temp_file("evict");
        for serial in 0..(MAX_OFFERS as u64 + 3) {
            store.insert(serial, &offer(b"f", &path, b"x"));
        }
        assert_eq!(store.len(), MAX_OFFERS);
        std::fs::remove_file(&path).unwrap();
    }
}
