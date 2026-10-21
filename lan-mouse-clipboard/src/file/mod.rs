//! Lazy file transfer: metadata first, bytes on demand.
//!
//! The sender parses a local file URI list, builds a manifest in a worker,
//! broadcasts it, and serves ranges for its last few offers. The receiver
//! mounts one read-only FUSE view for the daemon's lifetime and only fetches
//! chunks when an application actually reads a file.

pub mod broker;
#[cfg(target_os = "linux")]
pub mod fuse;
pub mod manifest;
pub mod receiver;
pub mod sender;
#[cfg(windows)]
pub mod windows;

use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};

use crate::FileOptions;
use crate::file::broker::Broker;
#[cfg(target_os = "linux")]
use crate::file::fuse::ClipboardMount;
use crate::file::manifest::{MAX_MANIFEST_BYTES, MAX_MANIFEST_ENTRIES, ManifestLimits};
use crate::file::receiver::Receiver;
use crate::file::sender::{MAX_INFLIGHT_READS, ManifestWorker, OfferStore, PreparedItem};

/// The running file-sync subsystem (sender, receiver and optional FUSE view).
pub struct FileService {
    pub offers: Arc<OfferStore>,
    pub broker: Arc<Broker>,
    pub receiver: Receiver,
    pub worker: ManifestWorker,
    #[cfg(target_os = "linux")]
    pub mount: Option<Arc<ClipboardMount>>,
    /// Runtime handle used by Windows OLE streams.
    pub runtime: tokio::runtime::Handle,
    /// Bounds driver-spawned read tasks: eight active plus eight queued.
    pub read_slots: Arc<Semaphore>,
}

impl FileService {
    /// Start the file subsystem. Returns `None` when disabled.
    ///
    /// A missing FUSE mount disables lazy receive but keeps sending working;
    /// new peers ignore offers and fall back to the Announce path.
    pub fn start(
        options: FileOptions,
        directed_tx: mpsc::Sender<([u8; 8], Vec<u8>)>,
        runtime: tokio::runtime::Handle,
    ) -> Option<(Self, mpsc::Receiver<PreparedItem>)> {
        if !options.enabled {
            return None;
        }
        let limits = ManifestLimits {
            max_entries: options.max_entries.clamp(1, MAX_MANIFEST_ENTRIES),
            max_bytes: options
                .max_manifest_bytes
                .clamp(1024 * 1024, MAX_MANIFEST_BYTES),
        };
        let broker = Arc::new(Broker::new(directed_tx));
        #[cfg(target_os = "linux")]
        let mount = ClipboardMount::start(broker.clone(), runtime.clone());
        #[cfg(target_os = "linux")]
        if mount.is_none() {
            log::warn!(
                "file sync: FUSE view unavailable; receiving files lazily is disabled \
                 (sending offers still works)"
            );
        }
        let (worker, build_rx) = ManifestWorker::spawn(limits);
        Some((
            Self {
                offers: Arc::new(OfferStore::new()),
                broker,
                receiver: {
                    #[cfg(target_os = "linux")]
                    {
                        Receiver::new(mount.clone(), limits)
                    }
                    #[cfg(windows)]
                    {
                        Receiver::new(limits)
                    }
                },
                worker,
                #[cfg(target_os = "linux")]
                mount,
                runtime,
                read_slots: Arc::new(Semaphore::new(MAX_INFLIGHT_READS * 2)),
            },
            build_rx,
        ))
    }
}
