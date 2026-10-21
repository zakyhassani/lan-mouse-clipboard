//! The clipboard sync subsystem.
//!
//! Owns the clipboard backend(s) and loop prevention, and drives the local
//! clipboard watcher. It runs as its own spawned task (mirroring the
//! capture/emulation subsystems). The separate [`network`] task owns the
//! TLS listener + connection pool and moves frames between peers and the
//! shared inbound/broadcast channels.

pub mod backend;
pub mod dedup;
#[cfg(any(target_os = "linux", windows))]
pub mod file;
pub mod item;
pub mod network;
pub mod protocol;
pub mod registry;
pub mod transport;

#[cfg(test)]
mod test_util;

pub use backend::BackendKind;
pub use item::{ClipboardItem, DEFAULT_MAX_ITEM_SIZE, MAX_ITEM_SIZE_LIMIT};

/// Default manifest entry cap for lazy file offers.
pub const DEFAULT_FILE_MANIFEST_MAX_ENTRIES: usize = 100_000;
/// Absolute manifest entry cap.
pub const MAX_FILE_MANIFEST_MAX_ENTRIES: usize = 1_000_000;
/// Default manifest byte cap for lazy file offers (16 MiB).
pub const DEFAULT_FILE_MANIFEST_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Absolute manifest byte cap (64 MiB).
pub const MAX_FILE_MANIFEST_MAX_BYTES: usize = 64 * 1024 * 1024;

use std::pin::Pin;

use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use crate::backend::{ClipboardBackend, build_backends};
use crate::dedup::LoopPrevention;
use crate::item::ClipboardChange;
use crate::network::InboundFrame;
use crate::protocol::Message;

/// Lazy file-sync settings.
///
/// `enabled` is the user preference; the file subsystem starts on the first
/// time clipboard sync is enabled (and only on Linux).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOptions {
    pub enabled: bool,
    pub max_entries: usize,
    pub max_manifest_bytes: usize,
}

impl Default for FileOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            max_entries: DEFAULT_FILE_MANIFEST_MAX_ENTRIES,
            max_manifest_bytes: DEFAULT_FILE_MANIFEST_MAX_BYTES,
        }
    }
}

/// Everything the clipboard driver needs to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClipboardOptions {
    pub enabled: bool,
    pub backend: BackendKind,
    pub max_item_size: usize,
    pub origin: [u8; 8],
    pub files: FileOptions,
}

/// Events emitted by the clipboard subsystem for frontends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClipboardEvent {
    /// Clipboard sync became enabled.
    Enabled,
    /// Clipboard sync became disabled.
    Disabled,
}

enum ClipboardRequest {
    SetEnabled(bool),
}

/// A handle to the clipboard subsystem (driver runs in a spawned task).
pub struct Clipboard {
    request_tx: mpsc::Sender<ClipboardRequest>,
    event_rx: mpsc::Receiver<ClipboardEvent>,
    _task: tokio::task::JoinHandle<()>,
}

struct ClipboardInner {
    enabled: bool,
    origin: [u8; 8],
    max_item_size: usize,
    backends: Vec<Box<dyn ClipboardBackend>>,
    loop_prevention: LoopPrevention,
    watcher: Option<Pin<Box<dyn Stream<Item = ClipboardChange> + Send>>>,
    #[cfg_attr(not(any(target_os = "linux", windows)), allow(dead_code))]
    file_options: FileOptions,
    #[cfg_attr(not(any(target_os = "linux", windows)), allow(dead_code))]
    direct_tx: mpsc::Sender<([u8; 8], Vec<u8>)>,
    #[cfg(any(target_os = "linux", windows))]
    files: Option<file::FileService>,
}

impl Clipboard {
    /// Create the clipboard subsystem, spawning its driver task.
    ///
    /// `inbound_rx` receives frames (tagged with the authenticated peer
    /// origin) from the network task; `broadcast_tx` carries broadcast frames
    /// and `directed_tx` carries replies to one peer. The item size cap applies
    /// to text/image items; file frames have their own caps.
    pub fn new(
        options: ClipboardOptions,
        inbound_rx: mpsc::Receiver<InboundFrame>,
        broadcast_tx: mpsc::Sender<Vec<u8>>,
        directed_tx: mpsc::Sender<([u8; 8], Vec<u8>)>,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(16);
        let inner = ClipboardInner {
            enabled: options.enabled,
            origin: options.origin,
            max_item_size: options.max_item_size,
            backends: build_backends(options.backend, options.max_item_size),
            loop_prevention: LoopPrevention::new(options.origin),
            watcher: None,
            file_options: options.files,
            direct_tx: directed_tx,
            #[cfg(any(target_os = "linux", windows))]
            #[cfg(any(target_os = "linux", windows))]
            files: None,
        };
        let task = tokio::spawn(run_clipboard(
            inner,
            request_rx,
            event_tx,
            inbound_rx,
            broadcast_tx,
        ));
        Self {
            request_tx,
            event_rx,
            _task: task,
        }
    }

    /// Enable/disable clipboard sync (fire-and-forget).
    pub fn set_enabled(&self, enabled: bool) {
        let _ = self
            .request_tx
            .try_send(ClipboardRequest::SetEnabled(enabled));
    }

    /// The next frontend-facing event.
    pub async fn event(&mut self) -> ClipboardEvent {
        self.event_rx
            .recv()
            .await
            .expect("clipboard event channel closed")
    }
}

async fn run_clipboard(
    mut inner: ClipboardInner,
    mut request_rx: mpsc::Receiver<ClipboardRequest>,
    event_tx: mpsc::Sender<ClipboardEvent>,
    mut inbound_rx: mpsc::Receiver<InboundFrame>,
    broadcast_tx: mpsc::Sender<Vec<u8>>,
) {
    #[cfg(any(target_os = "linux", windows))]
    let mut build_rx = if inner.enabled {
        start_files(&mut inner)
    } else {
        None
    };
    #[cfg(not(any(target_os = "linux", windows)))]
    let mut build_rx: Option<mpsc::Receiver<PreparedItem>> = None;

    if inner.enabled {
        ensure_watcher(&mut inner).await;
    }

    loop {
        tokio::select! {
            req = request_rx.recv() => match req {
                Some(ClipboardRequest::SetEnabled(enabled)) => {
                    if inner.enabled == enabled { continue; }
                    inner.enabled = enabled;
                    let _ = event_tx.send(if enabled {
                        ClipboardEvent::Enabled
                    } else {
                        ClipboardEvent::Disabled
                    }).await;
                    if enabled {
                        #[cfg(any(target_os = "linux", windows))]
                        if build_rx.is_none() {
                            build_rx = start_files(&mut inner);
                        }
                        ensure_watcher(&mut inner).await;
                    } else {
                        inner.watcher = None;
                    }
                }
                None => break,
            },
            change = poll_watcher(&mut inner.watcher) => match change {
                Some(ClipboardChange::New(item)) => {
                    if !inner.enabled { continue; }
                    if !handle_local_change(&mut inner, item, &broadcast_tx).await {
                        break;
                    }
                }
                None => {
                    // The backend's change stream ended. Clear it so the
                    // select loop parks instead of polling a closed stream.
                    log::warn!("clipboard watcher ended");
                    inner.watcher = None;
                }
            },
            prepared = poll_prepared(&mut build_rx) => {
                let Some(prepared) = prepared else {
                    // The build worker is gone; stop polling the closed
                    // channel so the select loop does not spin.
                    build_rx = None;
                    continue;
                };
                if !handle_prepared(&mut inner, prepared, &broadcast_tx).await { break; }
            }
            frame = inbound_rx.recv() => match frame {
                Some(frame) => handle_inbound(&mut inner, frame, &broadcast_tx).await,
                None => break,
            },
        }
    }
}

/// A manifest build result. Linux and Windows build the lazy file view.
#[cfg(any(target_os = "linux", windows))]
use crate::file::sender::PreparedItem;
#[cfg(not(any(target_os = "linux", windows)))]
#[allow(dead_code)]
struct PreparedItem;

/// Start the file subsystem on first enable, if it is configured.
#[cfg(any(target_os = "linux", windows))]
fn start_files(inner: &mut ClipboardInner) -> Option<mpsc::Receiver<PreparedItem>> {
    if inner.files.is_some() || !inner.file_options.enabled {
        return None;
    }
    match file::FileService::start(
        inner.file_options,
        inner.direct_tx.clone(),
        tokio::runtime::Handle::current(),
    ) {
        Some((files, rx)) => {
            inner.files = Some(files);
            Some(rx)
        }
        None => None,
    }
}

/// Await the next prepared local item (or park when file sync is off).
async fn poll_prepared(
    build_rx: &mut Option<mpsc::Receiver<PreparedItem>>,
) -> Option<PreparedItem> {
    match build_rx {
        Some(rx) => rx.recv().await,
        None => futures::future::pending().await,
    }
}

/// Broadcast a local clipboard item as an Announce. Returns false when the
/// network channel is closed.
async fn broadcast_announce(
    max_item_size: usize,
    item: ClipboardItem,
    broadcast_tx: &mpsc::Sender<Vec<u8>>,
) -> bool {
    match Message::Announce(item).encode(max_item_size) {
        Ok(frame) => broadcast_tx.send(frame).await.is_ok(),
        Err(e) => {
            log::warn!("failed to encode clipboard item: {e}");
            true
        }
    }
}

/// Apply loop prevention to a local change, queue file candidates for a
/// manifest, and broadcast everything else immediately.
async fn handle_local_change(
    inner: &mut ClipboardInner,
    mut item: ClipboardItem,
    broadcast_tx: &mpsc::Sender<Vec<u8>>,
) -> bool {
    #[cfg(target_os = "linux")]
    if let Some(files) = &inner.files {
        // Reading a file from the lazy view changes the local clipboard to
        // URIs that point back into our own mount. Never echo those.
        let paths = file::manifest::item_paths(&item);
        if !paths.is_empty() && paths.iter().all(|p| files.receiver.contains_path(p)) {
            log::debug!("file sync: ignoring local change from the lazy view");
            return true;
        }
    }

    item.origin = inner.origin;
    item.serial = inner.loop_prevention.next_serial();
    if !inner.loop_prevention.on_local_change(&item) {
        return true;
    }

    #[cfg(any(target_os = "linux", windows))]
    if let Some(files) = &inner.files {
        if file::manifest::is_file_candidate(&item) {
            if files.worker.submit(item.clone()).await {
                return true;
            }
            log::warn!("file sync: manifest worker stopped; sending Announce only");
        }
    }

    inner.loop_prevention.note_sent(&item);
    broadcast_announce(inner.max_item_size, item, broadcast_tx).await
}

/// Send a built file offer before its Announce so a new peer can create the
/// view and a new receiver can suppress the legacy path.
async fn handle_prepared(
    inner: &mut ClipboardInner,
    prepared: PreparedItem,
    broadcast_tx: &mpsc::Sender<Vec<u8>>,
) -> bool {
    #[cfg(any(target_os = "linux", windows))]
    {
        let file::sender::PreparedItem { item, offer } = prepared;
        if !inner.enabled {
            return true;
        }
        inner.loop_prevention.note_sent(&item);
        if let (Some(files), Some(offer)) = (inner.files.as_ref(), offer) {
            log::info!(
                "file sync: offering {} entries ({} manifest bytes) as serial {}",
                offer.manifest.entries.len(),
                offer.encoded.len(),
                item.serial
            );
            files.offers.insert(item.serial, &offer);
            let message = Message::FileOffer(protocol::FileOffer {
                version: protocol::FILE_PROTOCOL_VERSION,
                origin: item.origin,
                serial: item.serial,
                manifest: offer.encoded.clone(),
            });
            match message.encode(inner.max_item_size) {
                Ok(frame) => {
                    if broadcast_tx.send(frame).await.is_err() {
                        return false;
                    }
                }
                Err(e) => log::warn!("file sync: cannot encode offer: {e}"),
            }
        }
        broadcast_announce(inner.max_item_size, item, broadcast_tx).await
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (inner, prepared, broadcast_tx);
        true
    }
}

/// Dispatch one inbound frame from an authenticated peer.
async fn handle_inbound(
    inner: &mut ClipboardInner,
    frame: InboundFrame,
    _broadcast_tx: &mpsc::Sender<Vec<u8>>,
) {
    let peer_origin = frame.origin;
    match Message::decode(&frame.payload) {
        Ok(Message::Announce(item)) => {
            #[cfg(any(target_os = "linux", windows))]
            if let Some(files) = &mut inner.files {
                if files.receiver.take_lazy(item.origin, item.serial) {
                    log::debug!(
                        "file sync: suppressing Announce for lazy offer {}",
                        item.serial
                    );
                    return;
                }
            }
            apply_remote(inner, &item).await;
        }
        Ok(Message::FileOffer(offer)) => handle_file_offer(inner, peer_origin, &offer).await,
        Ok(Message::FileRead(read)) => handle_file_read(inner, peer_origin, read).await,
        Ok(Message::FileData(data)) => {
            #[cfg(any(target_os = "linux", windows))]
            if let Some(files) = &inner.files {
                files.broker.complete(peer_origin, data);
            }
            #[cfg(not(any(target_os = "linux", windows)))]
            let _ = data;
        }
        Ok(Message::FileError(error)) => {
            #[cfg(any(target_os = "linux", windows))]
            if let Some(files) = &inner.files {
                files.broker.fail(peer_origin, error);
            }
            #[cfg(not(any(target_os = "linux", windows)))]
            let _ = error;
        }
        Ok(Message::Ping | Message::Pong) => {}
        Err(e) => log::debug!("discarding clipboard frame: {e}"),
    }
}

async fn handle_file_offer(
    inner: &mut ClipboardInner,
    peer_origin: [u8; 8],
    offer: &protocol::FileOffer,
) {
    if !inner.enabled {
        return;
    }
    #[cfg(any(target_os = "linux", windows))]
    {
        let self_origin = inner.origin;
        let accepted = inner
            .files
            .as_mut()
            .and_then(|files| files.receiver.accept(peer_origin, self_origin, offer));

        #[cfg(target_os = "linux")]
        {
            let Some(crate::file::receiver::AcceptedFile::UriList(uri_item)) = accepted else {
                return;
            };
            // The URI list is generated locally, so it does not pass through the
            // backend's normal item-size accumulation. Enforce the same cap here.
            if uri_item.total_size() > inner.max_item_size {
                log::warn!(
                    "file sync: generated URI list is {} bytes, over the {} byte cap",
                    uri_item.total_size(),
                    inner.max_item_size
                );
                if let Some(files) = inner.files.as_mut() {
                    files.receiver.discard(peer_origin, offer.serial);
                }
                return;
            }
            // The primary backend must accept the URI list; history sinks are
            // best effort and do not make a failed file set a success.
            let mut primary_ok = false;
            for (index, backend) in inner.backends.iter().enumerate() {
                match backend.set(&uri_item).await {
                    Ok(()) if index == 0 => primary_ok = true,
                    Ok(()) => {}
                    Err(e) => log::warn!("{} set failed: {e}", backend.name()),
                }
            }
            let Some(files) = inner.files.as_mut() else {
                return;
            };
            if primary_ok {
                // Suppress the local watcher's echo of this exact URI list.
                inner.loop_prevention.note_sent(&uri_item);
                files.receiver.mark_lazy(peer_origin, offer.serial);
                files.receiver.gc();
                log::info!(
                    "file sync: accepted lazy offer serial {} from {:02x?}",
                    offer.serial,
                    peer_origin
                );
            } else {
                files.receiver.discard(peer_origin, offer.serial);
            }
        }

        #[cfg(windows)]
        {
            let Some(files) = inner.files.as_ref() else {
                return;
            };
            let Some(crate::file::receiver::AcceptedFile::VirtualFiles(virtual_files)) = accepted
            else {
                // A recognized offer that cannot be represented on Windows must
                // still suppress its Announce: the sender's paths are not local.
                if offer.origin == peer_origin && offer.origin != self_origin {
                    if let Some(files) = inner.files.as_mut() {
                        files.receiver.mark_lazy(peer_origin, offer.serial);
                    }
                }
                return;
            };
            let set = std::sync::Arc::new(crate::file::windows::RemoteFileSet {
                origin: offer.origin,
                serial: offer.serial,
                manifest: virtual_files.manifest,
                entries: virtual_files.entries,
                broker: files.broker.clone(),
                runtime: files.runtime.clone(),
            });
            let installed = match inner.backends.first() {
                Some(backend) => match backend.set_file_offer(&set).await {
                    Ok(installed) => installed,
                    Err(e) => {
                        log::warn!("{} file offer set failed: {e}", backend.name());
                        false
                    }
                },
                None => false,
            };
            if let Some(files) = inner.files.as_mut() {
                files.receiver.mark_lazy(peer_origin, offer.serial);
                files.receiver.gc();
            }
            if installed {
                log::info!(
                    "file sync: accepted virtual offer serial {} from {:02x?} ({} entries)",
                    offer.serial,
                    peer_origin,
                    set.entries.len()
                );
            } else {
                log::warn!(
                    "file sync: could not install virtual offer serial {}; Announce suppressed",
                    offer.serial
                );
            }
        }
    }

    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (inner, peer_origin, offer);
    }
}

async fn handle_file_read(
    inner: &mut ClipboardInner,
    peer_origin: [u8; 8],
    read: protocol::FileRead,
) {
    #[cfg(any(target_os = "linux", windows))]
    if let Some(files) = &inner.files {
        let store = files.offers.clone();
        let tx = inner.direct_tx.clone();
        let permit = match files.read_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                // Do not queue unbounded read tasks behind the semaphore. The
                // requester sees EIO and may retry a chunk later.
                let response = Message::FileError(protocol::FileError {
                    request_id: read.request_id,
                    code: protocol::FILE_ERR_UNAVAILABLE,
                    message: "sender busy".into(),
                });
                if let Ok(frame) = response.encode(0) {
                    let _ = tx.try_send((peer_origin, frame));
                }
                return;
            }
        };
        tokio::spawn(async move {
            let _permit = permit;
            let response = store.handle_read(read).await;
            match response.encode(0) {
                Ok(frame) => {
                    let _ = tx.send((peer_origin, frame)).await;
                }
                Err(e) => log::warn!("file sync: cannot encode read response: {e}"),
            }
        });
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (inner, peer_origin, read);
    }
}

/// Start watching the local clipboard via the primary backend, if not already.
async fn ensure_watcher(inner: &mut ClipboardInner) {
    if inner.watcher.is_none() {
        if let Some(primary) = inner.backends.first() {
            inner.watcher = Some(primary.watch().await);
        }
    }
}

/// Apply a remote item subject to loop prevention, setting the local
/// clipboard + history sinks.
async fn apply_remote(inner: &mut ClipboardInner, item: &ClipboardItem) {
    if !inner.enabled {
        return;
    }
    if !inner.loop_prevention.should_apply_remote(item) {
        return;
    }
    for backend in &inner.backends {
        if let Err(e) = backend.set(item).await {
            log::warn!("{} set failed: {e}", backend.name());
        }
    }
}

/// Await the next local-clipboard change from the watcher (or park if none).
async fn poll_watcher(
    watcher: &mut Option<Pin<Box<dyn Stream<Item = ClipboardChange> + Send>>>,
) -> Option<ClipboardChange> {
    match watcher {
        Some(w) => w.next().await,
        None => futures::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::Rng;

    #[tokio::test]
    async fn set_enabled_emits_event() {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(64);
        let (directed_tx, _directed_rx) = mpsc::channel(16);
        let mut clip = Clipboard::new(
            ClipboardOptions {
                enabled: false,
                backend: BackendKind::Dummy,
                max_item_size: DEFAULT_MAX_ITEM_SIZE,
                origin: [0xAA; 8],
                files: FileOptions::default(),
            },
            inbound_rx,
            broadcast_tx,
            directed_tx,
        );
        let _ = inbound_tx;
        clip.set_enabled(true);
        let ev = clip.event().await;
        assert_eq!(ev, ClipboardEvent::Enabled);
    }

    #[tokio::test]
    async fn inbound_frame_is_applied_to_backend() {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(64);
        let (directed_tx, _directed_rx) = mpsc::channel(16);
        let _clip = Clipboard::new(
            ClipboardOptions {
                enabled: true,
                backend: BackendKind::Dummy,
                max_item_size: DEFAULT_MAX_ITEM_SIZE,
                origin: [0xAA; 8],
                files: FileOptions::default(),
            },
            inbound_rx,
            broadcast_tx,
            directed_tx,
        );

        let item = ClipboardItem::text("remote", [0xBB; 8], 1);
        let frame = Message::Announce(item).encode(1024).unwrap();
        let payload = frame[4..].to_vec();
        // Driver applies it without erroring; we just exercise the path.
        inbound_tx
            .send(InboundFrame {
                origin: [0xBB; 8],
                payload,
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // ------------------------------------------------------------------
    // Driver tests using a controllable stub backend (a test double).
    // ------------------------------------------------------------------

    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use futures::Stream;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::backend::BackendError;

    /// A scriptable backend double: tests feed it local changes through
    /// `watch_tx` and observe what the driver writes via `set`.
    #[derive(Clone)]
    struct StubBackend {
        set_count: Arc<AtomicUsize>,
        last_set: Arc<StdMutex<Option<ClipboardItem>>>,
        watch_tx: mpsc::Sender<ClipboardChange>,
        watch_rx: Arc<StdMutex<Option<mpsc::Receiver<ClipboardChange>>>>,
    }

    impl StubBackend {
        fn new() -> Self {
            let (watch_tx, watch_rx) = mpsc::channel(64);
            Self {
                set_count: Arc::new(AtomicUsize::new(0)),
                last_set: Arc::new(StdMutex::new(None)),
                watch_tx,
                watch_rx: Arc::new(StdMutex::new(Some(watch_rx))),
            }
        }

        fn set_count(&self) -> usize {
            self.set_count.load(Ordering::SeqCst)
        }

        fn last_set(&self) -> Option<ClipboardItem> {
            self.last_set.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ClipboardBackend for StubBackend {
        fn name(&self) -> &'static str {
            "stub"
        }

        async fn read_current(&self) -> Option<ClipboardItem> {
            None
        }

        async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError> {
            self.set_count.fetch_add(1, Ordering::SeqCst);
            *self.last_set.lock().unwrap() = Some(item.clone());
            Ok(())
        }

        async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
            let rx = self
                .watch_rx
                .lock()
                .unwrap()
                .take()
                .expect("watch called once");
            Box::pin(ReceiverStream::new(rx))
        }
    }

    struct Driver {
        stub: StubBackend,
        inbound_tx: mpsc::Sender<InboundFrame>,
        broadcast_rx: mpsc::Receiver<Vec<u8>>,
        _request_tx: mpsc::Sender<ClipboardRequest>,
        _event_rx: mpsc::Receiver<ClipboardEvent>,
        _task: tokio::task::JoinHandle<()>,
    }

    fn spawn_driver(enabled: bool, origin: [u8; 8]) -> Driver {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (broadcast_tx, broadcast_rx) = mpsc::channel(64);
        let (directed_tx, _directed_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(16);
        let (request_tx, request_rx) = mpsc::channel(16);
        let stub = StubBackend::new();
        let inner = ClipboardInner {
            enabled,
            origin,
            max_item_size: 1024,
            backends: vec![Box::new(stub.clone())],
            loop_prevention: LoopPrevention::new(origin),
            watcher: None,
            file_options: FileOptions::default(),
            direct_tx: directed_tx,
            files: None,
        };
        let task = tokio::spawn(run_clipboard(
            inner,
            request_rx,
            event_tx,
            inbound_rx,
            broadcast_tx,
        ));
        Driver {
            stub,
            inbound_tx,
            broadcast_rx,
            _request_tx: request_tx,
            _event_rx: event_rx,
            _task: task,
        }
    }

    async fn wait_until(mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !cond() {
            if tokio::time::Instant::now() >= deadline {
                panic!("condition not met before timeout");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn local_change_is_broadcast_with_origin_and_serial() {
        let mut drv = spawn_driver(true, [0xAA; 8]);
        drv.stub
            .watch_tx
            .send(ClipboardChange::New(ClipboardItem::text("hi", [0; 8], 0)))
            .await
            .unwrap();

        let frame = drv.broadcast_rx.recv().await.unwrap();
        let payload = &frame[4..];
        match Message::decode(payload).unwrap() {
            Message::Announce(item) => {
                assert_eq!(item.origin, [0xAA; 8]);
                assert!(item.serial >= 1, "serial should be allocated");
                assert_eq!(item.text_plain().as_deref(), Some("hi"));
            }
            other => panic!("expected Announce, got {other:?}"),
        }
    }

    fn multi_rep_image_item() -> ClipboardItem {
        ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![
                (
                    "image/png".into(),
                    vec![0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x0a],
                ),
                ("text/html".into(), b"<img>".to_vec()),
                ("text/plain".into(), b"alt".to_vec()),
            ],
        }
    }

    #[tokio::test]
    async fn local_image_item_is_broadcast_with_all_reps() {
        let mut drv = spawn_driver(true, [0xAA; 8]);
        drv.stub
            .watch_tx
            .send(ClipboardChange::New(multi_rep_image_item()))
            .await
            .unwrap();

        let frame = drv.broadcast_rx.recv().await.unwrap();
        match Message::decode(&frame[4..]).unwrap() {
            Message::Announce(item) => {
                assert_eq!(item.origin, [0xAA; 8]);
                assert!(item.serial >= 1);
                assert_eq!(item.primary_mime(), Some("image/png"));
                assert_eq!(item.reps.len(), 3, "all offered reps travel");
            }
            other => panic!("expected Announce, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn image_remote_apply_then_primary_only_echo_is_suppressed() {
        let mut drv = spawn_driver(true, [0xAA; 8]);

        let mut remote = multi_rep_image_item();
        remote.origin = [0xBB; 8];
        remote.serial = 1;
        let frame = Message::Announce(remote.clone()).encode(1024).unwrap();
        drv.inbound_tx
            .send(InboundFrame {
                origin: [0xBB; 8],
                payload: frame[4..].to_vec(),
            })
            .await
            .unwrap();

        wait_until(|| drv.stub.set_count() >= 1).await;
        let applied = drv.stub.last_set().expect("applied");
        assert_eq!(applied.reps.len(), 3);

        // The receiver can only advertise the primary rep, so its echo is the
        // primary rep alone. That must still be recognized as an echo.
        let echo = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![remote.reps[0].clone()],
        };
        drv.stub
            .watch_tx
            .send(ClipboardChange::New(echo))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            drv.broadcast_rx.try_recv().is_err(),
            "primary-only echo of a multi-rep remote item must be suppressed"
        );
    }

    #[tokio::test]
    async fn legacy_peer_echo_of_a_secondary_rep_is_ignored() {
        let mut drv = spawn_driver(true, [0xAA; 8]);
        // We copy a multi-rep image and broadcast it.
        drv.stub
            .watch_tx
            .send(ClipboardChange::New(multi_rep_image_item()))
            .await
            .unwrap();
        let frame = drv.broadcast_rx.recv().await.unwrap();
        assert!(matches!(
            Message::decode(&frame[4..]).unwrap(),
            Message::Announce(_)
        ));

        // A peer that only preserved a secondary rep echoes it back as a
        // single-rep item. It must not be applied or re-broadcast.
        let echo = ClipboardItem {
            origin: [0xBB; 8],
            serial: 9,
            reps: vec![("text/plain".into(), b"alt".to_vec())],
        };
        let frame = Message::Announce(echo).encode(1024).unwrap();
        drv.inbound_tx
            .send(InboundFrame {
                origin: [0xBB; 8],
                payload: frame[4..].to_vec(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            drv.stub.set_count(),
            0,
            "a secondary-rep echo must not be applied"
        );
        assert!(
            drv.broadcast_rx.try_recv().is_err(),
            "a secondary-rep echo must not be re-broadcast"
        );
    }

    #[tokio::test]
    async fn remote_change_is_applied_and_echo_is_suppressed() {
        let mut drv = spawn_driver(true, [0xAA; 8]);
        let remote = ClipboardItem::text("hi", [0xBB; 8], 1);
        let frame = Message::Announce(remote).encode(1024).unwrap();
        drv.inbound_tx
            .send(InboundFrame {
                origin: [0xBB; 8],
                payload: frame[4..].to_vec(),
            })
            .await
            .unwrap();

        wait_until(|| drv.stub.set_count() >= 1).await;
        assert_eq!(
            drv.stub.last_set().and_then(|i| i.text_plain()).as_deref(),
            Some("hi")
        );

        // The local backend echoes the same content back; it must not be
        // re-broadcast (loop prevention).
        drv.stub
            .watch_tx
            .send(ClipboardChange::New(ClipboardItem::text("hi", [0; 8], 0)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            drv.broadcast_rx.try_recv().is_err(),
            "echo of a remote item must be suppressed"
        );
    }

    #[tokio::test]
    async fn random_remote_changes_are_applied_and_echoes_suppressed() {
        let mut rng = Rng::new(0x5000);
        for _ in 0..50 {
            let mut drv = spawn_driver(true, [0xAA; 8]);
            let payload = format!("random-{}", rng.next());
            let remote = ClipboardItem::text(&payload, [0xBB; 8], 1);
            let frame = Message::Announce(remote).encode(1024).unwrap();
            drv.inbound_tx
                .send(InboundFrame {
                    origin: [0xBB; 8],
                    payload: frame[4..].to_vec(),
                })
                .await
                .unwrap();

            wait_until(|| drv.stub.set_count() >= 1).await;
            assert_eq!(
                drv.stub.last_set().and_then(|i| i.text_plain()).as_deref(),
                Some(payload.as_str())
            );

            // Echo of the same content must be suppressed (no broadcast).
            drv.stub
                .watch_tx
                .send(ClipboardChange::New(ClipboardItem::text(
                    &payload, [0; 8], 0,
                )))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(
                drv.broadcast_rx.try_recv().is_err(),
                "random echo must be suppressed"
            );
        }
    }

    #[tokio::test]
    async fn self_origin_remote_is_ignored() {
        let drv = spawn_driver(true, [0xAA; 8]);
        let item = ClipboardItem::text("mine", [0xAA; 8], 1);
        let frame = Message::Announce(item).encode(1024).unwrap();
        drv.inbound_tx
            .send(InboundFrame {
                origin: [0xBB; 8],
                payload: frame[4..].to_vec(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(drv.stub.set_count(), 0, "self-origin must not be applied");
    }

    #[tokio::test]
    async fn disabled_driver_ignores_remote_and_local() {
        let mut drv = spawn_driver(false, [0xAA; 8]);

        let remote = ClipboardItem::text("x", [0xBB; 8], 1);
        let frame = Message::Announce(remote).encode(1024).unwrap();
        drv.inbound_tx
            .send(InboundFrame {
                origin: [0xBB; 8],
                payload: frame[4..].to_vec(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(drv.stub.set_count(), 0, "disabled driver must not apply");

        // No watcher is started while disabled, so nothing is broadcast.
        assert!(
            drv.broadcast_rx.try_recv().is_err(),
            "disabled driver must not broadcast"
        );
    }

    #[tokio::test]
    async fn enable_then_disable_emits_both_events() {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(64);
        let (directed_tx, _directed_rx) = mpsc::channel(16);
        let mut clip = Clipboard::new(
            ClipboardOptions {
                enabled: false,
                backend: BackendKind::Dummy,
                max_item_size: DEFAULT_MAX_ITEM_SIZE,
                origin: [0xAA; 8],
                files: FileOptions::default(),
            },
            inbound_rx,
            broadcast_tx,
            directed_tx,
        );
        let _ = inbound_tx;

        clip.set_enabled(true);
        assert_eq!(clip.event().await, ClipboardEvent::Enabled);
        clip.set_enabled(false);
        assert_eq!(clip.event().await, ClipboardEvent::Disabled);
    }
}
