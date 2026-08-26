//! The clipboard sync subsystem.
//!
//! Owns the clipboard backend(s) and loop prevention, and drives the local
//! clipboard watcher. It runs as its own spawned task (mirroring the
//! capture/emulation subsystems). The separate [`network`] task owns the
//! TLS listener + connection pool and moves frames between peers and the
//! shared inbound/broadcast channels.

pub mod backend;
pub mod dedup;
pub mod item;
pub mod network;
pub mod protocol;
pub mod registry;
pub mod transport;

pub use backend::BackendKind;
pub use item::{ClipboardItem, DEFAULT_MAX_ITEM_SIZE};

use std::pin::Pin;

use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use crate::backend::{ClipboardBackend, build_backends};
use crate::dedup::LoopPrevention;
use crate::item::ClipboardChange;
use crate::protocol::Message;

/// Events emitted by the clipboard subsystem for frontends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClipboardEvent {
    /// Clipboard sync became enabled.
    Enabled,
    /// Clipboard sync became disabled.
    Disabled,
}

/// Public status mirroring `lan_mouse_ipc::Status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClipboardStatus {
    #[default]
    Disabled,
    Enabled,
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
}

impl Clipboard {
    /// Create the clipboard subsystem, spawning its driver task.
    ///
    /// `inbound_rx` receives frames from the network task; `broadcast_tx`
    /// carries broadcast frames to the network task.
    pub fn new(
        enabled: bool,
        backend: BackendKind,
        origin: [u8; 8],
        inbound_rx: mpsc::Receiver<Vec<u8>>,
        broadcast_tx: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(16);
        let inner = ClipboardInner {
            enabled,
            origin,
            max_item_size: DEFAULT_MAX_ITEM_SIZE,
            backends: build_backends(backend),
            loop_prevention: LoopPrevention::new(origin),
            watcher: None,
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
    mut inbound_rx: mpsc::Receiver<Vec<u8>>,
    broadcast_tx: mpsc::Sender<Vec<u8>>,
) {
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
                        ensure_watcher(&mut inner).await;
                    } else {
                        inner.watcher = None;
                    }
                }
                None => break,
            },
            change = poll_watcher(&mut inner.watcher) => {
                let Some(ClipboardChange::New(mut item)) = change else { continue; };
                if !inner.enabled { continue; }
                item.origin = inner.origin;
                item.serial = inner.loop_prevention.next_serial();
                if inner.loop_prevention.on_local_change(&item) {
                    match Message::Announce(item).encode(inner.max_item_size) {
                        Ok(frame) => {
                            if broadcast_tx.send(frame).await.is_err() { break; }
                        }
                        Err(e) => log::warn!("failed to encode clipboard item: {e}"),
                    }
                }
            }
            frame = inbound_rx.recv() => match frame {
                Some(payload) => {
                    if let Ok(Message::Announce(item)) = Message::decode(&payload) {
                        apply_remote(&mut inner, &item).await;
                    }
                }
                None => break,
            },
        }
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

    #[tokio::test]
    async fn set_enabled_emits_event() {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(64);
        let mut clip = Clipboard::new(
            false,
            BackendKind::Dummy,
            [0xAA; 8],
            inbound_rx,
            broadcast_tx,
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
        let _clip = Clipboard::new(
            true,
            BackendKind::Dummy,
            [0xAA; 8],
            inbound_rx,
            broadcast_tx,
        );

        let item = ClipboardItem::text("remote", [0xBB; 8], 1);
        let frame = Message::Announce(item).encode(1024).unwrap();
        let payload = frame[4..].to_vec();
        // Driver applies it without erroring; we just exercise the path.
        inbound_tx.send(payload).await.unwrap();
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
        inbound_tx: mpsc::Sender<Vec<u8>>,
        broadcast_rx: mpsc::Receiver<Vec<u8>>,
        _request_tx: mpsc::Sender<ClipboardRequest>,
        _event_rx: mpsc::Receiver<ClipboardEvent>,
        _task: tokio::task::JoinHandle<()>,
    }

    fn spawn_driver(enabled: bool, origin: [u8; 8]) -> Driver {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (broadcast_tx, broadcast_rx) = mpsc::channel(64);
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
        }
    }

    #[tokio::test]
    async fn remote_change_is_applied_and_echo_is_suppressed() {
        let mut drv = spawn_driver(true, [0xAA; 8]);

        let remote = ClipboardItem::text("hi", [0xBB; 8], 1);
        let frame = Message::Announce(remote).encode(1024).unwrap();
        drv.inbound_tx.send(frame[4..].to_vec()).await.unwrap();

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

    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    #[tokio::test]
    async fn random_remote_changes_are_applied_and_echoes_suppressed() {
        let mut rng = Rng::new(0x5000);
        for _ in 0..50 {
            let mut drv = spawn_driver(true, [0xAA; 8]);
            let payload = format!("random-{}", rng.next());
            let remote = ClipboardItem::text(&payload, [0xBB; 8], 1);
            let frame = Message::Announce(remote).encode(1024).unwrap();
            drv.inbound_tx.send(frame[4..].to_vec()).await.unwrap();

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
        drv.inbound_tx.send(frame[4..].to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(drv.stub.set_count(), 0, "self-origin must not be applied");
    }

    #[tokio::test]
    async fn disabled_driver_ignores_remote_and_local() {
        let mut drv = spawn_driver(false, [0xAA; 8]);

        let remote = ClipboardItem::text("x", [0xBB; 8], 1);
        let frame = Message::Announce(remote).encode(1024).unwrap();
        drv.inbound_tx.send(frame[4..].to_vec()).await.unwrap();
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
        let mut clip = Clipboard::new(
            false,
            BackendKind::Dummy,
            [0xAA; 8],
            inbound_rx,
            broadcast_tx,
        );
        let _ = inbound_tx;

        clip.set_enabled(true);
        assert_eq!(clip.event().await, ClipboardEvent::Enabled);
        clip.set_enabled(false);
        assert_eq!(clip.event().await, ClipboardEvent::Disabled);
    }
}
