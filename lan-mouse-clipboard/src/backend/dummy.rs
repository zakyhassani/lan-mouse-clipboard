//! In-memory backend used for testing and as a safe fallback.

use std::pin::Pin;
use std::sync::{Mutex, mpsc};

use futures::future;
use futures::{FutureExt, Stream, stream};

use super::ClipboardBackend;
use crate::item::{ClipboardChange, ClipboardItem};

pub struct DummyBackend {
    current: Mutex<Option<ClipboardItem>>,
    tx: mpsc::Sender<ClipboardChange>,
    rx: Mutex<Option<mpsc::Receiver<ClipboardChange>>>,
}

impl DummyBackend {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            current: Mutex::new(None),
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    /// Simulate a local clipboard change for testing.
    pub fn push(&self, item: ClipboardItem) {
        *self
            .current
            .lock()
            .expect("lock")
            .get_or_insert(item.clone()) = item.clone();
        let _ = self.tx.send(ClipboardChange::New(item));
    }

    pub fn current(&self) -> Option<ClipboardItem> {
        self.current.lock().expect("lock").clone()
    }
}

impl Default for DummyBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ClipboardBackend for DummyBackend {
    fn name(&self) -> &'static str {
        "dummy"
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        self.current.lock().expect("lock").clone()
    }

    async fn set(&self, item: &ClipboardItem) -> Result<(), super::BackendError> {
        *self.current.lock().expect("lock") = Some(item.clone());
        Ok(())
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        // The watch receiver is consumed once.
        let rx = self.rx.lock().expect("lock").take();
        let stream = match rx {
            Some(rx) => {
                let s = stream::unfold(rx, |rx| {
                    // Non-blocking poll of the std mpsc receiver so the
                    // async runtime is never blocked.
                    match rx.try_recv() {
                        Ok(item) => future::ready(Some((item, rx))).boxed(),
                        Err(mpsc::TryRecvError::Empty) => future::pending().boxed(),
                        Err(_) => future::ready(None).boxed(),
                    }
                });
                Box::pin(s) as Pin<Box<dyn Stream<Item = ClipboardChange> + Send>>
            }
            None => Box::pin(futures::stream::empty()),
        };
        stream
    }
}
