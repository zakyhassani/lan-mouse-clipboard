//! cliphist backend (history sink only).
//!
//! cliphist is a clipboard history manager, not the live clipboard. It is
//! fed by piping raw bytes into `cliphist store`. Received items are
//! recorded into history; cliphist is never used as a change source.

use std::pin::Pin;
use std::process::Stdio;

use futures::Stream;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::{BackendError, ClipboardBackend, which};
use crate::item::{ClipboardChange, ClipboardItem};

pub struct ClipHistBackend;

impl ClipHistBackend {
    pub fn new() -> Self {
        Self
    }

    pub fn available() -> bool {
        which("cliphist")
    }
}

impl Default for ClipHistBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ClipboardBackend for ClipHistBackend {
    fn name(&self) -> &'static str {
        "cliphist"
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        None
    }

    async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError> {
        let Some((mime, data)) = item.reps.first() else {
            return Ok(());
        };
        let mut child = Command::new("cliphist")
            .arg("store")
            .arg("--mime")
            .arg(mime)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(data).await?;
            stdin.flush().await?;
        }
        let _ = child.wait().await?;
        Ok(())
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        Box::pin(futures::stream::empty())
    }
}
