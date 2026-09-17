//! cliphist backend (history sink only).
//!
//! cliphist is a clipboard history manager, not the live clipboard. It is
//! fed by piping raw bytes into `cliphist store`. Received items are
//! recorded into history; cliphist is never used as a change source.

use std::pin::Pin;

use futures::Stream;
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
        let Some((mime, data)) = item.primary() else {
            return Ok(());
        };
        // cliphist stores by MIME type; the legacy X11 text aliases are not
        // MIME types, so normalize them to plain text (and skip anything else
        // that is not a MIME type).
        let mime = if mime.contains('/') {
            mime.clone()
        } else if crate::item::is_text_mime(mime) {
            crate::item::MIME_TEXT_PLAIN.to_string()
        } else {
            log::debug!("cliphist: skipping non-MIME representation {mime}");
            return Ok(());
        };
        let mut cmd = Command::new("cliphist");
        cmd.arg("store").arg("--mime").arg(&mime);
        // cliphist is a best-effort history sink: a non-zero exit is not
        // surfaced to the driver.
        let _ = super::pipe_stdin(cmd, data).await?;
        Ok(())
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        Box::pin(futures::stream::empty())
    }
}
