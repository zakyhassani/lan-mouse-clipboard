//! wl-clipboard backend (source + sink).
//!
//! Shells out to the standard `wl-paste` / `wl-copy` utilities. This also
//! covers desktop environments that delegate their clipboard to
//! wl-clipboard (e.g. Noctalia v5). Only plain text is handled in v1.

use std::pin::Pin;
use std::process::Stdio;

use futures::{Stream, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio_stream::wrappers::{LinesStream, ReceiverStream};

use super::{BackendError, ClipboardBackend, which};
use crate::item::{ClipboardChange, ClipboardItem, MIME_TEXT_PLAIN};

pub struct WlClipboardBackend;

impl WlClipboardBackend {
    pub fn new() -> Self {
        Self
    }

    pub fn available() -> bool {
        which("wl-paste") && which("wl-copy")
    }
}

impl Default for WlClipboardBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ClipboardBackend for WlClipboardBackend {
    fn name(&self) -> &'static str {
        "wl-clipboard"
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        let out = Command::new("wl-paste")
            .arg("--no-newline")
            .arg("--type")
            .arg(MIME_TEXT_PLAIN)
            .output()
            .await
            .ok()?;
        if !out.status.success() || out.stdout.is_empty() {
            return None;
        }
        Some(ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![(MIME_TEXT_PLAIN.to_string(), out.stdout)],
        })
    }

    async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError> {
        let Some((_mime, data)) = item
            .reps
            .iter()
            .find(|(m, _)| m.to_ascii_lowercase().starts_with("text/plain"))
        else {
            return Err(BackendError::Other("no text/plain representation".into()));
        };
        let mut child = Command::new("wl-copy")
            .arg("--type")
            .arg(MIME_TEXT_PLAIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(data).await?;
            stdin.flush().await?;
        }
        let status = child.wait().await?;
        if !status.success() {
            return Err(BackendError::Other(format!("wl-copy failed: {status}")));
        }
        Ok(())
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let backend = WlClipboardBackend;
        tokio::spawn(async move {
            loop {
                let mut child = match Command::new("wl-paste")
                    .arg("--watch")
                    .arg("sh")
                    .arg("-c")
                    .arg("echo change")
                    .stdout(Stdio::piped())
                    .spawn()
                {
                    Ok(c) => c,
                    Err(e) => {
                        log::warn!("wl-paste --watch failed: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let Some(stdout) = child.stdout.take() else {
                    break;
                };
                let reader = tokio::io::BufReader::new(stdout);
                let mut lines = LinesStream::new(reader.lines());
                while let Some(line) = lines.next().await {
                    if line.is_err() {
                        break;
                    }
                    // Clipboard changed; read the current content.
                    if let Some(item) = backend.read_current().await {
                        if tx.send(ClipboardChange::New(item)).await.is_err() {
                            return;
                        }
                    }
                }
                // Watcher process exited; restart after a short delay.
                log::debug!("wl-paste --watch exited, restarting");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }
}
