//! DBus clipboard backend (source + sink) via `dbus-send`.
//!
//! Backs both the `klipper` (KDE) and `dbus` backend kinds, which expose the
//! same `org.kde.klipper` interface and differ only in their display name.
//! Best-effort: `dbus-send` output parsing is minimal and the backend
//! degrades gracefully (returns None / errors) when the service or tool is
//! unavailable.

use std::pin::Pin;
use std::time::Duration;

use futures::Stream;
use tokio::process::Command;
use tokio_stream::wrappers::ReceiverStream;

use super::{BackendError, ClipboardBackend, parse_dbus_string, which};
use crate::item::{ClipboardChange, ClipboardItem, MIME_TEXT_PLAIN, MIME_TEXT_PLAIN_ALT};

const SERVICE: &str = "org.kde.klipper";
const PATH: &str = "/klipper";
const IFACE: &str = "org.kde.klipper.klipper";

/// How often the clipboard is polled for changes.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// A DBus-backed clipboard, parameterized by its display name.
pub struct DbusClipboardBackend {
    name: &'static str,
}

impl DbusClipboardBackend {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }

    pub fn available() -> bool {
        which("dbus-send")
    }
}

/// Build a `dbus-send --session --print-reply` call to one klipper method.
fn dbus_send(method: &str) -> Command {
    let mut cmd = Command::new("dbus-send");
    cmd.arg("--session")
        .arg("--print-reply")
        .arg(format!("--dest={SERVICE}"))
        .arg(PATH)
        .arg(format!("{IFACE}.{method}"));
    cmd
}

/// Read the current clipboard text over DBus, if the service answers.
async fn read_dbus_text() -> Option<String> {
    let out = dbus_send("getClipboardContents").output().await.ok()?;
    if !out.status.success() {
        return None;
    }
    parse_dbus_string(&String::from_utf8_lossy(&out.stdout))
}

/// Wrap plain text as the item this backend produces.
fn text_item(text: String) -> ClipboardItem {
    ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![(MIME_TEXT_PLAIN.to_string(), text.into_bytes())],
    }
}

#[async_trait::async_trait]
impl ClipboardBackend for DbusClipboardBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        Some(text_item(read_dbus_text().await?))
    }

    async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError> {
        let Some((mime, data)) = item.primary() else {
            return Ok(());
        };
        // klipper carries plain text only and always reports it back as
        // `text/plain`. Applying any other primary rep would echo under a
        // different MIME and defeat echo suppression, so skip it rather than
        // falling back to a secondary text rep.
        let is_plain = mime.eq_ignore_ascii_case(MIME_TEXT_PLAIN)
            || mime.eq_ignore_ascii_case(MIME_TEXT_PLAIN_ALT);
        if !is_plain {
            log::debug!("klipper/dbus clipboard cannot carry {mime}; skipping");
            return Ok(());
        }
        let text = String::from_utf8_lossy(data).into_owned();
        let status = dbus_send("setClipboardContents")
            .arg(format!("string:{text}"))
            .status()
            .await?;
        if !status.success() {
            return Err(BackendError::Other(format!("dbus-send failed: {status}")));
        }
        Ok(())
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            // Last text reported to the driver; the first successful read
            // establishes the baseline and is emitted, later reads only when
            // the value changes (DBus has no change signal we rely on).
            let mut reported: Option<String> = None;
            loop {
                if let Some(text) = read_dbus_text().await {
                    if reported.as_deref() != Some(text.as_str()) {
                        reported = Some(text.clone());
                        if tx
                            .send(ClipboardChange::New(text_item(text)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }
}
