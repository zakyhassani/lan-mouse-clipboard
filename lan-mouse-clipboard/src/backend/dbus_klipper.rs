//! DBus clipboard backend (source + sink) via `dbus-send`.
//!
//! Backs both the `klipper` (KDE) and `dbus` backend kinds, which expose the
//! same `org.kde.klipper` interface and differ only in their display name.
//! Best-effort: `dbus-send` output parsing is minimal and the backend
//! degrades gracefully (returns None / errors) when the service or tool is
//! unavailable.

use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use futures::Stream;
use tokio::process::Command;
use tokio_stream::wrappers::ReceiverStream;

use super::{BackendError, ClipboardBackend, parse_dbus_string, which};
use crate::item::{ClipboardChange, ClipboardItem, MIME_TEXT_PLAIN, MIME_TEXT_PLAIN_ALT};

const SERVICE: &str = "org.kde.klipper";
const PATH: &str = "/klipper";
const IFACE: &str = "org.kde.klipper.klipper";

/// A shared DBus-backed clipboard, parameterized by its display name.
pub struct DbusClipboardBackend {
    name: &'static str,
    current: Mutex<Option<String>>,
}

impl DbusClipboardBackend {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            current: Mutex::new(None),
        }
    }

    pub fn available() -> bool {
        which("dbus-send")
    }
}

#[async_trait::async_trait]
impl ClipboardBackend for DbusClipboardBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        let args = vec![
            "--session".to_string(),
            "--print-reply".to_string(),
            format!("--dest={SERVICE}"),
            PATH.to_string(),
            format!("{IFACE}.getClipboardContents"),
        ];
        let out = Command::new("dbus-send").args(&args).output().await.ok()?;
        if !out.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        let text = parse_dbus_string(&stdout)?;
        *self.current.lock().expect("lock") = Some(text.clone());
        Some(ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![(MIME_TEXT_PLAIN.to_string(), text.into_bytes())],
        })
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
        let args = vec![
            "--session".to_string(),
            "--print-reply".to_string(),
            format!("--dest={SERVICE}"),
            PATH.to_string(),
            format!("{IFACE}.setClipboardContents"),
            format!("string:{text}"),
        ];
        let status = Command::new("dbus-send").args(&args).status().await?;
        if !status.success() {
            return Err(BackendError::Other(format!("dbus-send failed: {status}")));
        }
        Ok(())
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let backend = DbusClipboardBackend::new(self.name);
        tokio::spawn(async move {
            loop {
                if let Some(item) = backend.read_current().await {
                    let same = backend.current.lock().expect("lock").as_ref()
                        == item.text_plain().as_ref();
                    if !same {
                        *backend.current.lock().expect("lock") = item.text_plain();
                        if tx.send(ClipboardChange::New(item)).await.is_err() {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }
}
