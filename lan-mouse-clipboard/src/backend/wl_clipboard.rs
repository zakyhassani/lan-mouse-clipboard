//! wl-clipboard backend (source + sink).
//!
//! Shells out to the standard `wl-paste` / `wl-copy` utilities. This also
//! covers desktop environments that delegate their clipboard to
//! wl-clipboard (e.g. Noctalia v5).
//!
//! A selection can offer many MIME types at once (a browser image offers
//! `image/png` alongside `text/html` and `text/plain`, for example). The
//! backend reads every transferable type the selection advertises, in the
//! order `wl-paste --list-types` reports them, and stores them as ordered
//! representations. On write it advertises the *primary* (first)
//! representation, because `wl-copy` can only set one MIME type per
//! selection.

use std::collections::HashSet;
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio_stream::wrappers::{LinesStream, ReceiverStream};

use super::{BackendError, ClipboardBackend, which};
use crate::item::{
    ClipboardChange, ClipboardItem, DEFAULT_MAX_ITEM_SIZE, X11_TEXT_TARGETS, rep_size,
};

/// Per-tool timeout. A misbehaving selection target must not hang the
/// clipboard driver.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

/// Upper bound on representations read from a single selection. Guards
/// against pathological selections that advertise hundreds of targets.
const MAX_REPS: usize = 64;

/// How many `wl-paste --type` reads run concurrently. Keeps the per-change
/// process fan-out from serializing (a browser image can advertise dozens of
/// targets) while bounding how many large buffers are in flight.
const READ_CONCURRENCY: usize = 4;

pub struct WlClipboardBackend {
    /// Maximum total item size read from the selection.
    max_item_size: usize,
}

impl WlClipboardBackend {
    pub fn new(max_item_size: usize) -> Self {
        Self { max_item_size }
    }

    pub fn available() -> bool {
        which("wl-paste") && which("wl-copy")
    }
}

impl Default for WlClipboardBackend {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ITEM_SIZE)
    }
}

/// Whether a selection target is a transferable data type.
///
/// MIME types (they contain `/`) and the legacy X11 text targets are data.
/// Everything else (`TARGETS`, `MULTIPLE`, `TIMESTAMP`, `SAVE_TARGETS`,
/// `OWNER_OS`, ...) is a clipboard protocol target: reading it would yield a
/// control payload, and some of them block instead of returning data.
pub(crate) fn is_transferable_type(t: &str) -> bool {
    t.contains('/') || X11_TEXT_TARGETS.iter().any(|x| x.eq_ignore_ascii_case(t))
}

/// Parse `wl-paste --list-types` output into an ordered, de-duplicated list of
/// transferable types.
///
/// MIME types win over the legacy X11 text aliases: the aliases are dropped
/// when at least one MIME type is offered (they duplicate the same content),
/// and used only as a fallback for selections that expose no MIME type.
pub(crate) fn select_types(raw: &str) -> Vec<String> {
    let mut mimes: Vec<String> = Vec::new();
    let mut x11: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for line in raw.lines() {
        let t = line.trim();
        if t.is_empty() || !is_transferable_type(t) {
            continue;
        }
        if !seen.insert(t.to_ascii_lowercase()) {
            continue;
        }
        if t.contains('/') {
            mimes.push(t.to_string());
        } else {
            x11.push(t.to_string());
        }
    }
    if mimes.is_empty() { x11 } else { mimes }
}

/// Read one target's bytes without any newline normalization.
///
/// The read is bounded: at most `max_bytes` are accepted and the child is
/// killed as soon as the budget is exceeded, so a selection cannot make the
/// daemon allocate an unbounded buffer before the item cap is applied.
async fn read_rep(mime: &str, max_bytes: usize) -> Option<Vec<u8>> {
    if max_bytes == 0 {
        return None;
    }
    let mut cmd = Command::new("wl-paste");
    cmd.arg("--type")
        .arg(mime)
        .arg("--no-newline")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = cmd.spawn().ok()?;
    let stdout = child.stdout.take()?;

    let mut buf: Vec<u8> = Vec::new();
    // Read one byte past the budget so an over-size rep is detected without
    // buffering the whole payload.
    let read = async {
        let mut limited = stdout.take(max_bytes as u64 + 1);
        limited.read_to_end(&mut buf).await
    };
    match tokio::time::timeout(READ_TIMEOUT, read).await {
        Ok(Ok(_)) => {}
        _ => {
            let _ = child.kill().await;
            return None;
        }
    }
    if buf.len() > max_bytes {
        let _ = child.kill().await;
        return None;
    }
    match tokio::time::timeout(READ_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) if status.success() => Some(buf),
        _ => {
            let _ = child.kill().await;
            None
        }
    }
}

#[async_trait::async_trait]
impl ClipboardBackend for WlClipboardBackend {
    fn name(&self) -> &'static str {
        "wl-clipboard"
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        let mut list = Command::new("wl-paste");
        list.arg("--list-types").kill_on_drop(true);
        let listed = tokio::time::timeout(READ_TIMEOUT, list.output())
            .await
            .ok()?
            .ok()?;
        if !listed.status.success() {
            return None;
        }
        let types = select_types(&String::from_utf8_lossy(&listed.stdout));
        if types.is_empty() {
            return None;
        }

        // Read the offered types with bounded concurrency. `buffered` keeps
        // the offered order, and each read is individually capped, so a huge
        // rep cannot exhaust memory and breaking out cancels the remaining
        // reads (killing their children).
        let max = self.max_item_size;
        let reads = stream::iter(types.into_iter().map(|mime| async move {
            let data = read_rep(&mime, max).await;
            (mime, data)
        }))
        .buffered(READ_CONCURRENCY);
        futures::pin_mut!(reads);

        let mut reps: Vec<(String, Vec<u8>)> = Vec::new();
        let mut total = 0usize;
        while let Some((mime, data)) = reads.next().await {
            if reps.len() >= MAX_REPS {
                break;
            }
            let Some(data) = data else {
                continue;
            };
            if data.is_empty() {
                continue;
            }
            let size = rep_size(&mime, &data);
            if total + size > max {
                if reps.is_empty() {
                    log::warn!(
                        "clipboard type {mime} is {size} bytes, over the {max} byte cap; skipping item"
                    );
                    return None;
                }
                log::debug!(
                    "clipboard type {mime} would exceed the {max} byte cap; dropping remaining reps"
                );
                break;
            }
            total += size;
            reps.push((mime, data));
        }

        if reps.is_empty() {
            return None;
        }
        Some(ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps,
        })
    }

    async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError> {
        let Some((mime, data)) = item.primary() else {
            return Err(BackendError::Other("item has no representations".into()));
        };
        let mut child = Command::new("wl-copy")
            .arg("--type")
            .arg(mime)
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
        let max_item_size = self.max_item_size;
        tokio::spawn(async move {
            let backend = WlClipboardBackend::new(max_item_size);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transferable_types_filter_protocol_targets() {
        assert!(is_transferable_type("image/png"));
        assert!(is_transferable_type("text/plain;charset=utf-8"));
        assert!(is_transferable_type("UTF8_STRING"));
        assert!(!is_transferable_type("TARGETS"));
        assert!(!is_transferable_type("MULTIPLE"));
        assert!(!is_transferable_type("TIMESTAMP"));
        assert!(!is_transferable_type("SAVE_TARGETS"));
        assert!(!is_transferable_type("OWNER_OS"));
        assert!(!is_transferable_type("COMPOUND_TEXT"));
    }

    #[test]
    fn transferable_types_accept_legacy_targets_case_insensitively() {
        assert!(is_transferable_type("utf8_string"));
        assert!(is_transferable_type("String"));
        assert!(is_transferable_type("text"));
        assert!(!is_transferable_type("targets"));
    }

    #[test]
    fn select_types_keeps_offered_order_and_dedupes() {
        let raw = "text/plain;charset=utf-8\ntext/plain\nTARGETS\nMULTIPLE\n\
                   text/plain;charset=utf-8\nTEXT\nSTRING\nUTF8_STRING\n";
        // MIME types present -> X11 aliases dropped; duplicates collapse.
        assert_eq!(
            select_types(raw),
            vec![
                "text/plain;charset=utf-8".to_string(),
                "text/plain".to_string()
            ]
        );
    }

    #[test]
    fn select_types_preserves_sender_order_for_images() {
        let raw = "text/html\nimage/png\nimage/bmp\ntext/plain\n";
        assert_eq!(
            select_types(raw),
            vec![
                "text/html".to_string(),
                "image/png".to_string(),
                "image/bmp".to_string(),
                "text/plain".to_string()
            ]
        );
    }

    #[test]
    fn select_types_falls_back_to_x11_text_aliases() {
        let raw = "TARGETS\nUTF8_STRING\nSTRING\nTEXT\n";
        assert_eq!(
            select_types(raw),
            vec![
                "UTF8_STRING".to_string(),
                "STRING".to_string(),
                "TEXT".to_string()
            ]
        );
    }

    #[test]
    fn select_types_ignores_empty_and_protocol_only() {
        assert!(select_types("").is_empty());
        assert!(select_types("TARGETS\nMULTIPLE\nTIMESTAMP\n").is_empty());
    }
}
