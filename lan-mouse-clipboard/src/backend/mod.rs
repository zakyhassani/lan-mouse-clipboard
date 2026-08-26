//! Clipboard backend abstraction and selection.
//!
//! A backend is either a *source* (reads and watches the live clipboard)
//! or a *sink* (only writes, e.g. recording into a history store). The
//! orchestrator treats the first backend as the source and calls `set`
//! on every backend (source + history sinks).

use std::pin::Pin;

use futures::Stream;

use crate::item::{ClipboardChange, ClipboardItem};

pub mod dummy;

#[cfg(feature = "wl-clipboard")]
pub mod wl_clipboard;

#[cfg(feature = "cliphist")]
pub mod cliphist;

#[cfg(any(feature = "klipper", feature = "dbus"))]
pub mod dbus_klipper;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend `{0}` is not available on this system: {1}")]
    Unavailable(String, String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("operation failed: {0}")]
    Other(String),
}

/// A pluggable clipboard backend.
#[async_trait::async_trait]
pub trait ClipboardBackend: Send + Sync {
    /// Human-readable backend name (for logs and config display).
    fn name(&self) -> &'static str;

    /// Read the current live clipboard item, if any.
    async fn read_current(&self) -> Option<ClipboardItem>;

    /// Write an item to the live clipboard and/or record into history.
    async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError>;

    /// A stream of local clipboard changes. Only sources produce events;
    /// pure sinks return an empty/fused stream.
    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>>;
}

/// Which backend to use for the primary source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    /// Detect the best available backend automatically.
    Auto,
    #[cfg(feature = "wl-clipboard")]
    WlClipboard,
    #[cfg(feature = "cliphist")]
    ClipHist,
    #[cfg(feature = "klipper")]
    Klipper,
    #[cfg(feature = "dbus")]
    Dbus,
    /// In-memory backend, used for testing and as a safe fallback.
    Dummy,
}

impl BackendKind {
    /// True when this backend is a DBus-based integration (klipper / generic
    /// DBus). Used to keep a single clipboard manager running at a time: when
    /// a Wayland or Noctalia clipboard is detected, these are disabled.
    pub fn is_dbus(&self) -> bool {
        #[cfg(feature = "klipper")]
        if *self == BackendKind::Klipper {
            return true;
        }
        #[cfg(feature = "dbus")]
        if *self == BackendKind::Dbus {
            return true;
        }
        false
    }

    /// A short name usable in logs/config (snake/kebab case).
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendKind::Auto => "auto",
            #[cfg(feature = "wl-clipboard")]
            BackendKind::WlClipboard => "wl-clipboard",
            #[cfg(feature = "cliphist")]
            BackendKind::ClipHist => "cliphist",
            #[cfg(feature = "klipper")]
            BackendKind::Klipper => "klipper",
            #[cfg(feature = "dbus")]
            BackendKind::Dbus => "dbus",
            BackendKind::Dummy => "dummy",
        }
    }

    /// Check whether the tool/binary for this backend exists on `$PATH`.
    pub fn available(&self) -> bool {
        match self {
            BackendKind::Auto => Self::candidates()
                .into_iter()
                .any(|b| b != BackendKind::Auto && b.available()),
            #[cfg(feature = "wl-clipboard")]
            BackendKind::WlClipboard => which("wl-paste") && which("wl-copy"),
            #[cfg(feature = "cliphist")]
            BackendKind::ClipHist => which("cliphist"),
            #[cfg(feature = "klipper")]
            BackendKind::Klipper => which("dbus-send"),
            #[cfg(feature = "dbus")]
            BackendKind::Dbus => which("dbus-send"),
            BackendKind::Dummy => true,
        }
    }

    /// Candidate primary sources in priority order.
    #[allow(clippy::vec_init_then_push)]
    pub fn candidates() -> Vec<BackendKind> {
        let mut out = Vec::new();
        #[cfg(feature = "wl-clipboard")]
        out.push(BackendKind::WlClipboard);
        #[cfg(feature = "klipper")]
        out.push(BackendKind::Klipper);
        #[cfg(feature = "dbus")]
        out.push(BackendKind::Dbus);
        out.push(BackendKind::Dummy);
        out
    }
}

/// Resolve a configured `BackendKind` into an ordered list of backends:
/// the primary (source + sink) first, followed by any attached history sinks.
pub fn build_backends(kind: BackendKind) -> Vec<Box<dyn ClipboardBackend>> {
    // Only a single clipboard integration may run at a time. If a Wayland or
    // Noctalia clipboard manager is already running, the DBus (klipper)
    // integration must not also run or the two would fight over the clipboard.
    let wayland_active = wayland_clipboard_running();
    let mut out: Vec<Box<dyn ClipboardBackend>> = Vec::new();

    let primary = match kind {
        BackendKind::Auto => {
            let chosen = BackendKind::candidates()
                .into_iter()
                .filter(|b| {
                    *b != BackendKind::Auto && b.available() && !(wayland_active && b.is_dbus())
                })
                .find(|_| true);
            match chosen {
                Some(b) => b,
                None => BackendKind::Dummy,
            }
        }
        other => other,
    };

    push_primary(&mut out, primary, wayland_active);

    // Attach a cliphist history sink if available and not already present.
    #[cfg(feature = "cliphist")]
    {
        let has_cliphist = out.iter().any(|b| b.name() == "cliphist");
        if !has_cliphist && cliphist::ClipHistBackend::available() {
            out.push(Box::new(cliphist::ClipHistBackend::new()));
        }
    }

    if out.is_empty() {
        out.push(Box::new(dummy::DummyBackend::new()));
    }
    out
}

/// Push the primary backend for the resolved `primary` kind into `out`.
#[cfg_attr(
    not(any(feature = "klipper", feature = "dbus")),
    allow(unused_variables)
)]
fn push_primary(
    out: &mut Vec<Box<dyn ClipboardBackend>>,
    primary: BackendKind,
    wayland_active: bool,
) {
    match primary {
        BackendKind::Auto => unreachable!("resolved above"),
        BackendKind::Dummy => out.push(Box::new(dummy::DummyBackend::new())),
        #[cfg(feature = "wl-clipboard")]
        BackendKind::WlClipboard => out.push(Box::new(wl_clipboard::WlClipboardBackend::new())),
        #[cfg(feature = "cliphist")]
        BackendKind::ClipHist => {
            // cliphist is a sink; if chosen as primary there is no source,
            // so fall back to dummy for the source and still record history.
            out.push(Box::new(dummy::DummyBackend::new()));
            out.push(Box::new(cliphist::ClipHistBackend::new()));
        }
        #[cfg(feature = "klipper")]
        BackendKind::Klipper => {
            if wayland_active {
                push_wayland_or_dummy(out, "klipper");
            } else {
                out.push(Box::new(dbus_klipper::DbusClipboardBackend::new("klipper")));
            }
        }
        #[cfg(feature = "dbus")]
        BackendKind::Dbus => {
            if wayland_active {
                push_wayland_or_dummy(out, "dbus");
            } else {
                out.push(Box::new(dbus_klipper::DbusClipboardBackend::new("dbus")));
            }
        }
    }
}

/// Fallback used when a DBus integration is requested but a Wayland/Noctalia
/// clipboard is running: use the wl-clipboard backend when available, else a
/// dummy, so exactly one integration owns the clipboard.
#[cfg(any(feature = "klipper", feature = "dbus"))]
fn push_wayland_or_dummy(out: &mut Vec<Box<dyn ClipboardBackend>>, dbus_name: &str) {
    log::warn!(
        "wayland/noctalia clipboard is running; disabling {dbus_name} DBus integration to keep a single clipboard manager"
    );
    #[cfg(feature = "wl-clipboard")]
    {
        if wl_clipboard::WlClipboardBackend::available() {
            out.push(Box::new(wl_clipboard::WlClipboardBackend::new()));
            return;
        }
    }
    out.push(Box::new(dummy::DummyBackend::new()));
}

/// Whether an executable is available on `$PATH`.
fn which(name: &str) -> bool {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path).any(|dir| dir.join(name).is_file())
}

/// Process names that indicate an active Wayland or Noctalia clipboard
/// manager. When one of these is running it owns the clipboard, so our own
/// DBus (klipper) integration must not also run.
const WAYLAND_CLIPBOARD_PROCS: &[&str] = &[
    "wl-paste",
    "wl-copy",
    "wl-clipboard-manager",
    "clipboard-manager",
    "clipman",
    "cliphist",
    "cliphist-watch",
    "copyq",
    "noctalia",
    "noctalia-clipboard",
];

/// Detect a running Wayland or Noctalia clipboard manager by scanning `/proc`.
///
/// The process name is compared against `WAYLAND_CLIPBOARD_PROCS` using both
/// the truncated `comm` (15 char) name and the first command-line argument.
/// Returns `false` (never errors) if `/proc` is not available.
pub fn wayland_clipboard_running() -> bool {
    fn comm_matches(p: &std::path::Path) -> bool {
        if let Ok(c) = std::fs::read_to_string(p.join("comm")) {
            let c = c.trim();
            if WAYLAND_CLIPBOARD_PROCS.contains(&c) {
                return true;
            }
        }
        // Some managers have a distinct argv[0] even when `comm` is truncated.
        if let Ok(raw) = std::fs::read(p.join("cmdline")) {
            if let Some(first) = raw.split(|&b| b == 0).next() {
                if let Ok(s) = std::str::from_utf8(first) {
                    if let Some(base) = std::path::Path::new(s).file_name().and_then(|f| f.to_str())
                    {
                        return WAYLAND_CLIPBOARD_PROCS.contains(&base);
                    }
                }
            }
        }
        false
    }

    let Ok(rd) = std::fs::read_dir("/proc") else {
        return false;
    };
    for entry in rd.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if comm_matches(&entry.path()) {
            return true;
        }
    }
    false
}

/// Loosely extract `string "..."` from `dbus-send --print-reply` output.
#[cfg(any(feature = "klipper", feature = "dbus"))]
pub(crate) fn parse_dbus_string(output: &str) -> Option<String> {
    for line in output.lines() {
        let line = line.trim();
        if let Some(idx) = line.find("string \"") {
            let rest = &line[idx + "string \"".len()..];
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    /// Serializes PATH-mutating tests in this module (tests in the same
    /// binary run in parallel).
    static PATH_LOCK: Mutex<()> = Mutex::new(());

    /// Create a directory of do-nothing stub executables and return it.
    fn stub_dir(names: &[&str]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "lan_mouse_backend_stub_{}_{}",
            std::process::id(),
            names.join("_")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for n in names {
            let p = dir.join(n);
            std::fs::write(&p, "#!/bin/sh\nexit 0\n").unwrap();
            let mut perms = std::fs::metadata(&p).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&p, perms).unwrap();
        }
        dir
    }

    fn run_with_path<T>(stubs: &[&str], f: impl FnOnce() -> T) -> T {
        let _guard = PATH_LOCK.lock().unwrap();
        let dir = stub_dir(stubs);
        let old = std::env::var_os("PATH");
        std::env::set_var("PATH", &dir);
        let out = f();
        match old {
            Some(p) => std::env::set_var("PATH", p),
            None => std::env::remove_var("PATH"),
        }
        let _ = std::fs::remove_dir_all(&dir);
        out
    }

    #[test]
    #[cfg(any(feature = "klipper", feature = "dbus"))]
    fn parse_dbus_string_extracts_quoted_string() {
        assert_eq!(
            parse_dbus_string("   string \"hello\"\n"),
            Some("hello".into())
        );
        assert_eq!(
            parse_dbus_string("method return time=1\n   string \"two words\"\n"),
            Some("two words".into())
        );
        assert_eq!(parse_dbus_string("   string \"\"\n"), Some(String::new()));
        assert_eq!(parse_dbus_string("   int32 42\n"), None);
        assert_eq!(parse_dbus_string("no quotes here\n"), None);
    }

    #[test]
    fn as_str_maps_default_kinds() {
        assert_eq!(BackendKind::Auto.as_str(), "auto");
        assert_eq!(BackendKind::Dummy.as_str(), "dummy");
        #[cfg(feature = "wl-clipboard")]
        assert_eq!(BackendKind::WlClipboard.as_str(), "wl-clipboard");
        #[cfg(feature = "cliphist")]
        assert_eq!(BackendKind::ClipHist.as_str(), "cliphist");
    }

    #[test]
    fn candidates_contain_dummy_but_not_auto() {
        let c = BackendKind::candidates();
        assert!(c.contains(&BackendKind::Dummy));
        assert!(!c.contains(&BackendKind::Auto));
        assert!(!c.is_empty());
    }

    #[test]
    fn which_and_available_follow_stub_path() {
        run_with_path(&["wl-paste", "wl-copy"], || {
            assert!(BackendKind::WlClipboard.available());
        });
        run_with_path(&[], || {
            assert!(!BackendKind::WlClipboard.available());
            // Dummy is always available regardless of PATH.
            assert!(BackendKind::Dummy.available());
        });
    }

    #[test]
    fn build_backends_dummy_yields_dummy_primary() {
        let b = build_backends(BackendKind::Dummy);
        assert!(!b.is_empty());
        assert_eq!(b[0].name(), "dummy");
    }

    #[test]
    fn build_backends_auto_falls_back_to_dummy_without_tools() {
        run_with_path(&[], || {
            let b = build_backends(BackendKind::Auto);
            assert!(!b.is_empty());
            assert_eq!(b[0].name(), "dummy");
        });
    }

    #[test]
    fn is_dbus_marks_dbus_kinds_only() {
        #[cfg(feature = "klipper")]
        assert!(BackendKind::Klipper.is_dbus());
        #[cfg(feature = "dbus")]
        assert!(BackendKind::Dbus.is_dbus());
        #[cfg(feature = "wl-clipboard")]
        assert!(!BackendKind::WlClipboard.is_dbus());
        assert!(!BackendKind::Dummy.is_dbus());
    }

    #[test]
    #[cfg(feature = "klipper")]
    fn push_primary_uses_wayland_when_dbus_disabled_and_tools_present() {
        run_with_path(&["wl-paste", "wl-copy"], || {
            let mut out: Vec<Box<dyn ClipboardBackend>> = Vec::new();
            push_primary(&mut out, BackendKind::Klipper, true);
            assert_eq!(out.len(), 1);
            // DBus disabled: single integration becomes wl-clipboard.
            assert_eq!(out[0].name(), "wl-clipboard");
        });
    }

    #[test]
    #[cfg(feature = "klipper")]
    fn push_primary_dbus_disabled_falls_back_to_dummy_without_tools() {
        run_with_path(&[], || {
            let mut out: Vec<Box<dyn ClipboardBackend>> = Vec::new();
            push_primary(&mut out, BackendKind::Klipper, true);
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].name(), "dummy");
        });
    }

    #[test]
    #[cfg(feature = "klipper")]
    fn push_primary_keeps_dbus_when_wayland_not_running() {
        let mut out: Vec<Box<dyn ClipboardBackend>> = Vec::new();
        push_primary(&mut out, BackendKind::Klipper, false);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "klipper");
    }

    #[test]
    fn wayland_clipboard_running_returns_a_bool() {
        // Just ensures the /proc scan never panics and yields a decision.
        let _: bool = wayland_clipboard_running();
    }
}
