//! Simulated tests for the OS clipboard backends.
//!
//! `WlClipboardBackend` and `ClipHistBackend` shell out to external tools
//! (`wl-paste`, `wl-copy`, `cliphist`). Instead of relying on those tools
//! being installed, this file replaces `$PATH` with a directory of stub
//! executables that emulate the real tools' observable behavior against
//! plain files. That makes the backends' set/read logic unit-testable.
//!
//! The DBus backend is exercised under its own feature in-module (see
//! `src/backend/dbus_klipper.rs`), because `dbus`/`klipper` are not default
//! features.

mod common;

use common::{CLIPHIST_STUB, DBUS_SEND_STUB, Rng, StubEnv, WL_COPY_STUB, WL_PASTE_STUB, path_lock};
use lan_mouse_clipboard::backend::{
    BackendKind, ClipboardBackend, cliphist::ClipHistBackend, dummy::DummyBackend,
    wl_clipboard::WlClipboardBackend,
};
use lan_mouse_clipboard::item::ClipboardItem;

fn text_item(s: &str) -> ClipboardItem {
    ClipboardItem::text(s, [0x22; 8], 1)
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn wl_clipboard_set_then_read_roundtrips() {
    let _guard = path_lock();
    let _env = StubEnv::new(&[
        ("wl-paste", WL_PASTE_STUB),
        ("wl-copy", WL_COPY_STUB),
        ("dbus-send", DBUS_SEND_STUB),
    ]);
    let backend = WlClipboardBackend::new();

    // Empty clipboard -> None.
    assert!(backend.read_current().await.is_none());

    backend.set(&text_item("from wl-copy")).await.expect("set");
    let got = backend.read_current().await.expect("read back");
    assert_eq!(got.text_plain().as_deref(), Some("from wl-copy"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn wl_clipboard_set_without_text_rep_errors() {
    let _guard = path_lock();
    let _env = StubEnv::new(&[("wl-copy", WL_COPY_STUB)]);
    let backend = WlClipboardBackend::new();
    let item = ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![("image/png".into(), vec![1, 2, 3])],
    };
    assert!(backend.set(&item).await.is_err());
}

#[tokio::test]
async fn wl_clipboard_available_detects_both_tools() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("wl-paste", WL_PASTE_STUB), ("wl-copy", WL_COPY_STUB)]);
    assert!(WlClipboardBackend::available());
    assert!(BackendKind::WlClipboard.available());
    let _ = env;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn cliphist_set_writes_stdin_with_mime() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("cliphist", CLIPHIST_STUB)]);
    let backend = ClipHistBackend::new();

    let item = text_item("cliphist payload");
    backend.set(&item).await.expect("set");
    assert_eq!(env.read_cliphist(), b"cliphist payload");

    // cliphist is a sink: it never reads.
    assert!(backend.read_current().await.is_none());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn cliphist_set_empty_item_is_noop() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("cliphist", CLIPHIST_STUB)]);
    let backend = ClipHistBackend::new();
    let item = ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![],
    };
    backend.set(&item).await.expect("noop ok");
    assert!(env.read_cliphist().is_empty());
}

#[tokio::test]
async fn auto_backend_falls_back_to_dummy_when_no_tools() {
    let _guard = path_lock();
    // No stubs -> PATH contains nothing -> no external tool is available.
    let _env = StubEnv::new(&[]);
    let backends = lan_mouse_clipboard::backend::build_backends(BackendKind::Auto);
    assert!(!backends.is_empty());
    assert_eq!(backends[0].name(), DummyBackend::new().name());
}

#[test]
fn backend_kind_as_str_maps_names() {
    assert_eq!(BackendKind::Auto.as_str(), "auto");
    assert_eq!(BackendKind::Dummy.as_str(), "dummy");
}

#[test]
fn dummy_is_always_available() {
    let _guard = path_lock();
    let _env = StubEnv::new(&[]);
    assert!(BackendKind::Dummy.available());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn wl_clipboard_random_text_roundtrips() {
    let _guard = path_lock();
    let _env = StubEnv::new(&[("wl-paste", WL_PASTE_STUB), ("wl-copy", WL_COPY_STUB)]);
    let backend = WlClipboardBackend::new();
    let mut rng = Rng::new(0xCAFE);
    for _ in 0..50 {
        // Random-length, random-byte text payload (lossily valid UTF-8 so the
        // backend's text extraction succeeds).
        let len = 1 + rng.range(256);
        let s: String = (0..len)
            .map(|_| (rng.next() % 26 + 65) as u8 as char)
            .collect();
        backend.set(&text_item(&s)).await.expect("set");
        let got = backend.read_current().await.expect("read back");
        assert_eq!(got.text_plain().as_deref(), Some(s.as_str()));
    }
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn cliphist_random_bytes_are_stored() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("cliphist", CLIPHIST_STUB)]);
    let backend = ClipHistBackend::new();
    let mut rng = Rng::new(0xBEE2);
    for _ in 0..50 {
        let len = 1 + rng.range(256);
        let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 1,
            reps: vec![("application/octet-stream".into(), data.clone())],
        };
        backend.set(&item).await.expect("set");
        assert_eq!(env.read_cliphist(), data, "stored bytes must match");
    }
}
