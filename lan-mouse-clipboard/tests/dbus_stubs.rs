//! Simulated tests for the DBus (`klipper`) clipboard backend.
//!
//! Compiled only when the `dbus`/`klipper` feature is enabled. `dbus-send` is
//! replaced by a stub on `$PATH` that stores one string in a file, so the
//! backend's read/set/watch behavior is testable without a KDE session.

#![cfg(any(feature = "klipper", feature = "dbus"))]

mod common;

use std::time::Duration;

use common::{DBUS_SEND_STUB, StubEnv, path_lock};
use futures::StreamExt;
use lan_mouse_clipboard::backend::ClipboardBackend;
use lan_mouse_clipboard::backend::dbus_klipper::DbusClipboardBackend;
use lan_mouse_clipboard::item::{ClipboardChange, ClipboardItem};

fn text_of(change: ClipboardChange) -> Option<String> {
    match change {
        ClipboardChange::New(item) => item.text_plain(),
    }
}

fn image_primary_item() -> ClipboardItem {
    ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![
            ("image/png".into(), vec![0x89, 0x50, 0x4e, 0x47]),
            ("text/plain".into(), b"alt text".to_vec()),
        ],
    }
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn dbus_reads_and_writes_plain_text() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("dbus-send", DBUS_SEND_STUB)]);
    let backend = DbusClipboardBackend::new("klipper");

    env.write_dbus("from klipper");
    let read = backend.read_current().await.expect("read");
    assert_eq!(read.primary_mime(), Some("text/plain;charset=utf-8"));
    assert_eq!(read.text_plain().as_deref(), Some("from klipper"));

    backend
        .set(&ClipboardItem::text("to klipper", [0; 8], 0))
        .await
        .expect("set");
    assert_eq!(env.read_dbus(), b"to klipper");
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn dbus_set_skips_a_non_text_primary() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("dbus-send", DBUS_SEND_STUB)]);
    let backend = DbusClipboardBackend::new("dbus");

    backend.set(&image_primary_item()).await.expect("skip");
    assert!(
        env.read_dbus().is_empty(),
        "rich primary must not be written"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // PATH lock must span the whole test
async fn dbus_watch_emits_the_first_read_then_changes() {
    let _guard = path_lock();
    let env = StubEnv::new(&[("dbus-send", DBUS_SEND_STUB)]);
    let backend = DbusClipboardBackend::new("klipper");

    env.write_dbus("first");
    let mut stream = backend.watch().await;
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("a first change must be reported")
        .expect("stream item");
    assert_eq!(text_of(first).as_deref(), Some("first"));

    env.write_dbus("second");
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("a changed value must be reported")
        .expect("stream item");
    assert_eq!(text_of(second).as_deref(), Some("second"));
}
