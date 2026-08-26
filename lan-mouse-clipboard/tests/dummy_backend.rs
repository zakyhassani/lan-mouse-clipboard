//! Unit tests for the in-memory `DummyBackend`.
//!
//! The dummy backend is itself a test double for real OS clipboard
//! backends, so these tests pin down its observable contract: writes are
//! stored, reads reflect the latest write, and the watch stream surfaces
//! pushed changes exactly once.

use futures::StreamExt;
use lan_mouse_clipboard::backend::{BackendError, ClipboardBackend, dummy::DummyBackend};
use lan_mouse_clipboard::item::{ClipboardChange, ClipboardItem};

fn text_item(s: &str) -> ClipboardItem {
    ClipboardItem::text(s, [0x11; 8], 1)
}

#[test]
fn new_backend_starts_empty() {
    let b = DummyBackend::new();
    assert!(b.current().is_none());
}

#[tokio::test]
async fn read_current_returns_none_when_empty() {
    let b = DummyBackend::new();
    assert!(b.read_current().await.is_none());
}

#[tokio::test]
async fn set_stores_item_that_read_current_returns() {
    let b = DummyBackend::new();
    let item = text_item("hello");
    b.set(&item).await.expect("set should succeed");
    let got = b.read_current().await.expect("should read back");
    assert_eq!(got.text_plain().as_deref(), Some("hello"));
    assert_eq!(
        b.current().as_ref().and_then(|i| i.text_plain()).as_deref(),
        Some("hello")
    );
}

#[test]
fn push_records_latest_and_keeps_order() {
    let b = DummyBackend::new();
    b.push(text_item("first"));
    b.push(text_item("second"));
    assert_eq!(
        b.current().and_then(|i| i.text_plain()).as_deref(),
        Some("second")
    );
}

#[tokio::test]
async fn watch_yields_pushed_changes() {
    let b = DummyBackend::new();
    let mut stream = b.watch().await;
    b.push(text_item("one"));
    let first = stream.next().await;
    assert!(matches!(first, Some(ClipboardChange::New(_))));
}

#[tokio::test]
async fn watch_is_single_shot() {
    let b = DummyBackend::new();
    let mut first = b.watch().await;
    b.push(text_item("once"));
    assert!(matches!(first.next().await, Some(ClipboardChange::New(_))));
    // A second watch cannot re-consume the same receiver.
    let mut second = b.watch().await;
    b.push(text_item("twice"));
    assert!(second.next().await.is_none());
}

#[tokio::test]
async fn set_accepts_empty_item_without_error() {
    let b = DummyBackend::new();
    let item = ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![],
    };
    let res: Result<(), BackendError> = b.set(&item).await;
    assert!(res.is_ok());
}
