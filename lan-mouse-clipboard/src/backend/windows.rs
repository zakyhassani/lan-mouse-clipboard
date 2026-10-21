//! Native Windows clipboard backend.
//!
//! One dedicated thread owns all Win32 clipboard and OLE calls. It runs an OLE
//! STA message loop and is woken by `PostThreadMessageW`. File offers are
//! installed as OLE virtual files; local file copies are read from CF_HDROP.

mod image;
mod ole;

use std::path::PathBuf;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use windows::Win32::Foundation::{HANDLE, HGLOBAL, LPARAM, WPARAM};
use windows::Win32::System::DataExchange::{
    CloseClipboard, EmptyClipboard, GetClipboardData, GetClipboardSequenceNumber,
    IsClipboardFormatAvailable, OpenClipboard, SetClipboardData,
};
use windows::Win32::System::Memory::{GMEM_MOVEABLE, GlobalAlloc, GlobalLock, GlobalUnlock};
use windows::Win32::System::Ole::{
    CF_HDROP, CF_UNICODETEXT, OleInitialize, OleSetClipboard, OleUninitialize,
};
use windows::Win32::System::Threading::GetCurrentThreadId;
use windows::Win32::UI::Shell::{DragQueryFileW, HDROP};
use windows::Win32::UI::WindowsAndMessaging::{
    DispatchMessageW, GetMessageW, MSG, PM_NOREMOVE, PeekMessageW, PostQuitMessage,
    PostThreadMessageW, TranslateMessage, WM_APP, WM_USER,
};

use crate::backend::{BackendError, ClipboardBackend};
use crate::file::manifest::{is_file_mime, path_to_file_uri};
use crate::file::windows::RemoteFileSet;
use crate::item::{ClipboardChange, ClipboardItem, MIME_TEXT_PLAIN, is_image_mime, is_text_mime};

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const SET_TIMEOUT: Duration = Duration::from_secs(30);

enum SetPayload {
    Text(String),
    Image { mime: String, data: Vec<u8> },
}

enum PreparedPayload {
    Text(Vec<u16>),
    Image(Vec<(u32, Vec<u8>)>),
}

enum ReadOutcome {
    Item(ClipboardItem),
    Empty,
    Busy,
}

enum Command {
    SetItem {
        payload: SetPayload,
        reply: oneshot::Sender<Result<(), String>>,
    },
    SetVirtual {
        files: Arc<RemoteFileSet>,
        reply: oneshot::Sender<Result<bool, String>>,
    },
    ReadNow {
        reply: oneshot::Sender<Option<ClipboardItem>>,
    },
    Poll {
        reply: oneshot::Sender<Option<ClipboardItem>>,
    },
    Shutdown,
}

struct WorkerState {
    last_observed: u32,
    virtual_object: Option<windows::Win32::System::Com::IDataObject>,
    max_item_size: usize,
}

pub struct WindowsClipboardBackend {
    tx: std_mpsc::Sender<Command>,
    thread_id: u32,
    ole_available: Arc<AtomicBool>,
}

impl WindowsClipboardBackend {
    pub fn new(max_item_size: usize) -> Self {
        let (tx, rx) = std_mpsc::channel();
        let (init_tx, init_rx) = std_mpsc::channel();
        let ole_available = Arc::new(AtomicBool::new(false));
        let flag = ole_available.clone();
        std::thread::Builder::new()
            .name("lan-mouse-windows-clipboard".into())
            .spawn(move || worker(rx, init_tx, flag, max_item_size))
            .expect("failed to start the Windows clipboard worker");
        let (thread_id, ole_ok) = init_rx
            .recv()
            .expect("Windows clipboard worker failed to initialize");
        ole_available.store(ole_ok, Ordering::SeqCst);
        Self {
            tx,
            thread_id,
            ole_available,
        }
    }

    fn wake(&self) -> Result<(), String> {
        unsafe { PostThreadMessageW(self.thread_id, WM_APP, WPARAM(0), LPARAM(0)) }
            .map_err(|e| e.to_string())
    }

    async fn read_now(&self) -> Option<ClipboardItem> {
        let (reply, response) = oneshot::channel();
        if self.tx.send(Command::ReadNow { reply }).is_err() || self.wake().is_err() {
            return None;
        }
        tokio::time::timeout(READ_TIMEOUT, response)
            .await
            .ok()?
            .ok()?
    }
}

#[async_trait]
impl ClipboardBackend for WindowsClipboardBackend {
    fn name(&self) -> &'static str {
        "windows"
    }

    async fn read_current(&self) -> Option<ClipboardItem> {
        self.read_now().await
    }

    async fn set(&self, item: &ClipboardItem) -> Result<(), BackendError> {
        let payload = match set_payload(item) {
            Some(payload) => payload,
            None => {
                return Err(BackendError::Other(
                    "unsupported or unsafe Windows clipboard representation".into(),
                ));
            }
        };
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Command::SetItem { payload, reply })
            .map_err(|_| BackendError::Other("clipboard worker is gone".into()))?;
        self.wake()
            .map_err(|e| BackendError::Other(format!("cannot wake clipboard worker: {e}")))?;
        match tokio::time::timeout(SET_TIMEOUT, response).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(e))) => Err(BackendError::Other(e)),
            Ok(Err(_)) => Err(BackendError::Other("clipboard worker dropped reply".into())),
            Err(_) => Err(BackendError::Other("clipboard set timed out".into())),
        }
    }

    async fn watch(&self) -> Pin<Box<dyn Stream<Item = ClipboardChange> + Send>> {
        let (tx, rx) = tokio_mpsc::channel(16);
        let commands = self.tx.clone();
        let thread_id = self.thread_id;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(POLL_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let (reply, response) = oneshot::channel();
                if commands.send(Command::Poll { reply }).is_err() {
                    break;
                }
                if unsafe { PostThreadMessageW(thread_id, WM_APP, WPARAM(0), LPARAM(0)) }.is_err() {
                    break;
                }
                // A lost reply must not stop polling forever; the worker may be
                // busy rendering a virtual file for another consumer.
                match tokio::time::timeout(Duration::from_secs(5), response).await {
                    Ok(Ok(Some(item))) => {
                        if tx.send(ClipboardChange::New(item)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(_)) => break,
                    Err(_) => {
                        log::warn!("windows clipboard: poll timed out; retrying");
                    }
                }
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }

    async fn set_file_offer(&self, files: &Arc<RemoteFileSet>) -> Result<bool, BackendError> {
        if !self.ole_available.load(Ordering::SeqCst) {
            return Err(BackendError::Other(
                "OLE clipboard initialization failed".into(),
            ));
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Command::SetVirtual {
                files: files.clone(),
                reply,
            })
            .map_err(|_| BackendError::Other("clipboard worker is gone".into()))?;
        self.wake()
            .map_err(|e| BackendError::Other(format!("cannot wake clipboard worker: {e}")))?;
        match tokio::time::timeout(SET_TIMEOUT, response).await {
            Ok(Ok(Ok(installed))) => Ok(installed),
            Ok(Ok(Err(e))) => Err(BackendError::Other(e)),
            Ok(Err(_)) => Err(BackendError::Other("clipboard worker dropped reply".into())),
            Err(_) => Err(BackendError::Other("virtual file install timed out".into())),
        }
    }
}

impl Drop for WindowsClipboardBackend {
    fn drop(&mut self) {
        let _ = self.tx.send(Command::Shutdown);
        let _ = self.wake();
        // Do not join: a stream read may be waiting on the daemon runtime.
    }
}

fn set_payload(item: &ClipboardItem) -> Option<SetPayload> {
    let (mime, data) = item.primary()?;
    let mime = mime.to_ascii_lowercase();
    if is_file_mime(&mime) {
        // Remote file URI lists are never installed as local paths. File
        // offers use OLE virtual files instead.
        log::debug!("windows clipboard: rejecting a remote file URI list");
        return None;
    }
    if is_image_mime(&mime) {
        return Some(SetPayload::Image {
            mime,
            data: data.clone(),
        });
    }
    if is_text_mime(&mime) {
        if let Some(text) = item.text_plain() {
            return Some(SetPayload::Text(text));
        }
        if let Ok(text) = std::str::from_utf8(data) {
            return Some(SetPayload::Text(text.to_string()));
        }
        return None;
    }
    item.text_plain().map(SetPayload::Text)
}

fn worker(
    rx: std_mpsc::Receiver<Command>,
    init: std_mpsc::Sender<(u32, bool)>,
    ole_available: Arc<AtomicBool>,
    max_item_size: usize,
) {
    // CF_HDROP materializes offers under this directory; clear caches left by
    // a previous daemon run now that no Explorer copy can still be using them.
    let _ = std::fs::remove_dir_all(std::env::temp_dir().join("lan-mouse-clipboard"));
    let ole_ok = unsafe { OleInitialize(None) }.is_ok();
    ole_available.store(ole_ok, Ordering::SeqCst);
    let thread_id = unsafe { GetCurrentThreadId() };
    // Create the thread's message queue before the async side starts posting.
    let mut queue_message = MSG::default();
    unsafe {
        let _ = PeekMessageW(&mut queue_message, None, WM_USER, WM_USER, PM_NOREMOVE);
    }
    let _ = init.send((thread_id, ole_ok));

    let mut state = WorkerState {
        last_observed: unsafe { GetClipboardSequenceNumber() },
        virtual_object: None,
        max_item_size,
    };
    let mut message = MSG::default();
    loop {
        let result = unsafe { GetMessageW(&mut message, None, 0, 0) };
        if result.0 <= 0 {
            break;
        }
        if message.message == WM_APP {
            for command in rx.try_iter() {
                match command {
                    Command::SetItem { payload, reply } => {
                        let result = set_item(&payload, &mut state);
                        let _ = reply.send(result);
                    }
                    Command::SetVirtual { files, reply } => {
                        let result = set_virtual(&files, &mut state, ole_ok);
                        let _ = reply.send(result);
                    }
                    Command::ReadNow { reply } => {
                        let sequence = unsafe { GetClipboardSequenceNumber() };
                        let value = match read_current_item(state.max_item_size) {
                            ReadOutcome::Item(item) => {
                                state.last_observed = sequence;
                                Some(item)
                            }
                            ReadOutcome::Empty => {
                                state.last_observed = sequence;
                                None
                            }
                            ReadOutcome::Busy => None,
                        };
                        let _ = reply.send(value);
                    }
                    Command::Poll { reply } => {
                        let sequence = unsafe { GetClipboardSequenceNumber() };
                        if sequence == state.last_observed {
                            let _ = reply.send(None);
                        } else {
                            log::debug!(
                                "windows clipboard: observed sequence {sequence} (last {})",
                                state.last_observed
                            );
                            let value = match read_current_item(state.max_item_size) {
                                ReadOutcome::Item(item) => {
                                    state.last_observed = sequence;
                                    Some(item)
                                }
                                ReadOutcome::Empty => {
                                    state.last_observed = sequence;
                                    None
                                }
                                // The clipboard is held by another process.
                                // Retry the same sequence on the next poll.
                                ReadOutcome::Busy => None,
                            };
                            let _ = reply.send(value);
                        }
                    }
                    Command::Shutdown => {
                        unsafe { PostQuitMessage(0) };
                        break;
                    }
                }
            }
        }
        unsafe {
            let _ = TranslateMessage(&message);
            DispatchMessageW(&message);
        }
    }
    if ole_ok {
        unsafe { OleUninitialize() };
    }
}

fn open_clipboard() -> Result<(), String> {
    for _ in 0..10 {
        if unsafe { OpenClipboard(None) }.is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err("clipboard is busy".into())
}

fn set_item(payload: &SetPayload, state: &mut WorkerState) -> Result<(), String> {
    // Validate and build the payload before touching the clipboard. A failed
    // remote image must not clear the user's existing clipboard.
    let prepared = match prepare_payload(payload, state.max_item_size) {
        Ok(Some(prepared)) => prepared,
        Ok(None) => return Ok(()),
        Err(e) => return Err(e),
    };
    open_clipboard()?;
    let result = (|| {
        unsafe { EmptyClipboard() }.map_err(|e| e.to_string())?;
        state.virtual_object = None;
        match prepared {
            PreparedPayload::Text(utf16) => {
                set_hglobal(
                    CF_UNICODETEXT.0 as u32,
                    utf16.as_ptr() as *const u8,
                    utf16.len() * 2,
                )?;
            }
            PreparedPayload::Image(formats) => {
                let mut installed = 0usize;
                for (format, bytes) in formats {
                    match set_hglobal(format, bytes.as_ptr(), bytes.len()) {
                        Ok(()) => installed += 1,
                        Err(e) => log::warn!("windows clipboard: image format failed: {e}"),
                    }
                }
                if installed == 0 {
                    return Err("no image format could be placed on the clipboard".into());
                }
            }
        }
        Ok(())
    })();
    let _ = unsafe { CloseClipboard() };
    if result.is_ok() {
        state.last_observed = unsafe { GetClipboardSequenceNumber() };
    }
    result
}

fn prepare_payload(
    payload: &SetPayload,
    max_item_size: usize,
) -> Result<Option<PreparedPayload>, String> {
    match payload {
        SetPayload::Text(text) => {
            if text.is_empty() {
                return Ok(None);
            }
            Ok(Some(PreparedPayload::Text(
                text.encode_utf16().chain(std::iter::once(0)).collect(),
            )))
        }
        SetPayload::Image { mime, data } => Ok(Some(PreparedPayload::Image(
            image::build_image_formats(mime, data, max_item_size)?,
        ))),
    }
}

fn set_virtual(
    files: &Arc<RemoteFileSet>,
    state: &mut WorkerState,
    ole_ok: bool,
) -> Result<bool, String> {
    if !ole_ok {
        return Err("OLE clipboard is unavailable".into());
    }
    let object: windows::Win32::System::Com::IDataObject =
        ole::VirtualDataObject::new(files.clone()).into();
    unsafe { OleSetClipboard(Some(&object)) }.map_err(|e| e.to_string())?;
    state.virtual_object = Some(object);
    state.last_observed = unsafe { GetClipboardSequenceNumber() };
    Ok(true)
}

fn set_hglobal(format: u32, source: *const u8, len: usize) -> Result<(), String> {
    if len == 0 {
        return Err("empty clipboard data".into());
    }
    let memory = unsafe { GlobalAlloc(GMEM_MOVEABLE, len) }.map_err(|e| e.to_string())?;
    let locked = unsafe { GlobalLock(memory) };
    if locked.is_null() {
        unsafe {
            let _ = windows::Win32::Foundation::GlobalFree(Some(memory));
        }
        return Err("cannot lock clipboard memory".into());
    }
    unsafe {
        ptr::copy_nonoverlapping(source, locked as *mut u8, len);
        let _ = GlobalUnlock(memory);
        if let Err(e) = SetClipboardData(format, Some(HANDLE(memory.0))) {
            let _ = windows::Win32::Foundation::GlobalFree(Some(memory));
            return Err(e.to_string());
        }
    }
    Ok(())
}

fn read_current_item(max_item_size: usize) -> ReadOutcome {
    if open_clipboard().is_err() {
        return ReadOutcome::Busy;
    }
    let result = (|| {
        if let Some(item) = read_file_list(max_item_size) {
            return ReadOutcome::Item(item);
        }
        if let Some(item) = read_text(max_item_size) {
            return ReadOutcome::Item(item);
        }
        if let Some((mime, data)) = image::read_image(max_item_size) {
            return ReadOutcome::Item(ClipboardItem {
                origin: [0; 8],
                serial: 0,
                reps: vec![(mime, data)],
            });
        }
        ReadOutcome::Empty
    })();
    let _ = unsafe { CloseClipboard() };
    result
}

fn read_file_list(max_item_size: usize) -> Option<ClipboardItem> {
    if unsafe { IsClipboardFormatAvailable(CF_HDROP.0 as u32) }.is_err() {
        return None;
    }
    let handle = unsafe { GetClipboardData(CF_HDROP.0 as u32) }.ok()?;
    let drop = HDROP(handle.0);
    let count = unsafe { DragQueryFileW(drop, u32::MAX, None) };
    if count == 0 || count > 100_000 {
        return None;
    }
    let mut uris = String::new();
    for index in 0..count {
        let len = unsafe { DragQueryFileW(drop, index, None) } as usize;
        if len == 0 {
            continue;
        }
        let mut buffer = vec![0u16; len + 1];
        let copied = unsafe { DragQueryFileW(drop, index, Some(&mut buffer)) } as usize;
        if copied == 0 {
            continue;
        }
        let path = PathBuf::from(String::from_utf16_lossy(&buffer[..copied]));
        uris.push_str(&path_to_file_uri(&path));
        uris.push_str("\r\n");
    }
    if uris.is_empty() || uris.len() > max_item_size {
        return None;
    }
    Some(ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![("text/uri-list".into(), uris.into_bytes())],
    })
}

fn read_text(max_item_size: usize) -> Option<ClipboardItem> {
    if unsafe { IsClipboardFormatAvailable(CF_UNICODETEXT.0 as u32) }.is_err() {
        return None;
    }
    let handle = unsafe { GetClipboardData(CF_UNICODETEXT.0 as u32) }.ok()?;
    let memory = HGLOBAL(handle.0);
    let size = unsafe { windows::Win32::System::Memory::GlobalSize(memory) };
    if size < 2 || size > max_item_size.saturating_mul(4).max(4096) {
        return None;
    }
    let locked = unsafe { GlobalLock(memory) };
    if locked.is_null() {
        return None;
    }
    let units = unsafe { std::slice::from_raw_parts(locked as *const u16, size / 2) };
    let text = String::from_utf16_lossy(units.split(|unit| *unit == 0).next().unwrap_or(&[]));
    unsafe {
        let _ = GlobalUnlock(memory);
    }
    if text.is_empty() || text.len() > max_item_size {
        return None;
    }
    Some(ClipboardItem {
        origin: [0; 8],
        serial: 0,
        reps: vec![(MIME_TEXT_PLAIN.into(), text.into_bytes())],
    })
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    fn item(mime: &str, data: &[u8]) -> ClipboardItem {
        ClipboardItem {
            origin: [1; 8],
            serial: 1,
            reps: vec![(mime.into(), data.to_vec())],
        }
    }

    #[test]
    fn remote_file_lists_are_never_installed_as_local_paths() {
        assert!(set_payload(&item("text/uri-list", b"file:///C:/secret\r\n")).is_none());
        assert!(
            set_payload(&item("x-special/gnome-copied-files", b"copy\nfile:///C:/x")).is_none()
        );
    }

    #[test]
    fn text_and_image_payloads_are_selected() {
        match set_payload(&item("text/plain;charset=utf-8", b"hello")) {
            Some(SetPayload::Text(text)) => assert_eq!(text, "hello"),
            _ => panic!("expected text"),
        }
        match set_payload(&item("image/png", b"\x89PNG")) {
            Some(SetPayload::Image { mime, .. }) => assert_eq!(mime, "image/png"),
            _ => panic!("expected image"),
        }
    }

    #[test]
    fn text_prefers_a_plain_rep_when_html_is_primary() {
        let mut value = item("text/html", b"<b>hello</b>");
        value
            .reps
            .push(("text/plain;charset=utf-8".into(), b"hello".to_vec()));
        match set_payload(&value) {
            Some(SetPayload::Text(text)) => assert_eq!(text, "hello"),
            _ => panic!("expected text"),
        }
    }
}
