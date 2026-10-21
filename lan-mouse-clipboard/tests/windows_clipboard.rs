#![cfg(windows)]
//! Interactive Windows clipboard tests.
//!
//! Set `LAN_MOUSE_WIN_CLIPBOARD_TESTS=1` and run these in a logged-in desktop
//! session. They skip on headless CI and in service sessions.

use std::io::Cursor;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use lan_mouse_clipboard::backend::ClipboardBackend;
use lan_mouse_clipboard::backend::windows::WindowsClipboardBackend;
use lan_mouse_clipboard::file::broker::Broker;
use lan_mouse_clipboard::file::manifest::{
    KIND_DIR, KIND_FILE, Manifest, ManifestEntry, ManifestLimits,
};
use lan_mouse_clipboard::file::windows::{RemoteFileSet, VirtualEntry};
use lan_mouse_clipboard::item::ClipboardItem;
use lan_mouse_clipboard::network::InboundFrame;
use lan_mouse_clipboard::protocol::{FILE_PROTOCOL_VERSION, FileData, FileOffer, Message};
use lan_mouse_clipboard::{BackendKind, Clipboard, ClipboardOptions, FileOptions};
use tokio::sync::mpsc;
use windows::Win32::Foundation::{HANDLE, POINT};
use windows::Win32::System::Com::{
    DVASPECT_CONTENT, FORMATETC, IStream, TYMED_HGLOBAL, TYMED_ISTREAM,
};
use windows::Win32::System::DataExchange::{
    CloseClipboard, EmptyClipboard, OpenClipboard, RegisterClipboardFormatW, SetClipboardData,
};
use windows::Win32::System::Memory::{
    GMEM_MOVEABLE, GlobalAlloc, GlobalLock, GlobalSize, GlobalUnlock,
};
use windows::Win32::System::Ole::{
    CF_HDROP, OleGetClipboard, OleInitialize, OleUninitialize, ReleaseStgMedium,
};
use windows::Win32::UI::Shell::DROPFILES;
use windows::core::BOOL;
use windows::core::w;

fn enabled() -> bool {
    std::env::var("LAN_MOUSE_WIN_CLIPBOARD_TESTS").as_deref() == Ok("1")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_roundtrip() {
    if !enabled() {
        return;
    }
    let backend = WindowsClipboardBackend::new(1024 * 1024);
    backend
        .set(&ClipboardItem::text("lan-mouse-windows-text", [1; 8], 1))
        .await
        .expect("set text");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let item = backend.read_current().await.expect("read text");
    assert_eq!(item.text_plain().as_deref(), Some("lan-mouse-windows-text"));
}

const HELLO: &[u8] = b"hello virtual";

type Directed = (
    mpsc::Sender<([u8; 8], Vec<u8>)>,
    mpsc::Receiver<([u8; 8], Vec<u8>)>,
);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn virtual_file_roundtrip() {
    if !enabled() {
        return;
    }
    let manifest = Manifest {
        entries: vec![
            ManifestEntry {
                id: 0,
                parent: None,
                kind: KIND_DIR,
                name: b"dir".to_vec(),
                size: 0,
                mode: 0o755,
                mtime_sec: 0,
                mtime_nsec: 0,
            },
            ManifestEntry {
                id: 1,
                parent: Some(0),
                kind: KIND_FILE,
                name: b"hello.txt".to_vec(),
                size: HELLO.len() as u64,
                mode: 0o644,
                mtime_sec: 0,
                mtime_nsec: 0,
            },
        ],
    };
    let entries = vec![
        VirtualEntry {
            id: 0,
            relative: r"dir".encode_utf16().collect(),
            is_dir: true,
            size: 0,
            mode: 0o755,
            mtime_sec: 0,
            mtime_nsec: 0,
        },
        VirtualEntry {
            id: 1,
            relative: r"dir\hello.txt".encode_utf16().collect(),
            is_dir: false,
            size: HELLO.len() as u64,
            mode: 0o644,
            mtime_sec: 0,
            mtime_nsec: 0,
        },
    ];

    let (directed_tx, mut directed_rx) = tokio::sync::mpsc::channel(8);
    let broker = Arc::new(Broker::new(directed_tx));
    let responder = broker.clone();
    let responder_task = tokio::spawn(async move {
        while let Some((origin, frame)) = directed_rx.recv().await {
            if let Ok(Message::FileRead(read)) = Message::decode(&frame[4..]) {
                let start = (read.offset as usize).min(HELLO.len());
                let end = (start + read.length as usize).min(HELLO.len());
                responder.complete(
                    origin,
                    FileData {
                        request_id: read.request_id,
                        offset: read.offset,
                        data: HELLO[start..end].to_vec(),
                    },
                );
            }
        }
    });

    let files = Arc::new(RemoteFileSet {
        origin: [7; 8],
        serial: 77,
        manifest,
        entries,
        broker,
        runtime: tokio::runtime::Handle::current(),
    });
    let backend = WindowsClipboardBackend::new(1024 * 1024);
    assert!(
        backend
            .set_file_offer(&files)
            .await
            .expect("set virtual files")
    );

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = tx.send(read_virtual_clipboard(1, 2));
    });
    let result = tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .expect("timed out reading OLE clipboard")
        .expect("OLE reader dropped");
    responder_task.abort();
    match result {
        Ok((name, data)) => {
            assert_eq!(name, r"dir\hello.txt");
            assert_eq!(data, HELLO);
        }
        Err(e) => panic!("OLE clipboard read failed: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_image_set_keeps_existing_clipboard() {
    if !enabled() {
        return;
    }
    let backend = WindowsClipboardBackend::new(1024 * 1024);
    backend
        .set(&ClipboardItem::text("keep me", [1; 8], 1))
        .await
        .expect("set text");
    let result = backend
        .set(&ClipboardItem {
            origin: [1; 8],
            serial: 2,
            reps: vec![("image/png".into(), b"not a png".to_vec())],
        })
        .await;
    assert!(result.is_err(), "malformed image must fail");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let item = backend.read_current().await.expect("read text");
    assert_eq!(item.text_plain().as_deref(), Some("keep me"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_file_drop_is_read() {
    if !enabled() {
        return;
    }
    let path = std::env::temp_dir().join("lan_mouse_drop_test.txt");
    std::fs::write(&path, b"drop").expect("write");
    set_clipboard_path(&path);
    let backend = WindowsClipboardBackend::new(1024 * 1024);
    let item = backend.read_current().await.expect("file drop item");
    let (mime, data) = item.primary().expect("rep");
    assert_eq!(mime, "text/uri-list");
    let text = String::from_utf8_lossy(data);
    assert!(text.contains("lan_mouse_drop_test.txt"), "{text}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_roundtrip() {
    if !enabled() {
        return;
    }
    let image = image::RgbaImage::from_pixel(2, 2, image::Rgba([255, 0, 0, 255]));
    let mut png = Vec::new();
    image::DynamicImage::ImageRgba8(image)
        .write_to(&mut Cursor::new(&mut png), image::ImageFormat::Png)
        .expect("encode png");
    let backend = WindowsClipboardBackend::new(1024 * 1024);
    backend
        .set(&ClipboardItem {
            origin: [1; 8],
            serial: 2,
            reps: vec![("image/png".into(), png)],
        })
        .await
        .expect("set image");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let item = backend.read_current().await.expect("read image");
    let (mime, data) = item.primary().expect("image rep");
    assert!(mime.starts_with("image/"), "{mime}");
    assert!(data.starts_with(b"\x89PNG\r\n\x1a\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_offer_installs_virtual_files() {
    if !enabled() {
        return;
    }
    let manifest = Manifest {
        entries: vec![ManifestEntry {
            id: 0,
            parent: None,
            kind: KIND_FILE,
            name: b"hello.txt".to_vec(),
            size: HELLO.len() as u64,
            mode: 0o644,
            mtime_sec: 0,
            mtime_nsec: 0,
        }],
    };
    let limits = ManifestLimits {
        max_entries: 100,
        max_bytes: 1024 * 1024,
    };
    let encoded = manifest.encode(limits).expect("encode manifest");
    let max_item_size = 1024 * 1024;
    let (inbound_tx, inbound_rx) = mpsc::channel(16);
    let (broadcast_tx, _broadcast_rx) = mpsc::channel(16);
    let (directed_tx, mut directed_rx): Directed = mpsc::channel(16);
    let response_inbound = inbound_tx.clone();
    tokio::spawn(async move {
        while let Some((origin, frame)) = directed_rx.recv().await {
            if let Ok(Message::FileRead(read)) = Message::decode(&frame[4..]) {
                let start = (read.offset as usize).min(HELLO.len());
                let end = (start + read.length as usize).min(HELLO.len());
                let response = Message::FileData(FileData {
                    request_id: read.request_id,
                    offset: read.offset,
                    data: HELLO[start..end].to_vec(),
                });
                if let Ok(frame) = response.encode(0) {
                    let _ = response_inbound
                        .send(InboundFrame {
                            origin,
                            payload: frame[4..].to_vec(),
                        })
                        .await;
                }
            }
        }
    });
    let _clipboard = Clipboard::new(
        ClipboardOptions {
            enabled: true,
            backend: BackendKind::Windows,
            max_item_size,
            origin: [1; 8],
            files: FileOptions {
                enabled: true,
                ..Default::default()
            },
        },
        inbound_rx,
        broadcast_tx,
        directed_tx,
    );
    let offer = Message::FileOffer(FileOffer {
        version: FILE_PROTOCOL_VERSION,
        origin: [9; 8],
        serial: 3,
        manifest: encoded,
    })
    .encode(max_item_size)
    .expect("encode offer");
    inbound_tx
        .send(InboundFrame {
            origin: [9; 8],
            payload: offer[4..].to_vec(),
        })
        .await
        .expect("send offer");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = tx.send(read_virtual_clipboard(0, 1));
    });
    let result = tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .expect("timed out reading driver offer")
        .expect("OLE reader dropped");
    match result {
        Ok((name, data)) => {
            assert_eq!(name, "hello.txt");
            assert_eq!(data, HELLO);
        }
        Err(e) => panic!("driver OLE clipboard read failed: {e}"),
    }
}

fn set_clipboard_path(path: &std::path::Path) {
    use std::os::windows::ffi::OsStrExt;
    let header = DROPFILES {
        pFiles: std::mem::size_of::<DROPFILES>() as u32,
        pt: POINT::default(),
        fNC: BOOL(0),
        fWide: BOOL(1),
    };
    let mut bytes = vec![0u8; std::mem::size_of::<DROPFILES>()];
    unsafe {
        ptr::copy_nonoverlapping(
            &header as *const _ as *const u8,
            bytes.as_mut_ptr(),
            std::mem::size_of::<DROPFILES>(),
        );
    }
    for unit in path.as_os_str().encode_wide() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    let memory = unsafe { GlobalAlloc(GMEM_MOVEABLE, bytes.len()).expect("global") };
    let locked = unsafe { GlobalLock(memory) };
    assert!(!locked.is_null());
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), locked as *mut u8, bytes.len());
        let _ = GlobalUnlock(memory);
        OpenClipboard(None).expect("open clipboard");
        EmptyClipboard().expect("empty clipboard");
        SetClipboardData(CF_HDROP.0 as u32, Some(HANDLE(memory.0))).expect("set drop");
        CloseClipboard().expect("close clipboard");
    }
}

fn read_virtual_clipboard(
    entry_index: i32,
    expected_count: u32,
) -> Result<(String, Vec<u8>), String> {
    unsafe { OleInitialize(None) }.map_err(|e| e.to_string())?;
    let result = (|| {
        let data = unsafe { OleGetClipboard() }.map_err(|e| e.to_string())?;
        let descriptor_format =
            unsafe { RegisterClipboardFormatW(w!("FileGroupDescriptorW")) } as u16;
        let contents_format = unsafe { RegisterClipboardFormatW(w!("FileContents")) } as u16;
        let descriptor = FORMATETC {
            cfFormat: descriptor_format,
            dwAspect: DVASPECT_CONTENT.0,
            lindex: -1,
            tymed: TYMED_HGLOBAL.0 as u32,
            ptd: ptr::null_mut(),
        };
        let mut medium = unsafe { data.GetData(&descriptor) }.map_err(|e| e.to_string())?;
        let name = unsafe {
            let memory = medium.u.hGlobal;
            let size = GlobalSize(memory);
            let locked = GlobalLock(memory);
            if locked.is_null() {
                return Err("cannot lock descriptor".into());
            }
            let bytes = std::slice::from_raw_parts(locked as *const u8, size).to_vec();
            let _ = GlobalUnlock(memory);
            let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            if count != expected_count {
                return Err(format!("unexpected descriptor count {count}"));
            }
            let descriptor_size = 592;
            let offset = 4 + entry_index as usize * descriptor_size + 72;
            let units: Vec<u16> = bytes[offset..offset + 260]
                .chunks(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                .take_while(|unit| *unit != 0)
                .collect();
            String::from_utf16(&units).map_err(|e| e.to_string())?
        };
        unsafe { ReleaseStgMedium(&mut medium) };

        let contents = FORMATETC {
            cfFormat: contents_format,
            dwAspect: DVASPECT_CONTENT.0,
            lindex: entry_index,
            tymed: TYMED_ISTREAM.0 as u32,
            ptd: ptr::null_mut(),
        };
        let mut medium = unsafe { data.GetData(&contents) }.map_err(|e| e.to_string())?;
        let stream: IStream = unsafe { (*medium.u.pstm).clone() }
            .ok_or_else(|| "FileContents returned no stream".to_string())?;
        let mut payload = Vec::new();
        loop {
            let mut buffer = [0u8; 64];
            let mut read = 0u32;
            let hr = unsafe {
                stream.Read(
                    buffer.as_mut_ptr() as *mut core::ffi::c_void,
                    buffer.len() as u32,
                    Some(&mut read),
                )
            };
            hr.ok().map_err(|e| e.to_string())?;
            if read == 0 {
                break;
            }
            payload.extend_from_slice(&buffer[..read as usize]);
        }
        unsafe { ReleaseStgMedium(&mut medium) };
        Ok((name, payload))
    })();
    unsafe { OleUninitialize() };
    result
}
