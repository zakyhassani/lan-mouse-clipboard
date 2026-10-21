//! OLE virtual-file clipboard object for received file offers.

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::{Arc, Mutex};

use windows::Win32::Foundation::{
    DATA_S_SAMEFORMATETC, DV_E_FORMATETC, DV_E_LINDEX, E_FAIL, E_INVALIDARG, E_NOTIMPL, E_POINTER,
    FILETIME, HGLOBAL, OLE_E_ADVISENOTSUPPORTED, S_FALSE,
};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY,
};
use windows::Win32::System::Com::{
    DATADIR_GET, DVASPECT_CONTENT, FORMATETC, IAdviseSink, IDataObject, IDataObject_Impl,
    IEnumFORMATETC, IEnumSTATDATA, ISequentialStream_Impl, IStream, IStream_Impl, LOCKTYPE,
    STATFLAG, STATSTG, STGC, STGM_READ, STGMEDIUM, STGMEDIUM_0, STGTY_STREAM, STREAM_SEEK,
    TYMED_HGLOBAL, TYMED_ISTREAM,
};
use windows::Win32::System::DataExchange::RegisterClipboardFormatW;
use windows::Win32::System::Memory::{GMEM_MOVEABLE, GlobalAlloc, GlobalLock, GlobalUnlock};
use windows::Win32::UI::Shell::{
    FD_ATTRIBUTES, FD_FILESIZE, FD_UNICODE, FD_WRITESTIME, FILEDESCRIPTORW, SHCreateStdEnumFmtEtc,
};
use windows::core::{Error, GUID, HRESULT, Ref, implement, w};

use crate::file::manifest::READ_CHUNK_SIZE;
use crate::file::windows::{RemoteFileSet, VirtualEntry};

/// `FILEDESCRIPTORW` size at the fixed Windows ABI.
const DESCRIPTOR_SIZE: usize = std::mem::size_of::<FILEDESCRIPTORW>();

#[implement(IDataObject)]
pub struct VirtualDataObject {
    files: Arc<RemoteFileSet>,
    descriptor_format: u16,
    contents_format: u16,
}

impl VirtualDataObject {
    pub fn new(files: Arc<RemoteFileSet>) -> Self {
        let descriptor_format =
            unsafe { RegisterClipboardFormatW(w!("FileGroupDescriptorW")) } as u16;
        let contents_format = unsafe { RegisterClipboardFormatW(w!("FileContents")) } as u16;
        Self {
            files,
            descriptor_format,
            contents_format,
        }
    }

    fn descriptor_bytes(&self) -> Vec<u8> {
        let count = self.files.entries.len();
        let size = 4 + count * DESCRIPTOR_SIZE;
        let mut bytes = vec![0u8; size];
        bytes[..4].copy_from_slice(&(count as u32).to_le_bytes());
        for (index, entry) in self.files.entries.iter().enumerate() {
            let offset = 4 + index * DESCRIPTOR_SIZE;
            write_descriptor(&mut bytes[offset..offset + DESCRIPTOR_SIZE], entry);
        }
        bytes
    }

    fn entry_stream(&self, index: i32) -> Result<IStream, Error> {
        if index < 0 {
            return Err(Error::from_hresult(DV_E_LINDEX));
        }
        let entry = self
            .files
            .entries
            .get(index as usize)
            .ok_or_else(|| Error::from_hresult(DV_E_LINDEX))?;
        if entry.is_dir {
            return Err(Error::from_hresult(DV_E_LINDEX));
        }
        let stream: IStream = VirtualStream::new(
            self.files.clone(),
            entry.id,
            entry.size,
            entry.mtime_sec,
            entry.mtime_nsec,
            0,
        )
        .into();
        Ok(stream)
    }
}

fn write_descriptor(target: &mut [u8], entry: &VirtualEntry) {
    let mut flags = FD_ATTRIBUTES.0 as u32 | FD_WRITESTIME.0 as u32 | FD_UNICODE.0 as u32;
    if !entry.is_dir {
        flags |= FD_FILESIZE.0 as u32;
    }
    target[0..4].copy_from_slice(&flags.to_le_bytes());
    // clsid, sizel and pointl remain zero.
    let attributes = if entry.is_dir {
        FILE_ATTRIBUTE_DIRECTORY.0
    } else if entry.mode & 0o222 == 0 {
        FILE_ATTRIBUTE_READONLY.0
    } else {
        FILE_ATTRIBUTE_NORMAL.0
    };
    target[36..40].copy_from_slice(&attributes.to_le_bytes());
    let stamp = unix_to_filetime(entry.mtime_sec, entry.mtime_nsec);
    let stamp = ((stamp.dwHighDateTime as u64) << 32) | stamp.dwLowDateTime as u64;
    for offset in [40usize, 48, 56] {
        target[offset..offset + 8].copy_from_slice(&stamp.to_le_bytes());
    }
    target[64..68].copy_from_slice(&((entry.size >> 32) as u32).to_le_bytes());
    target[68..72].copy_from_slice(&(entry.size as u32).to_le_bytes());
    for (index, unit) in entry.relative.iter().take(259).enumerate() {
        let offset = 72 + index * 2;
        target[offset..offset + 2].copy_from_slice(&unit.to_le_bytes());
    }
}

fn unix_to_filetime(sec: i64, nsec: u32) -> FILETIME {
    let ticks = (sec as i128 + 11_644_473_600) * 10_000_000 + (nsec / 100) as i128;
    let ticks = ticks.max(0) as u64;
    FILETIME {
        dwLowDateTime: ticks as u32,
        dwHighDateTime: (ticks >> 32) as u32,
    }
}

fn alloc_hglobal(bytes: &[u8]) -> Result<HGLOBAL, Error> {
    if bytes.is_empty() {
        return Err(Error::from_hresult(E_INVALIDARG));
    }
    let memory = unsafe { GlobalAlloc(GMEM_MOVEABLE, bytes.len())? };
    let locked = unsafe { GlobalLock(memory) };
    if locked.is_null() {
        unsafe {
            let _ = windows::Win32::Foundation::GlobalFree(Some(memory));
        }
        return Err(Error::from_win32());
    }
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), locked as *mut u8, bytes.len());
        let _ = GlobalUnlock(memory);
    }
    Ok(memory)
}

impl IDataObject_Impl for VirtualDataObject_Impl {
    fn GetData(&self, format: *const FORMATETC) -> windows::core::Result<STGMEDIUM> {
        if format.is_null() {
            return Err(Error::from_hresult(DV_E_FORMATETC));
        }
        let format = unsafe { &*format };
        if format.cfFormat == self.descriptor_format {
            if format.tymed & TYMED_HGLOBAL.0 as u32 == 0 {
                return Err(Error::from_hresult(DV_E_FORMATETC));
            }
            let memory = alloc_hglobal(&self.descriptor_bytes())?;
            return Ok(STGMEDIUM {
                tymed: TYMED_HGLOBAL.0 as u32,
                u: STGMEDIUM_0 { hGlobal: memory },
                pUnkForRelease: ManuallyDrop::new(None),
            });
        }
        if format.cfFormat == self.contents_format {
            if format.tymed & TYMED_ISTREAM.0 as u32 == 0 {
                return Err(Error::from_hresult(DV_E_FORMATETC));
            }
            let stream = self.entry_stream(format.lindex)?;
            return Ok(STGMEDIUM {
                tymed: TYMED_ISTREAM.0 as u32,
                u: STGMEDIUM_0 {
                    pstm: ManuallyDrop::new(Some(stream)),
                },
                pUnkForRelease: ManuallyDrop::new(None),
            });
        }
        Err(Error::from_hresult(DV_E_FORMATETC))
    }

    fn GetDataHere(
        &self,
        _format: *const FORMATETC,
        _medium: *mut STGMEDIUM,
    ) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn QueryGetData(&self, format: *const FORMATETC) -> HRESULT {
        if format.is_null() {
            return DV_E_FORMATETC;
        }
        let format = unsafe { &*format };
        if format.cfFormat == self.descriptor_format && format.tymed & TYMED_HGLOBAL.0 as u32 != 0 {
            return HRESULT(0);
        }
        if format.cfFormat == self.contents_format && format.tymed & TYMED_ISTREAM.0 as u32 != 0 {
            if format.lindex < 0 {
                return HRESULT(0);
            }
            if let Some(entry) = self.files.entries.get(format.lindex as usize) {
                if !entry.is_dir {
                    return HRESULT(0);
                }
            }
            return DV_E_LINDEX;
        }
        DV_E_FORMATETC
    }

    fn GetCanonicalFormatEtc(&self, input: *const FORMATETC, output: *mut FORMATETC) -> HRESULT {
        if input.is_null() || output.is_null() {
            return E_POINTER;
        }
        let input = unsafe { &*input };
        let value = FORMATETC {
            cfFormat: input.cfFormat,
            ptd: ptr::null_mut(),
            dwAspect: input.dwAspect,
            lindex: -1,
            tymed: input.tymed,
        };
        unsafe { *output = value };
        DATA_S_SAMEFORMATETC
    }

    fn SetData(
        &self,
        _format: *const FORMATETC,
        _medium: *const STGMEDIUM,
        _release: windows::core::BOOL,
    ) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn EnumFormatEtc(&self, direction: u32) -> windows::core::Result<IEnumFORMATETC> {
        if direction != DATADIR_GET.0 as u32 {
            return Err(Error::from_hresult(E_NOTIMPL));
        }
        let descriptor = FORMATETC {
            cfFormat: self.descriptor_format,
            dwAspect: DVASPECT_CONTENT.0,
            lindex: -1,
            tymed: TYMED_HGLOBAL.0 as u32,
            ..Default::default()
        };
        let contents = FORMATETC {
            cfFormat: self.contents_format,
            dwAspect: DVASPECT_CONTENT.0,
            lindex: -1,
            tymed: TYMED_ISTREAM.0 as u32,
            ..Default::default()
        };
        unsafe { SHCreateStdEnumFmtEtc(&[descriptor, contents]) }
    }

    fn DAdvise(
        &self,
        _format: *const FORMATETC,
        _flags: u32,
        _sink: Ref<'_, IAdviseSink>,
    ) -> windows::core::Result<u32> {
        Err(Error::from_hresult(OLE_E_ADVISENOTSUPPORTED))
    }

    fn DUnadvise(&self, _connection: u32) -> windows::core::Result<()> {
        Err(Error::from_hresult(OLE_E_ADVISENOTSUPPORTED))
    }

    fn EnumDAdvise(&self) -> windows::core::Result<IEnumSTATDATA> {
        Err(Error::from_hresult(OLE_E_ADVISENOTSUPPORTED))
    }
}

struct StreamState {
    position: u64,
    chunk_start: u64,
    chunk: Arc<Vec<u8>>,
}

#[implement(IStream)]
struct VirtualStream {
    files: Arc<RemoteFileSet>,
    entry_id: u32,
    size: u64,
    mtime_sec: i64,
    mtime_nsec: u32,
    state: Mutex<StreamState>,
    fetch: Mutex<()>,
}

impl VirtualStream {
    fn new(
        files: Arc<RemoteFileSet>,
        entry_id: u32,
        size: u64,
        mtime_sec: i64,
        mtime_nsec: u32,
        position: u64,
    ) -> Self {
        Self {
            files,
            entry_id,
            size,
            mtime_sec,
            mtime_nsec,
            state: Mutex::new(StreamState {
                position,
                chunk_start: u64::MAX,
                chunk: Arc::new(Vec::new()),
            }),
            fetch: Mutex::new(()),
        }
    }

    fn read_into(&self, target: &mut [u8]) -> Result<usize, HRESULT> {
        if target.is_empty() {
            return Ok(0);
        }
        let chunk_size = READ_CHUNK_SIZE as u64;
        loop {
            let (chunk_start, cached) = {
                let state = self.state.lock().map_err(|_| E_FAIL)?;
                if state.position >= self.size {
                    return Ok(0);
                }
                let chunk_start = state.position / chunk_size * chunk_size;
                let cached = if state.chunk_start == chunk_start {
                    Some(state.chunk.clone())
                } else {
                    None
                };
                (chunk_start, cached)
            };
            let data = match cached {
                Some(data) => data,
                None => {
                    // Serialize chunk fetches, but do not hold the stream state
                    // lock while waiting for the network.
                    let _fetch = self.fetch.lock().map_err(|_| E_FAIL)?;
                    let cached = {
                        let state = self.state.lock().map_err(|_| E_FAIL)?;
                        if state.chunk_start == chunk_start {
                            Some(state.chunk.clone())
                        } else {
                            None
                        }
                    };
                    if let Some(data) = cached {
                        data
                    } else {
                        let want = (self.size - chunk_start).min(chunk_size) as u32;
                        let data = self
                            .files
                            .runtime
                            .block_on(self.files.broker.read(
                                self.files.origin,
                                self.files.serial,
                                self.entry_id,
                                chunk_start,
                                want,
                            ))
                            .map_err(|e| {
                                log::debug!(
                                    "windows virtual stream read {}:{} at {chunk_start} failed: {e}",
                                    self.files.serial,
                                    self.entry_id
                                );
                                E_FAIL
                            })?;
                        let data = Arc::new(data);
                        let mut state = self.state.lock().map_err(|_| E_FAIL)?;
                        state.chunk = data.clone();
                        state.chunk_start = chunk_start;
                        data
                    }
                }
            };
            let mut state = self.state.lock().map_err(|_| E_FAIL)?;
            if state.position / chunk_size * chunk_size != chunk_start {
                // Another reader advanced the shared position; refetch for the
                // new chunk instead of returning bytes for the wrong offset.
                continue;
            }
            let offset = (state.position - chunk_start) as usize;
            if offset >= data.len() {
                return Ok(0);
            }
            let available = data.len() - offset;
            let count = available.min(target.len());
            target[..count].copy_from_slice(&data[offset..offset + count]);
            state.position += count as u64;
            return Ok(count);
        }
    }
}

impl ISequentialStream_Impl for VirtualStream_Impl {
    fn Read(&self, buffer: *mut core::ffi::c_void, length: u32, read: *mut u32) -> HRESULT {
        if !read.is_null() {
            unsafe { *read = 0 };
        }
        if buffer.is_null() {
            return if length == 0 { HRESULT(0) } else { E_POINTER };
        }
        let target = unsafe { std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize) };
        match self.read_into(target) {
            Ok(count) => {
                if !read.is_null() {
                    unsafe { *read = count as u32 };
                }
                if count as u32 == length {
                    HRESULT(0)
                } else {
                    S_FALSE
                }
            }
            Err(error) => error,
        }
    }

    fn Write(&self, _buffer: *const core::ffi::c_void, _length: u32, written: *mut u32) -> HRESULT {
        if !written.is_null() {
            unsafe { *written = 0 };
        }
        E_NOTIMPL
    }
}

impl IStream_Impl for VirtualStream_Impl {
    fn Seek(
        &self,
        move_by: i64,
        origin: STREAM_SEEK,
        new_position: *mut u64,
    ) -> windows::core::Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::from_hresult(E_FAIL))?;
        let base = match origin.0 {
            0 => 0i64,
            1 => state.position as i64,
            2 => self.size as i64,
            _ => return Err(Error::from_hresult(E_INVALIDARG)),
        };
        let Some(position) = base.checked_add(move_by).filter(|p| *p >= 0) else {
            return Err(Error::from_hresult(E_INVALIDARG));
        };
        state.position = position as u64;
        if !new_position.is_null() {
            unsafe { *new_position = position as u64 };
        }
        Ok(())
    }

    fn SetSize(&self, _size: u64) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn CopyTo(
        &self,
        target: Ref<'_, IStream>,
        count: u64,
        read: *mut u64,
        written: *mut u64,
    ) -> windows::core::Result<()> {
        let target = target.ok()?;
        let mut remaining = count;
        let mut total_read = 0u64;
        let mut total_written = 0u64;
        let mut buffer = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let want = remaining.min(buffer.len() as u64) as usize;
            let got = self
                .read_into(&mut buffer[..want])
                .map_err(Error::from_hresult)?;
            if got == 0 {
                break;
            }
            let mut wrote = 0u32;
            let hr = unsafe { target.Write(buffer.as_ptr() as _, got as u32, Some(&mut wrote)) };
            hr.ok()?;
            total_read += got as u64;
            total_written += wrote as u64;
            remaining -= got as u64;
            if wrote == 0 {
                break;
            }
        }
        if !read.is_null() {
            unsafe { *read = total_read };
        }
        if !written.is_null() {
            unsafe { *written = total_written };
        }
        Ok(())
    }

    fn Commit(&self, _flags: &STGC) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn Revert(&self) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn LockRegion(&self, _offset: u64, _count: u64, _kind: &LOCKTYPE) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn UnlockRegion(&self, _offset: u64, _count: u64, _kind: u32) -> windows::core::Result<()> {
        Err(Error::from_hresult(E_NOTIMPL))
    }

    fn Stat(&self, stat: *mut STATSTG, _flags: &STATFLAG) -> windows::core::Result<()> {
        if stat.is_null() {
            return Err(Error::from_hresult(E_POINTER));
        }
        let value = STATSTG {
            pwcsName: windows::core::PWSTR::null(),
            r#type: STGTY_STREAM.0 as u32,
            cbSize: self.size,
            mtime: unix_to_filetime(self.mtime_sec, self.mtime_nsec),
            ctime: unix_to_filetime(self.mtime_sec, self.mtime_nsec),
            atime: unix_to_filetime(self.mtime_sec, self.mtime_nsec),
            grfMode: STGM_READ,
            grfLocksSupported: 0,
            clsid: GUID::zeroed(),
            grfStateBits: 0,
            reserved: 0,
        };
        unsafe { *stat = value };
        Ok(())
    }

    fn Clone(&self) -> windows::core::Result<IStream> {
        let position = self
            .state
            .lock()
            .map_err(|_| Error::from_hresult(E_FAIL))?
            .position;
        let stream: IStream = VirtualStream::new(
            self.files.clone(),
            self.entry_id,
            self.size,
            self.mtime_sec,
            self.mtime_nsec,
            position,
        )
        .into();
        Ok(stream)
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use crate::file::broker::Broker;
    use crate::file::manifest::{KIND_DIR, KIND_FILE, Manifest, ManifestEntry};
    use crate::file::windows::{RemoteFileSet, VirtualEntry};
    use crate::protocol::{FileData, Message};

    fn files() -> Arc<RemoteFileSet> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
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
                    name: b"file.txt".to_vec(),
                    size: 3,
                    mode: 0o444,
                    mtime_sec: 1,
                    mtime_nsec: 0,
                },
            ],
        };
        Arc::new(RemoteFileSet {
            origin: [1; 8],
            serial: 1,
            manifest,
            entries: vec![
                VirtualEntry {
                    id: 0,
                    relative: "dir".encode_utf16().collect(),
                    is_dir: true,
                    size: 0,
                    mode: 0o755,
                    mtime_sec: 0,
                    mtime_nsec: 0,
                },
                VirtualEntry {
                    id: 1,
                    relative: "dir\\file.txt".encode_utf16().collect(),
                    is_dir: false,
                    size: 3,
                    mode: 0o444,
                    mtime_sec: 1,
                    mtime_nsec: 0,
                },
            ],
            broker: Arc::new(Broker::new(tx)),
            runtime: tokio::runtime::Handle::current(),
        })
    }

    const PAYLOAD: &[u8] = b"virtual stream";

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn virtual_stream_reads_lazily_without_the_clipboard() {
        let manifest = Manifest {
            entries: vec![ManifestEntry {
                id: 0,
                parent: None,
                kind: KIND_FILE,
                name: b"hello.txt".to_vec(),
                size: PAYLOAD.len() as u64,
                mode: 0o644,
                mtime_sec: 0,
                mtime_nsec: 0,
            }],
        };
        let (directed_tx, mut directed_rx) = tokio::sync::mpsc::channel(8);
        let broker = Arc::new(Broker::new(directed_tx));
        let responder = broker.clone();
        let handle = tokio::spawn(async move {
            while let Some((origin, frame)) = directed_rx.recv().await {
                if let Ok(Message::FileRead(read)) = Message::decode(&frame[4..]) {
                    let start = (read.offset as usize).min(PAYLOAD.len());
                    let end = (start + read.length as usize).min(PAYLOAD.len());
                    responder.complete(
                        origin,
                        FileData {
                            request_id: read.request_id,
                            offset: read.offset,
                            data: PAYLOAD[start..end].to_vec(),
                        },
                    );
                }
            }
        });
        let files = Arc::new(RemoteFileSet {
            origin: [7; 8],
            serial: 8,
            manifest,
            entries: vec![VirtualEntry {
                id: 0,
                relative: "hello.txt".encode_utf16().collect(),
                is_dir: false,
                size: PAYLOAD.len() as u64,
                mode: 0o644,
                mtime_sec: 0,
                mtime_nsec: 0,
            }],
            broker,
            runtime: tokio::runtime::Handle::current(),
        });
        let stream = Arc::new(VirtualStream::new(files, 0, PAYLOAD.len() as u64, 0, 0, 0));
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut out = Vec::new();
            loop {
                let mut buffer = [0u8; 64];
                match stream.read_into(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => out.extend_from_slice(&buffer[..count]),
                    Err(error) => {
                        let _ = tx.send(Err(format!("{error:?}")));
                        return;
                    }
                }
            }
            let _ = tx.send(Ok(out));
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("reader dropped");
        handle.abort();
        assert_eq!(result.expect("stream read"), PAYLOAD);
    }

    #[tokio::test]
    async fn descriptor_block_has_the_expected_layout() {
        let object = VirtualDataObject::new(files());
        let bytes = object.descriptor_bytes();
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 2);
        let dir = 4;
        assert_eq!(
            u32::from_le_bytes(bytes[dir..dir + 4].try_into().unwrap()),
            0x8000_0024
        );
        assert_eq!(
            u32::from_le_bytes(bytes[dir + 36..dir + 40].try_into().unwrap()),
            FILE_ATTRIBUTE_DIRECTORY.0
        );
        let file = 4 + DESCRIPTOR_SIZE;
        assert_eq!(
            u32::from_le_bytes(bytes[file + 36..file + 40].try_into().unwrap()),
            FILE_ATTRIBUTE_READONLY.0
        );
        assert_eq!(
            u32::from_le_bytes(bytes[file + 68..file + 72].try_into().unwrap()),
            3
        );
        let name: Vec<u16> = bytes[file + 72..file + 72 + 24]
            .chunks(2)
            .map(|pair| u16::from_le_bytes(pair.try_into().unwrap()))
            .take_while(|unit| *unit != 0)
            .collect();
        assert_eq!(String::from_utf16(&name).unwrap(), "dir\\file.txt");
    }
}
