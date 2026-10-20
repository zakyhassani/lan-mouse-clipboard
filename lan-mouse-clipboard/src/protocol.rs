//! Wire protocol: length-prefixed, big-endian messages.
//!
//! The transport runs over an ordered, reliable TCP stream, so the wire
//! needs no chunking, sequence numbers, or acknowledgements. Each frame
//! is `[len: u32 BE][payload]` where `payload` is `[kind: u8][fields...]`.

use crate::item::{ClipboardItem, DEFAULT_MAX_ITEM_SIZE};

pub const KIND_ANNOUNCE: u8 = 1;
/// Liveness probe sent periodically by the network task.
pub const KIND_PING: u8 = 2;
/// Reply to a [`KIND_PING`]; proves the peer's read path is alive.
pub const KIND_PONG: u8 = 3;
/// Announces a lazy file offer (manifest only, no file bytes).
pub const KIND_FILE_OFFER: u8 = 4;
/// Requests one range of a previously offered file.
pub const KIND_FILE_READ: u8 = 5;
/// Answers a [`KIND_FILE_READ`] with file bytes.
pub const KIND_FILE_DATA: u8 = 6;
/// Answers a [`KIND_FILE_READ`] with an error.
pub const KIND_FILE_ERROR: u8 = 7;

/// Version of the file-offer protocol carried inside `FileOffer`.
pub const FILE_PROTOCOL_VERSION: u8 = 1;
/// Absolute manifest cap accepted by the framing layer.
pub const MAX_FILE_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
/// Largest read range accepted in one `FileRead`/`FileData`.
pub const FILE_READ_CHUNK_MAX: u32 = 1024 * 1024;

/// Largest payload any file frame may carry (manifest + its header).
pub const MAX_FILE_FRAME_PAYLOAD: usize = MAX_FILE_MANIFEST_BYTES + 1024;

/// A lazy file offer: metadata only, the bytes are read on demand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileOffer {
    pub version: u8,
    pub origin: [u8; 8],
    pub serial: u64,
    /// Encoded manifest, validated by the receiver against its own limits.
    pub manifest: Vec<u8>,
}

/// A request for one range of an offered file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileRead {
    pub request_id: u64,
    pub serial: u64,
    pub entry_id: u32,
    pub offset: u64,
    pub length: u32,
}

/// Data answering a [`FileRead`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileData {
    pub request_id: u64,
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Error answering a [`FileRead`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileError {
    pub request_id: u64,
    pub code: u8,
    pub message: String,
}

pub const FILE_ERR_IO: u8 = 1;
pub const FILE_ERR_NOT_FOUND: u8 = 2;
pub const FILE_ERR_RANGE: u8 = 3;
pub const FILE_ERR_UNAVAILABLE: u8 = 4;

/// Overhead margin allowed on top of the item size cap when framing. Covers
/// the kind/origin/serial/count header plus the per-representation length
/// prefixes, so a legitimate item at exactly the cap still fits its frame.
pub const FRAME_OVERHEAD_MARGIN: usize = 64 * 1024;

/// Upper bound for a decoded payload given the configured item size cap.
/// Guards against a misbehaving peer allocating huge buffers.
pub fn max_payload_size(max_item_size: usize) -> usize {
    max_item_size.saturating_add(FRAME_OVERHEAD_MARGIN)
}

/// Upper bound for a decoded payload given the item and manifest caps.
///
/// File frames are deliberately independent of `max_item_size`: a manifest
/// can be larger than a text/image item and a file chunk is always small.
pub fn max_frame_payload(max_item_size: usize, max_manifest_bytes: usize) -> usize {
    max_payload_size(max_item_size)
        .max(max_manifest_bytes.saturating_add(FRAME_OVERHEAD_MARGIN))
        .max(FILE_READ_CHUNK_MAX as usize + FRAME_OVERHEAD_MARGIN)
}

/// Upper bound for a decoded payload using the default item size cap.
pub const MAX_PAYLOAD_SIZE: usize = DEFAULT_MAX_ITEM_SIZE + FRAME_OVERHEAD_MARGIN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Share one clipboard item with a peer.
    Announce(ClipboardItem),
    /// Announce a lazy file offer (metadata only).
    FileOffer(FileOffer),
    /// Request one range of an offered file.
    FileRead(FileRead),
    /// Answer a read request with file bytes.
    FileData(FileData),
    /// Answer a read request with an error.
    FileError(FileError),
    /// Liveness probe (health check).
    Ping,
    /// Liveness reply to a [`Message::Ping`].
    Pong,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("item too large: {0} bytes exceeds cap of {1}")]
    TooLarge(usize, usize),
    #[error("payload too large: {0} bytes exceeds cap of {1}")]
    PayloadTooLarge(usize, usize),
    #[error("unknown message kind: {0}")]
    UnknownKind(u8),
    #[error("invalid payload: {0}")]
    Invalid(String),
}

impl Message {
    /// Encode this message into a length-prefixed frame.
    pub fn encode(&self, max_item_size: usize) -> Result<Vec<u8>, ProtocolError> {
        let (payload, cap) = match self {
            Message::Announce(item) => (
                encode_announce(item, max_item_size)?,
                max_payload_size(max_item_size),
            ),
            Message::FileOffer(offer) => (encode_file_offer(offer)?, MAX_FILE_FRAME_PAYLOAD),
            Message::FileRead(read) => {
                if read.length > FILE_READ_CHUNK_MAX {
                    return Err(ProtocolError::Invalid(format!(
                        "read length {} exceeds {FILE_READ_CHUNK_MAX}",
                        read.length
                    )));
                }
                (encode_file_read(read), MAX_FILE_FRAME_PAYLOAD)
            }
            Message::FileData(data) => {
                if data.data.len() > FILE_READ_CHUNK_MAX as usize {
                    return Err(ProtocolError::Invalid(format!(
                        "file data {} exceeds {FILE_READ_CHUNK_MAX}",
                        data.data.len()
                    )));
                }
                (encode_file_data(data), MAX_FILE_FRAME_PAYLOAD)
            }
            Message::FileError(err) => (encode_file_error(err)?, MAX_FILE_FRAME_PAYLOAD),
            Message::Ping => (vec![KIND_PING], max_payload_size(max_item_size)),
            Message::Pong => (vec![KIND_PONG], max_payload_size(max_item_size)),
        };
        if payload.len() > cap {
            return Err(ProtocolError::PayloadTooLarge(payload.len(), cap));
        }
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        Ok(frame)
    }

    /// Decode a message from a raw payload (without the length prefix).
    pub fn decode(payload: &[u8]) -> Result<Message, ProtocolError> {
        let (&kind, rest) = payload
            .split_first()
            .ok_or_else(|| ProtocolError::Invalid("empty payload".into()))?;
        match kind {
            KIND_ANNOUNCE => decode_announce(rest).map(Message::Announce),
            KIND_FILE_OFFER => decode_file_offer(rest).map(Message::FileOffer),
            KIND_FILE_READ => decode_file_read(rest).map(Message::FileRead),
            KIND_FILE_DATA => decode_file_data(rest).map(Message::FileData),
            KIND_FILE_ERROR => decode_file_error(rest).map(Message::FileError),
            KIND_PING | KIND_PONG => {
                if !rest.is_empty() {
                    return Err(ProtocolError::Invalid(
                        "trailing bytes after control frame".into(),
                    ));
                }
                Ok(if kind == KIND_PING {
                    Message::Ping
                } else {
                    Message::Pong
                })
            }
            _ => Err(ProtocolError::UnknownKind(kind)),
        }
    }
}

fn encode_announce(item: &ClipboardItem, max_item_size: usize) -> Result<Vec<u8>, ProtocolError> {
    let size = item.total_size();
    if size > max_item_size {
        return Err(ProtocolError::TooLarge(size, max_item_size));
    }
    let n_mimes = item.reps.len();
    if n_mimes > u16::MAX as usize {
        return Err(ProtocolError::Invalid(format!(
            "too many mime reps: {n_mimes}"
        )));
    }

    let mut out = Vec::with_capacity(1 + 8 + 8 + 2 + size);
    out.push(KIND_ANNOUNCE);
    out.extend_from_slice(&item.origin);
    out.extend_from_slice(&item.serial.to_be_bytes());
    out.extend_from_slice(&(n_mimes as u16).to_be_bytes());
    for (mime, data) in &item.reps {
        if mime.len() > u16::MAX as usize {
            return Err(ProtocolError::Invalid("mime too long".into()));
        }
        out.extend_from_slice(&(mime.len() as u16).to_be_bytes());
        out.extend_from_slice(mime.as_bytes());
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        out.extend_from_slice(data);
    }
    Ok(out)
}

fn decode_announce(rest: &[u8]) -> Result<ClipboardItem, ProtocolError> {
    let err = |m: &str| ProtocolError::Invalid(m.to_string());
    let mut cursor = Cursor::new(rest);

    let origin_arr = cursor.take_arr::<8>()?;
    let serial = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let n_mimes = u16::from_be_bytes(cursor.take_arr::<2>()?) as usize;

    let mut reps = Vec::with_capacity(n_mimes);
    for _ in 0..n_mimes {
        let mime_len = u16::from_be_bytes(cursor.take_arr::<2>()?) as usize;
        let mime = cursor.take(mime_len)?;
        let mime = String::from_utf8(mime.to_vec()).map_err(|_| err("mime not utf-8"))?;
        let data_len = u32::from_be_bytes(cursor.take_arr::<4>()?) as usize;
        let data = cursor.take(data_len)?.to_vec();
        reps.push((mime, data));
    }

    if !cursor.is_empty() {
        return Err(err("trailing bytes"));
    }

    Ok(ClipboardItem {
        origin: origin_arr,
        serial,
        reps,
    })
}

fn encode_file_offer(offer: &FileOffer) -> Result<Vec<u8>, ProtocolError> {
    if offer.manifest.len() > MAX_FILE_MANIFEST_BYTES {
        return Err(ProtocolError::TooLarge(
            offer.manifest.len(),
            MAX_FILE_MANIFEST_BYTES,
        ));
    }
    let mut out = Vec::with_capacity(1 + 1 + 8 + 8 + 4 + offer.manifest.len());
    out.push(KIND_FILE_OFFER);
    out.push(offer.version);
    out.extend_from_slice(&offer.origin);
    out.extend_from_slice(&offer.serial.to_be_bytes());
    out.extend_from_slice(&(offer.manifest.len() as u32).to_be_bytes());
    out.extend_from_slice(&offer.manifest);
    Ok(out)
}

fn decode_file_offer(rest: &[u8]) -> Result<FileOffer, ProtocolError> {
    let mut cursor = Cursor::new(rest);
    let version = u8::from_be_bytes(cursor.take_arr::<1>()?);
    if version != FILE_PROTOCOL_VERSION {
        return Err(ProtocolError::Invalid(format!(
            "unsupported file protocol version {version}"
        )));
    }
    let origin = cursor.take_arr::<8>()?;
    let serial = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let manifest_len = u32::from_be_bytes(cursor.take_arr::<4>()?) as usize;
    if manifest_len > MAX_FILE_MANIFEST_BYTES {
        return Err(ProtocolError::TooLarge(
            manifest_len,
            MAX_FILE_MANIFEST_BYTES,
        ));
    }
    let manifest = cursor.take(manifest_len)?.to_vec();
    if !cursor.is_empty() {
        return Err(ProtocolError::Invalid("trailing bytes".into()));
    }
    Ok(FileOffer {
        version,
        origin,
        serial,
        manifest,
    })
}

fn encode_file_read(read: &FileRead) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 8 + 4 + 8 + 4);
    out.push(KIND_FILE_READ);
    out.extend_from_slice(&read.request_id.to_be_bytes());
    out.extend_from_slice(&read.serial.to_be_bytes());
    out.extend_from_slice(&read.entry_id.to_be_bytes());
    out.extend_from_slice(&read.offset.to_be_bytes());
    out.extend_from_slice(&read.length.to_be_bytes());
    out
}

fn decode_file_read(rest: &[u8]) -> Result<FileRead, ProtocolError> {
    let mut cursor = Cursor::new(rest);
    let request_id = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let serial = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let entry_id = u32::from_be_bytes(cursor.take_arr::<4>()?);
    let offset = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let length = u32::from_be_bytes(cursor.take_arr::<4>()?);
    if !cursor.is_empty() {
        return Err(ProtocolError::Invalid("trailing bytes".into()));
    }
    if length > FILE_READ_CHUNK_MAX {
        return Err(ProtocolError::Invalid(format!(
            "read length {length} exceeds {FILE_READ_CHUNK_MAX}"
        )));
    }
    Ok(FileRead {
        request_id,
        serial,
        entry_id,
        offset,
        length,
    })
}

fn encode_file_data(data: &FileData) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 8 + 4 + data.data.len());
    out.push(KIND_FILE_DATA);
    out.extend_from_slice(&data.request_id.to_be_bytes());
    out.extend_from_slice(&data.offset.to_be_bytes());
    out.extend_from_slice(&(data.data.len() as u32).to_be_bytes());
    out.extend_from_slice(&data.data);
    out
}

fn decode_file_data(rest: &[u8]) -> Result<FileData, ProtocolError> {
    let mut cursor = Cursor::new(rest);
    let request_id = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let offset = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let len = u32::from_be_bytes(cursor.take_arr::<4>()?) as usize;
    if len > FILE_READ_CHUNK_MAX as usize {
        return Err(ProtocolError::Invalid(format!(
            "file data {len} exceeds {FILE_READ_CHUNK_MAX}"
        )));
    }
    let data = cursor.take(len)?.to_vec();
    if !cursor.is_empty() {
        return Err(ProtocolError::Invalid("trailing bytes".into()));
    }
    Ok(FileData {
        request_id,
        offset,
        data,
    })
}

fn encode_file_error(err: &FileError) -> Result<Vec<u8>, ProtocolError> {
    if err.message.len() > u16::MAX as usize {
        return Err(ProtocolError::Invalid("file error message too long".into()));
    }
    let mut out = Vec::with_capacity(1 + 8 + 1 + 2 + err.message.len());
    out.push(KIND_FILE_ERROR);
    out.extend_from_slice(&err.request_id.to_be_bytes());
    out.push(err.code);
    out.extend_from_slice(&(err.message.len() as u16).to_be_bytes());
    out.extend_from_slice(err.message.as_bytes());
    Ok(out)
}

fn decode_file_error(rest: &[u8]) -> Result<FileError, ProtocolError> {
    let mut cursor = Cursor::new(rest);
    let request_id = u64::from_be_bytes(cursor.take_arr::<8>()?);
    let code = u8::from_be_bytes(cursor.take_arr::<1>()?);
    let len = u16::from_be_bytes(cursor.take_arr::<2>()?) as usize;
    let message = String::from_utf8(cursor.take(len)?.to_vec())
        .map_err(|_| ProtocolError::Invalid("file error message not utf-8".into()))?;
    if !cursor.is_empty() {
        return Err(ProtocolError::Invalid("trailing bytes".into()));
    }
    Ok(FileError {
        request_id,
        code,
        message,
    })
}

/// A tiny cursor over a byte slice that advances an index on each read.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take_arr<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| ProtocolError::Invalid("overflow".into()))?;
        if end > self.buf.len() {
            return Err(ProtocolError::Invalid("truncated".into()));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }
}

/// Accumulates raw stream bytes and splits them into complete payloads,
/// handling frames that arrive across multiple reads and multiple frames
/// within a single read.
///
/// Feeds the length-prefixed envelope `[len: u32 BE][payload]` and yields
/// each payload as soon as its full frame is available.
#[derive(Debug)]
pub struct FrameReader {
    /// Bytes received but not yet consumed into a full frame.
    buf: Vec<u8>,
    /// Maximum accepted payload length for a single frame.
    max_payload: usize,
}

impl Default for FrameReader {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameReader {
    /// Reader using the default item size cap.
    pub fn new() -> Self {
        Self::with_max_payload(MAX_PAYLOAD_SIZE)
    }

    /// Reader that accepts payloads up to `max_payload` bytes.
    pub fn with_max_payload(max_payload: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_payload,
        }
    }

    /// Push freshly-read bytes onto the accumulator.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Drain the next complete payload, if one is buffered.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes(self.buf[..4].try_into().unwrap()) as usize;
        if len > self.max_payload {
            return Err(ProtocolError::PayloadTooLarge(len, self.max_payload));
        }
        let total = 4 + len;
        if self.buf.len() < total {
            return Ok(None);
        }
        let payload = self.buf[4..total].to_vec();
        // Drop the consumed frame, retaining any bytes that start the next one.
        self.buf.drain(..total);
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::Rng;

    fn sample_item() -> ClipboardItem {
        ClipboardItem {
            origin: [0xab; 8],
            serial: 42,
            reps: vec![
                ("text/plain;charset=utf-8".into(), b"hello world".to_vec()),
                ("text/html".into(), b"<b>hello</b>".to_vec()),
            ],
        }
    }

    #[test]
    fn announce_roundtrip() {
        let item = sample_item();
        let frame = Message::Announce(item.clone())
            .encode(64 * 1024 * 1024)
            .unwrap();
        let payload = &frame[4..];
        let decoded = Message::decode(payload).unwrap();
        assert_eq!(decoded, Message::Announce(item));
    }

    #[test]
    fn ping_pong_roundtrip() {
        for msg in [Message::Ping, Message::Pong] {
            let frame = msg.encode(1024).unwrap();
            assert_eq!(Message::decode(&frame[4..]).unwrap(), msg);
        }
    }

    #[test]
    fn reject_over_size_cap() {
        let item = ClipboardItem {
            origin: [1; 8],
            serial: 1,
            reps: vec![("text/plain".into(), vec![0u8; 1000])],
        };
        assert!(matches!(
            Message::Announce(item).encode(100),
            Err(ProtocolError::TooLarge(_, 100))
        ));
    }

    #[test]
    fn reject_unknown_kind() {
        assert!(matches!(
            Message::decode(&[0xff, 0, 0, 0]),
            Err(ProtocolError::UnknownKind(0xff))
        ));
    }

    #[test]
    fn reject_truncated() {
        let item = sample_item();
        let frame = Message::Announce(item).encode(64 * 1024 * 1024).unwrap();
        let payload = &frame[4..];
        // cut it off mid-way
        assert!(matches!(
            Message::decode(&payload[..payload.len() - 3]),
            Err(ProtocolError::Invalid(_))
        ));
    }

    #[test]
    fn decode_rejects_empty_payload() {
        assert!(matches!(
            Message::decode(&[]),
            Err(ProtocolError::Invalid(_))
        ));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let item = sample_item();
        let frame = Message::Announce(item).encode(64 * 1024 * 1024).unwrap();
        let mut payload = frame[4..].to_vec();
        payload.push(0x00); // extra trailing byte after a well-formed message
        assert!(matches!(
            Message::decode(&payload),
            Err(ProtocolError::Invalid(_))
        ));
    }

    #[test]
    fn decode_rejects_non_utf8_mime() {
        // kind + origin(8) + serial(8) + n_mimes(2=1) + mime_len(2=2) + bad bytes + data_len(0)
        let mut p = Vec::new();
        p.push(KIND_ANNOUNCE);
        p.extend_from_slice(&[0u8; 8]);
        p.extend_from_slice(&1u64.to_be_bytes());
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&2u16.to_be_bytes());
        p.extend_from_slice(&[0xff, 0xff]); // not valid UTF-8
        p.extend_from_slice(&0u32.to_be_bytes());
        assert!(matches!(
            Message::decode(&p),
            Err(ProtocolError::Invalid(_))
        ));
    }

    #[test]
    fn encode_rejects_mime_too_long() {
        let item = ClipboardItem {
            origin: [1; 8],
            serial: 1,
            reps: vec![("a".repeat(u16::MAX as usize + 1), Vec::new())],
        };
        assert!(matches!(
            Message::Announce(item).encode(64 * 1024 * 1024),
            Err(ProtocolError::Invalid(_))
        ));
    }

    // ---- deterministic PRNG for randomized tests (no external deps) ----

    fn random_item(rng: &mut Rng) -> ClipboardItem {
        let mut reps = Vec::new();
        for _ in 0..rng.range(4) {
            let mime_len = 1 + rng.range(16);
            let mime: String = (0..mime_len)
                .map(|_| (b'a' + (rng.next() % 26) as u8) as char)
                .collect();
            let data: Vec<u8> = (0..rng.range(64)).map(|_| rng.next() as u8).collect();
            reps.push((mime, data));
        }
        if reps.is_empty() {
            reps.push(("text/plain".into(), b"x".to_vec()));
        }
        ClipboardItem {
            origin: [rng.next() as u8; 8],
            serial: rng.next(),
            reps,
        }
    }

    #[test]
    fn random_items_roundtrip() {
        let mut rng = Rng::new(0xC0FFEE);
        for _ in 0..200 {
            let item = random_item(&mut rng);
            let frame = Message::Announce(item.clone())
                .encode(64 * 1024 * 1024)
                .unwrap_or_else(|e| panic!("encode failed for {item:?}: {e}"));
            let decoded = Message::decode(&frame[4..]).unwrap();
            assert_eq!(decoded, Message::Announce(item));
        }
    }

    #[test]
    fn random_frames_reassembled_through_random_chunks() {
        let mut rng = Rng::new(0xBEEF);
        let mut all = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..10 {
            let item = random_item(&mut rng);
            let frame = Message::Announce(item).encode(64 * 1024 * 1024).unwrap();
            expected.push(frame[4..].to_vec());
            all.extend_from_slice(&frame);
        }

        let mut reader = FrameReader::new();
        let mut got = Vec::new();
        let mut idx = 0;
        while idx < all.len() {
            let chunk = 1 + rng.range(all.len() - idx);
            reader.push(&all[idx..idx + chunk]);
            idx += chunk;
            while let Some(p) = reader.next_frame().unwrap() {
                got.push(p);
            }
        }
        while let Some(p) = reader.next_frame().unwrap() {
            got.push(p);
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn frame_stream_splits_multiple_frames() {
        let item = sample_item();
        let f1 = Message::Announce(item.clone())
            .encode(64 * 1024 * 1024)
            .unwrap();
        let f2 = Message::Announce(item).encode(64 * 1024 * 1024).unwrap();

        let all: Vec<u8> = f1.iter().chain(f2.iter()).copied().collect();

        let mut reader = FrameReader::new();
        // Feed in small chunks to exercise partial reads.
        let mut payloads = Vec::new();
        for chunk in all.chunks(7) {
            reader.push(chunk);
            while let Some(p) = reader.next_frame().unwrap() {
                payloads.push(p);
            }
        }
        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn frame_stream_handles_partial_header() {
        let item = sample_item();
        let f1 = Message::Announce(item).encode(64 * 1024 * 1024).unwrap();
        let mut reader = FrameReader::new();
        // not enough for the 4-byte header
        reader.push(&f1[..3]);
        assert!(reader.next_frame().unwrap().is_none());
        // push the rest of the header + a bit
        reader.push(&f1[3..7]);
        assert!(reader.next_frame().unwrap().is_none());
        // push the whole body
        reader.push(&f1[7..]);
        assert!(reader.next_frame().unwrap().is_some());
    }

    #[test]
    fn frame_rejects_huge_length() {
        let mut reader = FrameReader::new();
        // claim 4 GiB payload length
        reader.push(&[0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(
            reader.next_frame(),
            Err(ProtocolError::PayloadTooLarge(_, _))
        ));
    }

    #[test]
    fn frame_reader_honours_custom_payload_cap() {
        let mut reader = FrameReader::with_max_payload(1024);
        // A length prefix claiming 2 KiB is over the custom cap.
        reader.push(&2048u32.to_be_bytes());
        assert!(matches!(
            reader.next_frame(),
            Err(ProtocolError::PayloadTooLarge(2048, 1024))
        ));
        // The default reader accepts the same length.
        let mut dflt = FrameReader::new();
        dflt.push(&2048u32.to_be_bytes());
        assert!(dflt.next_frame().unwrap().is_none());
    }

    #[test]
    fn multi_rep_image_item_roundtrips() {
        let item = ClipboardItem {
            origin: [7; 8],
            serial: 3,
            reps: vec![
                (
                    "image/png".into(),
                    vec![0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x0a],
                ),
                ("text/html".into(), b"<img src=\"data:...\">".to_vec()),
                ("text/plain".into(), b"alt text".to_vec()),
            ],
        };
        let frame = Message::Announce(item.clone())
            .encode(1024 * 1024)
            .expect("encode");
        let decoded = Message::decode(&frame[4..]).expect("decode");
        assert_eq!(decoded, Message::Announce(item));
    }

    #[test]
    fn encode_scales_with_the_configured_cap() {
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![("image/png".into(), vec![0xAB; 8 * 1024])],
        };
        assert!(matches!(
            Message::Announce(item.clone()).encode(4 * 1024),
            Err(ProtocolError::TooLarge(_, 4096))
        ));
        assert!(Message::Announce(item).encode(16 * 1024).is_ok());
    }

    #[test]
    fn file_messages_roundtrip() {
        let messages = [
            Message::FileOffer(FileOffer {
                version: FILE_PROTOCOL_VERSION,
                origin: [0xAB; 8],
                serial: 7,
                manifest: vec![1, 2, 3, 4],
            }),
            Message::FileRead(FileRead {
                request_id: 9,
                serial: 7,
                entry_id: 3,
                offset: 1024,
                length: 4096,
            }),
            Message::FileData(FileData {
                request_id: 9,
                offset: 1024,
                data: vec![9; 4096],
            }),
            Message::FileError(FileError {
                request_id: 9,
                code: FILE_ERR_RANGE,
                message: "bad range".into(),
            }),
        ];
        for msg in messages {
            let frame = msg.encode(1024).expect("file frames ignore item cap");
            assert_eq!(Message::decode(&frame[4..]).unwrap(), msg);
        }
    }

    #[test]
    fn file_offer_rejects_unknown_versions() {
        let offer = Message::FileOffer(FileOffer {
            version: FILE_PROTOCOL_VERSION + 1,
            origin: [1; 8],
            serial: 1,
            manifest: vec![0; 8],
        });
        let frame = offer.encode(1024).unwrap();
        assert!(Message::decode(&frame[4..]).is_err());
    }

    #[test]
    fn file_read_and_data_reject_oversized_ranges() {
        let read = Message::FileRead(FileRead {
            request_id: 1,
            serial: 1,
            entry_id: 0,
            offset: 0,
            length: FILE_READ_CHUNK_MAX + 1,
        });
        assert!(read.encode(1024).is_err());

        let data = Message::FileData(FileData {
            request_id: 1,
            offset: 0,
            data: vec![0; FILE_READ_CHUNK_MAX as usize + 1],
        });
        assert!(data.encode(1024).is_err());
    }

    #[test]
    fn frame_reader_accepts_a_manifest_larger_than_the_item_cap() {
        let manifest_bytes = 2 * 1024 * 1024;
        let offer = Message::FileOffer(FileOffer {
            version: FILE_PROTOCOL_VERSION,
            origin: [3; 8],
            serial: 5,
            manifest: vec![0xA5; manifest_bytes],
        });
        let frame = offer.encode(1024).expect("file frame");
        let cap = max_frame_payload(1024, manifest_bytes);
        let mut reader = FrameReader::with_max_payload(cap);
        reader.push(&frame);
        let payload = reader.next_frame().unwrap().expect("complete frame");
        assert_eq!(payload.len(), frame.len() - 4);

        // The same frame is rejected by a reader sized only for items when
        // the manifest cap is smaller than the file frame.
        let mut small = FrameReader::with_max_payload(max_payload_size(1024));
        small.push(&frame);
        assert!(small.next_frame().is_err());
    }

    #[test]
    fn frame_cap_accounts_for_manifests_independently_of_items() {
        let cap = max_frame_payload(1024, 4 * 1024 * 1024);
        assert!(cap >= 4 * 1024 * 1024);
        assert!(cap >= FILE_READ_CHUNK_MAX as usize);
        assert_eq!(
            max_frame_payload(1024, 0),
            FILE_READ_CHUNK_MAX as usize + FRAME_OVERHEAD_MARGIN
        );
    }

    #[test]
    fn max_payload_size_tracks_the_item_cap() {
        assert_eq!(max_payload_size(0), FRAME_OVERHEAD_MARGIN);
        assert_eq!(
            max_payload_size(DEFAULT_MAX_ITEM_SIZE),
            DEFAULT_MAX_ITEM_SIZE + FRAME_OVERHEAD_MARGIN
        );
    }
}
