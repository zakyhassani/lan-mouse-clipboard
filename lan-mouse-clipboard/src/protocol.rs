//! Wire protocol: length-prefixed, big-endian messages.
//!
//! The transport runs over an ordered, reliable TCP stream, so the wire
//! needs no chunking, sequence numbers, or acknowledgements. Each frame
//! is `[len: u32 BE][payload]` where `payload` is `[kind: u8][fields...]`.

use crate::item::{ClipboardItem, DEFAULT_MAX_ITEM_SIZE};

pub const KIND_ANNOUNCE: u8 = 1;

/// Upper bound for a decoded payload. Guards against a misbehaving peer
/// allocating huge buffers; the effective cap is the item size limit.
pub const MAX_PAYLOAD_SIZE: usize = DEFAULT_MAX_ITEM_SIZE + 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Share one clipboard item with a peer.
    Announce(ClipboardItem),
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
        let payload = match self {
            Message::Announce(item) => encode_announce(item, max_item_size)?,
        };
        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(ProtocolError::PayloadTooLarge(
                payload.len(),
                MAX_PAYLOAD_SIZE,
            ));
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

    let origin_arr = cursor.take_arr8()?;
    let serial = u64::from_be_bytes(cursor.take_arr8()?);
    let n_mimes = u16::from_be_bytes(cursor.take_arr2()?) as usize;

    let mut reps = Vec::with_capacity(n_mimes);
    for _ in 0..n_mimes {
        let mime_len = u16::from_be_bytes(cursor.take_arr2()?) as usize;
        let mime = cursor.take(mime_len)?;
        let mime = String::from_utf8(mime.to_vec()).map_err(|_| err("mime not utf-8"))?;
        let data_len = u32::from_be_bytes(cursor.take_arr4()?) as usize;
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

/// A tiny cursor over a byte slice that advances an index on each read.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take_arr2(&mut self) -> Result<[u8; 2], ProtocolError> {
        let b = self.take(2)?;
        Ok([b[0], b[1]])
    }

    fn take_arr4(&mut self) -> Result<[u8; 4], ProtocolError> {
        let b = self.take(4)?;
        Ok([b[0], b[1], b[2], b[3]])
    }

    fn take_arr8(&mut self) -> Result<[u8; 8], ProtocolError> {
        let b = self.take(8)?;
        let mut out = [0u8; 8];
        out.copy_from_slice(b);
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
#[derive(Debug, Default)]
pub struct FrameReader {
    /// Bytes received but not yet consumed into a full frame.
    buf: Vec<u8>,
}

impl FrameReader {
    pub fn new() -> Self {
        Self::default()
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
        if len > MAX_PAYLOAD_SIZE {
            return Err(ProtocolError::PayloadTooLarge(len, MAX_PAYLOAD_SIZE));
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

    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn range(&mut self, hi: usize) -> usize {
            (self.next() % hi.max(1) as u64) as usize
        }
    }

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
}
