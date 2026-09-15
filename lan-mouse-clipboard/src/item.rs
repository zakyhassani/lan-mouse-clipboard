//! Core clipboard item types and helpers.

use sha2::{Digest, Sha256};

/// Default maximum total clipboard item size (all representations combined).
pub const DEFAULT_MAX_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// Hard ceiling for the configured item size cap. Guards against a
/// misconfigured or hostile config value causing unbounded allocations.
pub const MAX_ITEM_SIZE_LIMIT: usize = 256 * 1024 * 1024;

/// Preferred MIME type for plain text.
pub const MIME_TEXT_PLAIN: &str = "text/plain;charset=utf-8";
/// Common alias for plain text without a charset parameter.
pub const MIME_TEXT_PLAIN_ALT: &str = "text/plain";

/// MIME type for PNG images (the common clipboard image format).
pub const MIME_IMAGE_PNG: &str = "image/png";

/// Legacy X11 string targets. They carry text but are not MIME types, so they
/// are only transferable as a fallback when a selection offers no MIME type.
pub const X11_TEXT_TARGETS: &[&str] = &["UTF8_STRING", "STRING", "TEXT"];

/// Whether a MIME type carries UTF-8 text that may be newline-normalized.
///
/// Text MIME types (`text/*`) plus the legacy X11 string targets are treated
/// as text; everything else is passed through as raw binary. The X11 target
/// match is case-insensitive.
pub fn is_text_mime(mime: &str) -> bool {
    mime.to_ascii_lowercase().starts_with("text/")
        || X11_TEXT_TARGETS
            .iter()
            .any(|t| t.eq_ignore_ascii_case(mime))
}

/// Whether a MIME type carries an image.
pub fn is_image_mime(mime: &str) -> bool {
    mime.to_ascii_lowercase().starts_with("image/")
}

/// Size of one representation in bytes (MIME type plus data).
///
/// The single accounting rule for item size, shared by `total_size` and the
/// reading backends so the read-side and encode-side caps cannot diverge.
pub fn rep_size(mime: &str, data: &[u8]) -> usize {
    mime.len() + data.len()
}

/// Stable hash of a single representation.
pub fn rep_hash(mime: &str, data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(mime.as_bytes());
    hasher.update([0]);
    hasher.update(data);
    hasher.update([0]);
    hasher.finalize().into()
}

/// A clipboard item with one or more MIME representations.
///
/// `origin` is the 8-byte id of the machine that created the item and
/// `serial` is a per-machine monotonic counter. Together they make the
/// item globally identifiable and are used for loop prevention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClipboardItem {
    /// id of the originating machine (first 8 bytes of its cert fingerprint hash)
    pub origin: [u8; 8],
    /// monotonic per-machine counter, incremented per locally-originated item
    pub serial: u64,
    /// ordered list of (mime, data) representations; sender-preferred mime first
    pub reps: Vec<(String, Vec<u8>)>,
}

impl ClipboardItem {
    /// Build a text-only item in the preferred plain-text representation.
    pub fn text(text: &str, origin: [u8; 8], serial: u64) -> Self {
        Self {
            origin,
            serial,
            reps: vec![(MIME_TEXT_PLAIN.to_string(), text.as_bytes().to_vec())],
        }
    }

    /// Total size in bytes of all representations (does not include overhead).
    pub fn total_size(&self) -> usize {
        self.reps.iter().map(|(m, d)| rep_size(m, d)).sum()
    }

    /// The primary representation: the one receivers apply when they can only
    /// advertise a single MIME type (sender-preferred, so first).
    pub fn primary(&self) -> Option<&(String, Vec<u8>)> {
        self.reps.first()
    }

    /// MIME type of the primary representation, if any.
    pub fn primary_mime(&self) -> Option<&str> {
        self.reps.first().map(|(m, _)| m.as_str())
    }

    /// Whether the primary representation is plain text.
    pub fn primary_is_text(&self) -> bool {
        self.primary_mime().is_some_and(is_text_mime)
    }

    /// Whether the primary representation is an image.
    pub fn primary_is_image(&self) -> bool {
        self.primary_mime().is_some_and(is_image_mime)
    }

    /// Best-effort UTF-8 text extraction, preferring the plain-text MIME types.
    pub fn text_plain(&self) -> Option<String> {
        for (mime, data) in &self.reps {
            let m = mime.to_ascii_lowercase();
            if m == MIME_TEXT_PLAIN || m == MIME_TEXT_PLAIN_ALT || m.starts_with("text/plain") {
                return String::from_utf8(data.clone()).ok();
            }
        }
        None
    }

    /// Stable content hash over the *primary* representation only.
    ///
    /// Used for content-based dedup so two machines copying identical
    /// content do not ping-pong it forever. Hashing only the primary
    /// representation (rather than every rep) is deliberate: a receiver can
    /// advertise just one MIME type at a time, so the item it echoes back is
    /// the primary rep alone. Hashing the primary makes that echo hash
    /// identical to the broadcast item, which is what echo suppression
    /// relies on when an item carries several representations.
    pub fn content_hash(&self) -> [u8; 32] {
        match self.primary() {
            Some((mime, data)) => rep_hash(mime, data),
            None => rep_hash("", &[]),
        }
    }
}

/// A change observed on the local clipboard by a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClipboardChange {
    /// A new clipboard item is available.
    New(ClipboardItem),
}

/// Derive an 8-byte origin id from a certificate fingerprint string.
///
/// The fingerprint is stable and unique per machine (it already gates
/// connection auth), so it is a good persistent identity source.
pub fn origin_from_fingerprint(fingerprint: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(fingerprint.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_from_fingerprint_is_stable_and_unique() {
        let a = origin_from_fingerprint("aa:bb:cc");
        let b = origin_from_fingerprint("aa:bb:cc");
        let c = origin_from_fingerprint("dd:ee:ff");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn text_roundtrip() {
        let item = ClipboardItem::text("hello", [1; 8], 1);
        assert_eq!(item.text_plain().as_deref(), Some("hello"));
        assert_eq!(item.reps[0].0, MIME_TEXT_PLAIN);
    }

    #[test]
    fn content_hash_ignores_secondary_reps() {
        // The receiver advertises only the primary rep, so its echo carries a
        // single rep. Hashing the primary alone keeps broadcast and echo
        // hashes equal even when the sender bundles extra representations.
        let primary = ("image/png".to_string(), vec![1u8, 2, 3, 4]);
        let a = ClipboardItem {
            origin: [1; 8],
            serial: 1,
            reps: vec![primary.clone()],
        };
        let b = ClipboardItem {
            origin: [2; 8],
            serial: 9,
            reps: vec![
                primary,
                ("text/html".into(), b"<img>".to_vec()),
                ("text/plain".into(), b"alt".to_vec()),
            ],
        };
        assert_eq!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn content_hash_follows_the_primary_rep() {
        let a = ClipboardItem {
            origin: [1; 8],
            serial: 1,
            reps: vec![
                ("text/plain".into(), b"foo".to_vec()),
                ("text/html".into(), b"<b>foo</b>".to_vec()),
            ],
        };
        let b = a.clone();
        assert_eq!(a.content_hash(), b.content_hash());
        // A different primary rep changes the identity of what gets pasted.
        let c = ClipboardItem {
            origin: [1; 8],
            serial: 2,
            reps: vec![a.reps[1].clone(), a.reps[0].clone()],
        };
        assert_ne!(a.content_hash(), c.content_hash());
    }

    #[test]
    fn primary_helpers_report_the_first_rep() {
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![
                ("image/png".into(), vec![9, 9]),
                ("text/plain".into(), b"alt".to_vec()),
            ],
        };
        assert_eq!(item.primary_mime(), Some("image/png"));
        assert_eq!(
            item.primary().map(|(m, d)| (m.as_str(), d.len())),
            Some(("image/png", 2))
        );
        assert!(item.primary_is_image());
        assert!(!item.primary_is_text());
        assert!(ClipboardItem::text("t", [0; 8], 0).primary_is_text());
    }

    #[test]
    fn text_and_image_mime_detection() {
        assert!(is_text_mime(MIME_TEXT_PLAIN));
        assert!(is_text_mime("TEXT/PLAIN"));
        assert!(is_text_mime("UTF8_STRING"));
        // The legacy X11 target match is case-insensitive everywhere.
        assert!(is_text_mime("utf8_string"));
        assert!(is_text_mime("string"));
        assert!(!is_text_mime(MIME_IMAGE_PNG));
        assert!(is_image_mime("image/jpeg"));
        assert!(is_image_mime("IMAGE/PNG"));
        assert!(!is_image_mime(MIME_TEXT_PLAIN));
    }

    #[test]
    fn rep_size_and_hash_are_consistent_with_the_item() {
        let reps = vec![
            ("image/png".to_string(), vec![1u8, 2, 3]),
            ("text/plain".to_string(), b"hi".to_vec()),
        ];
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: reps.clone(),
        };
        let expected: usize = reps.iter().map(|(m, d)| rep_size(m, d)).sum();
        assert_eq!(item.total_size(), expected);
        // The primary hash is exactly the primary rep's hash.
        assert_eq!(item.content_hash(), rep_hash(&reps[0].0, &reps[0].1));
        assert_ne!(
            rep_hash(&reps[0].0, &reps[0].1),
            rep_hash(&reps[1].0, &reps[1].1)
        );
    }

    #[test]
    fn total_size_counts_reps() {
        let item = ClipboardItem::text("abc", [0; 8], 0);
        assert_eq!(item.total_size(), "text/plain;charset=utf-8".len() + 3);
    }

    #[test]
    fn text_plain_is_none_without_text_rep() {
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![("image/png".into(), vec![1, 2, 3])],
        };
        assert_eq!(item.text_plain(), None);
    }

    #[test]
    fn text_plain_is_none_for_invalid_utf8() {
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![(MIME_TEXT_PLAIN.into(), vec![0xff, 0xfe, 0xfd])],
        };
        assert_eq!(item.text_plain(), None);
    }

    #[test]
    fn text_plain_prefers_text_reps_in_order() {
        let item = ClipboardItem {
            origin: [0; 8],
            serial: 0,
            reps: vec![
                ("image/png".into(), vec![1, 2, 3]),
                ("TEXT/PLAIN".into(), b"pick me".to_vec()),
            ],
        };
        // Matching is case-insensitive and falls through non-text reps.
        assert_eq!(item.text_plain().as_deref(), Some("pick me"));
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

    fn random_item(rng: &mut Rng, with_text: bool) -> ClipboardItem {
        let mut reps = Vec::new();
        let n = 1 + rng.range(5);
        for i in 0..n {
            let data: Vec<u8> = (0..rng.range(32)).map(|_| rng.next() as u8).collect();
            if with_text && i == 0 {
                reps.push((MIME_TEXT_PLAIN.into(), b"hello".to_vec()));
            } else {
                let mime = format!("application/x-{}", rng.range(1000));
                reps.push((mime, data));
            }
        }
        ClipboardItem {
            origin: [rng.next() as u8; 8],
            serial: rng.next(),
            reps,
        }
    }

    #[test]
    fn content_hash_is_stable_across_random_items() {
        let mut rng = Rng::new(0xABCD);
        for _ in 0..200 {
            let item = random_item(&mut rng, true);
            let a = item.clone();
            assert_eq!(item.content_hash(), a.content_hash());
        }
    }

    #[test]
    fn content_hash_differs_for_random_distinct_primaries() {
        let mut rng = Rng::new(0x1234);
        let mut seen = std::collections::HashSet::new();
        for i in 0..200u64 {
            // A unique primary rep identifies the item; random secondary reps
            // must not affect the hash.
            let mut reps = vec![("image/png".to_string(), i.to_be_bytes().to_vec())];
            for _ in 0..rng.range(4) {
                let data: Vec<u8> = (0..1 + rng.range(32)).map(|_| rng.next() as u8).collect();
                reps.push((format!("application/x-{}", rng.range(1000)), data));
            }
            let item = ClipboardItem {
                origin: [rng.next() as u8; 8],
                serial: rng.next(),
                reps,
            };
            assert!(
                seen.insert(item.content_hash()),
                "collision for primary {i}"
            );
        }
    }

    #[test]
    fn total_size_matches_rep_sizes_for_random_items() {
        let mut rng = Rng::new(0xDEAD);
        for _ in 0..200 {
            let item = random_item(&mut rng, false);
            let expected: usize = item.reps.iter().map(|(m, d)| m.len() + d.len()).sum();
            assert_eq!(item.total_size(), expected);
        }
    }
}
