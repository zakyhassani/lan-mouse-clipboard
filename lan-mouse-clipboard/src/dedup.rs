//! Loop prevention: origin checks, serials, and content-hash dedup.

use crate::item::{ClipboardItem, origin_from_fingerprint};

/// Tracks per-machine origin identity and decides, for each clipboard
/// item, whether it should be applied (remote) or broadcast (local).
///
/// Combined strategy:
/// - items whose `origin` equals our own id are never applied or re-broadcast;
/// - items whose content hash matches a recently-seen remote or local item
///   are dropped (dedup), so two machines copying identical content do not
///   ping-pong forever;
/// - applying a remote item arms an "echo suppress": the local backend's
///   change notification for that exact content is not re-broadcast.
pub struct LoopPrevention {
    self_origin: [u8; 8],
    serial: u64,
    last_local_hash: Option<[u8; 32]>,
    last_remote_hash: Option<[u8; 32]>,
    pending_suppress: Option<[u8; 32]>,
}

impl LoopPrevention {
    pub fn new(self_origin: [u8; 8]) -> Self {
        Self {
            self_origin,
            serial: 0,
            last_local_hash: None,
            last_remote_hash: None,
            pending_suppress: None,
        }
    }

    pub fn origin(&self) -> [u8; 8] {
        self.self_origin
    }

    /// Convenience: build from a local certificate fingerprint.
    pub fn from_fingerprint(fingerprint: &str) -> Self {
        Self::new(origin_from_fingerprint(fingerprint))
    }

    /// Allocate the next serial for a locally-originated item.
    pub fn next_serial(&mut self) -> u64 {
        self.serial = self.serial.wrapping_add(1);
        self.serial
    }

    /// Whether an item with this origin came from us.
    pub fn is_self(&self, origin: [u8; 8]) -> bool {
        origin == self.self_origin
    }

    /// Decide whether to apply a remotely-received item.
    ///
    /// Returns `true` if the item should be applied to the local clipboard.
    /// When `true`, arms echo suppression so the resulting local change
    /// notification is not re-broadcast.
    pub fn should_apply_remote(&mut self, item: &ClipboardItem) -> bool {
        if self.is_self(item.origin) {
            return false;
        }
        let hash = item.content_hash();
        if Some(hash) == self.last_remote_hash || Some(hash) == self.last_local_hash {
            return false;
        }
        self.last_remote_hash = Some(hash);
        self.pending_suppress = Some(hash);
        true
    }

    /// Decide whether a locally-observed change should be broadcast.
    ///
    /// Returns `true` if the item is a genuine local copy that peers
    /// should receive.
    pub fn on_local_change(&mut self, item: &ClipboardItem) -> bool {
        let hash = item.content_hash();

        // Echo of a just-applied remote item => suppress.
        if self.pending_suppress.take() == Some(hash) {
            self.last_local_hash = Some(hash);
            return false;
        }

        if Some(hash) == self.last_remote_hash || Some(hash) == self.last_local_hash {
            return false;
        }

        self.last_local_hash = Some(hash);
        true
    }

    /// Record that we broadcast a locally-originated item (tracks its hash
    /// for dedup against future remote echoes).
    pub fn note_broadcast(&mut self, item: &ClipboardItem) {
        self.last_local_hash = Some(item.content_hash());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: [u8; 8] = [1; 8];
    const B: [u8; 8] = [2; 8];

    fn remote_item(origin: [u8; 8], text: &str, serial: u64) -> ClipboardItem {
        ClipboardItem::text(text, origin, serial)
    }

    #[test]
    fn ignores_self_origin() {
        let mut lp = LoopPrevention::new(A);
        let item = remote_item(A, "hi", 1);
        assert!(!lp.should_apply_remote(&item));
    }

    #[test]
    fn applies_foreign_and_suppresses_echo() {
        let mut lp = LoopPrevention::new(A);
        let item = remote_item(B, "hello", 1);
        assert!(lp.should_apply_remote(&item));

        // Backend fires a local change with the same content => echo, suppressed.
        let echo = remote_item(A, "hello", 99); // note: origin here is what *we* stamp
        assert!(!lp.on_local_change(&echo));
    }

    #[test]
    fn genuine_local_copy_is_broadcast() {
        let mut lp = LoopPrevention::new(A);
        let item = ClipboardItem::text("fresh", A, 1);
        assert!(lp.on_local_change(&item));
    }

    #[test]
    fn identical_content_from_two_machines_is_deduped() {
        let mut lp = LoopPrevention::new(A);
        let first = remote_item(B, "same", 1);
        assert!(lp.should_apply_remote(&first));
        // A different machine copies the exact same text.
        let second = remote_item([3; 8], "same", 1);
        assert!(!lp.should_apply_remote(&second));
    }

    #[test]
    fn dedups_remote_against_local() {
        let mut lp = LoopPrevention::new(A);
        let local = ClipboardItem::text("mine", A, 1);
        assert!(lp.on_local_change(&local));
        lp.note_broadcast(&local);
        // peer echoes the same content back
        let echo = remote_item(B, "mine", 1);
        assert!(!lp.should_apply_remote(&echo));
    }

    #[test]
    fn serials_are_monotonic() {
        let mut lp = LoopPrevention::new(A);
        let s1 = lp.next_serial();
        let s2 = lp.next_serial();
        assert!(s2 > s1);
    }

    #[test]
    fn serials_are_monotonic_and_unique_across_many() {
        let mut lp = LoopPrevention::new(A);
        let mut prev = None;
        for _ in 0..100_000 {
            let s = lp.next_serial();
            if let Some(p) = prev {
                assert!(s > p);
            }
            prev = Some(s);
        }
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
        fn bytes(&mut self, n: usize) -> Vec<u8> {
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    #[test]
    fn random_content_is_applied_then_its_echo_is_suppressed() {
        let mut rng = Rng::new(0xDED0);
        for _ in 0..200 {
            let mut lp = LoopPrevention::new(A);
            let len = 1 + (rng.next() % 64) as usize;
            let data = rng.bytes(len);
            let mut item = ClipboardItem::text(&String::from_utf8_lossy(&data), B, 1);
            item.reps = vec![("application/octet-stream".into(), data.clone())];

            assert!(lp.should_apply_remote(&item), "foreign item must apply");

            // Backend echoes the exact same bytes back -> suppressed.
            let echo = ClipboardItem {
                origin: A,
                serial: 99,
                reps: vec![("application/octet-stream".into(), data)],
            };
            assert!(!lp.on_local_change(&echo), "echo must be suppressed");
        }
    }

    #[test]
    fn random_distinct_contents_are_all_applied() {
        let mut rng = Rng::new(0xFACE);
        let mut lp = LoopPrevention::new(A);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let data = rng.bytes(8);
            let item = ClipboardItem {
                origin: B,
                serial: rng.next(),
                reps: vec![("application/octet-stream".into(), data)],
            };
            // Every distinct random content is applied (not deduped).
            assert!(lp.should_apply_remote(&item));
            assert!(seen.insert(item.content_hash()));
        }
    }
}
