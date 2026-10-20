//! Remote file offers: validation, FUSE generations and clipboard updates.
//!
//! A new peer ignores [`FileOffer`] frames, so the normal `Announce` still
//! carries the sender's original URI list. The receiver marks accepted offers
//! as lazy and suppresses the matching Announce, which prevents the legacy
//! same-path behavior from overwriting the local FUSE view.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::file::fuse::ClipboardMount;
use crate::file::manifest::{MIME_URI_LIST, Manifest, ManifestLimits, path_to_file_uri};
use crate::item::ClipboardItem;
use crate::protocol::{FILE_PROTOCOL_VERSION, FileOffer};

/// How many accepted offer ids are remembered to reject duplicates and
/// match their Announce.
const RECENT_OFFERS: usize = 64;

pub struct Receiver {
    mount: Option<Arc<ClipboardMount>>,
    limits: ManifestLimits,
    recent: VecDeque<([u8; 8], u64)>,
    lazy: VecDeque<([u8; 8], u64)>,
}

impl Receiver {
    pub fn new(mount: Option<Arc<ClipboardMount>>, limits: ManifestLimits) -> Self {
        Self {
            mount,
            limits,
            recent: VecDeque::new(),
            lazy: VecDeque::new(),
        }
    }

    /// Validate and apply a `FileOffer`, returning the clipboard item to set.
    ///
    /// `self_origin` rejects offers that claim to come from this machine.
    /// Returns `None` when the offer is invalid, unsupported, or FUSE is not
    /// mounted; in every such case the following Announce is applied normally.
    pub fn accept(
        &mut self,
        peer_origin: [u8; 8],
        self_origin: [u8; 8],
        offer: &FileOffer,
    ) -> Option<ClipboardItem> {
        if offer.version != FILE_PROTOCOL_VERSION {
            log::debug!(
                "file offer: unsupported version {}; applying Announce",
                offer.version
            );
            return None;
        }
        if offer.origin != peer_origin {
            log::warn!("file offer: origin does not match the authenticated peer");
            return None;
        }
        if offer.origin == self_origin {
            log::warn!("file offer: rejecting self-origin offer");
            return None;
        }
        if self.recent.contains(&(offer.origin, offer.serial)) {
            log::debug!("file offer: duplicate offer for serial {}", offer.serial);
            return None;
        }
        let mount = self.mount.as_ref()?;
        let manifest = match Manifest::decode(&offer.manifest, self.limits) {
            Ok(manifest) => manifest,
            Err(e) => {
                log::warn!("file offer: rejected manifest: {e}");
                return None;
            }
        };
        let Some(paths) = mount.add_generation(offer.origin, offer.serial, manifest) else {
            log::warn!("file offer: could not create a FUSE generation");
            return None;
        };
        if paths.is_empty() {
            log::warn!("file offer: manifest has no top-level entries");
            mount.remove_generation(offer.origin, offer.serial);
            return None;
        }
        let mut data = String::new();
        for path in &paths {
            data.push_str(&path_to_file_uri(path));
            data.push_str("\r\n");
        }
        self.remember(offer.origin, offer.serial);
        Some(ClipboardItem {
            origin: offer.origin,
            serial: offer.serial,
            reps: vec![(MIME_URI_LIST.to_string(), data.into_bytes())],
        })
    }

    /// Mark an applied offer so its Announce is skipped.
    pub fn mark_lazy(&mut self, origin: [u8; 8], serial: u64) {
        if !self.lazy.contains(&(origin, serial)) {
            self.lazy.push_back((origin, serial));
        }
        trim(&mut self.lazy, RECENT_OFFERS);
    }

    /// Drop an offer whose clipboard set failed.
    pub fn discard(&mut self, origin: [u8; 8], serial: u64) {
        self.recent.retain(|id| *id != (origin, serial));
        self.lazy.retain(|id| *id != (origin, serial));
        if let Some(mount) = &self.mount {
            mount.remove_generation(origin, serial);
        }
    }

    /// Consume the lazy marker for an incoming Announce.
    pub fn take_lazy(&mut self, origin: [u8; 8], serial: u64) -> bool {
        let Some(index) = self.lazy.iter().position(|id| *id == (origin, serial)) else {
            return false;
        };
        self.lazy.remove(index);
        true
    }

    /// Whether a local path lies inside the lazy view.
    pub fn contains_path(&self, path: &std::path::Path) -> bool {
        self.mount
            .as_ref()
            .is_some_and(|mount| mount.contains_path(path))
    }

    pub fn gc(&self) {
        if let Some(mount) = &self.mount {
            mount.gc();
        }
    }

    fn remember(&mut self, origin: [u8; 8], serial: u64) {
        self.recent.push_back((origin, serial));
        trim(&mut self.recent, RECENT_OFFERS);
    }
}

fn trim<T>(queue: &mut VecDeque<T>, max: usize) {
    while queue.len() > max {
        queue.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ManifestLimits {
        ManifestLimits {
            max_entries: 100,
            max_bytes: 1024 * 1024,
        }
    }

    fn empty_offer(origin: [u8; 8], serial: u64, version: u8) -> FileOffer {
        FileOffer {
            version,
            origin,
            serial,
            manifest: Vec::new(),
        }
    }

    #[test]
    fn without_a_mount_offers_fall_back_to_announce() {
        let mut rx = Receiver::new(None, limits());
        assert!(
            rx.accept([1; 8], [2; 8], &empty_offer([1; 8], 1, 1))
                .is_none()
        );
    }

    #[test]
    fn versions_origins_and_duplicates_are_rejected_before_fuse() {
        let mut rx = Receiver::new(None, limits());
        assert!(
            rx.accept([1; 8], [2; 8], &empty_offer([1; 8], 1, 2))
                .is_none()
        );
        assert!(
            rx.accept([1; 8], [2; 8], &empty_offer([3; 8], 1, 1))
                .is_none()
        );
        assert!(
            rx.accept([1; 8], [1; 8], &empty_offer([1; 8], 1, 1))
                .is_none()
        );
        assert!(
            rx.accept([1; 8], [2; 8], &empty_offer([1; 8], 1, 1))
                .is_none()
        );
    }

    #[test]
    fn lazy_marker_is_consumed_once() {
        let mut rx = Receiver::new(None, limits());
        rx.mark_lazy([1; 8], 9);
        assert!(rx.take_lazy([1; 8], 9));
        assert!(!rx.take_lazy([1; 8], 9));
    }

    #[test]
    fn recent_offers_are_bounded() {
        let mut rx = Receiver::new(None, limits());
        for serial in 0..(RECENT_OFFERS as u64 * 2) {
            rx.remember([1; 8], serial);
        }
        assert_eq!(rx.recent.len(), RECENT_OFFERS);
    }
}
