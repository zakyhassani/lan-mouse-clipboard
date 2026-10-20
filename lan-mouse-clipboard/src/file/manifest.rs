//! File offer manifests: URI parsing, tree walking, and wire encoding.
//!
//! Only metadata travels at copy time. The manifest is a flat list of entries
//! linked by an explicit parent id, so a receiver never has to trust a path
//! from the peer. File names are raw Unix bytes and are always single
//! components; paths are rebuilt locally from the entry graph.

use std::collections::HashSet;
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use percent_encoding::percent_decode_str;
use url::Url;

use crate::item::ClipboardItem;

/// MIME type carrying a newline-separated list of file URIs.
pub const MIME_URI_LIST: &str = "text/uri-list";
/// GNOME's file-copy format: a `copy`/`cut` command line followed by URIs.
pub const MIME_GNOME_COPIED_FILES: &str = "x-special/gnome-copied-files";

/// Maximum length of one file name component.
pub const MAX_NAME_LEN: usize = 255;
/// Sentinel parent id for a top-level entry.
pub const ROOT_PARENT: u32 = u32::MAX;

/// Absolute ceiling for an encoded manifest accepted from a peer.
pub const MAX_MANIFEST_BYTES: usize = crate::protocol::MAX_FILE_MANIFEST_BYTES;
/// Absolute ceiling for manifest entries accepted from a peer.
pub const MAX_MANIFEST_ENTRIES: usize = 1_000_000;
/// Read chunk size used by the lazy FUSE view and the read protocol.
pub const READ_CHUNK_SIZE: u32 = crate::protocol::FILE_READ_CHUNK_MAX;

pub const KIND_FILE: u8 = 0;
pub const KIND_DIR: u8 = 1;

/// Limits applied to a manifest, from config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestLimits {
    pub max_entries: usize,
    pub max_bytes: usize,
}

/// One entry in a file offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub id: u32,
    /// `None` for a top-level entry (encoded as [`ROOT_PARENT`]).
    pub parent: Option<u32>,
    pub kind: u8,
    /// Raw Unix name bytes, one path component.
    pub name: Vec<u8>,
    /// File size in bytes; zero for directories.
    pub size: u64,
    /// Permission bits (already masked to rwx).
    pub mode: u32,
    pub mtime_sec: i64,
    pub mtime_nsec: u32,
}

impl ManifestEntry {
    pub fn is_dir(&self) -> bool {
        self.kind == KIND_DIR
    }

    pub fn is_file(&self) -> bool {
        self.kind == KIND_FILE
    }
}

/// A validated file offer manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub entries: Vec<ManifestEntry>,
}

/// A built manifest plus the local paths aligned with `entries`.
#[derive(Debug, Clone)]
pub struct BuiltManifest {
    pub manifest: Manifest,
    pub paths: Vec<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("no files in the offer")]
    NoFiles,
    #[error("manifest has too many entries: {0} exceeds {1}")]
    TooManyEntries(usize, usize),
    #[error("manifest is too large: {0} bytes exceeds {1}")]
    TooLarge(usize, usize),
    #[error("invalid manifest: {0}")]
    Invalid(String),
}

/// The MIME type without parameters.
fn base_mime(mime: &str) -> &str {
    mime.split(';').next().unwrap_or(mime).trim()
}

/// Whether a MIME type is a file list type we understand.
pub fn is_file_mime(mime: &str) -> bool {
    let base = base_mime(mime);
    base.eq_ignore_ascii_case(MIME_URI_LIST) || base.eq_ignore_ascii_case(MIME_GNOME_COPIED_FILES)
}

/// Whether an item carries a file list representation worth parsing.
pub fn is_file_candidate(item: &ClipboardItem) -> bool {
    item.reps.iter().any(|(mime, _)| is_file_mime(mime))
}

/// Extract the file paths from an item, preferring `text/uri-list`.
pub fn item_paths(item: &ClipboardItem) -> Vec<PathBuf> {
    let uri_list = item
        .reps
        .iter()
        .find(|(mime, _)| base_mime(mime).eq_ignore_ascii_case(MIME_URI_LIST));
    if let Some((_, data)) = uri_list {
        return parse_uri_list(data, false);
    }
    if let Some((_, data)) = item
        .reps
        .iter()
        .find(|(mime, _)| base_mime(mime).eq_ignore_ascii_case(MIME_GNOME_COPIED_FILES))
    {
        return parse_uri_list(data, true);
    }
    Vec::new()
}

/// Parse a URI list into existing local file/directory paths.
///
/// Non-file schemes, remote hosts, comments and blank lines are ignored.
/// Paths are canonicalized and deduplicated. `gnome` strips the leading
/// `copy`/`cut` command line used by `x-special/gnome-copied-files`.
pub fn parse_uri_list(data: &[u8], gnome: bool) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut first_data_line = true;

    for raw in data.split(|b| *b == b'\n') {
        let line = match std::str::from_utf8(raw) {
            Ok(line) => line.trim_end_matches('\r').trim(),
            Err(_) => continue,
        };
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if gnome && first_data_line {
            first_data_line = false;
            let cmd = line.to_ascii_lowercase();
            if cmd == "copy" || cmd == "cut" {
                continue;
            }
        }
        first_data_line = false;
        let Some(path) = parse_file_uri(line) else {
            continue;
        };
        let Ok(canonical) = fs::canonicalize(&path) else {
            log::warn!("file offer: skipping missing path {}", path.display());
            continue;
        };
        if canonical == Path::new("/") {
            log::warn!("file offer: skipping the filesystem root");
            continue;
        }
        match fs::metadata(&canonical) {
            Ok(meta) if meta.is_file() || meta.is_dir() => {}
            Ok(_) => {
                log::warn!("file offer: skipping non-file {}", canonical.display());
                continue;
            }
            Err(e) => {
                log::warn!(
                    "file offer: skipping unreadable {}: {e}",
                    canonical.display()
                );
                continue;
            }
        }
        if seen.insert(canonical.clone()) {
            out.push(canonical);
        }
    }
    out
}

/// Parse one `file://` URI into a raw-bytes path.
fn parse_file_uri(line: &str) -> Option<PathBuf> {
    let url = match Url::parse(line) {
        Ok(url) => url,
        Err(e) => {
            log::warn!("file offer: invalid URI {line:?}: {e}");
            return None;
        }
    };
    if url.scheme() != "file" {
        return None;
    }
    match url.host_str() {
        None => {}
        Some(h) if h.eq_ignore_ascii_case("localhost") => {}
        Some(h) => {
            log::warn!("file offer: skipping non-local host {h:?}");
            return None;
        }
    }
    let bytes: Vec<u8> = percent_decode_str(url.path()).collect();
    if bytes.is_empty() || bytes[0] != b'/' {
        return None;
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
}

/// Encode a local path as a `file://` URI with percent escapes.
pub fn path_to_file_uri(path: &Path) -> String {
    let mut out = String::from("file://");
    for &b in path.as_os_str().as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Build a manifest for the given top-level paths.
///
/// Symlinks are dereferenced: the entry name comes from the selected path,
/// while kind, size and mode come from the target. Directory cycles are
/// detected by `(st_dev, st_ino)`; a cycle-breaking entry is included but not
/// descended into.
pub fn build_manifest(
    paths: &[PathBuf],
    limits: ManifestLimits,
) -> Result<BuiltManifest, ManifestError> {
    let mut walker = Walker {
        limits,
        entries: Vec::new(),
        paths: Vec::new(),
        dir_size: 4,
        visited_dirs: HashSet::new(),
    };
    for path in paths {
        walker.add(path, None)?;
    }
    if walker.entries.is_empty() {
        return Err(ManifestError::NoFiles);
    }
    Ok(BuiltManifest {
        manifest: Manifest {
            entries: walker.entries,
        },
        paths: walker.paths,
    })
}

struct Walker {
    limits: ManifestLimits,
    entries: Vec<ManifestEntry>,
    paths: Vec<PathBuf>,
    dir_size: usize,
    visited_dirs: HashSet<(u64, u64)>,
}

impl Walker {
    fn add(&mut self, path: &Path, parent: Option<u32>) -> Result<Option<u32>, ManifestError> {
        let Some(name) = path.file_name() else {
            log::warn!(
                "file offer: skipping path without a name {}",
                path.display()
            );
            return Ok(None);
        };
        let name = name.as_bytes().to_vec();
        if !valid_name(&name) {
            log::warn!("file offer: skipping invalid name in {}", path.display());
            return Ok(None);
        }
        let meta = match fs::metadata(path) {
            Ok(meta) => meta,
            Err(e) => {
                log::warn!("file offer: skipping {}: {e}", path.display());
                return Ok(None);
            }
        };
        if !meta.is_file() && !meta.is_dir() {
            log::warn!("file offer: skipping non-file {}", path.display());
            return Ok(None);
        }

        if self.entries.len() >= self.limits.max_entries {
            return Err(ManifestError::TooManyEntries(
                self.entries.len() + 1,
                self.limits.max_entries,
            ));
        }
        let id = self.entries.len() as u32;
        let kind = if meta.is_dir() { KIND_DIR } else { KIND_FILE };
        let entry_size = entry_encoded_len(&name);
        self.dir_size = self
            .dir_size
            .checked_add(entry_size)
            .ok_or(ManifestError::TooLarge(usize::MAX, self.limits.max_bytes))?;
        if self.dir_size > self.limits.max_bytes {
            return Err(ManifestError::TooLarge(
                self.dir_size,
                self.limits.max_bytes,
            ));
        }
        self.entries.push(ManifestEntry {
            id,
            parent,
            kind,
            name,
            size: if kind == KIND_FILE { meta.len() } else { 0 },
            mode: meta.mode() & 0o777,
            mtime_sec: meta.mtime(),
            mtime_nsec: meta.mtime_nsec().clamp(0, 999_999_999) as u32,
        });
        self.paths.push(path.to_path_buf());

        if kind == KIND_DIR {
            let key = (meta.dev(), meta.ino());
            if self.visited_dirs.insert(key) {
                let read = match fs::read_dir(path) {
                    Ok(read) => read,
                    Err(e) => {
                        log::warn!("file offer: cannot list {}: {e}", path.display());
                        return Ok(Some(id));
                    }
                };
                let mut children: Vec<PathBuf> =
                    read.flatten().map(|entry| entry.path()).collect::<Vec<_>>();
                children.sort_by(|a, b| a.as_os_str().as_bytes().cmp(b.as_os_str().as_bytes()));
                for child in children {
                    self.add(&child, Some(id))?;
                }
            }
        }
        Ok(Some(id))
    }
}

/// Encoded size of one entry, excluding the manifest's entry count.
fn entry_encoded_len(name: &[u8]) -> usize {
    4 + 4 + 1 + 2 + name.len() + 8 + 4 + 8 + 4
}

/// Smallest possible encoded entry (one-byte name).
const MIN_ENTRY_LEN: usize = 4 + 4 + 1 + 2 + 1 + 8 + 4 + 8 + 4;

impl Manifest {
    /// Validate structure and limits, then encode.
    pub fn encode(&self, limits: ManifestLimits) -> Result<Vec<u8>, ManifestError> {
        self.validate(limits)?;
        let mut out = Vec::with_capacity(4 + self.estimated_size());
        out.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.id.to_be_bytes());
            out.extend_from_slice(&entry.parent.unwrap_or(ROOT_PARENT).to_be_bytes());
            out.push(entry.kind);
            out.extend_from_slice(&(entry.name.len() as u16).to_be_bytes());
            out.extend_from_slice(&entry.name);
            out.extend_from_slice(&entry.size.to_be_bytes());
            out.extend_from_slice(&entry.mode.to_be_bytes());
            out.extend_from_slice(&entry.mtime_sec.to_be_bytes());
            out.extend_from_slice(&entry.mtime_nsec.to_be_bytes());
        }
        if out.len() > limits.max_bytes {
            return Err(ManifestError::TooLarge(out.len(), limits.max_bytes));
        }
        Ok(out)
    }

    /// Decode and validate a manifest received from a peer.
    pub fn decode(bytes: &[u8], limits: ManifestLimits) -> Result<Manifest, ManifestError> {
        if bytes.len() > limits.max_bytes {
            return Err(ManifestError::TooLarge(bytes.len(), limits.max_bytes));
        }
        let mut cursor = Cursor::new(bytes);
        let count = cursor.u32()? as usize;
        if count == 0 {
            return Err(ManifestError::NoFiles);
        }
        if count > limits.max_entries {
            return Err(ManifestError::TooManyEntries(count, limits.max_entries));
        }
        // A count far larger than the payload allows must not reserve memory
        // for entries that cannot possibly be present.
        let mut entries = Vec::with_capacity(count.min(bytes.len() / MIN_ENTRY_LEN));
        for _ in 0..count {
            let id = cursor.u32()?;
            let parent_raw = cursor.u32()?;
            let parent = if parent_raw == ROOT_PARENT {
                None
            } else {
                Some(parent_raw)
            };
            let kind = cursor.u8()?;
            if kind != KIND_FILE && kind != KIND_DIR {
                return Err(ManifestError::Invalid(format!("unknown entry kind {kind}")));
            }
            let name_len = cursor.u16()? as usize;
            let name = cursor.take(name_len)?.to_vec();
            if !valid_name(&name) {
                return Err(ManifestError::Invalid("invalid entry name".into()));
            }
            let size = cursor.u64()?;
            let mode = cursor.u32()?;
            let mtime_sec = cursor.i64()?;
            let mtime_nsec = cursor.u32()?;
            entries.push(ManifestEntry {
                id,
                parent,
                kind,
                name,
                size,
                mode,
                mtime_sec,
                mtime_nsec,
            });
        }
        if !cursor.is_empty() {
            return Err(ManifestError::Invalid("trailing bytes".into()));
        }
        let manifest = Manifest { entries };
        manifest.validate(limits)?;
        Ok(manifest)
    }

    /// Check ids, parent links and size caps.
    pub fn validate(&self, limits: ManifestLimits) -> Result<(), ManifestError> {
        if self.entries.is_empty() {
            return Err(ManifestError::NoFiles);
        }
        if self.entries.len() > limits.max_entries {
            return Err(ManifestError::TooManyEntries(
                self.entries.len(),
                limits.max_entries,
            ));
        }
        let mut size = 4usize;
        for (idx, entry) in self.entries.iter().enumerate() {
            if entry.id != idx as u32 {
                return Err(ManifestError::Invalid(format!(
                    "entry id {} is out of order at {idx}",
                    entry.id
                )));
            }
            if !valid_name(&entry.name) {
                return Err(ManifestError::Invalid(format!(
                    "invalid name on entry {}",
                    entry.id
                )));
            }
            if entry.kind != KIND_FILE && entry.kind != KIND_DIR {
                return Err(ManifestError::Invalid(format!(
                    "unknown kind on entry {}",
                    entry.id
                )));
            }
            if let Some(parent) = entry.parent {
                if parent >= entry.id {
                    return Err(ManifestError::Invalid(format!(
                        "entry {} has non-preceding parent {parent}",
                        entry.id
                    )));
                }
                let parent_entry = &self.entries[parent as usize];
                if !parent_entry.is_dir() {
                    return Err(ManifestError::Invalid(format!(
                        "entry {} has non-directory parent {parent}",
                        entry.id
                    )));
                }
            }
            size = size
                .checked_add(entry_encoded_len(&entry.name))
                .ok_or(ManifestError::TooLarge(usize::MAX, limits.max_bytes))?;
            if size > limits.max_bytes {
                return Err(ManifestError::TooLarge(size, limits.max_bytes));
            }
            if entry.mtime_nsec >= 1_000_000_000 {
                return Err(ManifestError::Invalid(format!(
                    "entry {} has invalid mtime",
                    entry.id
                )));
            }
        }
        Ok(())
    }

    pub fn estimated_size(&self) -> usize {
        4 + self
            .entries
            .iter()
            .map(|e| entry_encoded_len(&e.name))
            .sum::<usize>()
    }

    /// Children of `parent`, in manifest order.
    pub fn children(&self, parent: Option<u32>) -> Vec<&ManifestEntry> {
        self.entries.iter().filter(|e| e.parent == parent).collect()
    }
}

/// A name is one non-empty component, distinct from `.`/`..`, without NUL or
/// a path separator, and at most 255 bytes long.
pub fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name != b"."
        && name != b".."
        && !name.contains(&0)
        && !name.contains(&b'/')
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ManifestError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| ManifestError::Invalid("overflow".into()))?;
        if end > self.buf.len() {
            return Err(ManifestError::Invalid("truncated".into()));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, ManifestError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ManifestError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, ManifestError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, ManifestError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, ManifestError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "lan_mouse_manifest_{}_{}_{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn limits() -> ManifestLimits {
        ManifestLimits {
            max_entries: 100,
            max_bytes: 1024 * 1024,
        }
    }

    #[test]
    fn uri_list_parses_crlf_comments_localhost_and_dedupes() {
        let dir = temp_dir("uri");
        let file = dir.join("a file.txt");
        fs::write(&file, b"hello").unwrap();
        let uri = path_to_file_uri(&file);
        let data = format!(
            "# comment\r\n\r\n{uri}\r\n{uri}\r\nfile://localhost{}\r\n",
            uri.trim_start_matches("file://")
        );
        let paths = parse_uri_list(data.as_bytes(), false);
        assert_eq!(paths, vec![fs::canonicalize(&file).unwrap()]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn parameterized_uri_list_mime_is_recognized() {
        assert!(is_file_mime("text/uri-list;charset=utf-8"));
        assert!(is_file_mime("TEXT/URI-LIST; charset=UTF-8"));
        assert!(is_file_mime("x-special/gnome-copied-files;charset=utf-8"));
        assert!(!is_file_mime("text/plain"));
        let item = ClipboardItem {
            origin: [1; 8],
            serial: 1,
            reps: vec![(
                "text/uri-list;charset=utf-8".to_string(),
                b"file:///tmp/example\r\n".to_vec(),
            )],
        };
        assert!(is_file_candidate(&item));
        assert!(item_paths(&item).is_empty(), "missing path is skipped");
    }

    #[test]
    fn gnome_list_ignores_copy_command() {
        let dir = temp_dir("gnome");
        let file = dir.join("x");
        fs::write(&file, b"y").unwrap();
        let data = format!("copy\n{}\n", path_to_file_uri(&file));
        let paths = parse_uri_list(data.as_bytes(), true);
        assert_eq!(paths.len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn non_file_schemes_and_remote_hosts_are_skipped() {
        let data = b"https://example.com/x\nfile://server/share/x\n";
        assert!(parse_uri_list(data, false).is_empty());
    }

    #[test]
    fn percent_escapes_decode_to_raw_bytes() {
        let dir = temp_dir("bytes");
        let name = std::ffi::OsString::from_vec(vec![b'a', 0xff, b'b']);
        let file = dir.join(name);
        fs::write(&file, b"z").unwrap();
        let uri = path_to_file_uri(&file);
        let paths = parse_uri_list(format!("{uri}\n").as_bytes(), false);
        assert_eq!(paths, vec![fs::canonicalize(&file).unwrap()]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_roundtrip_and_tree_validation() {
        let dir = temp_dir("manifest");
        fs::create_dir(dir.join("sub")).unwrap();
        fs::write(dir.join("sub/file"), b"hello").unwrap();
        let built = build_manifest(std::slice::from_ref(&dir), limits()).unwrap();
        let encoded = built.manifest.encode(limits()).unwrap();
        let decoded = Manifest::decode(&encoded, limits()).unwrap();
        assert_eq!(decoded, built.manifest);
        assert_eq!(decoded.children(None).len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn cap_rejects_too_many_entries() {
        let dir = temp_dir("cap");
        for i in 0..5 {
            fs::write(dir.join(format!("f{i}")), b"x").unwrap();
        }
        let err = build_manifest(
            std::slice::from_ref(&dir),
            ManifestLimits {
                max_entries: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ManifestError::TooManyEntries(..)));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn build_clears_special_mode_bits() {
        let dir = temp_dir("mode");
        let file = dir.join("x");
        fs::write(&file, b"x").unwrap();
        let mut perms = fs::metadata(&file).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o6755);
        fs::set_permissions(&file, perms).unwrap();
        let built = build_manifest(std::slice::from_ref(&file), limits()).unwrap();
        let entry = built.manifest.entries.iter().find(|e| e.is_file()).unwrap();
        assert_eq!(entry.mode, 0o755);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn symlink_cycle_is_not_recursed() {
        let dir = temp_dir("cycle");
        fs::create_dir(dir.join("a")).unwrap();
        fs::write(dir.join("a/f"), b"x").unwrap();
        std::os::unix::fs::symlink(&dir, dir.join("a/loop")).unwrap();
        let built = build_manifest(std::slice::from_ref(&dir), limits()).unwrap();
        let loops = built
            .manifest
            .entries
            .iter()
            .filter(|e| e.name == b"loop")
            .count();
        assert_eq!(loops, 1);
        assert!(built.manifest.entries.len() < 10);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn decode_rejects_bad_names_and_traversal() {
        for bad in [b".".to_vec(), b"..".to_vec(), b"a/b".to_vec(), vec![0]] {
            let entry = ManifestEntry {
                id: 0,
                parent: None,
                kind: KIND_FILE,
                name: bad,
                size: 0,
                mode: 0o644,
                mtime_sec: 0,
                mtime_nsec: 0,
            };
            let mut out = Vec::new();
            out.extend_from_slice(&1u32.to_be_bytes());
            out.extend_from_slice(&entry.id.to_be_bytes());
            out.extend_from_slice(&ROOT_PARENT.to_be_bytes());
            out.push(entry.kind);
            out.extend_from_slice(&(entry.name.len() as u16).to_be_bytes());
            out.extend_from_slice(&entry.name);
            out.extend_from_slice(&entry.size.to_be_bytes());
            out.extend_from_slice(&entry.mode.to_be_bytes());
            out.extend_from_slice(&entry.mtime_sec.to_be_bytes());
            out.extend_from_slice(&entry.mtime_nsec.to_be_bytes());
            assert!(Manifest::decode(&out, limits()).is_err());
        }
    }

    #[test]
    fn valid_names_follow_wire_rules() {
        assert!(valid_name(b"hello"));
        assert!(valid_name(&[0xff]));
        assert!(!valid_name(b""));
        assert!(!valid_name(b"."));
        assert!(!valid_name(b".."));
        assert!(!valid_name(b"a/b"));
        assert!(!valid_name(b"a\0b"));
        assert!(!valid_name(&vec![b'x'; 256]));
    }

    #[test]
    fn uri_roundtrip_preserves_spaces_and_unicode() {
        let path = Path::new("/tmp/a b/\u{e4}\u{f6}");
        let uri = path_to_file_uri(path);
        assert!(!uri.contains(' '));
        let decoded = percent_decode_str(uri.trim_start_matches("file://")).collect::<Vec<_>>();
        assert_eq!(PathBuf::from(std::ffi::OsString::from_vec(decoded)), path);
    }

    #[test]
    fn encode_rejects_trailing_and_bad_parent() {
        let entry = ManifestEntry {
            id: 0,
            parent: Some(0),
            kind: KIND_FILE,
            name: b"x".to_vec(),
            size: 0,
            mode: 0o644,
            mtime_sec: 0,
            mtime_nsec: 0,
        };
        let manifest = Manifest {
            entries: vec![entry],
        };
        assert!(manifest.encode(limits()).is_err());
    }

    #[test]
    fn write_and_read_back_a_manifest_file() {
        let dir = temp_dir("io");
        let file = dir.join("f");
        let mut f = fs::File::create(&file).unwrap();
        f.write_all(b"data").unwrap();
        let built = build_manifest(&[file], limits()).unwrap();
        let bytes = built.manifest.encode(limits()).unwrap();
        assert!(bytes.len() >= 4);
        fs::remove_dir_all(&dir).unwrap();
    }
}
