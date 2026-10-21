//! Windows validation and descriptor planning for received file offers.
//!
//! The OLE virtual-file clipboard cannot use arbitrary Linux names: Windows
//! paths are case-insensitive, have reserved device names, and `FILEDESCRIPTORW`
//! stores a relative path in a fixed 260-unit field. An offer that cannot be
//! represented without renaming is rejected as a whole.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::file::broker::Broker;
use crate::file::manifest::{KIND_DIR, Manifest};

/// Maximum relative path length stored in `FILEDESCRIPTORW.cFileName`
/// (260 UTF-16 units including the terminating NUL).
pub const MAX_RELATIVE_PATH_UNITS: usize = 259;

/// Longest single name component, in UTF-16 units.
pub const MAX_COMPONENT_UNITS: usize = 255;
/// `FILEDESCRIPTORW` is 592 bytes on the Windows ABI.
const DESCRIPTOR_BYTES: usize = 592;
/// Bound the FILEGROUPDESCRIPTORW allocation independently of the manifest
/// byte cap; descriptors are much larger than compact manifest entries.
pub const MAX_DESCRIPTOR_BYTES: usize = 64 * 1024 * 1024;

/// One entry as it will be described to Windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualEntry {
    pub id: u32,
    /// Relative path with `\` separators, no NUL.
    pub relative: Vec<u16>,
    pub is_dir: bool,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub mtime_nsec: u32,
}

/// A validated virtual file set, before the broker/runtime are attached.
#[derive(Debug, Clone)]
pub struct VirtualFiles {
    pub manifest: Manifest,
    pub entries: Vec<VirtualEntry>,
}

/// Everything an OLE data object needs to serve one remote offer.
pub struct RemoteFileSet {
    pub origin: [u8; 8],
    pub serial: u64,
    pub manifest: Manifest,
    pub entries: Vec<VirtualEntry>,
    pub broker: Arc<Broker>,
    pub runtime: tokio::runtime::Handle,
}

#[derive(Debug, thiserror::Error)]
pub enum WindowsManifestError {
    #[error("entry {id}: {message}")]
    Entry { id: u32, message: String },
}

/// Validate a manifest for Windows and build the relative descriptor paths.
///
/// Rejects the whole offer on any unrepresentable name, path over
/// [`MAX_RELATIVE_PATH_UNITS`], or case-insensitive sibling collision.
pub fn validate_manifest(manifest: &Manifest) -> Result<VirtualFiles, WindowsManifestError> {
    let descriptor_bytes = manifest
        .entries
        .len()
        .saturating_mul(DESCRIPTOR_BYTES)
        .saturating_add(4);
    if descriptor_bytes > MAX_DESCRIPTOR_BYTES {
        return Err(WindowsManifestError::Entry {
            id: 0,
            message: format!(
                "descriptor block is {descriptor_bytes} bytes, over the {MAX_DESCRIPTOR_BYTES} limit"
            ),
        });
    }
    let mut paths: Vec<Option<Vec<u16>>> = vec![None; manifest.entries.len()];
    let mut sibling_names: HashMap<Option<u32>, HashSet<String>> = HashMap::new();
    let mut entries = Vec::with_capacity(manifest.entries.len());

    for entry in &manifest.entries {
        let name = std::str::from_utf8(&entry.name).map_err(|_| WindowsManifestError::Entry {
            id: entry.id,
            message: "name is not valid UTF-8".into(),
        })?;
        validate_component(name).map_err(|message| WindowsManifestError::Entry {
            id: entry.id,
            message,
        })?;

        let lower = name.to_lowercase();
        if !sibling_names.entry(entry.parent).or_default().insert(lower) {
            return Err(WindowsManifestError::Entry {
                id: entry.id,
                message: "case-insensitive name collision".into(),
            });
        }

        let name_utf16: Vec<u16> = name.encode_utf16().collect();
        let relative = match entry.parent {
            None => name_utf16,
            Some(parent) => {
                let parent_path = paths
                    .get(parent as usize)
                    .and_then(|path| path.clone())
                    .ok_or_else(|| WindowsManifestError::Entry {
                        id: entry.id,
                        message: format!("parent {parent} has no path"),
                    })?;
                let mut relative = parent_path;
                relative.push(b'\\' as u16);
                relative.extend(name_utf16);
                relative
            }
        };
        if relative.len() > MAX_RELATIVE_PATH_UNITS {
            return Err(WindowsManifestError::Entry {
                id: entry.id,
                message: format!(
                    "relative path is {} UTF-16 units, over the {MAX_RELATIVE_PATH_UNITS} limit",
                    relative.len()
                ),
            });
        }
        paths[entry.id as usize] = Some(relative.clone());
        entries.push(VirtualEntry {
            id: entry.id,
            relative,
            is_dir: entry.kind == KIND_DIR,
            size: entry.size,
            mode: entry.mode,
            mtime_sec: entry.mtime_sec,
            mtime_nsec: entry.mtime_nsec,
        });
    }

    Ok(VirtualFiles {
        manifest: manifest.clone(),
        entries,
    })
}

/// Validate one Windows path component.
pub(crate) fn validate_component(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("empty name".into());
    }
    if name == "." || name == ".." {
        return Err("dot names are not allowed".into());
    }
    if name.encode_utf16().count() > MAX_COMPONENT_UNITS {
        return Err("name exceeds 255 UTF-16 units".into());
    }
    if name.ends_with('.') || name.ends_with(' ') {
        return Err("name ends with a dot or space".into());
    }
    if let Some(ch) = name.chars().find(|ch| {
        *ch < '\u{20}' || matches!(ch, '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
    }) {
        return Err(format!("invalid character {ch:?}"));
    }
    let stem = name.split('.').next().unwrap_or(name).to_uppercase();
    if is_reserved_device(&stem) {
        return Err(format!("reserved device name {stem:?}"));
    }
    Ok(())
}

fn is_reserved_device(stem: &str) -> bool {
    const NAMES: &[&str] = &[
        "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "CLOCK$", "COM1", "COM2", "COM3", "COM4",
        "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6",
        "LPT7", "LPT8", "LPT9", "COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³",
    ];
    NAMES.iter().any(|name| name.eq_ignore_ascii_case(stem))
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use crate::file::manifest::{KIND_FILE, ManifestEntry};

    fn path(manifest: &Manifest) -> Result<VirtualFiles, WindowsManifestError> {
        validate_manifest(manifest)
    }

    fn entry(id: u32, parent: Option<u32>, name: &[u8]) -> ManifestEntry {
        ManifestEntry {
            id,
            parent,
            kind: KIND_FILE,
            name: name.to_vec(),
            size: 1,
            mode: 0o644,
            mtime_sec: 0,
            mtime_nsec: 0,
        }
    }

    #[test]
    fn accepts_unicode_and_nested_paths() {
        let mut dir = entry(0, None, b"dir");
        dir.kind = KIND_DIR;
        let manifest = Manifest {
            entries: vec![dir, entry(1, Some(0), "file \u{e4}.txt".as_bytes())],
        };
        let files = path(&manifest).unwrap();
        assert_eq!(
            files.entries[0].relative,
            "dir".encode_utf16().collect::<Vec<_>>()
        );
        assert_eq!(
            String::from_utf16(&files.entries[1].relative).unwrap(),
            "dir\\file \u{e4}.txt"
        );
    }

    #[test]
    fn rejects_invalid_and_reserved_names() {
        for name in [
            "CON",
            "con.txt",
            "COM1",
            "LPT9",
            "COM¹",
            "a:b",
            "a\\b",
            "trailing.",
            "trailing ",
            "a*b",
            "a?b",
            "a\"b",
            "a<b",
            "a>b",
            "a|b",
        ] {
            let manifest = Manifest {
                entries: vec![entry(0, None, name.as_bytes())],
            };
            assert!(path(&manifest).is_err(), "{name:?} must be rejected");
        }
    }

    #[test]
    fn rejects_case_insensitive_sibling_collisions() {
        let manifest = Manifest {
            entries: vec![entry(0, None, b"A"), entry(1, None, b"a")],
        };
        assert!(path(&manifest).is_err());
    }

    #[test]
    fn rejects_relative_paths_over_max_path() {
        let mut dir = entry(0, None, &vec![b'd'; 250]);
        dir.kind = KIND_DIR;
        let manifest = Manifest {
            entries: vec![dir, entry(1, Some(0), &[b'f'; 20])],
        };
        assert!(path(&manifest).is_err());
    }

    #[test]
    fn rejects_non_utf8_names() {
        let manifest = Manifest {
            entries: vec![entry(0, None, &[0xff, 0xfe])],
        };
        assert!(path(&manifest).is_err());
    }
}
