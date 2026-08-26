//! Shared helpers for tests that simulate external clipboard tools
//! (`wl-paste`, `wl-copy`, `cliphist`, `dbus-send`) with stub executables.
//!
//! The backends resolve these tools through `$PATH`, so a test can fully
//! replace `$PATH` with a directory of stub scripts. Replacing `PATH`
//! (rather than appending) makes availability deterministic regardless of
//! which tools happen to be installed on the host. Stub scripts set their
//! own `PATH` internally so they can still call `cat`/`printf`.

#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use std::os::unix::fs::PermissionsExt;

/// Serializes tests in this binary that mutate the process `PATH`. Rust runs
/// tests in the same binary in parallel, so PATH mutations must not overlap.
static PATH_MUTEX: Mutex<()> = Mutex::new(());

/// Acquire the global lock for PATH-mutating tests.
pub fn path_lock() -> MutexGuard<'static, ()> {
    PATH_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
}

/// A self-cleaning directory of stub executables. `PATH` is restored on drop.
pub struct StubEnv {
    pub dir: PathBuf,
    pub clip_file: PathBuf,
    pub cliphist_file: PathBuf,
    pub dbus_file: PathBuf,
    _old_path: Option<String>,
}

impl StubEnv {
    pub fn new(stubs: &[(&str, &str)]) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "lan_mouse_clipboard_stubs_{}_{}",
            std::process::id(),
            fresh_suffix()
        ));
        fs::create_dir_all(&dir).expect("create stub dir");

        for (name, body) in stubs {
            let path = dir.join(name);
            fs::write(&path, body).expect("write stub");
            let mut perms = fs::metadata(&path).expect("meta").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&path, perms).expect("chmod stub");
        }

        let clip_file = dir.join("clip.txt");
        let cliphist_file = dir.join("cliphist.txt");
        let dbus_file = dir.join("dbus.txt");

        // State files used by the stub scripts, pre-created empty.
        fs::write(&clip_file, b"").expect("clip file");
        fs::write(&cliphist_file, b"").expect("cliphist file");
        fs::write(&dbus_file, b"").expect("dbus file");

        let old_path = std::env::var_os("PATH").map(|v| v.to_string_lossy().into_owned());
        std::env::set_var("PATH", &dir);
        std::env::set_var("STUB_CLIP_FILE", &clip_file);
        std::env::set_var("STUB_CLIPHIST_FILE", &cliphist_file);
        std::env::set_var("STUB_DBUS_FILE", &dbus_file);

        Self {
            dir,
            clip_file,
            cliphist_file,
            dbus_file,
            _old_path: old_path,
        }
    }

    /// Read the contents a stub wrote to the simulated clipboard.
    pub fn read_clip(&self) -> Vec<u8> {
        fs::read(&self.clip_file).unwrap_or_default()
    }

    pub fn read_cliphist(&self) -> Vec<u8> {
        fs::read(&self.cliphist_file).unwrap_or_default()
    }

    pub fn read_dbus(&self) -> Vec<u8> {
        fs::read(&self.dbus_file).unwrap_or_default()
    }
}

impl Drop for StubEnv {
    fn drop(&mut self) {
        if let Some(old) = self._old_path.take() {
            std::env::set_var("PATH", old);
        }
        std::env::remove_var("STUB_CLIP_FILE");
        std::env::remove_var("STUB_CLIPHIST_FILE");
        std::env::remove_var("STUB_DBUS_FILE");
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn fresh_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:?}")
}

/// A `wl-paste` stub. Ignores arguments for read; writes through to the
/// shared simulated clipboard file. Exit code 1 when there is no content.
pub const WL_PASTE_STUB: &str = r#"#!/bin/sh
export PATH="/usr/bin:/bin"
case "$*" in
  *"--watch"*) cat "$STUB_CLIP_FILE"; exit 0;;
esac
if [ ! -s "$STUB_CLIP_FILE" ]; then exit 1; fi
cat "$STUB_CLIP_FILE"
"#;

/// A `wl-copy` stub that stores stdin into the simulated clipboard file.
pub const WL_COPY_STUB: &str = r#"#!/bin/sh
export PATH="/usr/bin:/bin"
cat > "$STUB_CLIP_FILE"
"#;

/// A `cliphist` stub that records `store --mime <mime>` stdin.
pub const CLIPHIST_STUB: &str = r#"#!/bin/sh
export PATH="/usr/bin:/bin"
cat > "$STUB_CLIPHIST_FILE"
"#;

/// A `dbus-send` stub: `getClipboardContents` prints the stored string,
/// `setClipboardContents string:<text>` stores it.
pub const DBUS_SEND_STUB: &str = r#"#!/bin/sh
export PATH="/usr/bin:/bin"
case "$*" in
  *"getClipboardContents"*)
    printf '   string "%s"\n' "$(cat "$STUB_DBUS_FILE")"
    exit 0;;
  *"setClipboardContents"*)
    for a in "$@"; do
      case "$a" in
        string:*) printf '%s' "${a#string:}" > "$STUB_DBUS_FILE"; exit 0;;
      esac
    done
    exit 0;;
esac
exit 0
"#;

/// Convenience to run a block while holding the PATH lock.
pub fn with_path_lock<T>(f: impl FnOnce() -> T) -> T {
    let _guard = path_lock();
    f()
}

/// A tiny deterministic PRNG (xorshift64) so randomized tests are repeatable
/// without pulling in a RNG dependency.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    pub fn range(&mut self, hi: usize) -> usize {
        (self.next() % hi.max(1) as u64) as usize
    }
}
