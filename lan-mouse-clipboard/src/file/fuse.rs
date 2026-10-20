//! Read-only FUSE view of lazily fetched file offers.
//!
//! One mount is kept for the daemon's lifetime. Each accepted offer becomes a
//! generation directory; entries are served from the in-memory manifest while
//! file bytes are read in 1 MiB chunks through the [`Broker`].

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use fuser::{
    AccessFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, MountOption, OpenAccMode, OpenFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    SessionACL,
};

use super::manifest::{Manifest, ManifestEntry, READ_CHUNK_SIZE, ROOT_PARENT};
use crate::file::broker::Broker;

const ROOT_INO: u64 = 1;
const GEN_DIR_MARKER: u32 = u32::MAX;
const TTL: Duration = Duration::from_secs(1);
const MAX_GENERATIONS: usize = 4;
const MAX_GENERATION_BYTES: usize = 64 * 1024 * 1024;
const CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ChunkKey {
    generation: u64,
    entry: u32,
    chunk: u64,
}

struct GenerationState {
    origin: [u8; 8],
    serial: u64,
    dir_name: String,
    manifest: Manifest,
    /// Visible top-level names (originals, with ` (2)` aliases on collision).
    top_names: HashMap<u32, Vec<u8>>,
    /// Ordered child entry ids per parent; top-level uses `ROOT_PARENT`.
    children: HashMap<u32, Vec<u32>>,
    /// Visible name to child id per parent; top-level uses `ROOT_PARENT`.
    names: HashMap<u32, HashMap<Vec<u8>, u32>>,
    open_handles: usize,
    encoded_len: usize,
}

struct OpenHandle {
    generation: u64,
    entry: u32,
    is_dir: bool,
}

struct ChunkCache {
    entries: HashMap<ChunkKey, Arc<Vec<u8>>>,
    order: VecDeque<ChunkKey>,
    bytes: usize,
}

impl ChunkCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
        }
    }

    fn get(&mut self, key: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        let data = self.entries.get(key).cloned()?;
        self.order.retain(|k| k != key);
        self.order.push_back(*key);
        Some(data)
    }

    fn insert(&mut self, key: ChunkKey, data: Arc<Vec<u8>>) {
        if let Some(old) = self.entries.insert(key, data.clone()) {
            self.bytes = self.bytes.saturating_sub(old.len());
            self.order.retain(|k| *k != key);
        }
        self.bytes += data.len();
        self.order.push_back(key);
        while self.bytes > CACHE_BYTES {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(old.len());
            }
        }
    }

    fn drop_generation(&mut self, generation: u64) {
        self.order.retain(|k| {
            if k.generation == generation {
                if let Some(old) = self.entries.remove(k) {
                    self.bytes = self.bytes.saturating_sub(old.len());
                }
                false
            } else {
                true
            }
        });
    }
}

struct FsState {
    generations: BTreeMap<u64, GenerationState>,
    order: VecDeque<u64>,
    next_generation: u64,
    next_handle: u64,
    handles: HashMap<u64, OpenHandle>,
    cache: ChunkCache,
}

impl FsState {
    fn new() -> Self {
        Self {
            generations: BTreeMap::new(),
            order: VecDeque::new(),
            next_generation: 1,
            next_handle: 1,
            handles: HashMap::new(),
            cache: ChunkCache::new(),
        }
    }

    fn generation(&self, id: u64) -> Option<&GenerationState> {
        self.generations.get(&id)
    }

    fn generation_mut(&mut self, id: u64) -> Option<&mut GenerationState> {
        self.generations.get_mut(&id)
    }

    /// Drop generations while over the count or byte cap. Open generations are
    /// skipped, not treated as a stop sign: a long-lived read must not pin an
    /// unbounded number of newer, closed generations. The newest generation is
    /// the one currently on the clipboard, so it is never a victim. Root is
    /// never a generation, so nothing outside the view is ever touched.
    fn gc(&mut self) {
        loop {
            let total: usize = self.generations.values().map(|g| g.encoded_len).sum();
            let too_many = self.generations.len() > MAX_GENERATIONS;
            let too_large = total > MAX_GENERATION_BYTES;
            if !too_many && !too_large {
                return;
            }
            self.order.retain(|id| self.generations.contains_key(id));
            let current = self.order.back().copied();
            let Some(victim) = self.order.iter().copied().find(|id| {
                Some(*id) != current
                    && self
                        .generations
                        .get(id)
                        .is_some_and(|generation| generation.open_handles == 0)
            }) else {
                return;
            };
            self.order.retain(|id| *id != victim);
            self.generations.remove(&victim);
            self.cache.drop_generation(victim);
        }
    }
}

/// A running FUSE mount plus the shared generation state.
pub struct ClipboardMount {
    mount_point: PathBuf,
    state: Arc<Mutex<FsState>>,
    session: Mutex<Option<fuser::BackgroundSession>>,
}

impl ClipboardMount {
    /// Mount the lazy view, or return `None` when FUSE is unavailable.
    pub fn start(
        broker: Arc<Broker>,
        runtime: tokio::runtime::Handle,
    ) -> Option<Arc<ClipboardMount>> {
        let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") else {
            log::warn!("file sync: XDG_RUNTIME_DIR is unset; lazy file view disabled");
            return None;
        };
        let mount_point = PathBuf::from(runtime_dir)
            .join("lan-mouse")
            .join("clipboard");
        Self::start_at(mount_point, broker, runtime)
    }

    fn start_at(
        mount_point: PathBuf,
        broker: Arc<Broker>,
        runtime: tokio::runtime::Handle,
    ) -> Option<Arc<ClipboardMount>> {
        let base = mount_point.parent()?.to_path_buf();
        if let Err(e) = std::fs::create_dir_all(&base) {
            log::warn!("file sync: cannot create {}: {e}", base.display());
            return None;
        }
        set_private_dir(&base);
        if let Err(e) = std::fs::create_dir_all(&mount_point) {
            log::warn!("file sync: cannot create {}: {e}", mount_point.display());
            return None;
        }
        let mount_point = std::fs::canonicalize(&mount_point).ok()?;
        set_private_dir(&mount_point);

        if is_mounted(&mount_point) {
            log::info!("file sync: clearing stale clipboard mount");
            force_unmount(&mount_point);
        }
        match std::fs::read_dir(&mount_point) {
            Ok(entries) => {
                if entries.count() != 0 {
                    log::warn!(
                        "file sync: {} is not empty; lazy file view disabled",
                        mount_point.display()
                    );
                    return None;
                }
            }
            Err(e) => {
                log::warn!("file sync: cannot inspect {}: {e}", mount_point.display());
                return None;
            }
        }

        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        let state = Arc::new(Mutex::new(FsState::new()));
        let fs = ClipboardFs {
            state: state.clone(),
            broker,
            runtime,
            uid,
            gid,
        };
        let mut config = Config::default();
        config.acl = SessionACL::Owner;
        config.n_threads = Some(4);
        config.mount_options = vec![
            MountOption::RO,
            MountOption::NoSuid,
            MountOption::NoDev,
            MountOption::FSName("lan-mouse".into()),
        ];
        let session = match fuser::Session::new(fs, &mount_point, &config) {
            Ok(session) => session,
            Err(e) => {
                log::warn!(
                    "file sync: cannot mount {}: {e}; lazy file view disabled",
                    mount_point.display()
                );
                return None;
            }
        };
        match session.spawn() {
            Ok(background) => {
                log::info!("file sync: lazy view mounted at {}", mount_point.display());
                Some(Arc::new(ClipboardMount {
                    mount_point,
                    state,
                    session: Mutex::new(Some(background)),
                }))
            }
            Err(e) => {
                log::warn!("file sync: cannot start FUSE session: {e}");
                None
            }
        }
    }

    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Create a generation for an accepted offer and return the absolute
    /// paths of its top-level entries.
    pub fn add_generation(
        &self,
        origin: [u8; 8],
        serial: u64,
        manifest: Manifest,
    ) -> Option<Vec<PathBuf>> {
        let dir_name = generation_dir_name(origin, serial);
        let mut state = self.state.lock().unwrap();
        if state
            .generations
            .values()
            .any(|gen| gen.origin == origin && gen.serial == serial)
        {
            return None;
        }
        let id = state.next_generation;
        state.next_generation = state.next_generation.wrapping_add(1).max(2);

        // Evict before inserting so a new, currently-clipboard generation can
        // never be its own eviction victim when older generations are open.
        state.gc();

        // Build the lookup index and assign top-level aliases in one pass.
        // A linear alias scan per entry would be O(n^2) for a large flat offer.
        let mut used: HashSet<Vec<u8>> = HashSet::new();
        let mut top_names = HashMap::new();
        let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut names: HashMap<u32, HashMap<Vec<u8>, u32>> = HashMap::new();
        for entry in &manifest.entries {
            let parent = entry.parent.unwrap_or(ROOT_PARENT);
            children.entry(parent).or_default().push(entry.id);
            let visible = if entry.parent.is_none() {
                let visible = unique_name(&entry.name, &mut used);
                top_names.insert(entry.id, visible.clone());
                visible
            } else {
                entry.name.clone()
            };
            names.entry(parent).or_default().insert(visible, entry.id);
        }
        let encoded_len = manifest.estimated_size();
        state.generations.insert(
            id,
            GenerationState {
                origin,
                serial,
                dir_name: dir_name.clone(),
                manifest,
                top_names,
                children,
                names,
                open_handles: 0,
                encoded_len,
            },
        );
        state.order.push_back(id);

        let gen = state.generation(id)?;
        let mut paths = Vec::new();
        for entry in gen
            .manifest
            .entries
            .iter()
            .filter(|entry| entry.parent.is_none())
        {
            let name = gen.top_names.get(&entry.id)?;
            paths.push(
                self.mount_point
                    .join(&dir_name)
                    .join(OsStr::from_bytes(name)),
            );
        }
        Some(paths)
    }

    /// Whether a local path points inside this mount.
    pub fn contains_path(&self, path: &Path) -> bool {
        path.starts_with(&self.mount_point)
    }

    /// Drop a generation that was never successfully applied. It is ignored
    /// while handles are open, because a paste may already be reading it.
    pub fn remove_generation(&self, origin: [u8; 8], serial: u64) -> bool {
        let dir_name = generation_dir_name(origin, serial);
        let mut state = self.state.lock().unwrap();
        let Some(id) = state
            .generations
            .iter()
            .find_map(|(id, gen)| (gen.dir_name == dir_name).then_some(*id))
        else {
            return false;
        };
        if state
            .generations
            .get(&id)
            .is_some_and(|gen| gen.open_handles > 0)
        {
            return false;
        }
        state.generations.remove(&id);
        state.order.retain(|g| *g != id);
        state.cache.drop_generation(id);
        true
    }

    /// Remove finished generations, if they are over a cap.
    pub fn gc(&self) {
        self.state.lock().unwrap().gc();
    }
}

impl Drop for ClipboardMount {
    fn drop(&mut self) {
        // Force a lazy unmount instead of umount_and_join: this Drop can run
        // on the runtime thread while a FUSE read is waiting for that same
        // runtime, so joining the fuser thread could deadlock. Dropping the
        // session detaches the guard; Mount::drop makes a best-effort normal
        // unmount without joining.
        force_unmount(&self.mount_point);
        if let Some(session) = self.session.lock().unwrap().take() {
            drop(session);
        }
    }
}

fn set_private_dir(path: &Path) {
    if let Ok(meta) = std::fs::metadata(path) {
        let mut perms = meta.permissions();
        perms.set_mode(0o700);
        let _ = std::fs::set_permissions(path, perms);
    }
}

fn is_mounted(path: &Path) -> bool {
    let Ok(info) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    let needle = path.to_string_lossy();
    info.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|mount| mount == needle)
    })
}

fn force_unmount(path: &Path) {
    let _ = Command::new("fusermount3")
        .arg("-u")
        .arg("-z")
        .arg(path)
        .status();
}

fn generation_dir_name(origin: [u8; 8], serial: u64) -> String {
    let hex: String = origin.iter().map(|b| format!("{b:02x}")).collect();
    format!("{hex}-{serial}")
}

fn unique_name(base: &[u8], used: &mut HashSet<Vec<u8>>) -> Vec<u8> {
    if used.insert(base.to_vec()) {
        return base.to_vec();
    }
    for n in 2u32.. {
        let suffix = format!(" ({n})");
        let max = 255usize.saturating_sub(suffix.len());
        let mut candidate = base[..base.len().min(max)].to_vec();
        candidate.extend_from_slice(suffix.as_bytes());
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    base.to_vec()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Node {
    Root,
    Generation(u64),
    Entry(u64, u32),
}

fn decode_ino(ino: u64) -> Option<Node> {
    if ino == ROOT_INO {
        return Some(Node::Root);
    }
    let generation = ino >> 32;
    if generation == 0 {
        return None;
    }
    let low = ino as u32;
    if low == GEN_DIR_MARKER {
        Some(Node::Generation(generation))
    } else {
        Some(Node::Entry(generation, low))
    }
}

fn generation_dir_ino(generation: u64) -> u64 {
    ((generation & 0xFFFF_FFFF) << 32) | GEN_DIR_MARKER as u64
}

fn entry_ino(generation: u64, entry: u32) -> u64 {
    ((generation & 0xFFFF_FFFF) << 32) | entry as u64
}

struct ClipboardFs {
    state: Arc<Mutex<FsState>>,
    broker: Arc<Broker>,
    runtime: tokio::runtime::Handle,
    uid: u32,
    gid: u32,
}

impl ClipboardFs {
    fn attr(&self, ino: u64, node: Node) -> Option<FileAttr> {
        let state = self.state.lock().unwrap();
        let (kind, perm, size, mtime, nlink) = match node {
            Node::Root => (FileType::Directory, 0o500, 0, SystemTime::UNIX_EPOCH, 2),
            Node::Generation(id) => {
                state.generation(id)?;
                (FileType::Directory, 0o500, 0, SystemTime::UNIX_EPOCH, 2)
            }
            Node::Entry(id, entry_id) => {
                let gen = state.generation(id)?;
                let entry = gen.manifest.entries.get(entry_id as usize)?;
                let kind = if entry.is_dir() {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };
                (
                    kind,
                    (entry.mode & 0o777) as u16,
                    entry.size,
                    mtime_of(entry),
                    if entry.is_dir() { 2 } else { 1 },
                )
            }
        };
        Some(FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: SystemTime::UNIX_EPOCH,
            kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }

    fn lookup_child(&self, parent: Node, name: &[u8]) -> Option<(u64, Node)> {
        let state = self.state.lock().unwrap();
        match parent {
            Node::Root => {
                let gen_id = *state
                    .generations
                    .iter()
                    .find_map(|(id, gen)| (gen.dir_name.as_bytes() == name).then_some(id))?;
                Some((generation_dir_ino(gen_id), Node::Generation(gen_id)))
            }
            Node::Generation(id) => {
                let gen = state.generation(id)?;
                let child = *gen.names.get(&ROOT_PARENT)?.get(name)?;
                Some((entry_ino(id, child), Node::Entry(id, child)))
            }
            Node::Entry(id, entry_id) => {
                let gen = state.generation(id)?;
                let parent = gen.manifest.entries.get(entry_id as usize)?;
                if !parent.is_dir() {
                    return None;
                }
                let child = *gen.names.get(&entry_id)?.get(name)?;
                Some((entry_ino(id, child), Node::Entry(id, child)))
            }
        }
    }

    /// Validate the node, check its type, and pin its generation under one
    /// lock so a concurrent GC cannot evict it between check and open.
    fn open_node(&self, node: Node, want_dir: bool) -> Result<u64, Errno> {
        let mut state = self.state.lock().unwrap();
        let (generation, entry, is_dir) = match node {
            Node::Root => (0, 0, true),
            Node::Generation(id) => {
                if state.generation(id).is_none() {
                    return Err(Errno::ENOENT);
                }
                (id, GEN_DIR_MARKER, true)
            }
            Node::Entry(id, entry_id) => {
                let Some(generation) = state.generation(id) else {
                    return Err(Errno::ENOENT);
                };
                let Some(entry) = generation.manifest.entries.get(entry_id as usize) else {
                    return Err(Errno::ENOENT);
                };
                (id, entry_id, entry.is_dir())
            }
        };
        if is_dir != want_dir {
            return Err(if want_dir {
                Errno::ENOTDIR
            } else {
                Errno::EISDIR
            });
        }
        let fh = state.next_handle;
        state.next_handle = fh.wrapping_add(1).max(1);
        state.handles.insert(
            fh,
            OpenHandle {
                generation,
                entry,
                is_dir,
            },
        );
        if generation != 0 {
            let Some(gen) = state.generation_mut(generation) else {
                state.handles.remove(&fh);
                return Err(Errno::ENOENT);
            };
            gen.open_handles += 1;
        }
        Ok(fh)
    }

    fn release_handle(&self, fh: u64, is_dir: bool) {
        let mut state = self.state.lock().unwrap();
        let Some(handle) = state.handles.get(&fh) else {
            return;
        };
        if handle.is_dir != is_dir {
            return;
        }
        let generation = handle.generation;
        state.handles.remove(&fh);
        if let Some(gen) = state.generation_mut(generation) {
            gen.open_handles = gen.open_handles.saturating_sub(1);
        }
        state.gc();
    }

    fn read_dir_entries(&self, node: Node) -> Option<Vec<(u64, FileType, Vec<u8>)>> {
        let state = self.state.lock().unwrap();
        let mut out = Vec::new();
        match node {
            Node::Root => {
                for id in &state.order {
                    let Some(gen) = state.generation(*id) else {
                        continue;
                    };
                    out.push((
                        generation_dir_ino(*id),
                        FileType::Directory,
                        gen.dir_name.as_bytes().to_vec(),
                    ));
                }
            }
            Node::Generation(id) => {
                let gen = state.generation(id)?;
                for child_id in gen.children.get(&ROOT_PARENT).into_iter().flatten() {
                    let entry = gen.manifest.entries.get(*child_id as usize)?;
                    let name = gen.top_names.get(child_id)?;
                    out.push((entry_ino(id, *child_id), file_type(entry), name.clone()));
                }
            }
            Node::Entry(id, entry_id) => {
                let gen = state.generation(id)?;
                let entry = gen.manifest.entries.get(entry_id as usize)?;
                if !entry.is_dir() {
                    return None;
                }
                for child_id in gen.children.get(&entry_id).into_iter().flatten() {
                    let child = gen.manifest.entries.get(*child_id as usize)?;
                    out.push((
                        entry_ino(id, *child_id),
                        file_type(child),
                        child.name.clone(),
                    ));
                }
            }
        }
        Some(out)
    }
}

fn file_type(entry: &ManifestEntry) -> FileType {
    if entry.is_dir() {
        FileType::Directory
    } else {
        FileType::RegularFile
    }
}

fn mtime_of(entry: &ManifestEntry) -> SystemTime {
    let nanos = entry.mtime_nsec.min(999_999_999);
    if entry.mtime_sec >= 0 {
        SystemTime::UNIX_EPOCH + Duration::new(entry.mtime_sec as u64, nanos)
    } else {
        let base = SystemTime::UNIX_EPOCH
            .checked_sub(Duration::from_secs(entry.mtime_sec.unsigned_abs()))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        base.checked_add(Duration::new(0, nanos)).unwrap_or(base)
    }
}

impl Filesystem for ClipboardFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_node) = decode_ino(parent.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some((ino, node)) = self.lookup_child(parent_node, name.as_bytes()) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(attr) = self.attr(ino, node) else {
            reply.error(Errno::ENOENT);
            return;
        };
        reply.entry(&TTL, &attr, Generation(0));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(node) = decode_ino(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.attr(ino.0, node) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            reply.error(Errno::EROFS);
            return;
        }
        let Some(node) = decode_ino(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.open_node(node, false) {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(e) => reply.error(e),
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let (generation, entry_id) = {
            let state = self.state.lock().unwrap();
            let Some(handle) = state.handles.get(&fh.0) else {
                reply.error(Errno::EBADF);
                return;
            };
            if handle.is_dir {
                reply.error(Errno::EISDIR);
                return;
            }
            (handle.generation, handle.entry)
        };
        let (origin, serial, entry) = {
            let state = self.state.lock().unwrap();
            let Some(gen) = state.generation(generation) else {
                reply.error(Errno::EIO);
                return;
            };
            let Some(entry) = gen.manifest.entries.get(entry_id as usize) else {
                reply.error(Errno::EIO);
                return;
            };
            (gen.origin, gen.serial, entry.clone())
        };
        if offset >= entry.size || size == 0 {
            reply.data(&[]);
            return;
        }
        let end = entry.size.min(offset.saturating_add(size as u64));
        let chunk_size = READ_CHUNK_SIZE as u64;
        let first = offset / chunk_size;
        let last = (end - 1) / chunk_size;
        let mut out = Vec::with_capacity((end - offset) as usize);
        for chunk in first..=last {
            let key = ChunkKey {
                generation,
                entry: entry_id,
                chunk,
            };
            let cached = self.state.lock().unwrap().cache.get(&key);
            let data = match cached {
                Some(data) => data,
                None => {
                    let chunk_offset = chunk * chunk_size;
                    let want = entry.size.saturating_sub(chunk_offset).min(chunk_size) as u32;
                    let result = self.runtime.block_on(self.broker.read(
                        origin,
                        serial,
                        entry_id,
                        chunk_offset,
                        want,
                    ));
                    match result {
                        Ok(data) => {
                            let data = Arc::new(data);
                            self.state.lock().unwrap().cache.insert(key, data.clone());
                            data
                        }
                        Err(e) => {
                            log::debug!(
                                "file sync: lazy read {generation}:{entry_id} at {chunk_offset} failed: {e}"
                            );
                            reply.error(Errno::EIO);
                            return;
                        }
                    }
                }
            };
            let chunk_offset = chunk * chunk_size;
            let from = offset.max(chunk_offset).saturating_sub(chunk_offset) as usize;
            let to = end
                .min(chunk_offset + data.len() as u64)
                .saturating_sub(chunk_offset) as usize;
            if from < to && from < data.len() {
                out.extend_from_slice(&data[from..to.min(data.len())]);
            }
        }
        reply.data(&out);
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.release_handle(fh.0, false);
        reply.ok();
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EROFS);
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let Some(node) = decode_ino(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.open_node(node, true) {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let Some(node) = decode_ino(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(entries) = self.read_dir_entries(node) else {
            reply.error(Errno::ENOTDIR);
            return;
        };
        let parent_ino = match node {
            Node::Root => ROOT_INO,
            Node::Generation(_) => ROOT_INO,
            Node::Entry(generation, entry_id) => {
                let state = self.state.lock().unwrap();
                let parent = state
                    .generation(generation)
                    .and_then(|g| g.manifest.entries.get(entry_id as usize))
                    .and_then(|e| e.parent);
                match parent {
                    Some(parent) => entry_ino(generation, parent),
                    None => generation_dir_ino(generation),
                }
            }
        };
        let mut items: Vec<(u64, FileType, Vec<u8>)> = vec![
            (ino.0, FileType::Directory, b".".to_vec()),
            (parent_ino, FileType::Directory, b"..".to_vec()),
        ];
        items.extend(entries);
        for (index, (child_ino, kind, name)) in items.into_iter().enumerate().skip(offset as usize)
        {
            let next = (index + 1) as u64;
            if reply.add(INodeNo(child_ino), next, kind, OsStr::from_bytes(&name)) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.release_handle(fh.0, true);
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let state = self.state.lock().unwrap();
        let files: u64 = state
            .generations
            .values()
            .map(|g| g.manifest.entries.len() as u64 + 1)
            .sum();
        reply.statfs(0, 0, 0, files, 0, 4096, 255, 4096);
    }

    fn access(&self, _req: &Request, ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if mask.contains(AccessFlags::W_OK) {
            reply.error(Errno::EACCES);
            return;
        }
        match decode_ino(ino.0) {
            Some(Node::Root) => reply.ok(),
            Some(Node::Generation(id)) => {
                if self.state.lock().unwrap().generation(id).is_some() {
                    reply.ok();
                } else {
                    reply.error(Errno::ENOENT);
                }
            }
            Some(Node::Entry(id, entry_id)) => {
                let exists = self
                    .state
                    .lock()
                    .unwrap()
                    .generation(id)
                    .and_then(|g| g.manifest.entries.get(entry_id as usize))
                    .is_some();
                if exists {
                    reply.ok();
                } else {
                    reply.error(Errno::ENOENT);
                }
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        reply.error(Errno::EROFS);
    }

    fn mknod(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn mkdir(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _newparent: INodeNo,
        _newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EROFS);
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        reply.error(Errno::EROFS);
    }

    fn create(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(Errno::EROFS);
    }

    fn readlink(&self, _req: &Request, _ino: INodeNo, reply: ReplyData) {
        reply.error(Errno::EINVAL);
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EROFS);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::manifest::KIND_FILE;

    #[test]
    fn inode_encoding_roundtrips() {
        for node in [
            Node::Root,
            Node::Generation(7),
            Node::Entry(7, 3),
            Node::Entry(1, 0),
        ] {
            let ino = match node {
                Node::Root => ROOT_INO,
                Node::Generation(id) => generation_dir_ino(id),
                Node::Entry(id, entry) => entry_ino(id, entry),
            };
            assert_eq!(decode_ino(ino), Some(node));
        }
    }

    #[test]
    fn aliases_are_unique_and_bounded() {
        let long = vec![b'a'; 255];
        let mut used: HashSet<Vec<u8>> = [b"a".to_vec(), long.clone()].into_iter().collect();
        let alias = unique_name(&long, &mut used);
        assert!(alias.len() <= 255);
        assert_ne!(alias, long);
        let alias2 = unique_name(b"a", &mut used);
        assert_eq!(alias2, b"a (2)".to_vec());
    }

    fn dir_names(path: &Path) -> Vec<String> {
        std::fs::read_dir(path)
            .expect("read_dir")
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect()
    }

    fn add_generation_for_test(state: &mut FsState, id: u64, open: usize) {
        state.generations.insert(
            id,
            GenerationState {
                origin: [id as u8; 8],
                serial: id,
                dir_name: format!("gen-{id}"),
                manifest: Manifest {
                    entries: Vec::new(),
                },
                top_names: HashMap::new(),
                children: HashMap::new(),
                names: HashMap::new(),
                open_handles: open,
                encoded_len: 1024,
            },
        );
        state.order.push_back(id);
    }

    #[test]
    fn gc_evicts_closed_generations_and_spares_open_ones() {
        let mut state = FsState::new();
        for id in 1..=5 {
            add_generation_for_test(&mut state, id, 0);
        }
        state.gc();
        assert_eq!(state.generations.len(), MAX_GENERATIONS);
        assert!(!state.generations.contains_key(&1));

        // An open oldest generation is skipped; older closed generations are
        // still evicted down to the cap.
        for id in 1..=5 {
            state.generations.remove(&id);
        }
        state.order.clear();
        add_generation_for_test(&mut state, 10, 1);
        for id in 11..=15 {
            add_generation_for_test(&mut state, id, 0);
        }
        state.gc();
        assert!(state.generations.contains_key(&10));
        assert!(!state.generations.contains_key(&11));
        assert_eq!(state.generations.len(), MAX_GENERATIONS);
    }

    #[test]
    fn gc_never_evicts_the_current_generation() {
        let mut state = FsState::new();
        for id in 1..=4 {
            add_generation_for_test(&mut state, id, 1);
        }
        add_generation_for_test(&mut state, 5, 0);
        state.gc();
        assert!(
            state.generations.contains_key(&5),
            "the just-set generation must stay"
        );
        assert_eq!(state.generations.len(), 5);

        // Once an older generation is closed it becomes evictable again.
        state.generations.get_mut(&1).unwrap().open_handles = 0;
        state.gc();
        assert!(!state.generations.contains_key(&1));
        assert!(state.generations.contains_key(&5));
    }

    #[test]
    fn gc_drops_cached_chunks_with_the_generation() {
        let mut state = FsState::new();
        state.generations.insert(
            1,
            GenerationState {
                origin: [1; 8],
                serial: 1,
                dir_name: "gen-1".into(),
                manifest: Manifest {
                    entries: Vec::new(),
                },
                top_names: HashMap::new(),
                children: HashMap::new(),
                names: HashMap::new(),
                open_handles: 0,
                encoded_len: 1,
            },
        );
        state.order.push_back(1);
        state.cache.insert(
            ChunkKey {
                generation: 1,
                entry: 0,
                chunk: 0,
            },
            Arc::new(vec![1, 2, 3]),
        );
        for id in 2..=5 {
            add_generation_for_test(&mut state, id, 0);
        }
        state.gc();
        assert!(!state.cache.entries.contains_key(&ChunkKey {
            generation: 1,
            entry: 0,
            chunk: 0,
        }));
    }

    #[tokio::test]
    async fn open_node_pins_type_and_generation_atomically() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let fs = ClipboardFs {
            state: Arc::new(Mutex::new(FsState::new())),
            broker: Arc::new(Broker::new(tx)),
            runtime: tokio::runtime::Handle::current(),
            uid: 0,
            gid: 0,
        };
        let manifest = Manifest {
            entries: vec![
                ManifestEntry {
                    id: 0,
                    parent: None,
                    kind: KIND_FILE,
                    name: b"file".to_vec(),
                    size: 1,
                    mode: 0o644,
                    mtime_sec: 0,
                    mtime_nsec: 0,
                },
                ManifestEntry {
                    id: 1,
                    parent: None,
                    kind: crate::file::manifest::KIND_DIR,
                    name: b"dir".to_vec(),
                    size: 0,
                    mode: 0o755,
                    mtime_sec: 0,
                    mtime_nsec: 0,
                },
            ],
        };
        {
            let mut state = fs.state.lock().unwrap();
            state.generations.insert(
                1,
                GenerationState {
                    origin: [1; 8],
                    serial: 1,
                    dir_name: "gen-1".into(),
                    manifest,
                    top_names: HashMap::new(),
                    children: HashMap::new(),
                    names: HashMap::new(),
                    open_handles: 0,
                    encoded_len: 10,
                },
            );
            state.order.push_back(1);
        }

        let fh = fs.open_node(Node::Entry(1, 0), false).expect("file opens");
        assert_eq!(
            fs.state.lock().unwrap().generation(1).unwrap().open_handles,
            1
        );
        fs.release_handle(fh, false);
        assert_eq!(
            fs.state.lock().unwrap().generation(1).unwrap().open_handles,
            0
        );

        assert!(matches!(
            fs.open_node(Node::Entry(1, 1), false),
            Err(Errno::EISDIR)
        ));
        assert!(matches!(
            fs.open_node(Node::Entry(1, 0), true),
            Err(Errno::ENOTDIR)
        ));
        assert!(matches!(
            fs.open_node(Node::Entry(9, 0), false),
            Err(Errno::ENOENT)
        ));
        assert!(matches!(
            fs.open_node(Node::Root, false),
            Err(Errno::EISDIR)
        ));
        fs.release_handle(fs.open_node(Node::Root, true).expect("root opens"), true);
    }

    #[test]
    fn generation_names_are_stable() {
        assert_eq!(
            generation_dir_name([0x0a, 0xff, 0, 0, 0, 0, 0, 1], 42),
            "0aff000000000001-42"
        );
    }

    /// Mount smoke test. Skipped silently when FUSE is not usable.
    ///
    /// Uses a current-thread runtime, like the daemon, with the runtime driven
    /// on a separate thread while the FUSE callback bridges through
    /// `Handle::block_on`.
    #[test]
    fn mount_smoke_reads_a_lazy_file() {
        if !Path::new("/dev/fuse").exists()
            || Command::new("fusermount3")
                .arg("--version")
                .output()
                .is_err()
        {
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.handle().clone();
        let (directed_tx, mut directed_rx) = tokio::sync::mpsc::channel(8);
        let broker = Arc::new(Broker::new(directed_tx));
        let responder_broker = broker.clone();
        let contents = b"hello lazy file".to_vec();
        let responder_contents = contents.clone();
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let runtime_thread = std::thread::spawn(move || {
            runtime.block_on(async move {
                loop {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        message = directed_rx.recv() => {
                            let Some((origin, frame)) = message else { break };
                            let Ok(crate::protocol::Message::FileRead(read)) =
                                crate::protocol::Message::decode(&frame[4..])
                            else {
                                continue;
                            };
                            let start = (read.offset as usize).min(responder_contents.len());
                            let end =
                                (start + read.length as usize).min(responder_contents.len());
                            responder_broker.complete(
                                origin,
                                crate::protocol::FileData {
                                    request_id: read.request_id,
                                    offset: read.offset,
                                    data: responder_contents[start..end].to_vec(),
                                },
                            );
                        }
                    }
                }
            });
        });

        let dir = std::env::temp_dir().join(format!(
            "lan_mouse_fuse_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let Some(mount) = ClipboardMount::start_at(dir.join("mnt"), broker.clone(), handle) else {
            let _ = stop_tx.send(());
            let _ = runtime_thread.join();
            let _ = std::fs::remove_dir_all(&dir);
            return;
        };
        let manifest = Manifest {
            entries: vec![ManifestEntry {
                id: 0,
                parent: None,
                kind: KIND_FILE,
                name: b"hello.txt".to_vec(),
                size: contents.len() as u64,
                mode: 0o644,
                mtime_sec: 0,
                mtime_nsec: 0,
            }],
        };
        let paths = mount
            .add_generation([7; 8], 1, manifest.clone())
            .expect("generation");
        let root_names = dir_names(mount.mount_point());
        assert!(root_names.contains(&generation_dir_name([7; 8], 1)));
        assert_eq!(paths[0].file_name(), Some(OsStr::from_bytes(b"hello.txt")));
        assert_eq!(
            dir_names(paths[0].parent().expect("generation dir")),
            vec!["hello.txt".to_string()]
        );

        // A new generation has its own directory, so the same name does not
        // collide across generations.
        let second = mount
            .add_generation([7; 8], 2, manifest.clone())
            .expect("second generation");
        assert_eq!(second[0].file_name(), Some(OsStr::from_bytes(b"hello.txt")));
        assert!(dir_names(mount.mount_point()).contains(&generation_dir_name([7; 8], 2)));

        // Two top-level entries with the same name in one offer are aliased.
        let mut duplicate = manifest.clone();
        duplicate.entries.push(ManifestEntry {
            id: 1,
            parent: None,
            kind: KIND_FILE,
            name: b"hello.txt".to_vec(),
            size: contents.len() as u64,
            mode: 0o644,
            mtime_sec: 0,
            mtime_nsec: 0,
        });
        let aliased = mount
            .add_generation([7; 8], 3, duplicate)
            .expect("aliased generation");
        assert_eq!(
            aliased[0].file_name(),
            Some(OsStr::from_bytes(b"hello.txt"))
        );
        assert_eq!(
            aliased[1].file_name(),
            Some(OsStr::from_bytes(b"hello.txt (2)"))
        );
        assert_eq!(
            dir_names(aliased[0].parent().expect("aliased generation dir")),
            vec!["hello.txt".to_string(), "hello.txt (2)".to_string()]
        );

        let path = paths[0].clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(std::fs::read(&path));
        });
        let read = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("FUSE read timed out")
            .expect("read");
        assert_eq!(read, contents);

        // A failed set removes its generation; the root and old paths stay.
        assert!(mount.remove_generation([7; 8], 2));
        assert!(!dir_names(mount.mount_point()).contains(&generation_dir_name([7; 8], 2)));
        assert!(mount.mount_point().exists());

        drop(mount);
        drop(broker);
        let _ = stop_tx.send(());
        let _ = runtime_thread.join();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
