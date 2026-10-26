use super::StoreError;
use crate::at_rest::{AtRest, Records};
use crate::file_lock::FileLock;
use std::{
    fs::{self, File},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

#[cfg(all(test, unix))]
#[path = "lock_tests.rs"]
mod lock_tests;

const MARKER: &str = "INITIALIZED";
const INITIALIZED: &[u8; 8] = b"FCLOPS01";
const OVERHEAD: usize = 44;
const MANAGED_INITIALIZED: &[u8; 8] = b"FCLMST01";
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    Legacy,
    Managed,
    Native,
    Coordinator,
    Watch,
}
impl Layout {
    fn marker(self) -> &'static [u8; 8] {
        match self {
            Self::Legacy => INITIALIZED,
            Self::Managed => MANAGED_INITIALIZED,
            Self::Native => b"FCLNST01",
            Self::Coordinator => b"FCLMCO02",
            Self::Watch => b"FCLWAT01",
        }
    }
}

pub(crate) struct Directory {
    path: PathBuf,
    _lock: FileLock,
    layout: Layout,
    /// The store's records at rest (29 §12.11): sealed from the at-rest level, in the clear below it.
    records: Records,
}
impl Directory {
    /// The coordinator lock is an initialized marker outside its managed child.
    /// An empty lock is recoverable only before any maintenance can be returned.
    pub(crate) fn coordinator(
        parent: &Path,
        name: &str,
        create: bool,
        rest: AtRest,
    ) -> Result<(Self, bool), StoreError> {
        Self::named_owner(parent, name, create, Layout::Coordinator, rest)
    }
    pub(crate) fn watch(
        parent: &Path,
        name: &str,
        create: bool,
        rest: AtRest,
    ) -> Result<(Self, bool), StoreError> {
        Self::named_owner(parent, name, create, Layout::Watch, rest)
    }
    fn named_owner(
        parent: &Path,
        name: &str,
        create: bool,
        layout: Layout,
        rest: AtRest,
    ) -> Result<(Self, bool), StoreError> {
        let owner = focal_platform::fs::private_dir_owner(parent)
            .map_err(missing_is_corrupt)?
            .ok_or(StoreError::Permissions)?;
        let suffix = if matches!(layout, Layout::Watch) {
            "watch"
        } else {
            "managed"
        };
        let path = parent.join(format!("{name}.{suffix}-lock"));
        match fs::symlink_metadata(&path) {
            Ok(_) => check_file(&path, &owner)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if parent.join(format!("{name}.{suffix}-owner")).try_exists()?
                    || parent.join(name).try_exists()?
                {
                    return Err(StoreError::Corrupt);
                }
            }
            Err(error) => return Err(error.into()),
        }
        let lock =
            focal_platform::fs::open_private(&path, true, true, create).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    StoreError::MissingOperation
                } else {
                    error.into()
                }
            })?;
        check_open_file(&path, &lock, &owner)?;
        let lock = FileLock::acquire(lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                StoreError::Locked
            } else {
                error.into()
            }
        })?;
        let initialized = read_marker_prefix(lock.file(), layout.marker())?;
        lock.file().sync_all()?;
        sync_dir(parent)?;
        // Several owners share the directory: each its own mark.
        let records = Records::owned(parent, rest, format!("{name}.{suffix}-sealed"))?;
        Ok((
            Self {
                path: parent.into(),
                _lock: lock,
                layout,
                records,
            },
            initialized,
        ))
    }
    pub(crate) fn finish_coordinator(&self) -> Result<(), StoreError> {
        let mut lock = self._lock.file();
        lock.rewind()?;
        lock.write_all(self.layout.marker())?;
        lock.set_len(8)?;
        lock.sync_all()?;
        sync_dir(&self.path)?;
        Ok(())
    }
    /// Only a durable outer initialization intent may resume this path; no
    /// child request can have escaped before that outer intent becomes Ready.
    pub(crate) fn resume_managed_creation(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        match focal_platform::fs::create_dir_private(path) {
            Ok(()) => sync_dir(parent(path))?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let missing_lock = !path.join("LOCK").try_exists()?;
        let mut count = 0usize;
        for entry in fs::read_dir(path)? {
            count = count.checked_add(1).ok_or(StoreError::Capacity)?;
            let name = entry?.file_name();
            if count > 5
                || !matches!(
                    name.to_str(),
                    Some("LOCK" | "INITIALIZED" | "stream.bin" | "stream.pending")
                        | Some(crate::at_rest::SEALED)
                )
            {
                return Err(StoreError::Corrupt);
            }
        }
        let directory = Self::lock(path, missing_lock, Layout::Managed, rest)?;
        if directory.exists(MARKER)? {
            let marker_path = path.join(MARKER);
            directory.check_path(&marker_path)?;
            let mut marker = focal_platform::fs::open_private(&marker_path, true, true, false)?;
            check_open_file(&marker_path, &marker, &store_owner(path)?)?;
            if !read_marker_prefix(&marker, MANAGED_INITIALIZED)? {
                marker.rewind()?;
                marker.write_all(MANAGED_INITIALIZED)?;
                marker.set_len(8)?;
            }
            marker.sync_all()?;
        } else {
            directory.initialize()?;
        }
        Ok(directory)
    }
    pub(crate) fn create(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        Self::create_layout(path, Layout::Legacy, rest)
    }
    pub(crate) fn create_managed(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        Self::create_layout(path, Layout::Managed, rest)
    }
    pub(crate) fn create_native(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        Self::create_layout(path, Layout::Native, rest)
    }
    fn create_layout(path: &Path, layout: Layout, rest: AtRest) -> Result<Self, StoreError> {
        focal_platform::fs::create_dir_private(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StoreError::Exists
            } else {
                error.into()
            }
        })?;
        sync_dir(parent(path))?;
        Self::lock(path, true, layout, rest)
    }
    pub(crate) fn open(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        Self::open_layout(path, Layout::Legacy, rest)
    }
    pub(crate) fn open_managed(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        Self::open_layout(path, Layout::Managed, rest)
    }
    pub(crate) fn open_native(path: &Path, rest: AtRest) -> Result<Self, StoreError> {
        Self::open_layout(path, Layout::Native, rest)
    }
    fn open_layout(path: &Path, layout: Layout, rest: AtRest) -> Result<Self, StoreError> {
        let directory = Self::lock(path, false, layout, rest)?;
        let mut file = directory.checked_open(&path.join(MARKER))?;
        if file.metadata()?.len() != 8 {
            return Err(StoreError::Corrupt);
        }
        let mut marker = [0; 8];
        file.read_exact(&mut marker)?;
        if marker != *layout.marker() {
            return Err(StoreError::Corrupt);
        }
        // The marker was written durably at initialization; a read handle needs
        // no re-sync (FlushFileBuffers rejects a read-only handle on Windows).
        // The directory fence below re-establishes a prior ambiguous sync_dir.
        sync_dir(path)?;
        Ok(directory)
    }
    fn lock(path: &Path, create: bool, layout: Layout, rest: AtRest) -> Result<Self, StoreError> {
        let owner = focal_platform::fs::private_dir_owner(path)
            .map_err(missing_is_corrupt)?
            .ok_or(StoreError::Permissions)?;
        let lock_path = path.join("LOCK");
        if !create {
            check_file(&lock_path, &owner)?;
        }
        let lock = if create {
            focal_platform::fs::create_private_new(&lock_path, true, true)
        } else {
            focal_platform::fs::open_private(&lock_path, true, true, false)
        }
        .map_err(missing_is_corrupt)?;
        check_open_file(&lock_path, &lock, &owner)?;
        // The native journal is shared by every process of one adapter, and
        // every holder holds it for its own file work, never across a network
        // wait (the driver reads before it prepares and sends after it lets
        // go): a contending process waits for the holder to let go, however
        // long its syncs take on a busy disk. A five-second clock refused it
        // ("locked by another admission") whenever the syncs of the makers
        // ahead of it took longer, as six makers at once on one directory
        // under other processes' F_FULLFSYNCs did.
        let acquired = if layout == Layout::Native {
            FileLock::acquire_waiting(lock)
        } else {
            FileLock::acquire(lock)
        };
        let lock = acquired.map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                StoreError::Locked
            } else {
                error.into()
            }
        })?;
        if create {
            lock.file().sync_all()?;
            sync_dir(path)?;
        }
        let records = Records::new(path, rest)?;
        if create && records.sealing() {
            // Made sealed, with nothing in it to convert: a record in the clear is refused from
            // its first.
            records.mark()?;
        }
        Ok(Self {
            path: path.into(),
            _lock: lock,
            layout,
            records,
        })
    }
    /// Whether the store is marked: every record sealed, and one in the clear refused.
    pub(crate) fn marked(&self) -> Result<bool, StoreError> {
        Ok(self.records.marked()?)
    }
    /// Converts the records `names` (each a path in the store, its magic and the most its payload
    /// holds) in the clear to sealed, then marks the store (29 §12.11): what the store's open does
    /// under its lock once its data directory's fence opens the at-rest level. A record sealed
    /// already, by a conversion a cut ended, is read to check it and left; one in the clear is read
    /// as its reader reads it (its magic, length and hash checked, a publication a cut left
    /// recovered) and written sealed through the replace that writes it. A cut anywhere leaves each
    /// record whole in one form or the other, and the next open finishes. Below the level, or
    /// marked already, nothing is done.
    pub(crate) fn convert<'a>(
        &self,
        names: impl IntoIterator<Item = (String, &'a [u8; 8], usize)>,
    ) -> Result<(), StoreError> {
        if !self.records.sealing() || self.records.marked()? {
            return Ok(());
        }
        for (relative, magic, maximum) in names {
            if !self.exists(&relative)? {
                continue;
            }
            let payload = self.read(&relative, magic, maximum)?;
            if !self.sealed_on_disk(&self.path.join(&relative))? {
                self.write(&relative, magic, &payload, true)?;
                crate::at_rest::step()?;
            }
        }
        self.records.mark()?;
        Ok(())
    }
    /// Whether the record at `path` is sealed on disk: what its first bytes say.
    fn sealed_on_disk(&self, path: &Path) -> Result<bool, StoreError> {
        let mut file = self.checked_open(path)?;
        let mut head = [0; 8];
        let mut read = 0;
        while read < head.len() {
            let Some(rest) = head.get_mut(read..) else {
                break;
            };
            match file.read(rest)? {
                0 => break,
                more => read = read.saturating_add(more),
            }
        }
        Ok(Records::sealed(head.get(..read).unwrap_or_default()))
    }
    pub(crate) fn initialize(&self) -> Result<(), StoreError> {
        let mut file =
            focal_platform::fs::create_private_new(&self.path.join(MARKER), false, true)?;
        file.write_all(self.layout.marker())?;
        file.sync_all()?;
        sync_dir(&self.path)?;
        Ok(())
    }
    pub(crate) fn exists(&self, relative: &str) -> Result<bool, StoreError> {
        match fs::symlink_metadata(self.path.join(relative)) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) fn create_child(&self, component: &str) -> Result<(), StoreError> {
        focal_platform::fs::create_dir_private(&self.path.join(component))?;
        sync_dir(&self.path)?;
        self.check_child(component)
    }
    /// Remove a retired child store that this owner's durable record already
    /// fences; a missing child is fine, anything but an owner-private
    /// directory is refused before deletion.
    pub(crate) fn remove_child(&self, component: &str) -> Result<(), StoreError> {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.contains('/')
            || component.contains('\\')
        {
            return Err(StoreError::Permissions);
        }
        if !self.exists(component)? {
            return Ok(());
        }
        self.check_child(component)?;
        fs::remove_dir_all(self.path.join(component))?;
        sync_dir(&self.path)?;
        Ok(())
    }
    /// The child directories by name, at most `limit` of them; files and
    /// links are not children.
    pub(crate) fn children(&self, limit: usize) -> Result<Vec<String>, StoreError> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if names.len() >= limit {
                break;
            }
            names.try_reserve(1).map_err(|_| StoreError::Capacity)?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        Ok(names)
    }
    pub(crate) fn check_child(&self, component: &str) -> Result<(), StoreError> {
        let owner = store_owner(&self.path)?;
        match focal_platform::fs::private_dir_owner(&self.path.join(component)) {
            Ok(Some(found)) if found == owner => Ok(()),
            Ok(_) => Err(StoreError::Permissions),
            Err(error) => Err(missing_is_corrupt(error)),
        }
    }
    pub(crate) fn read(
        &self,
        relative: &str,
        magic: &[u8; 8],
        maximum: usize,
    ) -> Result<Vec<u8>, StoreError> {
        let path = self.path.join(relative);
        let (mut file, linked_temporary) = match self.checked_open(&path) {
            Ok(file) => (file, None),
            Err(StoreError::Permissions) => {
                self.record_limit(relative)?;
                let temporary = path.with_extension("pending");
                let file = self.open_link_pair(&path, &temporary)?;
                (file, Some(temporary))
            }
            Err(error) => return Err(error),
        };
        let maximum = maximum.checked_add(OVERHEAD).ok_or(StoreError::Capacity)?;
        // Either form: the record's own bytes, or their seal (29 §12.11).
        let on_disk = self.records.bound(maximum)?;
        let size = usize::try_from(file.metadata()?.len()).map_err(|_| StoreError::Capacity)?;
        if size > on_disk {
            return Err(StoreError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| StoreError::Capacity)?;
        (&mut file)
            .take(
                u64::try_from(on_disk)
                    .map_err(|_| StoreError::Capacity)?
                    .checked_add(1)
                    .ok_or(StoreError::Capacity)?,
            )
            .read_to_end(&mut bytes)?;
        if bytes.len() > on_disk {
            return Err(StoreError::Capacity);
        }
        let bytes = self.records.opened(relative, bytes, maximum)?;
        if bytes.len() > maximum {
            return Err(StoreError::Capacity);
        }
        if bytes.get(..8) != Some(magic.as_slice()) {
            return Err(StoreError::Corrupt);
        }
        let length = u32::from_be_bytes(
            bytes
                .get(8..12)
                .ok_or(StoreError::Corrupt)?
                .try_into()
                .map_err(|_| StoreError::Corrupt)?,
        ) as usize;
        let end = length.checked_add(12).ok_or(StoreError::Corrupt)?;
        if end.checked_add(32) != Some(bytes.len()) {
            return Err(StoreError::Corrupt);
        }
        let digest = blake3::hash(bytes.get(..end).ok_or(StoreError::Corrupt)?);
        if bytes.get(end..) != Some(digest.as_bytes().as_slice()) {
            return Err(StoreError::Corrupt);
        }
        // A previous caller may have received an ambiguous directory-fsync
        // failure. The record's bytes are already durable (written and synced
        // before publication); only the directory entry needs re-fencing, done
        // by sync_dir below. A read handle cannot be fsync'd on Windows anyway.
        if let Some(temporary) = linked_temporary {
            // No name is removed until the bounded complete frame has passed
            // its checksum and both paths still name this exact private inode.
            self.check_link_pair(&path, &temporary, &file)?;
            fs::remove_file(&temporary)?;
            check_open_file(&path, &file, &store_owner(&self.path)?)?;
        }
        sync_dir(parent(&path))?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(length)
            .map_err(|_| StoreError::Capacity)?;
        payload.extend_from_slice(bytes.get(12..end).ok_or(StoreError::Corrupt)?);
        Ok(payload)
    }
    pub(crate) fn write(
        &self,
        relative: &str,
        magic: &[u8; 8],
        payload: &[u8],
        replace: bool,
    ) -> Result<(), StoreError> {
        let path = self.path.join(relative);
        let temporary = path.with_extension("pending");
        match fs::symlink_metadata(&temporary) {
            Ok(_) => {
                if focal_platform::fs::path_hard_link_count(&temporary)? == 2 {
                    // Recover a prior no-clobber publication before stale
                    // temporary cleanup; every other linked shape rejects.
                    let limit = self.record_limit(relative)?;
                    self.read(relative, magic, limit)?;
                }
                if temporary.try_exists()? {
                    self.check_path(&temporary)?;
                    fs::remove_file(&temporary)?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if replace {
            self.check_path(&path)?;
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| StoreError::Capacity)?
            .to_be_bytes();
        let mut hash = blake3::Hasher::new();
        hash.update(magic);
        hash.update(&length);
        hash.update(payload);
        // The record whole, as every earlier binary wrote it, then in the form the store keeps.
        let mut record = Vec::new();
        record
            .try_reserve_exact(payload.len().saturating_add(OVERHEAD))
            .map_err(|_| StoreError::Capacity)?;
        record.extend_from_slice(magic);
        record.extend_from_slice(&length);
        record.extend_from_slice(payload);
        record.extend_from_slice(hash.finalize().as_bytes());
        let record = self.records.to_disk(relative, record)?;
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
        file.write_all(&record)?;
        file.sync_all()?;
        // Close before renaming/linking: Windows refuses to rename a file with
        // an open handle, and the link-pair recovery reopens by path.
        drop(file);
        if replace {
            focal_platform::fs::atomic_replace(&temporary, &path)?;
        } else {
            fs::hard_link(&temporary, &path)?;
            fs::remove_file(&temporary)?;
        }
        sync_dir(parent(&path))?;
        Ok(())
    }
    pub(crate) fn remove_record(&self, relative: &str) -> Result<(), StoreError> {
        self.record_limit(relative)?;
        let path = self.path.join(relative);
        match self.check_path(&path) {
            Ok(()) => fs::remove_file(&path)?,
            Err(StoreError::Corrupt) if !self.exists(relative)? => {}
            Err(error) => return Err(error),
        }
        sync_dir(parent(&path))?;
        Ok(())
    }
    fn record_limit(&self, relative: &str) -> Result<usize, StoreError> {
        if matches!(self.layout, Layout::Watch) {
            return crate::watch::record_limit(relative).ok_or(StoreError::Permissions);
        }
        if matches!(self.layout, Layout::Coordinator) {
            return crate::managed_requests::record_limit(relative).ok_or(StoreError::Permissions);
        }
        if matches!(self.layout, Layout::Managed) {
            return crate::managed_store::record_limit(relative).ok_or(StoreError::Permissions);
        }
        if relative == super::CATALOGUE {
            return Ok(super::CATALOGUE_BYTES);
        }
        let (directory, record) = relative.split_once('/').ok_or(StoreError::Permissions)?;
        if record != super::PREPARED
            || directory.len() != 32
            || !directory
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || directory.bytes().all(|byte| byte == b'0')
        {
            return Err(StoreError::Permissions);
        }
        self.check_child(directory)?;
        Ok(super::PREPARED_BYTES)
    }
    fn open_link_pair(&self, path: &Path, temporary: &Path) -> Result<File, StoreError> {
        let owner = store_owner(&self.path)?;
        check_file_links(path, &owner, 2)?;
        check_file_links(temporary, &owner, 2).map_err(|error| match error {
            StoreError::Corrupt => StoreError::Permissions,
            other => other,
        })?;
        let file = File::open(path).map_err(missing_is_corrupt)?;
        self.check_link_pair(path, temporary, &file)?;
        Ok(file)
    }
    fn check_link_pair(
        &self,
        path: &Path,
        temporary: &Path,
        file: &File,
    ) -> Result<(), StoreError> {
        let owner = store_owner(&self.path)?;
        check_file_links(path, &owner, 2)?;
        check_file_links(temporary, &owner, 2)?;
        // The two names must be the very same file (one inode, two links) that
        // the open handle refers to: an owner-private regular file linked twice.
        if !focal_platform::fs::check_open_private_file_links(path, file, &owner, 2)? {
            return Err(StoreError::Permissions);
        }
        let identity = focal_platform::fs::file_identity(file)?;
        for name in [path, temporary] {
            if focal_platform::fs::path_identity(name)? != identity {
                return Err(StoreError::Permissions);
            }
        }
        Ok(())
    }
    fn check_path(&self, path: &Path) -> Result<(), StoreError> {
        check_file(path, &store_owner(&self.path)?)
    }
    fn checked_open(&self, path: &Path) -> Result<File, StoreError> {
        self.check_path(path)?;
        let file = File::open(path).map_err(missing_is_corrupt)?;
        check_open_file(path, &file, &store_owner(&self.path)?)?;
        Ok(file)
    }
}
fn read_marker_prefix(mut file: &File, expected: &[u8; 8]) -> Result<bool, StoreError> {
    let length = usize::try_from(file.metadata()?.len()).map_err(|_| StoreError::Corrupt)?;
    if length > 8 {
        return Err(StoreError::Corrupt);
    }
    let mut bytes = [0; 8];
    let prefix = bytes.get_mut(..length).ok_or(StoreError::Corrupt)?;
    file.read_exact(prefix)?;
    if Some(&*prefix) != expected.get(..length) {
        return Err(StoreError::Corrupt);
    }
    Ok(length == 8)
}
fn sync_dir(path: &Path) -> Result<(), StoreError> {
    // Unix makes a new or removed directory entry durable with an fsync of the
    // directory; Windows uses write-through file writes and rename and does not
    // flush a directory handle.
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn missing_is_corrupt(error: std::io::Error) -> StoreError {
    if error.kind() == std::io::ErrorKind::NotFound {
        StoreError::Corrupt
    } else {
        error.into()
    }
}
type Owner = focal_platform::fs::Owner;
fn store_owner(path: &Path) -> Result<Owner, StoreError> {
    focal_platform::fs::owner_at(path).map_err(Into::into)
}
fn check_file(path: &Path, owner: &Owner) -> Result<(), StoreError> {
    check_file_links(path, owner, 1)
}
fn check_file_links(path: &Path, owner: &Owner, links: u64) -> Result<(), StoreError> {
    match focal_platform::fs::check_private_file(path, owner, links) {
        Ok(true) => Ok(()),
        Ok(false) => Err(StoreError::Permissions),
        Err(error) => Err(missing_is_corrupt(error)),
    }
}
fn check_open_file(path: &Path, file: &File, owner: &Owner) -> Result<(), StoreError> {
    check_file(path, owner)?;
    match focal_platform::fs::check_open_private_file(path, file, owner) {
        Ok(true) => Ok(()),
        Ok(false) => Err(StoreError::Permissions),
        Err(error) => Err(missing_is_corrupt(error)),
    }
}
