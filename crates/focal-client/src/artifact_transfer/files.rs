use super::{MAX_STATE_BYTES, TransferError};
use crate::file_lock::FileLock;
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

#[cfg(all(test, unix))]
#[path = "lock_tests.rs"]
mod lock_tests;

const MARKER: &str = "INITIALIZED";
const PAYLOAD: &str = "payload.bin";
const TEMPORARY: &str = ".upload.pending";
#[derive(Clone, Copy)]
enum Layout {
    Upload,
    Catalogue,
}
impl Layout {
    fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Upload => b"FCLUP001",
            Self::Catalogue => b"FCLUPCAT",
        }
    }
    fn record(self) -> &'static str {
        match self {
            Self::Upload => "upload.bin",
            Self::Catalogue => "uploads.bin",
        }
    }
    fn maximum(self) -> usize {
        match self {
            Self::Upload => MAX_STATE_BYTES,
            Self::Catalogue => super::store::CATALOGUE_BYTES,
        }
    }
}

pub(super) struct Directory {
    path: PathBuf,
    _lock: FileLock,
    layout: Layout,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl Directory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn create(path: &Path) -> Result<(Self, File), TransferError> {
        let directory = Self::create_layout(path, Layout::Upload)?;
        let payload = focal_platform::fs::create_private_new(&path.join(PAYLOAD), true, true)?;
        directory.check_open(&path.join(PAYLOAD), &payload)?;
        payload.sync_all()?;
        sync_dir(path)?;
        Ok((directory, payload))
    }
    pub(super) fn create_catalogue(path: &Path) -> Result<Self, TransferError> {
        Self::create_layout(path, Layout::Catalogue)
    }
    pub(super) fn open_catalogue(path: &Path) -> Result<Self, TransferError> {
        let directory = Self::lock(path, false, Layout::Catalogue)?;
        directory.read()?;
        directory.recover_marker()?;
        Ok(directory)
    }
    fn create_layout(path: &Path, layout: Layout) -> Result<Self, TransferError> {
        focal_platform::fs::create_dir_private(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                TransferError::Exists
            } else {
                e.into()
            }
        })?;
        sync_dir(parent(path))?;
        Self::lock(path, true, layout)
    }
    pub(super) fn open(path: &Path) -> Result<(Self, File), TransferError> {
        let directory = Self::lock(path, false, Layout::Upload)?;
        let payload = directory.open_file(PAYLOAD, true)?;
        Ok((directory, payload))
    }
    fn lock(path: &Path, create: bool, layout: Layout) -> Result<Self, TransferError> {
        let owner = focal_platform::fs::private_dir_owner(path)
            .map_err(missing)?
            .ok_or(TransferError::Permissions)?;
        let lock_path = path.join("LOCK");
        if !create {
            check_file(&lock_path, &owner)?;
        }
        let lock = if create {
            focal_platform::fs::create_private_new(&lock_path, true, true)
        } else {
            focal_platform::fs::open_private(&lock_path, true, true, false)
        }
        .map_err(missing)?;
        check_open_file(&lock_path, &lock, &owner)?;
        let lock = FileLock::acquire(lock).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                TransferError::Locked
            } else {
                e.into()
            }
        })?;
        if create {
            lock.file().sync_all()?;
            sync_dir(path)?;
        }
        Ok(Self {
            path: path.into(),
            _lock: lock,
            layout,
            #[cfg(test)]
            fault: std::cell::Cell::new(None),
        })
    }
    fn open_file(&self, name: &str, write: bool) -> Result<File, TransferError> {
        let path = self.path.join(name);
        self.check_path(&path)?;
        let file = focal_platform::fs::open_private(&path, true, write, false).map_err(missing)?;
        self.check_open(&path, &file)?;
        Ok(file)
    }
    fn check_path(&self, path: &Path) -> Result<(), TransferError> {
        check_file(path, &store_owner(&self.path)?)
    }
    fn check_open(&self, path: &Path, file: &File) -> Result<(), TransferError> {
        check_open_file(path, file, &store_owner(&self.path)?)
    }
    pub(super) fn read(&self) -> Result<Vec<u8>, TransferError> {
        let mut file = self.open_file(self.layout.record(), false)?;
        let length =
            usize::try_from(file.metadata()?.len()).map_err(|_| TransferError::Capacity)?;
        let maximum = self
            .layout
            .maximum()
            .checked_add(44)
            .ok_or(TransferError::Capacity)?;
        if !(44..=maximum).contains(&length) {
            return Err(TransferError::Corrupt);
        }
        let mut bytes = super::buffer(length)?;
        file.read_exact(&mut bytes)?;
        let mut extra = [0; 1];
        if file.read(&mut extra)? != 0 || bytes.get(..8) != Some(self.layout.magic().as_slice()) {
            return Err(TransferError::Corrupt);
        }
        let size = u32::from_be_bytes(
            bytes
                .get(8..12)
                .ok_or(TransferError::Corrupt)?
                .try_into()
                .map_err(|_| TransferError::Corrupt)?,
        ) as usize;
        let end = size.checked_add(12).ok_or(TransferError::Corrupt)?;
        if end.checked_add(32) != Some(bytes.len())
            || bytes.get(end..)
                != Some(
                    blake3::hash(bytes.get(..end).ok_or(TransferError::Corrupt)?)
                        .as_bytes()
                        .as_slice(),
                )
        {
            return Err(TransferError::Corrupt);
        }
        bytes.copy_within(12..end, 0);
        bytes.truncate(size);
        Ok(bytes)
    }
    pub(super) fn install(&self, payload: &[u8], initial: bool) -> Result<(), TransferError> {
        if payload.len() > self.layout.maximum() {
            return Err(TransferError::Capacity);
        }
        let state = self.path.join(self.layout.record());
        if initial {
            if present(&state)? || present(&self.path.join(MARKER))? {
                return Err(TransferError::Corrupt);
            }
        } else {
            self.read()?;
            self.check_marker()?;
        }
        let temporary = self.path.join(TEMPORARY);
        if present(&temporary)? {
            self.check_path(&temporary)?;
            fs::remove_file(&temporary)?;
        }
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
        let length = u32::try_from(payload.len())
            .map_err(|_| TransferError::Capacity)?
            .to_be_bytes();
        let mut hash = blake3::Hasher::new();
        hash.update(self.layout.magic());
        hash.update(&length);
        hash.update(payload);
        file.write_all(self.layout.magic())?;
        file.write_all(&length)?;
        file.write_all(payload)?;
        file.write_all(hash.finalize().as_bytes())?;
        file.sync_all()?;
        // Close before rename: Windows refuses to rename a file with an open handle.
        drop(file);
        #[cfg(test)]
        self.fail_at(Fault::FileSynced)?;
        // The private exclusive lock protects both initial and replacement
        // publication; rename avoids a two-link crash window entirely.
        focal_platform::fs::atomic_replace(&temporary, &state)?;
        #[cfg(test)]
        self.fail_at(Fault::Renamed)?;
        sync_dir(&self.path)?;
        self.recover_marker()
    }
    fn check_marker(&self) -> Result<(), TransferError> {
        let mut file = self.open_file(MARKER, false)?;
        if file.metadata()?.len() != 8 {
            return Err(TransferError::Corrupt);
        }
        let mut value = [0; 8];
        file.read_exact(&mut value)?;
        if &value != self.layout.magic() {
            return Err(TransferError::Corrupt);
        }
        Ok(())
    }
    pub(super) fn recover_marker(&self) -> Result<(), TransferError> {
        // read() validates the record, which is already durable (write() fsyncs
        // it and publishes write-through before the marker is created). A read
        // handle needs no re-sync, and Windows rejects fsync of one.
        self.read()?;
        let marker = self.path.join(MARKER);
        if present(&marker)? {
            return self.check_marker();
        }
        let mut file = focal_platform::fs::create_private_new(&marker, false, true)?;
        file.write_all(self.layout.magic())?;
        file.sync_all()?;
        sync_dir(&self.path)?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn inject(&self, fault: Fault) {
        self.fault.set(Some(fault));
    }
    #[cfg(test)]
    fn fail_at(&self, fault: Fault) -> Result<(), TransferError> {
        if self.fault.get() == Some(fault) {
            self.fault.set(None);
            return Err(std::io::Error::other("injected transfer crash").into());
        }
        Ok(())
    }
}
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    FileSynced,
    Renamed,
}

type Owner = focal_platform::fs::Owner;
fn store_owner(path: &Path) -> Result<Owner, TransferError> {
    focal_platform::fs::owner_at(path).map_err(Into::into)
}
fn sync_dir(path: &Path) -> Result<(), TransferError> {
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
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn present(path: &Path) -> Result<bool, TransferError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn missing(error: std::io::Error) -> TransferError {
    if error.kind() == std::io::ErrorKind::NotFound {
        TransferError::Corrupt
    } else {
        error.into()
    }
}

/// The durable bootstrap marker is outside the child store. Its lock is held
/// until the caller completes create/open, then released before network waits.
pub(super) fn bootstrap(
    parent: &Path,
    name: &str,
    context: crate::pending::OperationContext,
) -> Result<(FileLock, bool), TransferError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        || context.cluster == [0; 16]
        || context.principal.is_zero()
        || context.ledger.tenant.is_zero()
        || context.ledger.session.is_zero()
    {
        return Err(TransferError::Invalid);
    }
    let owner = focal_platform::fs::private_dir_owner(parent)?.ok_or(TransferError::Permissions)?;
    let lock_path = parent.join(format!("{name}.lock"));
    let marker = parent.join(format!("{name}.initialized"));
    let first = !present(&lock_path)?;
    if first && (present(&marker)? || present(&parent.join(name))?) {
        return Err(TransferError::Corrupt);
    }
    if !first {
        check_file(&lock_path, &owner)?;
    }
    let lock = if first {
        focal_platform::fs::create_private_new(&lock_path, true, true)?
    } else {
        focal_platform::fs::open_private(&lock_path, true, true, false)?
    };
    check_open_file(&lock_path, &lock, &owner)?;
    let lock = FileLock::acquire(lock).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            TransferError::Locked
        } else {
            error.into()
        }
    })?;
    let mut value = [0; 104];
    let mut cursor = std::io::Cursor::new(value.as_mut_slice());
    cursor.write_all(b"FCLUPBS1")?;
    for bytes in [
        &context.cluster,
        &context.principal.0,
        &context.ledger.tenant.0,
        &context.ledger.session.0,
    ] {
        cursor.write_all(bytes)?;
    }
    let checksum = *blake3::hash(value.get(..72).ok_or(TransferError::Corrupt)?).as_bytes();
    value
        .get_mut(72..)
        .ok_or(TransferError::Corrupt)?
        .copy_from_slice(&checksum);
    if first {
        lock.file().sync_all()?;
        sync_dir(parent)?;
        let mut file = focal_platform::fs::create_private_new(&marker, false, true)?;
        file.write_all(&value)?;
        file.sync_all()?;
        sync_dir(parent)?;
    } else {
        check_file(&marker, &owner)?;
        let mut file = focal_platform::fs::open_private(&marker, true, false, false)?;
        check_open_file(&marker, &file, &owner)?;
        if file.metadata()?.len() != 104 {
            return Err(TransferError::Corrupt);
        }
        let mut saved = [0; 104];
        file.read_exact(&mut saved)?;
        if saved != value {
            return Err(TransferError::Conflict);
        }
    }
    Ok((lock, first))
}
fn check_file(path: &Path, owner: &Owner) -> Result<(), TransferError> {
    match focal_platform::fs::check_private_file(path, owner, 1) {
        Ok(true) => Ok(()),
        Ok(false) => Err(TransferError::Permissions),
        Err(error) => Err(missing(error)),
    }
}
fn check_open_file(path: &Path, file: &File, owner: &Owner) -> Result<(), TransferError> {
    check_file(path, owner)?;
    match focal_platform::fs::check_open_private_file(path, file, owner) {
        Ok(true) => Ok(()),
        Ok(false) => Err(TransferError::Permissions),
        Err(error) => Err(missing(error)),
    }
}
