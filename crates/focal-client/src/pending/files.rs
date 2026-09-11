use super::{MAX_STATE_BYTES, PendingError};
use crate::file_lock::FileLock;
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

#[cfg(all(test, unix))]
#[path = "lock_tests.rs"]
mod lock_tests;

const MAGIC: &[u8; 8] = b"FCLOP001";
const RECORD: &str = "state.bin";
const MARKER: &str = "INITIALIZED";
const TEMPORARY: &str = ".pending";
const FRAME_OVERHEAD: usize = 44;

pub(super) struct Directory {
    path: PathBuf,
    _lock: FileLock,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl Directory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn create(path: &Path) -> Result<Self, PendingError> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        focal_platform::fs::create_dir_private(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                PendingError::Exists
            } else {
                error.into()
            }
        })?;
        sync_dir(parent)?;
        Self::lock(path, true)
    }
    pub(super) fn open(path: &Path) -> Result<Self, PendingError> {
        Self::lock(path, false)
    }
    /// Only the store's unready, fully prepared operation path may use this.
    /// An absent lock is recoverable solely in a checked unpublished directory.
    pub(super) fn resume_prepared(path: &Path) -> Result<Self, PendingError> {
        let create_lock = !present(&path.join("LOCK"))?;
        if create_lock {
            check_unpublished_directory(path)?;
        }
        Self::lock(path, create_lock)
    }
    pub(super) fn unpublished(&self) -> Result<bool, PendingError> {
        if present(&self.path.join(RECORD))? {
            return Ok(false);
        }
        check_unpublished_directory(&self.path)?;
        Ok(true)
    }
    fn lock(path: &Path, create: bool) -> Result<Self, PendingError> {
        let owner = focal_platform::fs::private_dir_owner(path)
            .map_err(missing_is_corrupt)?
            .ok_or(PendingError::Permissions)?;
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
        let lock = FileLock::acquire(lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                PendingError::Locked
            } else {
                error.into()
            }
        })?;
        if create {
            lock.file().sync_all()?;
            sync_dir(path)?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            _lock: lock,
            #[cfg(test)]
            fault: std::cell::Cell::new(None),
        })
    }
    pub(super) fn read(&self) -> Result<Vec<u8>, PendingError> {
        let path = self.path.join(RECORD);
        let file = self.checked_open(&path)?;
        let maximum = MAX_STATE_BYTES
            .checked_add(FRAME_OVERHEAD)
            .ok_or(PendingError::Capacity)?;
        let size = usize::try_from(file.metadata()?.len()).map_err(|_| PendingError::Capacity)?;
        if size > maximum {
            return Err(PendingError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| PendingError::Capacity)?;
        file.take(
            u64::try_from(maximum)
                .map_err(|_| PendingError::Capacity)?
                .checked_add(1)
                .ok_or(PendingError::Capacity)?,
        )
        .read_to_end(&mut bytes)?;
        if bytes.len() > maximum {
            return Err(PendingError::Capacity);
        }
        if bytes.get(..8) != Some(MAGIC.as_slice()) {
            return Err(PendingError::Corrupt);
        }
        let length = u32::from_be_bytes(
            bytes
                .get(8..12)
                .ok_or(PendingError::Corrupt)?
                .try_into()
                .map_err(|_| PendingError::Corrupt)?,
        ) as usize;
        let end = length.checked_add(12).ok_or(PendingError::Corrupt)?;
        if end.checked_add(32) != Some(bytes.len()) {
            return Err(PendingError::Corrupt);
        }
        if Some(
            blake3::hash(bytes.get(..end).ok_or(PendingError::Corrupt)?)
                .as_bytes()
                .as_slice(),
        ) != bytes.get(end..)
        {
            return Err(PendingError::Corrupt);
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(length)
            .map_err(|_| PendingError::Capacity)?;
        payload.extend_from_slice(bytes.get(12..end).ok_or(PendingError::Corrupt)?);
        Ok(payload)
    }
    pub(super) fn install(&self, payload: &[u8], initial: bool) -> Result<(), PendingError> {
        if payload.len() > MAX_STATE_BYTES {
            return Err(PendingError::Capacity);
        }
        // Do not overwrite a missing/corrupt already initialized operation.
        if !initial
            || self.path.join(MARKER).try_exists()?
            || self.path.join(RECORD).try_exists()?
        {
            let _ = self.read()?;
        }
        let temporary = self.path.join(TEMPORARY);
        if temporary.try_exists()? {
            // An orphan temporary is never authoritative. Exclusion and private
            // single-link checks prevent deleting another owner's retry state.
            self.check_path(&temporary)?;
            fs::remove_file(&temporary)?;
        }
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
        let length = u32::try_from(payload.len())
            .map_err(|_| PendingError::Capacity)?
            .to_be_bytes();
        let mut digest = blake3::Hasher::new();
        digest.update(MAGIC);
        digest.update(&length);
        digest.update(payload);
        file.write_all(MAGIC)?;
        file.write_all(&length)?;
        file.write_all(payload)?;
        file.write_all(digest.finalize().as_bytes())?;
        file.sync_all()?;
        #[cfg(test)]
        self.fail_at(Fault::FileSynced)?;
        fs::rename(&temporary, self.path.join(RECORD))?;
        #[cfg(test)]
        self.fail_at(Fault::Renamed)?;
        sync_dir(&self.path)?;
        #[cfg(test)]
        self.fail_at(Fault::DirectorySynced)?;
        self.recover_marker()
    }
    pub(super) fn recover_marker(&self) -> Result<(), PendingError> {
        // Validate/sync the complete record before recovering the creation
        // window between rename and initialized-marker installation.
        self.checked_open(&self.path.join(RECORD))?.sync_all()?;
        let marker = self.path.join(MARKER);
        if marker.try_exists()? {
            let mut file = self.checked_open(&marker)?;
            if file.metadata()?.len() != 8 {
                return Err(PendingError::Corrupt);
            }
            let mut bytes = [0; 8];
            file.read_exact(&mut bytes)?;
            if bytes != *MAGIC {
                return Err(PendingError::Corrupt);
            }
            file.sync_all()?;
        } else {
            let mut file = focal_platform::fs::create_private_new(&marker, false, true)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
        }
        sync_dir(&self.path)?;
        Ok(())
    }
    fn check_path(&self, path: &Path) -> Result<(), PendingError> {
        check_file(path, &store_owner(&self.path)?)
    }
    fn checked_open(&self, path: &Path) -> Result<File, PendingError> {
        self.check_path(path)?;
        let file = File::open(path).map_err(missing_is_corrupt)?;
        check_open_file(path, &file, &store_owner(&self.path)?)?;
        Ok(file)
    }
    #[cfg(test)]
    fn fail_at(&self, point: Fault) -> Result<(), PendingError> {
        if self.fault.get() == Some(point) {
            self.fault.set(None);
            Err(std::io::Error::other("injected journal durability failure").into())
        } else {
            Ok(())
        }
    }
    #[cfg(test)]
    pub(super) fn inject(&self, point: Fault) {
        self.fault.set(Some(point));
    }
}
fn missing_is_corrupt(error: std::io::Error) -> PendingError {
    if error.kind() == std::io::ErrorKind::NotFound {
        PendingError::Corrupt
    } else {
        error.into()
    }
}
fn present(path: &Path) -> Result<bool, PendingError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
fn check_unpublished_directory(path: &Path) -> Result<(), PendingError> {
    let owner = focal_platform::fs::private_dir_owner(path)
        .map_err(missing_is_corrupt)?
        .ok_or(PendingError::Permissions)?;
    // At most LOCK and the unpublished initial temporary are admissible.
    // A marker, any existing state (even corrupt), or an unexpected name
    // prevents initialization. This scan allocates no unbounded collection.
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        if name != "LOCK" && name != TEMPORARY {
            return Err(PendingError::Corrupt);
        }
        check_file(&entry.path(), &owner)?;
    }
    Ok(())
}
type Owner = focal_platform::fs::Owner;
fn store_owner(path: &Path) -> Result<Owner, PendingError> {
    focal_platform::fs::owner_at(path).map_err(Into::into)
}
fn sync_dir(path: &Path) -> Result<(), PendingError> {
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
fn check_file(path: &Path, owner: &Owner) -> Result<(), PendingError> {
    match focal_platform::fs::check_private_file(path, owner, 1) {
        Ok(true) => Ok(()),
        Ok(false) => Err(PendingError::Permissions),
        Err(error) => Err(missing_is_corrupt(error)),
    }
}
fn check_open_file(path: &Path, file: &File, owner: &Owner) -> Result<(), PendingError> {
    check_file(path, owner)?;
    match focal_platform::fs::check_open_private_file(path, file, owner) {
        Ok(true) => Ok(()),
        Ok(false) => Err(PendingError::Permissions),
        Err(error) => Err(missing_is_corrupt(error)),
    }
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    FileSynced,
    Renamed,
    DirectorySynced,
}
