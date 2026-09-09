use crate::*;
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

/// One node-owned private directory. Unix ownership and mode are required rather
/// than silently pretending POSIX mode bits protect a different platform.
pub(crate) struct PrivateDirectory {
    path: PathBuf,
    _lock: File,
    /// A shared owner reads beside other readers and never writes; a writer
    /// holds the directory exclusively.
    shared: bool,
}
impl PrivateDirectory {
    pub(crate) fn open(path: &Path) -> Result<Self, EnrollmentError> {
        Self::open_with(path, false)
    }
    /// Open an existing directory for reading beside other readers. Concurrent
    /// processes of one participant (a human CLI beside its MCP adapter, or
    /// several CLI invocations) read the same credentials; any writer still
    /// excludes them all and is excluded by them.
    pub(crate) fn open_shared(path: &Path) -> Result<Self, EnrollmentError> {
        Self::open_with(path, true)
    }
    fn open_with(path: &Path, shared: bool) -> Result<Self, EnrollmentError> {
        #[cfg(not(unix))]
        {
            let _ = (path, shared);
            Err(EnrollmentError::Permissions)
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
            if shared && !path.exists() {
                return Err(EnrollmentError::Invalid);
            }
            if !path.exists() {
                // Require an existing deployment-owned parent; do not create a
                // chain of private directories with ambiguous owner permissions.
                let parent = path.parent().ok_or(EnrollmentError::Invalid)?;
                fs::DirBuilder::new().mode(0o700).create(path)?;
                File::open(parent)?.sync_all()?;
            }
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
                return Err(EnrollmentError::Permissions);
            }
            let lock_path = path.join("LOCK");
            if lock_path.exists() {
                check_file(&lock_path, metadata.uid())?;
            }
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .mode(0o600)
                .open(&lock_path)?;
            check_file(&lock_path, metadata.uid())?;
            let acquired = if shared {
                FileExt::try_lock_shared(&lock)
            } else {
                FileExt::try_lock_exclusive(&lock)
            };
            acquired.map_err(|error| {
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    EnrollmentError::Locked
                } else {
                    error.into()
                }
            })?;
            if !shared {
                lock.sync_all()?;
                File::open(path)?.sync_all()?;
            }
            Ok(Self {
                path: path.to_path_buf(),
                _lock: lock,
                shared,
            })
        }
    }
    pub(crate) fn read(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>, EnrollmentError> {
        let path = self.path.join(name);
        if !path.exists() {
            return if self.path.join(format!("{name}.initialized")).exists() {
                Err(EnrollmentError::Corrupt)
            } else {
                Ok(None)
            };
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            check_file(&path, fs::metadata(&self.path)?.uid())?;
        }
        let mut file = File::open(path)?;
        if file.metadata()?.len() > 64 * 1024 {
            return Err(EnrollmentError::Capacity);
        }
        let mut bytes = Zeroizing::new(Vec::new());
        std::io::Read::by_ref(&mut file)
            .take((64_u64 * 1024).saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 {
            return Err(EnrollmentError::Capacity);
        }
        if bytes.len() < 40 || bytes.get(..8) != Some(b"FCLKEY01".as_slice()) {
            return Err(EnrollmentError::Corrupt);
        }
        let split = bytes
            .len()
            .checked_sub(32)
            .ok_or(EnrollmentError::Corrupt)?;
        let (payload, checksum) = bytes
            .split_at_checked(split)
            .ok_or(EnrollmentError::Corrupt)?;
        if blake3::hash(payload).as_bytes() != checksum {
            return Err(EnrollmentError::Corrupt);
        }
        // Recover the narrow crash window after the complete file was installed
        // but before its initialization marker was synced.
        file.sync_all()?;
        self.ensure_marker(name)?;
        Ok(Some(Zeroizing::new(
            payload.get(8..).ok_or(EnrollmentError::Corrupt)?.to_vec(),
        )))
    }
    pub(crate) fn install_new(&self, name: &str, payload: &[u8]) -> Result<(), EnrollmentError> {
        if self.shared {
            return Err(EnrollmentError::Locked);
        }
        let path = self.path.join(name);
        if path.exists() {
            return Err(EnrollmentError::Conflict);
        }
        self.install(name, payload)
    }
    pub(crate) fn replace(&self, name: &str, payload: &[u8]) -> Result<(), EnrollmentError> {
        if self.shared {
            return Err(EnrollmentError::Locked);
        }
        // Validate existing state and its initialization marker before replacing
        // it; missing/corrupt private retry state is never silently reset.
        let _ = self.read(name)?;
        self.install(name, payload)
    }
    fn install(&self, name: &str, payload: &[u8]) -> Result<(), EnrollmentError> {
        if payload.len() > 60 * 1024 {
            return Err(EnrollmentError::Capacity);
        }
        let path = self.path.join(name);
        let temporary = self
            .path
            .join(format!(".pending-{}", hex(&random::<16>()?)));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        let mut bytes = Zeroizing::new(Vec::with_capacity(
            payload
                .len()
                .checked_add(40)
                .ok_or(EnrollmentError::Capacity)?,
        ));
        bytes.extend_from_slice(b"FCLKEY01");
        bytes.extend_from_slice(payload);
        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        file.write_all(&bytes)?;
        file.sync_all()?;
        // Same-directory rename is atomic; held exclusive writer lock excludes
        // another initializer. The directory sync is the acknowledgment fence.
        fs::rename(&temporary, &path)?;
        File::open(&self.path)?.sync_all()?;
        self.ensure_marker(name)?;
        Ok(())
    }
    fn ensure_marker(&self, name: &str) -> Result<(), EnrollmentError> {
        let marker = self.path.join(format!("{name}.initialized"));
        if marker.exists() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                check_file(&marker, fs::metadata(&self.path)?.uid())?;
            }
            return Ok(());
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(marker)?.sync_all()?;
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
}
#[cfg(unix)]
fn check_file(path: &Path, owner: u32) -> Result<(), EnrollmentError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err(EnrollmentError::Permissions);
    }
    Ok(())
}
