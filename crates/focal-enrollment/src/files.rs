use crate::*;
use std::{
    fs::{self, File},
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
        if shared && !path.exists() {
            return Err(EnrollmentError::Invalid);
        }
        if !path.exists() {
            // Require an existing deployment-owned parent; do not create a
            // chain of private directories with ambiguous owner permissions.
            let parent = path.parent().ok_or(EnrollmentError::Invalid)?;
            focal_platform::fs::create_dir_private(path)?;
            sync_dir(parent)?;
        }
        let owner =
            focal_platform::fs::private_dir_owner(path)?.ok_or(EnrollmentError::Permissions)?;
        let lock_path = path.join("LOCK");
        if lock_path.exists() {
            check_file(&lock_path, &owner)?;
        }
        let lock = focal_platform::fs::open_private(&lock_path, true, true, true)?;
        check_file(&lock_path, &owner)?;
        let acquired = if shared {
            focal_platform::try_lock_shared(&lock)
        } else {
            focal_platform::try_lock_exclusive(&lock)
        };
        acquired.map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                EnrollmentError::Locked
            } else {
                error.into()
            }
        })?;
        if !shared {
            lock.sync_all()
                .map_err(|e| io_ctx("lock sync_all", &lock_path, e))?;
            sync_dir(path)?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            _lock: lock,
            shared,
        })
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
        check_file(&path, &store_owner(&self.path)?)?;
        let mut file = File::open(&path).map_err(|e| io_ctx("read open", &path, e))?;
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
        // The file was installed durably (write-through atomic_replace fsyncs the
        // bytes before the rename), so it is already on disk before any marker
        // claims it - there is no read-time re-sync to do, and none is portable:
        // FlushFileBuffers refuses a read-only handle on Windows, and fsync of a
        // read handle buys nothing on Unix. Only the initialization marker's own
        // crash window remains to complete.
        drop(file);
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
    /// Remove a file and its initialization marker: staged material that
    /// was adopted elsewhere, so the next staging starts fresh. A missing
    /// file is not an error.
    pub(crate) fn remove(&self, name: &str) -> Result<(), EnrollmentError> {
        if self.shared {
            return Err(EnrollmentError::Locked);
        }
        for path in [
            self.path.join(name),
            self.path.join(format!("{name}.initialized")),
        ] {
            match fs::remove_file(&path) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
        sync_dir(&self.path)?;
        Ok(())
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
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
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
        file.write_all(&bytes)
            .map_err(|e| io_ctx("credential write_all", &temporary, e))?;
        file.sync_all()
            .map_err(|e| io_ctx("credential sync_all", &temporary, e))?;
        // Close the handle before the rename: Windows refuses to rename a file
        // that still has an open handle. Same-directory rename is atomic; the
        // held exclusive writer lock excludes another initializer, and the
        // directory sync is the acknowledgment fence.
        drop(file);
        focal_platform::fs::atomic_replace(&temporary, &path)?;
        sync_dir(&self.path)?;
        self.ensure_marker(name)?;
        Ok(())
    }
    fn ensure_marker(&self, name: &str) -> Result<(), EnrollmentError> {
        let marker = self.path.join(format!("{name}.initialized"));
        if marker.exists() {
            check_file(&marker, &store_owner(&self.path)?)?;
            return Ok(());
        }
        focal_platform::fs::create_private_new(&marker, false, true)?
            .sync_all()
            .map_err(|e| io_ctx("marker sync_all", &marker, e))?;
        sync_dir(&self.path)?;
        Ok(())
    }
}
type Owner = focal_platform::fs::Owner;
fn store_owner(path: &Path) -> Result<Owner, EnrollmentError> {
    focal_platform::fs::owner_at(path).map_err(Into::into)
}
fn sync_dir(path: &Path) -> Result<(), EnrollmentError> {
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
fn check_file(path: &Path, owner: &Owner) -> Result<(), EnrollmentError> {
    match focal_platform::fs::check_private_file(path, owner, 1) {
        Ok(true) => Ok(()),
        Ok(false) => Err(EnrollmentError::Permissions),
        Err(error) => Err(error.into()),
    }
}
/// Attach the failing operation and path to a raw I/O error. A bare
/// "Access is denied" from a `sync_all`/`write_all`/`open` is undiagnosable
/// across platforms; every persistence step names itself instead.
fn io_ctx(op: &str, path: &Path, error: std::io::Error) -> EnrollmentError {
    EnrollmentError::Io(std::io::Error::new(
        error.kind(),
        format!("{op} {}: {error}", path.display()),
    ))
}
