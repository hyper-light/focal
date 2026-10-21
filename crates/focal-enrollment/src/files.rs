use crate::*;
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

/// The format of a record's framed bytes: what is sealed, and what an earlier binary wrote in the
/// clear.
const FRAME_MAGIC: &[u8; 8] = b"FCLKEY01";
/// The most a record's payload is.
const MAX_PAYLOAD: usize = 60 * 1024;
/// The most a record's framed bytes are: its magic, its payload and the BLAKE3 hash after them.
const MAX_FRAMED: usize = MAX_PAYLOAD + FRAME_MAGIC.len() + 32;
/// The marker a directory holds once every record in it is sealed (29 §12): from then on a record
/// in the clear is refused as corrupt, never read.
const SEALED: &str = "SEALED";
/// The records a private directory holds by fixed name: the signer's journal, the authority's,
/// the founder's and a pending invitation's drafts, a join key and its enrollment.
const FIXED_RECORDS: usize = 6;
/// The most entries a writer's open visits: every record and its initialization marker (the fixed
/// ones, and one an invitation or an enrollment the registry admits under its default limits), the
/// lock and the seal's marker. A directory holding more was written by no binary.
fn max_entries() -> usize {
    let limits = crate::registry::EnrollmentLimits::default();
    FIXED_RECORDS
        .saturating_add(limits.max_invitations)
        .saturating_add(limits.max_enrollments)
        .saturating_mul(2)
        .saturating_add(2)
}

/// One node-owned private directory. Unix ownership and mode are required rather
/// than silently pretending POSIX mode bits protect a different platform.
///
/// Every record is sealed at rest (29 §12) under the journal key of the data directory it
/// belongs to (`focal_seal::Store::Journal`), unwrapped for that one record and dropped after
/// it: its framed bytes (magic, payload, BLAKE3 hash) are the sealed file's plaintext, so a file
/// of another record's bytes under a valid seal still reads as corrupt. A directory an earlier
/// binary wrote in the clear is sealed the first time a writer opens it, record by record, each
/// through the atomic replace that writes any record, then marked sealed.
pub(crate) struct PrivateDirectory {
    path: PathBuf,
    keys: focal_seal::DataKeys,
    _lock: focal_platform::FileLock,
    /// A shared owner reads beside other readers and never writes; a writer
    /// holds the directory exclusively.
    shared: bool,
}
impl PrivateDirectory {
    pub(crate) fn open(path: &Path, keys: &focal_seal::DataKeys) -> Result<Self, EnrollmentError> {
        Self::open_with(path, keys, false)
    }
    /// Open an existing directory for reading beside other readers. Concurrent
    /// processes of one participant (a human CLI beside its MCP adapter, or
    /// several CLI invocations) read the same credentials; any writer still
    /// excludes them all and is excluded by them.
    pub(crate) fn open_shared(
        path: &Path,
        keys: &focal_seal::DataKeys,
    ) -> Result<Self, EnrollmentError> {
        Self::open_with(path, keys, true)
    }
    fn open_with(
        path: &Path,
        keys: &focal_seal::DataKeys,
        shared: bool,
    ) -> Result<Self, EnrollmentError> {
        if shared && !path.exists() {
            return Err(EnrollmentError::Invalid);
        }
        if !path.exists() {
            // Require an existing deployment-owned parent; do not create a
            // chain of private directories with ambiguous owner permissions.
            // Another process may make it after the look; its owner is
            // checked below as one there before.
            let parent = path.parent().ok_or(EnrollmentError::Invalid)?;
            focal_platform::fs::ensure_dir_private(path)?;
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
            focal_platform::FileLock::shared(lock)
        } else {
            focal_platform::FileLock::exclusive(lock)
        };
        let lock = acquired.map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                EnrollmentError::Locked
            } else {
                error.into()
            }
        })?;
        if !shared {
            lock.file()
                .sync_all()
                .map_err(|e| io_ctx("lock sync_all", &lock_path, e))?;
            sync_dir(path)?;
        }
        let directory = Self {
            path: path.to_path_buf(),
            keys: keys.clone(),
            _lock: lock,
            shared,
        };
        if !shared {
            directory.convert(&owner)?;
        }
        Ok(directory)
    }
    /// The entries of this directory, to the stated bound: more is refused, never listed.
    fn entries(&self) -> Result<Vec<(String, fs::FileType)>, EnrollmentError> {
        let most = max_entries();
        let mut entries = Vec::new();
        for entry in fs::read_dir(&self.path)? {
            if entries.len() >= most {
                return Err(EnrollmentError::Capacity);
            }
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| EnrollmentError::Corrupt)?;
            entries
                .try_reserve(1)
                .map_err(|_| EnrollmentError::Capacity)?;
            entries.push((name, entry.file_type()?));
        }
        Ok(entries)
    }
    /// What a writer's open settles, under its exclusive lock, so no reader or writer meets it half
    /// done: the temporaries a crash left before their rename are let go (never records), and,
    /// the first time a sealing binary's writer opens a directory an earlier binary wrote in the
    /// clear, each record's framed bytes are checked and written again sealed, through the atomic
    /// replace that writes any record, and the directory is then marked sealed. A crash before the
    /// mark leaves each record whole in one form or the other, and the next writer's open finishes.
    fn convert(&self, owner: &Owner) -> Result<(), EnrollmentError> {
        let marker = self.path.join(SEALED);
        let sealed = marker.try_exists()?;
        let mut records = Vec::new();
        let mut removed = false;
        for (name, kind) in self.entries()? {
            if name.starts_with(".pending-") {
                // A record's temporary that a crash left before its rename: never a record.
                fs::remove_file(self.path.join(&name))?;
                removed = true;
                continue;
            }
            if sealed
                || name == "LOCK"
                || name == SEALED
                || name.ends_with(".initialized")
                || !kind.is_file()
            {
                continue;
            }
            records
                .try_reserve(1)
                .map_err(|_| EnrollmentError::Capacity)?;
            records.push(name);
        }
        if removed {
            sync_dir(&self.path)?;
        }
        if sealed {
            return check_file(&marker, owner);
        }
        for name in records {
            let path = self.path.join(&name);
            check_file(&path, owner)?;
            let bytes = read_file(&path, sealed_limit()?)?;
            if bytes.get(..FRAME_MAGIC.len()) == Some(FRAME_MAGIC.as_slice()) {
                let payload = unframe(&bytes)?;
                self.install(&name, &payload)?;
            }
        }
        focal_platform::fs::create_private_new(&marker, false, true)?
            .sync_all()
            .map_err(|e| io_ctx("sealed marker sync_all", &marker, e))?;
        sync_dir(&self.path)?;
        Ok(())
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
        let bytes = read_file(&path, sealed_limit()?)?;
        let payload = open_record(&self.path, &bytes, Some(&self.keys))?;
        // The file was installed durably (write-through atomic_replace fsyncs the
        // bytes before the rename), so it is already on disk before any marker
        // claims it - there is no read-time re-sync to do, and none is portable:
        // FlushFileBuffers refuses a read-only handle on Windows, and fsync of a
        // read handle buys nothing on Unix. Only the initialization marker's own
        // crash window remains to complete.
        self.ensure_marker(name)?;
        Ok(Some(payload))
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
        if payload.len() > MAX_PAYLOAD {
            return Err(EnrollmentError::Capacity);
        }
        let path = self.path.join(name);
        let temporary = self
            .path
            .join(format!(".pending-{}", hex(&random::<16>()?)));
        let mut framed = Zeroizing::new(Vec::new());
        framed
            .try_reserve_exact(
                payload
                    .len()
                    .checked_add(FRAME_MAGIC.len() + 32)
                    .ok_or(EnrollmentError::Capacity)?,
            )
            .map_err(|_| EnrollmentError::Capacity)?;
        framed.extend_from_slice(FRAME_MAGIC);
        framed.extend_from_slice(payload);
        let checksum = blake3::hash(&framed);
        framed.extend_from_slice(checksum.as_bytes());
        // Sealed under the journal key, unwrapped for this record alone (29 §12).
        let key = self
            .keys
            .key(focal_seal::Store::Journal)
            .map_err(EnrollmentError::Keys)?;
        let bytes = focal_seal::file::seal(&key, &framed).map_err(|_| EnrollmentError::Crypto)?;
        drop(key);
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
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
/// The payload of a record's file `bytes` in the private directory `directory`, under the keys of
/// the data directory it lies in: sealed; or, before the directory's first writer under a sealing
/// binary has sealed it, in the clear as an earlier binary wrote it. Once the directory is marked
/// sealed, never in the clear. What a reader that holds no lease on the directory (a joined node's
/// local principal) opens a record with, as the directory's own reads do; a data directory with no
/// keys yet (`keys` none) holds no sealed record.
pub fn open_record(
    directory: &Path,
    bytes: &[u8],
    keys: Option<&focal_seal::DataKeys>,
) -> Result<Zeroizing<Vec<u8>>, EnrollmentError> {
    if bytes.get(..FRAME_MAGIC.len()) == Some(FRAME_MAGIC.as_slice()) {
        if directory.join(SEALED).try_exists()? {
            return Err(EnrollmentError::Corrupt);
        }
        return unframe(bytes);
    }
    let key = keys
        .ok_or(EnrollmentError::Corrupt)?
        .key(focal_seal::Store::Journal)
        .map_err(EnrollmentError::Keys)?;
    let framed = focal_seal::file::open(&key, bytes.to_vec(), MAX_FRAMED)
        .map_err(|_| EnrollmentError::Corrupt)?;
    unframe(&framed)
}
/// The most a record's file holds on its medium: its framed bytes sealed.
fn sealed_limit() -> Result<usize, EnrollmentError> {
    focal_seal::file::sealed_bound(MAX_FRAMED).ok_or(EnrollmentError::Capacity)
}
/// `path`'s bytes, at most `limit` of them: more is refused, never read.
fn read_file(path: &Path, limit: usize) -> Result<Zeroizing<Vec<u8>>, EnrollmentError> {
    let mut file = File::open(path).map_err(|e| io_ctx("read open", path, e))?;
    let most = u64::try_from(limit).map_err(|_| EnrollmentError::Capacity)?;
    if file.metadata()?.len() > most {
        return Err(EnrollmentError::Capacity);
    }
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::Read::by_ref(&mut file)
        .take(most.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(EnrollmentError::Capacity);
    }
    Ok(bytes)
}
/// The payload of a record's framed bytes: its magic, the payload and the BLAKE3 hash of both,
/// checked.
fn unframe(framed: &[u8]) -> Result<Zeroizing<Vec<u8>>, EnrollmentError> {
    if framed.len() < FRAME_MAGIC.len() + 32
        || framed.get(..FRAME_MAGIC.len()) != Some(FRAME_MAGIC.as_slice())
    {
        return Err(EnrollmentError::Corrupt);
    }
    let split = framed
        .len()
        .checked_sub(32)
        .ok_or(EnrollmentError::Corrupt)?;
    let (body, checksum) = framed
        .split_at_checked(split)
        .ok_or(EnrollmentError::Corrupt)?;
    if blake3::hash(body).as_bytes() != checksum {
        return Err(EnrollmentError::Corrupt);
    }
    Ok(Zeroizing::new(
        body.get(FRAME_MAGIC.len()..)
            .ok_or(EnrollmentError::Corrupt)?
            .to_vec(),
    ))
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
