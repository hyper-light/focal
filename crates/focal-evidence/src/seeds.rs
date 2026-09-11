//! Checkpoint seeds (25 §5): the rows of a native checkpoint too large for one
//! consensus snapshot travel as content-addressed chunks of at most one
//! mebibyte. The author's session seals them here before it proposes the
//! snapshot that names them; an installing replica reads them here, pulling
//! any it lacks from a peer first. Chunks are immutable, verified by their
//! hash on every read, retained across reopen and removed only by an
//! explicit retention decision. One writer holds the directory; readers
//! share it, since every install is a durable atomic rename.
use super::*;

/// The most bytes one seed chunk holds.
pub const SEED_CHUNK_BYTES: usize = 1024 * 1024;

/// The one writer of a seed directory.
pub struct SeedStore {
    pub(super) root: PathBuf,
    disk: DiskBudget,
    pub(super) failed: bool,
    /// A seed sweep in progress (26 §5).
    pub(super) sweep: Option<std::fs::ReadDir>,
    _writer_lock: File,
}

/// A read-only view of a seed directory another owner writes.
#[derive(Clone)]
pub struct SeedReader {
    root: PathBuf,
}

impl SeedStore {
    /// Open or create the directory, holding its writer lock.
    pub fn open(root: impl AsRef<Path>, disk: DiskBudget) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        durable_directory(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("LOCK"))?;
        focal_platform::try_lock_exclusive(&lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                ContentError::Locked
            } else {
                ContentError::Io(error)
            }
        })?;
        Ok(Self {
            root,
            disk,
            failed: false,
            sweep: None,
            _writer_lock: lock,
        })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn reader(&self) -> SeedReader {
        SeedReader {
            root: self.root.clone(),
        }
    }
    pub(super) fn check(&self) -> Result<(), ContentError> {
        if self.failed {
            return Err(ContentError::Failed);
        }
        Ok(())
    }
    /// Seal one chunk under its hash. Disk is promised before the write; an
    /// identical chunk already present is complete; a differing file under
    /// the same name is corruption. An IO failure never returns a hash and
    /// fails the store until it is reopened.
    pub fn install(&mut self, bytes: &[u8]) -> Result<ContentHash, ContentError> {
        self.check()?;
        if bytes.is_empty() || bytes.len() > SEED_CHUNK_BYTES {
            return Err(ContentError::Capacity);
        }
        let hash = ContentHash(*blake3::hash(bytes).as_bytes());
        let path = seed_path(&self.root, hash);
        let record = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Checkpoint,
            BudgetLane::Completion,
            u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?,
        )?;
        if let Err(error) = install_verified_chunk(&path, bytes, hash) {
            if matches!(error, ContentError::Io(_)) {
                self.failed = true;
            }
            return Err(error);
        }
        record.commit();
        Ok(hash)
    }
    /// Verify a chunk another owner wrote under `hash`, taking it as the
    /// author would have: refused when it does not hash to `hash`.
    pub fn install_as(&mut self, hash: ContentHash, bytes: &[u8]) -> Result<(), ContentError> {
        if ContentHash(*blake3::hash(bytes).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        self.install(bytes).map(|_| ())
    }
    pub fn contains(&self, hash: ContentHash) -> bool {
        seed_path(&self.root, hash).is_file()
    }
    pub fn read(&self, hash: ContentHash, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        read_seed(&self.root, hash, max_bytes)
    }
    /// Remove one chunk; absent is complete. A retention decision, never a
    /// side effect of a read or an install.
    pub fn remove(&mut self, hash: ContentHash) -> Result<(), ContentError> {
        self.check()?;
        let path = seed_path(&self.root, hash);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                self.failed = true;
                return Err(ContentError::Io(error));
            }
        }
        if let Err(error) = sync_directory(&self.root) {
            self.failed = true;
            return Err(ContentError::Io(error));
        }
        Ok(())
    }
}

impl SeedReader {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(ContentError::Invalid);
        }
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn contains(&self, hash: ContentHash) -> bool {
        seed_path(&self.root, hash).is_file()
    }
    pub fn read(&self, hash: ContentHash, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        read_seed(&self.root, hash, max_bytes)
    }
}

fn seed_path(root: &Path, hash: ContentHash) -> PathBuf {
    root.join(format!("{hash}.seed"))
}

fn read_seed(root: &Path, hash: ContentHash, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
    let bytes = read_bounded(&seed_path(root, hash), max_bytes.min(SEED_CHUNK_BYTES))?;
    if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
        return Err(ContentError::Corrupt);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk() -> DiskBudget {
        DiskBudget::new(DiskBudgetConfig::default()).unwrap()
    }

    #[test]
    fn seeds_are_sealed_read_back_verified_and_removed_only_on_purpose() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("seeds");
        let mut store = SeedStore::open(&root, disk()).unwrap();
        assert!(matches!(
            SeedStore::open(&root, disk()),
            Err(ContentError::Locked)
        ));
        let chunk = vec![7u8; 4096];
        let hash = store.install(&chunk).unwrap();
        assert_eq!(hash, ContentHash(*blake3::hash(&chunk).as_bytes()));
        assert!(store.contains(hash));
        assert_eq!(store.read(hash, 4096).unwrap(), chunk);
        // Too small a read allowance, an empty chunk and an oversized one.
        assert!(matches!(
            store.read(hash, 4095),
            Err(ContentError::Capacity)
        ));
        assert!(matches!(store.install(&[]), Err(ContentError::Capacity)));
        assert!(matches!(
            store.install(&vec![1u8; SEED_CHUNK_BYTES + 1]),
            Err(ContentError::Capacity)
        ));
        // The same chunk again is complete; another owner's copy is taken
        // only when it hashes right.
        assert_eq!(store.install(&chunk).unwrap(), hash);
        assert!(matches!(
            store.install_as(hash, &[1, 2, 3]),
            Err(ContentError::Corrupt)
        ));
        store.install_as(hash, &chunk).unwrap();
        // A reader shares the directory and verifies every byte.
        let reader = store.reader();
        assert!(reader.contains(hash));
        assert_eq!(reader.read(hash, SEED_CHUNK_BYTES).unwrap(), chunk);
        let missing = ContentHash([9; 32]);
        assert!(!reader.contains(missing));
        assert!(matches!(
            reader.read(missing, SEED_CHUNK_BYTES),
            Err(ContentError::Io(_))
        ));
        // A corrupted file is refused, never returned.
        std::fs::write(root.join(format!("{hash}.seed")), b"corrupt").unwrap();
        assert!(matches!(
            reader.read(hash, SEED_CHUNK_BYTES),
            Err(ContentError::Corrupt)
        ));
        assert!(matches!(store.install(&chunk), Err(ContentError::Corrupt)));
        // Removal is explicit and idempotent; the chunk survives reopen until then.
        drop(store);
        let mut reopened = SeedStore::open(&root, disk()).unwrap();
        assert!(reopened.contains(hash));
        reopened.remove(hash).unwrap();
        assert!(!reopened.contains(hash));
        reopened.remove(hash).unwrap();
        let again = reopened.install(&chunk).unwrap();
        assert_eq!(again, hash);
        assert_eq!(
            SeedReader::open(&root)
                .unwrap()
                .read(hash, SEED_CHUNK_BYTES)
                .unwrap(),
            chunk
        );
        assert!(matches!(
            SeedReader::open(directory.path().join("absent")),
            Err(ContentError::Invalid)
        ));
    }
}
