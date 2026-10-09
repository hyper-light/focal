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
    /// Batches begun by this writer: what names a batch's unsynced files, so
    /// that two batches, or a batch and a single install, never share one.
    batches: u64,
    _writer_lock: focal_platform::FileLock,
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
        let lock = focal_platform::FileLock::exclusive(lock).map_err(|error| {
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
            batches: 0,
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
    /// Seal the chunks of one checkpoint as one batch (25 §5). Each chunk is
    /// written as it is added, to a file of its own and unsynced; `commit`
    /// then makes them durable together: every new file synced (in parallel,
    /// on a bounded number of scoped threads), each renamed under its hash,
    /// and the directory synced once. A checkpoint of N chunks costs N file
    /// syncs that overlap and one directory sync, where installing them one
    /// at a time cost a file sync, a rename and two directory syncs each,
    /// one after another, on the owner's thread. No hash is promised durable
    /// before `commit` returns; an IO failure fails the store, as `install`
    /// does.
    ///
    /// The batch owns what it writes with (the directory and its disk
    /// envelope), so it may be filled on another thread while this store's
    /// owner goes on. Its IO failure is the store's: whoever receives a
    /// batch's error fails the store ([`Self::fail`]), as `commit` here does.
    pub fn batch(&mut self) -> Result<SeedBatch, ContentError> {
        self.check()?;
        self.batches = self.batches.wrapping_add(1);
        Ok(SeedBatch {
            root: self.root.clone(),
            disk: self.disk.clone(),
            number: self.batches,
            failed: false,
            pending: PendingFiles::default(),
        })
    }
    /// A commit run away from this store failed: the store refuses until it is
    /// reopened, as after any IO failure of its own.
    pub fn fail(&mut self) {
        self.failed = true;
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

/// One checkpoint's chunks on their way to durability; see [`SeedStore::batch`].
/// Dropped before `commit`, nothing is promised and the files it wrote are
/// removed; what a crash leaves unnamed (`.batch`) the seed sweep removes.
pub struct SeedBatch {
    root: PathBuf,
    disk: DiskBudget,
    number: u64,
    /// An IO failure of this batch: it adds nothing more.
    failed: bool,
    pending: PendingFiles,
}

/// A batch's durable commit, owning everything it needs: it may run on
/// another thread while its store's owner goes on (its files are not named
/// as chunks, so nothing reads or sweeps them meanwhile). A failure must be
/// reported to the store with [`SeedStore::fail`].
pub struct SeedCommit {
    root: PathBuf,
    pending: PendingFiles,
}

/// A batch's unsynced files. Dropped before they were made durable (a batch
/// abandoned, a commit that failed or never ran), the ones still unnamed are
/// removed; the seed sweep removes what a crash leaves.
#[derive(Default)]
struct PendingFiles(Vec<PendingSeedFile>);

impl Drop for PendingFiles {
    fn drop(&mut self) {
        for file in self.0.drain(..) {
            // Absent once renamed under its hash; a failure leaves it to the sweep.
            let _ = fs::remove_file(&file.temp);
        }
    }
}

struct PendingSeedFile {
    temp: PathBuf,
    path: PathBuf,
    file: File,
    record: DiskReservation,
}

/// The most threads a batch syncs its files on: enough to keep a device's
/// queue busy (NVMe and SSD serve concurrent flushes, and a journaling
/// filesystem commits concurrent syncs together), few enough to stay a small
/// share of the host.
const SYNC_THREADS: usize = 8;

impl SeedBatch {
    /// Add one chunk: its hash, the bytes written to a file of their own (not
    /// yet durable). An identical chunk already present costs no write; a
    /// different file under the same name is corruption.
    pub fn add(&mut self, bytes: &[u8]) -> Result<ContentHash, ContentError> {
        if self.failed {
            return Err(ContentError::Failed);
        }
        if bytes.is_empty() || bytes.len() > SEED_CHUNK_BYTES {
            return Err(ContentError::Capacity);
        }
        let hash = ContentHash(*blake3::hash(bytes).as_bytes());
        let path = seed_path(&self.root, hash);
        if self.pending.0.iter().any(|p| p.path == path) {
            return Ok(hash);
        }
        let result = self.write(bytes, hash, path);
        if let Err(ContentError::Io(_)) = &result {
            self.failed = true;
        }
        result.map(|()| hash)
    }

    fn write(
        &mut self,
        bytes: &[u8],
        hash: ContentHash,
        path: PathBuf,
    ) -> Result<(), ContentError> {
        if path.exists() {
            // As `install`: identical bytes are complete, anything else is corrupt.
            return install_verified_chunk(&path, bytes, hash);
        }
        let record = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Checkpoint,
            BudgetLane::Completion,
            u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?,
        )?;
        let temp = path.with_extension(format!("{}.batch", self.number));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        self.pending
            .0
            .try_reserve(1)
            .map_err(|_| ContentError::Capacity)?;
        self.pending.0.push(PendingSeedFile {
            temp,
            path,
            file,
            record,
        });
        Ok(())
    }

    /// Make every added chunk durable here, on this thread: the files synced
    /// together, renamed, and the directory synced once. Only then are their
    /// hashes promised.
    pub fn commit(self, store: &mut SeedStore) -> Result<(), ContentError> {
        let result = self.detach().run();
        if result.is_err() {
            store.fail();
        }
        result
    }

    /// The batch's commit, to run away from the store's owner: the chunks are
    /// promised only once it returns `Ok`.
    pub fn detach(self) -> SeedCommit {
        let SeedBatch { root, pending, .. } = self;
        SeedCommit { root, pending }
    }
}

impl SeedCommit {
    /// Make every chunk of the batch durable; see [`SeedBatch::commit`].
    pub fn run(self) -> Result<(), ContentError> {
        let SeedCommit { root, mut pending } = self;
        Self::durable(&root, &pending.0)?;
        for file in pending.0.drain(..) {
            file.record.commit();
        }
        Ok(())
    }

    /// How many chunks the commit makes durable.
    pub fn chunks(&self) -> usize {
        self.pending.0.len()
    }

    fn durable(root: &Path, pending: &[PendingSeedFile]) -> Result<(), ContentError> {
        if pending.is_empty() {
            return Ok(());
        }
        let threads = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .clamp(1, SYNC_THREADS)
            .min(pending.len());
        let per = pending.len().div_ceil(threads.max(1)).max(1);
        std::thread::scope(|scope| -> Result<(), ContentError> {
            let mut workers = Vec::new();
            workers
                .try_reserve_exact(threads)
                .map_err(|_| ContentError::Capacity)?;
            for share in pending.chunks(per) {
                let worker = std::thread::Builder::new()
                    .name("focal-seed-sync".into())
                    .spawn_scoped(scope, move || -> std::io::Result<()> {
                        for file in share {
                            file.file.sync_all()?;
                        }
                        Ok(())
                    })?;
                workers.push(worker);
            }
            for worker in workers {
                // A sync thread that did not return is a failed sync.
                worker
                    .join()
                    .map_err(|_| ContentError::Failed)?
                    .map_err(ContentError::Io)?;
            }
            Ok(())
        })?;
        for file in pending {
            focal_platform::fs::atomic_replace(&file.temp, &file.path)?;
        }
        sync_directory(root)?;
        Ok(())
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
    fn a_batch_promises_its_chunks_only_once_committed_and_skips_what_is_there() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("seeds");
        let mut store = SeedStore::open(&root, disk()).unwrap();
        let present = vec![1u8; 4096];
        let present_hash = store.install(&present).unwrap();
        let chunks: Vec<Vec<u8>> = (0..20u8).map(|n| vec![n.wrapping_add(2); 8192]).collect();
        // Uncommitted: nothing is named, so nothing is promised.
        {
            let mut batch = store.batch().unwrap();
            for chunk in &chunks {
                batch.add(chunk).unwrap();
            }
        }
        for chunk in &chunks {
            assert!(!store.contains(ContentHash(*blake3::hash(chunk).as_bytes())));
        }
        // Committed: every chunk named and verified on read, the one already
        // present taken as it is, the same chunk twice in a batch once.
        let mut batch = store.batch().unwrap();
        let mut hashes = Vec::new();
        for chunk in &chunks {
            hashes.push(batch.add(chunk).unwrap());
        }
        assert_eq!(batch.add(&present).unwrap(), present_hash);
        assert_eq!(batch.add(&chunks[0]).unwrap(), hashes[0]);
        assert!(matches!(batch.add(&[]), Err(ContentError::Capacity)));
        // Filled anywhere: the batch owns what it writes with.
        fn sendable<T: Send>() {}
        sendable::<SeedBatch>();
        batch.commit(&mut store).unwrap();
        for (chunk, hash) in chunks.iter().zip(&hashes) {
            assert_eq!(store.read(*hash, SEED_CHUNK_BYTES).unwrap(), *chunk);
        }
        assert_eq!(store.read(present_hash, 4096).unwrap(), present);
        // No unnamed file is left beside the chunks.
        let leftovers = std::fs::read_dir(&root)
            .unwrap()
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.extension()
                    .is_some_and(|x| x == "install" || x == "batch")
            })
            .count();
        assert_eq!(leftovers, 0);
        // A different file under a chunk's name is corruption, as for `install`.
        let path = seed_path(&root, hashes[1]);
        std::fs::write(&path, b"not the chunk").unwrap();
        let mut batch = store.batch().unwrap();
        assert!(matches!(batch.add(&chunks[1]), Err(ContentError::Corrupt)));
    }

    #[test]
    fn a_detached_commit_runs_on_another_thread_and_promises_only_when_done() {
        fn sendable<T: Send>(_: &T) {}
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("seeds");
        let mut store = SeedStore::open(&root, disk()).unwrap();
        let chunks: Vec<Vec<u8>> = (0..6u8)
            .map(|n| vec![n.wrapping_mul(3); 16 * 1024])
            .collect();
        let mut batch = store.batch().unwrap();
        let hashes: Vec<ContentHash> = chunks.iter().map(|c| batch.add(c).unwrap()).collect();
        let commit = batch.detach();
        sendable(&commit);
        assert_eq!(commit.chunks(), chunks.len());
        // Before the commit returns nothing is named; the owner may go on.
        assert!(hashes.iter().all(|h| !store.contains(*h)));
        let done = std::thread::scope(|scope| scope.spawn(move || commit.run()).join().unwrap());
        done.unwrap();
        for (chunk, hash) in chunks.iter().zip(&hashes) {
            assert_eq!(store.read(*hash, SEED_CHUNK_BYTES).unwrap(), *chunk);
        }
        // A commit that failed away from the store fails it through its owner.
        store.fail();
        assert!(matches!(store.batch(), Err(ContentError::Failed)));
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
