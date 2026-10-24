//! Checkpoint seeds (25 §5): the rows of a native checkpoint too large for one
//! consensus snapshot travel as content-addressed chunks of at most one
//! mebibyte. The author's session seals them here before it proposes the
//! snapshot that names them; an installing replica reads them here, pulling
//! any it lacks from a peer first. Chunks are immutable, verified by their
//! hash on every read, retained across reopen and removed only by an
//! explicit retention decision. One writer holds the directory; readers
//! share it, since every install is a durable atomic rename.
//!
//! At rest (29 §12): below the at-rest level a chunk lies in the clear, named by its hash, byte for
//! byte as every earlier binary wrote it, so a node can still be rolled back while the fence is
//! below the level. From the level the directory has a key of its own, random, kept as its record
//! under the data directory's group key (`DOMAIN.key`); each chunk is sealed whole, keyed by its
//! hash (`SealedWriter::content_keyed`, the BLAKE3 hash it is named by, so its bytes are passed over
//! once), and named by the directory's keyed name of that hash: no name or byte of the directory
//! holds a chunk's hash or plaintext, and destroying the record erases every chunk of it. A
//! directory that holds chunks in the clear when its writer is asked to seal is marked as
//! converting (`SEALING`) and converted a chunk a step while it serves ([`SeedStore::convert_step`],
//! driven by the node's collector), then marked sealed (`SEALED`). Until then every lookup looks in
//! the clear first and then under the keyed name, so a reader finds a chunk whatever the conversion
//! does between; a directory that holds nothing in the clear is sealed as it opens.
//!
//! Key slots (`focal_seal::key_slots`, a hard bound for the whole process): a writer or reader
//! unwraps the directory's key for the one chunk it writes or reads, and a writer lets it go before
//! it writes the chunk's bytes, so a node's sessions hold no slot between their writes.
use super::chunk_convert::{ChunkConversion, ChunkDirectory, SeedJobDone, SeedStep, clear_hash};
use super::convert::{self, Conversion, ConversionStatus};
use super::sealing::{self, ContentForm, ContentKeys, Hashed, Kind};
use super::*;

/// The most bytes one seed chunk holds.
pub const SEED_CHUNK_BYTES: usize = 1024 * 1024;

/// The identity every seed directory's key and names are made under: each directory has a key of
/// its own, so one identity serves them all, and a directory's names depend on nothing of its path.
const SEED_DOMAIN: ContentDomainId = ContentDomainId(*b"focal seed chunk");

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
    /// The directory's keys and the forms its chunks may lie in (29 §12).
    pub(super) keys: ContentKeys<'static>,
    /// The data directory's keys, which every reader this store makes takes.
    data_keys: focal_seal::DataKeys,
    /// The conversion of the chunks in the clear, while one runs ([`Self::conversion_job`]), and
    /// its progress since this open.
    conversion: ChunkConversion,
    _writer_lock: focal_platform::FileLock,
}

/// A read-only view of a seed directory another owner writes.
#[derive(Clone)]
pub struct SeedReader {
    root: PathBuf,
    keys: focal_seal::DataKeys,
    /// Whether a chunk may lie in the clear: the directory was not marked sealed when the reader
    /// was made.
    clear: bool,
}

impl SeedStore {
    /// Open or create the directory, holding its writer lock, its chunks installed in `form`: in
    /// the clear below the at-rest level, sealed from it (29 §12). A directory marked sealed stays
    /// sealed, and one whose conversion began finishes it, whatever `form` asks; one asked to seal
    /// that holds chunks in the clear is marked as converting and converted as it serves
    /// ([`Self::convert_step`]), and one that holds none is sealed at once. `keys` are the data
    /// directory's, under whose group key the directory's own key is kept.
    pub fn open(
        root: impl AsRef<Path>,
        disk: DiskBudget,
        keys: &focal_seal::DataKeys,
        form: ContentForm,
    ) -> Result<Self, ContentError> {
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
        let sealed = root.join(sealing::SEALED).try_exists()?;
        let begun = root.join(sealing::SEALING).try_exists()?;
        let group = focal_seal::Store::Group;
        let mut conversion = ChunkConversion::default();
        let directory_keys = if sealed {
            // A conversion that ended before its marker went.
            convert::remove_marker(&root, sealing::SEALING)?;
            ContentKeys::per_record(keys.clone(), group, false)
        } else if begun || form == ContentForm::Sealed {
            if !begun && !super::chunk_convert::holds_clear(&root)? {
                // Nothing in the clear to convert: sealed as it opens, no reader having been
                // told it may find a chunk in the clear.
                convert::write_marker(&root, sealing::SEALED)?;
                ContentKeys::per_record(keys.clone(), group, false)
            } else {
                // Marked before anything is sealed: from here the directory holds chunks in either
                // form, and only a writer that reads both may open it.
                if !begun {
                    convert::write_marker(&root, sealing::SEALING)?;
                }
                conversion = ChunkConversion::begun(begun);
                ContentKeys::per_record(keys.clone(), group, true)
            }
        } else {
            ContentKeys::clear()
        };
        Ok(Self {
            root,
            disk,
            failed: false,
            sweep: None,
            batches: 0,
            keys: directory_keys,
            data_keys: keys.clone(),
            conversion,
            _writer_lock: lock,
        })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn reader(&self) -> SeedReader {
        SeedReader {
            root: self.root.clone(),
            keys: self.data_keys.clone(),
            clear: self.keys.clear_allowed(),
        }
    }
    pub(super) fn check(&self) -> Result<(), ContentError> {
        if self.failed {
            return Err(ContentError::Failed);
        }
        Ok(())
    }
    /// The directory's key for one chunk: unwrapped from its record, which a writer that seals
    /// makes where none stands only once it has promised its bytes ([`Self::writable`]).
    pub(super) fn key(&self) -> Result<sealing::DomainKey, ContentError> {
        self.keys.domain(&self.root, SEED_DOMAIN, false)
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
        let seed = Hashed::of(bytes);
        let result = self.install_hashed(&seed);
        if let Err(ContentError::Io(_)) = &result {
            self.failed = true;
        }
        result.map(|()| seed.hash())
    }
    fn install_hashed(&mut self, seed: &Hashed<'_>) -> Result<(), ContentError> {
        let key = self.key()?;
        if present(&key, &self.root, seed)? {
            return Ok(());
        }
        let record = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Checkpoint,
            BudgetLane::Completion,
            key.file_bytes(seed.bytes().len())?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        )?;
        let key = self.keys.writable(&self.root, SEED_DOMAIN, key)?;
        let path = key.path(&self.root, seed.hash(), Kind::Seed)?;
        let writer = key.seed_writer(&path.with_extension("install"), *seed)?;
        // The directory's key goes before the chunk's bytes are written.
        drop(key);
        writer.install(&path)?;
        record.commit();
        Ok(())
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
    /// The batch owns what it writes with (the directory, its keys and its disk
    /// envelope), so it may be filled on another thread while this store's
    /// owner goes on. Its IO failure is the store's: whoever receives a
    /// batch's error fails the store ([`Self::fail`]), as `commit` here does.
    pub fn batch(&mut self) -> Result<SeedBatch, ContentError> {
        self.check()?;
        self.batches = self.batches.wrapping_add(1);
        Ok(SeedBatch {
            root: self.root.clone(),
            disk: self.disk.clone(),
            keys: self.keys.owned_copy()?,
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
        self.key()
            .and_then(|key| key.exists(&self.root, hash, Kind::Seed))
            .unwrap_or(false)
    }
    pub fn read(&self, hash: ContentHash, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        self.key()?.read_sized(
            &self.root,
            hash,
            Kind::Seed,
            max_bytes.min(SEED_CHUNK_BYTES),
        )
    }
    /// Remove one chunk; absent is complete. A retention decision, never a
    /// side effect of a read or an install. While the conversion counts, a
    /// chunk in the clear it removes sets the count going again from the
    /// start, so the count stays exact; once it has counted, it is tallied as
    /// gone.
    pub fn remove(&mut self, hash: ContentHash) -> Result<(), ContentError> {
        self.check()?;
        let key = self.key()?;
        let Some(path) = key.existing(&self.root, hash, Kind::Seed)? else {
            return Ok(());
        };
        let clear = super::chunk_convert::clear_seed(&path)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                self.failed = true;
                return Err(ContentError::Io(error));
            }
        }
        if let Some(bytes) = clear {
            self.removed_clear(bytes);
        }
        if let Err(error) = sync_directory(&self.root) {
            self.failed = true;
            return Err(ContentError::Io(error));
        }
        Ok(())
    }
    /// A chunk in the clear of `bytes` left the directory other than by the conversion: counted
    /// again from the start while the count runs (whether the count met it is not known), tallied
    /// as gone once it has counted.
    pub(super) fn removed_clear(&mut self, bytes: u64) {
        self.conversion.removed_clear(bytes);
    }
    /// The keyed marks of `protected`, sorted: what the seed sweep keeps a sealed chunk by. None
    /// where the directory holds no sealed chunk (its writer keeps the clear, or its key's record
    /// does not stand).
    pub(super) fn protected_marks(
        &self,
        protected: &[ContentHash],
    ) -> Result<Vec<sealing::Mark>, ContentError> {
        let mut marks = Vec::new();
        if !self.keys.seals() {
            return Ok(marks);
        }
        let key = self.key()?;
        if key.record_bytes() != 0 {
            return Ok(marks);
        }
        marks
            .try_reserve_exact(protected.len())
            .map_err(|_| ContentError::Capacity)?;
        for hash in protected {
            marks.push(key.mark(*hash)?);
        }
        marks.sort_unstable();
        Ok(marks)
    }
    /// Whether the conversion is counting the chunks in the clear: the seed sweep removes none of
    /// them meanwhile, so the count is exact.
    pub(super) fn conversion_counting(&self) -> bool {
        self.conversion.counting()
    }
    /// The form the directory is in and its conversion's progress since this open (29 §12), as
    /// [`ContentStore::conversion_status`] gives the content store's.
    pub fn conversion_status(&self) -> ConversionStatus {
        self.conversion.status(&self.keys)
    }
    /// The next piece of the directory's conversion, at most `visits` entries visited. While it
    /// counts the chunks in the clear, the count goes on ([`SeedStep::Visited`]); once counted, the
    /// next at most `chunks` chunks in the clear are handed out as a job ([`SeedStep::Job`]) whose
    /// heavy part (each chunk read, sealed beside itself under its keyed name, the files flushed
    /// together, renamed, the directory flushed) runs off the owner's thread ([`SeedJob::run`]),
    /// and whose result the owner takes back ([`Self::conversion_done`]), removing the converted
    /// chunks' names in the clear. A job handed before whose result never came back has its chunks
    /// taken again, first: they are still in the clear, and a sealed form it left is found and
    /// kept. The pass ended with no chunk out, the directory is flushed and marked sealed
    /// ([`SeedStep::Done`]).
    pub fn conversion_job(
        &mut self,
        visits: usize,
        chunks: usize,
    ) -> Result<SeedStep, ContentError> {
        self.check()?;
        let directory = ChunkDirectory {
            root: &self.root,
            keys: &mut self.keys,
            disk: &self.disk,
            kind: DiskKind::Checkpoint,
            domain: SEED_DOMAIN,
            jobs: &mut self.batches,
        };
        let result = self.conversion.job(directory, visits, chunks);
        if let Err(ContentError::Io(_)) = &result {
            self.failed = true;
        }
        result
    }
    /// A job's result taken back ([`Self::conversion_job`]): the name in the clear of every chunk
    /// whose sealed form it made durable removed, each such chunk tallied converted once (one the
    /// sweep removed first was tallied gone then); the chunks it had no room for taken first next;
    /// a job that failed ends this conversion with its cause, and an IO failure fails the store, as
    /// a batch's does. What it did, as a step of the conversion.
    pub fn conversion_done(&mut self, done: SeedJobDone) -> Result<Conversion, ContentError> {
        self.check()?;
        let result = self.conversion.done(done);
        if let Err(ContentError::Io(_)) = &result {
            self.failed = true;
        }
        result
    }
    /// One step of the directory's conversion run here, on the owner's thread: at most `visits`
    /// entries visited and at most one chunk converted, read into the head of `scratch`, which must
    /// hold a chunk ([`SEED_CHUNK_BYTES`]). What a caller with no other thread to run a job on does
    /// ([`Self::conversion_job`] says what a step is).
    pub fn convert_step(
        &mut self,
        scratch: &mut [u8],
        visits: usize,
    ) -> Result<Conversion, ContentError> {
        match self.conversion_job(visits, 1)? {
            SeedStep::Done => Ok(Conversion::Done),
            SeedStep::Visited => Ok(Conversion::Visited),
            SeedStep::Job(job) => {
                let done = job.run(scratch);
                self.conversion_done(done)
            }
        }
    }
}

/// Whether a file filed under `seed`'s hash already opens, in either form, to its bytes: kept
/// then, its freshness refreshed, so the sweep's grace counts from this install. One that opens to
/// anything else, or not at all, is corrupt under a content-addressed name and refused.
fn present(key: &sealing::DomainKey, root: &Path, seed: &Hashed<'_>) -> Result<bool, ContentError> {
    if !key.exists(root, seed.hash(), Kind::Seed)? {
        return Ok(false);
    }
    let mut previous = zeroed_buffer(seed.bytes().len())?;
    let (path, opened) = key.opened(
        root,
        seed.hash(),
        Kind::Seed,
        seed.bytes().len(),
        &mut previous,
    )?;
    if opened != seed.bytes() {
        return Err(ContentError::Corrupt);
    }
    refresh_mtime(&path)?;
    Ok(true)
}

impl SeedReader {
    /// A reader of the seed directory at `root`, its chunks' keys under the data directory's
    /// `keys`.
    pub fn open(root: impl AsRef<Path>, keys: focal_seal::DataKeys) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(ContentError::Invalid);
        }
        let clear = !root.join(sealing::SEALED).try_exists()?;
        Ok(Self { root, keys, clear })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn keys(&self) -> ContentKeys<'_> {
        ContentKeys::reading_under(&self.keys, focal_seal::Store::Group, self.clear)
    }
    pub fn contains(&self, hash: ContentHash) -> bool {
        self.opened()
            .and_then(|opened| opened.contains(hash))
            .unwrap_or(false)
    }
    pub fn read(&self, hash: ContentHash, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        self.keys()
            .domain(&self.root, SEED_DOMAIN, false)?
            .read_sized(
                &self.root,
                hash,
                Kind::Seed,
                max_bytes.min(SEED_CHUNK_BYTES),
            )
    }
    /// The hash of every chunk the directory holds, in either form, sorted: a name in the clear is
    /// its hash (while the directory may hold one); a sealed file is opened and its bytes hashed,
    /// and refused where its name is not the directory's keyed name of that hash. Every chunk is
    /// read whole, one at a time: an operator's or a test's look at a directory, never a session's
    /// path.
    pub fn chunks(&self) -> Result<Vec<ContentHash>, ContentError> {
        let keys = self.keys();
        let key = keys.domain(&self.root, SEED_DOMAIN, false)?;
        let mut chunks = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str())
                != Some(Kind::Seed.extension())
            {
                continue;
            }
            let hash = match clear_hash(&path) {
                Some(hash) if self.clear => Some(hash),
                Some(_) => None,
                None if key.record_bytes() == 0 => key
                    .open_listed(&path, SEED_CHUNK_BYTES)?
                    .map(|(_, hash)| hash),
                None => None,
            };
            if let Some(hash) = hash {
                chunks.try_reserve(1).map_err(|_| ContentError::Capacity)?;
                chunks.push(hash);
            }
        }
        chunks.sort_unstable();
        chunks.dedup();
        Ok(chunks)
    }
    /// The directory with its key unwrapped once, for a pass over many chunks' names (which of a
    /// checkpoint's chunks are local): it holds a key slot until it is let go.
    pub fn opened(&self) -> Result<OpenedSeeds<'_>, ContentError> {
        Ok(OpenedSeeds {
            root: &self.root,
            key: self.keys().domain(&self.root, SEED_DOMAIN, false)?,
        })
    }
}

/// A seed directory with its key unwrapped ([`SeedReader::opened`]).
pub struct OpenedSeeds<'r> {
    root: &'r Path,
    key: sealing::DomainKey,
}

impl OpenedSeeds<'_> {
    /// Whether a chunk filed under `hash` lies in the directory, in either form.
    pub fn contains(&self, hash: ContentHash) -> Result<bool, ContentError> {
        self.key.exists(self.root, hash, Kind::Seed)
    }
}

/// One checkpoint's chunks on their way to durability; see [`SeedStore::batch`].
/// Dropped before `commit`, nothing is promised and the files it wrote are
/// removed; what a crash leaves unnamed (`.batch`) the seed sweep removes.
pub struct SeedBatch {
    root: PathBuf,
    disk: DiskBudget,
    /// The directory's keys, for each chunk it seals.
    keys: ContentKeys<'static>,
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
pub(super) struct PendingFiles(pub(super) Vec<PendingSeedFile>);

impl Drop for PendingFiles {
    fn drop(&mut self) {
        for file in self.0.drain(..) {
            // Absent once renamed under its hash; a failure leaves it to the sweep.
            let _ = fs::remove_file(&file.temp);
        }
    }
}

pub(super) struct PendingSeedFile {
    pub(super) hash: ContentHash,
    pub(super) temp: PathBuf,
    pub(super) path: PathBuf,
    pub(super) file: File,
    pub(super) record: DiskReservation,
}

/// The most threads a batch syncs its files on: enough to keep a device's
/// queue busy (NVMe and SSD serve concurrent flushes, and a journaling
/// filesystem commits concurrent syncs together), few enough to stay a small
/// share of the host.
pub(super) const SYNC_THREADS: usize = 8;

impl SeedBatch {
    /// Add one chunk: its hash, the bytes written to a file of their own (not
    /// yet durable), sealed where the directory seals. An identical chunk
    /// already present costs no write; a different file under the same name
    /// is corruption.
    pub fn add(&mut self, bytes: &[u8]) -> Result<ContentHash, ContentError> {
        if self.failed {
            return Err(ContentError::Failed);
        }
        if bytes.is_empty() || bytes.len() > SEED_CHUNK_BYTES {
            return Err(ContentError::Capacity);
        }
        let seed = Hashed::of(bytes);
        let result = self.write(seed);
        if let Err(ContentError::Io(_)) = &result {
            self.failed = true;
        }
        result.map(|()| seed.hash())
    }

    fn write(&mut self, seed: Hashed<'_>) -> Result<(), ContentError> {
        if self.pending.0.iter().any(|p| p.hash == seed.hash()) {
            return Ok(());
        }
        let key = self.keys.domain(&self.root, SEED_DOMAIN, false)?;
        // As `install`: identical bytes are complete, anything else is corrupt.
        if present(&key, &self.root, &seed)? {
            return Ok(());
        }
        let record = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Checkpoint,
            BudgetLane::Completion,
            key.file_bytes(seed.bytes().len())?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        )?;
        self.pending
            .0
            .try_reserve(1)
            .map_err(|_| ContentError::Capacity)?;
        // The directory's record made first where it seals and none stands: a keyed name needs it.
        let key = self.keys.writable(&self.root, SEED_DOMAIN, key)?;
        let path = key.path(&self.root, seed.hash(), Kind::Seed)?;
        let temp = path.with_extension(format!("{}.batch", self.number));
        let writer = key.seed_writer(&temp, seed)?;
        // The directory's key goes before the chunk's bytes are written: the writer seals with a
        // key of its own, derived as it was made, and a session holds no slot while it writes.
        drop(key);
        let file = writer.finish()?;
        self.pending.0.push(PendingSeedFile {
            hash: seed.hash(),
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
    /// The chunks this commit makes durable: those its batch wrote, not the
    /// ones it found already sealed.
    pub fn written(&self) -> usize {
        self.pending.0.len()
    }
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

    pub(super) fn durable(root: &Path, pending: &[PendingSeedFile]) -> Result<(), ContentError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn disk() -> DiskBudget {
        DiskBudget::new(DiskBudgetConfig::default()).unwrap()
    }

    const FORMS: [ContentForm; 2] = [ContentForm::Clear, ContentForm::Sealed];

    fn keys(data: &Path) -> focal_seal::DataKeys {
        crate::store::test_keys(data)
    }

    /// The seed directory `name` of the data directory `data`, opened in `form`.
    fn open(data: &Path, name: &str, form: ContentForm) -> (PathBuf, SeedStore) {
        let root = data.join(name);
        let store = SeedStore::open(&root, disk(), &keys(data), form).unwrap();
        (root, store)
    }

    /// The file the chunk filed under `hash` lies in, in whichever form.
    fn file_of(store: &SeedStore, hash: ContentHash) -> PathBuf {
        store
            .key()
            .unwrap()
            .existing(&store.root, hash, Kind::Seed)
            .unwrap()
            .unwrap()
    }

    /// Plaintext no stored byte may repeat: long and patterned, so a copy of any 16 bytes shows.
    fn plaintext(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(31) ^ (i >> 8)) as u8 ^ seed)
            .collect()
    }

    fn names(root: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// The chunks in the clear a directory holds, by name.
    fn clear_names(root: &Path) -> usize {
        names(root)
            .iter()
            .filter(|name| {
                name.strip_suffix(".seed")
                    .is_some_and(|stem| sealing::parse_hash(stem).is_some())
            })
            .count()
    }

    #[test]
    fn a_batch_promises_its_chunks_only_once_committed_and_skips_what_is_there() {
        for form in FORMS {
            let directory = tempfile::tempdir().unwrap();
            let (root, mut store) = open(directory.path(), "seeds", form);
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
            let leftovers = names(&root)
                .iter()
                .filter(|name| name.ends_with(".install") || name.ends_with(".batch"))
                .count();
            assert_eq!(leftovers, 0, "{form:?}");
            // A different file under a chunk's name is corruption, as for `install`.
            std::fs::write(file_of(&store, hashes[1]), b"not the chunk").unwrap();
            let mut batch = store.batch().unwrap();
            assert!(
                matches!(batch.add(&chunks[1]), Err(ContentError::Corrupt)),
                "{form:?}"
            );
        }
    }

    #[test]
    fn a_detached_commit_runs_on_another_thread_and_promises_only_when_done() {
        fn sendable<T: Send>(_: &T) {}
        for form in FORMS {
            let directory = tempfile::tempdir().unwrap();
            let (_, mut store) = open(directory.path(), "seeds", form);
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
            let done =
                std::thread::scope(|scope| scope.spawn(move || commit.run()).join().unwrap());
            done.unwrap();
            for (chunk, hash) in chunks.iter().zip(&hashes) {
                assert_eq!(store.read(*hash, SEED_CHUNK_BYTES).unwrap(), *chunk);
            }
            // A commit that failed away from the store fails it through its owner.
            store.fail();
            assert!(matches!(store.batch(), Err(ContentError::Failed)));
        }
    }

    #[test]
    fn seeds_are_sealed_read_back_verified_and_removed_only_on_purpose() {
        for form in FORMS {
            let directory = tempfile::tempdir().unwrap();
            let (root, mut store) = open(directory.path(), "seeds", form);
            assert!(matches!(
                SeedStore::open(&root, disk(), &keys(directory.path()), form),
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
            std::fs::write(file_of(&store, hash), b"corrupt").unwrap();
            assert!(matches!(
                reader.read(hash, SEED_CHUNK_BYTES),
                Err(ContentError::Corrupt)
            ));
            assert!(matches!(store.install(&chunk), Err(ContentError::Corrupt)));
            // Removal is explicit and idempotent; the chunk survives reopen until then.
            drop(store);
            let mut reopened =
                SeedStore::open(&root, disk(), &keys(directory.path()), form).unwrap();
            assert!(reopened.contains(hash));
            reopened.remove(hash).unwrap();
            assert!(!reopened.contains(hash));
            reopened.remove(hash).unwrap();
            let again = reopened.install(&chunk).unwrap();
            assert_eq!(again, hash);
            assert_eq!(
                SeedReader::open(&root, keys(directory.path()))
                    .unwrap()
                    .read(hash, SEED_CHUNK_BYTES)
                    .unwrap(),
                chunk
            );
            assert!(matches!(
                SeedReader::open(directory.path().join("absent"), keys(directory.path())),
                Err(ContentError::Invalid)
            ));
        }
    }

    /// Below the at-rest level a seed directory is byte for byte what every earlier binary wrote: a
    /// chunk under its hash holding its bytes, no key record and no marker (29 §12).
    #[test]
    fn the_clear_form_writes_the_layout_earlier_binaries_wrote() {
        let directory = tempfile::tempdir().unwrap();
        let (root, mut store) = open(directory.path(), "seeds", ContentForm::Clear);
        let one = plaintext(1, 5000);
        let two = plaintext(2, 9000);
        let hash = store.install(&one).unwrap();
        let mut batch = store.batch().unwrap();
        let other = batch.add(&two).unwrap();
        batch.commit(&mut store).unwrap();
        assert_eq!(
            std::fs::read(root.join(format!("{hash}.seed"))).unwrap(),
            one
        );
        assert_eq!(
            std::fs::read(root.join(format!("{other}.seed"))).unwrap(),
            two
        );
        let mut expected = vec![
            "LOCK".to_owned(),
            format!("{hash}.seed"),
            format!("{other}.seed"),
        ];
        expected.sort();
        assert_eq!(names(&root), expected);
        assert_eq!(store.conversion_status().form, StoreForm::Clear);
    }

    /// From the level, no name or byte of a seed directory holds a chunk's hash or plaintext; a
    /// flipped byte, a cut, a chunk under another chunk's name and a chunk carried into another
    /// session's directory are refused; and destroying a directory's key record erases its chunks
    /// while another directory's still read (29 §11's battery, on seeds).
    #[test]
    fn a_sealed_seed_is_refused_changed_cut_renamed_or_moved_and_erased_with_its_record() {
        let directory = tempfile::tempdir().unwrap();
        let (root, mut store) = open(directory.path(), "one", ContentForm::Sealed);
        let (other_root, mut other) = open(directory.path(), "two", ContentForm::Sealed);
        let first = plaintext(3, 150_000);
        let second = plaintext(4, 70_000);
        let a = store.install(&first).unwrap();
        let mut batch = store.batch().unwrap();
        let b = batch.add(&second).unwrap();
        batch.commit(&mut store).unwrap();
        let c = other.install(&first).unwrap();
        assert_eq!(a, c);
        // No name or byte holds a hash or plaintext.
        for (root, chunks) in [(&root, [&first, &second]), (&other_root, [&first, &first])] {
            for name in names(root) {
                for hash in [a, b] {
                    assert!(!name.contains(&hash.to_string()), "{name} names a hash");
                }
                let stored = std::fs::read(root.join(&name)).unwrap();
                for chunk in chunks {
                    for probe in chunk.chunks(4096).map(|piece| &piece[..16]) {
                        assert!(
                            !stored.windows(16).any(|w| w == probe),
                            "{name} holds plaintext"
                        );
                    }
                }
                for hash in [a, b] {
                    assert!(
                        !stored.windows(32).any(|w| w == hash.0),
                        "{name} holds a hash"
                    );
                }
            }
        }
        // The same chunk in two directories: two names, two files.
        assert_ne!(
            file_of(&store, a).file_name(),
            file_of(&other, c).file_name()
        );
        assert_ne!(
            std::fs::read(file_of(&store, a)).unwrap(),
            std::fs::read(file_of(&other, c)).unwrap()
        );
        let reader = store.reader();
        let original = std::fs::read(file_of(&store, a)).unwrap();
        // A flipped byte anywhere a reader opens is refused.
        for at in [0, 200, original.len() / 2, original.len() - 9] {
            let mut flipped = original.clone();
            flipped[at] ^= 1;
            std::fs::write(file_of(&store, a), &flipped).unwrap();
            assert!(
                matches!(reader.read(a, SEED_CHUNK_BYTES), Err(ContentError::Corrupt)),
                "flipped at {at}"
            );
        }
        // A cut.
        std::fs::write(file_of(&store, a), &original[..original.len() - 4096]).unwrap();
        assert!(matches!(
            reader.read(a, SEED_CHUNK_BYTES),
            Err(ContentError::Corrupt)
        ));
        std::fs::write(file_of(&store, a), &original).unwrap();
        assert_eq!(reader.read(a, SEED_CHUNK_BYTES).unwrap(), first);
        // Another chunk's file under this chunk's name.
        let b_path = file_of(&store, b);
        let b_bytes = std::fs::read(&b_path).unwrap();
        std::fs::copy(file_of(&store, a), &b_path).unwrap();
        assert!(matches!(
            reader.read(b, SEED_CHUNK_BYTES),
            Err(ContentError::Corrupt)
        ));
        std::fs::write(&b_path, &b_bytes).unwrap();
        // A chunk carried into another session's directory, under the name that directory gives
        // its hash: sealed under another key, it opens to nothing there.
        let carried = second_name(&other, b);
        std::fs::copy(&b_path, &carried).unwrap();
        assert!(matches!(
            other.reader().read(b, SEED_CHUNK_BYTES),
            Err(ContentError::Corrupt)
        ));
        std::fs::remove_file(&carried).unwrap();
        // The directory's record destroyed: its chunks read no more, the other's still do.
        std::fs::remove_file(root.join(sealing::DOMAIN_KEY)).unwrap();
        assert!(store.reader().read(a, SEED_CHUNK_BYTES).is_err());
        assert!(store.read(b, SEED_CHUNK_BYTES).is_err());
        assert_eq!(other.reader().read(c, SEED_CHUNK_BYTES).unwrap(), first);
    }

    /// The path the directory of `store` files a chunk filed under `hash` at.
    fn second_name(store: &SeedStore, hash: ContentHash) -> PathBuf {
        store
            .key()
            .unwrap()
            .path(&store.root, hash, Kind::Seed)
            .unwrap()
    }

    /// A directory of `count` chunks in the clear, as a binary below the at-rest level left it,
    /// with their bytes.
    fn clear_directory(
        data: &Path,
        name: &str,
        count: u8,
    ) -> (PathBuf, Vec<(ContentHash, Vec<u8>)>) {
        let (root, mut store) = open(data, name, ContentForm::Clear);
        let mut chunks = Vec::new();
        for n in 0..count {
            let bytes = plaintext(n.wrapping_mul(17), 3000 + usize::from(n) * 101);
            chunks.push((store.install(&bytes).unwrap(), bytes));
        }
        (root, chunks)
    }

    /// A directory in the clear asked to seal converts a chunk a step while it serves: every chunk
    /// reads back at every step through the store, through a reader made before the conversion and
    /// through one made during it; a batch commits and a peer's chunk is installed meanwhile, both
    /// sealed; it ends marked sealed with nothing named in the clear, every chunk counted converted.
    #[test]
    fn a_directory_in_the_clear_is_converted_while_it_serves() {
        let directory = tempfile::tempdir().unwrap();
        let (root, mut chunks) = clear_directory(directory.path(), "seeds", 12);
        let before = SeedReader::open(&root, keys(directory.path())).unwrap();
        let mut store =
            SeedStore::open(&root, disk(), &keys(directory.path()), ContentForm::Sealed).unwrap();
        assert!(root.join(sealing::SEALING).exists());
        assert_eq!(store.conversion_status().form, StoreForm::Counting);
        let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
        let mut steps = 0;
        let mut converted = 0;
        loop {
            let during = store.reader();
            for (hash, bytes) in &chunks {
                assert_eq!(
                    &store.read(*hash, SEED_CHUNK_BYTES).unwrap(),
                    bytes,
                    "step {steps}"
                );
                assert_eq!(
                    &before.read(*hash, SEED_CHUNK_BYTES).unwrap(),
                    bytes,
                    "step {steps}"
                );
                assert_eq!(
                    &during.read(*hash, SEED_CHUNK_BYTES).unwrap(),
                    bytes,
                    "step {steps}"
                );
            }
            if steps == 3 {
                let peer = plaintext(99, 4321);
                let hash = ContentHash(*blake3::hash(&peer).as_bytes());
                store.install_as(hash, &peer).unwrap();
                chunks.push((hash, peer));
                let fresh = plaintext(77, 6543);
                let mut batch = store.batch().unwrap();
                let hash = batch.add(&fresh).unwrap();
                batch.commit(&mut store).unwrap();
                chunks.push((hash, fresh));
            }
            match store.convert_step(&mut scratch, 2).unwrap() {
                Conversion::Done => break,
                Conversion::Converted => converted += 1,
                Conversion::Visited => {}
                Conversion::Held => panic!("held without a disk bound"),
            }
            steps += 1;
            assert!(steps < 1000, "the conversion ended");
        }
        let status = store.conversion_status();
        assert_eq!(status.form, StoreForm::Sealed);
        assert_eq!(status.counted.files, 12);
        assert_eq!(status.converted.files, 12);
        assert_eq!(converted, 12);
        assert_eq!(status.left().files, 0);
        assert!(root.join(sealing::SEALED).exists());
        assert!(!root.join(sealing::SEALING).exists());
        assert_eq!(clear_names(&root), 0);
        let after = SeedReader::open(&root, keys(directory.path())).unwrap();
        for (hash, bytes) in &chunks {
            assert_eq!(&after.read(*hash, SEED_CHUNK_BYTES).unwrap(), bytes);
            assert_eq!(&before.read(*hash, SEED_CHUNK_BYTES).unwrap(), bytes);
        }
        // A chunk planted in the clear after the mark is not looked for.
        let planted = plaintext(200, 1234);
        let hash = ContentHash(*blake3::hash(&planted).as_bytes());
        std::fs::write(root.join(format!("{hash}.seed")), &planted).unwrap();
        assert!(!after.contains(hash));
        assert!(after.read(hash, SEED_CHUNK_BYTES).is_err());
        assert!(!store.contains(hash));
        // Its chunks listed by their hashes, sealed as they are now: the planted file is not one.
        let mut hashes: Vec<ContentHash> = chunks.iter().map(|(hash, _)| *hash).collect();
        hashes.sort_unstable();
        assert_eq!(after.chunks().unwrap(), hashes);
        // An empty directory asked to seal is sealed as it opens.
        let (empty, store) = open(directory.path(), "empty", ContentForm::Sealed);
        assert!(empty.join(sealing::SEALED).exists());
        assert_eq!(store.conversion_status().form, StoreForm::Sealed);
    }

    /// A conversion cut after any step, a name in the clear put back beside its sealed form and a
    /// sealed form damaged beside its bytes in the clear, resumes at the next open, whatever form
    /// that open asks, and loses nothing.
    #[test]
    fn a_conversion_cut_at_any_step_resumes_and_loses_nothing() {
        let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
        let mut cut = 0;
        loop {
            let directory = tempfile::tempdir().unwrap();
            let (root, chunks) = clear_directory(directory.path(), "seeds", 6);
            let originals: Vec<(String, Vec<u8>)> = names(&root)
                .into_iter()
                .filter(|name| name.ends_with(".seed"))
                .map(|name| {
                    let bytes = std::fs::read(root.join(&name)).unwrap();
                    (name, bytes)
                })
                .collect();
            let mut store =
                SeedStore::open(&root, disk(), &keys(directory.path()), ContentForm::Sealed)
                    .unwrap();
            let mut finished = false;
            for _ in 0..cut {
                if store.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                    finished = true;
                    break;
                }
            }
            drop(store);
            if finished {
                break;
            }
            // Where a chunk's sealed form stands (the directory's record made by then).
            let reader = SeedReader::open(&root, keys(directory.path())).unwrap();
            let sealed_path = |name: &str| -> Option<PathBuf> {
                let hash = sealing::parse_hash(name.strip_suffix(".seed")?)?;
                let key = ContentKeys::reading_under(&reader.keys, focal_seal::Store::Group, true)
                    .domain(&root, SEED_DOMAIN, false)
                    .ok()?;
                let path = key.path(&root, hash, Kind::Seed).ok()?;
                path.exists().then_some(path)
            };
            // A crash between a chunk's sealed form and its name's removal: the name in the clear
            // stands beside the sealed form again.
            if let Some((name, bytes)) = originals.first()
                && sealed_path(name).is_some()
            {
                std::fs::write(root.join(name), bytes).unwrap();
            }
            // And a chunk's sealed form damaged while its bytes still lie in the clear.
            if let Some((name, bytes)) = originals.get(1)
                && let Some(sealed) = sealed_path(name)
            {
                std::fs::write(root.join(name), bytes).unwrap();
                std::fs::write(&sealed, b"damaged").unwrap();
            }
            let mut store =
                SeedStore::open(&root, disk(), &keys(directory.path()), ContentForm::Clear)
                    .unwrap();
            assert!(store.conversion_status().resumed || cut == 0, "cut {cut}");
            for _ in 0..1000 {
                if store.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                    break;
                }
            }
            assert_eq!(
                store.conversion_status().form,
                StoreForm::Sealed,
                "cut {cut}"
            );
            assert_eq!(clear_names(&root), 0, "cut {cut}");
            for (hash, bytes) in &chunks {
                assert_eq!(
                    &store.read(*hash, SEED_CHUNK_BYTES).unwrap(),
                    bytes,
                    "cut {cut}"
                );
            }
            cut += 1;
            assert!(cut < 200, "a conversion of six chunks ended");
        }
    }

    /// The seed sweep keeps a protected chunk by either of its names and removes an unprotected one
    /// past the grace in either form; while the conversion counts it removes nothing in the clear,
    /// and a chunk in the clear it removes once the count is taken is tallied as gone.
    #[test]
    fn the_sweep_keeps_what_is_protected_in_either_form_and_holds_the_clear_while_counting() {
        let directory = tempfile::tempdir().unwrap();
        let (root, chunks) = clear_directory(directory.path(), "seeds", 4);
        let mut store =
            SeedStore::open(&root, disk(), &keys(directory.path()), ContentForm::Sealed).unwrap();
        let sealed_kept = store.install(&plaintext(50, 2000)).unwrap();
        let sealed_gone = store.install(&plaintext(51, 2000)).unwrap();
        let mut protected = vec![chunks[0].0, sealed_kept];
        protected.sort();
        // Counting: nothing in the clear leaves, though every chunk stood past the grace.
        assert!(store.conversion_counting());
        let report = store.collect(&protected, 0, u64::MAX, 64).unwrap();
        assert!(report.complete);
        assert_eq!(clear_names(&root), 4);
        assert!(!store.contains(sealed_gone));
        assert!(store.contains(sealed_kept));
        // The count taken, an unprotected chunk in the clear leaves and is tallied as gone.
        let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
        while store.conversion_counting() {
            store.convert_step(&mut scratch, 1).unwrap();
        }
        let report = store.collect(&protected, 0, u64::MAX, 64).unwrap();
        assert!(report.complete);
        assert!(store.contains(chunks[0].0));
        for (hash, _) in &chunks[1..] {
            assert!(!store.contains(*hash));
        }
        loop {
            if store.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                break;
            }
        }
        let status = store.conversion_status();
        assert_eq!(status.counted.files, 4);
        assert_eq!(status.gone.files, 3);
        assert_eq!(status.converted.files, 1);
        assert_eq!(status.left().files, 0);
        // Sealed: the protected chunk is kept by its keyed name.
        let report = store.collect(&protected, 0, u64::MAX, 64).unwrap();
        assert!(report.complete);
        assert!(store.contains(chunks[0].0));
        assert!(store.contains(sealed_kept));
    }

    /// A job seals several chunks on another thread while the owner goes on, their files flushed
    /// together; its result taken back removes their names in the clear, each tallied once.
    #[test]
    fn a_job_seals_its_chunks_off_the_owner_and_its_result_removes_their_clear_names() {
        let directory = tempfile::tempdir().unwrap();
        let (root, chunks) = clear_directory(directory.path(), "seeds", 9);
        let mut store =
            SeedStore::open(&root, disk(), &keys(directory.path()), ContentForm::Sealed).unwrap();
        while store.conversion_counting() {
            store.conversion_job(4, SYNC_THREADS).unwrap();
        }
        let mut jobs = 0;
        loop {
            match store.conversion_job(64, 4).unwrap() {
                SeedStep::Done => break,
                SeedStep::Visited => {}
                SeedStep::Job(job) => {
                    fn sendable<T: Send>(_: &T) {}
                    sendable(&job);
                    assert!(job.hashes().len() <= 4);
                    let done = std::thread::scope(|scope| {
                        scope
                            .spawn(move || {
                                let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
                                job.run(&mut scratch)
                            })
                            .join()
                            .unwrap()
                    });
                    // Until its result comes back, every chunk still reads, in either form.
                    for (hash, bytes) in &chunks {
                        assert_eq!(&store.read(*hash, SEED_CHUNK_BYTES).unwrap(), bytes);
                    }
                    assert_eq!(store.conversion_done(done).unwrap(), Conversion::Converted);
                    jobs += 1;
                }
            }
        }
        assert_eq!(jobs, 3);
        let status = store.conversion_status();
        assert_eq!(status.form, StoreForm::Sealed);
        assert_eq!(status.converted.files, 9);
        assert_eq!(status.left().files, 0);
        assert_eq!(clear_names(&root), 0);
        let leftovers = names(&root)
            .iter()
            .filter(|name| name.ends_with(".convert"))
            .count();
        assert_eq!(leftovers, 0);
        for (hash, bytes) in &chunks {
            assert_eq!(&store.read(*hash, SEED_CHUNK_BYTES).unwrap(), bytes);
        }
    }

    /// A job whose result never comes back has its chunks taken again, first, by the next; the
    /// lost job finishing late renames the same bytes under the same names, and its result taken
    /// back after the other's counts nothing twice.
    #[test]
    fn a_lost_jobs_chunks_are_taken_again_and_its_late_result_counts_nothing_twice() {
        let directory = tempfile::tempdir().unwrap();
        let (root, chunks) = clear_directory(directory.path(), "seeds", 3);
        let mut store =
            SeedStore::open(&root, disk(), &keys(directory.path()), ContentForm::Sealed).unwrap();
        while store.conversion_counting() {
            store.conversion_job(64, 3).unwrap();
        }
        let SeedStep::Job(lost) = store.conversion_job(64, 3).unwrap() else {
            panic!("a job");
        };
        let SeedStep::Job(again) = store.conversion_job(64, 3).unwrap() else {
            panic!("the lost job's chunks again");
        };
        let lost_hashes = lost.hashes();
        let again_hashes = again.hashes();
        assert_eq!(lost_hashes, again_hashes);
        let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
        let late = lost.run(&mut scratch);
        let done = again.run(&mut scratch);
        assert_eq!(store.conversion_done(done).unwrap(), Conversion::Converted);
        assert_eq!(store.conversion_done(late).unwrap(), Conversion::Visited);
        loop {
            if let SeedStep::Done = store.conversion_job(64, 3).unwrap() {
                break;
            }
        }
        let status = store.conversion_status();
        assert_eq!(status.counted.files, 3);
        assert_eq!(status.converted.files, 3);
        assert_eq!(status.left().files, 0);
        for (hash, bytes) in &chunks {
            assert_eq!(&store.read(*hash, SEED_CHUNK_BYTES).unwrap(), bytes);
        }
    }

    /// A volume without room for a chunk's sealed form holds the job's chunks, writes nothing, and
    /// takes the same chunk first once there is room.
    #[test]
    fn a_job_without_room_holds_and_its_chunks_go_first_next() {
        let directory = tempfile::tempdir().unwrap();
        let (root, chunks) = clear_directory(directory.path(), "seeds", 4);
        let tight = DiskBudget::new(DiskBudgetConfig {
            headroom: 0,
            completion_reserve: 0,
            sample_interval: 64,
        })
        .unwrap();
        tight.observe(0);
        let mut store = SeedStore::open(
            &root,
            tight.clone(),
            &keys(directory.path()),
            ContentForm::Sealed,
        )
        .unwrap();
        while store.conversion_counting() {
            store.conversion_job(64, 2).unwrap();
        }
        let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
        let SeedStep::Job(job) = store.conversion_job(64, 2).unwrap() else {
            panic!("a job");
        };
        let first = job.hashes();
        assert_eq!(
            store.conversion_done(job.run(&mut scratch)).unwrap(),
            Conversion::Held
        );
        let status = store.conversion_status();
        assert!(status.held);
        assert_eq!(status.held_steps, 1);
        assert_eq!(status.converted.files, 0);
        assert_eq!(clear_names(&root), 4);
        assert_eq!(tight.stats().outstanding, 0);
        tight.observe(u64::MAX / 4);
        let SeedStep::Job(job) = store.conversion_job(64, 2).unwrap() else {
            panic!("the held chunks");
        };
        let next = job.hashes();
        assert_eq!(next, first);
        assert_eq!(
            store.conversion_done(job.run(&mut scratch)).unwrap(),
            Conversion::Converted
        );
        assert!(!store.conversion_status().held);
        for _ in 0..64 {
            if store.convert_step(&mut scratch, 64).unwrap() == Conversion::Done {
                break;
            }
        }
        assert_eq!(store.conversion_status().form, StoreForm::Sealed);
        assert_eq!(store.conversion_status().converted.files, 4);
        for (hash, bytes) in &chunks {
            assert_eq!(&store.read(*hash, SEED_CHUNK_BYTES).unwrap(), bytes);
        }
    }
}
