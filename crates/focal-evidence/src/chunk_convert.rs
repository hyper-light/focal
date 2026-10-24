//! A directory of content-addressed chunks converted to its sealed form while its store serves (29
//! §12.5, §12.6): a session's checkpoint seeds, and its cold streams' whole chunks and manifest
//! chunks. Its chunks in the clear are counted first, then handed out as jobs of at most
//! [`SEED_JOB_CHUNKS`] chunks whose heavy part (each chunk read, sealed beside itself under its
//! keyed name, the files flushed together, renamed, the directory flushed once) runs off the
//! store's owner ([`SeedJob::run`]); the owner lists, tallies and lets a converted chunk's name in
//! the clear go when the job's result comes back. A job whose result never comes back has its
//! chunks taken again, first; a sealed form a cut left is verified and kept, a damaged one sealed
//! over. The pass ended with no job out, the directory is flushed and marked sealed.
use super::convert::{Conversion, ConversionStatus, Progress, StoreForm};
use super::sealing::{self, ContentKeys, Hashed, Kind};
use super::seeds::{PendingFiles, PendingSeedFile, SEED_CHUNK_BYTES, SYNC_THREADS, SeedCommit};
use super::*;

/// The most chunks a conversion job seals ([`SeedStep::Job`]): as many as a batch flushes at once,
/// on its sync threads.
pub const SEED_JOB_CHUNKS: usize = SYNC_THREADS;

/// What a conversion works in: the directory, its keys, the volume its sealed forms are promised
/// from and the kind of bytes they are there (a seed's a checkpoint's, a cold chunk's an
/// archive's, as their stores promise them), the identity its key and names are made under, and
/// the counter that names a job's files.
pub(super) struct ChunkDirectory<'d> {
    pub(super) root: &'d Path,
    pub(super) keys: &'d mut ContentKeys<'static>,
    pub(super) disk: &'d DiskBudget,
    pub(super) kind: DiskKind,
    pub(super) domain: ContentDomainId,
    pub(super) jobs: &'d mut u64,
}

/// A directory's conversion and its progress since its store opened: none where no conversion
/// runs or ran. Boxed: a session holds its stores inline and moves by value through its hosts'
/// frames (`Session`'s inline bound).
#[derive(Default)]
pub(super) struct ChunkConversion {
    state: Option<Box<State>>,
    progress: Option<Box<Progress>>,
}

/// Where a conversion stands: the count of its chunks in the clear, then the pass that seals them.
enum State {
    /// Counting, the listing so far: none before the count begins.
    Counting(Option<std::fs::ReadDir>),
    Sealing(Sealing),
}

/// The pass that seals a directory's chunks in the clear.
#[derive(Default)]
struct Sealing {
    /// The pass's listing: none before it begins.
    pass: Option<std::fs::ReadDir>,
    /// Whether the listing has ended.
    listed: bool,
    /// Chunks a job had no room for, taken before the listing's next (the first held last).
    /// Bounded by a job's chunks.
    queued: Vec<ClearChunk>,
    /// The chunks of the job handed last, until its result comes back, and its number.
    out: Vec<ClearChunk>,
    number: u64,
    /// Whether a name in the clear went since the directory was last flushed.
    removed: bool,
}

/// A chunk in the clear the conversion seals: where it lies, the hash it is filed under, its bytes.
#[derive(Clone, Debug)]
struct ClearChunk {
    path: PathBuf,
    hash: ContentHash,
    bytes: u64,
}

/// What the next piece of a directory's conversion is.
pub enum SeedStep {
    /// Chunks in the clear to seal off the owner's thread.
    Job(SeedJob),
    /// Entries visited: the count went on, or the listing passed only what is left as it is.
    Visited,
    /// Every chunk is sealed and the directory marked so; or no conversion runs.
    Done,
}

/// Chunks in the clear of one directory, to seal off its owner's thread ([`SeedJob::run`]): it
/// owns what it writes with, so it runs on any thread.
pub struct SeedJob {
    root: PathBuf,
    keys: ContentKeys<'static>,
    disk: DiskBudget,
    kind: DiskKind,
    domain: ContentDomainId,
    /// What names its unsynced files, so that no two jobs or batches share one.
    number: u64,
    chunks: Vec<ClearChunk>,
}

/// What a [`SeedJob`] did, for its store's owner to take back.
pub struct SeedJobDone {
    number: u64,
    /// The chunks whose sealed forms are durable: their names in the clear may go.
    sealed: Vec<ClearChunk>,
    /// The chunk the volume had no room for and those after it, in order.
    held: Vec<ClearChunk>,
    /// What stopped the job: nothing it wrote is taken as sealed.
    failed: Option<ContentError>,
}

impl ChunkConversion {
    /// A conversion begun at this open, `resumed` where an earlier open began it.
    pub(super) fn begun(resumed: bool) -> Self {
        Self {
            state: Some(Box::new(State::Counting(None))),
            progress: Some(Box::new(Progress::begun(resumed))),
        }
    }
    /// Whether the conversion is counting the chunks in the clear: what removes one meanwhile
    /// waits, or says so ([`Self::removed_clear`]), so the count is exact.
    pub(super) fn counting(&self) -> bool {
        matches!(self.state.as_deref(), Some(State::Counting(_)))
    }
    /// The form the directory is in, by its keys, and the conversion's progress since its open.
    pub(super) fn status(&self, keys: &ContentKeys<'_>) -> ConversionStatus {
        let form = if !keys.seals() {
            StoreForm::Clear
        } else if !keys.clear_allowed() {
            StoreForm::Sealed
        } else if self.counting() {
            StoreForm::Counting
        } else {
            StoreForm::Converting
        };
        self.progress
            .as_deref()
            .copied()
            .unwrap_or_default()
            .status(form)
    }
    /// A chunk in the clear of `bytes` left the directory other than by the conversion: counted
    /// again from the start while the count runs (whether the count met it is not known), tallied
    /// as gone once it has counted.
    pub(super) fn removed_clear(&mut self, bytes: u64) {
        match self.state.as_deref_mut() {
            Some(State::Counting(listing)) => {
                *listing = None;
                let resumed = progress(&mut self.progress).resumed;
                self.progress = Some(Box::new(Progress::begun(resumed)));
            }
            Some(State::Sealing(_)) => progress(&mut self.progress).gone.add(bytes),
            None => {}
        }
    }
    /// The next piece of the conversion, at most `visits` entries visited. While it counts the
    /// chunks in the clear, the count goes on ([`SeedStep::Visited`]); once counted, the next at
    /// most `chunks` chunks in the clear are handed out as a job ([`SeedStep::Job`]). A job handed
    /// before whose result never came back has its chunks taken again, first: they are still in
    /// the clear, and a sealed form it left is found and kept. The pass ended with no chunk out,
    /// the directory is flushed and marked sealed ([`SeedStep::Done`]).
    pub(super) fn job(
        &mut self,
        directory: ChunkDirectory<'_>,
        visits: usize,
        chunks: usize,
    ) -> Result<SeedStep, ContentError> {
        let Some(state) = self.state.as_deref_mut() else {
            return Ok(SeedStep::Done);
        };
        if let State::Counting(listing) = state {
            let listing = match listing {
                Some(listing) => listing,
                None => listing.insert(fs::read_dir(directory.root)?),
            };
            let mut counted = false;
            for _ in 0..visits {
                let Some(entry) = listing.next() else {
                    counted = true;
                    break;
                };
                if let Some(bytes) = clear_seed(&entry?.path())? {
                    progress(&mut self.progress).counted.add(bytes);
                }
            }
            if counted {
                progress(&mut self.progress).counting = false;
                *state = State::Sealing(Sealing::default());
            }
            return Ok(SeedStep::Visited);
        }
        let State::Sealing(sealing) = state else {
            return Err(ContentError::Invalid);
        };
        // A job out whose result never came back: its chunks are taken again, first.
        let lost = std::mem::take(&mut sealing.out);
        let mut taken: Vec<ClearChunk> = Vec::new();
        taken
            .try_reserve_exact(chunks.max(lost.len()))
            .map_err(|_| ContentError::Capacity)?;
        taken.extend(lost);
        while taken.len() < chunks {
            let Some(chunk) = sealing.queued.pop() else {
                break;
            };
            taken.push(chunk);
        }
        if !sealing.listed {
            let listing = match &mut sealing.pass {
                Some(listing) => listing,
                None => sealing.pass.insert(fs::read_dir(directory.root)?),
            };
            for _ in 0..visits {
                if taken.len() >= chunks {
                    break;
                }
                let Some(entry) = listing.next() else {
                    sealing.listed = true;
                    break;
                };
                let path = entry?.path();
                if let Some(bytes) = clear_seed(&path)?
                    && let Some(hash) = clear_hash(&path)
                {
                    taken.push(ClearChunk { path, hash, bytes });
                }
            }
        }
        if taken.is_empty() {
            if !sealing.listed || !sealing.queued.is_empty() {
                return Ok(SeedStep::Visited);
            }
            if sealing.removed {
                sync_directory(directory.root)?;
            }
            self.mark_sealed(directory)?;
            return Ok(SeedStep::Done);
        }
        let mut out = Vec::new();
        out.try_reserve_exact(taken.len())
            .map_err(|_| ContentError::Capacity)?;
        out.extend(taken.iter().cloned());
        sealing.out = out;
        *directory.jobs = directory.jobs.wrapping_add(1);
        sealing.number = *directory.jobs;
        Ok(SeedStep::Job(SeedJob {
            root: directory.root.to_path_buf(),
            keys: directory.keys.owned_copy()?,
            disk: directory.disk.clone(),
            kind: directory.kind,
            domain: directory.domain,
            number: *directory.jobs,
            chunks: taken,
        }))
    }
    /// A job's result taken back ([`Self::job`]): the name in the clear of every chunk whose sealed
    /// form it made durable removed, each such chunk tallied converted once (one removed first
    /// otherwise was tallied gone then); the chunks it had no room for taken first next; a job that
    /// failed ends this conversion with its cause. What it did, as a step of the conversion; an IO
    /// failure is the caller's to fail its store with, as a batch's is.
    pub(super) fn done(&mut self, done: SeedJobDone) -> Result<Conversion, ContentError> {
        let SeedJobDone {
            number,
            sealed,
            held,
            failed,
        } = done;
        let Some(State::Sealing(sealing)) = self.state.as_deref_mut() else {
            return Ok(Conversion::Done);
        };
        if sealing.number == number {
            sealing.out.clear();
        }
        let mut converted = 0usize;
        for chunk in sealed {
            match fs::remove_file(&chunk.path) {
                Ok(()) => {
                    sealing.removed = true;
                    converted = converted.saturating_add(1);
                    progress(&mut self.progress).converted.add(chunk.bytes);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    let error = ContentError::Io(error);
                    progress(&mut self.progress).stopped = Some(error.kind());
                    self.state = None;
                    return Err(error);
                }
            }
        }
        let was_held = !held.is_empty();
        sealing
            .queued
            .try_reserve(held.len())
            .map_err(|_| ContentError::Capacity)?;
        // Taken from the back: the first held goes first.
        sealing.queued.extend(held.into_iter().rev());
        if let Some(error) = failed {
            progress(&mut self.progress).stopped = Some(error.kind());
            self.state = None;
            return Err(error);
        }
        let progress = progress(&mut self.progress);
        if converted > 0 {
            progress.held = false;
            Ok(Conversion::Converted)
        } else if was_held {
            progress.held = true;
            progress.held_steps = progress.held_steps.saturating_add(1);
            Ok(Conversion::Held)
        } else {
            Ok(Conversion::Visited)
        }
    }
    /// Every chunk is sealed: the directory is marked so, its marker of the conversion removed,
    /// and nothing in the clear is looked for from here on.
    fn mark_sealed(&mut self, directory: ChunkDirectory<'_>) -> Result<(), ContentError> {
        convert::write_marker(directory.root, sealing::SEALED)?;
        convert::remove_marker(directory.root, sealing::SEALING)?;
        directory.keys.mark_sealed();
        self.state = None;
        let progress = progress(&mut self.progress);
        progress.counting = false;
        progress.finished_ms = convert::now_ms();
        Ok(())
    }
}

/// A conversion's progress, made where none was kept (a store that ran no conversion keeps none).
fn progress(progress: &mut Option<Box<Progress>>) -> &mut Progress {
    progress.get_or_insert_with(Box::default)
}

/// The bytes of the entry at `path` where it is a chunk in the clear: named by its hash, a file
/// itself (a link to one is left as it is), no longer than a chunk. None for anything else, and
/// for an entry gone since it was listed.
pub(super) fn clear_seed(path: &Path) -> Result<Option<u64>, ContentError> {
    if path.extension().and_then(|extension| extension.to_str()) != Some(Kind::Seed.extension()) {
        return Ok(None);
    }
    if clear_hash(path).is_none() {
        return Ok(None);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(convert::convertible(Kind::Seed, &metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// The hash a chunk in the clear at `path` is filed under: its name's.
pub(super) fn clear_hash(path: &Path) -> Option<ContentHash> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(sealing::parse_hash)
}

/// Whether the directory at `root` holds a chunk in the clear: listed until the first one.
pub(super) fn holds_clear(root: &Path) -> Result<bool, ContentError> {
    for entry in fs::read_dir(root)? {
        if clear_seed(&entry?.path())?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

impl SeedJob {
    /// The hashes of the chunks the job seals, in order.
    #[cfg(test)]
    pub(super) fn hashes(&self) -> Vec<ContentHash> {
        self.chunks.iter().map(|chunk| chunk.hash).collect()
    }
    /// Seal the job's chunks, on whatever thread runs it: each chunk in the clear read into the
    /// head of `scratch` (a chunk long) and sealed beside itself under its keyed name into a file of
    /// its own, unflushed; the files then flushed together, renamed, and the directory flushed
    /// once, as a batch of seeds is made durable. A chunk whose sealed form stands already and
    /// opens to its bytes (a conversion cut before its removal) is taken as it is; one gone from
    /// the clear is passed over. The volume without room for a chunk's sealed form, that chunk and
    /// those after it are handed back held. A failure takes nothing the job wrote as sealed.
    pub fn run(self, scratch: &mut [u8]) -> SeedJobDone {
        let SeedJob {
            root,
            keys,
            disk,
            kind,
            domain,
            number,
            chunks,
        } = self;
        let mut done = SeedJobDone {
            number,
            sealed: Vec::new(),
            held: Vec::new(),
            failed: None,
        };
        let job = Job {
            root: &root,
            keys: &keys,
            disk: &disk,
            kind,
            domain,
            number,
        };
        if let Err(error) = job.seal(chunks, scratch, &mut done) {
            done.sealed.clear();
            done.failed = Some(error);
        }
        done
    }
}

/// What a job seals with, borrowed for its run.
struct Job<'j> {
    root: &'j Path,
    keys: &'j ContentKeys<'static>,
    disk: &'j DiskBudget,
    kind: DiskKind,
    domain: ContentDomainId,
    number: u64,
}

impl Job<'_> {
    fn seal(
        &self,
        chunks: Vec<ClearChunk>,
        scratch: &mut [u8],
        done: &mut SeedJobDone,
    ) -> Result<(), ContentError> {
        let mut pending = PendingFiles::default();
        pending
            .0
            .try_reserve_exact(chunks.len())
            .map_err(|_| ContentError::Capacity)?;
        let mut sealed = Vec::new();
        sealed
            .try_reserve_exact(chunks.len())
            .map_err(|_| ContentError::Capacity)?;
        let mut chunks = chunks.into_iter();
        while let Some(chunk) = chunks.next() {
            let key = self.keys.domain(self.root, self.domain, false)?;
            if key.record_bytes() == 0
                && key.holds_sealed(
                    &key.path(self.root, chunk.hash, Kind::Seed)?,
                    chunk.hash,
                    scratch,
                )?
            {
                sealed.push(chunk);
                continue;
            }
            let mut file = match File::open(&chunk.path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let length =
                usize::try_from(file.metadata()?.len()).map_err(|_| ContentError::Capacity)?;
            if length > SEED_CHUNK_BYTES {
                // Grown past a chunk since it was listed: not a chunk any writer installed.
                return Err(ContentError::Corrupt);
            }
            let room = match disk_reserve(
                self.disk,
                self.root,
                self.kind,
                BudgetLane::Ordinary,
                key.file_bytes(length)?
                    .checked_add(key.record_bytes())
                    .ok_or(ContentError::Capacity)?,
            ) {
                Ok(room) => room,
                Err(ContentError::Capacity) => {
                    done.held
                        .try_reserve_exact(chunks.len().saturating_add(1))
                        .map_err(|_| ContentError::Capacity)?;
                    done.held.push(chunk);
                    done.held.extend(chunks);
                    break;
                }
                Err(error) => return Err(error),
            };
            let block = scratch.get_mut(..length).ok_or(ContentError::Capacity)?;
            file.read_exact(block)?;
            // The file is exactly the length it was opened at: one written into since is not a
            // chunk the writer installed (every install renames a whole file into place).
            let mut extra = [0u8; 1];
            if file.read(&mut extra)? != 0 {
                return Err(ContentError::Corrupt);
            }
            drop(file);
            let key = self.keys.writable(self.root, self.domain, key)?;
            let path = key.path(self.root, chunk.hash, Kind::Seed)?;
            let temp = path.with_extension(format!("{}.convert", self.number));
            // Sealed as the bytes are, under the name the chunk is filed by: a chunk that does not
            // hash to its name is refused as corrupt by every read in either form, so it is
            // converted as it is, never judged here; its key is derived from the hash of exactly
            // these bytes.
            let writer = key.seed_writer(&temp, Hashed::of(block))?;
            drop(key);
            let file = writer.finish()?;
            pending.0.push(PendingSeedFile {
                hash: chunk.hash,
                temp,
                path,
                file,
                record: room,
            });
            sealed.push(chunk);
        }
        SeedCommit::durable(self.root, &pending.0)?;
        for file in pending.0.drain(..) {
            file.record.commit();
        }
        done.sealed = sealed;
        Ok(())
    }
}
