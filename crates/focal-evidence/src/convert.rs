//! A store's files in the clear converted to their sealed form (29 §11, "Existing stores"), a file
//! a step, while the store serves.
//!
//! A store whose writer seals and that is not yet marked sealed holds files in either form: what an
//! earlier binary or this one below the fence wrote in the clear, and what its writer has sealed
//! since. Its owner converts the rest when no other work waits ([`ContentStore::convert_step`]):
//! each file in the clear is read, sealed under its domain's key at its keyed name beside it, and
//! only then is its name in the clear removed, so every file lies in at least one form at every
//! moment and a reader that looks in the clear first finds it whatever the conversion does
//! between (`sealing::DomainKey::opened`). A file already sealed (by the writer, or by a
//! conversion cut before its removal) keeps its sealed form and loses its name in the clear. Each
//! domain's directory is flushed as the conversion leaves it, so a removal is durable before the
//! store is marked sealed: a crash before the mark leaves each file whole in one form or both, and
//! the next start resumes from the store's marker (`SEALING`), which the writer keeps until the
//! last domain is done.
//!
//! The quarantine rounds go first, then the store's own objects: a quarantined object restored while
//! the conversion runs comes back in the clear from a round not yet visited into objects not yet
//! visited, and sealed from a round visited, so the pass over the objects meets every file that is
//! still in the clear. The collector holds its passes over objects while the store converts.
//!
//! Before it converts a file, the conversion counts the files in the clear it must convert and their
//! bytes, in its own order (the rounds by name, then the store's own objects, each tree's domains by
//! identity), so an operator reads how far it has come as a share of a whole, as CockroachDB's stores
//! report gives the files and bytes under the active key ([`ConversionStatus`]). From then on every
//! such file is tallied once: converted by a step, or gone with an expired round before the
//! conversion met it; a restore only moves one. A restore while the count runs moves a file between
//! places the count has passed and places it has not: the count's place says whether the file was
//! counted where it left and will be where it lands, and a domain whose listing the restore touches
//! is listed again, POSIX leaving unspecified whether a listing open as a file comes or goes returns
//! it. The collector deletes no round while the store counts.
//!
//! - **Bounded.** A step visits at most the entries its caller allows and converts at most one file,
//!   a chunk or a manifest no longer than its kind's bound, read into the owner's scratch. The
//!   count and the pass each visit each round, each domain and each file once (a domain listed again
//!   once for each restore that touched its listing), and hold no more than the names of the rounds
//!   and of one tree's domains.
//! - **Charged.** A file's sealed form (and its domain's record where none stands) is promised its
//!   bytes before it is written; refused, the step is held and nothing written, and the same file is
//!   taken first when the owner asks again.
//! - **Left as it is.** What a domain's directory holds besides chunks and manifests in the clear:
//!   keyed names, parts a transfer is receiving, temporaries, the domain's record; and an entry that
//!   is not a file no longer than its kind's bound, which was never an object's. The count passes
//!   over the same entries.
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, ReadDir};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use focal_memory::{BudgetLane, DiskKind};
use focal_model::{ContentDomainId, ContentHash};

use super::sealing::{self, Kind};
use super::staging::StagingStep;
use super::{
    ContentError, ContentErrorKind, ContentStore, CustodyRecordKind, MAX_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_MANIFEST_BYTES, disk_reserve, domain_directory, durable_directory, hex,
    sync_directory,
};

/// What a store's conversion converts: its objects (the content level's, 29 §11), its files beside
/// them (the at-rest level's, §12.7: custody records, parts, staging), or both at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Converts {
    pub(super) objects: bool,
    pub(super) rest: bool,
}

impl Converts {
    /// Whether a file of `kind` in the clear is the conversion's.
    fn takes(self, kind: Kind) -> bool {
        match kind {
            Kind::Chunk | Kind::Manifest => self.objects,
            Kind::Part | Kind::Record => self.rest,
            Kind::Seed => false,
        }
    }
}

/// A change to a file of staging's in the clear while the store converts
/// ([`ContentStore::conversion_staging`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagingChange {
    /// Bytes appended to an upload's part.
    Grew(u64),
    /// A file of these bytes written: an upload's terminal record, replacing its metadata.
    Appeared(u64),
    /// A file of these bytes gone: an upload's part or metadata at its finish, a terminal record
    /// released.
    Gone(u64),
}

/// What one step of a store's conversion did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conversion {
    /// A file in the clear was sealed, and its name in the clear removed.
    Converted,
    /// The step's visits went to entries it leaves as they are.
    Visited,
    /// The volume had no room for the next file's sealed form: nothing was written, and the file
    /// is taken first when the owner asks again.
    Held,
    /// Every file is sealed and the store is marked so; or no conversion runs.
    Done,
}

/// Files and the bytes they hold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    pub files: u64,
    pub bytes: u64,
}

impl Tally {
    pub(super) fn add(&mut self, bytes: u64) {
        self.files = self.files.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
    }
    /// Bytes more of a file already tallied.
    pub(super) fn grow(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
    }
    pub(super) fn plus(self, other: Self) -> Self {
        Self {
            files: self.files.saturating_add(other.files),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
    fn less(self, other: Self) -> Self {
        Self {
            files: self.files.saturating_sub(other.files),
            bytes: self.bytes.saturating_sub(other.bytes),
        }
    }
}

/// The form a store is in, as an operator reads it (29 §11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreForm {
    /// Its writer installs files in the clear: below the fence.
    Clear,
    /// It seals, and counts the files in the clear it must convert before it converts one.
    Counting,
    /// It seals, and converts its files in the clear a file a step.
    Converting,
    /// Every file its form seals is sealed and the store is marked so: its objects from the
    /// content level, the files beside them from the at-rest level (29 §12.7), which lie in the
    /// clear below it as every earlier binary wrote them.
    Sealed,
}

/// Where a store's conversion stands since this start (29 §11): what it counted to convert, what
/// became of it, and whether a step waits for room or the conversion stopped. A start that finds
/// the conversion begun by an earlier one counts what is left of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversionStatus {
    pub form: StoreForm,
    /// Whether this start resumed a conversion an earlier start began.
    pub resumed: bool,
    /// Milliseconds since the Unix epoch when this start's conversion began, and when it marked the
    /// store sealed; zero for one that has not.
    pub started_ms: u64,
    pub finished_ms: u64,
    /// The files in the clear the conversion must convert, as far as it has counted them: the whole
    /// once the form is converting.
    pub counted: Tally,
    /// Of those, the files a step sealed, and the files that left with an expired quarantine round
    /// before the conversion met them.
    pub converted: Tally,
    pub gone: Tally,
    /// Whether the last step found no room for its file's sealed form, and how many steps did.
    pub held: bool,
    pub held_steps: u64,
    /// What stopped this start's conversion: the store reads either form, and its next start
    /// resumes the conversion.
    pub stopped: Option<ContentErrorKind>,
}

impl ConversionStatus {
    /// What the conversion has still to meet: the files counted, less those converted and gone.
    pub fn left(&self) -> Tally {
        self.counted.less(self.converted).less(self.gone)
    }
}

/// What a store keeps of its conversion since this start, behind [`ConversionStatus`].
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Progress {
    pub(super) resumed: bool,
    pub(super) started_ms: u64,
    pub(super) finished_ms: u64,
    /// Whether the count runs, or ran when the conversion stopped.
    pub(super) counting: bool,
    pub(super) counted: Tally,
    pub(super) converted: Tally,
    pub(super) gone: Tally,
    pub(super) held: bool,
    pub(super) held_steps: u64,
    pub(super) stopped: Option<ContentErrorKind>,
}

impl Progress {
    /// A conversion begun at this start, `resumed` where an earlier start began it.
    pub(super) fn begun(resumed: bool) -> Self {
        Self {
            resumed,
            started_ms: now_ms(),
            counting: true,
            ..Self::default()
        }
    }

    /// The status this progress gives, the store being in `form`.
    pub(super) fn status(&self, form: StoreForm) -> ConversionStatus {
        ConversionStatus {
            form,
            resumed: self.resumed,
            started_ms: self.started_ms,
            finished_ms: self.finished_ms,
            counted: self.counted,
            converted: self.converted,
            gone: self.gone,
            held: self.held,
            held_steps: self.held_steps,
            stopped: self.stopped,
        }
    }
}

/// Milliseconds since the Unix epoch, zero where the clock reads before it.
pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Where a store's conversion stands: the count of what it must convert, then the quarantine rounds'
/// objects, then the store's own, a domain at a time, a file at a time.
pub(super) struct Converting {
    /// The count of the files in the clear, taken before any is converted; none once taken.
    count: Option<Count>,
    /// The quarantine rounds to visit, by name as the count listed them, and the next of them.
    rounds: Vec<OsString>,
    round: usize,
    /// Whether the store's own objects have been entered.
    own: bool,
    /// The domains of the tree being visited (a round's `objects`, or the store's own).
    tree: Option<ReadDir>,
    /// The domain being visited.
    domain: Option<Visit>,
    /// The custody records' directories, as the count listed them, the index of the next, and
    /// the one being visited with its kind.
    record_dirs: Vec<PathBuf>,
    record: usize,
    records: Option<(CustodyRecordKind, PathBuf, ReadDir, bool)>,
    /// Staging's pass, once the objects are done ([`super::staging::StagingPass`]).
    staging: Option<super::staging::StagingPass>,
    /// Whether a pass over staging met nothing in the clear.
    staged: bool,
    /// What it converts.
    converts: Converts,
}

/// The count of a conversion's files in the clear, in the conversion's order: the rounds by name,
/// then the store's own objects, each tree's domains by identity.
struct Count {
    /// The quarantine's rounds by name; past them, the store's own objects.
    rounds: Vec<OsString>,
    /// The tree being counted: an index into `rounds`, or `rounds.len()` for the store's own.
    tree: usize,
    /// The domains of the tree being counted, by identity, once listed.
    domains: Option<Vec<ContentDomainId>>,
    /// The index in `domains` of the domain being counted, or of the next.
    domain: usize,
    /// The domain being listed.
    listing: Option<Listing>,
    /// What the domains counted to the end hold.
    total: Tally,
    /// The custody records' directories, each round's kinds then the store's own, and the index of
    /// the one being counted or next (29 §12.7).
    record_dirs: Vec<PathBuf>,
    record: usize,
    /// The records' directory being listed.
    records: Option<Listing>,
    /// Staging's listing, once the objects are counted: its files in the clear (29 §12.7).
    staging: Option<Listing>,
    /// Whether staging is counted: the count is whole.
    staged: bool,
    /// What it counts.
    converts: Converts,
}

/// One domain's directory being counted.
struct Listing {
    directory: PathBuf,
    entries: ReadDir,
    /// What the listing has met so far.
    partial: Tally,
}

/// Where a file's place stands in the count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// The count has passed it, or will not list it.
    Counted,
    /// Its domain is being listed.
    Listing,
    /// The count has still to list it.
    Ahead,
}

impl Count {
    fn begin(root: &Path, converts: Converts) -> Result<Self, ContentError> {
        let mut rounds = Vec::new();
        match fs::read_dir(root.join("quarantine")) {
            Ok(listing) => {
                for entry in listing {
                    rounds
                        .try_reserve_exact(1)
                        .map_err(|_| ContentError::Capacity)?;
                    rounds.push(entry?.file_name());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        rounds.sort();
        let record_dirs = if converts.rest {
            record_directories(root, &rounds)?
        } else {
            Vec::new()
        };
        // Where only the files beside the objects convert, the rounds' objects are not the count's.
        if !converts.objects {
            rounds.clear();
        }
        Ok(Self {
            rounds,
            tree: 0,
            domains: None,
            domain: 0,
            listing: None,
            total: Tally::default(),
            record_dirs,
            record: 0,
            records: None,
            staging: None,
            staged: !converts.rest,
            converts,
        })
    }

    /// Where the store's own records of `kind` stand in the count.
    fn record_standing(&self, kind: CustodyRecordKind) -> Standing {
        let index = self
            .rounds
            .len()
            .saturating_mul(CustodyRecordKind::ALL.len())
            .saturating_add(
                CustodyRecordKind::ALL
                    .iter()
                    .position(|each| *each == kind)
                    .unwrap_or(0),
            );
        if index < self.record {
            Standing::Counted
        } else if index == self.record && self.records.is_some() {
            Standing::Listing
        } else {
            Standing::Ahead
        }
    }

    /// The records' directory being listed, listed again from its start.
    fn relist_records(&mut self) -> Result<(), ContentError> {
        let Some(listing) = self.records.take() else {
            return Ok(());
        };
        self.records = Some(Listing {
            entries: fs::read_dir(&listing.directory)?,
            directory: listing.directory,
            partial: Tally::default(),
        });
        Ok(())
    }

    /// What the count has met so far.
    fn so_far(&self) -> Tally {
        let partial = self
            .listing
            .iter()
            .chain(&self.records)
            .chain(&self.staging)
            .fold(Tally::default(), |sum, listing| sum.plus(listing.partial));
        self.total.plus(partial)
    }

    /// Where staging stands in the count.
    fn staging_standing(&self) -> Standing {
        if self.staged {
            Standing::Counted
        } else if self.staging.is_some() {
            Standing::Listing
        } else {
            Standing::Ahead
        }
    }

    /// Staging listed again from its start: a file of it changed while it was listed.
    fn relist_staging(&mut self) -> Result<(), ContentError> {
        let Some(listing) = self.staging.take() else {
            return Ok(());
        };
        self.staging = Some(Listing {
            entries: fs::read_dir(&listing.directory)?,
            directory: listing.directory,
            partial: Tally::default(),
        });
        Ok(())
    }

    /// The directory of the tree at `tree`, none past the store's own.
    fn tree_root(&self, root: &Path, tree: usize) -> Option<PathBuf> {
        match self.rounds.get(tree) {
            Some(round) => Some(root.join("quarantine").join(round).join("objects")),
            None if tree == self.rounds.len() => Some(root.join("objects")),
            None => None,
        }
    }

    /// At most `visits` entries counted: whether the count is whole.
    fn step(&mut self, root: &Path, visits: usize) -> Result<bool, ContentError> {
        for _ in 0..visits {
            if let Some(listing) = self.listing.as_mut() {
                if let Some(entry) = listing.entries.next() {
                    if let Some(bytes) = counted_entry(&entry?, self.converts)? {
                        listing.partial.add(bytes);
                    }
                    continue;
                }
                if let Some(done) = self.listing.take() {
                    self.total = self.total.plus(done.partial);
                }
                self.domain = self.domain.saturating_add(1);
                continue;
            }
            let Some(tree_root) = self.tree_root(root, self.tree) else {
                // Past the objects, the custody records' directories, each round's then the
                // store's own.
                if let Some(records) = self.records.as_mut() {
                    match records.entries.next() {
                        Some(entry) => {
                            if let Some(bytes) = counted_entry(&entry?, self.converts)? {
                                records.partial.add(bytes);
                            }
                        }
                        None => {
                            if let Some(done) = self.records.take() {
                                self.total = self.total.plus(done.partial);
                            }
                            self.record = self.record.saturating_add(1);
                        }
                    }
                    continue;
                }
                if let Some(directory) = self.record_dirs.get(self.record) {
                    match fs::read_dir(directory) {
                        Ok(entries) => {
                            self.records = Some(Listing {
                                directory: directory.clone(),
                                entries,
                                partial: Tally::default(),
                            });
                        }
                        Err(error) if gone(&error) => self.record = self.record.saturating_add(1),
                        Err(error) => return Err(error.into()),
                    }
                    continue;
                }
                // Then staging's files in the clear.
                if self.staged {
                    return Ok(true);
                }
                let Some(staging) = self.staging.as_mut() else {
                    let directory = root.join("staging");
                    self.staging = Some(Listing {
                        entries: fs::read_dir(&directory)?,
                        directory,
                        partial: Tally::default(),
                    });
                    continue;
                };
                match staging.entries.next() {
                    Some(entry) => {
                        let found = super::staging::counted_staging(&entry?.path())?;
                        staging.partial = staging.partial.plus(found);
                    }
                    None => {
                        if let Some(done) = self.staging.take() {
                            self.total = self.total.plus(done.partial);
                        }
                        self.staged = true;
                    }
                }
                continue;
            };
            let Some(domains) = &self.domains else {
                self.domains = Some(sorted_domains(&tree_root)?);
                continue;
            };
            let Some(domain) = domains.get(self.domain).copied() else {
                self.domains = None;
                self.domain = 0;
                self.tree = self.tree.saturating_add(1);
                continue;
            };
            let directory = tree_root.join(hex(&domain.0));
            match fs::read_dir(&directory) {
                Ok(entries) => {
                    self.listing = Some(Listing {
                        directory,
                        entries,
                        partial: Tally::default(),
                    });
                }
                Err(error) if gone(&error) => self.domain = self.domain.saturating_add(1),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(false)
    }

    /// Where `domain` of the tree at `tree` stands.
    fn standing(&self, tree: usize, domain: ContentDomainId) -> Standing {
        if tree != self.tree {
            return if tree < self.tree {
                Standing::Counted
            } else {
                Standing::Ahead
            };
        }
        let Some(domains) = &self.domains else {
            return Standing::Ahead;
        };
        match domains.binary_search(&domain) {
            Ok(index) if index < self.domain => Standing::Counted,
            Ok(index) if index == self.domain && self.listing.is_some() => Standing::Listing,
            Ok(_) => Standing::Ahead,
            // A domain the tree did not hold when it was listed: the count will not list it.
            Err(_) => Standing::Counted,
        }
    }

    /// A file moved by a restore from `domain` of the round named `round` to the store's own: of
    /// `moved` bytes where the count counts it, over a file of `replaced` bytes where one it counts
    /// stood at the same name. The moved file stays counted once, where it lands if the count has
    /// still to pass that; the replaced one leaves the count.
    fn restored(
        &mut self,
        round: &OsString,
        domain: ContentDomainId,
        moved: Option<u64>,
        replaced: Option<u64>,
    ) -> Result<(), ContentError> {
        let from = match self.rounds.binary_search(round) {
            Ok(tree) => self.standing(tree, domain),
            // A round the count never listed: the file was not counted there.
            Err(_) => Standing::Ahead,
        };
        let to = self.standing(self.rounds.len(), domain);
        if let Some(bytes) = replaced
            && to == Standing::Counted
        {
            self.total = self.total.less(Tally { files: 1, bytes });
        }
        if let Some(bytes) = moved
            && from == Standing::Counted
            && to != Standing::Counted
        {
            self.total = self.total.less(Tally { files: 1, bytes });
        }
        if (moved.is_some() || replaced.is_some())
            && (from == Standing::Listing || to == Standing::Listing)
        {
            self.relist()?;
        }
        Ok(())
    }

    /// The domain being listed, listed again from its start.
    fn relist(&mut self) -> Result<(), ContentError> {
        let Some(listing) = self.listing.take() else {
            return Ok(());
        };
        match fs::read_dir(&listing.directory) {
            Ok(entries) => {
                self.listing = Some(Listing {
                    directory: listing.directory,
                    entries,
                    partial: Tally::default(),
                });
            }
            Err(error) if gone(&error) => self.domain = self.domain.saturating_add(1),
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

/// The domains of the tree at `tree_root`, by identity; none where it is gone.
fn sorted_domains(tree_root: &Path) -> Result<Vec<ContentDomainId>, ContentError> {
    let mut domains = Vec::new();
    let listing = match fs::read_dir(tree_root) {
        Ok(listing) => listing,
        Err(error) if gone(&error) => return Ok(domains),
        Err(error) => return Err(error.into()),
    };
    for entry in listing {
        if let Some(domain) = entry?
            .file_name()
            .to_str()
            .and_then(super::gc::parse_domain)
        {
            domains
                .try_reserve_exact(1)
                .map_err(|_| ContentError::Capacity)?;
            domains.push(domain);
        }
    }
    domains.sort();
    Ok(domains)
}

/// The hash and kind a name in the clear gives a chunk or a manifest; none for any other name.
fn clear_name(path: &Path) -> Option<(ContentHash, Kind)> {
    let stem = path.file_stem()?.to_str()?;
    let kind = match path.extension()?.to_str()? {
        "chunk" => Kind::Chunk,
        "manifest" => Kind::Manifest,
        // A chunk a transfer is receiving, staged in parts beside the objects (29 §12.7).
        "part" => Kind::Part,
        // A custody record, in its kind's directory or a round's (29 §12.7).
        "record" => Kind::Record,
        _ => return None,
    };
    Some((sealing::parse_hash(stem)?, kind))
}

/// The length of a file the conversion converts: a file itself, not a link to one, no longer than
/// its kind's bound, which no file the store writes exceeds. None for anything else, which was
/// never an object's. `metadata` is the entry's own, its links not followed.
pub(super) fn convertible(kind: Kind, metadata: &fs::Metadata) -> Option<u64> {
    let bound = match kind {
        Kind::Chunk | Kind::Part => MAX_TRANSFER_CHUNK_BYTES,
        Kind::Manifest => MAX_TRANSFER_MANIFEST_BYTES,
        Kind::Seed => super::SEED_CHUNK_BYTES,
        // The largest record's kind's bound: a checkpoint's.
        Kind::Record => CustodyRecordKind::ALL
            .iter()
            .map(|kind| kind.limit())
            .max()
            .unwrap_or(0),
    };
    let length = metadata.len();
    (metadata.is_file() && usize::try_from(length).is_ok_and(|length| length <= bound))
        .then_some(length)
}

/// The bytes of the entry at `path` where the conversion converts it: a chunk or a manifest in the
/// clear, named by its hash, a file no longer than its kind's bound. None for anything the
/// conversion leaves as it is, and for an entry gone since it was listed.
pub(super) fn counted_file(path: &Path, converts: Converts) -> Result<Option<u64>, ContentError> {
    let Some((_, kind)) = clear_name(path).filter(|(_, kind)| converts.takes(*kind)) else {
        return Ok(None);
    };
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(convertible(kind, &metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// [`counted_file`] of a listed entry, from its listing: its name, and its metadata read beside its
/// directory, no path resolved.
fn counted_entry(entry: &fs::DirEntry, converts: Converts) -> Result<Option<u64>, ContentError> {
    let name = entry.file_name();
    let Some((_, kind)) = clear_name(Path::new(&name)).filter(|(_, kind)| converts.takes(*kind))
    else {
        return Ok(None);
    };
    match entry.metadata() {
        Ok(metadata) => Ok(convertible(kind, &metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// One domain's directory being visited.
struct Visit {
    domain: ContentDomainId,
    directory: PathBuf,
    listing: ReadDir,
    /// A file whose sealed form found no room: taken first at the next step.
    pending: Option<PathBuf>,
    /// A name in the clear was removed since the directory was last flushed.
    removed: bool,
}

/// What a file's visit did.
enum Visited {
    /// Sealed, its name in the clear removed: a file of these bytes.
    Converted(u64),
    Left,
    Held,
}

impl Converting {
    /// A conversion of the store at `root`, nothing counted yet: of its `objects` and its files
    /// beside them (`rest`), as each converts.
    pub(super) fn begin(root: &Path, objects: bool, rest: bool) -> Result<Self, ContentError> {
        let converts = Converts { objects, rest };
        Ok(Self {
            count: Some(Count::begin(root, converts)?),
            rounds: Vec::new(),
            round: 0,
            own: false,
            tree: None,
            domain: None,
            record_dirs: Vec::new(),
            record: 0,
            records: None,
            staging: None,
            staged: !rest,
            converts,
        })
    }

    /// Whether the conversion is still counting what it must convert.
    pub(super) fn counting(&self) -> bool {
        self.count.is_some()
    }
}

impl ContentStore {
    /// Whether a conversion runs in this store: its owner calls [`Self::convert_step`] whenever no
    /// other work waits, until it is done.
    pub fn converting(&self) -> bool {
        self.conversion.is_some() && !self.failed
    }

    /// The store's form and where its conversion stands since this start.
    pub fn conversion_status(&self) -> ConversionStatus {
        let form = if !self.keys.seals() {
            StoreForm::Clear
        } else if !self.keys.converting() && !self.rest.converting() {
            StoreForm::Sealed
        } else if self.progress.counting {
            StoreForm::Counting
        } else {
            StoreForm::Converting
        };
        self.progress.status(form)
    }

    /// A restore about to move the file at `source`, of `domain` in the round named `round`, to
    /// `destination` among the store's own, replacing what stands there: called before the move,
    /// while both are where the count may have met them. While the conversion counts, the count
    /// stays exact ([`Count::restored`]); once it has counted, a file in the clear the move replaces
    /// leaves before the conversion meets it, and is tallied as gone.
    pub(super) fn conversion_restoring(
        &mut self,
        round: &OsString,
        domain: ContentDomainId,
        source: &Path,
        destination: &Path,
    ) -> Result<(), ContentError> {
        if !self.keys.converting() {
            return Ok(());
        }
        let objects = Converts {
            objects: true,
            rest: false,
        };
        let replaced = counted_file(destination, objects)?;
        match self
            .conversion
            .as_mut()
            .and_then(|state| state.count.as_mut())
        {
            Some(count) => {
                count.restored(round, domain, counted_file(source, objects)?, replaced)?;
                self.progress.counted = count.so_far();
            }
            None => {
                if let Some(bytes) = replaced {
                    self.progress.gone.add(bytes);
                }
            }
        }
        Ok(())
    }

    /// The file at `path` of a round the collector deletes, about to go: where it is one the
    /// conversion converts, it left with its round before the conversion met it.
    pub(super) fn conversion_deleting(&mut self, path: &Path) -> Result<(), ContentError> {
        let Some(converts) = self.conversion.as_ref().map(|state| state.converts) else {
            return Ok(());
        };
        if let Some(bytes) = counted_file(path, converts)? {
            self.progress.gone.add(bytes);
        }
        Ok(())
    }

    /// A file of staging's in the clear changed while the store converts (29 §12.7): what the count
    /// passed is kept exact by the change, a listing it is in the middle of is listed again, and
    /// what it has still to list it counts as it finds it.
    pub(super) fn conversion_staging(&mut self, change: StagingChange) -> Result<(), ContentError> {
        if !self.rest.converting() {
            return Ok(());
        }
        if let Some(count) = self
            .conversion
            .as_mut()
            .and_then(|state| state.count.as_mut())
        {
            if count.staging_standing() == Standing::Listing {
                count.relist_staging()?;
                self.progress.counted = count.so_far();
            }
            return Ok(());
        }
        match change {
            StagingChange::Grew(bytes) => self.progress.counted.grow(bytes),
            StagingChange::Appeared(bytes) => self.progress.counted.add(bytes),
            StagingChange::Gone(bytes) => self.progress.gone.add(bytes),
        }
        Ok(())
    }

    /// Whether the collector must leave the quarantine's rounds as they are: the conversion counts
    /// them.
    pub(super) fn conversion_counting(&self) -> bool {
        self.conversion.as_ref().is_some_and(Converting::counting)
    }

    /// One step of the store's conversion: at most `visits` entries visited (counted, while the
    /// conversion counts what it must convert) and at most one file converted, its sealed form read
    /// into the head of `scratch` (a chunk long, the owner's). A step that fails ends this
    /// conversion, its cause kept in the status; the store still reads either form, and its next
    /// open resumes the conversion from its marker. An I/O failure stops the store as any write's
    /// does.
    pub fn convert_step(
        &mut self,
        scratch: &mut [u8],
        visits: usize,
    ) -> Result<Conversion, ContentError> {
        self.check()?;
        let Some(mut state) = self.conversion.take() else {
            return Ok(Conversion::Done);
        };
        let result = self.convert_inner(&mut state, scratch, visits);
        match &result {
            Ok(Conversion::Converted | Conversion::Visited | Conversion::Held) => {
                self.conversion = Some(state);
            }
            Ok(Conversion::Done) => {}
            Err(error) => self.progress.stopped = Some(error.kind()),
        }
        match &result {
            Ok(Conversion::Held) => {
                self.progress.held = true;
                self.progress.held_steps = self.progress.held_steps.saturating_add(1);
            }
            Ok(Conversion::Converted | Conversion::Visited) => self.progress.held = false,
            Ok(Conversion::Done) | Err(_) => {}
        }
        self.mark_failure(&result);
        result
    }

    fn convert_inner(
        &mut self,
        state: &mut Converting,
        scratch: &mut [u8],
        visits: usize,
    ) -> Result<Conversion, ContentError> {
        if let Some(count) = state.count.as_mut() {
            let whole = count.step(&self.root, visits);
            self.progress.counted = count.so_far();
            if !whole? {
                return Ok(Conversion::Visited);
            }
            if let Some(count) = state.count.take() {
                state.rounds = count.rounds;
                state.record_dirs = count.record_dirs;
            }
            self.progress.counting = false;
            return Ok(Conversion::Visited);
        }
        for _ in 0..visits {
            if let Some(visit) = &mut state.domain {
                let next = match visit.pending.take() {
                    Some(path) => Some(path),
                    None => match visit.listing.next() {
                        Some(entry) => Some(entry?.path()),
                        None => None,
                    },
                };
                let Some(path) = next else {
                    if visit.removed {
                        sync_directory(&visit.directory)?;
                    }
                    state.domain = None;
                    continue;
                };
                match self.convert_file(
                    visit.domain,
                    &visit.directory,
                    &path,
                    scratch,
                    state.converts,
                )? {
                    Visited::Converted(bytes) => {
                        visit.removed = true;
                        self.progress.converted.add(bytes);
                        return Ok(Conversion::Converted);
                    }
                    Visited::Left => continue,
                    Visited::Held => {
                        visit.pending = Some(path);
                        return Ok(Conversion::Held);
                    }
                }
            }
            if let Some(tree) = &mut state.tree {
                let Some(entry) = tree.next() else {
                    state.tree = None;
                    continue;
                };
                let entry = entry?;
                let Some(domain) = entry.file_name().to_str().and_then(super::gc::parse_domain)
                else {
                    continue;
                };
                let directory = entry.path();
                match fs::read_dir(&directory) {
                    Ok(listing) => {
                        state.domain = Some(Visit {
                            domain,
                            directory,
                            listing,
                            pending: None,
                            removed: false,
                        });
                    }
                    Err(error) if gone(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                continue;
            }
            if let Some(round) = state
                .rounds
                .get(state.round)
                .filter(|_| state.converts.objects)
            {
                let objects = self.root.join("quarantine").join(round).join("objects");
                state.round = state.round.saturating_add(1);
                // A round the collector deleted since the count listed it is nothing to convert.
                match fs::read_dir(objects) {
                    Ok(listing) => state.tree = Some(listing),
                    Err(error) if gone(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                continue;
            }
            if !state.own {
                state.own = true;
                state.tree = Some(fs::read_dir(self.root.join("objects"))?);
                continue;
            }
            // The custody records' directories (29 §12.7), each round's then the store's own.
            if let Some((kind, directory, listing, removed)) = &mut state.records {
                let Some(entry) = listing.next() else {
                    if *removed {
                        sync_directory(directory)?;
                    }
                    state.records = None;
                    continue;
                };
                let path = entry?.path();
                match self.convert_record(*kind, &path, scratch)? {
                    Visited::Converted(bytes) => {
                        *removed = true;
                        self.progress.converted.add(bytes);
                        return Ok(Conversion::Converted);
                    }
                    Visited::Left => continue,
                    Visited::Held => return Ok(Conversion::Held),
                }
            }
            if let Some(directory) = state.record_dirs.get(state.record).cloned() {
                state.record = state.record.saturating_add(1);
                let kind = record_kind(&directory)?;
                match fs::read_dir(&directory) {
                    Ok(listing) => state.records = Some((kind, directory, listing, false)),
                    Err(error) if gone(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                continue;
            }
            // Staging last (29 §12.7), passes until one meets nothing in the clear.
            if !state.staged {
                let Some(pass) = state.staging.as_mut() else {
                    state.staging = Some(self.staging_pass()?);
                    continue;
                };
                match self.staging_step(pass, scratch)? {
                    StagingStep::Converted => return Ok(Conversion::Converted),
                    StagingStep::Visited => continue,
                    StagingStep::Copied => return Ok(Conversion::Visited),
                    StagingStep::Held => return Ok(Conversion::Held),
                    StagingStep::Ended { met } => {
                        if pass.removed {
                            sync_directory(&self.root.join("staging"))?;
                        }
                        state.staging = None;
                        state.staged = !met;
                        continue;
                    }
                }
            }
            self.mark_sealed()?;
            return Ok(Conversion::Done);
        }
        Ok(Conversion::Visited)
    }

    /// The entry at `path` of `domain`'s directory `directory` (the store's own, or a round's):
    /// where it is a chunk or a manifest in the clear, sealed beside it under its keyed name and
    /// its name in the clear removed; anything else left as it is.
    fn convert_file(
        &mut self,
        domain: ContentDomainId,
        directory: &Path,
        path: &Path,
        scratch: &mut [u8],
        converts: Converts,
    ) -> Result<Visited, ContentError> {
        let Some((hash, kind)) = clear_name(path).filter(|(_, kind)| converts.takes(*kind)) else {
            return Ok(Visited::Left);
        };
        // The same entries the count met (`counted_file`): a file itself, a link to one left as it
        // is, no longer than its kind's bound, read into the scratch, which holds the bound;
        // anything else was never an object's.
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => return Ok(Visited::Left),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Visited::Left);
            }
            Err(error) => return Err(error.into()),
        }
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Visited::Left);
            }
            Err(error) => return Err(error.into()),
        };
        let Some(bytes) = convertible(kind, &file.metadata()?) else {
            return Ok(Visited::Left);
        };
        let length = usize::try_from(bytes).map_err(|_| ContentError::Capacity)?;
        // The domain's key lives in its directory among the store's own objects, a round's files
        // sealed under it too, so a restored file opens where it comes back.
        let own = domain_directory(&self.root, domain);
        let key = self.keys.domain(&own, domain, false)?;
        if kind == Kind::Part {
            return self.convert_part(domain, directory, path, hash, file, length, key, scratch);
        }
        // Sealed already, by the writer since the store began converting or by a conversion cut
        // before its removal: the sealed form stands where it opens to the bytes its name says,
        // and the name in the clear goes; where it does not, the bytes in the clear are sealed
        // over it.
        if key.record_bytes() == 0
            && key.holds_sealed(&key.path(directory, hash, kind)?, hash, scratch)?
        {
            drop(file);
            remove_clear(path)?;
            return Ok(Visited::Converted(bytes));
        }
        let room = match disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            key.file_bytes(length)?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        ) {
            Ok(room) => room,
            Err(ContentError::Capacity) => return Ok(Visited::Held),
            Err(error) => return Err(error),
        };
        let block = scratch.get_mut(..length).ok_or(ContentError::Capacity)?;
        file.read_exact(block)?;
        // The file is exactly the length it was opened at: one written into since is not a file
        // the store installed (every install renames a whole file into place), and is refused.
        let mut extra = [0u8; 1];
        if file.read(&mut extra)? != 0 {
            return Err(ContentError::Corrupt);
        }
        drop(file);
        durable_directory(&own)?;
        let key = self.keys.writable(&own, domain, key)?;
        // Sealed as the bytes are: a file that does not hash to its name is refused as corrupt by
        // every read in either form, so it is converted as it is, never judged here.
        key.install(&key.path(directory, hash, kind)?, block)?;
        room.commit();
        remove_clear(path)?;
        Ok(Visited::Converted(bytes))
    }

    /// A custody record in the clear (29 §12.7), of `kind`, in its kind's directory or a round's:
    /// sealed under the kind's key at its keyed name beside it, and only then its name in the clear
    /// removed. Where the sealed form stands already (written since the store began converting,
    /// or by a conversion cut before its removal) it is the record, and the clear one goes: a named
    /// record is replaced sealed and its clear one removed, so the sealed one is the later. A record
    /// named by its hash (a checkpoint, a transfer manifest) is streamed through `scratch` keyed by
    /// it; a receipt, named for what it is about, is sealed keyed by its own bytes, so two versions
    /// of it are never sealed under one key.
    fn convert_record(
        &mut self,
        kind: CustodyRecordKind,
        path: &Path,
        scratch: &mut [u8],
    ) -> Result<Visited, ContentError> {
        let Some((name, Kind::Record)) = clear_name(path) else {
            return Ok(Visited::Left);
        };
        let mut file = match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => match File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Visited::Left);
                }
                Err(error) => return Err(error.into()),
            },
            Ok(_) => return Ok(Visited::Left),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Visited::Left);
            }
            Err(error) => return Err(error.into()),
        };
        let bytes = file.metadata()?.len();
        let length = usize::try_from(bytes)
            .ok()
            .filter(|length| *length <= kind.limit())
            .ok_or(ContentError::Corrupt)?;
        let own = self.root.join(kind.directory());
        let directory = path.parent().ok_or(ContentError::Invalid)?.to_path_buf();
        let key = self.record_key(kind, false)?;
        if key.record_bytes() == 0 && key.path(&directory, name, Kind::Record)?.is_file() {
            drop(file);
            remove_clear(path)?;
            return Ok(Visited::Converted(bytes));
        }
        let room = match disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            key.file_bytes(length)?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        ) {
            Ok(room) => room,
            Err(ContentError::Capacity) => return Ok(Visited::Held),
            Err(error) => return Err(error),
        };
        durable_directory(&own)?;
        let key = self.keys.writable(&own, kind.key_domain(), key)?;
        let sealed = key.path(&directory, name, Kind::Record)?;
        match kind {
            CustodyRecordKind::Receipt => {
                let block = scratch.get_mut(..length).ok_or(ContentError::Capacity)?;
                file.read_exact(block)?;
                let mut extra = [0u8; 1];
                if file.read(&mut extra)? != 0 {
                    return Err(ContentError::Corrupt);
                }
                drop(file);
                key.install(&sealed, block)?;
            }
            CustodyRecordKind::Checkpoint | CustodyRecordKind::Manifest => {
                key.install_streamed(&sealed, name, &mut file, length, scratch)?;
                drop(file);
            }
        }
        room.commit();
        remove_clear(path)?;
        Ok(Visited::Converted(bytes))
    }

    /// A custody record in the clear of `kind`, of `bytes`, gone from the store's own records while
    /// it converts (replaced sealed, 29 §12.7): what the count passed leaves it, a directory it is
    /// listing is listed again, and one it has still to list never counts it.
    pub(super) fn conversion_record(
        &mut self,
        kind: CustodyRecordKind,
        bytes: u64,
    ) -> Result<(), ContentError> {
        if !self.rest.converting() {
            return Ok(());
        }
        if let Some(count) = self
            .conversion
            .as_mut()
            .and_then(|state| state.count.as_mut())
        {
            match count.record_standing(kind) {
                Standing::Listing => count.relist_records()?,
                Standing::Counted => {
                    count.total = count.total.less(Tally { files: 1, bytes });
                }
                Standing::Ahead => {}
            }
            self.progress.counted = count.so_far();
            return Ok(());
        }
        self.progress.gone.add(bytes);
        Ok(())
    }

    /// A chunk's part in the clear (29 §12.7), a transfer receiving it: its bytes so far read into
    /// `scratch` and written as a record file kept for the chunk under its keyed name beside it,
    /// synced, and only then its name in the clear removed; a sealed copy a crash left beside it is
    /// written over (the part in the clear stands while it does). Its bytes are checked against the
    /// chunk's hash when it is whole, as a part in the clear is.
    #[allow(clippy::too_many_arguments)] // The visit's inputs, each its own owner's.
    fn convert_part(
        &mut self,
        domain: ContentDomainId,
        directory: &Path,
        path: &Path,
        hash: ContentHash,
        mut file: File,
        length: usize,
        key: sealing::DomainKey,
        scratch: &mut [u8],
    ) -> Result<Visited, ContentError> {
        let own = domain_directory(&self.root, domain);
        let room = match disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            super::record_file::records_bytes(length)?
                .checked_add(
                    u64::try_from(super::record_file::PART_HEADER)
                        .map_err(|_| ContentError::Capacity)?,
                )
                .and_then(|bytes| bytes.checked_add(key.record_bytes()))
                .ok_or(ContentError::Capacity)?,
        ) {
            Ok(room) => room,
            Err(ContentError::Capacity) => return Ok(Visited::Held),
            Err(error) => return Err(error),
        };
        let block = scratch.get_mut(..length).ok_or(ContentError::Capacity)?;
        file.read_exact(block)?;
        let mut extra = [0u8; 1];
        if file.read(&mut extra)? != 0 {
            // Grown since it was measured: not the part this visit read.
            return Err(ContentError::Corrupt);
        }
        drop(file);
        durable_directory(&own)?;
        let key = self.keys.writable(&own, domain, key)?;
        let mut part = super::transfer::create_part(
            &key,
            &key.path(directory, hash, Kind::Part)?,
            hash,
            length,
        )?;
        part.append(block)?;
        part.sync()?;
        drop(part);
        room.commit();
        remove_clear(path)?;
        let bytes = u64::try_from(length).map_err(|_| ContentError::Capacity)?;
        Ok(Visited::Converted(bytes))
    }

    /// A chunk's part in the clear of `domain` changed while the store converts (29 §12.7): grew by
    /// a part a transfer sent, or went (whole and installed, or discarded). What the count passed
    /// is kept exact by the change, a domain it is listing is listed again, and one it has still to
    /// list it counts as it finds it.
    pub(super) fn conversion_part(
        &mut self,
        domain: ContentDomainId,
        change: StagingChange,
    ) -> Result<(), ContentError> {
        if !self.rest.converting() {
            return Ok(());
        }
        if let Some(count) = self
            .conversion
            .as_mut()
            .and_then(|state| state.count.as_mut())
        {
            match count.standing(count.rounds.len(), domain) {
                Standing::Listing => count.relist()?,
                Standing::Counted => match change {
                    StagingChange::Grew(bytes) => count.total.grow(bytes),
                    StagingChange::Appeared(bytes) => count.total.add(bytes),
                    StagingChange::Gone(bytes) => {
                        count.total = count.total.less(Tally { files: 1, bytes });
                    }
                },
                Standing::Ahead => {}
            }
            self.progress.counted = count.so_far();
            return Ok(());
        }
        match change {
            StagingChange::Grew(bytes) => self.progress.counted.grow(bytes),
            StagingChange::Appeared(bytes) => self.progress.counted.add(bytes),
            StagingChange::Gone(bytes) => self.progress.gone.add(bytes),
        }
        Ok(())
    }

    /// Every file is sealed: the store is marked so (each domain's directory flushed as the
    /// conversion left it), its marker of the conversion removed, and nothing in the clear is looked
    /// for from here on.
    pub(super) fn mark_sealed(&mut self) -> Result<(), ContentError> {
        if self.keys.converting() {
            write_marker(&self.root, sealing::SEALED)?;
            remove_marker(&self.root, sealing::SEALING)?;
            self.keys.mark_sealed();
        }
        if self.rest.converting() {
            write_marker(&self.root, sealing::REST_SEALED)?;
            remove_marker(&self.root, sealing::REST_SEALING)?;
            self.rest = super::Rest {
                seals: true,
                clear: false,
            };
        }
        self.conversion = None;
        self.progress.counting = false;
        self.progress.finished_ms = now_ms();
        Ok(())
    }
}

/// `name` in the store's root, durable when it returns: what marks a conversion begun or ended.
pub(super) fn write_marker(root: &Path, name: &str) -> Result<(), ContentError> {
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(root.join(name))?
        .sync_all()?;
    sync_directory(root)?;
    Ok(())
}

/// `name` gone from the store's root, durably.
pub(super) fn remove_marker(root: &Path, name: &str) -> Result<(), ContentError> {
    match fs::remove_file(root.join(name)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    sync_directory(root)?;
    Ok(())
}

/// A name in the clear removed; its directory is flushed as the conversion leaves it.
pub(super) fn remove_clear(path: &Path) -> Result<(), ContentError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// The custody records' directories a conversion lists, in its order: each round's kinds (by the
/// rounds' names), then the store's own.
fn record_directories(root: &Path, rounds: &[OsString]) -> Result<Vec<PathBuf>, ContentError> {
    let mut directories = Vec::new();
    directories
        .try_reserve_exact(
            rounds
                .len()
                .saturating_add(1)
                .saturating_mul(CustodyRecordKind::ALL.len()),
        )
        .map_err(|_| ContentError::Capacity)?;
    for round in rounds {
        for kind in CustodyRecordKind::ALL {
            directories.push(root.join("quarantine").join(round).join(kind.directory()));
        }
    }
    for kind in CustodyRecordKind::ALL {
        directories.push(root.join(kind.directory()));
    }
    Ok(directories)
}

/// The kind of record a records' directory holds, by its name.
fn record_kind(directory: &Path) -> Result<CustodyRecordKind, ContentError> {
    let name = directory.file_name().and_then(|name| name.to_str());
    CustodyRecordKind::ALL
        .into_iter()
        .find(|kind| Some(kind.directory()) == name)
        .ok_or(ContentError::Invalid)
}

/// Whether a directory the conversion lists is gone, or never was one: a round deleted as it is
/// listed, or a file where a directory may stand.
fn gone(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
