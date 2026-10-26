use focal_memory::{
    BudgetLane, DiskBudget, DiskBudgetConfig, DiskKind, DiskReservation, DiskStats,
};
use focal_model::{ContentClass, ContentDomainId, ContentHash, ContentRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

const MANIFEST_MAGIC: &[u8] = b"focal.evidence.manifest\0\x01\0";
/// The most a manifest's magic and header take before its chunk list: the
/// magic, the schema, domain, class, length and stream digest, and the
/// chunk count, each varint at its widest.
const MANIFEST_HEADER_BOUND: usize = 256;
const UPLOAD_SCHEMA: u16 = 1;
#[path = "upload_terminal.rs"]
mod terminal;
/// Permanent scoped upload IDs retained as small terminal records. Active
/// admission reserves its completion slot. No clock-based eviction is safe.
pub const MAX_TERMINAL_UPLOADS: usize = 65_536;

/// Format bounds shared by storage, peer transfer and cold replica recovery.
pub const MAX_TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSFER_MANIFEST_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSFER_CONTENT_BYTES: u64 =
    (MAX_TRANSFER_MANIFEST_BYTES as u64 / 33) * MAX_TRANSFER_CHUNK_BYTES as u64;

/// Immutable local records used by the custody owner. Writing a record alone
/// grants no readiness authority; that owner must verify its bound contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CustodyRecordKind {
    Checkpoint,
    Manifest,
    /// One copy's verified receipt for one object (`custody_receipt`).
    Receipt,
}
impl CustodyRecordKind {
    pub(crate) fn directory(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoints",
            Self::Manifest => "custody",
            Self::Receipt => "receipts",
        }
    }
    fn tag(self) -> u8 {
        match self {
            Self::Checkpoint => 0,
            Self::Manifest => 1,
            Self::Receipt => 2,
        }
    }
    /// What the kind's key directory is made under (29 §12.7): each kind's directory a key of its
    /// own, so a record carried into another kind's directory opens to nothing.
    pub(crate) fn key_domain(self) -> ContentDomainId {
        match self {
            Self::Checkpoint => ContentDomainId(*b"focal checkpoint"),
            Self::Manifest => ContentDomainId(*b"focal manifests "),
            Self::Receipt => ContentDomainId(*b"focal receipts  "),
        }
    }
    pub(crate) const ALL: [Self; 3] = [Self::Checkpoint, Self::Manifest, Self::Receipt];
    pub(crate) fn limit(self) -> usize {
        match self {
            Self::Checkpoint => 8 * 1024 * 1024,
            Self::Manifest => 16 * 1024,
            Self::Receipt => crate::custody_receipt::RECEIPT_BYTES,
        }
    }
}

#[path = "transfer.rs"]
mod transfer;
pub use transfer::{ImportCompletion, TransferManifest, describe_encoded};
#[path = "gc.rs"]
mod gc;
pub use gc::{CollectorConfig, CollectorReport, ProtectionSet, SeedReport};
#[path = "seeds.rs"]
mod seeds;
pub use seeds::{OpenedSeeds, SEED_CHUNK_BYTES, SeedBatch, SeedCommit, SeedReader, SeedStore};
#[path = "chunk_convert.rs"]
mod chunk_convert;
pub use chunk_convert::{SEED_JOB_CHUNKS, SeedJob, SeedJobDone, SeedStep};
#[path = "cold.rs"]
mod cold;
pub use cold::{
    ColdGeometry, ColdPending, ColdReader, ColdRoom, ColdState, ColdStore, ColdStreams,
    install_cold_chunk,
};
// Public for the client's upload journal, which stages an upload's bytes in a part as staging
// does (29 §12.11).
#[path = "record_file.rs"]
pub mod record_file;

#[path = "native_artifact.rs"]
mod native_artifact;
#[path = "sealing.rs"]
mod sealing;
#[path = "staging.rs"]
mod staging;
pub use sealing::ContentForm;
#[path = "convert.rs"]
mod convert;
pub use convert::{Conversion, ConversionStatus, StoreForm, Tally};

/// The keys of the data directory a test's store lies in, as a node's start finds or makes them
/// (`focal_seal::test_keys`): every test store is sealed, as every store is.
#[cfg(test)]
pub(crate) fn test_keys(root: impl AsRef<Path>) -> focal_seal::DataKeys {
    #[allow(clippy::unwrap_used)]
    focal_seal::test_keys(root.as_ref()).unwrap()
}

/// A stored file's bytes as a test's scan of a directory takes them: an empty file without a read.
/// A store's held lock is an empty file, and Windows locks its bytes against every other handle
/// (a read there is refused, `ERROR_LOCK_VIOLATION`), where a Unix lock is advisory.
#[cfg(test)]
pub(crate) fn stored_bytes(path: &Path) -> Vec<u8> {
    #[allow(clippy::unwrap_used)]
    if fs::metadata(path).unwrap().len() == 0 {
        return Vec::new();
    }
    #[allow(clippy::unwrap_used)]
    fs::read(path).unwrap()
}
pub use native_artifact::*;

#[derive(Debug, Clone)]
pub struct StoreLimits {
    /// Maximum local upload length. Imported immutable trees use format bounds.
    pub max_content_bytes: u64,
    pub max_staging_bytes: u64,
    pub max_uploads: usize,
    /// Preferred local upload chunk size. Imported format chunks may be larger.
    pub chunk_bytes: usize,
    /// Local upload admission limit. Existing/imported manifests use the format cap.
    pub max_manifest_bytes: usize,
}

impl StoreLimits {
    /// Staging allowance of one content domain: half of the whole, and never
    /// less than one maximal upload, so one tenant cannot fill the staging
    /// area while another still admits its own upload.
    pub fn domain_staging_bytes(&self) -> u64 {
        (self.max_staging_bytes / 2).max(self.max_content_bytes)
    }
}

/// Promise `bytes` of the store's volume in `lane`, sampling the volume when
/// the envelope's cadence says so. Refused as `Capacity` before any write.
fn disk_reserve(
    disk: &DiskBudget,
    root: &Path,
    kind: DiskKind,
    lane: BudgetLane,
    bytes: u64,
) -> Result<DiskReservation, ContentError> {
    disk.refresh_with(|| focal_platform::available_space(root));
    disk.reserve(kind, lane, bytes)
        .map_err(|_| ContentError::Capacity)
}

#[derive(Debug, thiserror::Error)]
pub enum ContentError {
    #[error("evidence IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("evidence store already has a writer")]
    Locked,
    #[error("invalid evidence-store limits or upload")]
    Invalid,
    #[error("evidence upload or read exceeds its declared budget")]
    Capacity,
    #[error("unknown upload")]
    MissingUpload,
    #[error("upload identity is durably finished and cannot be reopened")]
    FinishedUpload,
    #[error("upload offset differs from the durable offset {0}")]
    Offset(u64),
    #[error("upload is incomplete: {received} of {expected} bytes")]
    Incomplete { received: u64, expected: u64 },
    #[error("evidence checksum, manifest, or length is invalid")]
    Corrupt,
    #[error("evidence format: {0}")]
    Format(#[from] postcard::Error),
    #[error("evidence writer stopped after an I/O failure; reopen to recover")]
    Failed,
}

/// What kind of failure a [`ContentError`] is, kept where the error cannot be (an I/O error is not
/// copied): a stopped conversion's cause in its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentErrorKind {
    Io(std::io::ErrorKind),
    Locked,
    Invalid,
    Capacity,
    MissingUpload,
    FinishedUpload,
    Offset,
    Incomplete,
    Corrupt,
    Format,
    Failed,
}

impl ContentError {
    pub fn kind(&self) -> ContentErrorKind {
        match self {
            Self::Io(error) => ContentErrorKind::Io(error.kind()),
            Self::Locked => ContentErrorKind::Locked,
            Self::Invalid => ContentErrorKind::Invalid,
            Self::Capacity => ContentErrorKind::Capacity,
            Self::MissingUpload => ContentErrorKind::MissingUpload,
            Self::FinishedUpload => ContentErrorKind::FinishedUpload,
            Self::Offset(_) => ContentErrorKind::Offset,
            Self::Incomplete { .. } => ContentErrorKind::Incomplete,
            Self::Corrupt => ContentErrorKind::Corrupt,
            Self::Format(_) => ContentErrorKind::Format,
            Self::Failed => ContentErrorKind::Failed,
        }
    }
}

impl std::fmt::Display for ContentErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(kind) => write!(f, "io: {kind}"),
            Self::Locked => f.write_str("locked"),
            Self::Invalid => f.write_str("invalid"),
            Self::Capacity => f.write_str("capacity"),
            Self::MissingUpload => f.write_str("missing upload"),
            Self::FinishedUpload => f.write_str("finished upload"),
            Self::Offset => f.write_str("offset"),
            Self::Incomplete => f.write_str("incomplete"),
            Self::Corrupt => f.write_str("corrupt"),
            Self::Format => f.write_str("format"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UploadId(pub [u8; 16]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct UploadMeta {
    schema: u16,
    id: UploadId,
    domain: ContentDomainId,
    class: ContentClass,
    expected_length: u64,
    /// Digest of the byte stream, distinct from its chunk-tree manifest root.
    expected_digest: Option<ContentHash>,
}

struct Upload {
    meta: UploadMeta,
    /// Its staged bytes, in the form its metadata was written in (29 §12.7).
    part: staging::Part,
    part_path: PathBuf,
    meta_path: PathBuf,
    offset: u64,
    /// The staging bytes promised to this upload; returned when the part is
    /// removed. Absent only for an upload recovered on a volume that could
    /// not promise its remainder, which the watermark still guards.
    _staging: Option<DiskReservation>,
}
/// An upload's seal in slices (the audit's F51), held by its one caller (a
/// node's custody coordinator): the staged bytes read a chunk at a time, each
/// chunk installed under its name as it is read, the stream hashed over
/// them, the manifest's chunk list growing; the manifest is installed once
/// the last byte is read ([`ContentStore::advance_seal`]). The volume's
/// promise for the sealed copy is held from before the first chunk to the
/// manifest. Dropped, it releases both; a seal asked again begins again, the
/// chunks already installed found in place.
pub struct UploadSealing {
    id: UploadId,
    length: u64,
    done: u64,
    whole: blake3::Hasher,
    chunks: Vec<Chunk>,
    objects: Option<DiskReservation>,
}
impl UploadSealing {
    /// The heap and value storage the seal holds: its chunk list, reserved
    /// whole when it began.
    pub fn resident_bytes(&self) -> Result<usize, ContentError> {
        self.chunks
            .capacity()
            .checked_mul(std::mem::size_of::<Chunk>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .ok_or(ContentError::Capacity)
    }
    /// The bytes of the upload sealed so far, of its length.
    pub fn done(&self) -> (u64, u64) {
        (self.done, self.length)
    }
    /// The most a seal of `count` chunks holds: its chunk list, and at its
    /// end the manifest's encoding (at most forty bytes a chunk and its
    /// header) twice over, as the encoding is written and then framed, each
    /// buffer at most twice what it holds.
    pub fn resident_for(count: usize) -> Result<usize, ContentError> {
        let encoded = count
            .checked_mul(40)
            .and_then(|n| n.checked_add(MANIFEST_HEADER_BOUND))
            .ok_or(ContentError::Capacity)?;
        count
            .checked_mul(std::mem::size_of::<Chunk>())
            .and_then(|n| n.checked_add(encoded.checked_mul(4)?))
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .ok_or(ContentError::Capacity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Chunk {
    hash: ContentHash,
    length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Manifest {
    schema: u16,
    domain: ContentDomainId,
    class: ContentClass,
    length: u64,
    stream_digest: ContentHash,
    chunks: Vec<Chunk>,
}

/// An installed object's verification in slices (the audit's F51): what
/// [`ContentStore::verify`] reads at once — every chunk against its name, the
/// stream against the manifest's digest — one chunk a call
/// ([`ContentStore::advance_verify`]), so the store's one owner serves other
/// work between chunks. It holds the object's chunk list, validated once
/// from its manifest; its holder charges [`Self::resident_bytes`].
pub struct ObjectVerification {
    reference: ContentRef,
    chunks: Vec<Chunk>,
    stream_digest: ContentHash,
    next: usize,
    whole: blake3::Hasher,
}
impl ObjectVerification {
    pub fn reference(&self) -> &ContentRef {
        &self.reference
    }
    /// The chunks verified so far, of the object's [`Self::chunks`].
    pub fn verified(&self) -> usize {
        self.next
    }
    pub fn chunks(&self) -> usize {
        self.chunks.len()
    }
    /// The heap and value storage a verification holds.
    pub fn resident_bytes(&self) -> Result<usize, ContentError> {
        self.chunks
            .capacity()
            .checked_mul(std::mem::size_of::<Chunk>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .ok_or(ContentError::Capacity)
    }
    /// The most a verification of an object whose manifest is `encoded`
    /// bytes holds as it begins: the encoding while it is decoded, and the
    /// chunk list it names ([`Self::resident_bytes`] is never more).
    pub fn resident_for(encoded: usize) -> Result<usize, ContentError> {
        chunk_list_bound(encoded)
            .and_then(|n| n.checked_add(encoded))
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .ok_or(ContentError::Capacity)
    }
}

/// The most the chunk list of a manifest of `encoded` bytes holds decoded:
/// an entry for every 33 bytes of the encoding, as each names a 32-byte hash
/// and at least a byte of length (as the decoder checks before it reserves).
pub(crate) fn chunk_list_bound(encoded: usize) -> Option<usize> {
    encoded
        .checked_div(33)?
        .checked_mul(std::mem::size_of::<Chunk>())
}

/// The fixed header contains no allocating fields. Validate the sequence count
/// against actual encoded bytes before allocating; serde's generic Vec visitor
/// trusts size_hint and can otherwise allocate 1 MiB for a truncated tiny input.
fn decode_manifest(bytes: &[u8]) -> Result<Manifest, ContentError> {
    #[derive(Deserialize)]
    struct Header {
        schema: u16,
        domain: ContentDomainId,
        class: ContentClass,
        length: u64,
        stream_digest: ContentHash,
        count: usize,
    }
    if bytes.len() > MAX_TRANSFER_MANIFEST_BYTES {
        return Err(ContentError::Capacity);
    }
    let (header, mut remaining) = postcard::take_from_bytes::<Header>(bytes)?;
    // Each Chunk contains 32 hash bytes and at least one length varint byte.
    let maximum_count = remaining
        .len()
        .checked_div(33)
        .ok_or(ContentError::Corrupt)?;
    if header.count > maximum_count {
        return Err(ContentError::Corrupt);
    }
    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(header.count)
        .map_err(|_| ContentError::Capacity)?;
    for _ in 0..header.count {
        let (chunk, tail) = postcard::take_from_bytes::<Chunk>(remaining)?;
        chunks.push(chunk);
        remaining = tail;
    }
    if !remaining.is_empty() {
        return Err(ContentError::Corrupt);
    }
    Ok(Manifest {
        schema: header.schema,
        domain: header.domain,
        class: header.class,
        length: header.length,
        stream_digest: header.stream_digest,
        chunks,
    })
}

/// Node-owned store. The sealed return value proves LOCAL durable custody only.
/// Distributed ingress must add remote custody before promising a stronger policy.
/// The form of a content store's files beside its objects (29 §12.7: uploads being staged, chunks a
/// transfer is receiving, custody records), which the at-rest level opens as the content level
/// opens the objects': below it they are written byte for byte as every earlier binary wrote them,
/// so a node rolled back below it reads them. Its markers (`REST_SEALING`, `REST_SEALED`) say where
/// its conversion stands, apart from the objects'.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Rest {
    seals: bool,
    clear: bool,
}
impl Rest {
    /// Whether these files are written sealed.
    pub(crate) fn seals(self) -> bool {
        self.seals
    }
    /// Whether one of them may lie in the clear.
    pub(crate) fn clear_allowed(self) -> bool {
        self.clear
    }
    /// Whether they lie in either form: written sealed, not yet marked sealed.
    pub(crate) fn converting(self) -> bool {
        self.seals && self.clear
    }
}

pub struct ContentStore {
    root: PathBuf,
    limits: StoreLimits,
    uploads: BTreeMap<UploadId, Upload>,
    staged_bytes: u64,
    /// Staged bytes per content domain, so one tenant's uploads cannot fill
    /// the whole staging allowance.
    staged_by_domain: BTreeMap<ContentDomainId, u64>,
    terminal_uploads: usize,
    failed: bool,
    disk: DiskBudget,
    /// The collector's pass in progress (26 §5).
    collector: gc::CollectorState,
    /// Sealed objects' chunk plans, each validated once under its immutable
    /// digest (the audit's F19); a lock, since readers share the store
    /// across threads, held for one range's read; poisoned, it is an error.
    plans: Mutex<PlanCache>,
    /// The node's content key, held for the writer's life where it seals (29 §11): every domain's
    /// key is unwrapped under it for one file. Its form says where a file may lie.
    keys: sealing::ContentKeys<'static>,
    /// The form of its files beside its objects (29 §12.7).
    rest: Rest,
    /// The conversion of the store's files in the clear, while one runs ([`Self::convert_step`]).
    conversion: Option<convert::Converting>,
    /// What the conversion counted and did since this start ([`Self::conversion_status`]).
    progress: convert::Progress,
    /// Where the data directory's keys are, which a reader of this store is given
    /// ([`ContentStore::reader`]).
    data_keys: focal_seal::DataKeys,
    _writer_lock: focal_platform::FileLock,
}

/// A sealed object's chunks with each chunk's start: a range finds its first
/// chunk by search instead of scanning the manifest from its head, and the
/// manifest is read, hashed and validated once per object while its plan is
/// held. Every chunk delivered is still read whole and hashed.
struct Plan {
    class: ContentClass,
    length: u64,
    chunks: Vec<Chunk>,
    starts: Vec<u64>,
}
impl Plan {
    fn of(manifest: Manifest) -> Result<Self, ContentError> {
        let mut starts = Vec::new();
        starts
            .try_reserve_exact(manifest.chunks.len())
            .map_err(|_| ContentError::Capacity)?;
        let mut start = 0u64;
        for chunk in &manifest.chunks {
            starts.push(start);
            start = start
                .checked_add(u64::from(chunk.length))
                .ok_or(ContentError::Corrupt)?;
        }
        Ok(Self {
            class: manifest.class,
            length: manifest.length,
            chunks: manifest.chunks,
            starts,
        })
    }
    /// The chunk holding `offset`: the last one starting at or before it.
    fn first_chunk(&self, offset: u64) -> usize {
        self.starts
            .partition_point(|&start| start <= offset)
            .saturating_sub(1)
    }
}
/// The plans held: as many as the store admits uploads, the least recently
/// used given up first — a bound on memory of that many manifests' chunks,
/// the same allowance the store's upload states have.
struct PlanCache {
    plans: VecDeque<((ContentDomainId, ContentHash), Plan)>,
    bound: usize,
    /// Manifests read and validated: a test's evidence that a paged read
    /// loads its plan once.
    loads: u64,
}
impl PlanCache {
    fn new(bound: usize) -> Self {
        Self {
            plans: VecDeque::new(),
            bound: bound.max(1),
            loads: 0,
        }
    }
    /// The object's plan, loaded and validated once, then held; a reference
    /// that names the same root with another class or length is refused as
    /// the manifest's own validation refuses it.
    fn plan(
        &mut self,
        root: &Path,
        keys: &sealing::ContentKeys<'_>,
        reference: &ContentRef,
    ) -> Result<&Plan, ContentError> {
        let key = (reference.domain, reference.root);
        if let Some(at) = self.plans.iter().position(|(held, _)| *held == key) {
            if let Some(entry) = self.plans.remove(at) {
                self.plans.push_back(entry);
            }
        } else {
            let plan = Plan::of(manifest_at(root, keys, reference)?)?;
            self.loads = self.loads.saturating_add(1);
            while self.plans.len() >= self.bound {
                self.plans.pop_front();
            }
            self.plans
                .try_reserve(1)
                .map_err(|_| ContentError::Capacity)?;
            self.plans.push_back((key, plan));
        }
        let (_, plan) = self.plans.back().ok_or(ContentError::Failed)?;
        if plan.class != reference.class || plan.length != reference.length {
            return Err(ContentError::Corrupt);
        }
        Ok(plan)
    }
}

impl ContentStore {
    /// Local upload/download page preference, independent of imported chunk sizes.
    pub fn upload_chunk_bytes(&self) -> usize {
        self.limits.chunk_bytes
    }
    pub fn max_chunk_bytes(&self) -> usize {
        MAX_TRANSFER_CHUNK_BYTES
    }
    pub fn max_manifest_bytes(&self) -> usize {
        MAX_TRANSFER_MANIFEST_BYTES
    }
    /// The length of an installed object's manifest, from its file's
    /// metadata, nothing read: what a holder charges reading it at
    /// ([`TransferManifest::resident_for`], [`ObjectVerification::resident_for`]).
    pub fn manifest_length(&self, reference: &ContentRef) -> Result<usize, ContentError> {
        self.check()?;
        manifest_length_at(&self.root, &self.keys, reference)
    }
    /// The chunk of `domain` named `hash`, of `length` bytes, read and
    /// checked against its name: what a sender that holds the object's
    /// manifest moves, naming each chunk by it, the manifest never read
    /// again.
    pub fn read_chunk(
        &self,
        domain: ContentDomainId,
        hash: ContentHash,
        length: usize,
    ) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        if length > MAX_TRANSFER_CHUNK_BYTES {
            return Err(ContentError::Capacity);
        }
        let directory = domain_directory(&self.root, domain);
        let key = self.keys.domain(&directory, domain, false)?;
        let mut bytes = zeroed_buffer(length)?;
        verify_chunk_into(&directory, &key, hash, length, &mut bytes)?;
        Ok(bytes)
    }
    /// The uploads the store stages at once at most.
    pub fn max_uploads(&self) -> usize {
        self.limits.max_uploads
    }
    /// Open the store guarded by the standard disk watermark alone, sealing what it installs.
    pub fn open(
        root: impl AsRef<Path>,
        limits: StoreLimits,
        keys: &focal_seal::DataKeys,
    ) -> Result<Self, ContentError> {
        let disk =
            DiskBudget::new(DiskBudgetConfig::default()).map_err(|_| ContentError::Capacity)?;
        Self::open_with_disk(root, limits, disk, keys, ContentForm::Sealed)
    }
    /// Open the store on the disk envelope shared by every durable owner of
    /// its volume: uploads, sealed objects, imports and custody records are
    /// promised their bytes before any acknowledgement.
    ///
    /// `form` is what the node's fence asks of its files (29 §11): in the clear below the content
    /// level, sealed from it. A store marked sealed stays sealed, and one whose conversion began
    /// finishes it, whatever `form` asks; a store asked to seal that holds files in the clear is
    /// marked as converting and converted by its owner as it serves ([`Self::convert_step`]), and
    /// one that holds nothing is sealed at once.
    pub fn open_with_disk(
        root: impl AsRef<Path>,
        limits: StoreLimits,
        disk: DiskBudget,
        keys: &focal_seal::DataKeys,
        form: ContentForm,
    ) -> Result<Self, ContentError> {
        Self::open_forms(root, limits, disk, keys, form, form)
    }

    /// [`Self::open_with_disk`], its files beside its objects in `rest` (29 §12.7): what the at-rest
    /// level asks of them, as `form` is what the content level asks of the objects. They are sealed
    /// only where the objects are (the at-rest level is past the content level), are marked so apart
    /// from them, and convert with them, or alone where the objects are sealed already.
    pub fn open_forms(
        root: impl AsRef<Path>,
        limits: StoreLimits,
        disk: DiskBudget,
        keys: &focal_seal::DataKeys,
        form: ContentForm,
        rest: ContentForm,
    ) -> Result<Self, ContentError> {
        if limits.chunk_bytes == 0
            || limits.chunk_bytes > MAX_TRANSFER_CHUNK_BYTES
            || limits.max_uploads == 0
            || limits.max_manifest_bytes < MANIFEST_MAGIC.len()
            || limits.max_manifest_bytes > MAX_TRANSFER_MANIFEST_BYTES
            || limits.max_content_bytes > limits.max_staging_bytes
        {
            return Err(ContentError::Invalid);
        }
        let root = root.as_ref().to_path_buf();
        durable_directory(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("LOCK"))?;
        let lock = focal_platform::FileLock::exclusive(lock).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                ContentError::Locked
            } else {
                ContentError::Io(e)
            }
        })?;
        durable_directory(&root.join("staging"))?;
        durable_directory(&root.join("objects"))?;
        durable_directory(&root.join("checkpoints"))?;
        durable_directory(&root.join("custody"))?;
        durable_directory(&root.join("receipts"))?;
        let plans = Mutex::new(PlanCache::new(limits.max_uploads));
        let data_keys = keys.clone();
        let sealed = root.join(sealing::SEALED).try_exists()?;
        let begun = root.join(sealing::SEALING).try_exists()?;
        let keys = if sealed {
            // A conversion that ended before its marker went.
            convert::remove_marker(&root, sealing::SEALING)?;
            sealing::ContentKeys::sealing(keys, false)?
        } else if begun || form == ContentForm::Sealed {
            // Marked before anything is sealed: from here the store holds files in either form,
            // and only a writer that reads both may open it.
            if !begun {
                convert::write_marker(&root, sealing::SEALING)?;
            }
            sealing::ContentKeys::sealing(keys, true)?
        } else {
            sealing::ContentKeys::clear()
        };
        let rest_begun = root.join(sealing::REST_SEALING).try_exists()?;
        let rest = if !keys.seals() {
            Rest {
                seals: false,
                clear: true,
            }
        } else if root.join(sealing::REST_SEALED).try_exists()? {
            // A conversion that ended before its marker went.
            convert::remove_marker(&root, sealing::REST_SEALING)?;
            Rest {
                seals: true,
                clear: false,
            }
        } else if rest_begun || rest == ContentForm::Sealed {
            if !rest_begun {
                convert::write_marker(&root, sealing::REST_SEALING)?;
            }
            Rest {
                seals: true,
                clear: true,
            }
        } else {
            Rest {
                seals: false,
                clear: true,
            }
        };
        let mut store = Self {
            root,
            limits,
            uploads: BTreeMap::new(),
            staged_bytes: 0,
            staged_by_domain: BTreeMap::new(),
            terminal_uploads: 0,
            disk,
            failed: false,
            collector: gc::CollectorState::new(),
            plans,
            keys,
            rest,
            conversion: None,
            progress: convert::Progress::default(),
            data_keys,
            _writer_lock: lock,
        };
        store.recover()?;
        // A previous process may have renamed a synced file and failed before
        // its directory sync. Re-establish namespace durability before reads
        // can attest custody or recovered offsets can be acknowledged.
        for entry in fs::read_dir(store.root.join("objects"))? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                sync_directory(&entry.path())?;
            }
        }
        sync_directory(&store.root.join("objects"))?;
        sync_directory(&store.root.join("checkpoints"))?;
        sync_directory(&store.root.join("custody"))?;
        sync_directory(&store.root.join("staging"))?;
        // Quarantine rounds moved into before a crash are durable by their
        // own directory syncs; the root that lists them is re-synced here.
        if store.root.join("quarantine").is_dir() {
            sync_directory(&store.root.join("quarantine"))?;
        }
        // Re-sync created ancestors left visible by a previous mkdir/sync
        // failure; filesystem root itself is not a newly created entry.
        for directory in store.root.ancestors() {
            if directory.as_os_str().is_empty() {
                sync_directory(Path::new("."))?;
            } else if directory.parent().is_some() {
                sync_directory(directory)?;
            }
        }
        let (objects, files) = (store.keys.converting(), store.rest.converting());
        if objects || files {
            store.progress = convert::Progress::begun(begun || rest_begun);
            if holds_nothing_to_convert(&store.root, objects, files)? {
                store.mark_sealed()?;
            } else {
                store.conversion = Some(convert::Converting::begin(&store.root, objects, files)?);
            }
        }
        Ok(store)
    }

    /// Content-addressed immutable custody metadata. The caller reserves read
    /// scratch up to the kind's format bound before retrying an existing record.
    /// Records are retained across reopen and are never removed by upload or
    /// transfer expiration. A failed write cannot return a durable record hash.
    pub fn install_custody_record(
        &mut self,
        kind: CustodyRecordKind,
        bytes: &[u8],
    ) -> Result<ContentHash, ContentError> {
        self.check()?;
        if bytes.is_empty() || bytes.len() > kind.limit() {
            return Err(ContentError::Capacity);
        }
        let hash = ContentHash(*blake3::hash(bytes).as_bytes());
        let key = self.record_key(kind, false)?;
        if !self.rest.seals() {
            let path = self
                .root
                .join(kind.directory())
                .join(format!("{hash}.record"));
            let record = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
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
            return Ok(hash);
        }
        // Sealed (29 §12.7): a record already there, in either form, that opens to these bytes is
        // kept; one that opens to anything else is corrupt under a content-addressed name.
        for (path, sealed) in self.record_places(kind, hash)? {
            let held = match self.read_record_at(kind, &path, sealed, bytes.len()) {
                Ok(held) => held,
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            if held != bytes {
                return Err(ContentError::Corrupt);
            }
            refresh_mtime(&path)?;
            return Ok(hash);
        }
        let directory = self.root.join(kind.directory());
        let record = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Completion,
            key.file_bytes(bytes.len())?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        )?;
        let installed = self
            .keys
            .writable(&directory, kind.key_domain(), key)
            .and_then(|key| {
                key.install(&key.path(&directory, hash, sealing::Kind::Record)?, bytes)
            });
        if let Err(error) = installed {
            if matches!(error, ContentError::Io(_)) {
                self.failed = true;
            }
            return Err(error);
        }
        record.commit();
        Ok(hash)
    }
    /// Install or replace a record under a caller-chosen name (a receipt is
    /// named by what it is about, so it is found without a scan); the bytes
    /// are promised their volume space first and land atomically.
    pub fn replace_named_custody_record(
        &mut self,
        kind: CustodyRecordKind,
        name: ContentHash,
        bytes: &[u8],
    ) -> Result<(), ContentError> {
        self.check()?;
        if bytes.is_empty() || bytes.len() > kind.limit() {
            return Err(ContentError::Capacity);
        }
        let directory = self.root.join(kind.directory());
        let key = self.record_key(kind, false)?;
        if self.rest.seals() {
            return self.replace_sealed_record(kind, key, name, bytes);
        }
        let path = directory.join(format!("{name}.record"));
        // The identical record on the volume already needs no promise and
        // adds no byte.
        if !already_installed(&path, bytes, None)? {
            let record = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
                BudgetLane::Completion,
                u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?,
            )?;
            let installed = durable_directory(&directory)
                .map_err(ContentError::Io)
                .and_then(|()| atomic_install(&path, bytes));
            if let Err(error) = installed {
                if matches!(error, ContentError::Io(_)) {
                    self.failed = true;
                }
                return Err(error);
            }
            record.commit();
        }
        Ok(())
    }
    /// A named record's bytes, `None` when none was installed: in the clear first while a record may
    /// lie in the clear, then sealed under its keyed name (29 §12.7).
    pub fn read_named_custody_record(
        &self,
        kind: CustodyRecordKind,
        name: ContentHash,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, ContentError> {
        self.check()?;
        let most = max_bytes.min(kind.limit());
        for (path, sealed) in self.record_places(kind, name)? {
            match self.read_record_at(kind, &path, sealed, most) {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    /// Kind `kind`'s key directory's key: made with its record where `create` is set and the store
    /// seals (29 §12.7).
    pub(crate) fn record_key(
        &self,
        kind: CustodyRecordKind,
        create: bool,
    ) -> Result<sealing::DomainKey, ContentError> {
        self.keys
            .domain(&self.root.join(kind.directory()), kind.key_domain(), create)
    }

    /// Where kind `kind`'s record named `name` may lie, each form's place with whether it seals: its
    /// name in the clear while a record may lie in the clear, its keyed name once the kind's key
    /// directory stands.
    pub(crate) fn record_places(
        &self,
        kind: CustodyRecordKind,
        name: ContentHash,
    ) -> Result<Vec<(PathBuf, bool)>, ContentError> {
        let directory = self.root.join(kind.directory());
        let mut places = Vec::new();
        places
            .try_reserve_exact(2)
            .map_err(|_| ContentError::Capacity)?;
        if self.rest.clear_allowed() {
            places.push((directory.join(format!("{name}.record")), false));
        }
        let key = self.record_key(kind, false)?;
        if self.rest.seals() && key.record_bytes() == 0 {
            places.push((key.path(&directory, name, sealing::Kind::Record)?, true));
        }
        Ok(places)
    }

    /// The record at `path` read whole in its form, at most `most` bytes.
    pub(crate) fn read_record_at(
        &self,
        kind: CustodyRecordKind,
        path: &Path,
        sealed: bool,
        most: usize,
    ) -> Result<Vec<u8>, ContentError> {
        if !path.is_file() {
            return Err(ContentError::Io(std::io::Error::from(
                std::io::ErrorKind::NotFound,
            )));
        }
        if sealed {
            self.record_key(kind, false)?.read_record(path, most)
        } else {
            read_bounded(path, most)
        }
    }

    /// A named record replaced sealed (29 §12.7): the same bytes there already need no promise and
    /// add no byte; otherwise promised its file's bytes and sealed at its keyed name. A record in
    /// the clear of that name goes either way, since a reader looks in the clear first and would
    /// meet the bytes it replaces.
    fn replace_sealed_record(
        &mut self,
        kind: CustodyRecordKind,
        key: sealing::DomainKey,
        name: ContentHash,
        bytes: &[u8],
    ) -> Result<(), ContentError> {
        let directory = self.root.join(kind.directory());
        let held = if key.record_bytes() == 0 {
            let path = key.path(&directory, name, sealing::Kind::Record)?;
            match self.read_record_at(kind, &path, true, kind.limit()) {
                Ok(held) => held == bytes,
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    false
                }
                Err(error) => return Err(error),
            }
        } else {
            false
        };
        let result = (|| {
            if !held {
                let record = disk_reserve(
                    &self.disk,
                    &self.root,
                    DiskKind::Content,
                    BudgetLane::Completion,
                    key.file_bytes(bytes.len())?
                        .checked_add(key.record_bytes())
                        .ok_or(ContentError::Capacity)?,
                )?;
                durable_directory(&directory)?;
                let key = self.keys.writable(&directory, kind.key_domain(), key)?;
                key.install(&key.path(&directory, name, sealing::Kind::Record)?, bytes)?;
                record.commit();
            }
            let clear = directory.join(format!("{name}.record"));
            if self.rest.clear_allowed() && clear.is_file() {
                let gone = fs::metadata(&clear)?.len();
                fs::remove_file(&clear)?;
                sync_directory(&directory)?;
                self.conversion_record(kind, gone)?;
            }
            Ok(())
        })();
        self.mark_failure(&result);
        result
    }
    /// Reading a recorded manifest never creates a trusted custody witness.
    /// A new process must repeat the snapshot/content verification protocol.
    pub fn read_custody_record(
        &self,
        kind: CustodyRecordKind,
        hash: ContentHash,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        let most = max_bytes.min(kind.limit());
        let mut missed = None;
        for (path, sealed) in self.record_places(kind, hash)? {
            match self.read_record_at(kind, &path, sealed, most) {
                Ok(bytes) if ContentHash(*blake3::hash(&bytes).as_bytes()) == hash => {
                    return Ok(bytes);
                }
                Ok(_) => missed = Some(ContentError::Corrupt),
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => missed = Some(error),
            }
        }
        Err(missed.unwrap_or_else(|| {
            ContentError::Io(std::io::Error::from(std::io::ErrorKind::NotFound))
        }))
    }

    pub fn begin(
        &mut self,
        id: UploadId,
        domain: ContentDomainId,
        class: ContentClass,
        length: u64,
        expected_digest: Option<ContentHash>,
    ) -> Result<u64, ContentError> {
        self.check()?;
        let result = self.begin_inner(id, domain, class, length, expected_digest);
        self.mark_failure(&result);
        result
    }
    fn begin_inner(
        &mut self,
        id: UploadId,
        domain: ContentDomainId,
        class: ContentClass,
        length: u64,
        expected_digest: Option<ContentHash>,
    ) -> Result<u64, ContentError> {
        let meta = UploadMeta {
            schema: UPLOAD_SCHEMA,
            id,
            domain,
            class,
            expected_length: length,
            expected_digest,
        };
        if let Some(existing) = self.uploads.get(&id) {
            return if existing.meta == meta {
                Ok(existing.offset)
            } else {
                Err(ContentError::Invalid)
            };
        }
        if self.terminal_at(id)?.is_some() {
            return Err(ContentError::FinishedUpload);
        }
        self.reserve_upload(domain, length)?;
        let staged_bytes = self
            .staged_bytes
            .checked_add(length)
            .ok_or(ContentError::Capacity)?;
        let domain_bytes = self
            .staged_by_domain
            .get(&domain)
            .copied()
            .unwrap_or(0)
            .checked_add(length)
            .ok_or(ContentError::Capacity)?;
        // In the store's form: sealed, a record file whose header is promised with the bytes, each
        // append promising its records' overhead as it is written.
        let key = self.staging_key(self.rest.seals())?;
        let header = if self.rest.seals() {
            u64::try_from(record_file::PART_HEADER).map_err(|_| ContentError::Capacity)?
        } else {
            0
        };
        let staging = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Staging,
            BudgetLane::Ordinary,
            length.checked_add(header).ok_or(ContentError::Capacity)?,
        )?;
        // An orphan with no durable metadata is never an accepted upload; remove it, in either
        // form.
        for (orphan, _) in self.staging_places(id, "part")? {
            if orphan.exists() {
                fs::remove_file(&orphan)?;
            }
        }
        let part_path = self.staging_path(&key, id, "part")?;
        let meta_path = self.staging_path(&key, id, "meta")?;
        let part = self.create_part(&key, id, &part_path, length)?;
        let bytes = postcard::to_stdvec(&meta)?;
        if self.rest.seals() {
            key.install(&meta_path, &bytes)?;
        } else {
            atomic_install(&meta_path, &bytes)?;
        }
        self.staged_bytes = staged_bytes;
        self.staged_by_domain.insert(domain, domain_bytes);
        self.uploads.insert(
            id,
            Upload {
                meta,
                part,
                part_path,
                meta_path,
                offset: 0,
                _staging: Some(staging),
            },
        );
        Ok(0)
    }

    /// Each acknowledged offset is flushed. A retry of already present identical bytes
    /// succeeds; different bytes at the same offset cannot silently replace evidence.
    pub fn append(&mut self, id: UploadId, offset: u64, bytes: &[u8]) -> Result<u64, ContentError> {
        self.check()?;
        let result = self.append_inner(id, offset, bytes);
        self.mark_failure(&result);
        result
    }
    fn append_inner(
        &mut self,
        id: UploadId,
        offset: u64,
        bytes: &[u8],
    ) -> Result<u64, ContentError> {
        if bytes.len() > self.limits.chunk_bytes {
            return Err(ContentError::Capacity);
        }
        let upload = self
            .uploads
            .get_mut(&id)
            .ok_or(ContentError::MissingUpload)?;
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(ContentError::Capacity)?;
        if end > upload.meta.expected_length {
            return Err(ContentError::Capacity);
        }
        if offset < upload.offset && end <= upload.offset {
            let mut existing = zeroed_buffer(bytes.len())?;
            upload
                .part
                .read_at(&upload.part_path, offset, &mut existing)?;
            return if existing == bytes {
                Ok(upload.offset)
            } else {
                Err(ContentError::Corrupt)
            };
        }
        if offset != upload.offset {
            return Err(ContentError::Offset(upload.offset));
        }
        let grew = match &mut upload.part {
            staging::Part::Clear(file) => {
                file.seek(SeekFrom::Start(offset))?;
                if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
                    // A partial/error write has an unknown outcome. Reload its actual prefix on
                    // next retry; the caller must use the returned/queried durable offset.
                    file.set_len(offset)?;
                    file.sync_all()?;
                    return Err(ContentError::Io(error));
                }
                true
            }
            staging::Part::Sealed(writer) => {
                // The records' overhead promised before they are written. A write that fails
                // fails the store: opened again, the part is read through, and a part whose last
                // append was cut short is written again under a new key before anything is
                // appended (`RecordWriter::resume`).
                let overhead = record_file::records_bytes(bytes.len())?
                    .checked_sub(bytes.len() as u64)
                    .ok_or(ContentError::Capacity)?;
                let promise = disk_reserve(
                    &self.disk,
                    &self.root,
                    DiskKind::Staging,
                    BudgetLane::Ordinary,
                    overhead,
                )?;
                writer.append(bytes)?;
                writer.sync()?;
                promise.commit();
                false
            }
        };
        upload.offset = end;
        if grew {
            // A part in the clear grew while the store converts: its count follows it.
            self.conversion_staging(convert::StagingChange::Grew(bytes.len() as u64))?;
        }
        Ok(end)
    }

    pub fn offset(&self, id: UploadId) -> Result<u64, ContentError> {
        self.check()?;
        self.uploads
            .get(&id)
            .map(|u| u.offset)
            .ok_or(ContentError::MissingUpload)
    }

    /// Seal a complete upload at once: [`Self::advance_seal`] to its end,
    /// for a caller that holds the store alone (a tool, a test). A node's
    /// content owner seals a chunk a call, serving other work between.
    pub fn seal(&mut self, id: UploadId) -> Result<ContentRef, ContentError> {
        let mut sealing = self.begin_seal(id)?;
        let block = sealing.length.min(self.limits.chunk_bytes as u64);
        let mut scratch =
            zeroed_buffer(usize::try_from(block).map_err(|_| ContentError::Capacity)?)?;
        // A slice a chunk, and the manifest's.
        let slices = sealing
            .length
            .div_ceil(block.max(1))
            .checked_add(1)
            .ok_or(ContentError::Capacity)?;
        for _ in 0..slices {
            if let Some(reference) = self.advance_seal(&mut sealing, &mut scratch)? {
                return Ok(reference);
            }
        }
        Err(ContentError::Failed)
    }
    /// Begin a complete upload's seal in slices ([`UploadSealing`]): the
    /// staged bytes synced, the volume promised the sealed copy, the
    /// manifest's chunk list reserved whole from the upload's length.
    pub fn begin_seal(&mut self, id: UploadId) -> Result<UploadSealing, ContentError> {
        self.check()?;
        let result = self.begin_seal_inner(id);
        self.mark_failure(&result);
        result
    }
    /// The most the seal of upload `id` holds ([`UploadSealing::resident_for`]
    /// of its chunks): what its holder charges before it begins it.
    pub fn seal_charge(&self, id: UploadId) -> Result<usize, ContentError> {
        self.check()?;
        UploadSealing::resident_for(self.seal_chunks(id)?)
    }
    /// The chunks a seal of upload `id` installs, known from the object's
    /// size and the chunk length; the manifest is bounded at forty bytes a
    /// chunk (as each slice checks): a seal of more chunks than it can name
    /// fails at its last anyway, so it is refused before anything is synced,
    /// promised or read.
    fn seal_chunks(&self, id: UploadId) -> Result<usize, ContentError> {
        let chunk_bytes = self.limits.chunk_bytes as u64;
        let upload = self.uploads.get(&id).ok_or(ContentError::MissingUpload)?;
        if upload.offset != upload.meta.expected_length {
            return Err(ContentError::Incomplete {
                received: upload.offset,
                expected: upload.meta.expected_length,
            });
        }
        let count = usize::try_from(
            upload
                .offset
                .div_ceil(upload.offset.min(chunk_bytes).max(1)),
        )
        .map_err(|_| ContentError::Capacity)?;
        if count.checked_mul(40).ok_or(ContentError::Capacity)? > self.limits.max_manifest_bytes {
            return Err(ContentError::Capacity);
        }
        Ok(count)
    }
    fn begin_seal_inner(&mut self, id: UploadId) -> Result<UploadSealing, ContentError> {
        // The list is reserved once instead of growing a chunk at a time.
        let count = self.seal_chunks(id)?;
        let upload = self
            .uploads
            .get_mut(&id)
            .ok_or(ContentError::MissingUpload)?;
        upload.part.sync()?;
        let directory = domain_directory(&self.root, upload.meta.domain);
        // The sealed object is a second copy of the staged bytes until the
        // part is removed at finish; promise it before the first chunk lands:
        // each chunk's file as the writer installs it, and the domain's key
        // record where the writer seals and the domain has none yet. The
        // manifest is promised its file's bytes as it is installed, its
        // length known then.
        let key = self.keys.domain(&directory, upload.meta.domain, false)?;
        let objects = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            chunk_files_bound(&key, upload.offset, self.limits.chunk_bytes)?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        )?;
        durable_directory(&directory)?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| ContentError::Capacity)?;
        Ok(UploadSealing {
            id,
            length: upload.offset,
            done: 0,
            whole: blake3::Hasher::new(),
            chunks,
            objects: Some(objects),
        })
    }
    /// One slice of an upload's seal (the audit's F51): the next chunk of the
    /// staged bytes read into `scratch`, hashed into the stream and
    /// installed under its name; past the last byte, the manifest installed
    /// and its reference answered. The upload must still be staged, complete
    /// and of the length the seal began with.
    pub fn advance_seal(
        &mut self,
        sealing: &mut UploadSealing,
        scratch: &mut [u8],
    ) -> Result<Option<ContentRef>, ContentError> {
        self.check()?;
        let result = self.advance_seal_inner(sealing, scratch);
        self.mark_failure(&result);
        result
    }
    fn advance_seal_inner(
        &mut self,
        sealing: &mut UploadSealing,
        scratch: &mut [u8],
    ) -> Result<Option<ContentRef>, ContentError> {
        let chunk_bytes = self.limits.chunk_bytes as u64;
        let max_manifest_bytes = self.limits.max_manifest_bytes;
        let upload = self
            .uploads
            .get_mut(&sealing.id)
            .ok_or(ContentError::MissingUpload)?;
        if upload.offset != sealing.length || upload.meta.expected_length != sealing.length {
            return Err(ContentError::Invalid);
        }
        let domain = upload.meta.domain;
        let directory = domain_directory(&self.root, domain);
        let key = self.keys.domain(&directory, domain, true)?;
        let remaining = sealing
            .length
            .checked_sub(sealing.done)
            .ok_or(ContentError::Corrupt)?;
        if remaining > 0 {
            let count =
                usize::try_from(remaining.min(chunk_bytes)).map_err(|_| ContentError::Capacity)?;
            let block = scratch.get_mut(..count).ok_or(ContentError::Capacity)?;
            upload
                .part
                .read_at(&upload.part_path, sealing.done, block)?;
            sealing.whole.update(block);
            let hash = ContentHash(*blake3::hash(block).as_bytes());
            install_chunk(&key, &directory, block, hash)?;
            // The list was reserved whole: a chunk past it is no seal of
            // this length.
            if sealing.chunks.len() >= sealing.chunks.capacity() {
                return Err(ContentError::Corrupt);
            }
            sealing.chunks.push(Chunk {
                hash,
                length: u32::try_from(count).map_err(|_| ContentError::Capacity)?,
            });
            // Bound intermediate manifest growth before serializing it.
            if sealing
                .chunks
                .len()
                .checked_mul(40)
                .ok_or(ContentError::Capacity)?
                > max_manifest_bytes
            {
                return Err(ContentError::Capacity);
            }
            sealing.done = sealing
                .done
                .checked_add(count as u64)
                .ok_or(ContentError::Corrupt)?;
            return Ok(None);
        }
        let objects = sealing.objects.take().ok_or(ContentError::Invalid)?;
        let stream_digest = ContentHash(*sealing.whole.finalize().as_bytes());
        if upload
            .meta
            .expected_digest
            .is_some_and(|h| h != stream_digest)
        {
            return Err(ContentError::Corrupt);
        }
        let manifest = Manifest {
            schema: 1,
            domain,
            class: upload.meta.class,
            length: sealing.length,
            stream_digest,
            chunks: std::mem::take(&mut sealing.chunks),
        };
        let mut bytes = MANIFEST_MAGIC.to_vec();
        bytes.extend(postcard::to_stdvec(&manifest)?);
        if bytes.len() > max_manifest_bytes {
            return Err(ContentError::Capacity);
        }
        let root = ContentHash(*blake3::hash(&bytes).as_bytes());
        let manifest_file = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            key.file_bytes(bytes.len())?,
        )?;
        key.install(
            &key.path(&directory, root, sealing::Kind::Manifest)?,
            &bytes,
        )?;
        manifest_file.commit();
        objects.commit();
        // Upload metadata stays for an idempotent seal after a lost
        // response or a restart, until an explicit finish acknowledges that
        // the caller retains the reference.
        Ok(Some(ContentRef {
            domain,
            root,
            length: manifest.length,
            class: manifest.class,
        }))
    }

    pub fn finish(&mut self, id: UploadId) -> Result<(), ContentError> {
        self.check()?;
        let result = self.finish_inner(id);
        self.mark_failure(&result);
        result
    }
    fn finish_inner(&mut self, id: UploadId) -> Result<(), ContentError> {
        let upload = self
            .uploads
            .get(&id)
            .map(|upload| (upload.meta.domain, upload.meta.expected_length));
        let length = upload.map(|(_, length)| length);
        // The terminal record replaces the metadata where it lies, in its form; an upload not
        // staged gets one in the store's form.
        let (meta_path, part_path, sealed) = match self.uploads.get(&id) {
            Some(live) => (
                live.meta_path.clone(),
                live.part_path.clone(),
                matches!(live.part, staging::Part::Sealed(_)),
            ),
            None => {
                if let Some((path, _)) = self.terminal_at(id)? {
                    let part = path.with_extension("part");
                    return terminal::remove_part(&part);
                }
                self.reserve_terminal_slot()?;
                let key = self.staging_key(self.rest.seals())?;
                (
                    self.staging_path(&key, id, "meta")?,
                    self.staging_path(&key, id, "part")?,
                    self.rest.seals(),
                )
            }
        };
        let staged_bytes = self
            .staged_bytes
            .checked_sub(length.unwrap_or(0))
            .ok_or(ContentError::Corrupt)?;
        let terminal_uploads = self
            .terminal_uploads
            .checked_add(1)
            .filter(|count| *count <= MAX_TERMINAL_UPLOADS)
            .ok_or(ContentError::Capacity)?;
        // A live upload in the clear, while the store converts: its metadata and part leave the
        // count as they go, its terminal record joins it.
        let clear_files = match (sealed, upload.is_some()) {
            (false, true) => Some((
                fs::metadata(&meta_path)?.len(),
                fs::metadata(&part_path)?.len(),
            )),
            _ => None,
        };
        // Publish the durable replay fence before releasing bytes or the live
        // handle. A delayed Begin can never reconstruct this scoped identity.
        self.install_terminal(&meta_path, sealed, id)?;
        if let Some((meta_bytes, part_bytes)) = clear_files {
            self.conversion_staging(convert::StagingChange::Gone(meta_bytes))?;
            self.conversion_staging(convert::StagingChange::Gone(part_bytes))?;
            self.conversion_staging(convert::StagingChange::Appeared(
                terminal::RECORD_BYTES as u64,
            ))?;
        }
        self.terminal_uploads = terminal_uploads;
        self.uploads.remove(&id);
        self.staged_bytes = staged_bytes;
        if let Some((domain, length)) = upload {
            let remaining = self
                .staged_by_domain
                .get(&domain)
                .copied()
                .unwrap_or(0)
                .saturating_sub(length);
            if remaining == 0 {
                self.staged_by_domain.remove(&domain);
            } else {
                self.staged_by_domain.insert(domain, remaining);
            }
        }
        terminal::remove_part(&part_path)
    }

    /// Delivers only checksum-verified chunks, so a corrupt later chunk cannot cause
    /// earlier unverified bytes to escape. The complete manifest is authenticated first.
    pub fn read_verified(
        &self,
        reference: &ContentRef,
        sink: impl Write,
    ) -> Result<(), ContentError> {
        self.check()?;
        read_verified_at(&self.root, &self.keys, reference, sink)
    }

    pub fn read_bytes(
        &self,
        reference: &ContentRef,
        budget: usize,
    ) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        read_bytes_at(&self.root, &self.keys, reference, budget)
    }

    pub fn verify(&self, reference: &ContentRef) -> Result<(), ContentError> {
        self.read_verified(reference, std::io::sink())
    }
    /// Begin `verify` in slices ([`ObjectVerification`]): the object's
    /// manifest read and validated once, nothing else read yet.
    pub fn begin_verify(&self, reference: &ContentRef) -> Result<ObjectVerification, ContentError> {
        self.begin_verify_within(reference, MAX_TRANSFER_MANIFEST_BYTES)
    }
    /// [`Self::begin_verify`] of an object whose manifest is at most
    /// `encoded` bytes (as [`Self::manifest_length`] found it, and its holder
    /// charged): a longer one is refused before it is read.
    pub fn begin_verify_within(
        &self,
        reference: &ContentRef,
        encoded: usize,
    ) -> Result<ObjectVerification, ContentError> {
        self.check()?;
        let manifest = manifest_within(&self.root, &self.keys, reference, encoded)?;
        Ok(ObjectVerification {
            reference: reference.clone(),
            chunks: manifest.chunks,
            stream_digest: manifest.stream_digest,
            next: 0,
            whole: blake3::Hasher::new(),
        })
    }
    /// One slice of a verification: the next chunk read into `scratch` and
    /// checked against its name, or, past the last, the stream against the
    /// manifest's digest. Whether the object is verified whole.
    pub fn advance_verify(
        &self,
        verification: &mut ObjectVerification,
        scratch: &mut [u8],
    ) -> Result<bool, ContentError> {
        self.check()?;
        let domain = verification.reference.domain;
        let directory = domain_directory(&self.root, domain);
        if let Some(chunk) = verification.chunks.get(verification.next) {
            let key = self.keys.domain(&directory, domain, false)?;
            let block =
                verify_chunk_into(&directory, &key, chunk.hash, chunk.length as usize, scratch)?;
            verification.whole.update(block);
            verification.next = verification
                .next
                .checked_add(1)
                .ok_or(ContentError::Capacity)?;
            return Ok(false);
        }
        if ContentHash(*verification.whole.finalize().as_bytes()) != verification.stream_digest {
            return Err(ContentError::Corrupt);
        }
        Ok(true)
    }

    /// Authenticates the manifest and every selected chunk before returning a
    /// bounded byte range up to MAX_TRANSFER_CHUNK_BYTES, independent of local
    /// upload preferences. Unselected chunk bodies are not read or claimed verified.
    pub fn read_range(
        &self,
        reference: &ContentRef,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ContentError> {
        if max_bytes == 0 || max_bytes > MAX_TRANSFER_CHUNK_BYTES || offset > reference.length {
            return Err(ContentError::Capacity);
        }
        self.check()?;
        let mut plans = self.plans.lock().map_err(|_| ContentError::Failed)?;
        let plan = plans.plan(&self.root, &self.keys, reference)?;
        let end = offset
            .checked_add(max_bytes as u64)
            .ok_or(ContentError::Capacity)?
            .min(reference.length);
        let length = end.checked_sub(offset).ok_or(ContentError::Corrupt)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(usize::try_from(length).map_err(|_| ContentError::Capacity)?)
            .map_err(|_| ContentError::Capacity)?;
        let directory = domain_directory(&self.root, reference.domain);
        let key = self.keys.domain(&directory, reference.domain, false)?;
        let first = plan.first_chunk(offset);
        let mut start = plan.starts.get(first).copied().unwrap_or(reference.length);
        for chunk in plan.chunks.iter().skip(first) {
            let next = start
                .checked_add(u64::from(chunk.length))
                .ok_or(ContentError::Corrupt)?;
            if next > offset && start < end {
                let mut bytes = zeroed_buffer(chunk.length as usize)?;
                verify_chunk_into(
                    &directory,
                    &key,
                    chunk.hash,
                    chunk.length as usize,
                    &mut bytes,
                )?;
                let from = usize::try_from(
                    offset
                        .max(start)
                        .checked_sub(start)
                        .ok_or(ContentError::Corrupt)?,
                )
                .map_err(|_| ContentError::Capacity)?;
                let through = usize::try_from(
                    end.min(next)
                        .checked_sub(start)
                        .ok_or(ContentError::Corrupt)?,
                )
                .map_err(|_| ContentError::Capacity)?;
                output.extend_from_slice(bytes.get(from..through).ok_or(ContentError::Corrupt)?);
            }
            start = next;
            if start >= end {
                break;
            }
        }
        if output.len() as u64 != length {
            return Err(ContentError::Corrupt);
        }
        Ok(output)
    }

    fn check(&self) -> Result<(), ContentError> {
        if self.failed {
            Err(ContentError::Failed)
        } else {
            Ok(())
        }
    }
    fn mark_failure<T>(&mut self, result: &Result<T, ContentError>) {
        if matches!(result, Err(ContentError::Io(_))) {
            self.failed = true;
        }
    }

    /// Staged bytes of one content domain.
    pub fn staged_for(&self, domain: ContentDomainId) -> u64 {
        self.staged_by_domain.get(&domain).copied().unwrap_or(0)
    }
    pub fn staged(&self) -> (usize, u64) {
        (self.uploads.len(), self.staged_bytes)
    }

    /// The file the chunk of `domain` filed under `hash` lies in, its domain's keyed name: what a
    /// test that changes a stored chunk writes to.
    #[cfg(test)]
    pub(crate) fn chunk_file(&self, domain: ContentDomainId, hash: ContentHash) -> PathBuf {
        let directory = domain_directory(&self.root, domain);
        #[allow(clippy::unwrap_used)]
        self.keys
            .domain(&directory, domain, false)
            .and_then(|key| key.path(&directory, hash, sealing::Kind::Chunk))
            .unwrap()
    }
    /// The file the manifest of `domain` whose root is `root` lies in.
    #[cfg(test)]
    pub(crate) fn manifest_file(&self, domain: ContentDomainId, root: ContentHash) -> PathBuf {
        let directory = domain_directory(&self.root, domain);
        #[allow(clippy::unwrap_used)]
        self.keys
            .domain(&directory, domain, false)
            .and_then(|key| key.path(&directory, root, sealing::Kind::Manifest))
            .unwrap()
    }
    /// The manifest read and validated afresh: what the tests compare a
    /// held plan against.
    #[cfg(test)]
    fn manifest(&self, reference: &ContentRef) -> Result<Manifest, ContentError> {
        self.check()?;
        manifest_at(&self.root, &self.keys, reference)
    }
    /// The store directory; a lock-free [`ContentReader`] over the same path
    /// serves replay and recovery reads while this writer owns installation.
    pub fn root(&self) -> &Path {
        &self.root
    }
    /// The data directory's keys the store seals under, which the stores a session keeps beside
    /// it (its seeds) take theirs from.
    pub fn data_keys(&self) -> &focal_seal::DataKeys {
        &self.data_keys
    }
    /// The form the store installs its files in: sealed once its writer seals (29 §11).
    pub fn form(&self) -> ContentForm {
        if self.keys.seals() {
            ContentForm::Sealed
        } else {
            ContentForm::Clear
        }
    }
    /// A lock-free reader of this store, opening what it installed under the same data
    /// directory's keys.
    pub fn reader(&self) -> Result<ContentReader, ContentError> {
        Ok(ContentReader {
            root: self.root.clone(),
            keys: self.data_keys.clone(),
            clear: self.keys.clear_allowed(),
            rest_clear: self.rest.clear_allowed(),
        })
    }

    fn reserve_upload(&self, domain: ContentDomainId, length: u64) -> Result<(), ContentError> {
        self.reserve_terminal_slot()?;
        if length > self.limits.max_content_bytes
            || self.uploads.len() >= self.limits.max_uploads
            || self
                .staged_bytes
                .checked_add(length)
                .ok_or(ContentError::Capacity)?
                > self.limits.max_staging_bytes
            || self
                .staged_by_domain
                .get(&domain)
                .copied()
                .unwrap_or(0)
                .checked_add(length)
                .ok_or(ContentError::Capacity)?
                > self.limits.domain_staging_bytes()
        {
            return Err(ContentError::Capacity);
        }
        Ok(())
    }
    /// The volume envelope this store draws from.
    pub fn disk_budget(&self) -> DiskBudget {
        self.disk.clone()
    }
    pub fn disk_stats(&self) -> DiskStats {
        self.disk.stats()
    }
    fn reserve_terminal_slot(&self) -> Result<(), ContentError> {
        if self
            .terminal_uploads
            .checked_add(self.uploads.len())
            .is_none_or(|count| count >= MAX_TERMINAL_UPLOADS)
        {
            return Err(ContentError::Capacity);
        }
        Ok(())
    }
}

fn zeroed_buffer(length: usize) -> Result<Vec<u8>, ContentError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| ContentError::Capacity)?;
    bytes.resize(length, 0);
    Ok(bytes)
}

fn manifest_at(
    root: &Path,
    keys: &sealing::ContentKeys<'_>,
    reference: &ContentRef,
) -> Result<Manifest, ContentError> {
    manifest_within(root, keys, reference, MAX_TRANSFER_MANIFEST_BYTES)
}
/// The directory `domain`'s objects are installed in under `root`.
pub(crate) fn domain_directory(root: &Path, domain: ContentDomainId) -> PathBuf {
    root.join("objects").join(hex(&domain.0))
}
/// The length of an installed object's manifest, from its file's stated stream, nothing opened
/// past a sealed file's header: what a holder charges reading it at.
fn manifest_length_at(
    root: &Path,
    keys: &sealing::ContentKeys<'_>,
    reference: &ContentRef,
) -> Result<usize, ContentError> {
    let directory = domain_directory(root, reference.domain);
    let key = keys.domain(&directory, reference.domain, false)?;
    let length =
        usize::try_from(key.plain_length(&directory, reference.root, sealing::Kind::Manifest)?)
            .map_err(|_| ContentError::Capacity)?;
    if length > MAX_TRANSFER_MANIFEST_BYTES {
        return Err(ContentError::Capacity);
    }
    Ok(length)
}
/// The bytes the chunks of a `length`-byte object take as `key`'s writer installs them, cut at
/// `chunk` bytes.
pub(crate) fn chunk_files_bound(
    key: &sealing::DomainKey,
    length: u64,
    chunk: usize,
) -> Result<u64, ContentError> {
    let chunk64 = u64::try_from(chunk.max(1)).map_err(|_| ContentError::Capacity)?;
    let full = length.checked_div(chunk64).ok_or(ContentError::Capacity)?;
    let rest = usize::try_from(length.checked_rem(chunk64).ok_or(ContentError::Capacity)?)
        .map_err(|_| ContentError::Capacity)?;
    let whole = full
        .checked_mul(key.file_bytes(chunk)?)
        .ok_or(ContentError::Capacity)?;
    let last = if rest == 0 { 0 } else { key.file_bytes(rest)? };
    whole.checked_add(last).ok_or(ContentError::Capacity)
}
/// Whether the store at `root` holds no object file: its objects' directory and its quarantine are
/// empty, so a store asked to seal has nothing to convert.
/// Whether the store holds nothing its conversion converts (29 §11, §12.7): where the `objects`
/// convert, no object or quarantine round; where the files beside them (`rest`) do, no custody
/// record, part or staging record in the clear. Sealed at once then.
fn holds_nothing_to_convert(root: &Path, objects: bool, rest: bool) -> Result<bool, ContentError> {
    if objects && !holds_no_object(root)? {
        return Ok(false);
    }
    if !rest {
        return Ok(true);
    }
    let mut directories = Vec::new();
    for kind in CustodyRecordKind::ALL {
        directories.push(root.join(kind.directory()));
    }
    for entry in fs::read_dir(root.join("objects"))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            directories
                .try_reserve_exact(1)
                .map_err(|_| ContentError::Capacity)?;
            directories.push(entry.path());
        }
    }
    let wanted = convert::Converts {
        objects: false,
        rest: true,
    };
    for directory in directories {
        for entry in fs::read_dir(directory)? {
            if convert::counted_file(&entry?.path(), wanted)?.is_some() {
                return Ok(false);
            }
        }
    }
    for entry in fs::read_dir(root.join("staging"))? {
        if staging::counted_staging(&entry?.path())?.files > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}
fn holds_no_object(root: &Path) -> Result<bool, ContentError> {
    if fs::read_dir(root.join("objects"))?.next().is_some() {
        return Ok(false);
    }
    match fs::read_dir(root.join("quarantine")) {
        Ok(mut rounds) => Ok(rounds.next().is_none()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
    }
}
/// The manifest `root` of `domain` opened to its plaintext, at most `limit` bytes, and checked
/// against its root: the encoded bytes a transfer sends (inside TLS) and a description decodes.
pub(crate) fn manifest_plain(
    store: &Path,
    keys: &sealing::ContentKeys<'_>,
    domain: ContentDomainId,
    root: ContentHash,
    limit: usize,
) -> Result<Vec<u8>, ContentError> {
    let directory = domain_directory(store, domain);
    keys.domain(&directory, domain, false)?.read_sized(
        &directory,
        root,
        sealing::Kind::Manifest,
        limit.min(MAX_TRANSFER_MANIFEST_BYTES),
    )
}
/// [`manifest_at`] of a manifest at most `limit` bytes, nothing more read.
fn manifest_within(
    root: &Path,
    keys: &sealing::ContentKeys<'_>,
    reference: &ContentRef,
    limit: usize,
) -> Result<Manifest, ContentError> {
    let directory = domain_directory(root, reference.domain);
    let key = keys.domain(&directory, reference.domain, false)?;
    manifest_with(&directory, &key, reference, limit)
}
/// [`manifest_within`] under the domain's key already opened, its file opened once.
fn manifest_with(
    directory: &Path,
    key: &sealing::DomainKey,
    reference: &ContentRef,
    limit: usize,
) -> Result<Manifest, ContentError> {
    let bytes = key.read_sized(
        directory,
        reference.root,
        sealing::Kind::Manifest,
        limit.min(MAX_TRANSFER_MANIFEST_BYTES),
    )?;
    if !bytes.starts_with(MANIFEST_MAGIC) {
        return Err(ContentError::Corrupt);
    }
    let m = decode_manifest(
        bytes
            .get(MANIFEST_MAGIC.len()..)
            .ok_or(ContentError::Corrupt)?,
    )?;
    let total = m
        .chunks
        .iter()
        .try_fold(0u64, |sum, c| sum.checked_add(u64::from(c.length)))
        .ok_or(ContentError::Corrupt)?;
    if m.schema != 1
        || m.length > MAX_TRANSFER_CONTENT_BYTES
        || m.domain != reference.domain
        || m.class != reference.class
        || m.length != reference.length
        || total != m.length
        || m.chunks
            .iter()
            .any(|c| c.length == 0 || c.length as usize > MAX_TRANSFER_CHUNK_BYTES)
    {
        return Err(ContentError::Corrupt);
    }
    Ok(m)
}
fn read_verified_at(
    root: &Path,
    keys: &sealing::ContentKeys<'_>,
    reference: &ContentRef,
    mut sink: impl Write,
) -> Result<(), ContentError> {
    // The domain's key opened once for the manifest and every chunk.
    let dir = domain_directory(root, reference.domain);
    let key = keys.domain(&dir, reference.domain, false)?;
    let manifest = manifest_with(&dir, &key, reference, MAX_TRANSFER_MANIFEST_BYTES)?;
    let mut whole = blake3::Hasher::new();
    // One scratch buffer sized to the largest chunk, reused for every chunk,
    // instead of allocating a fresh buffer (up to a full chunk) per chunk.
    let scratch_len = manifest
        .chunks
        .iter()
        .map(|chunk| chunk.length as usize)
        .max()
        .unwrap_or(0);
    let mut scratch = zeroed_buffer(scratch_len)?;
    for chunk in &manifest.chunks {
        let block = verify_chunk_into(&dir, &key, chunk.hash, chunk.length as usize, &mut scratch)?;
        whole.update(block);
        sink.write_all(block)?;
    }
    if ContentHash(*whole.finalize().as_bytes()) != manifest.stream_digest {
        return Err(ContentError::Corrupt);
    }
    Ok(())
}
/// The chunk of `length` bytes filed under `hash` in `directory`, opened in whichever form it lies
/// (`sealing::DomainKey::opened`) into the head of `scratch` and checked against its hash: the
/// verified bytes, borrowed from the scratch. A file of another plaintext length, one that does
/// not open, or bytes that hash otherwise are corrupt, and a scratch too small for the chunk is
/// refused before anything is read.
pub(crate) fn verify_chunk_into<'a>(
    directory: &Path,
    key: &sealing::DomainKey,
    hash: ContentHash,
    length: usize,
    scratch: &'a mut [u8],
) -> Result<&'a [u8], ContentError> {
    if length > MAX_TRANSFER_CHUNK_BYTES {
        return Err(ContentError::Capacity);
    }
    key.open_into(directory, hash, sealing::Kind::Chunk, length, scratch)
}
fn read_bytes_at(
    root: &Path,
    keys: &sealing::ContentKeys<'_>,
    reference: &ContentRef,
    budget: usize,
) -> Result<Vec<u8>, ContentError> {
    if reference.length > budget as u64 || reference.length > MAX_TRANSFER_CONTENT_BYTES {
        return Err(ContentError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(reference.length).map_err(|_| ContentError::Capacity)?)
        .map_err(|_| ContentError::Capacity)?;
    read_verified_at(root, keys, reference, &mut bytes)?;
    Ok(bytes)
}

/// Lock-free, read-only access to installed content. Installed trees are
/// immutable and reached by atomic rename, so a reader never observes a partial
/// object: content is either verifiable or absent. It never installs, stages or
/// witnesses custody; the exclusive [`ContentStore`] writer keeps those rights.
#[derive(Debug, Clone)]
pub struct ContentReader {
    root: PathBuf,
    /// The data directory's keys: the content key is unwrapped from them as a domain's record is
    /// opened, and dropped with it, so a reader holds no key between reads (29 §11).
    keys: focal_seal::DataKeys,
    /// Whether a file may lie in the clear: the store was not marked sealed as the reader was made,
    /// so each file is looked for in the clear first and then sealed (`sealing::DomainKey::opened`).
    clear: bool,
    /// The same of its files beside its objects (29 §12.7): a custody record.
    rest_clear: bool,
}
impl ContentReader {
    /// The length of an installed object's manifest, from its file's stated stream, nothing
    /// opened past a sealed file's header.
    pub fn manifest_length(&self, reference: &ContentRef) -> Result<usize, ContentError> {
        manifest_length_at(&self.root, &self.keys(), reference)
    }
    /// A reader of the store at `root` under the data directory's `keys`, in the form the store's
    /// marker says its files are in.
    pub fn open(root: impl AsRef<Path>, keys: focal_seal::DataKeys) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        if !root.join("objects").is_dir() || !root.join("custody").is_dir() {
            return Err(ContentError::Invalid);
        }
        let clear = !root.join(sealing::SEALED).try_exists()?;
        let rest_clear = !root.join(sealing::REST_SEALED).try_exists()?;
        Ok(Self {
            root,
            keys,
            clear,
            rest_clear,
        })
    }
    fn keys(&self) -> sealing::ContentKeys<'_> {
        sealing::ContentKeys::reading(&self.keys, self.clear)
    }
    /// The data directory's keys the reader opens files under, which the stores a session keeps
    /// beside the content store (its seeds) take theirs from.
    pub fn data_keys(&self) -> &focal_seal::DataKeys {
        &self.keys
    }
    /// Where the manifest of `domain` whose root is `root` lies, in whichever form, if anywhere:
    /// what a test that removes, damages or looks for a stored object's file names it by.
    #[cfg(any(test, feature = "test-support"))]
    pub fn manifest_file(
        &self,
        domain: ContentDomainId,
        root: ContentHash,
    ) -> Result<Option<PathBuf>, ContentError> {
        let directory = domain_directory(&self.root, domain);
        self.keys().domain(&directory, domain, false)?.existing(
            &directory,
            root,
            sealing::Kind::Manifest,
        )
    }
    /// The roots of every manifest `domain` holds, in whichever form each lies: what a test lists
    /// a store's objects by, their names keyed. A file gone since it was listed is passed over.
    #[cfg(any(test, feature = "test-support"))]
    pub fn manifest_roots(
        &self,
        domain: ContentDomainId,
    ) -> Result<Vec<ContentHash>, ContentError> {
        let directory = domain_directory(&self.root, domain);
        let key = self.keys().domain(&directory, domain, false)?;
        let mut roots = Vec::new();
        let listing = match fs::read_dir(&directory) {
            Ok(listing) => listing,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(roots),
            Err(error) => return Err(error.into()),
        };
        for entry in listing {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("manifest") {
                continue;
            }
            match self.plaintext_with(&key, &path) {
                Ok(Some((_, root))) => roots.push(root),
                Ok(None) => {}
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        roots.sort();
        Ok(roots)
    }
    /// The plaintext of the chunk or manifest of this store at `path`, in whichever form it lies,
    /// checked against the hash it is filed under: what a test compares a copy's bytes by, each
    /// node's sealed under its own keys.
    #[cfg(any(test, feature = "test-support"))]
    pub fn plaintext(&self, path: &Path) -> Result<Vec<u8>, ContentError> {
        let domain = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .and_then(gc::parse_domain)
            .ok_or(ContentError::Invalid)?;
        let key = self
            .keys()
            .domain(&domain_directory(&self.root, domain), domain, false)?;
        self.plaintext_with(&key, path)?
            .map(|(bytes, _)| bytes)
            .ok_or(ContentError::Invalid)
    }
    #[cfg(any(test, feature = "test-support"))]
    fn plaintext_with(
        &self,
        key: &sealing::DomainKey,
        path: &Path,
    ) -> Result<Option<(Vec<u8>, ContentHash)>, ContentError> {
        let most = MAX_TRANSFER_CHUNK_BYTES.max(MAX_TRANSFER_MANIFEST_BYTES);
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or(ContentError::Invalid)?;
        let Some(hash) = sealing::parse_hash(stem) else {
            return key.open_listed(path, most);
        };
        if !self.clear {
            return Ok(None);
        }
        let bytes = read_bounded(path, most)?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        Ok(Some((bytes, hash)))
    }
    /// Where the chunk of `domain` filed under `hash` lies, in whichever form, if anywhere.
    #[cfg(any(test, feature = "test-support"))]
    pub fn chunk_file(
        &self,
        domain: ContentDomainId,
        hash: ContentHash,
    ) -> Result<Option<PathBuf>, ContentError> {
        let directory = domain_directory(&self.root, domain);
        self.keys().domain(&directory, domain, false)?.existing(
            &directory,
            hash,
            sealing::Kind::Chunk,
        )
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn read_verified(
        &self,
        reference: &ContentRef,
        sink: impl Write,
    ) -> Result<(), ContentError> {
        read_verified_at(&self.root, &self.keys(), reference, sink)
    }
    pub fn read_bytes(
        &self,
        reference: &ContentRef,
        budget: usize,
    ) -> Result<Vec<u8>, ContentError> {
        read_bytes_at(&self.root, &self.keys(), reference, budget)
    }
    pub fn verify(&self, reference: &ContentRef) -> Result<(), ContentError> {
        read_verified_at(&self.root, &self.keys(), reference, std::io::sink())
    }
    /// Kind `kind`'s record filed under `hash`, checked against it: in the clear first while a record
    /// may lie in the clear, then sealed under its keyed name, opened under the kind's key (29 §12.7).
    pub fn read_custody_record(
        &self,
        kind: CustodyRecordKind,
        hash: ContentHash,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ContentError> {
        let most = max_bytes.min(kind.limit());
        let directory = self.root.join(kind.directory());
        let keys = self.keys();
        let mut missed = None;
        if self.rest_clear {
            match read_bounded(&directory.join(format!("{hash}.record")), most) {
                Ok(bytes) if ContentHash(*blake3::hash(&bytes).as_bytes()) == hash => {
                    return Ok(bytes);
                }
                Ok(_) => missed = Some(ContentError::Corrupt),
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => missed = Some(error),
            }
        }
        let key = keys.domain(&directory, kind.key_domain(), false)?;
        if key.seals() && key.record_bytes() == 0 {
            let path = key.path(&directory, hash, sealing::Kind::Record)?;
            if path.is_file() {
                let bytes = key.read_record(&path, most)?;
                if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
                    return Err(ContentError::Corrupt);
                }
                return Ok(bytes);
            }
        }
        Err(missed.unwrap_or_else(|| {
            ContentError::Io(std::io::Error::from(std::io::ErrorKind::NotFound))
        }))
    }
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, ContentError> {
    let mut file = File::open(path)?;
    let length = usize::try_from(file.metadata()?.len()).map_err(|_| ContentError::Capacity)?;
    if length > limit {
        return Err(ContentError::Capacity);
    }
    // Exact allocation avoids read_to_end's geometric spare capacity escaping
    // the host's declared chunk/manifest scratch reservation.
    let mut bytes = zeroed_buffer(length)?;
    file.read_exact(&mut bytes)?;
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(ContentError::Capacity);
    }
    Ok(bytes)
}

/// A file in the clear named by its content's `hash` (the stores 29 §12 has yet to seal: seeds, the
/// cold stream, custody records): a file already there holding these bytes is kept, its freshness
/// refreshed; one holding anything else is corrupt under a content-addressed name and refused.
fn install_verified_chunk(
    path: &Path,
    bytes: &[u8],
    hash: ContentHash,
) -> Result<(), ContentError> {
    if path.exists() {
        let previous = read_bounded(path, bytes.len())?;
        if previous != bytes || ContentHash(*blake3::hash(&previous).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        refresh_mtime(path)?;
        return Ok(());
    }
    atomic_install(path, bytes)
}
/// A chunk of an upload installed under its domain's name of `hash` in `directory`, in the form the
/// writer installs: a file already there, in either form, that opens to these bytes is kept, its
/// freshness refreshed; one that opens to anything else, or not at all, is corrupt under a
/// content-addressed name and refused.
fn install_chunk(
    key: &sealing::DomainKey,
    directory: &Path,
    bytes: &[u8],
    hash: ContentHash,
) -> Result<(), ContentError> {
    if key.exists(directory, hash, sealing::Kind::Chunk)? {
        let mut previous = zeroed_buffer(bytes.len())?;
        let (path, _) = key.opened(
            directory,
            hash,
            sealing::Kind::Chunk,
            bytes.len(),
            &mut previous,
        )?;
        refresh_mtime(&path)?;
        return Ok(());
    }
    key.install(&key.path(directory, hash, sealing::Kind::Chunk)?, bytes)
}
/// Whether a file filed under `hash` in `directory` already opens, in either form, to `length`
/// bytes hashing to `hash`; when one does its freshness is refreshed and nothing else is written.
/// Asked before a durable write is promised its bytes (the audit's F59), as [`already_installed`]
/// asks of a custody record: a chunk an exact retry or a resumed transfer sends again, or a
/// manifest a repeated completion installs, adds no byte to the volume.
pub(crate) fn already_held(
    key: &sealing::DomainKey,
    directory: &Path,
    hash: ContentHash,
    kind: sealing::Kind,
    length: usize,
) -> Result<bool, ContentError> {
    if !key.exists(directory, hash, kind)? {
        return Ok(false);
    }
    let mut previous = zeroed_buffer(length)?;
    match key.opened(directory, hash, kind, length, &mut previous) {
        Ok((path, _)) => {
            refresh_mtime(&path)?;
            Ok(true)
        }
        Err(ContentError::Corrupt) => Ok(false),
        Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
/// Mark a content-addressed file as freshly referenced. Dedup keeps an existing
/// file rather than rewriting it, so without this a chunk shared with a newly
/// sealed object would keep its old mtime; a collector pass that scanned the
/// object domain before the seal could then reclaim a live chunk (its grace
/// window already elapsed). Refreshing the mtime keeps a re-referenced chunk
/// inside its grace window. It is a hint, not a durability fence, so no fsync.
fn refresh_mtime(path: &Path) -> Result<(), ContentError> {
    OpenOptions::new()
        .write(true)
        .open(path)?
        .set_modified(SystemTime::now())?;
    Ok(())
}
/// Install a chunk a custody transfer received (24 §20). The bytes were
/// verified against the chunk's hash by the caller. A file already holding
/// them is left alone; one holding anything else is corrupt under this
/// content-addressed name and is replaced by the verified bytes, never
/// kept: a recopy is how a corrupt chunk is repaired.
/// Whether an identical file — equal bytes, and the hash when one is named
/// — is already at `path`; when it is, its freshness is refreshed and nothing
/// else is written. Asked before a durable write is promised its bytes (the
/// audit's F59): a chunk an exact retry or a resumed transfer sends again, a
/// manifest a repeated completion installs, a custody record installed
/// twice add no byte to the volume, so they neither need the promise nor
/// lower the estimate — and near the watermark they are not refused.
pub(crate) fn already_installed(
    path: &Path,
    bytes: &[u8],
    hash: Option<ContentHash>,
) -> Result<bool, ContentError> {
    if path.exists()
        && read_bounded(path, bytes.len()).is_ok_and(|previous| {
            previous == bytes
                && hash.is_none_or(|hash| ContentHash(*blake3::hash(&previous).as_bytes()) == hash)
        })
    {
        refresh_mtime(path)?;
        return Ok(true);
    }
    Ok(false)
}

fn atomic_install(path: &Path, bytes: &[u8]) -> Result<(), ContentError> {
    let temp = path.with_extension("install");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    focal_platform::fs::atomic_replace(&temp, path)?;
    sync_directory(path.parent().ok_or(ContentError::Invalid)?)?;
    Ok(())
}

fn durable_directory(path: &Path) -> std::io::Result<()> {
    focal_platform::fs::create_dir_all_durable(path, |dir| fs::create_dir(dir))
}
fn sync_directory(path: &Path) -> std::io::Result<()> {
    focal_platform::sync_dir(path)
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
#[path = "record_seal_tests.rs"]
mod record_seal_tests;

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> StoreLimits {
        StoreLimits {
            max_content_bytes: 64,
            max_staging_bytes: 128,
            max_uploads: 2,
            chunk_bytes: 4,
            max_manifest_bytes: 2048,
        }
    }
    #[test]
    fn uploads_are_promised_volume_bytes_before_any_part_exists() {
        let dir = tempfile::tempdir().unwrap();
        let guarded = DiskBudget::new(DiskBudgetConfig {
            headroom: u64::MAX / 2,
            completion_reserve: 0,
            sample_interval: 4,
        })
        .unwrap();
        let mut store = ContentStore::open_with_disk(
            dir.path(),
            limits(),
            guarded.clone(),
            &crate::store::test_keys(dir.path()),
            ContentForm::Sealed,
        )
        .unwrap();
        let id = UploadId([7; 16]);
        assert!(matches!(
            store.begin(
                id,
                ContentDomainId([1; 16]),
                ContentClass::Evidence,
                16,
                None
            ),
            Err(ContentError::Capacity)
        ));
        assert_eq!(store.staged(), (0, 0));
        // No part in either form: the refusal came before any was made.
        assert!(
            fs::read_dir(dir.path().join("staging"))
                .unwrap()
                .all(|entry| entry.unwrap().path().extension() != Some("part".as_ref()))
        );
        assert_eq!(guarded.stats().outstanding, 0);
        assert!(matches!(
            store.install_custody_record(CustodyRecordKind::Checkpoint, b"cp"),
            Err(ContentError::Capacity)
        ));
        drop(store);
        // An unguarded envelope promises the staging bytes to the upload and
        // the object copy to the seal, and charges the copy once installed.
        let open = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
        let mut store = ContentStore::open_with_disk(
            dir.path(),
            limits(),
            open.clone(),
            &crate::store::test_keys(dir.path()),
            ContentForm::Sealed,
        )
        .unwrap();
        store
            .begin(
                id,
                ContentDomainId([1; 16]),
                ContentClass::Evidence,
                8,
                None,
            )
            .unwrap();
        // The bytes and, the part sealed, its record file's header; each append promises its
        // records' overhead as it is written.
        let staged = 8 + record_file::PART_HEADER as u64;
        assert_eq!(open.stats().by_kind[DiskKind::Staging as usize], staged);
        store.append(id, 0, b"abcd").unwrap();
        store.append(id, 4, b"efgh").unwrap();
        store.seal(id).unwrap();
        assert_eq!(open.stats().by_kind[DiskKind::Content as usize], 0);
        assert_eq!(open.stats().outstanding, staged);
        store.finish(id).unwrap();
        assert_eq!(open.stats().outstanding, 0);
    }
    #[test]
    fn one_domain_cannot_fill_the_whole_staging_allowance() {
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        assert_eq!(limits().domain_staging_bytes(), 64);
        let noisy = ContentDomainId([1; 16]);
        let other = ContentDomainId([2; 16]);
        store
            .begin(UploadId([1; 16]), noisy, ContentClass::Evidence, 64, None)
            .unwrap();
        assert!(matches!(
            store.begin(UploadId([2; 16]), noisy, ContentClass::Evidence, 1, None),
            Err(ContentError::Capacity)
        ));
        store
            .begin(UploadId([3; 16]), other, ContentClass::Evidence, 64, None)
            .unwrap();
        assert_eq!(store.staged_for(noisy), 64);
        assert_eq!(store.staged_for(other), 64);
        assert_eq!(store.staged(), (2, 128));
        drop(store);
        // The per-domain accounting is rebuilt from the recovered uploads.
        let mut store =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        assert_eq!(store.staged_for(noisy), 64);
        for offset in (0..64).step_by(4) {
            store.append(UploadId([1; 16]), offset, &[9; 4]).unwrap();
        }
        store.seal(UploadId([1; 16])).unwrap();
        store.finish(UploadId([1; 16])).unwrap();
        assert_eq!(store.staged_for(noisy), 0);
        store
            .begin(UploadId([2; 16]), noisy, ContentClass::Evidence, 1, None)
            .unwrap();
    }
    #[test]
    fn custody_records_are_bounded_immutable_and_checksum_checked_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = b"exact checkpoint bytes";
        let hash;
        {
            let mut store =
                ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path()))
                    .unwrap();
            hash = store
                .install_custody_record(CustodyRecordKind::Checkpoint, checkpoint)
                .unwrap();
            assert_eq!(
                store
                    .install_custody_record(CustodyRecordKind::Checkpoint, checkpoint)
                    .unwrap(),
                hash
            );
            assert!(matches!(
                store.read_custody_record(CustodyRecordKind::Checkpoint, hash, 1),
                Err(ContentError::Capacity)
            ));
            assert!(matches!(
                store.install_custody_record(CustodyRecordKind::Manifest, &[0; 16 * 1024 + 1]),
                Err(ContentError::Capacity)
            ));
        }
        let mut reopened =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        assert_eq!(
            reopened
                .read_custody_record(CustodyRecordKind::Checkpoint, hash, 4096)
                .unwrap(),
            checkpoint
        );
        std::fs::write(
            reopened.record_path(CustodyRecordKind::Checkpoint, hash),
            b"bad checkpoint",
        )
        .unwrap();
        assert!(matches!(
            reopened.read_custody_record(CustodyRecordKind::Checkpoint, hash, 4096),
            Err(ContentError::Corrupt)
        ));
        assert!(matches!(
            reopened.install_custody_record(CustodyRecordKind::Checkpoint, checkpoint),
            Err(ContentError::Corrupt)
        ));
    }
    #[test]
    fn custody_record_io_failure_never_returns_success_and_requires_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        std::fs::remove_dir(dir.path().join("custody")).unwrap();
        assert!(matches!(
            store.install_custody_record(CustodyRecordKind::Manifest, b"verified prefix"),
            Err(ContentError::Io(_))
        ));
        assert!(matches!(
            store.install_custody_record(CustodyRecordKind::Manifest, b"verified prefix"),
            Err(ContentError::Failed)
        ));
        drop(store);
        let mut reopened =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        assert!(
            reopened
                .install_custody_record(CustodyRecordKind::Manifest, b"verified prefix")
                .is_ok()
        );
    }
    #[test]
    fn durable_resume_retry_and_seal_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let id = UploadId([1; 16]);
        let domain = ContentDomainId([2; 16]);
        let digest = ContentHash(*blake3::hash(b"abcdefghij").as_bytes());
        {
            let mut s =
                ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path()))
                    .unwrap();
            s.begin(id, domain, ContentClass::Evidence, 10, Some(digest))
                .unwrap();
            assert_eq!(s.append(id, 0, b"abcd").unwrap(), 4);
            assert_eq!(s.append(id, 0, b"abcd").unwrap(), 4);
            assert!(matches!(
                s.append(id, 0, b"BAD!"),
                Err(ContentError::Corrupt)
            ));
            assert!(matches!(s.seal(id), Err(ContentError::Incomplete { .. })));
        }
        let reference;
        {
            let mut s =
                ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path()))
                    .unwrap();
            assert_eq!(
                s.begin(id, domain, ContentClass::Evidence, 10, Some(digest))
                    .unwrap(),
                4
            );
            s.append(id, 4, b"efgh").unwrap();
            s.append(id, 8, b"ij").unwrap();
            reference = s.seal(id).unwrap();
            assert_eq!(s.seal(id).unwrap(), reference);
            assert_eq!(s.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
            s.finish(id).unwrap();
            assert_eq!(s.staged(), (0, 0));
        }
        let s =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        assert_eq!(s.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
        let mut wrong = reference.clone();
        wrong.domain = ContentDomainId([3; 16]);
        assert!(s.verify(&wrong).is_err());
    }

    /// A paged read loads and validates the object's manifest once and finds
    /// each page's first chunk by search (the audit's F19); the plans held
    /// are bounded by the uploads the store admits.
    #[test]
    fn paged_reads_load_the_plan_once_and_hold_a_bounded_number_of_plans() {
        let root = tempfile::tempdir().unwrap();
        let mut limits = limits();
        limits.chunk_bytes = 4;
        limits.max_uploads = 2;
        let mut store =
            ContentStore::open(root.path(), limits, &crate::store::test_keys(root.path())).unwrap();
        let mut references = Vec::new();
        for object in 0..3u8 {
            let id = UploadId([object; 16]);
            let bytes: Vec<u8> = (0..64u8)
                .map(|i| i.wrapping_mul(object.wrapping_add(1)))
                .collect();
            store
                .begin(
                    id,
                    ContentDomainId([2; 16]),
                    ContentClass::Evidence,
                    bytes.len() as u64,
                    None,
                )
                .unwrap();
            for (i, part) in bytes.chunks(4).enumerate() {
                store.append(id, (i * 4) as u64, part).unwrap();
            }
            let reference = store.seal(id).unwrap();
            store.finish(id).unwrap();
            references.push((reference, bytes));
        }
        let (reference, bytes) = &references[0];
        let mut paged = Vec::new();
        for offset in (0..bytes.len()).step_by(3) {
            paged.extend(store.read_range(reference, offset as u64, 3).unwrap());
        }
        assert_eq!(&paged, bytes, "the pages are the object");
        assert_eq!(
            store.plans.lock().unwrap().loads,
            1,
            "one manifest read for every page"
        );
        for (reference, bytes) in &references {
            assert_eq!(&store.read_range(reference, 5, 7).unwrap(), &bytes[5..12]);
        }
        assert_eq!(
            store.plans.lock().unwrap().plans.len(),
            2,
            "held plans are bounded"
        );
        assert_eq!(store.plans.lock().unwrap().loads, 3);
        // A reference with the right root and the wrong length is refused.
        let mut wrong = reference.clone();
        wrong.length = 63;
        assert!(matches!(
            store.read_range(&wrong, 0, 4),
            Err(ContentError::Corrupt)
        ));
    }

    #[test]
    fn bounded_ranges_verify_selected_chunks_across_boundaries() {
        let root = tempfile::tempdir().unwrap();
        let mut store =
            ContentStore::open(root.path(), limits(), &crate::store::test_keys(root.path()))
                .unwrap();
        let id = UploadId([1; 16]);
        let bytes = b"abcdefghij";
        store
            .begin(
                id,
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                bytes.len() as u64,
                None,
            )
            .unwrap();
        for (i, part) in bytes.chunks(4).enumerate() {
            store.append(id, (i * 4) as u64, part).unwrap();
        }
        let reference = store.seal(id).unwrap();
        for offset in 0..=bytes.len() {
            assert_eq!(
                store.read_range(&reference, offset as u64, 4).unwrap(),
                bytes[offset..(offset + 4).min(bytes.len())]
            );
        }
        assert!(matches!(
            store.read_range(&reference, 11, 4),
            Err(ContentError::Capacity)
        ));
        assert!(matches!(
            store.read_range(&reference, 0, MAX_TRANSFER_CHUNK_BYTES + 1),
            Err(ContentError::Capacity)
        ));
        let manifest = store.manifest(&reference).unwrap();
        let path = store.chunk_file(reference.domain, manifest.chunks[1].hash);
        fs::write(path, b"BAD!").unwrap();
        assert!(matches!(
            store.read_range(&reference, 3, 4),
            Err(ContentError::Corrupt)
        ));
        assert_eq!(store.read_range(&reference, 0, 3).unwrap(), b"abc");
    }

    #[test]
    fn mutation_io_error_stops_writer_until_recovery() {
        let root = tempfile::tempdir().unwrap();
        let mut store =
            ContentStore::open(root.path(), limits(), &crate::store::test_keys(root.path()))
                .unwrap();
        let old_upload = UploadId([9; 16]);
        store
            .begin(
                old_upload,
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                4,
                None,
            )
            .unwrap();
        store.append(old_upload, 0, b"safe").unwrap();
        let reference = store.seal(old_upload).unwrap();
        store.finish(old_upload).unwrap();
        let staging = root.path().join("staging");
        let retained_staging = root.path().join("retained-staging");
        fs::rename(&staging, &retained_staging).unwrap();
        fs::write(&staging, b"injected unavailable directory").unwrap();
        assert!(matches!(
            store.begin(
                UploadId([1; 16]),
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                0,
                None
            ),
            Err(ContentError::Io(_))
        ));
        fs::remove_file(&staging).unwrap();
        fs::rename(&retained_staging, &staging).unwrap();
        assert!(matches!(
            store.begin(
                UploadId([1; 16]),
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                0,
                None
            ),
            Err(ContentError::Failed)
        ));
        assert!(matches!(
            store.verify(&reference),
            Err(ContentError::Failed)
        ));
        assert!(matches!(
            store.read_bytes(&reference, 4),
            Err(ContentError::Failed)
        ));
        assert!(matches!(
            store.read_range(&reference, 0, 4),
            Err(ContentError::Failed)
        ));
        assert!(matches!(
            store.offset(old_upload),
            Err(ContentError::Failed)
        ));
        drop(store);
        let mut recovered =
            ContentStore::open(root.path(), limits(), &crate::store::test_keys(root.path()))
                .unwrap();
        assert_eq!(recovered.read_bytes(&reference, 4).unwrap(), b"safe");
        recovered
            .begin(
                UploadId([1; 16]),
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                0,
                None,
            )
            .unwrap();
    }
    #[test]
    fn unrepresentable_buffer_is_a_capacity_error() {
        assert!(matches!(
            zeroed_buffer(usize::MAX),
            Err(ContentError::Capacity)
        ));
    }
    #[test]
    fn corruption_and_budget_limits_do_not_produce_valid_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut s =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        let id = UploadId([1; 16]);
        s.begin(
            id,
            ContentDomainId([2; 16]),
            ContentClass::Evidence,
            4,
            None,
        )
        .unwrap();
        assert!(matches!(
            s.append(id, 0, b"12345"),
            Err(ContentError::Capacity)
        ));
        s.append(id, 0, b"1234").unwrap();
        let r = s.seal(id).unwrap();
        assert!(matches!(s.read_bytes(&r, 3), Err(ContentError::Capacity)));
        let hash = ContentHash(*blake3::hash(b"1234").as_bytes());
        fs::write(s.chunk_file(r.domain, hash), b"oops").unwrap();
        assert!(matches!(s.verify(&r), Err(ContentError::Corrupt)));
        assert!(matches!(
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())),
            Err(ContentError::Locked)
        ));
    }

    /// An upload sealed a chunk a call installs what a seal at once does —
    /// the same chunks and manifest, the same reference — with nothing of the
    /// manifest installed before the last chunk is read (the audit's F51);
    /// a seal whose chunks a manifest could not name is refused before a byte
    /// is read.
    #[test]
    fn a_seal_in_slices_installs_what_a_seal_at_once_does() {
        let at_once = tempfile::tempdir().unwrap();
        let sliced = tempfile::tempdir().unwrap();
        let bytes: Vec<u8> = (0..30u8).collect();
        let id = UploadId([7; 16]);
        let domain = ContentDomainId([1; 16]);
        let staged = |root: &Path, limits: StoreLimits| {
            let mut store =
                ContentStore::open(root, limits, &crate::store::test_keys(root)).unwrap();
            store
                .begin(id, domain, ContentClass::Evidence, 30, None)
                .unwrap();
            for (index, part) in bytes.chunks(4).enumerate() {
                store.append(id, (index * 4) as u64, part).unwrap();
            }
            store
        };
        let whole = staged(at_once.path(), limits()).seal(id).unwrap();
        let mut store = staged(sliced.path(), limits());
        let mut sealing = store.begin_seal(id).unwrap();
        let mut scratch = vec![0u8; 4];
        let mut slices = 0;
        let reference = loop {
            slices += 1;
            if let Some(reference) = store.advance_seal(&mut sealing, &mut scratch).unwrap() {
                break reference;
            }
            // Named by the domain's key, which the first slice's chunk made.
            assert!(
                !store.manifest_file(domain, whole.root).exists(),
                "{slices}"
            );
        };
        // Eight chunks — seven of four bytes and one of two — then the
        // manifest.
        assert_eq!(slices, 9);
        assert_eq!(reference, whole);
        store.verify(&reference).unwrap();
        let tight = tempfile::tempdir().unwrap();
        let mut store = staged(
            tight.path(),
            StoreLimits {
                max_manifest_bytes: 64,
                ..limits()
            },
        );
        assert!(matches!(store.begin_seal(id), Err(ContentError::Capacity)));
        assert_eq!(
            fs::read_dir(tight.path().join("objects")).unwrap().count(),
            0
        );
    }
    /// An installed object is verified a chunk a call, and a chunk that has
    /// turned corrupt on the volume is found where it lies (the audit's F51).
    #[test]
    fn a_verification_in_slices_reads_every_chunk_and_finds_a_corrupt_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            ContentStore::open(dir.path(), limits(), &crate::store::test_keys(dir.path())).unwrap();
        let id = UploadId([3; 16]);
        let domain = ContentDomainId([2; 16]);
        let bytes: Vec<u8> = (0..16u8).collect();
        store
            .begin(id, domain, ContentClass::Evidence, 16, None)
            .unwrap();
        for (index, part) in bytes.chunks(4).enumerate() {
            store.append(id, (index * 4) as u64, part).unwrap();
        }
        let reference = store.seal(id).unwrap();
        let mut scratch = vec![0u8; MAX_TRANSFER_CHUNK_BYTES];
        let mut verification = store.begin_verify(&reference).unwrap();
        let mut slices = 1;
        while !store
            .advance_verify(&mut verification, &mut scratch)
            .unwrap()
        {
            slices += 1;
        }
        assert_eq!(slices, 5, "four chunks, then the stream");
        assert_eq!(verification.verified(), 4);
        // The third chunk overwritten with other bytes of its length.
        let third = ContentHash(*blake3::hash(&bytes[8..12]).as_bytes());
        fs::write(store.chunk_file(domain, third), b"wxyz").unwrap();
        let mut verification = store.begin_verify(&reference).unwrap();
        let mut outcome = Ok(false);
        for _ in 0..5 {
            outcome = store.advance_verify(&mut verification, &mut scratch);
            if !matches!(outcome, Ok(false)) {
                break;
            }
        }
        assert!(matches!(outcome, Err(ContentError::Corrupt)));
        assert_eq!(verification.verified(), 2);
    }
}
