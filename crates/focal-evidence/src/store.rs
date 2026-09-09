use focal_memory::{
    BudgetLane, DiskBudget, DiskBudgetConfig, DiskKind, DiskReservation, DiskStats,
};
use focal_model::{ContentClass, ContentDomainId, ContentHash, ContentRef};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MANIFEST_MAGIC: &[u8] = b"focal.evidence.manifest\0\x01\0";
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
#[derive(Debug, Clone, Copy)]
pub enum CustodyRecordKind {
    Checkpoint,
    Manifest,
}
impl CustodyRecordKind {
    fn directory(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoints",
            Self::Manifest => "custody",
        }
    }
    fn limit(self) -> usize {
        match self {
            Self::Checkpoint => 8 * 1024 * 1024,
            Self::Manifest => 16 * 1024,
        }
    }
}

#[path = "transfer.rs"]
mod transfer;
pub use transfer::TransferManifest;
#[path = "native_artifact.rs"]
mod native_artifact;
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
    disk.refresh_with(|| fs2::available_space(root).ok());
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
    file: File,
    offset: u64,
    /// The staging bytes promised to this upload; returned when the part is
    /// removed. Absent only for an upload recovered on a volume that could
    /// not promise its remainder, which the watermark still guards.
    _staging: Option<DiskReservation>,
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
    _writer_lock: File,
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
    /// Open the store guarded by the standard disk watermark alone.
    pub fn open(root: impl AsRef<Path>, limits: StoreLimits) -> Result<Self, ContentError> {
        let disk =
            DiskBudget::new(DiskBudgetConfig::default()).map_err(|_| ContentError::Capacity)?;
        Self::open_with_disk(root, limits, disk)
    }
    /// Open the store on the disk envelope shared by every durable owner of
    /// its volume: uploads, sealed objects, imports and custody records are
    /// promised their bytes before any acknowledgement.
    pub fn open_with_disk(
        root: impl AsRef<Path>,
        limits: StoreLimits,
        disk: DiskBudget,
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
        lock.try_lock_exclusive().map_err(|e| {
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
        let mut store = Self {
            root,
            limits,
            uploads: BTreeMap::new(),
            staged_bytes: 0,
            staged_by_domain: BTreeMap::new(),
            terminal_uploads: 0,
            disk,
            failed: false,
            _writer_lock: lock,
        };
        store.recover_uploads()?;
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
        // Re-sync created ancestors left visible by a previous mkdir/sync
        // failure; filesystem root itself is not a newly created entry.
        for directory in store.root.ancestors() {
            if directory.as_os_str().is_empty() {
                sync_directory(Path::new("."))?;
            } else if directory.parent().is_some() {
                sync_directory(directory)?;
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
        Ok(hash)
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
        let bytes = read_bounded(
            &self
                .root
                .join(kind.directory())
                .join(format!("{hash}.record")),
            max_bytes.min(kind.limit()),
        )?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        Ok(bytes)
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
        if terminal::contains(&self.meta_path(id), id)? {
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
        let staging = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Staging,
            BudgetLane::Ordinary,
            length,
        )?;
        let part = self.part_path(id);
        // An orphan with no durable metadata is never an accepted upload; remove it.
        if part.exists() {
            fs::remove_file(&part)?;
        }
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&part)?;
        file.sync_all()?;
        let bytes = postcard::to_stdvec(&meta)?;
        atomic_install(&self.meta_path(id), &bytes)?;
        self.staged_bytes = staged_bytes;
        self.staged_by_domain.insert(domain, domain_bytes);
        self.uploads.insert(
            id,
            Upload {
                meta,
                file,
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
            upload.file.seek(SeekFrom::Start(offset))?;
            let mut existing = zeroed_buffer(bytes.len())?;
            upload.file.read_exact(&mut existing)?;
            return if existing == bytes {
                Ok(upload.offset)
            } else {
                Err(ContentError::Corrupt)
            };
        }
        if offset != upload.offset {
            return Err(ContentError::Offset(upload.offset));
        }
        upload.file.seek(SeekFrom::Start(offset))?;
        if let Err(error) = upload
            .file
            .write_all(bytes)
            .and_then(|()| upload.file.sync_all())
        {
            // A partial/error write has an unknown outcome. Reload its actual prefix on
            // next retry; the caller must use the returned/queried durable offset.
            upload.file.set_len(offset)?;
            upload.file.sync_all()?;
            return Err(ContentError::Io(error));
        }
        upload.offset = end;
        Ok(end)
    }

    pub fn offset(&self, id: UploadId) -> Result<u64, ContentError> {
        self.check()?;
        self.uploads
            .get(&id)
            .map(|u| u.offset)
            .ok_or(ContentError::MissingUpload)
    }

    pub fn seal(&mut self, id: UploadId) -> Result<ContentRef, ContentError> {
        self.check()?;
        let result = self.seal_inner(id);
        self.mark_failure(&result);
        result
    }
    fn seal_inner(&mut self, id: UploadId) -> Result<ContentRef, ContentError> {
        let upload = self
            .uploads
            .get_mut(&id)
            .ok_or(ContentError::MissingUpload)?;
        if upload.offset != upload.meta.expected_length {
            return Err(ContentError::Incomplete {
                received: upload.offset,
                expected: upload.meta.expected_length,
            });
        }
        upload.file.sync_all()?;
        upload.file.seek(SeekFrom::Start(0))?;
        let domain = upload.meta.domain;
        // The sealed object is a second copy of the staged bytes until the
        // part is removed at finish; promise it before the first chunk lands.
        let objects = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            BudgetLane::Ordinary,
            upload.offset,
        )?;
        let directory = self.root.join("objects").join(hex(&domain.0));
        durable_directory(&directory)?;
        let mut chunks = Vec::new();
        let mut hasher = blake3::Hasher::new();
        let buffer_len = usize::try_from(upload.offset.min(self.limits.chunk_bytes as u64))
            .map_err(|_| ContentError::Capacity)?;
        let mut buffer = zeroed_buffer(buffer_len)?;
        let mut remaining = upload.offset;
        while remaining > 0 {
            let count = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| ContentError::Capacity)?;
            let block = buffer.get_mut(..count).ok_or(ContentError::Corrupt)?;
            upload.file.read_exact(block)?;
            hasher.update(block);
            let hash = ContentHash(*blake3::hash(block).as_bytes());
            install_verified_chunk(&directory.join(format!("{hash}.chunk")), block, hash)?;
            chunks.try_reserve(1).map_err(|_| ContentError::Capacity)?;
            chunks.push(Chunk {
                hash,
                length: count as u32,
            });
            // Bound intermediate manifest growth before serializing it.
            if chunks.len().checked_mul(40).ok_or(ContentError::Capacity)?
                > self.limits.max_manifest_bytes
            {
                return Err(ContentError::Capacity);
            }
            remaining = remaining
                .checked_sub(count as u64)
                .ok_or(ContentError::Corrupt)?;
        }
        let stream_digest = ContentHash(*hasher.finalize().as_bytes());
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
            length: upload.offset,
            stream_digest,
            chunks,
        };
        let mut bytes = MANIFEST_MAGIC.to_vec();
        bytes.extend(postcard::to_stdvec(&manifest)?);
        if bytes.len() > self.limits.max_manifest_bytes {
            return Err(ContentError::Capacity);
        }
        let root = ContentHash(*blake3::hash(&bytes).as_bytes());
        atomic_install(&directory.join(format!("{root}.manifest")), &bytes)?;
        objects.commit();
        let reference = ContentRef {
            domain,
            root,
            length: manifest.length,
            class: manifest.class,
        };
        // Keep upload metadata for idempotent seal after a lost response/restart until
        // explicit finish acknowledges the reference is retained by its caller.
        Ok(reference)
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
        if length.is_none() {
            if terminal::contains(&self.meta_path(id), id)? {
                return terminal::remove_part(&self.part_path(id));
            }
            self.reserve_terminal_slot()?;
        }
        let staged_bytes = self
            .staged_bytes
            .checked_sub(length.unwrap_or(0))
            .ok_or(ContentError::Corrupt)?;
        let terminal_uploads = self
            .terminal_uploads
            .checked_add(1)
            .filter(|count| *count <= MAX_TERMINAL_UPLOADS)
            .ok_or(ContentError::Capacity)?;
        // Publish the durable replay fence before releasing bytes or the live
        // handle. A delayed Begin can never reconstruct this scoped identity.
        terminal::install(&self.meta_path(id), id)?;
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
        terminal::remove_part(&self.part_path(id))
    }

    /// Delivers only checksum-verified chunks, so a corrupt later chunk cannot cause
    /// earlier unverified bytes to escape. The complete manifest is authenticated first.
    pub fn read_verified(
        &self,
        reference: &ContentRef,
        sink: impl Write,
    ) -> Result<(), ContentError> {
        self.check()?;
        read_verified_at(&self.root, reference, sink)
    }

    pub fn read_bytes(
        &self,
        reference: &ContentRef,
        budget: usize,
    ) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        read_bytes_at(&self.root, reference, budget)
    }

    pub fn verify(&self, reference: &ContentRef) -> Result<(), ContentError> {
        self.read_verified(reference, std::io::sink())
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
        let manifest = self.manifest(reference)?;
        let end = offset
            .checked_add(max_bytes as u64)
            .ok_or(ContentError::Capacity)?
            .min(reference.length);
        let length = end.checked_sub(offset).ok_or(ContentError::Corrupt)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(usize::try_from(length).map_err(|_| ContentError::Capacity)?)
            .map_err(|_| ContentError::Capacity)?;
        let directory = self.root.join("objects").join(hex(&reference.domain.0));
        let mut start = 0u64;
        for chunk in manifest.chunks {
            let next = start
                .checked_add(u64::from(chunk.length))
                .ok_or(ContentError::Corrupt)?;
            if next > offset && start < end {
                let bytes = read_bounded(
                    &directory.join(format!("{}.chunk", chunk.hash)),
                    MAX_TRANSFER_CHUNK_BYTES,
                )?;
                if bytes.len() != chunk.length as usize
                    || ContentHash(*blake3::hash(&bytes).as_bytes()) != chunk.hash
                {
                    return Err(ContentError::Corrupt);
                }
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

    fn manifest(&self, reference: &ContentRef) -> Result<Manifest, ContentError> {
        self.check()?;
        manifest_at(&self.root, reference)
    }
    /// The store directory; a lock-free [`ContentReader`] over the same path
    /// serves replay and recovery reads while this writer owns installation.
    pub fn root(&self) -> &Path {
        &self.root
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

    fn part_path(&self, id: UploadId) -> PathBuf {
        self.root
            .join("staging")
            .join(format!("{}.part", hex(&id.0)))
    }
    fn meta_path(&self, id: UploadId) -> PathBuf {
        self.root
            .join("staging")
            .join(format!("{}.meta", hex(&id.0)))
    }

    fn recover_uploads(&mut self) -> Result<(), ContentError> {
        for entry in fs::read_dir(self.root.join("staging"))? {
            let entry = entry?;
            if entry.path().extension().and_then(|x| x.to_str()) == Some("terminal") {
                terminal::discard_pending(&entry.path())?;
                continue;
            }
            if entry.path().extension().and_then(|x| x.to_str()) != Some("meta") {
                continue;
            }
            let bytes = read_bounded(
                &entry.path(),
                self.limits.max_manifest_bytes.max(terminal::RECORD_BYTES),
            )?;
            if let Some(id) = terminal::decode(&bytes)? {
                if entry.path() != self.meta_path(id) {
                    return Err(ContentError::Corrupt);
                }
                terminal::contains(&entry.path(), id)?;
                self.terminal_uploads = self
                    .terminal_uploads
                    .checked_add(1)
                    .filter(|count| *count <= MAX_TERMINAL_UPLOADS)
                    .ok_or(ContentError::Capacity)?;
                if self
                    .terminal_uploads
                    .checked_add(self.uploads.len())
                    .is_none_or(|count| count > MAX_TERMINAL_UPLOADS)
                {
                    return Err(ContentError::Capacity);
                }
                terminal::remove_part(&self.part_path(id))?;
                continue;
            }
            let (meta, remaining): (UploadMeta, &[u8]) = postcard::take_from_bytes(&bytes)?;
            if !remaining.is_empty()
                || meta.schema != UPLOAD_SCHEMA
                || entry.path() != self.meta_path(meta.id)
            {
                return Err(ContentError::Corrupt);
            }
            self.reserve_upload(meta.domain, meta.expected_length)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.part_path(meta.id))?;
            let offset = file.metadata()?.len();
            if offset > meta.expected_length {
                return Err(ContentError::Corrupt);
            }
            // Full chunks acknowledged before restart were synced. An unacknowledged
            // tail can also survive and is verified against the caller's digest at seal.
            file.sync_all()?;
            self.staged_bytes = self
                .staged_bytes
                .checked_add(meta.expected_length)
                .ok_or(ContentError::Corrupt)?;
            let domain_bytes = self
                .staged_by_domain
                .get(&meta.domain)
                .copied()
                .unwrap_or(0)
                .checked_add(meta.expected_length)
                .ok_or(ContentError::Corrupt)?;
            self.staged_by_domain.insert(meta.domain, domain_bytes);
            // The bytes already received are on the volume; promise only the
            // remainder, and keep an upload resumable when the volume cannot.
            let staging = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Staging,
                BudgetLane::Completion,
                meta.expected_length.saturating_sub(offset),
            )
            .ok();
            self.uploads.insert(
                meta.id,
                Upload {
                    meta,
                    file,
                    offset,
                    _staging: staging,
                },
            );
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

fn manifest_at(root: &Path, reference: &ContentRef) -> Result<Manifest, ContentError> {
    let path = root
        .join("objects")
        .join(hex(&reference.domain.0))
        .join(format!("{}.manifest", reference.root));
    let bytes = read_bounded(&path, MAX_TRANSFER_MANIFEST_BYTES)?;
    if ContentHash(*blake3::hash(&bytes).as_bytes()) != reference.root
        || !bytes.starts_with(MANIFEST_MAGIC)
    {
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
    reference: &ContentRef,
    mut sink: impl Write,
) -> Result<(), ContentError> {
    let manifest = manifest_at(root, reference)?;
    let dir = root.join("objects").join(hex(&reference.domain.0));
    let mut whole = blake3::Hasher::new();
    for chunk in manifest.chunks {
        let bytes = read_bounded(
            &dir.join(format!("{}.chunk", chunk.hash)),
            MAX_TRANSFER_CHUNK_BYTES,
        )?;
        if bytes.len() != chunk.length as usize
            || ContentHash(*blake3::hash(&bytes).as_bytes()) != chunk.hash
        {
            return Err(ContentError::Corrupt);
        }
        whole.update(&bytes);
        sink.write_all(&bytes)?;
    }
    if ContentHash(*whole.finalize().as_bytes()) != manifest.stream_digest {
        return Err(ContentError::Corrupt);
    }
    Ok(())
}
fn read_bytes_at(
    root: &Path,
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
    read_verified_at(root, reference, &mut bytes)?;
    Ok(bytes)
}

/// Lock-free, read-only access to installed content. Installed trees are
/// immutable and reached by atomic rename, so a reader never observes a partial
/// object: content is either verifiable or absent. It never installs, stages or
/// witnesses custody; the exclusive [`ContentStore`] writer keeps those rights.
#[derive(Debug, Clone)]
pub struct ContentReader {
    root: PathBuf,
}
impl ContentReader {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        if !root.join("objects").is_dir() || !root.join("custody").is_dir() {
            return Err(ContentError::Invalid);
        }
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn read_verified(
        &self,
        reference: &ContentRef,
        sink: impl Write,
    ) -> Result<(), ContentError> {
        read_verified_at(&self.root, reference, sink)
    }
    pub fn read_bytes(
        &self,
        reference: &ContentRef,
        budget: usize,
    ) -> Result<Vec<u8>, ContentError> {
        read_bytes_at(&self.root, reference, budget)
    }
    pub fn verify(&self, reference: &ContentRef) -> Result<(), ContentError> {
        read_verified_at(&self.root, reference, std::io::sink())
    }
    pub fn read_custody_record(
        &self,
        kind: CustodyRecordKind,
        hash: ContentHash,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ContentError> {
        let bytes = read_bounded(
            &self
                .root
                .join(kind.directory())
                .join(format!("{hash}.record")),
            max_bytes.min(kind.limit()),
        )?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        Ok(bytes)
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
        return Ok(());
    }
    atomic_install(path, bytes)
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
    fs::rename(&temp, path)?;
    sync_directory(path.parent().ok_or(ContentError::Invalid)?)?;
    Ok(())
}

fn durable_directory(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        durable_directory(parent)?;
    }
    fs::create_dir(path)?;
    sync_directory(path)?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        sync_directory(parent)?;
    }
    Ok(())
}
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

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
        let mut store =
            ContentStore::open_with_disk(dir.path(), limits(), guarded.clone()).unwrap();
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
        assert!(
            !dir.path()
                .join("staging")
                .join(format!("{}.part", hex(&id.0)))
                .exists()
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
        let mut store = ContentStore::open_with_disk(dir.path(), limits(), open.clone()).unwrap();
        store
            .begin(
                id,
                ContentDomainId([1; 16]),
                ContentClass::Evidence,
                8,
                None,
            )
            .unwrap();
        assert_eq!(open.stats().by_kind[DiskKind::Staging as usize], 8);
        store.append(id, 0, b"abcd").unwrap();
        store.append(id, 4, b"efgh").unwrap();
        store.seal(id).unwrap();
        assert_eq!(open.stats().by_kind[DiskKind::Content as usize], 0);
        assert_eq!(open.stats().outstanding, 8);
        store.finish(id).unwrap();
        assert_eq!(open.stats().outstanding, 0);
    }
    #[test]
    fn one_domain_cannot_fill_the_whole_staging_allowance() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = ContentStore::open(dir.path(), limits()).unwrap();
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
        let mut store = ContentStore::open(dir.path(), limits()).unwrap();
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
            let mut store = ContentStore::open(dir.path(), limits()).unwrap();
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
        let mut reopened = ContentStore::open(dir.path(), limits()).unwrap();
        assert_eq!(
            reopened
                .read_custody_record(CustodyRecordKind::Checkpoint, hash, 4096)
                .unwrap(),
            checkpoint
        );
        std::fs::write(
            dir.path()
                .join("checkpoints")
                .join(format!("{hash}.record")),
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
        let mut store = ContentStore::open(dir.path(), limits()).unwrap();
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
        let mut reopened = ContentStore::open(dir.path(), limits()).unwrap();
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
            let mut s = ContentStore::open(dir.path(), limits()).unwrap();
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
            let mut s = ContentStore::open(dir.path(), limits()).unwrap();
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
        let s = ContentStore::open(dir.path(), limits()).unwrap();
        assert_eq!(s.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
        let mut wrong = reference.clone();
        wrong.domain = ContentDomainId([3; 16]);
        assert!(s.verify(&wrong).is_err());
    }

    #[test]
    fn bounded_ranges_verify_selected_chunks_across_boundaries() {
        let root = tempfile::tempdir().unwrap();
        let mut store = ContentStore::open(root.path(), limits()).unwrap();
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
        let path = root
            .path()
            .join("objects")
            .join(hex(&reference.domain.0))
            .join(format!("{}.chunk", manifest.chunks[1].hash));
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
        let mut store = ContentStore::open(root.path(), limits()).unwrap();
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
        let mut recovered = ContentStore::open(root.path(), limits()).unwrap();
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
        let mut s = ContentStore::open(dir.path(), limits()).unwrap();
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
        fs::write(
            s.root
                .join("objects")
                .join(hex(&r.domain.0))
                .join(format!("{hash}.chunk")),
            b"oops",
        )
        .unwrap();
        assert!(matches!(s.verify(&r), Err(ContentError::Corrupt)));
        assert!(matches!(
            ContentStore::open(dir.path(), limits()),
            Err(ContentError::Locked)
        ));
    }
}
