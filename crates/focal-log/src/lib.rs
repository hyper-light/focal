#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! A node-local, multiplexed, durable physical write-ahead log.
//!
//! A group commit's frames are flushed, then a commit frame is written after them and
//! flushed: a commit on disk proves its batch reached the disk first, and the batch is
//! durable once its commit is (docs/archictecutre/28-commit-frames.md). The
//! checksummed `CURRENT` file is a durability fence written rarely: when a generation
//! begins, when the base passes a segment boundary, and at open. Every byte before it
//! must validate; past it, recovery follows the chain to the last commit frame and
//! drops a torn tail, but fails closed on damage a crash does not leave.
//! A failed write permanently poisons the writer until it is reopened and recovered.
//!
//! The fence also names the base: where the durable prefix starts. A group's
//! checkpoint writes what the group keeps and a floor that retires what it
//! held before; nothing else is rewritten. The shared writer moves the base
//! toward the tail over the frames floors retired, writes each live frame it
//! meets again at the tail under the sequence it was first written at (its
//! origin, which keeps its place in its group's order), and removes the
//! segments behind the base — the copies and the base durable by one fence.
//!
//! `File::sync_all` and directory synchronization provide the OS/filesystem flush
//! contract. This does not claim protection against a drive that lies about flushes.

use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;

const MAGIC: &[u8; 8] = b"FOCALW01";
/// Format: a segment whose frames end at the fence (no commit frames).
const SEGMENT_FENCED: u32 = 1;
/// Format: a segment whose group commits end with commit frames (doc 28).
const SEGMENT_COMMITTED: u32 = 2;
/// Format: the bit of a frame's length field that marks a commit frame. A record's length stays below it.
const COMMIT_FLAG: u32 = 1 << 31;
/// Format: a commit frame's payload when the base moved since the last commit: the durable base (segment u64,
/// byte u64, sequence u64, checksum u32). A commit whose base stayed carries none.
const COMMIT_PAYLOAD: usize = 8 + 8 + 8 + 4;
/// Derived: a commit frame's bytes on disk at most, its header and a base: what a group commit adds.
const COMMIT_FRAME_BYTES: usize = FRAME_HEADER + COMMIT_PAYLOAD;
/// Cited: the unit a drive writes atomically, so a write cut short leaves whole unwritten sectors (etcd's
/// `minSectorSize`, the 512-byte logical sector every drive exposes).
const SECTOR: u64 = 512;
const FENCE_MAGIC: &[u8; 8] = b"FOCALF01";
const HEADER_LEN: u64 = 72;
const FRAME_HEADER: usize = 20;
const MAX_FENCE_BYTES: u64 = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogicalLogId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalIdentity {
    pub cluster: [u8; 16],
    pub node: u64,
    pub stream: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RecordKind {
    Entry,
    HardState,
    Configuration,
    Snapshot,
    Identity,
    Checkpoint,
    /// Immutable application decoder requirement for one logical Raft group.
    /// Appended at variant 6 so older WAL decoders reject the physical log.
    DecoderFloor,
    /// One ordered successor promise, preserving the original decoder floor.
    /// Append-only ordinal: floor-aware older binaries must refuse this stream.
    DecoderTransition,
    /// The group has the fast track: part of what the group is, written
    /// once beside its identity. Appended at variant 8, so a binary that
    /// knows no fast track refuses the stream and never joins such a group
    /// by the classic rules.
    FastTrack,
    /// An entry a member approved by itself, held beside its log until a
    /// write releases it ([`RecordKind::Released`]), whatever the log reaches.
    /// Variant 9.
    Proposal,
    /// The physical layer's own record: every frame of `log` whose origin
    /// is before the sequence in `index` is dead — a checkpoint of the
    /// group wrote what it keeps from that sequence on, in the `term`
    /// frames just before this one. Never delivered to a group's replay.
    /// Variant 10, so a binary that knows no floors refuses the stream
    /// rather than replaying the frames a floor retired.
    Floor,
    /// The physical layer's own wrapper: a live frame the base of the log
    /// passed, written again at its tail. `index` is the frame's origin —
    /// the sequence it was first written at, which places it among its
    /// group's frames and is what its group's floor is compared with — and
    /// the payload is the record as it was first encoded, which is what a
    /// group's replay is given. Variant 11.
    Moved,
    /// The proposals a member approved by itself at or below `index` are held
    /// no more: it knew its log committed through `index` by a classic quorum
    /// (hyper-raft `Ready::released`). Written before the proposals of the same
    /// write, which it does not end, and given back at opening so the member
    /// holds again only what it never released. Only in a group with the fast
    /// track. Variant 12, so a binary that knows no release refuses the stream
    /// rather than holding every proposal it ever approved.
    Released,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub log: LogicalLogId,
    pub kind: RecordKind,
    pub index: u64,
    pub term: u64,
    #[serde(with = "focal_memory::serde_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct WalOptions {
    pub identity: WalIdentity,
    pub segment_bytes: u64,
    pub max_record_bytes: usize,
    pub max_batch_bytes: usize,
}

impl WalOptions {
    pub fn new(identity: WalIdentity) -> Self {
        Self {
            identity,
            segment_bytes: 64 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            max_batch_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurablePosition {
    pub generation: u64,
    pub segment: u64,
    pub byte: u64,
    pub sequence: u64,
    pub checksum: u32,
}

/// Where the durable prefix starts: the first retained frame's place, and
/// the chain state before it (the sequence and checksum of the frame the
/// base last passed). Nothing before it is live: segments before its
/// segment are gone, and the frames before it in its segment are read by
/// nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableBase {
    pub segment: u64,
    pub byte: u64,
    pub sequence: u64,
    pub checksum: u32,
}
impl Default for DurableBase {
    /// The start of a stream: the first frame of segment zero.
    fn default() -> Self {
        Self {
            segment: 0,
            byte: HEADER_LEN,
            sequence: 0,
            checksum: 0,
        }
    }
}

/// What the wrapper of a moved frame adds to the record it carries: the
/// log, the kind, the origin, the term and the payload's length, each at
/// its longest encoding.
const MOVED_OVERHEAD: usize = 16 + 5 + 10 + 10 + 10;

const FENCE_VERSION: u32 = 2;
#[derive(Serialize, Deserialize)]
struct Fence {
    version: u32,
    identity: WalIdentity,
    position: DurablePosition,
    base: DurableBase,
}
/// The fence as version 1 wrote it: a prefix that starts at segment zero.
#[derive(Deserialize)]
struct FenceV1 {
    version: u32,
    identity: WalIdentity,
    position: DurablePosition,
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("WAL I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL is already owned by another writer")]
    Locked,
    #[error("logical WAL is already owned by another group instance")]
    LogicalLocked,
    #[error("WAL identity or format does not match this node")]
    Identity,
    #[error("WAL corruption at {path}:{offset}: {reason}")]
    Corruption {
        path: PathBuf,
        offset: u64,
        reason: &'static str,
    },
    #[error("record or batch exceeds the configured bounded allocation")]
    Capacity,
    #[error("WAL writer stopped after an ambiguous I/O failure; reopen for recovery")]
    Failed,
    #[error("WAL encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("synchronous WAL operation cannot run inside a replay callback")]
    ReplayReentry,
    #[error("WAL append receipt was already consumed")]
    ReceiptConsumed,
    #[error("WAL holds moved frames; its groups replay through the shared writer's index")]
    Relocated,
    #[error("WAL was converted to hyper-log; its groups are read there (27 §15.8)")]
    Converted,
}

pub struct Wal {
    directory: PathBuf,
    /// The fence's file and its staging file, joined once at open: a fence is installed on every group
    /// commit, and joining the paths there grew two path buffers per commit.
    fence: FencePaths,
    options: WalOptions,
    _lock: focal_platform::FileLock,
    active: File,
    position: DurablePosition,
    base: DurableBase,
    /// What the installed fence names: recovery checks every frame up to it strictly, and no segment behind its
    /// base is removed.
    fence_position: DurablePosition,
    fence_base: DurableBase,
    /// The commit frame the last group commit wrote, for the writer's index to count.
    last_commit: Option<FrameLocation>,
    /// The base the last commit frame carried (or the fence's): a commit carries the base only when it moved.
    committed_base: DurableBase,
    failed: bool,
    fault: Option<FaultPoint>,
}

pub mod conversion;
mod writer;
#[cfg(feature = "test-support")]
pub use writer::{MAX_QUEUE_ITEMS, WalPause};
pub use writer::{
    Persisted, SharedWal, WalAppend, WalLease, WalWriterId, WalWriterLimits, WalWriterStats,
};

/// Faults are injected at actual durability boundaries for crash-model tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    AfterAppend,
    AfterDataSync,
    AfterFenceInstall,
    /// The fence names a later base and the segments before it are still
    /// on disk.
    AfterBaseFence,
}

impl Wal {
    /// Open an exclusively owned stream, validate the entire durable prefix, and
    /// discard only bytes that no successful append could have acknowledged.
    pub fn open(directory: impl AsRef<Path>, options: WalOptions) -> Result<Self, LogError> {
        Self::open_indexed(directory, options, |_| Ok(()))
    }

    /// Open, reading the durable prefix twice: first the header of every
    /// frame (`ScanEvent::Header`: its log, kind, index and term, from the
    /// record's leading fields, nothing owned), then, after
    /// `ScanEvent::Counted`, every record with its location — so an index
    /// can size each group exactly, and know each group's floor, before it
    /// holds a frame.
    fn open_indexed(
        directory: impl AsRef<Path>,
        options: WalOptions,
        mut visitor: impl FnMut(ScanEvent) -> Result<(), LogError>,
    ) -> Result<Self, LogError> {
        if options.max_record_bytes == 0
            // A frame's length stays below the commit flag (doc 28).
            || frame_limit(&options) >= COMMIT_FLAG as usize
            || options.max_batch_bytes < options.max_record_bytes
            || options.segment_bytes
                < HEADER_LEN
                    .saturating_add(FRAME_HEADER as u64)
                    .saturating_add(1)
        {
            return Err(LogError::Capacity);
        }
        let directory = directory.as_ref().to_path_buf();
        create_durable_directory(&directory)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("LOCK"))?;
        let lock = focal_platform::FileLock::exclusive(lock).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                LogError::Locked
            } else {
                LogError::Io(e)
            }
        })?;
        let fence_paths = FencePaths::of(&directory);
        let current = fence_paths.current.clone();
        let (position, base) = if current.exists() {
            let fence = read_fence(&current)?;
            if fence.version == conversion::FENCE_CONVERTED {
                return Err(LogError::Converted);
            }
            if fence.version != FENCE_VERSION || fence.identity != options.identity {
                return Err(LogError::Identity);
            }
            (fence.position, fence.base)
        } else {
            // A missing fence is safe only for a never-acknowledged initial stream.
            // A durable sentinel distinguishes that case from accidental metadata loss.
            if directory.join("INITIALIZED").exists() {
                return Err(corrupt(&current, 0, "durability fence missing"));
            }
            let position = DurablePosition {
                generation: 1,
                segment: 0,
                byte: HEADER_LEN,
                sequence: 0,
                checksum: 0,
            };
            let path = segment_path(&directory, 1, 0);
            if path.exists() {
                fs::remove_file(&path)?;
            }
            create_segment(&directory, &options, position, 0)?;
            install_fence(
                &fence_paths,
                &directory,
                &options,
                position,
                DurableBase::default(),
            )?;
            let sentinel = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join("INITIALIZED"))?;
            sentinel.sync_all()?;
            sync_dir(&directory)?;
            (position, DurableBase::default())
        };
        let (fence_position, fence_base) = (position, base);
        // Past the fence, the chain runs to the last commit frame; its base is the durable base (doc 28).
        let (position, base) = discover_commit(&directory, &options, position, base)?;
        visitor(ScanEvent::Base(base))?;
        scan_headers(&directory, &options, base, position, |header, sequence| {
            visitor(ScanEvent::Header(header, sequence))
        })?;
        visitor(ScanEvent::Counted)?;
        scan_indexed(&directory, &options, base, position, |record, location| {
            visitor(ScanEvent::Frame(record, location))
        })?;
        // An interrupted first initialization may have installed CURRENT before
        // the sentinel. Never admit appends until metadata-loss detection is durable.
        if !directory.join("INITIALIZED").exists() {
            let sentinel = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join("INITIALIZED"))?;
            sentinel.sync_all()?;
            sync_dir(&directory)?;
        }
        // A later commit than the fence names is fenced before anything behind its base is removed.
        if (position, base) != (fence_position, fence_base) {
            install_fence(&fence_paths, &directory, &options, position, base)?;
        }
        let path = segment_path(&directory, position.generation, position.segment);
        let mut active = OpenOptions::new().read(true).write(true).open(&path)?;
        if active.metadata()?.len() != position.byte {
            active.set_len(position.byte)?;
            active.sync_all()?;
        }
        cleanup_segments(&directory, base, position)?;
        // A log written before commit frames rolls to a segment that has them before its first append, so no
        // segment mixes the two formats (doc 28 §3).
        let mut header = [0u8; HEADER_LEN as usize];
        active.seek(SeekFrom::Start(0))?;
        active.read_exact(&mut header)?;
        let (active, position) =
            if segment_version(&header, &options, position.generation, position.segment)
                == Some(SEGMENT_FENCED)
            {
                let next = DurablePosition {
                    segment: position.segment.checked_add(1).ok_or(LogError::Capacity)?,
                    byte: HEADER_LEN,
                    ..position
                };
                let active = create_segment(&directory, &options, next, position.checksum)?;
                install_fence(&fence_paths, &directory, &options, next, base)?;
                (active, next)
            } else {
                (active, position)
            };
        let mut active = active;
        active.seek(SeekFrom::Start(position.byte))?;
        Ok(Self {
            directory,
            fence: fence_paths,
            options,
            _lock: lock,
            active,
            position,
            base,
            fence_position: position,
            fence_base: base,
            last_commit: None,
            committed_base: base,
            failed: false,
            fault: None,
        })
    }

    pub fn position(&self) -> DurablePosition {
        self.position
    }
    /// Where the durable prefix starts.
    pub fn base(&self) -> DurableBase {
        self.base
    }

    /// Recovery is streaming: at most one bounded frame is decoded at a
    /// time. Only live records are delivered: a frame below its group's
    /// floor, and the floors themselves, are the physical layer's.
    pub fn replay(
        &self,
        mut visitor: impl FnMut(Record) -> Result<(), LogError>,
    ) -> Result<(), LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        let mut floors: std::collections::BTreeMap<LogicalLogId, u64> =
            std::collections::BTreeMap::new();
        scan_headers(
            &self.directory,
            &self.options,
            self.base,
            self.position,
            |header, _| {
                // A moved frame stands after frames its group wrote later:
                // only an index that places frames by origin replays such
                // a stream in its groups' order.
                if header.kind == RecordKind::Moved {
                    return Err(LogError::Relocated);
                }
                if header.kind == RecordKind::Floor {
                    if !floors.contains_key(&header.log) && floors.len() >= MAX_REPLAY_FLOORS {
                        return Err(LogError::Capacity);
                    }
                    floors.insert(header.log, header.index);
                }
                Ok(())
            },
        )?;
        scan_indexed(
            &self.directory,
            &self.options,
            self.base,
            self.position,
            |record, location| {
                if record.kind == RecordKind::Floor
                    || floors
                        .get(&record.log)
                        .is_some_and(|floor| location.origin < *floor)
                {
                    return Ok(());
                }
                visitor(record)
            },
        )
    }

    /// Append one natural group-commit batch. No returned position precedes both
    /// the data flush and the durability-fence directory flush.
    pub fn append(&mut self, records: &[Record]) -> Result<DurablePosition, LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        let encoded = self.encode_batch(records)?;
        if encoded.is_empty() {
            return Ok(self.position);
        }
        let result = self.append_encoded(&encoded);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Replace retained WAL records using a new, independently verified generation.
    /// Callers must supply a complete recoverable checkpoint plus all retained log
    /// suffixes and current per-log metadata. The previous generation survives until
    /// the new data and fence are durable; this never truncates the live WAL in place.
    pub fn rewrite_checkpoint(&mut self, retained: &[Record]) -> Result<DurablePosition, LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        let encoded = self.encode_batch(retained)?;
        let generation = self
            .position
            .generation
            .checked_add(1)
            .ok_or(LogError::Capacity)?;
        let position = DurablePosition {
            generation,
            segment: 0,
            byte: HEADER_LEN,
            sequence: 0,
            checksum: 0,
        };
        let result = (|| {
            self.active = create_segment(&self.directory, &self.options, position, 0)?;
            self.position = position;
            // The replacement generation starts its own prefix.
            self.base = DurableBase::default();
            // A new generation is due a fence, so this commit installs one.
            let position = self.append_encoded(&encoded)?;
            scan(&self.directory, &self.options, self.base, position, |_| {
                Ok(())
            })?;
            cleanup_segments(&self.directory, self.fence_base, position)?;
            Ok(position)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Name a later base for the next fence to install. The caller has
    /// read every frame between the two from the durable prefix and written
    /// each live one again, in the same group commit the fence closes; the
    /// fence makes both durable at once, or neither.
    fn set_base(&mut self, base: DurableBase) -> Result<(), LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        if (base.segment, base.byte) < (self.base.segment, self.base.byte)
            || base.sequence < self.base.sequence
            || base.segment > self.position.segment
            || base.sequence > self.position.sequence
        {
            return Err(LogError::Failed);
        }
        self.base = base;
        Ok(())
    }
    /// Remove the segments before the base the fence names (a crash before
    /// this leaves them for the next open to remove). Only the fenced base's:
    /// a base a commit frame carried has no fence yet, and recovery starts
    /// from the fence's.
    fn retire_segments(&mut self) -> Result<(), LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        let result = (|| {
            self.fail_at(FaultPoint::AfterBaseFence)?;
            cleanup_segments(&self.directory, self.fence_base, self.position)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    pub fn inject_fault_once(&mut self, point: FaultPoint) {
        self.fault = Some(point);
    }

    fn fail_at(&mut self, point: FaultPoint) -> Result<(), LogError> {
        if self.fault == Some(point) {
            self.fault = None;
            return Err(LogError::Io(std::io::Error::other(
                "injected durability failure",
            )));
        }
        Ok(())
    }

    fn encode_batch(&self, records: &[Record]) -> Result<Vec<Vec<u8>>, LogError> {
        let mut total = 0usize;
        let mut encoded = Vec::with_capacity(records.len().min(1024));
        for record in records {
            // The floor and the moved frame are the physical layer's own.
            if matches!(record.kind, RecordKind::Floor | RecordKind::Moved) {
                return Err(LogError::Identity);
            }
            if record.payload.len() > self.options.max_record_bytes {
                return Err(LogError::Capacity);
            }
            let data = postcard::to_stdvec(record)?;
            if data.len() > self.options.max_record_bytes {
                return Err(LogError::Capacity);
            }
            total = total
                .checked_add(
                    data.len()
                        .checked_add(FRAME_HEADER)
                        .ok_or(LogError::Capacity)?,
                )
                .ok_or(LogError::Capacity)?;
            if total > self.options.max_batch_bytes {
                return Err(LogError::Capacity);
            }
            encoded.push(data);
        }
        Ok(encoded)
    }

    fn append_encoded(&mut self, encoded: &[Vec<u8>]) -> Result<DurablePosition, LogError> {
        self.write_encoded(encoded)?;
        self.finish_append()
    }

    fn write_encoded(&mut self, encoded: &[Vec<u8>]) -> Result<(), LogError> {
        self.write_encoded_indexed(encoded, |_| Ok(()))
    }

    fn write_encoded_indexed(
        &mut self,
        encoded: &[Vec<u8>],
        mut visit: impl FnMut(FrameLocation) -> Result<(), LogError>,
    ) -> Result<(), LogError> {
        for data in encoded {
            visit(self.write_frame(data)?)?;
        }
        Ok(())
    }

    /// Write one frame at the tail. Its origin is its own sequence.
    fn write_frame(&mut self, data: &[u8]) -> Result<FrameLocation, LogError> {
        let field = u32::try_from(data.len()).map_err(|_| LogError::Capacity)?;
        if field & COMMIT_FLAG != 0 {
            return Err(LogError::Capacity);
        }
        self.write_frame_with(field, data)
    }

    /// Ends the group commit written so far with a commit frame (doc 28): its header alone when the base
    /// stayed where the last commit left it, and the base after it when the base moved.
    fn write_commit(&mut self) -> Result<FrameLocation, LogError> {
        if self.base == self.committed_base {
            return self.write_frame_with(COMMIT_FLAG, &[]);
        }
        let mut payload = [0u8; COMMIT_PAYLOAD];
        let (segment, rest) = payload.split_at_mut(8);
        segment.copy_from_slice(&self.base.segment.to_le_bytes());
        let (byte, rest) = rest.split_at_mut(8);
        byte.copy_from_slice(&self.base.byte.to_le_bytes());
        let (sequence, checksum) = rest.split_at_mut(8);
        sequence.copy_from_slice(&self.base.sequence.to_le_bytes());
        checksum.copy_from_slice(&self.base.checksum.to_le_bytes());
        // The payload's length is fixed and below the flag.
        let field = COMMIT_FLAG | (COMMIT_PAYLOAD as u32);
        let location = self.write_frame_with(field, &payload)?;
        self.committed_base = self.base;
        Ok(location)
    }

    /// Writes one frame at the tail with `field` as its length word: a record's length, or a commit frame's.
    fn write_frame_with(&mut self, field: u32, data: &[u8]) -> Result<FrameLocation, LogError> {
        {
            let frame_bytes = FRAME_HEADER
                .checked_add(data.len())
                .ok_or(LogError::Capacity)? as u64;
            if self.position.byte > HEADER_LEN
                && self
                    .position
                    .byte
                    .checked_add(frame_bytes)
                    .ok_or(LogError::Capacity)?
                    > self.options.segment_bytes
            {
                self.active.sync_all()?;
                self.position.segment = self
                    .position
                    .segment
                    .checked_add(1)
                    .ok_or(LogError::Capacity)?;
                self.position.byte = HEADER_LEN;
                self.active = create_segment(
                    &self.directory,
                    &self.options,
                    self.position,
                    self.position.checksum,
                )?;
            }
            let sequence = self
                .position
                .sequence
                .checked_add(1)
                .ok_or(LogError::Capacity)?;
            // A fixed-size stack header (length word u32, sequence u64, previous CRC
            // u32, then this frame's CRC u32) avoids a per-record heap allocation; the
            // byte layout is identical to the frozen frame format.
            let mut header = [0u8; FRAME_HEADER];
            header[0..4].copy_from_slice(&field.to_le_bytes());
            header[4..12].copy_from_slice(&sequence.to_le_bytes());
            header[12..16].copy_from_slice(&self.position.checksum.to_le_bytes());
            let mut hash = crc32fast::Hasher::new();
            hash.update(&header[..16]);
            hash.update(data);
            let checksum = hash.finalize();
            header[16..20].copy_from_slice(&checksum.to_le_bytes());
            let location = FrameLocation {
                generation: self.position.generation,
                segment: self.position.segment,
                byte: self.position.byte,
                length: data.len(),
                sequence,
                origin: sequence,
                previous: self.position.checksum,
                checksum,
            };
            self.active.write_all(&header)?;
            self.active.write_all(data)?;
            self.position.byte = self
                .position
                .byte
                .checked_add(frame_bytes)
                .ok_or(LogError::Capacity)?;
            self.position.sequence = sequence;
            self.position.checksum = checksum;
            Ok(location)
        }
    }

    /// Closes the group commit (doc 28): the batch's data is flushed, then a commit frame carrying the base is
    /// written and flushed, so a commit on disk proves its batch's data reached the disk before it. The fence
    /// is installed only when it is due: a new generation, or a base past the fenced base's segment (whose
    /// segments may then be removed).
    fn finish_append(&mut self) -> Result<DurablePosition, LogError> {
        self.fail_at(FaultPoint::AfterAppend)?;
        self.active.sync_data()?;
        self.fail_at(FaultPoint::AfterDataSync)?;
        let commit = self.write_commit()?;
        self.active.sync_all()?;
        self.last_commit = Some(commit);
        if self.fence_due() {
            self.install_fence_here()?;
        }
        self.fail_at(FaultPoint::AfterFenceInstall)?;
        Ok(self.position)
    }

    /// Whether the fence must move: a generation it does not name, or a base past its base's segment.
    fn fence_due(&self) -> bool {
        self.position.generation != self.fence_position.generation
            || self.base.segment > self.fence_base.segment
    }

    /// Installs the fence at the current position and base.
    fn install_fence_here(&mut self) -> Result<(), LogError> {
        install_fence(
            &self.fence,
            &self.directory,
            &self.options,
            self.position,
            self.base,
        )?;
        self.fence_position = self.position;
        self.fence_base = self.base;
        Ok(())
    }

    /// The commit frame the last group commit wrote, taken once (the shared writer counts its bytes).
    pub(crate) fn take_commit(&mut self) -> Option<FrameLocation> {
        self.last_commit.take()
    }
}

fn corrupt(path: &Path, offset: u64, reason: &'static str) -> LogError {
    LogError::Corruption {
        path: path.to_path_buf(),
        offset,
        reason,
    }
}

fn segment_path(directory: &Path, generation: u64, segment: u64) -> PathBuf {
    directory.join(format!("wal-{generation:020}-{segment:020}.seg"))
}

fn sync_dir(directory: &Path) -> Result<(), LogError> {
    focal_platform::sync_dir(directory)?;
    Ok(())
}

/// Durably install every newly created path component, including a new data dir.
pub fn create_durable_directory(path: &Path) -> Result<(), LogError> {
    if path.as_os_str().is_empty() {
        return Err(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "empty WAL directory").into(),
        );
    }
    let mut missing = Vec::new();
    for ancestor in path.ancestors().take_while(|p| !p.as_os_str().is_empty()) {
        if ancestor.is_dir() {
            break;
        }
        missing.push(ancestor);
    }
    for directory in missing.into_iter().rev() {
        match fs::create_dir(directory) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && directory.is_dir() => {}
            Err(e) => return Err(e.into()),
        }
        sync_dir(directory)?;
        if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

fn create_segment(
    directory: &Path,
    options: &WalOptions,
    p: DurablePosition,
    previous: u32,
) -> Result<File, LogError> {
    let mut header = Vec::with_capacity(HEADER_LEN as usize);
    header.extend_from_slice(MAGIC);
    // Every segment written now ends its group commits with commit frames.
    header.extend_from_slice(&SEGMENT_COMMITTED.to_le_bytes());
    header.extend_from_slice(&options.identity.cluster);
    header.extend_from_slice(&options.identity.node.to_le_bytes());
    header.extend_from_slice(&options.identity.stream.to_le_bytes());
    header.extend_from_slice(&p.generation.to_le_bytes());
    header.extend_from_slice(&p.segment.to_le_bytes());
    header.extend_from_slice(&p.sequence.to_le_bytes());
    header.extend_from_slice(&previous.to_le_bytes());
    header.extend_from_slice(&crc32fast::hash(&header).to_le_bytes());
    if header.len() != HEADER_LEN as usize {
        return Err(LogError::Capacity);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(segment_path(directory, p.generation, p.segment))?;
    file.write_all(&header)?;
    file.sync_all()?;
    sync_dir(directory)?;
    Ok(file)
}

/// The base a prefix starting at `segment` has: what the segment's own
/// header says came before its first frame.
fn segment_base(
    directory: &Path,
    options: &WalOptions,
    generation: u64,
    segment: u64,
) -> Result<DurableBase, LogError> {
    let path = segment_path(directory, generation, segment);
    let mut file = File::open(&path)?;
    let mut header = [0u8; HEADER_LEN as usize];
    file.read_exact(&mut header)?;
    segment_version(&header, options, generation, segment)
        .ok_or_else(|| corrupt(&path, 0, "segment header mismatch"))?;
    Ok(DurableBase {
        segment,
        byte: HEADER_LEN,
        sequence: read_u64(&header, 56..64)?,
        checksum: read_u32(&header, 64..68)?,
    })
}

/// The format version of a segment header that names this log's identity, generation and segment and
/// checks out; `None` for any other header.
fn segment_version(
    header: &[u8; HEADER_LEN as usize],
    options: &WalOptions,
    generation: u64,
    segment: u64,
) -> Option<u32> {
    let version = read_u32(header, 8..12).ok()?;
    let matches = header.get(..8) == Some(MAGIC.as_slice())
        && (version == SEGMENT_FENCED || version == SEGMENT_COMMITTED)
        && header.get(12..28) == Some(options.identity.cluster.as_slice())
        && read_u64(header, 28..36).ok()? == options.identity.node
        && read_u32(header, 36..40).ok()? == options.identity.stream
        && read_u64(header, 40..48).ok()? == generation
        && read_u64(header, 48..56).ok()? == segment
        && read_u32(header, 68..72).ok()? == crc32fast::hash(header.get(..68)?);
    matches.then_some(version)
}

/// The fence's file and the file it is staged in before the atomic replace.
#[derive(Debug)]
struct FencePaths {
    current: PathBuf,
    temp: PathBuf,
}

impl FencePaths {
    fn of(directory: &Path) -> FencePaths {
        FencePaths {
            current: directory.join("CURRENT"),
            temp: directory.join("CURRENT.tmp"),
        }
    }
}

/// Derived: the longest postcard encoding of a [`Fence`]. Varints take at most 5 bytes for a `u32` and 10 for
/// a `u64`, and a `[u8; 16]` is its 16 bytes: the version (5), the identity (16 + 10 + 5), the position
/// (4 × 10 + 5) and the base (3 × 10 + 5).
const FENCE_BODY_MAX: usize = 5 + (16 + 10 + 5) + (4 * 10 + 5) + (3 * 10 + 5);
/// Derived: the fence file at its longest: the magic, the body and its CRC-32.
const FENCE_FILE_MAX: usize = FENCE_MAGIC.len() + FENCE_BODY_MAX + 4;

/// Installs the durability fence: the magic, the encoded fence and its CRC-32, built in one stack buffer and
/// written at once, then synced and atomically replaced (the bytes are as before; the encoding no longer
/// grows a heap buffer a byte at a time, and the file takes one write instead of three).
fn install_fence(
    paths: &FencePaths,
    directory: &Path,
    options: &WalOptions,
    position: DurablePosition,
    base: DurableBase,
) -> Result<(), LogError> {
    let mut file_bytes = [0u8; FENCE_FILE_MAX];
    let (magic, rest) = file_bytes.split_at_mut(FENCE_MAGIC.len());
    magic.copy_from_slice(FENCE_MAGIC);
    let body_len = postcard::to_slice(
        &Fence {
            version: FENCE_VERSION,
            identity: options.identity,
            position,
            base,
        },
        rest,
    )?
    .len();
    let crc_at = FENCE_MAGIC.len().saturating_add(body_len);
    let checksum = crc32fast::hash(file_bytes.get(FENCE_MAGIC.len()..crc_at).unwrap_or(&[]));
    let end = crc_at.saturating_add(4);
    file_bytes
        .get_mut(crc_at..end)
        .ok_or(LogError::Capacity)?
        .copy_from_slice(&checksum.to_le_bytes());
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&paths.temp)?;
    file.write_all(file_bytes.get(..end).ok_or(LogError::Capacity)?)?;
    file.sync_all()?;
    focal_platform::fs::atomic_replace(&paths.temp, &paths.current)?;
    sync_dir(directory)
}

/// The fence's payload, its magic, length and checksum checked: what every version's fields are read from.
fn fence_payload(path: &Path) -> Result<Vec<u8>, LogError> {
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_FENCE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if !(12..=MAX_FENCE_BYTES as usize).contains(&bytes.len()) {
        return Err(corrupt(path, 0, "invalid fence length"));
    }
    if bytes.get(..8) != Some(FENCE_MAGIC.as_slice()) {
        return Err(corrupt(path, 0, "invalid fence magic"));
    }
    let end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| corrupt(path, 0, "invalid fence length"))?;
    let (prefix, checksum) = bytes
        .split_at_checked(end)
        .ok_or_else(|| corrupt(path, 0, "invalid fence length"))?;
    let checksum = u32::from_le_bytes(
        checksum
            .try_into()
            .map_err(|_| corrupt(path, 0, "invalid fence checksum"))?,
    );
    let payload = prefix
        .get(8..)
        .ok_or_else(|| corrupt(path, 0, "invalid fence payload"))?;
    if crc32fast::hash(payload) != checksum {
        return Err(corrupt(path, 0, "fence checksum mismatch"));
    }
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(payload.len())
        .map_err(|_| LogError::Capacity)?;
    owned.extend_from_slice(payload);
    Ok(owned)
}

fn read_fence(path: &Path) -> Result<Fence, LogError> {
    let bytes = fence_payload(path)?;
    let payload = bytes.as_slice();
    let (version, _) = postcard::take_from_bytes::<u32>(payload)
        .map_err(|_| corrupt(path, 0, "invalid fence payload"))?;
    if version == 1 {
        // A version 1 fence names a prefix that starts at segment zero.
        let legacy: FenceV1 =
            postcard::from_bytes(payload).map_err(|_| corrupt(path, 0, "invalid fence payload"))?;
        if legacy.version != 1 {
            return Err(corrupt(path, 0, "invalid fence payload"));
        }
        return Ok(Fence {
            version: FENCE_VERSION,
            identity: legacy.identity,
            position: legacy.position,
            base: DurableBase::default(),
        });
    }
    postcard::from_bytes(payload).map_err(|_| corrupt(path, 0, "invalid fence payload"))
}

#[derive(Clone, Copy)]
struct FrameLocation {
    generation: u64,
    segment: u64,
    byte: u64,
    length: usize,
    sequence: u64,
    /// The sequence the frame's record was first written at: its own,
    /// unless the frame is a moved one.
    origin: u64,
    previous: u32,
    checksum: u32,
}

/// The longest frame a stream of these options holds: a record at its
/// bound, inside the wrapper of a moved frame.
fn frame_limit(options: &WalOptions) -> usize {
    options.max_record_bytes.saturating_add(MOVED_OVERHEAD)
}

/// Follows the chain past the fence (doc 28): every frame from the fenced position on, through the last
/// commit frame of segments that have them. Answers the last commit's position and the base it carried,
/// or the fence's when no commit follows it. At the first frame that does not validate, the rest is a
/// torn tail only when the frame is torn as an interrupted write leaves it ([`torn`]) and no later segment
/// exists; anything else is corruption and fails closed (Alagappan et al., FAST 2018).
fn discover_commit(
    directory: &Path,
    options: &WalOptions,
    fence: DurablePosition,
    fence_base: DurableBase,
) -> Result<(DurablePosition, DurableBase), LogError> {
    let mut committed = (fence, fence_base);
    let generation = fence.generation;
    let mut segment = fence.segment;
    let mut sequence = fence.sequence;
    let mut previous = fence.checksum;
    let mut data = Vec::new();
    loop {
        let path = segment_path(directory, generation, segment);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            // The fenced segment is read by the strict scan, which says what is missing.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(committed),
            Err(error) => return Err(error.into()),
        };
        let end = file.metadata()?.len();
        let mut header = [0u8; HEADER_LEN as usize];
        if end < HEADER_LEN {
            // A segment whose creation was cut short: torn only as the last one.
            return later_segment(directory, generation, segment, &path, committed);
        }
        file.read_exact(&mut header)?;
        let Some(version) = segment_version(&header, options, generation, segment) else {
            return Err(corrupt(&path, 0, "segment header mismatch"));
        };
        if version == SEGMENT_FENCED {
            // Nothing past the fence is admitted in a segment without commit frames.
            return Ok(committed);
        }
        let mut offset = if segment == fence.segment {
            fence.byte
        } else {
            if read_u64(&header, 56..64)? != sequence || read_u32(&header, 64..68)? != previous {
                return Err(corrupt(&path, 0, "segment header or predecessor mismatch"));
            }
            HEADER_LEN
        };
        file.seek(SeekFrom::Start(offset))?;
        while offset < end {
            let remaining = end.checked_sub(offset).ok_or(LogError::Capacity)?;
            let mut frame = [0u8; FRAME_HEADER];
            if remaining < FRAME_HEADER as u64 {
                // A header the end of the file cut.
                return later_segment(directory, generation, segment, &path, committed);
            }
            file.read_exact(&mut frame)?;
            let field = read_u32(&frame, 0..4)?;
            let commit = field & COMMIT_FLAG != 0;
            let len = (field & !COMMIT_FLAG) as usize;
            let next = sequence.checked_add(1).ok_or(LogError::Capacity)?;
            let body_room = remaining.saturating_sub(FRAME_HEADER as u64);
            let shaped = (if commit {
                len == 0 || len == COMMIT_PAYLOAD
            } else {
                len <= frame_limit(options)
            }) && read_u64(&frame, 4..12)? == next
                && read_u32(&frame, 12..16)? == previous;
            let valid = shaped && len as u64 <= body_room && {
                data.clear();
                data.try_reserve_exact(len)
                    .map_err(|_| LogError::Capacity)?;
                data.resize(len, 0);
                file.read_exact(&mut data)?;
                let mut hash = crc32fast::Hasher::new();
                hash.update(frame.get(..16).ok_or(LogError::Capacity)?);
                hash.update(&data);
                hash.finalize() == read_u32(&frame, 16..20)?
            };
            if !valid {
                let torn = frame == [0u8; FRAME_HEADER]
                    || (shaped && len as u64 > body_room)
                    || (shaped && torn(&mut file, offset, len, end)?);
                // A commit is written only after its batch's data is flushed, so a commit past the damage
                // proves the damaged bytes had reached the disk: corruption, never a torn tail.
                if !torn || commits_after(&mut file, offset, end, sequence)? {
                    return Err(corrupt(&path, offset, "durable frame checksum mismatch"));
                }
                return later_segment(directory, generation, segment, &path, committed);
            }
            sequence = next;
            previous = read_u32(&frame, 16..20)?;
            offset = offset
                .checked_add(FRAME_HEADER as u64)
                .and_then(|value| value.checked_add(len as u64))
                .ok_or(LogError::Capacity)?;
            if commit {
                // A commit without a payload kept the base the last one named.
                let base = if len == 0 {
                    committed.1
                } else {
                    DurableBase {
                        segment: read_u64(&data, 0..8)?,
                        byte: read_u64(&data, 8..16)?,
                        sequence: read_u64(&data, 16..24)?,
                        checksum: read_u32(&data, 24..28)?,
                    }
                };
                if (base.segment, base.byte) < (committed.1.segment, committed.1.byte)
                    || base.sequence < committed.1.sequence
                    || base.sequence > sequence
                {
                    return Err(corrupt(&path, offset, "commit names a base out of order"));
                }
                committed = (
                    DurablePosition {
                        generation,
                        segment,
                        byte: offset,
                        sequence,
                        checksum: previous,
                    },
                    base,
                );
            }
        }
        segment = segment.checked_add(1).ok_or(LogError::Capacity)?;
    }
}

/// Whether a valid commit frame lies past `offset` in this segment with a sequence after `sequence`.
/// A commit frame verifies on its own: its checksum covers its header (length word, sequence, predecessor)
/// and its payload, so one is found at any byte offset without walking the chain the damage broke.
fn commits_after(file: &mut File, offset: u64, end: u64, sequence: u64) -> Result<bool, LogError> {
    /// Format: the commit flag's byte in a length word as it lies on disk (little-endian: its last byte).
    const FLAG_BYTE: u8 = (COMMIT_FLAG >> 24) as u8;
    /// Derived: a commit frame's bytes at most.
    const FRAME: usize = COMMIT_FRAME_BYTES;
    /// Shape: bytes read at once while searching; a frame straddling two reads is kept by overlap.
    const CHUNK: usize = 64 * 1024;
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(CHUNK)
        .map_err(|_| LogError::Capacity)?;
    buffer.resize(CHUNK, 0);
    let mut at = offset.saturating_add(1);
    while at < end {
        let want = usize::try_from(end.saturating_sub(at))
            .unwrap_or(usize::MAX)
            .min(CHUNK);
        let window = buffer.get_mut(..want).ok_or(LogError::Capacity)?;
        file.seek(SeekFrom::Start(at))?;
        file.read_exact(window)?;
        let mut index = 0usize;
        while let Some(candidate) = window.get(index..).and_then(|rest| {
            rest.windows(4)
                .position(|bytes| {
                    bytes.get(3) == Some(&FLAG_BYTE)
                        && read_u32(bytes, 0..4).is_ok_and(|word| {
                            word == COMMIT_FLAG || word == COMMIT_FLAG | COMMIT_PAYLOAD as u32
                        })
                })
                .map(|place| place.saturating_add(index))
        }) {
            let payload = window
                .get(candidate..candidate.saturating_add(4))
                .and_then(|word| read_u32(word, 0..4).ok())
                .map_or(0, |word| (word & !COMMIT_FLAG) as usize);
            let length = FRAME_HEADER.saturating_add(payload).min(FRAME);
            if let Some(frame) = window.get(candidate..candidate.saturating_add(length)) {
                let later = read_u64(frame, 4..12).is_ok_and(|found| found > sequence);
                let mut hash = crc32fast::Hasher::new();
                hash.update(frame.get(..16).unwrap_or(&[]));
                hash.update(frame.get(FRAME_HEADER..).unwrap_or(&[]));
                if later && read_u32(frame, 16..20).is_ok_and(|stored| stored == hash.finalize()) {
                    return Ok(true);
                }
            }
            index = candidate.saturating_add(1);
        }
        if want < CHUNK {
            break;
        }
        // Step back one frame less a byte so a commit frame cut by this read is read whole by the next.
        at = at.saturating_add((CHUNK - (FRAME - 1)) as u64);
    }
    Ok(false)
}

/// A torn tail ends the log only if no segment follows it: a segment is flushed whole before the next is
/// created, so damage followed by a segment is damage to flushed data.
fn later_segment(
    directory: &Path,
    generation: u64,
    segment: u64,
    path: &Path,
    committed: (DurablePosition, DurableBase),
) -> Result<(DurablePosition, DurableBase), LogError> {
    let next = segment.checked_add(1).ok_or(LogError::Capacity)?;
    if segment_path(directory, generation, next).exists() {
        return Err(corrupt(path, 0, "damaged segment followed by another"));
    }
    Ok(committed)
}

/// Whether the frame at `offset` of `len` body bytes holds a 512-byte sector that is all zero: what a write
/// cut short leaves, since a sector is written whole and unwritten sectors past the old end read as zero
/// (etcd's `isTornEntry`). Bit rot and misdirected writes leave non-zero bytes instead.
fn torn(file: &mut File, offset: u64, len: usize, end: u64) -> Result<bool, LogError> {
    let frame_end = offset
        .checked_add(FRAME_HEADER as u64)
        .and_then(|value| value.checked_add(len as u64))
        .ok_or(LogError::Capacity)?
        .min(end);
    let mut sector = offset.div_ceil(SECTOR).saturating_mul(SECTOR);
    let mut bytes = [0u8; SECTOR as usize];
    while sector
        .checked_add(SECTOR)
        .is_some_and(|stop| stop <= frame_end)
    {
        file.seek(SeekFrom::Start(sector))?;
        file.read_exact(&mut bytes)?;
        if bytes.iter().all(|byte| *byte == 0) {
            return Ok(true);
        }
        sector = sector.saturating_add(SECTOR);
    }
    Ok(false)
}

fn scan(
    directory: &Path,
    options: &WalOptions,
    base: DurableBase,
    fence: DurablePosition,
    mut visitor: impl FnMut(Record) -> Result<(), LogError>,
) -> Result<(), LogError> {
    scan_indexed(directory, options, base, fence, |record, _| visitor(record))
}

/// The floors a single-owner replay holds at once, one a logical log.
const MAX_REPLAY_FLOORS: usize = 65536;

/// What a frame says of itself before its payload: read from the record's
/// leading fields, owning nothing.
#[derive(Clone, Copy, Debug, Deserialize)]
pub(crate) struct FrameHeader {
    pub(crate) log: LogicalLogId,
    pub(crate) kind: RecordKind,
    pub(crate) index: u64,
    pub(crate) term: u64,
}
/// What an indexed open tells its visitor, in order: where the durable
/// prefix starts, each frame's header with its sequence, then that the
/// count is complete, then each frame's record.
pub(crate) enum ScanEvent {
    Base(DurableBase),
    Header(FrameHeader, u64),
    Counted,
    Frame(Record, FrameLocation),
}
/// Every durable record of the fenced prefix, decoded, with its location.
fn scan_indexed(
    directory: &Path,
    options: &WalOptions,
    base: DurableBase,
    fence: DurablePosition,
    mut visitor: impl FnMut(Record, FrameLocation) -> Result<(), LogError>,
) -> Result<(), LogError> {
    scan_frames(directory, options, base, fence, |bytes, location, path| {
        let (record, origin) = decode_frame(bytes)
            .map_err(|_| corrupt(path, location.byte, "invalid durable record"))?;
        visitor(
            record,
            FrameLocation {
                origin: origin.unwrap_or(location.sequence),
                ..location
            },
        )
    })
}
/// The header of every durable record of the fenced prefix, read from the
/// record's leading fields alone: what a recovery index counts, and learns
/// each group's floor from, before it packs its groups.
fn scan_headers(
    directory: &Path,
    options: &WalOptions,
    base: DurableBase,
    fence: DurablePosition,
    mut visitor: impl FnMut(FrameHeader, u64) -> Result<(), LogError>,
) -> Result<(), LogError> {
    scan_frames(directory, options, base, fence, |bytes, location, path| {
        let (header, _) = postcard::take_from_bytes::<FrameHeader>(bytes)
            .map_err(|_| corrupt(path, location.byte, "invalid durable record"))?;
        visitor(header, location.sequence)
    })
}
/// Every durable frame of the fenced prefix, in order, its bytes verified
/// (length, sequence, predecessor, checksum) and lent from one buffer that
/// grows to the largest record and no further.
fn scan_frames(
    directory: &Path,
    options: &WalOptions,
    base: DurableBase,
    fence: DurablePosition,
    mut on_frame: impl FnMut(&[u8], FrameLocation, &Path) -> Result<(), LogError>,
) -> Result<(), LogError> {
    if base.segment > fence.segment
        || base.sequence > fence.sequence
        || base.byte < HEADER_LEN
        || (base.segment == fence.segment && base.byte > fence.byte)
    {
        return Err(corrupt(
            &directory.join("CURRENT"),
            0,
            "fence base is beyond its position",
        ));
    }
    let mut sequence = base.sequence;
    let mut previous = base.checksum;
    let mut data = Vec::new();
    for segment in base.segment..=fence.segment {
        let path = segment_path(directory, fence.generation, segment);
        let mut file = File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                corrupt(&path, 0, "durable segment missing")
            } else {
                e.into()
            }
        })?;
        let end = if segment == fence.segment {
            fence.byte
        } else {
            file.metadata()?.len()
        };
        if end < HEADER_LEN || end > file.metadata()?.len() {
            return Err(corrupt(&path, end, "durable segment truncated"));
        }
        let mut header = [0u8; HEADER_LEN as usize];
        file.read_exact(&mut header)?;
        let version = segment_version(&header, options, fence.generation, segment)
            .ok_or_else(|| corrupt(&path, 0, "segment header or predecessor mismatch"))?;
        // A base inside its segment starts past that segment's first
        // frames: the chain state there is the fence's, not the header's.
        let inside = segment == base.segment && base.byte > HEADER_LEN;
        if !inside
            && (read_u64(&header, 56..64)? != sequence || read_u32(&header, 64..68)? != previous)
        {
            return Err(corrupt(&path, 0, "segment header or predecessor mismatch"));
        }
        let mut offset = if inside { base.byte } else { HEADER_LEN };
        if offset > end {
            return Err(corrupt(&path, offset, "fence base is beyond its segment"));
        }
        if inside {
            file.seek(SeekFrom::Start(offset))?;
        }
        while offset < end {
            let remaining = end.checked_sub(offset).ok_or(LogError::Capacity)?;
            if remaining < FRAME_HEADER as u64 {
                return Err(corrupt(&path, offset, "incomplete durable frame header"));
            }
            let mut header = [0u8; FRAME_HEADER];
            file.read_exact(&mut header)?;
            let field = read_u32(&header, 0..4)?;
            // A commit frame (doc 28) closes a group commit: it continues the chain and carries the base, and
            // is no record.
            let commit = version == SEGMENT_COMMITTED && field & COMMIT_FLAG != 0;
            let len = (field & !COMMIT_FLAG) as usize;
            if (commit && len != 0 && len != COMMIT_PAYLOAD)
                || (!commit && field & COMMIT_FLAG != 0)
                || len > frame_limit(options)
                || len as u64
                    > remaining
                        .checked_sub(FRAME_HEADER as u64)
                        .ok_or(LogError::Capacity)?
            {
                return Err(corrupt(&path, offset, "invalid durable frame length"));
            }
            let next = sequence.checked_add(1).ok_or(LogError::Capacity)?;
            if read_u64(&header, 4..12)? != next || read_u32(&header, 12..16)? != previous {
                return Err(corrupt(
                    &path,
                    offset,
                    "frame sequence or predecessor mismatch",
                ));
            }
            data.clear();
            data.try_reserve_exact(len)
                .map_err(|_| LogError::Capacity)?;
            data.resize(len, 0);
            file.read_exact(&mut data)?;
            let mut hash = crc32fast::Hasher::new();
            hash.update(header.get(..16).ok_or(LogError::Capacity)?);
            hash.update(&data);
            let checksum = hash.finalize();
            if checksum != read_u32(&header, 16..20)? {
                return Err(corrupt(&path, offset, "durable frame checksum mismatch"));
            }
            if !commit {
                on_frame(
                    &data,
                    FrameLocation {
                        generation: fence.generation,
                        segment,
                        byte: offset,
                        length: len,
                        sequence: next,
                        origin: next,
                        previous,
                        checksum,
                    },
                    &path,
                )?;
            }
            sequence = next;
            previous = checksum;
            offset = offset
                .checked_add(FRAME_HEADER as u64)
                .and_then(|value| value.checked_add(len as u64))
                .ok_or(LogError::Capacity)?;
        }
    }
    if sequence != fence.sequence || previous != fence.checksum {
        return Err(corrupt(
            &directory.join("CURRENT"),
            0,
            "fence does not match durable prefix",
        ));
    }
    Ok(())
}

fn decode_record(bytes: &[u8]) -> Result<Record, LogError> {
    #[derive(Deserialize)]
    struct Header {
        log: LogicalLogId,
        kind: RecordKind,
        index: u64,
        term: u64,
        length: usize,
    }
    let (header, payload) = postcard::take_from_bytes::<Header>(bytes)?;
    if payload.len() != header.length {
        return Err(LogError::Encoding(
            postcard::Error::DeserializeUnexpectedEnd,
        ));
    }
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(payload.len())
        .map_err(|_| LogError::Capacity)?;
    owned.extend_from_slice(payload);
    Ok(Record {
        log: header.log,
        kind: header.kind,
        index: header.index,
        term: header.term,
        payload: owned,
    })
}

/// A durable frame's record and, for a moved frame, the origin it carries:
/// the record a moved frame wraps is decoded from the frame's own bytes.
fn decode_frame(bytes: &[u8]) -> Result<(Record, Option<u64>), LogError> {
    let (header, payload) = postcard::take_from_bytes::<FrameHeader>(bytes)?;
    if header.kind != RecordKind::Moved {
        return Ok((decode_record(bytes)?, None));
    }
    let (length, inner) = postcard::take_from_bytes::<usize>(payload)?;
    if inner.len() != length {
        return Err(LogError::Encoding(
            postcard::Error::DeserializeUnexpectedEnd,
        ));
    }
    let record = decode_record(inner)?;
    if record.log != header.log || matches!(record.kind, RecordKind::Moved | RecordKind::Floor) {
        return Err(LogError::Encoding(postcard::Error::DeserializeBadEnum));
    }
    Ok((record, Some(header.index)))
}
/// The frame a live record is written again as: the wrapper that carries
/// its origin, around the record's bytes as they were first encoded.
fn moved_frame(log: LogicalLogId, origin: u64, record: &[u8]) -> Result<Vec<u8>, LogError> {
    #[derive(Serialize)]
    struct Wrapper {
        log: LogicalLogId,
        kind: RecordKind,
        index: u64,
        term: u64,
        length: usize,
    }
    let wrapper = Wrapper {
        log,
        kind: RecordKind::Moved,
        index: origin,
        term: 0,
        length: record.len(),
    };
    let head = postcard::experimental::serialized_size(&wrapper)?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(head.checked_add(record.len()).ok_or(LogError::Capacity)?)
        .map_err(|_| LogError::Capacity)?;
    frame.resize(head, 0);
    postcard::to_slice(&wrapper, &mut frame)?;
    frame.extend_from_slice(record);
    Ok(frame)
}

fn read_u64(bytes: &[u8], range: std::ops::Range<usize>) -> Result<u64, LogError> {
    let value = bytes
        .get(range)
        .and_then(|part| part.try_into().ok())
        .ok_or(LogError::Capacity)?;
    Ok(u64::from_le_bytes(value))
}
fn read_u32(bytes: &[u8], range: std::ops::Range<usize>) -> Result<u32, LogError> {
    let value = bytes
        .get(range)
        .and_then(|part| part.try_into().ok())
        .ok_or(LogError::Capacity)?;
    Ok(u32::from_le_bytes(value))
}

/// Remove every segment file that is not of the fenced prefix: another
/// generation's, one beyond the position, one before the base.
fn cleanup_segments(
    directory: &Path,
    base: DurableBase,
    p: DurablePosition,
) -> Result<(), LogError> {
    let mut changed = false;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(rest) = name
            .strip_prefix("wal-")
            .and_then(|n| n.strip_suffix(".seg"))
        else {
            continue;
        };
        let Some((generation, segment)) = rest.split_once('-') else {
            continue;
        };
        let (Ok(generation), Ok(segment)) = (generation.parse::<u64>(), segment.parse::<u64>())
        else {
            continue;
        };
        if generation != p.generation || segment > p.segment || segment < base.segment {
            fs::remove_file(entry.path())?;
            changed = true;
        }
    }
    if changed {
        sync_dir(directory)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fence written from a stack buffer is byte for byte the fence as it was written from a heap encode:
    /// the magic, postcard's encoding and its CRC-32. A reader of either reads the other.
    #[test]
    fn a_fence_is_written_as_before() {
        let dir = tempfile::tempdir().unwrap();
        let options = WalOptions::new(WalIdentity {
            cluster: [7; 16],
            node: 3,
            stream: 9,
        });
        let position = DurablePosition {
            generation: 2,
            segment: 5,
            byte: 4_096,
            sequence: 77,
            checksum: 0xDEAD_BEEF,
        };
        let base = DurableBase {
            segment: 1,
            byte: HEADER_LEN,
            sequence: 3,
            checksum: 9,
        };
        install_fence(
            &FencePaths::of(dir.path()),
            dir.path(),
            &options,
            position,
            base,
        )
        .unwrap();
        let body = postcard::to_stdvec(&Fence {
            version: FENCE_VERSION,
            identity: options.identity,
            position,
            base,
        })
        .unwrap();
        let mut expected = FENCE_MAGIC.to_vec();
        expected.extend_from_slice(&body);
        expected.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        assert_eq!(std::fs::read(dir.path().join("CURRENT")).unwrap(), expected);
        let read = read_fence(&dir.path().join("CURRENT")).unwrap();
        assert_eq!((read.position, read.base), (position, base));
    }

    /// The derived bound holds: a fence with every field at its longest encoding fits the stack buffer.
    #[test]
    fn the_longest_fence_fits_its_bound() {
        let longest = Fence {
            version: u32::MAX,
            identity: WalIdentity {
                cluster: [u8::MAX; 16],
                node: u64::MAX,
                stream: u32::MAX,
            },
            position: DurablePosition {
                generation: u64::MAX,
                segment: u64::MAX,
                byte: u64::MAX,
                sequence: u64::MAX,
                checksum: u32::MAX,
            },
            base: DurableBase {
                segment: u64::MAX,
                byte: u64::MAX,
                sequence: u64::MAX,
                checksum: u32::MAX,
            },
        };
        assert_eq!(postcard::to_stdvec(&longest).unwrap().len(), FENCE_BODY_MAX);
    }
    #[test]
    fn appended_transition_preserves_all_floor_era_ordinals_and_refuses_its_decoder() {
        #[derive(Serialize, Deserialize)]
        enum FloorEraKind {
            Entry,
            HardState,
            Configuration,
            Snapshot,
            Identity,
            Checkpoint,
            DecoderFloor,
        }
        #[derive(Serialize, Deserialize)]
        struct FloorEraRecord {
            log: LogicalLogId,
            kind: FloorEraKind,
            index: u64,
            term: u64,
            #[serde(with = "focal_memory::serde_bytes")]
            payload: Vec<u8>,
        }
        for (kind, old_kind) in [
            (RecordKind::Entry, FloorEraKind::Entry),
            (RecordKind::HardState, FloorEraKind::HardState),
            (RecordKind::Configuration, FloorEraKind::Configuration),
            (RecordKind::Snapshot, FloorEraKind::Snapshot),
            (RecordKind::Identity, FloorEraKind::Identity),
            (RecordKind::Checkpoint, FloorEraKind::Checkpoint),
            (RecordKind::DecoderFloor, FloorEraKind::DecoderFloor),
        ] {
            let current = Record {
                log: LogicalLogId([1; 16]),
                kind,
                index: 3,
                term: 2,
                payload: vec![4, 5],
            };
            let old = FloorEraRecord {
                log: current.log,
                kind: old_kind,
                index: current.index,
                term: current.term,
                payload: current.payload.clone(),
            };
            assert_eq!(
                postcard::to_allocvec(&current).unwrap(),
                postcard::to_allocvec(&old).unwrap()
            );
        }
        let mut payload = b"FOCALDT1".to_vec();
        payload.extend_from_slice(&[0, 1]);
        payload.extend_from_slice(&[41; 32]);
        payload.extend_from_slice(&[42; 32]);
        let transition = Record {
            log: LogicalLogId([1; 16]),
            kind: RecordKind::DecoderTransition,
            index: 0,
            term: 0,
            payload,
        };
        let encoded = postcard::to_allocvec(&transition).unwrap();
        let mut expected = vec![1; 16];
        expected.extend_from_slice(&[7, 0, 0, 74]);
        expected.extend_from_slice(b"FOCALDT1");
        expected.extend_from_slice(&[0, 1]);
        expected.extend_from_slice(&[41; 32]);
        expected.extend_from_slice(&[42; 32]);
        assert_eq!(encoded, expected);
        assert!(postcard::from_bytes::<FloorEraRecord>(&encoded).is_err());
        assert_eq!(
            postcard::from_bytes::<Record>(&encoded).unwrap(),
            transition
        );
    }
    #[test]
    fn appended_fast_track_kinds_preserve_every_ordinal_before_them_and_refuse_older_decoders() {
        #[derive(Serialize, Deserialize)]
        enum TransitionEraKind {
            Entry,
            HardState,
            Configuration,
            Snapshot,
            Identity,
            Checkpoint,
            DecoderFloor,
            DecoderTransition,
        }
        #[derive(Serialize, Deserialize)]
        struct TransitionEraRecord {
            log: LogicalLogId,
            kind: TransitionEraKind,
            index: u64,
            term: u64,
            #[serde(with = "focal_memory::serde_bytes")]
            payload: Vec<u8>,
        }
        for (kind, old_kind) in [
            (RecordKind::Entry, TransitionEraKind::Entry),
            (RecordKind::HardState, TransitionEraKind::HardState),
            (RecordKind::Configuration, TransitionEraKind::Configuration),
            (RecordKind::Snapshot, TransitionEraKind::Snapshot),
            (RecordKind::Identity, TransitionEraKind::Identity),
            (RecordKind::Checkpoint, TransitionEraKind::Checkpoint),
            (RecordKind::DecoderFloor, TransitionEraKind::DecoderFloor),
            (
                RecordKind::DecoderTransition,
                TransitionEraKind::DecoderTransition,
            ),
        ] {
            let current = Record {
                log: LogicalLogId([1; 16]),
                kind,
                index: 3,
                term: 2,
                payload: vec![4, 5],
            };
            let old = TransitionEraRecord {
                log: current.log,
                kind: old_kind,
                index: current.index,
                term: current.term,
                payload: current.payload.clone(),
            };
            assert_eq!(
                postcard::to_allocvec(&current).unwrap(),
                postcard::to_allocvec(&old).unwrap()
            );
        }
        for (kind, ordinal) in [
            (RecordKind::FastTrack, 8u8),
            (RecordKind::Proposal, 9),
            (RecordKind::Released, 12),
        ] {
            let record = Record {
                log: LogicalLogId([1; 16]),
                kind,
                index: 5,
                term: 2,
                payload: vec![7, 7],
            };
            let encoded = postcard::to_allocvec(&record).unwrap();
            let mut expected = vec![1; 16];
            expected.extend_from_slice(&[ordinal, 5, 2, 2, 7, 7]);
            assert_eq!(encoded, expected);
            assert!(postcard::from_bytes::<TransitionEraRecord>(&encoded).is_err());
            assert_eq!(postcard::from_bytes::<Record>(&encoded).unwrap(), record);
        }
    }
    #[test]
    fn appended_decoder_floor_refuses_frozen_old_decoder_and_preserves_legacy_bytes() {
        #[derive(Serialize, Deserialize)]
        enum PreviousKind {
            Entry,
            HardState,
            Configuration,
            Snapshot,
            Identity,
            Checkpoint,
        }
        #[derive(Serialize, Deserialize)]
        struct PreviousRecord {
            log: LogicalLogId,
            kind: PreviousKind,
            index: u64,
            term: u64,
            #[serde(with = "focal_memory::serde_bytes")]
            payload: Vec<u8>,
        }
        for (kind, previous) in [
            (RecordKind::Entry, PreviousKind::Entry),
            (RecordKind::HardState, PreviousKind::HardState),
            (RecordKind::Configuration, PreviousKind::Configuration),
            (RecordKind::Snapshot, PreviousKind::Snapshot),
            (RecordKind::Identity, PreviousKind::Identity),
            (RecordKind::Checkpoint, PreviousKind::Checkpoint),
        ] {
            let current = Record {
                log: LogicalLogId([1; 16]),
                kind,
                index: 3,
                term: 2,
                payload: vec![4, 5],
            };
            let old = PreviousRecord {
                log: current.log,
                kind: previous,
                index: current.index,
                term: current.term,
                payload: current.payload.clone(),
            };
            assert_eq!(
                postcard::to_allocvec(&current).unwrap(),
                postcard::to_allocvec(&old).unwrap()
            );
        }
        let floor = Record {
            log: LogicalLogId([1; 16]),
            kind: RecordKind::DecoderFloor,
            index: 0,
            term: 0,
            payload: b"FOCALDF1".iter().copied().chain([41; 32]).collect(),
        };
        let encoded = postcard::to_allocvec(&floor).unwrap();
        let mut expected = vec![1; 16];
        expected.extend_from_slice(&[6, 0, 0, 40]);
        expected.extend_from_slice(b"FOCALDF1");
        expected.extend_from_slice(&[41; 32]);
        assert_eq!(encoded, expected);
        assert!(postcard::from_bytes::<PreviousRecord>(&encoded).is_err());
        assert_eq!(postcard::from_bytes::<Record>(&encoded).unwrap(), floor);
    }

    fn options() -> WalOptions {
        let mut o = WalOptions::new(WalIdentity {
            cluster: [7; 16],
            node: 1,
            stream: 0,
        });
        o.segment_bytes = 256;
        o
    }
    fn record(log: u8, index: u64) -> Record {
        Record {
            log: LogicalLogId([log; 16]),
            kind: RecordKind::Entry,
            index,
            term: 3,
            payload: vec![42; 64],
        }
    }
    #[test]
    fn multiplex_rotation_recovery_and_lock() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), options()).unwrap();
        assert!(matches!(
            Wal::open(dir.path(), options()),
            Err(LogError::Locked)
        ));
        let expected = vec![record(1, 1), record(2, 1), record(1, 2), record(2, 2)];
        wal.append(&expected).unwrap();
        assert!(wal.position.segment > 0);
        drop(wal);
        let wal = Wal::open(dir.path(), options()).unwrap();
        let mut actual = Vec::new();
        wal.replay(|r| {
            actual.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(actual, expected);
    }
    #[test]
    fn failed_append_is_not_acknowledged_and_poisoned() {
        for point in [
            FaultPoint::AfterAppend,
            FaultPoint::AfterDataSync,
            FaultPoint::AfterFenceInstall,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut wal = Wal::open(dir.path(), options()).unwrap();
            wal.append(&[record(1, 1)]).unwrap();
            wal.inject_fault_once(point);
            assert!(wal.append(&[record(1, 2)]).is_err());
            assert!(matches!(wal.append(&[record(1, 3)]), Err(LogError::Failed)));
            drop(wal);
            let wal = Wal::open(dir.path(), options()).unwrap();
            let mut records = Vec::new();
            wal.replay(|r| {
                records.push(r);
                Ok(())
            })
            .unwrap();
            assert_eq!(records[0], record(1, 1));
            // A batch whose commit frame was flushed is durable (doc 28); one cut before its commit frame
            // was written is not.
            assert_eq!(
                records.len(),
                if point == FaultPoint::AfterFenceInstall {
                    2
                } else {
                    1
                }
            );
        }
    }
    /// Options with segments large enough to hold whole 512-byte sectors of frames.
    fn wide() -> WalOptions {
        let mut o = options();
        o.segment_bytes = 1 << 20;
        o
    }

    fn replayed(dir: &Path, options: WalOptions) -> Result<Vec<Record>, LogError> {
        let wal = Wal::open(dir, options)?;
        let mut records = Vec::new();
        wal.replay(|r| {
            records.push(r);
            Ok(())
        })?;
        Ok(records)
    }

    fn damage(dir: &Path, p: DurablePosition, at: u64, bytes: &[u8]) {
        let mut f = OpenOptions::new()
            .write(true)
            .open(segment_path(dir, p.generation, p.segment))
            .unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }

    /// Doc 28: a crash may cut the last group commit anywhere. Do: cut the file at every byte of the last
    /// batch. Expect: recovery finds the commit before it, with every earlier record, and appends again.
    #[test]
    fn a_cut_anywhere_in_the_last_batch_recovers_the_commit_before_it() {
        let template = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(template.path(), wide()).unwrap();
        wal.append(&[record(1, 1), record(1, 2)]).unwrap();
        let before = wal.position();
        wal.append(&[record(1, 3), record(1, 4)]).unwrap();
        let after = wal.position();
        drop(wal);
        assert_eq!(
            before.segment, after.segment,
            "the test's batches share a segment"
        );
        for cut in before.byte..after.byte {
            let dir = tempfile::tempdir().unwrap();
            for entry in std::fs::read_dir(template.path()).unwrap() {
                let entry = entry.unwrap();
                if entry.file_name() != "LOCK" {
                    std::fs::copy(entry.path(), dir.path().join(entry.file_name())).unwrap();
                }
            }
            OpenOptions::new()
                .write(true)
                .open(segment_path(dir.path(), after.generation, after.segment))
                .unwrap()
                .set_len(cut)
                .unwrap();
            let records = replayed(dir.path(), wide()).unwrap();
            assert_eq!(records, vec![record(1, 1), record(1, 2)], "cut at {cut}");
            let mut wal = Wal::open(dir.path(), wide()).unwrap();
            wal.append(&[record(1, 5)]).unwrap();
        }
    }

    /// A damaged frame that a crash cannot leave fails closed: non-zero bytes changed in a batch committed
    /// before the last, a zeroed sector with commits after it (in the last batch too: its commit was
    /// written only after its data was flushed, so the sector was lost after acknowledgement), damage
    /// before the fence, and garbage past the last commit.
    #[test]
    fn damage_a_crash_cannot_leave_fails_closed() {
        let corrupt = |result: Result<Vec<Record>, LogError>| {
            assert!(
                matches!(result, Err(LogError::Corruption { .. })),
                "{result:?}"
            );
        };
        let big = |index: u64| Record {
            payload: vec![42; 1500],
            ..record(1, index)
        };
        // A flipped byte in a committed batch with commits after it.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        let start = wal.position();
        wal.append(&[big(1)]).unwrap();
        wal.append(&[big(2)]).unwrap();
        wal.append(&[big(3)]).unwrap();
        let p = wal.position();
        drop(wal);
        damage(dir.path(), p, start.byte + 100, &[0x55]);
        corrupt(replayed(dir.path(), wide()));
        // A zeroed sector in an early batch, two commits after it.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        wal.append(&[big(1)]).unwrap();
        wal.append(&[big(2)]).unwrap();
        wal.append(&[big(3)]).unwrap();
        let p = wal.position();
        drop(wal);
        damage(dir.path(), p, 512, &[0; 512]);
        corrupt(replayed(dir.path(), wide()));
        // A zeroed sector in the last batch, its own commit after it: a write lost after it was acknowledged.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        wal.append(&[big(1)]).unwrap();
        let last = wal.position();
        wal.append(&[big(2)]).unwrap();
        let p = wal.position();
        drop(wal);
        let sector = (last.byte + FRAME_HEADER as u64).div_ceil(512) * 512;
        assert!(sector + 512 <= last.byte + (FRAME_HEADER + 1500) as u64);
        damage(dir.path(), p, sector, &[0; 512]);
        corrupt(replayed(dir.path(), wide()));
        // Damage before the fence: the reopen fenced the batch.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        let start = wal.position();
        wal.append(&[record(1, 1)]).unwrap();
        drop(wal);
        let wal = Wal::open(dir.path(), wide()).unwrap();
        let p = wal.position();
        assert_eq!(read_fence(&dir.path().join("CURRENT")).unwrap().position, p);
        drop(wal);
        damage(dir.path(), p, start.byte + 30, &[0]);
        corrupt(replayed(dir.path(), wide()));
        // Garbage past the last commit.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        wal.append(&[record(1, 1)]).unwrap();
        let p = wal.position();
        drop(wal);
        damage(dir.path(), p, p.byte, &[0x5A; 64]);
        corrupt(replayed(dir.path(), wide()));
    }

    /// Doc 28 §3: a log written before commit frames keeps its records and appends after the upgrade. Do:
    /// write a version-1 segment with frames and a fence as the old format did, open it, append, reopen.
    /// Expect: every record, the old segment untouched at version 1, and the appends in a version-2 one.
    #[test]
    fn a_fenced_log_upgrades_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        let p = wal.position();
        // Rewrite the segment's header as version 1.
        let path = segment_path(dir.path(), p.generation, p.segment);
        let mut header = std::fs::read(&path).unwrap();
        header[8..12].copy_from_slice(&SEGMENT_FENCED.to_le_bytes());
        let crc = crc32fast::hash(&header[..68]);
        header[68..72].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &header).unwrap();
        wal.active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        wal.active.seek(SeekFrom::Start(p.byte)).unwrap();
        // Frames as version 1 wrote them: records, then the fence; no commit frame.
        let encoded = wal.encode_batch(&[record(1, 1), record(1, 2)]).unwrap();
        wal.write_encoded(&encoded).unwrap();
        wal.active.sync_all().unwrap();
        install_fence(&wal.fence, dir.path(), &wal.options, wal.position, wal.base).unwrap();
        drop(wal);
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        assert_eq!(
            wal.position().segment,
            p.segment + 1,
            "rolled to a new segment"
        );
        wal.append(&[record(1, 3)]).unwrap();
        drop(wal);
        assert_eq!(
            replayed(dir.path(), wide()).unwrap(),
            vec![record(1, 1), record(1, 2), record(1, 3)]
        );
        let old = std::fs::read(&path).unwrap();
        assert_eq!(read_u32(&old, 8..12).unwrap(), SEGMENT_FENCED);
        let new = std::fs::read(segment_path(dir.path(), p.generation, p.segment + 1)).unwrap();
        assert_eq!(read_u32(&new, 8..12).unwrap(), SEGMENT_COMMITTED);
    }

    /// Doc 28: a group commit takes one flush and no fence while the base stays in its segment. Do: append
    /// batches. Expect: the fence still names the position the open fenced.
    #[test]
    fn a_commit_installs_no_fence_until_one_is_due() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), wide()).unwrap();
        let fenced = read_fence(&dir.path().join("CURRENT")).unwrap().position;
        for index in 1..=10 {
            wal.append(&[record(1, index)]).unwrap();
        }
        assert_eq!(
            read_fence(&dir.path().join("CURRENT")).unwrap().position,
            fenced
        );
        assert!(wal.position().sequence > fenced.sequence);
    }
    #[test]
    fn checkpoint_generation_retains_supplied_recovery_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), options()).unwrap();
        wal.append(&[record(1, 1), record(1, 2)]).unwrap();
        let mut snapshot = record(1, 2);
        snapshot.kind = RecordKind::Snapshot;
        wal.rewrite_checkpoint(&[snapshot.clone(), record(1, 3)])
            .unwrap();
        drop(wal);
        let wal = Wal::open(dir.path(), options()).unwrap();
        let mut actual = Vec::new();
        wal.replay(|r| {
            actual.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(actual, vec![snapshot, record(1, 3)]);
    }
    #[test]
    fn malformed_fence_lengths_return_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-fence");
        for length in (0..12).chain([MAX_FENCE_BYTES as usize + 1]) {
            fs::write(&path, vec![0; length]).unwrap();
            assert!(matches!(
                read_fence(&path),
                Err(LogError::Corruption { .. })
            ));
        }
    }
}
