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
//! The checksummed `CURRENT` file is a durability fence, not an application redo
//! log. Appends flush segment data before atomically installing and flushing that
//! fence. Recovery therefore never guesses whether a damaged frame was acknowledged:
//! every byte before the fence must validate; only the suffix after it is discarded.
//! A failed write permanently poisons the writer until it is reopened and recovered.
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub log: LogicalLogId,
    pub kind: RecordKind,
    pub index: u64,
    pub term: u64,
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

#[derive(Serialize, Deserialize)]
struct Fence {
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
}

pub struct Wal {
    directory: PathBuf,
    options: WalOptions,
    _lock: File,
    active: File,
    position: DurablePosition,
    failed: bool,
    fault: Option<FaultPoint>,
}

mod writer;
#[cfg(feature = "test-support")]
pub use writer::WalPause;
pub use writer::{SharedWal, WalAppend, WalLease, WalWriterId, WalWriterLimits, WalWriterStats};

/// Faults are injected at actual durability boundaries for crash-model tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    AfterAppend,
    AfterDataSync,
    AfterFenceInstall,
}

impl Wal {
    /// Open an exclusively owned stream, validate the entire durable prefix, and
    /// discard only bytes that no successful append could have acknowledged.
    pub fn open(directory: impl AsRef<Path>, options: WalOptions) -> Result<Self, LogError> {
        Self::open_indexed(directory, options, |_, _| Ok(()))
    }

    fn open_indexed(
        directory: impl AsRef<Path>,
        options: WalOptions,
        visitor: impl FnMut(Record, FrameLocation) -> Result<(), LogError>,
    ) -> Result<Self, LogError> {
        if options.max_record_bytes == 0
            || options.max_record_bytes > u32::MAX as usize
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
        focal_platform::try_lock_exclusive(&lock).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                LogError::Locked
            } else {
                LogError::Io(e)
            }
        })?;
        let current = directory.join("CURRENT");
        let position = if current.exists() {
            let fence = read_fence(&current)?;
            if fence.version != 1 || fence.identity != options.identity {
                return Err(LogError::Identity);
            }
            fence.position
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
            install_fence(&directory, &options, position)?;
            let sentinel = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join("INITIALIZED"))?;
            sentinel.sync_all()?;
            sync_dir(&directory)?;
            position
        };
        scan_indexed(&directory, &options, position, visitor)?;
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
        let path = segment_path(&directory, position.generation, position.segment);
        let mut active = OpenOptions::new().read(true).write(true).open(path)?;
        if active.metadata()?.len() != position.byte {
            active.set_len(position.byte)?;
            active.sync_all()?;
        }
        active.seek(SeekFrom::Start(position.byte))?;
        cleanup_segments(&directory, position)?;
        Ok(Self {
            directory,
            options,
            _lock: lock,
            active,
            position,
            failed: false,
            fault: None,
        })
    }

    pub fn position(&self) -> DurablePosition {
        self.position
    }

    /// Recovery is streaming: at most one bounded frame is decoded at a time.
    pub fn replay(
        &self,
        visitor: impl FnMut(Record) -> Result<(), LogError>,
    ) -> Result<(), LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        scan(&self.directory, &self.options, self.position, visitor)
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
            let position = self.append_encoded(&encoded)?;
            if encoded.is_empty() {
                install_fence(&self.directory, &self.options, position)?;
            }
            scan(&self.directory, &self.options, position, |_| Ok(()))?;
            cleanup_segments(&self.directory, position)?;
            Ok(position)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Compact one logical group while streaming all other groups through the
    /// replacement generation. Other sessions' records are never discarded or
    /// accumulated into an unbounded temporary vector.
    pub fn rewrite_log_checkpoint(
        &mut self,
        log: LogicalLogId,
        retained: &[Record],
    ) -> Result<DurablePosition, LogError> {
        if self.failed {
            return Err(LogError::Failed);
        }
        if retained.iter().any(|r| r.log != log) {
            return Err(LogError::Identity);
        }
        let encoded = self.encode_batch(retained)?;
        self.rewrite_log_encoded(log, &encoded, |_, _| Ok(()))
    }

    fn rewrite_log_encoded(
        &mut self,
        log: LogicalLogId,
        encoded: &[Vec<u8>],
        mut visitor: impl FnMut(LogicalLogId, FrameLocation) -> Result<(), LogError>,
    ) -> Result<DurablePosition, LogError> {
        let old = self.position;
        let generation = old.generation.checked_add(1).ok_or(LogError::Capacity)?;
        let directory = self.directory.clone();
        let options = self.options.clone();
        let position = DurablePosition {
            generation,
            segment: 0,
            byte: HEADER_LEN,
            sequence: 0,
            checksum: 0,
        };
        let result = (|| {
            self.active = create_segment(&directory, &options, position, 0)?;
            self.position = position;
            scan(&directory, &options, old, |record| {
                if record.log != log {
                    let group = record.log;
                    let record = self.encode_batch(&[record])?;
                    self.write_encoded_indexed(&record, |location| visitor(group, location))?;
                }
                Ok(())
            })?;
            self.write_encoded_indexed(encoded, |location| visitor(log, location))?;
            let position = self.finish_append()?;
            scan(&directory, &options, position, |_| Ok(()))?;
            cleanup_segments(&directory, position)?;
            Ok(position)
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
            let len = u32::try_from(data.len()).map_err(|_| LogError::Capacity)?;
            let mut header = Vec::with_capacity(FRAME_HEADER);
            header.extend_from_slice(&len.to_le_bytes());
            header.extend_from_slice(&sequence.to_le_bytes());
            header.extend_from_slice(&self.position.checksum.to_le_bytes());
            let mut hash = crc32fast::Hasher::new();
            hash.update(&header);
            hash.update(data);
            let checksum = hash.finalize();
            header.extend_from_slice(&checksum.to_le_bytes());
            let location = FrameLocation {
                generation: self.position.generation,
                segment: self.position.segment,
                byte: self.position.byte,
                length: data.len(),
                sequence,
                previous: self.position.checksum,
                checksum,
            };
            self.active.write_all(&header)?;
            self.active.write_all(data)?;
            visit(location)?;
            self.position.byte = self
                .position
                .byte
                .checked_add(frame_bytes)
                .ok_or(LogError::Capacity)?;
            self.position.sequence = sequence;
            self.position.checksum = checksum;
        }
        Ok(())
    }

    fn finish_append(&mut self) -> Result<DurablePosition, LogError> {
        self.fail_at(FaultPoint::AfterAppend)?;
        self.active.sync_all()?;
        self.fail_at(FaultPoint::AfterDataSync)?;
        install_fence(&self.directory, &self.options, self.position)?;
        self.fail_at(FaultPoint::AfterFenceInstall)?;
        Ok(self.position)
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
    header.extend_from_slice(&1u32.to_le_bytes());
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

fn install_fence(
    directory: &Path,
    options: &WalOptions,
    position: DurablePosition,
) -> Result<(), LogError> {
    let data = postcard::to_stdvec(&Fence {
        version: 1,
        identity: options.identity,
        position,
    })?;
    let temp = directory.join("CURRENT.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(FENCE_MAGIC)?;
    file.write_all(&data)?;
    file.write_all(&crc32fast::hash(&data).to_le_bytes())?;
    file.sync_all()?;
    focal_platform::fs::atomic_replace(&temp, &directory.join("CURRENT"))?;
    sync_dir(directory)
}

fn read_fence(path: &Path) -> Result<Fence, LogError> {
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
    postcard::from_bytes(payload).map_err(|_| corrupt(path, 0, "invalid fence payload"))
}

#[derive(Clone, Copy)]
struct FrameLocation {
    generation: u64,
    segment: u64,
    byte: u64,
    length: usize,
    sequence: u64,
    previous: u32,
    checksum: u32,
}

fn scan(
    directory: &Path,
    options: &WalOptions,
    fence: DurablePosition,
    mut visitor: impl FnMut(Record) -> Result<(), LogError>,
) -> Result<(), LogError> {
    scan_indexed(directory, options, fence, |record, _| visitor(record))
}

fn scan_indexed(
    directory: &Path,
    options: &WalOptions,
    fence: DurablePosition,
    mut visitor: impl FnMut(Record, FrameLocation) -> Result<(), LogError>,
) -> Result<(), LogError> {
    let mut sequence = 0u64;
    let mut previous = 0u32;
    for segment in 0..=fence.segment {
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
        if header.get(..8) != Some(MAGIC.as_slice())
            || read_u32(&header, 8..12)? != 1
            || header.get(12..28) != Some(options.identity.cluster.as_slice())
            || read_u64(&header, 28..36)? != options.identity.node
            || read_u32(&header, 36..40)? != options.identity.stream
            || read_u64(&header, 40..48)? != fence.generation
            || read_u64(&header, 48..56)? != segment
            || read_u64(&header, 56..64)? != sequence
            || read_u32(&header, 64..68)? != previous
            || read_u32(&header, 68..72)?
                != crc32fast::hash(header.get(..68).ok_or(LogError::Capacity)?)
        {
            return Err(corrupt(&path, 0, "segment header or predecessor mismatch"));
        }
        let mut offset = HEADER_LEN;
        while offset < end {
            let remaining = end.checked_sub(offset).ok_or(LogError::Capacity)?;
            if remaining < FRAME_HEADER as u64 {
                return Err(corrupt(&path, offset, "incomplete durable frame header"));
            }
            let mut header = [0u8; FRAME_HEADER];
            file.read_exact(&mut header)?;
            let len = read_u32(&header, 0..4)? as usize;
            if len > options.max_record_bytes
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
            let mut data = vec![0u8; len];
            file.read_exact(&mut data)?;
            let mut hash = crc32fast::Hasher::new();
            hash.update(header.get(..16).ok_or(LogError::Capacity)?);
            hash.update(&data);
            let checksum = hash.finalize();
            if checksum != read_u32(&header, 16..20)? {
                return Err(corrupt(&path, offset, "durable frame checksum mismatch"));
            }
            let record = decode_record(&data)
                .map_err(|_| corrupt(&path, offset, "invalid durable record"))?;
            visitor(
                record,
                FrameLocation {
                    generation: fence.generation,
                    segment,
                    byte: offset,
                    length: len,
                    sequence: next,
                    previous,
                    checksum,
                },
            )?;
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

fn cleanup_segments(directory: &Path, p: DurablePosition) -> Result<(), LogError> {
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
        if generation != p.generation || segment > p.segment {
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
    #[test]
    fn durable_truncation_and_checksum_damage_fail_closed() {
        for truncate in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut wal = Wal::open(dir.path(), options()).unwrap();
            wal.append(&[record(1, 1)]).unwrap();
            let p = wal.position();
            drop(wal);
            let mut f = OpenOptions::new()
                .write(true)
                .open(segment_path(dir.path(), p.generation, p.segment))
                .unwrap();
            if truncate {
                f.set_len(p.byte - 1).unwrap();
            } else {
                f.seek(SeekFrom::Start(p.byte - 1)).unwrap();
                f.write_all(&[0]).unwrap();
            }
            f.sync_all().unwrap();
            assert!(matches!(
                Wal::open(dir.path(), options()),
                Err(LogError::Corruption { .. })
            ));
        }
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
