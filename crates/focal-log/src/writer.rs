//! One physical disk owner. Group callers own their Raft state and wait for the
//! covering flush; independent callers can share one fsync without sharing locks.
use super::*;
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, DiskBudget, DiskBudgetConfig, DiskKind, DiskReservation,
    MemoryBudget,
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, mpsc},
    task::{Context, Poll},
    thread::JoinHandle,
};
use tokio::sync::oneshot;

const CONTROL_SLOTS: usize = 4;
const GROUP_BYTES: usize = 4096;
const WRITER_STACK_BYTES: usize = 2 * 1024 * 1024;
std::thread_local! { static REPLAY_CALLBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
struct ReplayCallback;
impl ReplayCallback {
    fn enter() -> Result<Self, LogError> {
        reject_replay_reentry()?;
        REPLAY_CALLBACK
            .try_with(|active| active.set(true))
            .map_err(|_| LogError::Failed)?;
        Ok(Self)
    }
}
impl Drop for ReplayCallback {
    fn drop(&mut self) {
        let _ = REPLAY_CALLBACK.try_with(|active| active.set(false));
    }
}
fn reject_replay_reentry() -> Result<(), LogError> {
    if REPLAY_CALLBACK
        .try_with(std::cell::Cell::get)
        .map_err(|_| LogError::Failed)?
    {
        Err(LogError::ReplayReentry)
    } else {
        Ok(())
    }
}

/// Internal node resource policy. Defaults require no operator configuration.
#[derive(Clone, Debug)]
pub struct WalWriterLimits {
    pub queue_items: usize,
    pub max_batch_requests: usize,
    pub max_groups: usize,
}
impl Default for WalWriterLimits {
    fn default() -> Self {
        Self {
            queue_items: 64,
            max_batch_requests: 64,
            max_groups: 65536,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalWriterStats {
    pub group_commits: u64,
    pub appended_records: u64,
    pub startup_scan_records: u64,
    pub replayed_records: u64,
    pub indexed_records: usize,
}

/// The sole Arc owns channel shutdown and the thread join across independent
/// Send group handles. Disk state never sits behind a Mutex. Tickets contain no
/// handle, so the final handle can drain/join with unconsumed receipts present.
#[derive(Clone)]
pub struct SharedWal(Arc<Handle>);
/// Opaque process-local writer provenance. Never serialized or reused after
/// reopening, even when the physical path and durable WalIdentity are equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalWriterId(focal_memory::OwnerId);
#[cfg(feature = "test-support")]
pub struct WalPause {
    resume: mpsc::SyncSender<()>,
}
#[cfg(feature = "test-support")]
impl WalPause {
    pub fn resume(self) -> Result<(), LogError> {
        self.resume.send(()).map_err(|_| LogError::Failed)
    }
}
struct Handle {
    owner: WalWriterId,
    directory: std::path::PathBuf,
    sender: Option<mpsc::SyncSender<Command>>,
    thread: Option<JoinHandle<()>>,
    options: WalOptions,
    budget: MemoryBudget,
    slots: MemoryBudget,
    /// The volume envelope every batch is promised from before it is queued.
    disk: DiskBudget,
    _configuration: Allocation,
}
impl Drop for Handle {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub struct WalLease {
    // Disconnecting this single-owner token releases the lease at the next
    // lease admission. Commands already queued remain ordered ahead of reuse.
    _release: mpsc::SyncSender<()>,
    shared: SharedWal,
    log: LogicalLogId,
    generation: u64,
}
struct LeaseRow {
    generation: u64,
    released: mpsc::Receiver<()>,
    _allocation: Allocation,
}
struct LeaseAdmission {
    generation: u64,
    release: mpsc::SyncSender<()>,
}

/// Cancelling this future cancels interest in the receipt, not an admitted write.
/// Recovery decides ambiguous outcomes using the unchanged durable CURRENT fence.
pub struct WalAppend {
    receiver: Option<oneshot::Receiver<Result<DurablePosition, LogError>>>,
    completed: mpsc::Receiver<()>,
    _allocation: Allocation,
}
impl Future for WalAppend {
    type Output = Result<DurablePosition, LogError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Poll::Ready(Err(LogError::ReceiptConsumed));
        };
        let outcome = match Pin::new(receiver).poll(cx) {
            Poll::Ready(Ok(result)) => result,
            Poll::Ready(Err(_)) => Err(LogError::Failed),
            Poll::Pending => match reject_replay_reentry() {
                Ok(()) => return Poll::Pending,
                Err(error) => Err(error),
            },
        };
        self.receiver = None;
        Poll::Ready(outcome)
    }
}
impl WalAppend {
    /// Wait for this exact admitted write from a synchronous owner, including
    /// callers currently inside a Tokio runtime. The one-slot completion signal
    /// avoids runtime-dependent blocking_recv panics and adds no writer handle.
    pub fn wait_blocking(&mut self) -> Result<DurablePosition, LogError> {
        if let Some(result) = self.try_complete() {
            return result;
        }
        reject_replay_reentry()?;
        self.completed.recv().map_err(|_| LogError::Failed)?;
        self.try_complete().ok_or(LogError::Failed)?
    }
    /// A completed observation consumes the receipt. Later polling is a typed
    /// `ReceiptConsumed` error and never re-polls Tokio's completed receiver.
    pub fn try_complete(&mut self) -> Option<Result<DurablePosition, LogError>> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Some(Err(LogError::ReceiptConsumed));
        };
        let outcome = match receiver.try_recv() {
            Ok(result) => result,
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => Err(LogError::Failed),
        };
        self.receiver = None;
        Some(outcome)
    }
}

enum Reply<T> {
    Blocking(mpsc::SyncSender<Result<T, LogError>>),
    Async(oneshot::Sender<Result<T, LogError>>, mpsc::SyncSender<()>),
}
impl<T> Reply<T> {
    fn finish(self, value: Result<T, LogError>) {
        match self {
            Self::Blocking(sender) => {
                let _ = sender.try_send(value);
            }
            Self::Async(sender, completed) => {
                let _ = sender.send(value);
                let _ = completed.try_send(());
            }
        }
    }
}
struct Batch {
    log: LogicalLogId,
    generation: u64,
    encoded: Vec<Vec<u8>>,
    bytes: usize,
    index: Option<IndexChunk>,
    reply: Reply<DurablePosition>,
    /// Committed once the batch reached its durable fence; returned otherwise.
    disk: DiskReservation,
    _allocation: Allocation,
    _slot: Allocation,
}
struct RecoveredRecord {
    record: Record,
    _allocation: Allocation,
}
enum ReplayItem {
    Record(RecoveredRecord),
    Complete(Result<(), LogError>),
}
enum Command {
    Append(Batch),
    Checkpoint(Batch),
    Lease(LogicalLogId, Reply<LeaseAdmission>, Allocation),
    Identity(Reply<WalIdentity>, Allocation),
    Replay(LogicalLogId, u64, mpsc::SyncSender<ReplayItem>, Allocation),
    Fault(FaultPoint, Reply<()>, Allocation),
    Stats(Reply<WalWriterStats>, Allocation),
    #[cfg(any(test, feature = "test-support"))]
    Pause(mpsc::SyncSender<()>, mpsc::Receiver<()>),
}

struct IndexChunk {
    frames: Vec<FrameLocation>,
    _allocation: Allocation,
}
impl IndexChunk {
    fn allocate(budget: &MemoryBudget, count: usize, lane: BudgetLane) -> Result<Self, LogError> {
        let amount = count
            .checked_mul(std::mem::size_of::<FrameLocation>())
            .and_then(|n| n.checked_add(256))
            .ok_or(LogError::Capacity)?;
        let allocation = reserve(budget, BudgetKind::Index, lane, amount)?;
        let mut frames = Vec::new();
        frames
            .try_reserve_exact(count)
            .map_err(|_| LogError::Capacity)?;
        Ok(Self {
            frames,
            _allocation: allocation,
        })
    }
}
struct GroupIndex {
    chunks: Vec<IndexChunk>,
    _allocation: Allocation,
}
struct RecoveryIndex {
    groups: BTreeMap<LogicalLogId, GroupIndex>,
    budget: MemoryBudget,
    max_groups: usize,
    records: usize,
}
impl RecoveryIndex {
    fn new(budget: MemoryBudget, max_groups: usize) -> Self {
        Self {
            groups: BTreeMap::new(),
            budget,
            max_groups,
            records: 0,
        }
    }
    fn ensure_group(&mut self, log: LogicalLogId) -> Result<(), LogError> {
        if self.groups.contains_key(&log) {
            return Ok(());
        }
        if self.groups.len() >= self.max_groups {
            return Err(LogError::Capacity);
        }
        let allocation = reserve(
            &self.budget,
            BudgetKind::Index,
            BudgetLane::Completion,
            GROUP_BYTES,
        )?;
        self.groups.insert(
            log,
            GroupIndex {
                chunks: Vec::new(),
                _allocation: allocation,
            },
        );
        Ok(())
    }
    fn reserve_slot(&mut self, log: LogicalLogId, count: usize) -> Result<(), LogError> {
        self.groups
            .get_mut(&log)
            .ok_or(LogError::Failed)?
            .chunks
            .try_reserve(count)
            .map_err(|_| LogError::Capacity)
    }
    /// Total on-disk bytes across every indexed group. A checkpoint rewrite
    /// copies all of these forward into a new generation while the old one is
    /// still live, so this is the additional disk a checkpoint must be promised.
    fn total_bytes(&self) -> Result<u64, LogError> {
        let mut total = 0u64;
        for group in self.groups.values() {
            for chunk in &group.chunks {
                for frame in &chunk.frames {
                    total = total
                        .checked_add(frame.length as u64)
                        .ok_or(LogError::Capacity)?;
                }
            }
        }
        Ok(total)
    }
    fn prepare(
        &mut self,
        log: LogicalLogId,
        count: usize,
        pending_chunks: usize,
        lane: BudgetLane,
    ) -> Result<IndexChunk, LogError> {
        self.records.checked_add(count).ok_or(LogError::Capacity)?;
        let chunk = IndexChunk::allocate(&self.budget, count, lane)?;
        self.ensure_group(log)?;
        self.reserve_slot(log, pending_chunks)?;
        Ok(chunk)
    }
    fn publish(&mut self, log: LogicalLogId, chunk: IndexChunk) -> Result<(), LogError> {
        if chunk.frames.is_empty() {
            return Ok(());
        }
        self.records = self
            .records
            .checked_add(chunk.frames.len())
            .ok_or(LogError::Capacity)?;
        self.groups
            .get_mut(&log)
            .ok_or(LogError::Failed)?
            .chunks
            .push(chunk);
        Ok(())
    }
    fn push(&mut self, log: LogicalLogId, frame: FrameLocation) -> Result<(), LogError> {
        let mut chunk = self.prepare(log, 1, 1, BudgetLane::Completion)?;
        chunk.frames.push(frame);
        self.publish(log, chunk)
    }
}
struct Writer {
    wal: Wal,
    index: RecoveryIndex,
    leases: BTreeMap<LogicalLogId, LeaseRow>,
    lease_generation: u64,
    limits: WalWriterLimits,
    budget: MemoryBudget,
    disk: DiskBudget,
    stats: WalWriterStats,
}
fn reserve(
    budget: &MemoryBudget,
    kind: BudgetKind,
    lane: BudgetLane,
    amount: usize,
) -> Result<Allocation, LogError> {
    budget
        .reserve(kind, lane, amount)
        .map(|p| p.commit())
        .map_err(|_| LogError::Capacity)
}
impl SharedWal {
    /// Test-only physical writer backpressure. The returned guard contains no
    /// writer ownership; dropping it also resumes the writer after a failed test.
    #[cfg(feature = "test-support")]
    pub fn pause_for_test(&self) -> Result<WalPause, LogError> {
        reject_replay_reentry()?;
        let (entered, entry) = mpsc::sync_channel(1);
        let (resume, resumed) = mpsc::sync_channel(1);
        self.send(Command::Pause(entered, resumed))?;
        entry.recv().map_err(|_| LogError::Failed)?;
        Ok(WalPause { resume })
    }
    pub fn is_same_writer(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn writer_id(&self) -> WalWriterId {
        self.0.owner
    }
    pub fn is_budgeted_within(&self, parent: &MemoryBudget) -> bool {
        self.0.budget.is_within(parent)
    }
    pub fn open(directory: impl AsRef<Path>, options: WalOptions) -> Result<Self, LogError> {
        let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024)
            .map_err(|_| LogError::Capacity)?;
        Self::open_with_budget(directory, options, WalWriterLimits::default(), budget)
    }
    /// Node hosts can attach the writer to their hierarchical RAM budget. The
    /// writer then guards its volume with the standard disk watermark alone.
    pub fn open_with_budget(
        directory: impl AsRef<Path>,
        options: WalOptions,
        limits: WalWriterLimits,
        budget: MemoryBudget,
    ) -> Result<Self, LogError> {
        let disk = DiskBudget::new(DiskBudgetConfig::default()).map_err(|_| LogError::Capacity)?;
        Self::open_with_budgets(directory, options, limits, budget, disk)
    }
    /// Attach the writer to the RAM budget and to the disk envelope shared by
    /// every durable owner of the same volume: a batch is promised its bytes
    /// before it is queued and refused with `Capacity` before any
    /// acknowledgement when the volume cannot take it.
    pub fn open_with_budgets(
        directory: impl AsRef<Path>,
        options: WalOptions,
        limits: WalWriterLimits,
        budget: MemoryBudget,
        disk: DiskBudget,
    ) -> Result<Self, LogError> {
        reject_replay_reentry()?;
        let directory_path = directory.as_ref().to_path_buf();
        if !(1..=4096).contains(&limits.queue_items)
            || !(1..=64).contains(&limits.max_batch_requests)
            || !(1..=65536).contains(&limits.max_groups)
        {
            return Err(LogError::Capacity);
        }
        let owner = WalWriterId(focal_memory::OwnerId::new().map_err(|_| LogError::Capacity)?);
        let capacity = limits
            .queue_items
            .checked_add(CONTROL_SLOTS)
            .ok_or(LogError::Capacity)?;
        let queue_bytes = std::mem::size_of::<Command>()
            .checked_add(64)
            .and_then(|size| capacity.checked_mul(size))
            .ok_or(LogError::Capacity)?;
        let batch_bytes = std::mem::size_of::<Batch>()
            .checked_mul(3)
            .and_then(|size| size.checked_add(std::mem::size_of::<IndexChunk>()))
            .and_then(|size| size.checked_add(128))
            .and_then(|size| limits.max_batch_requests.checked_mul(size))
            .ok_or(LogError::Capacity)?;
        let configuration_bytes = queue_bytes
            .checked_add(batch_bytes)
            .and_then(|size| size.checked_add(WRITER_STACK_BYTES))
            .and_then(|size| size.checked_add(4096))
            .ok_or(LogError::Capacity)?;
        let configuration = reserve(
            &budget,
            BudgetKind::Control,
            BudgetLane::Completion,
            configuration_bytes,
        )?;
        let slots = MemoryBudget::new(capacity, CONTROL_SLOTS).map_err(|_| LogError::Capacity)?;
        // The scan decodes one bounded frame. Its payload and encoded bytes may coexist.
        let scratch = reserve(
            &budget,
            BudgetKind::Recovery,
            BudgetLane::Completion,
            options
                .max_record_bytes
                .checked_mul(3)
                .ok_or(LogError::Capacity)?,
        )?;
        let mut index = RecoveryIndex::new(budget.clone(), limits.max_groups);
        let wal = Wal::open_indexed(directory, options.clone(), |record, location| {
            index.push(record.log, location)
        })?;
        drop(scratch);
        let stats = WalWriterStats {
            startup_scan_records: u64::try_from(index.records).map_err(|_| LogError::Capacity)?,
            ..Default::default()
        };
        let writer = Writer {
            wal,
            index,
            leases: BTreeMap::new(),
            lease_generation: 0,
            limits,
            budget: budget.clone(),
            disk: disk.clone(),
            stats,
        };
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let thread = std::thread::Builder::new()
            .stack_size(WRITER_STACK_BYTES)
            .name(format!(
                "focal-wal-{}-{}",
                options.identity.node, options.identity.stream
            ))
            .spawn(move || writer.run(receiver))?;
        Ok(Self(Arc::new(Handle {
            owner,
            directory: directory_path,
            sender: Some(sender),
            thread: Some(thread),
            options,
            budget,
            slots,
            disk,
            _configuration: configuration,
        })))
    }
    /// Free bytes of the volume holding this WAL directory that no queued
    /// write has been promised yet, from a sample the disk envelope refreshes
    /// at its bounded cadence. Admission watermarks compare against this
    /// before any in-memory acknowledgement; while the volume cannot be
    /// sampled it is zero.
    pub fn available_bytes(&self) -> Result<u64, LogError> {
        let directory = &self.0.directory;
        self.0
            .disk
            .refresh_with(|| focal_platform::available_space(directory));
        Ok(self.0.disk.uncommitted_free())
    }
    /// The disk envelope this writer draws from, for the other durable owners
    /// of the same volume.
    pub fn disk_budget(&self) -> DiskBudget {
        self.0.disk.clone()
    }
    fn disk_reserve(
        &self,
        kind: DiskKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<DiskReservation, LogError> {
        let directory = &self.0.directory;
        self.0
            .disk
            .refresh_with(|| focal_platform::available_space(directory));
        let bytes = u64::try_from(bytes).map_err(|_| LogError::Capacity)?;
        self.0
            .disk
            .reserve(kind, lane, bytes)
            .map_err(|_| LogError::Capacity)
    }
    fn send(&self, command: Command) -> Result<(), LogError> {
        self.0
            .sender
            .as_ref()
            .ok_or(LogError::Failed)?
            .try_send(command)
            .map_err(|e| match e {
                mpsc::TrySendError::Full(_) => LogError::Capacity,
                mpsc::TrySendError::Disconnected(_) => LogError::Failed,
            })
    }
    fn control_slot(&self) -> Result<Allocation, LogError> {
        reserve(
            &self.0.slots,
            BudgetKind::Control,
            BudgetLane::Completion,
            1,
        )
    }
    pub fn identity(&self) -> Result<WalIdentity, LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(Command::Identity(
            Reply::Blocking(sender),
            self.control_slot()?,
        ))?;
        receiver.recv().map_err(|_| LogError::Failed)?
    }
    pub fn stats(&self) -> Result<WalWriterStats, LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(Command::Stats(
            Reply::Blocking(sender),
            self.control_slot()?,
        ))?;
        receiver.recv().map_err(|_| LogError::Failed)?
    }
    pub fn lease(&self, log: LogicalLogId) -> Result<WalLease, LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(Command::Lease(
            log,
            Reply::Blocking(sender),
            self.control_slot()?,
        ))?;
        let admission = receiver.recv().map_err(|_| LogError::Failed)??;
        Ok(WalLease {
            _release: admission.release,
            shared: self.clone(),
            log,
            generation: admission.generation,
        })
    }
    fn batch(
        &self,
        log: LogicalLogId,
        generation: u64,
        records: &[Record],
        reply: Reply<DurablePosition>,
        checkpoint: bool,
        lane: BudgetLane,
    ) -> Result<(), LogError> {
        let mut sizes = Vec::new();
        let bytes = self.validate_batch(log, records, Some(&mut sizes))?;
        let slot = reserve(&self.0.slots, BudgetKind::Pending, lane, 1)?;
        let amount = records
            .len()
            .checked_mul(std::mem::size_of::<Vec<u8>>())
            .and_then(|n| n.checked_add(bytes))
            .and_then(|n| n.checked_add(512))
            .ok_or(LogError::Capacity)?;
        let allocation = reserve(&self.0.budget, BudgetKind::Pending, lane, amount)?;
        let disk = self.disk_reserve(
            if checkpoint {
                DiskKind::Checkpoint
            } else {
                DiskKind::Wal
            },
            lane,
            bytes,
        )?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(records.len())
            .map_err(|_| LogError::Capacity)?;
        for (record, &length) in records.iter().zip(&sizes) {
            let mut data = Vec::new();
            data.try_reserve_exact(length)
                .map_err(|_| LogError::Capacity)?;
            data.resize(length, 0);
            postcard::to_slice(record, &mut data)?;
            encoded.push(data);
        }
        let index = IndexChunk::allocate(&self.0.budget, encoded.len(), lane)?;
        let batch = Batch {
            log,
            generation,
            encoded,
            bytes,
            index: Some(index),
            reply,
            disk,
            _allocation: allocation,
            _slot: slot,
        };
        self.send(if checkpoint {
            Command::Checkpoint(batch)
        } else {
            Command::Append(batch)
        })
    }
    fn validate_batch(
        &self,
        log: LogicalLogId,
        records: &[Record],
        mut sizes: Option<&mut Vec<usize>>,
    ) -> Result<usize, LogError> {
        if let Some(sizes) = sizes.as_deref_mut() {
            sizes.clear();
            sizes
                .try_reserve_exact(records.len())
                .map_err(|_| LogError::Capacity)?;
        }
        let mut bytes = 0usize;
        for record in records {
            if record.log != log {
                return Err(LogError::Identity);
            }
            let length = postcard::experimental::serialized_size(record)?;
            if length > self.0.options.max_record_bytes {
                return Err(LogError::Capacity);
            }
            // Reuse this size in the encode loop instead of a second full
            // serialization traversal per record.
            if let Some(sizes) = sizes.as_deref_mut() {
                sizes.push(length);
            }
            bytes = bytes
                .checked_add(length)
                .and_then(|n| n.checked_add(FRAME_HEADER))
                .ok_or(LogError::Capacity)?;
        }
        if bytes > self.0.options.max_batch_bytes {
            return Err(LogError::Capacity);
        }
        Ok(bytes)
    }
}
fn default_lane(records: &[Record]) -> Result<BudgetLane, LogError> {
    if records.iter().any(|record| {
        !matches!(
            record.kind,
            RecordKind::HardState
                | RecordKind::Configuration
                | RecordKind::Identity
                | RecordKind::DecoderFloor
                | RecordKind::DecoderTransition
        )
    }) {
        return Ok(BudgetLane::Ordinary);
    }
    let bytes = records.iter().try_fold(0usize, |total, record| {
        total
            .checked_add(postcard::experimental::serialized_size(record)?)
            .and_then(|size| size.checked_add(FRAME_HEADER))
            .ok_or(LogError::Capacity)
    })?;
    Ok(if bytes <= 64 * 1024 {
        BudgetLane::Completion
    } else {
        BudgetLane::Ordinary
    })
}

impl WalLease {
    /// The disk envelope of the volume this log lives on, shared with the
    /// other durable owners of the same volume.
    pub fn disk_budget(&self) -> DiskBudget {
        self.shared.disk_budget()
    }
    /// Free bytes on the filesystem holding the shared WAL, sampled now.
    pub fn available_bytes(&self) -> Result<u64, LogError> {
        self.shared.available_bytes()
    }
    /// Visitors may enqueue async writes, but synchronous WAL reentry and
    /// waiting on an unfinished append return `ReplayReentry`. Finish replay
    /// before awaiting durability; cancelling interest does not cancel a write.
    pub fn replay(
        &self,
        mut visitor: impl FnMut(Record) -> Result<(), LogError>,
    ) -> Result<(), LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.shared.send(Command::Replay(
            self.log,
            self.generation,
            sender,
            self.shared.control_slot()?,
        ))?;
        while let Ok(item) = receiver.recv() {
            match item {
                ReplayItem::Record(record) => {
                    let _callback = ReplayCallback::enter()?;
                    visitor(record.record)?;
                }
                ReplayItem::Complete(result) => return result,
            }
        }
        Err(LogError::Failed)
    }
    pub fn append(&mut self, records: &[Record]) -> Result<DurablePosition, LogError> {
        self.append_in(records, default_lane(records)?)
    }
    /// Retain the physical writer independently of this logical-log lease.
    pub fn shared_wal(&self) -> SharedWal {
        self.shared.clone()
    }
    /// Pure format/identity preflight before retaining a batch for admission.
    /// Rejects a batch that cannot fit even under immutable ancestor ceilings.
    /// Current pressure can still prevent admission; this reserves no space.
    pub fn validate_append(&self, records: &[Record]) -> Result<(), LogError> {
        let bytes = self.shared.validate_batch(self.log, records, None)?;
        let metadata = std::mem::size_of::<Vec<u8>>()
            .checked_add(std::mem::size_of::<FrameLocation>())
            .and_then(|size| size.checked_mul(records.len()))
            .ok_or(LogError::Capacity)?;
        let required = bytes
            .checked_add(metadata)
            // Encoded batch bookkeeping + index chunk + asynchronous ticket.
            .and_then(|size| size.checked_add(512 + 256 + 1024))
            .and_then(|size| size.checked_add(self.shared.0._configuration.bytes()))
            .ok_or(LogError::Capacity)?;
        if required
            > self
                .shared
                .0
                .budget
                .reservation_limit(BudgetLane::Completion)
        {
            return Err(LogError::Capacity);
        }
        Ok(())
    }
    /// Trusted host policy for completing already admitted durable work. A wire
    /// caller must never select its own admission lane.
    pub fn append_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
    ) -> Result<DurablePosition, LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.shared.batch(
            self.log,
            self.generation,
            records,
            Reply::Blocking(sender),
            false,
            lane,
        )?;
        receiver.recv().map_err(|_| LogError::Failed)?
    }
    pub fn append_async(&mut self, records: &[Record]) -> Result<WalAppend, LogError> {
        self.append_async_in(records, default_lane(records)?)
    }
    /// Asynchronous completion admission; the receipt and queued batch use the
    /// same trusted lane and remain independently charged for their lifetimes.
    pub fn append_async_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
    ) -> Result<WalAppend, LogError> {
        self.batch_async_in(records, lane, false)
    }
    /// Queues the same atomic checkpoint rewrite as the synchronous API. The
    /// receipt resolves only after its CURRENT fence; dropping it does not
    /// cancel an admitted rewrite or release the writer's owned batch permits.
    pub fn rewrite_checkpoint_async_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
    ) -> Result<WalAppend, LogError> {
        self.batch_async_in(records, lane, true)
    }
    fn batch_async_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
        checkpoint: bool,
    ) -> Result<WalAppend, LogError> {
        let allocation = reserve(&self.shared.0.budget, BudgetKind::Pending, lane, 1024)?;
        let (sender, receiver) = oneshot::channel();
        let (completed, completion) = mpsc::sync_channel(1);
        self.shared.batch(
            self.log,
            self.generation,
            records,
            Reply::Async(sender, completed),
            checkpoint,
            lane,
        )?;
        Ok(WalAppend {
            receiver: Some(receiver),
            completed: completion,
            _allocation: allocation,
        })
    }
    pub fn rewrite_checkpoint(&mut self, records: &[Record]) -> Result<DurablePosition, LogError> {
        self.rewrite_checkpoint_in(records, BudgetLane::Ordinary)
    }
    /// Checkpoint durability for a trusted host's previously admitted work.
    pub fn rewrite_checkpoint_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
    ) -> Result<DurablePosition, LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.shared.batch(
            self.log,
            self.generation,
            records,
            Reply::Blocking(sender),
            true,
            lane,
        )?;
        receiver.recv().map_err(|_| LogError::Failed)?
    }
    pub fn inject_fault_once(&mut self, point: FaultPoint) {
        let _ = self.try_inject_fault_once(point);
    }
    pub fn try_inject_fault_once(&mut self, point: FaultPoint) -> Result<(), LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.shared.send(Command::Fault(
            point,
            Reply::Blocking(sender),
            self.shared.control_slot()?,
        ))?;
        receiver.recv().map_err(|_| LogError::Failed)?
    }
}

impl Writer {
    fn valid(&self, log: LogicalLogId, generation: u64) -> Result<(), LogError> {
        if self.wal.failed {
            return Err(LogError::Failed);
        }
        if self
            .leases
            .get(&log)
            .is_none_or(|lease| lease.generation != generation)
        {
            return Err(LogError::LogicalLocked);
        }
        Ok(())
    }
    fn run(mut self, receiver: mpsc::Receiver<Command>) {
        let mut deferred = None;
        loop {
            let command = match deferred.take() {
                Some(command) => command,
                None => match receiver.recv() {
                    Ok(command) => command,
                    Err(_) => break,
                },
            };
            match command {
                Command::Append(first) => {
                    let mut batches = Vec::new();
                    if batches
                        .try_reserve_exact(self.limits.max_batch_requests)
                        .is_err()
                    {
                        first.reply.finish(Err(LogError::Capacity));
                        continue;
                    }
                    let mut bytes = first.bytes;
                    batches.push(first);
                    while batches.len() < self.limits.max_batch_requests {
                        match receiver.try_recv() {
                            Ok(Command::Append(next))
                                if bytes
                                    .checked_add(next.bytes)
                                    .is_some_and(|n| n <= self.wal.options.max_batch_bytes) =>
                            {
                                bytes = bytes.saturating_add(next.bytes);
                                batches.push(next);
                            }
                            Ok(other) => {
                                deferred = Some(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    self.append(batches);
                }
                Command::Checkpoint(batch) => self.checkpoint(batch),
                Command::Lease(log, reply, _slot) => {
                    let result = (|| {
                        if self.wal.failed {
                            return Err(LogError::Failed);
                        }
                        self.leases.retain(|_, lease| {
                            !matches!(
                                lease.released.try_recv(),
                                Err(mpsc::TryRecvError::Disconnected)
                            )
                        });
                        self.index.groups.retain(|id, group| {
                            !group.chunks.is_empty() || self.leases.contains_key(id)
                        });
                        if self.leases.contains_key(&log) {
                            return Err(LogError::LogicalLocked);
                        }
                        if self.leases.len() >= self.limits.max_groups {
                            return Err(LogError::Capacity);
                        }
                        let generation = self
                            .lease_generation
                            .checked_add(1)
                            .ok_or(LogError::Capacity)?;
                        let allocation = reserve(
                            &self.budget,
                            BudgetKind::Control,
                            BudgetLane::Completion,
                            GROUP_BYTES,
                        )?;
                        self.index.ensure_group(log)?;
                        let (release, released) = mpsc::sync_channel(0);
                        self.leases.insert(
                            log,
                            LeaseRow {
                                generation,
                                released,
                                _allocation: allocation,
                            },
                        );
                        self.lease_generation = generation;
                        Ok(LeaseAdmission {
                            generation,
                            release,
                        })
                    })();
                    reply.finish(result);
                }
                Command::Identity(reply, _slot) => reply.finish(if self.wal.failed {
                    Err(LogError::Failed)
                } else {
                    Ok(self.wal.options.identity)
                }),
                Command::Stats(reply, _slot) => reply.finish(Ok(WalWriterStats {
                    indexed_records: self.index.records,
                    ..self.stats
                })),
                Command::Fault(point, reply, _slot) => {
                    if self.wal.failed {
                        reply.finish(Err(LogError::Failed));
                    } else {
                        self.wal.inject_fault_once(point);
                        reply.finish(Ok(()));
                    }
                }
                Command::Replay(log, generation, reply, _slot) => {
                    let result = self.replay(log, generation, &reply);
                    if matches!(result, Err(LogError::Io(_) | LogError::Corruption { .. })) {
                        self.wal.failed = true;
                    }
                    let _ = reply.send(ReplayItem::Complete(result));
                }
                #[cfg(any(test, feature = "test-support"))]
                Command::Pause(entered, resume) => {
                    let _ = entered.send(());
                    let _ = resume.recv();
                }
            }
        }
    }
    fn append(&mut self, batches: Vec<Batch>) {
        let mut prepared: Vec<(Batch, IndexChunk)> = Vec::new();
        let mut completed = Vec::new();
        if prepared.try_reserve_exact(batches.len()).is_err()
            || completed.try_reserve_exact(batches.len()).is_err()
        {
            for batch in batches {
                batch.reply.finish(Err(LogError::Capacity));
            }
            return;
        }
        for mut batch in batches {
            let pending_chunks = prepared
                .iter()
                .filter(|(other, _)| other.log == batch.log)
                .count()
                .saturating_add(1);
            let result = self.valid(batch.log, batch.generation).and_then(|()| {
                self.index.reserve_slot(batch.log, pending_chunks)?;
                batch.index.take().ok_or(LogError::Failed)
            });
            match result {
                Ok(chunk) => prepared.push((batch, chunk)),
                Err(error) => {
                    // Pre-write, per-batch errors (a bounded slot-reservation
                    // shortage, a locked logical group) are batch-local: nothing
                    // has touched disk yet, so the physical writer and every other
                    // logical group stay healthy. Fail only this batch and keep
                    // the rest; the writer is poisoned only by post-write failures
                    // below, never by a recoverable prepare-phase condition.
                    batch.reply.finish(Err(error));
                }
            }
        }
        if self.wal.failed {
            for (batch, _) in prepared {
                batch.reply.finish(Err(LogError::Failed));
            }
            return;
        }
        if prepared.is_empty() {
            return;
        }
        let result = (|| {
            let record_count = prepared
                .iter()
                .try_fold(0u64, |n, (batch, _)| {
                    n.checked_add(batch.encoded.len() as u64)
                })
                .ok_or(LogError::Capacity)?;
            self.index
                .records
                .checked_add(usize::try_from(record_count).map_err(|_| LogError::Capacity)?)
                .ok_or(LogError::Capacity)?;
            let appended_records = self
                .stats
                .appended_records
                .checked_add(record_count)
                .ok_or(LogError::Capacity)?;
            let group_commits = self
                .stats
                .group_commits
                .checked_add(u64::from(record_count != 0))
                .ok_or(LogError::Capacity)?;
            for (batch, chunk) in &mut prepared {
                self.wal.write_encoded_indexed(&batch.encoded, |location| {
                    chunk.frames.push(location);
                    Ok(())
                })?;
            }
            if record_count != 0 {
                self.wal.finish_append()?;
            }
            self.stats.appended_records = appended_records;
            self.stats.group_commits = group_commits;
            Ok(self.wal.position)
        })();
        match result {
            Ok(position) => {
                // Every chunk and outer index slot was allocated before writing.
                // Checked index count overflow is bounded by the allocated index.
                let mut failure = None;
                for (batch, chunk) in prepared {
                    if let Err(error) = self.index.publish(batch.log, chunk) {
                        failure = Some(error);
                    }
                    completed.push(batch);
                }
                self.wal.failed = failure.is_some();
                for batch in completed {
                    let Batch { reply, disk, .. } = batch;
                    if self.wal.failed {
                        reply.finish(Err(failure.take().unwrap_or(LogError::Failed)));
                    } else {
                        // The bytes are behind the durable fence: charge them
                        // to the volume rather than returning the promise.
                        disk.commit();
                        reply.finish(Ok(position));
                    }
                }
            }
            Err(error) => {
                self.wal.failed = true;
                let mut cause = Some(error);
                for (batch, _) in prepared {
                    batch
                        .reply
                        .finish(Err(cause.take().unwrap_or(LogError::Failed)));
                }
            }
        }
    }
    fn checkpoint(&mut self, batch: Batch) {
        let result = (|| {
            self.valid(batch.log, batch.generation)?;
            // The rewrite copies every logical group forward into a new
            // generation while the old one is still on disk, so its peak is the
            // whole current log, not just the retained batch reserved at
            // admission. Reserve that peak as transient headroom held across the
            // rewrite (released when this scope ends, since cleanup frees the old
            // generation): a full volume is refused cleanly here instead of
            // hitting ENOSPC mid-copy and poisoning the physical writer.
            let _forward = self
                .disk
                .reserve(
                    DiskKind::Checkpoint,
                    BudgetLane::Completion,
                    self.index.total_bytes()?,
                )
                .map_err(|_| LogError::Capacity)?;
            let _scratch = reserve(
                &self.budget,
                BudgetKind::Recovery,
                BudgetLane::Completion,
                self.wal
                    .options
                    .max_record_bytes
                    .checked_mul(3)
                    .ok_or(LogError::Capacity)?,
            )?;
            let mut index = RecoveryIndex::new(self.budget.clone(), self.limits.max_groups);
            // Active empty logs have no records to drive the rewrite visitor.
            // Reserve their rows before the durable replacement fence is installed.
            for log in self.leases.keys() {
                index.ensure_group(*log)?;
            }
            // The replacement index is built while streaming the new generation,
            // before its durable fence can be installed. The old index stays live.
            let position =
                self.wal
                    .rewrite_log_encoded(batch.log, &batch.encoded, |log, location| {
                        index.push(log, location)
                    })?;
            self.index = index;
            Ok(position)
        })();
        let Batch { reply, disk, .. } = batch;
        if result.is_ok() {
            disk.commit();
        }
        reply.finish(result);
    }
    fn replay(
        &mut self,
        log: LogicalLogId,
        generation: u64,
        reply: &mpsc::SyncSender<ReplayItem>,
    ) -> Result<(), LogError> {
        self.valid(log, generation)?;
        let Some(group) = self.index.groups.get(&log) else {
            return Ok(());
        };
        let mut file: Option<(u64, File)> = None;
        for location in group.chunks.iter().flat_map(|chunk| &chunk.frames) {
            let record = read_indexed(&self.wal.directory, location, &mut file, &self.budget)?;
            if record.record.log != log {
                return Err(LogError::Identity);
            }
            self.stats.replayed_records = self
                .stats
                .replayed_records
                .checked_add(1)
                .ok_or(LogError::Capacity)?;
            // A dropped visitor ends its bounded recovery stream without leaving
            // the physical writer blocked or cancelling other groups' writes.
            if reply.send(ReplayItem::Record(record)).is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}
fn read_indexed(
    directory: &Path,
    location: &FrameLocation,
    current: &mut Option<(u64, File)>,
    budget: &MemoryBudget,
) -> Result<RecoveredRecord, LogError> {
    let amount = location
        .length
        .checked_mul(3)
        .and_then(|n| n.checked_add(256))
        .ok_or(LogError::Capacity)?;
    let allocation = reserve(budget, BudgetKind::Recovery, BudgetLane::Completion, amount)?;
    let path = segment_path(directory, location.generation, location.segment);
    if current
        .as_ref()
        .is_none_or(|(segment, _)| *segment != location.segment)
    {
        *current = Some((location.segment, File::open(&path)?));
    }
    let (_, file) = current.as_mut().ok_or(LogError::Failed)?;
    file.seek(SeekFrom::Start(location.byte))?;
    let mut header = [0; FRAME_HEADER];
    file.read_exact(&mut header)?;
    if read_u32(&header, 0..4)? as usize != location.length
        || read_u64(&header, 4..12)? != location.sequence
        || read_u32(&header, 12..16)? != location.previous
        || read_u32(&header, 16..20)? != location.checksum
    {
        return Err(corrupt(
            &path,
            location.byte,
            "indexed durable frame changed",
        ));
    }
    let mut data = Vec::new();
    data.try_reserve_exact(location.length)
        .map_err(|_| LogError::Capacity)?;
    data.resize(location.length, 0);
    file.read_exact(&mut data)?;
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(header.get(..16).ok_or(LogError::Capacity)?);
    checksum.update(&data);
    if checksum.finalize() != location.checksum {
        return Err(corrupt(
            &path,
            location.byte,
            "indexed durable checksum mismatch",
        ));
    }
    let record = decode_record(&data)
        .map_err(|_| corrupt(&path, location.byte, "invalid indexed record"))?;
    Ok(RecoveredRecord {
        record,
        _allocation: allocation,
    })
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
