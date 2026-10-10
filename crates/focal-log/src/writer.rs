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

/// The most queued commands a writer admits (`WalWriterLimits::queue_items`).
pub const MAX_QUEUE_ITEMS: usize = 4096;
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
    /// Bytes of every frame the log holds from its base to its tail.
    pub physical_bytes: u64,
    /// Bytes of the frames that are live: at or above their group's floor.
    pub live_bytes: u64,
    /// Bytes checkpoints wrote: their retained records and their floors.
    pub checkpoint_bytes: u64,
    /// Segments removed because the base of the log left them.
    pub reclaimed_segments: u64,
    /// Bytes of the frames the base passed.
    pub reclaimed_bytes: u64,
    /// Live frames the base met, and the bytes they were written again as
    /// at the tail.
    pub relocated_records: u64,
    pub relocated_bytes: u64,
    /// How long its group commits took to be durable.
    pub syncs: crate::SyncLatency,
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

/// What the writer calls once an asynchronous append is answered, durable
/// or not: its owner's wake, so that an owner of many groups learns of the
/// answer when it is given instead of asking for it at intervals. Called on
/// the writer's thread: it does no more than send a signal.
pub type Persisted = Box<dyn FnOnce() + Send>;
enum Reply<T> {
    Blocking(mpsc::SyncSender<Result<T, LogError>>),
    Async(
        oneshot::Sender<Result<T, LogError>>,
        mpsc::SyncSender<()>,
        Option<Persisted>,
    ),
}
impl<T> Reply<T> {
    fn finish(self, value: Result<T, LogError>) {
        match self {
            Self::Blocking(sender) => {
                let _ = sender.try_send(value);
            }
            Self::Async(sender, completed, persisted) => {
                let _ = sender.send(value);
                let _ = completed.try_send(());
                if let Some(persisted) = persisted {
                    persisted();
                }
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
impl Batch {
    /// Tell the batch's caller it was refused or failed, once what the
    /// batch held is given back: its bytes of the volume, its memory and
    /// its slot. Told first, a caller that asked again at once, or looked at
    /// what was outstanding, found its own refused batch still holding them.
    fn refuse(self, error: LogError) {
        let Batch {
            reply,
            disk,
            encoded,
            index,
            _allocation,
            _slot,
            ..
        } = self;
        drop((disk, encoded, index, _allocation, _slot));
        reply.finish(Err(error));
    }
    /// Tell the batch's caller where its records are durable, once its
    /// bytes are charged to the volume and its memory and slot given back.
    fn done(self, position: DurablePosition) {
        let Batch {
            reply,
            disk,
            encoded,
            index,
            _allocation,
            _slot,
            ..
        } = self;
        disk.commit();
        drop((encoded, index, _allocation, _slot));
        reply.finish(Ok(position));
    }
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
    Logs(Reply<Vec<LogicalLogId>>, Allocation),
    #[cfg(any(test, feature = "test-support"))]
    Pause(mpsc::SyncSender<()>, mpsc::Receiver<()>),
    /// Clean until the base can move no further, then answer.
    #[cfg(test)]
    Settle(mpsc::SyncSender<()>),
    /// Whether the writer cleans while no command waits.
    #[cfg(test)]
    Idle(bool, mpsc::SyncSender<()>),
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
    /// The group's live frames in its order: ascending origins, within a
    /// chunk and from one chunk to the next.
    chunks: Vec<IndexChunk>,
    /// Frames counted toward the chunk that will hold them, before it is
    /// allocated.
    expected: usize,
    /// The chunk being packed, exactly `expected` frames.
    open: Option<IndexChunk>,
    /// The group's floor: every frame whose origin is before it is dead.
    floor: u64,
    _allocation: Allocation,
}
/// What a frame takes on disk.
fn frame_bytes(length: usize) -> u64 {
    (FRAME_HEADER as u64).saturating_add(length as u64)
}
/// What a segment holds from the base on: every frame's bytes, and how
/// much of it is live.
struct SegmentUse {
    total_bytes: u64,
    live_frames: u64,
    live_bytes: u64,
    _allocation: Allocation,
}
/// What one segment's row costs the index.
const SEGMENT_ROW_BYTES: usize = 96;
struct RecoveryIndex {
    groups: BTreeMap<LogicalLogId, GroupIndex>,
    /// One row a segment of the durable prefix that holds a frame.
    segments: BTreeMap<u64, SegmentUse>,
    /// Rows reserved ahead of a write for the segments it may open, so
    /// publishing what was written never fails for memory.
    spare_segments: Vec<Allocation>,
    /// Bytes of every frame from the base to the tail, and of the live ones.
    physical_bytes: u64,
    live_bytes: u64,
    /// The first sequence a recovery scan reads: the one after the base.
    scan_start: u64,
    budget: MemoryBudget,
    max_groups: usize,
    records: usize,
}
impl RecoveryIndex {
    /// The logs the index holds, in order.
    fn logs(&self) -> Result<Vec<LogicalLogId>, LogError> {
        let mut logs = Vec::new();
        logs.try_reserve_exact(self.groups.len())
            .map_err(|_| LogError::Capacity)?;
        logs.extend(self.groups.keys().copied());
        Ok(logs)
    }
    fn new(budget: MemoryBudget, max_groups: usize) -> Self {
        Self {
            scan_start: 1,
            groups: BTreeMap::new(),
            segments: BTreeMap::new(),
            spare_segments: Vec::new(),
            physical_bytes: 0,
            live_bytes: 0,
            budget,
            max_groups,
            records: 0,
        }
    }
    /// Reserve the rows a write of `bytes` may need: one for each segment
    /// it can open, and the one it starts in. A write of nothing needs none.
    fn reserve_segments(&mut self, bytes: usize, segment_bytes: u64) -> Result<(), LogError> {
        if bytes == 0 {
            return Ok(());
        }
        let payload = segment_bytes.saturating_sub(HEADER_LEN).max(1);
        let rows = (bytes as u64)
            .checked_div(payload)
            .and_then(|rows| rows.checked_add(2))
            .and_then(|rows| usize::try_from(rows).ok())
            .ok_or(LogError::Capacity)?;
        let missing = rows.saturating_sub(self.spare_segments.len());
        self.spare_segments
            .try_reserve(missing)
            .map_err(|_| LogError::Capacity)?;
        for _ in 0..missing {
            self.spare_segments.push(reserve(
                &self.budget,
                BudgetKind::Index,
                BudgetLane::Completion,
                SEGMENT_ROW_BYTES,
            )?);
        }
        Ok(())
    }
    /// The row of `segment`, made from a reserved one when it is new.
    fn segment(&mut self, segment: u64) -> Result<&mut SegmentUse, LogError> {
        if !self.segments.contains_key(&segment) {
            let allocation = match self.spare_segments.pop() {
                Some(allocation) => allocation,
                None => reserve(
                    &self.budget,
                    BudgetKind::Index,
                    BudgetLane::Completion,
                    SEGMENT_ROW_BYTES,
                )?,
            };
            self.segments.insert(
                segment,
                SegmentUse {
                    total_bytes: 0,
                    live_frames: 0,
                    live_bytes: 0,
                    _allocation: allocation,
                },
            );
        }
        self.segments.get_mut(&segment).ok_or(LogError::Failed)
    }
    /// A frame is on disk: its bytes are the segment's, live or not.
    fn wrote(&mut self, frame: &FrameLocation, live: bool) -> Result<(), LogError> {
        let bytes = frame_bytes(frame.length);
        let row = self.segment(frame.segment)?;
        row.total_bytes = row.total_bytes.saturating_add(bytes);
        if live {
            row.live_frames = row.live_frames.saturating_add(1);
            row.live_bytes = row.live_bytes.saturating_add(bytes);
        }
        self.physical_bytes = self.physical_bytes.saturating_add(bytes);
        if live {
            self.live_bytes = self.live_bytes.saturating_add(bytes);
        }
        Ok(())
    }
    /// A frame is below its group's floor now: its segment holds it still,
    /// and nothing reads it again.
    fn retired(&mut self, frame: &FrameLocation) {
        let bytes = frame_bytes(frame.length);
        if let Some(row) = self.segments.get_mut(&frame.segment) {
            row.live_frames = row.live_frames.saturating_sub(1);
            row.live_bytes = row.live_bytes.saturating_sub(bytes);
        }
        self.live_bytes = self.live_bytes.saturating_sub(bytes);
    }
    /// The live frames `segment` holds from the base on.
    fn live_in(&self, segment: u64) -> u64 {
        self.segments.get(&segment).map_or(0, |row| row.live_frames)
    }
    /// The base passed `bytes` of `segment`'s frames.
    fn passed(&mut self, segment: u64, bytes: u64) {
        if let Some(row) = self.segments.get_mut(&segment) {
            row.total_bytes = row.total_bytes.saturating_sub(bytes);
        }
        self.physical_bytes = self.physical_bytes.saturating_sub(bytes);
    }
    /// The base left `segment`: what it still held is behind the base.
    /// Answers those bytes.
    fn left(&mut self, segment: u64) -> u64 {
        let bytes = self
            .segments
            .remove(&segment)
            .map_or(0, |row| row.total_bytes);
        self.physical_bytes = self.physical_bytes.saturating_sub(bytes);
        bytes
    }
    /// The index's row of the frame of `log` whose origin is `origin`.
    fn find(&mut self, log: LogicalLogId, origin: u64) -> Option<&mut FrameLocation> {
        let group = self.groups.get_mut(&log)?;
        // No chunk is empty, and origins ascend from one to the next.
        let after = group.chunks.partition_point(|chunk| {
            chunk
                .frames
                .first()
                .is_some_and(|frame| frame.origin <= origin)
        });
        let chunk = group.chunks.get_mut(after.checked_sub(1)?)?;
        let at = chunk
            .frames
            .binary_search_by_key(&origin, |frame| frame.origin)
            .ok()?;
        chunk.frames.get_mut(at)
    }
    /// Whether the live frame of `log` with `origin` is the one at `frame`.
    fn holds(&mut self, log: LogicalLogId, origin: u64, frame: &FrameLocation) -> bool {
        self.find(log, origin)
            .is_some_and(|row| row.segment == frame.segment && row.byte == frame.byte)
    }
    /// The live frame of `log` at `old` was written again at `new`.
    fn moved(
        &mut self,
        log: LogicalLogId,
        old: &FrameLocation,
        new: FrameLocation,
    ) -> Result<(), LogError> {
        self.wrote(&new, true)?;
        self.retired(old);
        let row = self.find(log, new.origin).ok_or(LogError::Failed)?;
        *row = new;
        Ok(())
    }
    /// The group's frames are replaced by `chunk`, its floor at `floor`:
    /// every frame it held before is retired.
    fn replace(
        &mut self,
        log: LogicalLogId,
        chunk: IndexChunk,
        floor: u64,
    ) -> Result<(), LogError> {
        let old = {
            let group = self.groups.get_mut(&log).ok_or(LogError::Failed)?;
            group.floor = floor;
            std::mem::take(&mut group.chunks)
        };
        for frame in old.iter().flat_map(|chunk| &chunk.frames) {
            self.retired(frame);
            self.records = self.records.saturating_sub(1);
        }
        drop(old);
        self.publish(log, chunk)
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
                expected: 0,
                open: None,
                floor: 0,
                _allocation: allocation,
            },
        );
        Ok(())
    }
    /// The first reading of a frame. A floor sets its group's floor, and
    /// its group's count to the frames it kept that the scan reads: a
    /// checkpoint's frames are the `term` just before its floor, from the
    /// sequence the floor names, and those the base has passed since were
    /// written again after the floor and are counted there. Any other
    /// frame counts toward its group.
    fn header(&mut self, header: FrameHeader, sequence: u64) -> Result<(), LogError> {
        if header.kind != RecordKind::Floor {
            return self.count(header.log, 1);
        }
        if header.index.checked_add(header.term) != Some(sequence) {
            return Err(LogError::Failed);
        }
        let passed = self
            .scan_start
            .saturating_sub(header.index)
            .min(header.term);
        let kept = header.term.saturating_sub(passed);
        self.ensure_group(header.log)?;
        let group = self.groups.get_mut(&header.log).ok_or(LogError::Failed)?;
        group.expected = usize::try_from(kept).map_err(|_| LogError::Capacity)?;
        group.floor = header.index;
        Ok(())
    }
    /// Count `frames` of `log` toward the one chunk that will hold the
    /// group when it is packed.
    fn count(&mut self, log: LogicalLogId, frames: usize) -> Result<(), LogError> {
        self.ensure_group(log)?;
        let group = self.groups.get_mut(&log).ok_or(LogError::Failed)?;
        group.expected = group
            .expected
            .checked_add(frames)
            .ok_or(LogError::Capacity)?;
        Ok(())
    }
    /// Allocate every counted group's chunk at exactly its count: the index
    /// of a history costs what its records cost — one chunk a group — never
    /// what the batches that wrote it cost, so a history admitted under a
    /// budget reopens under it (the audit's F46).
    fn pack(&mut self) -> Result<(), LogError> {
        for group in self.groups.values_mut() {
            if group.expected == 0 {
                continue;
            }
            let chunk = IndexChunk::allocate(&self.budget, group.expected, BudgetLane::Completion)?;
            group
                .chunks
                .try_reserve(1)
                .map_err(|_| LogError::Capacity)?;
            group.open = Some(chunk);
        }
        Ok(())
    }
    /// Place one frame into its group's packed chunk. A frame beyond the
    /// count is the durable prefix changing between the two readings — a
    /// failure of the writer's own premise, never capacity.
    fn place(
        &mut self,
        log: LogicalLogId,
        kind: RecordKind,
        frame: FrameLocation,
    ) -> Result<(), LogError> {
        let floor = self.groups.get(&log).ok_or(LogError::Failed)?.floor;
        // A floor, and a frame whose origin is before its group's floor,
        // are on disk and read by nothing.
        if kind == RecordKind::Floor || frame.origin < floor {
            return self.wrote(&frame, false);
        }
        self.wrote(&frame, true)?;
        let group = self.groups.get_mut(&log).ok_or(LogError::Failed)?;
        let chunk = group.open.as_mut().ok_or(LogError::Failed)?;
        if chunk.frames.len() >= group.expected {
            return Err(LogError::Failed);
        }
        chunk.frames.push(frame);
        Ok(())
    }
    /// Every packed chunk becomes its group's, in the group's order: a
    /// moved frame stands on disk after frames its group wrote later, and
    /// the order of a group is its origins'. One short of its count is the
    /// same failure as one beyond it, and so is an origin held twice.
    fn seal(&mut self) -> Result<(), LogError> {
        for group in self.groups.values_mut() {
            if let Some(mut chunk) = group.open.take() {
                if chunk.frames.len() != group.expected {
                    return Err(LogError::Failed);
                }
                chunk.frames.sort_unstable_by_key(|frame| frame.origin);
                if chunk
                    .frames
                    .windows(2)
                    .any(|pair| matches!(pair, [a, b] if a.origin >= b.origin))
                {
                    return Err(LogError::Failed);
                }
                self.records = self
                    .records
                    .checked_add(chunk.frames.len())
                    .ok_or(LogError::Capacity)?;
                group.chunks.push(chunk);
            }
            group.expected = 0;
        }
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
    /// The frames of `chunk` were written for `log` and are live.
    fn publish(&mut self, log: LogicalLogId, chunk: IndexChunk) -> Result<(), LogError> {
        if chunk.frames.is_empty() {
            return Ok(());
        }
        self.records = self
            .records
            .checked_add(chunk.frames.len())
            .ok_or(LogError::Capacity)?;
        for frame in &chunk.frames {
            self.wrote(frame, true)?;
        }
        let group = self.groups.get_mut(&log).ok_or(LogError::Failed)?;
        group.chunks.push(chunk);
        Ok(())
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
    /// The tail the last fence made durable: the base never passes it.
    fenced: DurablePosition,
    /// Bytes the callers' own commands wrote that cleaning has not spent,
    /// never more than a batch: what a commit's cleaning may write again,
    /// so that under load it writes no more than the callers did.
    credit: u64,
    /// A test holds the base still between its commands.
    #[cfg(test)]
    still: bool,
}
/// What one step of cleaning did.
struct Cleaned {
    /// The base before the step.
    from: DurableBase,
    /// The volume's promise for what the step wrote, to commit behind the
    /// fence.
    disk: Option<DiskReservation>,
    copied: u64,
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
        Self::open_as(directory, options, limits, budget, disk, false)
    }
    /// [`Self::open`] for a test that holds the base still from the
    /// writer's first instant: no step of idle cleaning runs between the
    /// open and the test's first command, however fast the machine is. A
    /// test that held it with a command after the open (`Command::Idle`)
    /// raced the writer, which cleans as soon as nothing waits: a reopen of
    /// a log with garbage behind its base moved the base by as many steps
    /// as the writer took before the command came (a reopen loop's base
    /// stood inside the checkpoint's frames no time in 400 rounds on a
    /// loaded Linux runner, CI run 38063080415).
    #[cfg(test)]
    pub(crate) fn open_still(
        directory: impl AsRef<Path>,
        options: WalOptions,
    ) -> Result<Self, LogError> {
        let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024)
            .map_err(|_| LogError::Capacity)?;
        let disk = DiskBudget::new(DiskBudgetConfig::default()).map_err(|_| LogError::Capacity)?;
        Self::open_as(
            directory,
            options,
            WalWriterLimits::default(),
            budget,
            disk,
            true,
        )
    }
    /// The writer of `directory`; `still` starts it holding the base still (tests alone).
    fn open_as(
        directory: impl AsRef<Path>,
        options: WalOptions,
        limits: WalWriterLimits,
        budget: MemoryBudget,
        disk: DiskBudget,
        still: bool,
    ) -> Result<Self, LogError> {
        #[cfg(not(test))]
        let _ = still;
        reject_replay_reentry()?;
        let directory_path = directory.as_ref().to_path_buf();
        if !(1..=MAX_QUEUE_ITEMS).contains(&limits.queue_items)
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
        // Two readings of the durable prefix: the first counts each group's
        // frames, the second places them into one chunk a group, sized
        // exactly — the index costs what the history's records cost, never
        // what the batches that wrote them cost (the audit's F46).
        let wal = Wal::open_indexed(directory, options.clone(), |event| match event {
            crate::ScanEvent::Base(base) => {
                index.scan_start = base.sequence.saturating_add(1);
                Ok(())
            }
            crate::ScanEvent::Header(header, sequence) => index.header(header, sequence),
            crate::ScanEvent::Counted => index.pack(),
            crate::ScanEvent::Frame(record, location) => {
                index.place(record.log, record.kind, location)
            }
        })?;
        index.seal()?;
        drop(scratch);
        let stats = WalWriterStats {
            startup_scan_records: u64::try_from(index.records).map_err(|_| LogError::Capacity)?,
            ..Default::default()
        };
        let fenced = wal.position();
        let writer = Writer {
            wal,
            index,
            leases: BTreeMap::new(),
            lease_generation: 0,
            limits,
            budget: budget.clone(),
            disk: disk.clone(),
            stats,
            fenced,
            credit: 0,
            #[cfg(test)]
            still,
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
    /// Every logical log the durable prefix holds a frame of, in order: what a reader of the whole
    /// WAL visits, a log at a time (the conversion to hyper-log, 27 §15.8). At most the writer's
    /// `max_groups`, the index's own bound.
    pub fn logs(&self) -> Result<Vec<LogicalLogId>, LogError> {
        reject_replay_reentry()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(Command::Logs(Reply::Blocking(sender), self.control_slot()?))?;
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
        // An append may be the batch that closes its group commit, so it promises that commit frame too (doc
        // 28); a checkpoint's is promised with its floor, since a checkpoint always closes its own.
        let disk = self.disk_reserve(
            if checkpoint {
                DiskKind::Checkpoint
            } else {
                DiskKind::Wal
            },
            lane,
            if checkpoint {
                bytes
            } else {
                bytes
                    .checked_add(COMMIT_FRAME_BYTES)
                    .ok_or(LogError::Capacity)?
            },
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
            // The floor and the moved frame are the physical layer's own.
            if record.log != log || matches!(record.kind, RecordKind::Floor | RecordKind::Moved) {
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
                | RecordKind::FastTrack
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
        self.batch_async_in(records, lane, false, None)
    }
    /// The same append, with what the writer calls once it is answered
    /// ([`Persisted`]). An append the writer never took calls nothing: its
    /// refusal is this call's.
    pub fn append_async_notified(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
        persisted: Option<Persisted>,
    ) -> Result<WalAppend, LogError> {
        self.batch_async_in(records, lane, false, persisted)
    }
    /// Queues the same checkpoint as the synchronous API. The receipt
    /// resolves only after its CURRENT fence; dropping it does not cancel an
    /// admitted checkpoint or release the writer's owned batch permits.
    pub fn rewrite_checkpoint_async_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
    ) -> Result<WalAppend, LogError> {
        self.batch_async_in(records, lane, true, None)
    }
    /// The same checkpoint, with what the writer calls once it is answered
    /// ([`Persisted`]), as [`Self::append_async_notified`].
    pub fn rewrite_checkpoint_async_notified(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
        persisted: Option<Persisted>,
    ) -> Result<WalAppend, LogError> {
        self.batch_async_in(records, lane, true, persisted)
    }
    fn batch_async_in(
        &mut self,
        records: &[Record],
        lane: BudgetLane,
        checkpoint: bool,
        persisted: Option<Persisted>,
    ) -> Result<WalAppend, LogError> {
        let allocation = reserve(&self.shared.0.budget, BudgetKind::Pending, lane, 1024)?;
        let (sender, receiver) = oneshot::channel();
        let (completed, completion) = mpsc::sync_channel(1);
        self.shared.batch(
            self.log,
            self.generation,
            records,
            Reply::Async(sender, completed, persisted),
            checkpoint,
            lane,
        )?;
        Ok(WalAppend {
            receiver: Some(receiver),
            completed: completion,
            _allocation: allocation,
        })
    }
    /// Replace everything this group holds by `records`: they are written
    /// at the tail with the floor that retires every frame the group held
    /// before, in one group commit behind one fence. No other group's frame
    /// is read or written for it, and the volume is asked for these records
    /// and the floor alone; the frames it retired are freed as the base of
    /// the log reaches them.
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
                None => match receiver.try_recv() {
                    Ok(command) => command,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                    Err(mpsc::TryRecvError::Empty) => {
                        // Nothing waits: the base moves while it can, one
                        // bounded step between two looks at the queue.
                        if self.clean_idle() {
                            continue;
                        }
                        match receiver.recv() {
                            Ok(command) => command,
                            Err(_) => break,
                        }
                    }
                },
            };
            match command {
                Command::Append(first) => {
                    let mut batches = Vec::new();
                    if batches
                        .try_reserve_exact(self.limits.max_batch_requests)
                        .is_err()
                    {
                        first.refuse(LogError::Capacity);
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
                Command::Lease(log, reply, slot) => {
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
                    drop(slot);
                    reply.finish(result);
                }
                // A command's slot is given back before its caller is
                // answered, as a batch's is (`Batch::refuse`): one that is
                // answered and asks again at once finds its own slot free.
                Command::Identity(reply, slot) => {
                    drop(slot);
                    reply.finish(if self.wal.failed {
                        Err(LogError::Failed)
                    } else {
                        Ok(self.wal.options.identity)
                    });
                }
                Command::Logs(reply, slot) => {
                    drop(slot);
                    reply.finish(if self.wal.failed {
                        Err(LogError::Failed)
                    } else {
                        self.index.logs()
                    });
                }
                Command::Stats(reply, slot) => {
                    drop(slot);
                    reply.finish(Ok(WalWriterStats {
                        indexed_records: self.index.records,
                        physical_bytes: self.index.physical_bytes,
                        live_bytes: self.index.live_bytes,
                        syncs: self.wal.syncs,
                        ..self.stats
                    }));
                }
                Command::Fault(point, reply, slot) => {
                    drop(slot);
                    if self.wal.failed {
                        reply.finish(Err(LogError::Failed));
                    } else {
                        self.wal.inject_fault_once(point);
                        reply.finish(Ok(()));
                    }
                }
                Command::Replay(log, generation, reply, slot) => {
                    let result = self.replay(log, generation, &reply);
                    if matches!(result, Err(LogError::Io(_) | LogError::Corruption { .. })) {
                        self.wal.failed = true;
                    }
                    drop(slot);
                    let _ = reply.send(ReplayItem::Complete(result));
                }
                #[cfg(any(test, feature = "test-support"))]
                Command::Pause(entered, resume) => {
                    let _ = entered.send(());
                    let _ = resume.recv();
                }
                #[cfg(test)]
                Command::Settle(done) => {
                    let still = std::mem::replace(&mut self.still, false);
                    while self.clean_idle() {}
                    self.still = still;
                    let _ = done.send(());
                }
                #[cfg(test)]
                Command::Idle(idle, done) => {
                    self.still = !idle;
                    let _ = done.send(());
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
                batch.refuse(LogError::Capacity);
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
                    batch.refuse(error);
                }
            }
        }
        if self.wal.failed {
            for (batch, _) in prepared {
                batch.refuse(LogError::Failed);
            }
            return;
        }
        if prepared.is_empty() {
            return;
        }
        let written_bytes = prepared.iter().fold(0usize, |total, (batch, _)| {
            total.saturating_add(batch.bytes)
        });
        // The rows of the segments this write may open are reserved before a
        // byte is written: a shortage fails these batches and nothing else.
        if self
            .index
            .reserve_segments(written_bytes, self.wal.options.segment_bytes)
            .is_err()
        {
            for (batch, _) in prepared {
                batch.refuse(LogError::Capacity);
            }
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
            // Cleaning rides the callers' commit: what they wrote is what
            // it may write again, behind the same fence.
            self.earn(written_bytes);
            let cleaned = self.clean(self.credit)?;
            if record_count != 0 || cleaned.from != self.wal.base {
                self.wal.finish_append()?;
                self.count_commit()?;
            }
            self.stats.appended_records = appended_records;
            self.stats.group_commits = group_commits;
            Ok((self.wal.position, cleaned))
        })();
        match result {
            Ok((position, cleaned)) => {
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
                    if self.wal.failed {
                        batch.refuse(failure.take().unwrap_or(LogError::Failed));
                    } else {
                        // The bytes are behind the durable fence: charge them
                        // to the volume rather than returning the promise.
                        batch.done(position);
                    }
                }
                if !self.wal.failed {
                    self.cleaned(cleaned);
                }
            }
            Err(error) => {
                self.wal.failed = true;
                let mut cause = Some(error);
                for (batch, _) in prepared {
                    batch.refuse(cause.take().unwrap_or(LogError::Failed));
                }
            }
        }
    }
    /// A group's checkpoint: what it keeps, written at the tail, and the
    /// floor that retires every frame it held before — one group commit,
    /// behind one fence. Nothing of any other group is read or written for
    /// it, and the volume is asked for the checkpoint's own bytes alone
    /// (the audit's F14). What the floor retired is what the base may pass
    /// in the same commit.
    fn checkpoint(&mut self, mut batch: Batch) {
        let prepared = (|| {
            self.valid(batch.log, batch.generation)?;
            let chunk = batch.index.take().ok_or(LogError::Failed)?;
            self.index.reserve_slot(batch.log, 1)?;
            let first = self
                .wal
                .position
                .sequence
                .checked_add(1)
                .ok_or(LogError::Capacity)?;
            let floor = floor_frame(batch.log, first, batch.encoded.len())?;
            let floor_bytes = FRAME_HEADER
                .checked_add(floor.len())
                .ok_or(LogError::Capacity)?;
            let bytes = batch
                .bytes
                .checked_add(floor_bytes)
                .ok_or(LogError::Capacity)?;
            self.index
                .reserve_segments(bytes, self.wal.options.segment_bytes)?;
            let disk = self
                .disk
                .reserve(
                    DiskKind::Checkpoint,
                    BudgetLane::Completion,
                    floor_bytes.saturating_add(COMMIT_FRAME_BYTES) as u64,
                )
                .map_err(|_| LogError::Capacity)?;
            Ok((chunk, first, floor, bytes, disk))
        })();
        let (mut chunk, first, floor, bytes, floor_disk) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                // Nothing was written: the refusal is this batch's alone.
                batch.refuse(error);
                return;
            }
        };
        let log = batch.log;
        let written = (|| {
            self.wal.write_encoded_indexed(&batch.encoded, |location| {
                chunk.frames.push(location);
                Ok(())
            })?;
            let floor = self.wal.write_frame(&floor)?;
            self.index.wrote(&floor, false)?;
            // The floor stands in the index before the base moves: the
            // frames it retired are passed, never written again.
            self.index.replace(log, chunk, first)?;
            self.earn(bytes);
            let cleaned = self.clean(self.credit)?;
            let position = self.wal.finish_append()?;
            self.count_commit()?;
            Ok((position, cleaned))
        })();
        match written {
            Ok((position, cleaned)) => {
                self.stats.checkpoint_bytes =
                    self.stats.checkpoint_bytes.saturating_add(bytes as u64);
                self.stats.group_commits = self.stats.group_commits.saturating_add(1);
                floor_disk.commit();
                batch.done(position);
                self.cleaned(cleaned);
            }
            Err(error) => {
                // Bytes may be on disk without their fence, and the index
                // may say more than the log holds: only a reopen recovers.
                self.wal.failed = true;
                drop(floor_disk);
                batch.refuse(error);
            }
        }
    }
    /// What one step of cleaning may write again: a batch, and the header
    /// and wrapper of one frame, so that the largest record a batch admits
    /// can always be moved.
    fn step_bytes(&self) -> u64 {
        (self.wal.options.max_batch_bytes as u64)
            .saturating_add(FRAME_HEADER as u64)
            .saturating_add(crate::MOVED_OVERHEAD as u64)
    }
    /// The callers wrote `bytes`: cleaning may write as much again, and
    /// never banks more than a step.
    fn earn(&mut self, bytes: usize) {
        self.credit = self
            .credit
            .saturating_add(bytes as u64)
            .min(self.step_bytes());
    }
    /// Whether the log holds more dead bytes than live ones and a segment:
    /// the point past which a whole pass over it, which writes the live
    /// bytes once, frees more than it writes.
    fn due(&self) -> bool {
        let live = self.index.live_bytes;
        self.index.physical_bytes.saturating_sub(live)
            > live.saturating_add(self.wal.options.segment_bytes)
    }
    /// The commit frame the group commit just wrote is the segment's bytes, live in nothing.
    fn count_commit(&mut self) -> Result<(), LogError> {
        match self.wal.take_commit() {
            Some(commit) => self.index.wrote(&commit, false),
            None => Ok(()),
        }
    }

    /// One bounded step of cleaning: the base moves toward the tail over
    /// frames the last fence made durable. A segment that holds nothing
    /// live is left without being read. While the log is due, the frames
    /// the base meets are read and verified, and each live one is written
    /// again at the tail under its origin — at most `copy` bytes of them,
    /// for at most one batch of bytes read. Nothing of it is durable, and
    /// no segment is removed, before the caller's fence: the base and the
    /// frames written again become durable together.
    fn clean(&mut self, copy: u64) -> Result<Cleaned, LogError> {
        let from = self.wal.base;
        let fenced = self.fenced;
        let generation = self.wal.position.generation;
        let mut cleaned = Cleaned {
            from,
            disk: None,
            copied: 0,
        };
        let mut cursor = from;
        let mut read = self.wal.options.max_batch_bytes as u64;
        let mut promised = 0u64;
        let mut file: Option<(u64, File)> = None;
        while cursor.sequence < fenced.sequence {
            if self.index.live_in(cursor.segment) == 0 {
                if cursor.segment < fenced.segment {
                    let next = cursor.segment.checked_add(1).ok_or(LogError::Capacity)?;
                    let bytes = self.index.left(cursor.segment);
                    self.stats.reclaimed_bytes = self.stats.reclaimed_bytes.saturating_add(bytes);
                    cursor =
                        segment_base(&self.wal.directory, &self.wal.options, generation, next)?;
                    file = None;
                } else {
                    // What the fence made durable of the tail is all dead.
                    let bytes = fenced.byte.saturating_sub(cursor.byte);
                    self.index.passed(cursor.segment, bytes);
                    self.stats.reclaimed_bytes = self.stats.reclaimed_bytes.saturating_add(bytes);
                    cursor = DurableBase {
                        segment: fenced.segment,
                        byte: fenced.byte,
                        sequence: fenced.sequence,
                        checksum: fenced.checksum,
                    };
                }
                continue;
            }
            if !self.due() || read == 0 {
                break;
            }
            let frame = match read_at(
                &self.wal.directory,
                &self.wal.options,
                generation,
                cursor,
                &mut file,
                &self.budget,
            ) {
                Ok(Some(frame)) => frame,
                // The index counts a live frame in a segment that has no
                // frame left: the writer's own premise failed.
                Ok(None) => return Err(LogError::Failed),
                // No memory for the frame now: the base waits where it is.
                Err(LogError::Capacity) => break,
                Err(error) => return Err(error),
            };
            let location = frame.location;
            let total = frame_bytes(location.length);
            if frame.commit {
                // A commit frame closed a group commit the base has passed: nothing in it lives.
                self.index.passed(location.segment, total);
                self.stats.reclaimed_bytes = self.stats.reclaimed_bytes.saturating_add(total);
                read = read.saturating_sub(total);
                cursor = DurableBase {
                    segment: location.segment,
                    byte: location.byte.checked_add(total).ok_or(LogError::Capacity)?,
                    sequence: location.sequence,
                    checksum: location.checksum,
                };
                continue;
            }
            let (header, _) = postcard::take_from_bytes::<FrameHeader>(&frame.bytes)?;
            let origin = if header.kind == RecordKind::Moved {
                header.index
            } else {
                location.sequence
            };
            if header.kind != RecordKind::Floor && self.index.holds(header.log, origin, &location) {
                let wrapped;
                let bytes = if header.kind == RecordKind::Moved {
                    &frame.bytes
                } else {
                    wrapped = moved_frame(header.log, origin, &frame.bytes)?;
                    &wrapped
                };
                let cost = frame_bytes(bytes.len());
                let spent = cleaned.copied.checked_add(cost).ok_or(LogError::Capacity)?;
                if spent > copy {
                    break;
                }
                if cleaned.disk.is_none() {
                    // One promise for what the step may write, no larger
                    // than the volume has for completing work.
                    let directory = &self.wal.directory;
                    self.disk
                        .refresh_with(|| focal_platform::available_space(directory));
                    promised = copy.min(self.disk.available(BudgetLane::Completion));
                    if promised >= cost {
                        cleaned.disk = self
                            .disk
                            .reserve(DiskKind::Wal, BudgetLane::Completion, promised)
                            .ok();
                    }
                }
                if cleaned.disk.is_none()
                    || spent > promised
                    || self
                        .index
                        .reserve_segments(bytes.len(), self.wal.options.segment_bytes)
                        .is_err()
                {
                    break;
                }
                let mut new = self.wal.write_frame(bytes)?;
                new.origin = origin;
                self.index.moved(header.log, &location, new)?;
                cleaned.copied = spent;
                self.stats.relocated_records = self.stats.relocated_records.saturating_add(1);
                self.stats.relocated_bytes = self.stats.relocated_bytes.saturating_add(cost);
            }
            self.index.passed(location.segment, total);
            self.stats.reclaimed_bytes = self.stats.reclaimed_bytes.saturating_add(total);
            read = read.saturating_sub(total);
            cursor = DurableBase {
                segment: location.segment,
                byte: location.byte.checked_add(total).ok_or(LogError::Capacity)?,
                sequence: location.sequence,
                checksum: location.checksum,
            };
        }
        if let Some(disk) = cleaned.disk.as_mut() {
            disk.shrink_to(cleaned.copied)
                .map_err(|_| LogError::Failed)?;
        }
        if cursor != from {
            self.wal.set_base(cursor)?;
        }
        Ok(cleaned)
    }
    /// A fence made a commit and its step of cleaning durable: the tail it
    /// names is what the base may reach next, what the step wrote is the
    /// volume's, the segments behind the base are removed, and the rows
    /// reserved for segments the commit did not open are returned.
    fn cleaned(&mut self, cleaned: Cleaned) {
        self.index.spare_segments.clear();
        self.fenced = self.wal.position;
        self.credit = self.credit.saturating_sub(cleaned.copied);
        if let Some(disk) = cleaned.disk {
            disk.commit();
        }
        let retired = self.wal.base.segment.saturating_sub(cleaned.from.segment);
        if retired != 0 && self.wal.retire_segments().is_ok() {
            self.stats.reclaimed_segments = self.stats.reclaimed_segments.saturating_add(retired);
        }
    }
    /// A step of cleaning on its own, while no command waits: up to a
    /// batch written again, behind a fence of its own. Answers whether the
    /// base moved.
    fn clean_idle(&mut self) -> bool {
        if self.wal.failed {
            return false;
        }
        #[cfg(test)]
        if self.still {
            return false;
        }
        let step = (|| {
            let cleaned = self.clean(self.step_bytes())?;
            if cleaned.from == self.wal.base {
                return Ok(None);
            }
            self.wal.finish_append()?;
            self.count_commit()?;
            Ok(Some(cleaned))
        })();
        match step {
            Ok(Some(cleaned)) => {
                self.stats.group_commits = self.stats.group_commits.saturating_add(1);
                self.cleaned(cleaned);
                !self.wal.failed
            }
            Ok(None) => false,
            Err::<_, LogError>(_) => {
                self.wal.failed = true;
                false
            }
        }
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
/// One frame's record bytes, verified against the chain: its length,
/// sequence, predecessor and checksum.
struct RecoveredFrame {
    location: FrameLocation,
    bytes: Vec<u8>,
    /// A commit frame (doc 28): it closes a group commit and is no record.
    commit: bool,
    _allocation: Allocation,
}
/// The group's floor as a frame: every frame of `log` whose origin is
/// before `first` is dead, and the `count` frames before this one are what
/// the group keeps.
fn floor_frame(log: LogicalLogId, first: u64, count: usize) -> Result<Vec<u8>, LogError> {
    Ok(postcard::to_stdvec(&Record {
        log,
        kind: RecordKind::Floor,
        index: first,
        term: u64::try_from(count).map_err(|_| LogError::Capacity)?,
        payload: Vec::new(),
    })?)
}
/// The segment's file, opened once while consecutive reads stay in it.
fn segment_file<'a>(
    directory: &Path,
    generation: u64,
    segment: u64,
    current: &'a mut Option<(u64, File)>,
) -> Result<&'a mut File, LogError> {
    if current.as_ref().is_none_or(|(held, _)| *held != segment) {
        *current = Some((
            segment,
            File::open(segment_path(directory, generation, segment))?,
        ));
    }
    current
        .as_mut()
        .map(|(_, file)| file)
        .ok_or(LogError::Failed)
}
/// A frame's bytes after its header, under a permit for them and for the
/// record decoded or wrapped from them.
fn read_body(
    file: &mut File,
    header: &[u8; FRAME_HEADER],
    length: usize,
    budget: &MemoryBudget,
) -> Result<(Vec<u8>, Allocation, u32), LogError> {
    let amount = length
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(LogError::Capacity)?;
    let allocation = reserve(budget, BudgetKind::Recovery, BudgetLane::Completion, amount)?;
    let mut data = Vec::new();
    data.try_reserve_exact(length)
        .map_err(|_| LogError::Capacity)?;
    data.resize(length, 0);
    file.read_exact(&mut data)?;
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(header.get(..16).ok_or(LogError::Capacity)?);
    checksum.update(&data);
    Ok((data, allocation, checksum.finalize()))
}
/// The frame the index located, verified against what it recorded.
fn read_frame(
    directory: &Path,
    location: &FrameLocation,
    current: &mut Option<(u64, File)>,
    budget: &MemoryBudget,
) -> Result<RecoveredFrame, LogError> {
    let file = segment_file(directory, location.generation, location.segment, current)?;
    let path = || segment_path(directory, location.generation, location.segment);
    file.seek(SeekFrom::Start(location.byte))?;
    let mut header = [0; FRAME_HEADER];
    file.read_exact(&mut header)?;
    if read_u32(&header, 0..4)? as usize != location.length
        || read_u64(&header, 4..12)? != location.sequence
        || read_u32(&header, 12..16)? != location.previous
        || read_u32(&header, 16..20)? != location.checksum
    {
        return Err(corrupt(
            &path(),
            location.byte,
            "indexed durable frame changed",
        ));
    }
    let (bytes, allocation, checksum) = read_body(file, &header, location.length, budget)?;
    if checksum != location.checksum {
        return Err(corrupt(
            &path(),
            location.byte,
            "indexed durable checksum mismatch",
        ));
    }
    Ok(RecoveredFrame {
        location: *location,
        bytes,
        commit: false,
        _allocation: allocation,
    })
}
/// The frame at the base's place, verified against the chain state the
/// base carries; `None` at the end of its segment.
fn read_at(
    directory: &Path,
    options: &WalOptions,
    generation: u64,
    base: DurableBase,
    current: &mut Option<(u64, File)>,
    budget: &MemoryBudget,
) -> Result<Option<RecoveredFrame>, LogError> {
    let file = segment_file(directory, generation, base.segment, current)?;
    let path = || segment_path(directory, generation, base.segment);
    let end = file.metadata()?.len();
    if base.byte == end {
        return Ok(None);
    }
    let room = end
        .checked_sub(base.byte)
        .and_then(|room| room.checked_sub(FRAME_HEADER as u64))
        .ok_or_else(|| corrupt(&path(), base.byte, "incomplete durable frame header"))?;
    file.seek(SeekFrom::Start(base.byte))?;
    let mut header = [0; FRAME_HEADER];
    file.read_exact(&mut header)?;
    let field = read_u32(&header, 0..4)?;
    let commit = field & COMMIT_FLAG != 0;
    let length = (field & !COMMIT_FLAG) as usize;
    let sequence = base.sequence.checked_add(1).ok_or(LogError::Capacity)?;
    if (commit && length != 0 && length != COMMIT_PAYLOAD)
        || length > crate::frame_limit(options)
        || length as u64 > room
        || read_u64(&header, 4..12)? != sequence
        || read_u32(&header, 12..16)? != base.checksum
    {
        return Err(corrupt(
            &path(),
            base.byte,
            "frame sequence or predecessor mismatch",
        ));
    }
    let (bytes, allocation, checksum) = read_body(file, &header, length, budget)?;
    if checksum != read_u32(&header, 16..20)? {
        return Err(corrupt(
            &path(),
            base.byte,
            "durable frame checksum mismatch",
        ));
    }
    Ok(Some(RecoveredFrame {
        location: FrameLocation {
            generation,
            segment: base.segment,
            byte: base.byte,
            length,
            sequence,
            origin: sequence,
            previous: base.checksum,
            checksum,
        },
        bytes,
        commit,
        _allocation: allocation,
    }))
}
fn read_indexed(
    directory: &Path,
    location: &FrameLocation,
    current: &mut Option<(u64, File)>,
    budget: &MemoryBudget,
) -> Result<RecoveredRecord, LogError> {
    let RecoveredFrame {
        bytes,
        _allocation: allocation,
        ..
    } = read_frame(directory, location, current, budget)?;
    let (record, origin) = decode_frame(&bytes).map_err(|_| {
        corrupt(
            &segment_path(directory, location.generation, location.segment),
            location.byte,
            "invalid indexed record",
        )
    })?;
    if origin.unwrap_or(location.sequence) != location.origin {
        return Err(corrupt(
            &segment_path(directory, location.generation, location.segment),
            location.byte,
            "indexed frame origin changed",
        ));
    }
    Ok(RecoveredRecord {
        record,
        _allocation: allocation,
    })
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
