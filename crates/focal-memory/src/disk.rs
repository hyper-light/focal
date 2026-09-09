//! Disk admission: a bounded envelope of free bytes on one volume that every
//! durable owner (log writer, content store, checkpoint writer) draws from
//! before it acknowledges work, so concurrent admissions cannot together
//! promise more space than the volume has, and a watermark of headroom is
//! never spent on ordinary work.
//!
//! The budget performs no IO: an owner samples the volume (with a bounded
//! frequency the budget decides) and reports the free bytes it observed. A
//! reservation covers the bytes between admission and durability; committing
//! it after the durable fence charges them against the last sample, dropping
//! it uncommitted returns them. The next sample replaces the estimate.
//!
//! One budget is shared by the physical owners of a volume, which run on
//! different threads with independent lifetimes; the counters are atomic and
//! the shared handle is the same kind of boundary as [`crate::MemoryBudget`].
use crate::{BudgetLane, MemoryError};
use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
};

/// What a reservation is for; reported per kind, never a separate limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum DiskKind {
    Wal,
    Checkpoint,
    Content,
    Archive,
    Staging,
}
pub const DISK_KIND_COUNT: usize = 5;

/// The volume policy: `headroom` is never spent (the watermark below which
/// the node refuses fresh work), `completion_reserve` is spendable only by
/// completion-lane work, and `sample_interval` bounds how many admissions may
/// rely on one sample far above the watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskBudgetConfig {
    pub headroom: u64,
    pub completion_reserve: u64,
    pub sample_interval: u32,
}
impl Default for DiskBudgetConfig {
    fn default() -> Self {
        Self {
            headroom: 64 * 1024 * 1024,
            completion_reserve: 16 * 1024 * 1024,
            sample_interval: 32,
        }
    }
}
impl DiskBudgetConfig {
    /// No watermark and no sampling requirement: every reservation is
    /// admitted and only the accounting remains. For owners that stand alone.
    pub fn unbounded() -> Self {
        Self {
            headroom: 0,
            completion_reserve: 0,
            sample_interval: u32::MAX,
        }
    }
}

const UNKNOWN: u64 = u64::MAX;

struct Counters {
    config: DiskBudgetConfig,
    /// Free bytes at the last sample, `UNKNOWN` before the first or after a
    /// failed one; committed reservations lower it until the next sample.
    free: AtomicU64,
    outstanding: AtomicU64,
    ordinary: AtomicU64,
    admissions: AtomicU32,
    kinds: [AtomicU64; DISK_KIND_COUNT],
}

/// A shared handle to one volume's admission envelope.
#[derive(Clone)]
pub struct DiskBudget(Arc<Counters>);

impl std::fmt::Debug for DiskBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskBudget")
            .field("stats", &self.stats())
            .finish()
    }
}

/// A bounded view of the envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskStats {
    /// `None` until a sample is known.
    pub free: Option<u64>,
    pub outstanding: u64,
    pub ordinary_outstanding: u64,
    pub headroom: u64,
    pub completion_reserve: u64,
    pub by_kind: [u64; DISK_KIND_COUNT],
}

impl DiskBudget {
    pub fn new(config: DiskBudgetConfig) -> Result<Self, MemoryError> {
        if config.sample_interval == 0 {
            return Err(MemoryError::InvalidConfiguration(
                "disk sample interval must be positive",
            ));
        }
        if config
            .headroom
            .checked_add(config.completion_reserve)
            .is_none()
        {
            return Err(MemoryError::InvalidConfiguration(
                "disk headroom and reserve overflow",
            ));
        }
        Ok(Self(Arc::new(Counters {
            config,
            free: AtomicU64::new(UNKNOWN),
            outstanding: AtomicU64::new(0),
            ordinary: AtomicU64::new(0),
            admissions: AtomicU32::new(0),
            kinds: [const { AtomicU64::new(0) }; DISK_KIND_COUNT],
        })))
    }
    pub fn config(&self) -> DiskBudgetConfig {
        self.0.config
    }
    /// Whether the next admission should first sample the volume: no sample
    /// is known, the last one served its bounded run of admissions, or the
    /// estimate is within twice the headroom of the watermark.
    pub fn sample_due(&self) -> bool {
        let free = self.0.free.load(Ordering::Acquire);
        if free == UNKNOWN {
            return true;
        }
        if self.0.admissions.load(Ordering::Acquire) >= self.0.config.sample_interval {
            return true;
        }
        let headroom = self.0.config.headroom;
        headroom != 0
            && free.saturating_sub(self.0.outstanding.load(Ordering::Acquire))
                < headroom.saturating_mul(2)
    }
    /// Record the free bytes an owner observed on the volume.
    pub fn observe(&self, free: u64) {
        self.0
            .free
            .store(free.min(UNKNOWN.saturating_sub(1)), Ordering::Release);
        self.0.admissions.store(0, Ordering::Release);
    }
    /// Forget the estimate: fresh work is refused until the next observation.
    pub fn forget(&self) {
        self.0.free.store(UNKNOWN, Ordering::Release);
    }
    /// Sample through the owner's probe when due. Returns whether an estimate
    /// is known afterwards; a failed probe refuses fresh work rather than
    /// promising space it cannot see.
    pub fn refresh_with(&self, probe: impl FnOnce() -> Option<u64>) -> bool {
        if self.sample_due() {
            match probe() {
                Some(free) => self.observe(free),
                None => self.forget(),
            }
        }
        self.0.free.load(Ordering::Acquire) != UNKNOWN
    }
    /// Free bytes not promised to any outstanding reservation, or zero while
    /// no estimate is known. This is the figure a node reports as its disk
    /// availability and the one watermarks compare against.
    pub fn uncommitted_free(&self) -> u64 {
        let free = self.0.free.load(Ordering::Acquire);
        if free == UNKNOWN {
            return 0;
        }
        free.saturating_sub(self.0.outstanding.load(Ordering::Acquire))
    }
    /// Bytes a reservation in `lane` may still take.
    pub fn available(&self, lane: BudgetLane) -> u64 {
        self.available_from(
            self.0.free.load(Ordering::Acquire),
            self.0.outstanding.load(Ordering::Acquire),
            lane,
        )
    }
    fn available_from(&self, free: u64, outstanding: u64, lane: BudgetLane) -> u64 {
        let config = &self.0.config;
        if free == UNKNOWN {
            // Without a watermark there is nothing to protect and no sample
            // is required; with one, unknown space admits nothing fresh.
            return if config.headroom == 0 { u64::MAX } else { 0 };
        }
        let protected = match lane {
            BudgetLane::Completion => config.headroom,
            BudgetLane::Ordinary => config.headroom.saturating_add(config.completion_reserve),
        };
        free.saturating_sub(outstanding).saturating_sub(protected)
    }
    /// Promise `bytes` of the volume to one durable write. Refused with the
    /// bytes actually available to the lane; nothing is retained on refusal.
    pub fn reserve(
        &self,
        kind: DiskKind,
        lane: BudgetLane,
        bytes: u64,
    ) -> Result<DiskReservation, MemoryError> {
        let mut outstanding = self.0.outstanding.load(Ordering::Acquire);
        loop {
            let free = self.0.free.load(Ordering::Acquire);
            let available = self.available_from(free, outstanding, lane);
            if bytes > available {
                return Err(MemoryError::DiskCapacity {
                    requested: bytes,
                    available,
                });
            }
            let next = outstanding
                .checked_add(bytes)
                .ok_or(MemoryError::CounterExhausted("disk reservations"))?;
            match self.0.outstanding.compare_exchange_weak(
                outstanding,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => outstanding = current,
            }
        }
        if lane == BudgetLane::Ordinary {
            self.0.ordinary.fetch_add(bytes, Ordering::AcqRel);
        }
        self.kind_counter(kind).fetch_add(bytes, Ordering::AcqRel);
        self.0.admissions.fetch_add(1, Ordering::AcqRel);
        Ok(DiskReservation {
            budget: self.clone(),
            kind,
            lane,
            bytes,
        })
    }
    pub fn stats(&self) -> DiskStats {
        let free = self.0.free.load(Ordering::Acquire);
        let mut by_kind = [0; DISK_KIND_COUNT];
        for (slot, counter) in by_kind.iter_mut().zip(&self.0.kinds) {
            *slot = counter.load(Ordering::Acquire);
        }
        DiskStats {
            free: (free != UNKNOWN).then_some(free),
            outstanding: self.0.outstanding.load(Ordering::Acquire),
            ordinary_outstanding: self.0.ordinary.load(Ordering::Acquire),
            headroom: self.0.config.headroom,
            completion_reserve: self.0.config.completion_reserve,
            by_kind,
        }
    }
    fn kind_counter(&self, kind: DiskKind) -> &AtomicU64 {
        let [wal, checkpoint, content, archive, staging] = &self.0.kinds;
        match kind {
            DiskKind::Wal => wal,
            DiskKind::Checkpoint => checkpoint,
            DiskKind::Content => content,
            DiskKind::Archive => archive,
            DiskKind::Staging => staging,
        }
    }
    fn release(&self, kind: DiskKind, lane: BudgetLane, bytes: u64, committed: bool) {
        if bytes == 0 {
            return;
        }
        if committed {
            // The bytes are on the volume now: lower the estimate so the run
            // of admissions until the next sample sees them.
            let _ = self
                .0
                .free
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |free| {
                    (free != UNKNOWN).then(|| free.saturating_sub(bytes))
                });
        }
        self.0.outstanding.fetch_sub(bytes, Ordering::AcqRel);
        if lane == BudgetLane::Ordinary {
            self.0.ordinary.fetch_sub(bytes, Ordering::AcqRel);
        }
        self.kind_counter(kind).fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Bytes promised to one durable write. Drop returns them (the write did not
/// happen or failed); `commit` after the durable fence charges them.
pub struct DiskReservation {
    budget: DiskBudget,
    kind: DiskKind,
    lane: BudgetLane,
    bytes: u64,
}
impl std::fmt::Debug for DiskReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskReservation")
            .field("kind", &self.kind)
            .field("lane", &self.lane)
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl DiskReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn kind(&self) -> DiskKind {
        self.kind
    }
    /// The write reached its durable fence: charge the bytes to the volume.
    pub fn commit(mut self) {
        let bytes = std::mem::replace(&mut self.bytes, 0);
        self.budget.release(self.kind, self.lane, bytes, true);
    }
    /// Keep only `bytes` of the promise (a write that turned out smaller).
    pub fn shrink_to(&mut self, bytes: u64) -> Result<(), MemoryError> {
        let released = self
            .bytes
            .checked_sub(bytes)
            .ok_or(MemoryError::InvalidConfiguration(
                "disk reservation cannot grow",
            ))?;
        self.budget.release(self.kind, self.lane, released, false);
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for DiskReservation {
    fn drop(&mut self) {
        self.budget.release(self.kind, self.lane, self.bytes, false);
    }
}
