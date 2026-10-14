//! Rows read back from the replica's payload log (doc 31 M3).
//!
//! A claim's row and an authored definition are spilled there as their record encodings once a
//! candidate's checks have read them (`payloads::spill_changes`), as Kafka and NATS keep a
//! message in their logs rather than on the heap. A read that reaches one takes it back here:
//! decoded and checked as a replayed row is, charged while it is held, and lent for as long as
//! the read borrows the state, so every reader sees a held row and none changes. What is read
//! back is let go together the next time the state changes ([`ReadBack::release`]): no read
//! outlives that, and a row read back twice between two changes is decoded once. A spilled row's
//! bytes never change where they are, so a row read back is the same whichever read took it.
//!
//! A row that cannot be read back (the log's error, a decode refusal, no room for it) is never
//! taken for an absent one: the read is lent the spilled row as it is stored, which holds nothing
//! a reader can take, and the failure is kept until the operation asks for it
//! ([`ReadBack::failure`]) and fails with it.
//!
//! Reads of one state may run on several threads (a record's materialization validates in
//! parallel), so its tables are set once a slot, from any of them.
//!
//! A state that spills holds a small floor from the time its log is attached
//! ([`ReadBack::hold_floor`]): what is read back is charged to it first, and only past it to the
//! budget, so an operation's few reads (a delivery's claim and its definitions) never wait on
//! memory that work which must go on under pressure cannot take, and a refusal leaves the
//! budget as it was. Each release gives the floor back what reads took of it.
use super::payloads::PayloadRef;
use super::{NativeError, NativeLimits, Row};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

/// The slots of the first table; each later one has twice its predecessor's. Rows read back
/// between two changes of the state are an operation's own reads, so the first tables suffice.
const FIRST_SLOTS: usize = 16;
/// Tables at most: sixteen, doubling from sixteen slots, hold about half a million rows at half
/// load, past what [`READ_BACK_BYTES`] lets be read back.
const TABLES: usize = 16;
/// Most bytes the rows read back between two changes of the state may hold: past it a read back
/// is refused for capacity (retried, it meets a released table).
pub(super) const READ_BACK_BYTES: usize = 32 << 20;
/// The lane what is read back is charged in: it holds what was committed (or is pending on it),
/// and a session whose fresh admissions filled its ordinary lane must still read its claims to
/// complete them.
const LANE: BudgetLane = BudgetLane::Committed;
/// What a spilling state holds for reading back from the time its log is attached: a few
/// operations' reads (a claim read back is about 1.4 KB, a definition 0.6 KB, a table's first
/// sixteen slots 0.9 KB).
pub(super) const FLOOR: usize = 16 << 10;

type Slot = OnceLock<(PayloadRef, Row)>;

/// What a state reads back between two of its changes: open-addressed tables of slots set once,
/// each lending its row for the state's borrow. A table is filled to half its slots before the
/// next is made, and every table is made whole, so no slot moves while it is lent.
pub(super) struct ReadBack {
    /// The bounds a claim read back is built under: its session's.
    limits: NativeLimits,
    tables: [OnceLock<Vec<Slot>>; TABLES],
    used: [AtomicUsize; TABLES],
    /// What every table and every row read back is charged, together.
    held: Mutex<Option<Allocation>>,
    /// What of the floor reads have not taken ([`FLOOR`]).
    floor: Mutex<Option<Allocation>>,
    /// The first failure since the last time it was asked for.
    failure: Mutex<Option<NativeError>>,
}

impl std::fmt::Debug for ReadBack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadBack")
            .field("rows", &self.rows())
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl ReadBack {
    pub(super) fn new(limits: NativeLimits) -> Self {
        Self {
            limits,
            tables: std::array::from_fn(|_| OnceLock::new()),
            used: std::array::from_fn(|_| AtomicUsize::new(0)),
            held: Mutex::new(None),
            floor: Mutex::new(None),
            failure: Mutex::new(None),
        }
    }

    /// Holds the floor from `budget`, where none is held: refused, reads are charged to the
    /// budget as they come.
    pub(super) fn hold_floor(&mut self, budget: &MemoryBudget) {
        let Ok(floor) = self.floor.get_mut() else {
            return;
        };
        if floor.is_none() {
            *floor = budget
                .reserve(BudgetKind::Query, LANE, FLOOR)
                .ok()
                .map(|reserved| reserved.commit());
        }
    }

    /// `bytes` for a row or a table about to be read back: from what is left of the floor where
    /// it has them, else from `budget`.
    pub(super) fn fund(
        &self,
        budget: &MemoryBudget,
        bytes: usize,
    ) -> Result<Allocation, NativeError> {
        if let Ok(mut floor) = self.floor.lock()
            && let Some(floor) = floor.as_mut()
            && floor.bytes() >= bytes
        {
            return Ok(floor.split_off(bytes)?);
        }
        Ok(budget.reserve(BudgetKind::Query, LANE, bytes)?.commit())
    }

    /// Lets go of every row read back and what it was charged: the state is about to change, so
    /// nothing borrows them.
    pub(super) fn release(&mut self) {
        for (table, used) in self.tables.iter_mut().zip(&mut self.used) {
            table.take();
            *used.get_mut() = 0;
        }
        let Ok(held) = self.held.get_mut() else {
            return;
        };
        let Some(mut held) = held.take() else {
            return;
        };
        // What reads took of the floor goes back to it; the rest of their charge to the budget.
        if let Ok(Some(floor)) = self.floor.get_mut().map(Option::as_mut) {
            let taken = FLOOR.saturating_sub(floor.bytes()).min(held.bytes());
            if let Ok(mut back) = held.split_off(taken) {
                let _ = floor.absorb(&mut back);
            }
        }
    }

    /// The first read back that failed since the last call, which the operation that read fails
    /// with.
    pub(super) fn failure(&self) -> Result<(), NativeError> {
        match self.failure.lock() {
            Ok(mut failure) => failure.take().map_or(Ok(()), Err),
            Err(_) => Err(NativeError::Capacity("read back failure lock poisoned")),
        }
    }

    /// Keeps `error`, unless an earlier failure is kept.
    pub(super) fn fail(&self, error: NativeError) {
        if let Ok(mut failure) = self.failure.lock()
            && failure.is_none()
        {
            *failure = Some(error);
        }
    }

    /// Rows read back since the last release.
    pub(super) fn rows(&self) -> usize {
        self.used.iter().fold(0usize, |rows, used| {
            rows.saturating_add(used.load(Ordering::Relaxed))
        })
    }

    /// Bytes charged for what is read back since the last release.
    pub(super) fn bytes(&self) -> usize {
        self.held
            .lock()
            .ok()
            .and_then(|held| held.as_ref().map(Allocation::bytes))
            .unwrap_or(0)
    }

    /// The row read back from `at` since the last release.
    pub(super) fn find(&self, at: PayloadRef) -> Option<&Row> {
        for table in &self.tables {
            let slots = table.get()?;
            if let Some(row) = probe(slots, at) {
                return Some(row);
            }
        }
        None
    }

    /// The bounds a claim read back is built under.
    pub(super) fn limits(&self) -> NativeLimits {
        self.limits
    }

    /// Holds `row`, read back from `at` and charged `charge`, and lends it.
    pub(super) fn hold(
        &self,
        budget: &MemoryBudget,
        at: PayloadRef,
        row: Row,
        charge: Allocation,
    ) -> Result<&Row, NativeError> {
        self.within(charge.bytes())?;
        let (slots, used) = self.table(budget)?;
        let mask = slots.len().saturating_sub(1);
        let mut index = slot_of(at, mask);
        let mut value = (at, row);
        for _ in 0..slots.len() {
            let slot = slots
                .get(index)
                .ok_or(NativeError::Capacity("read back slot"))?;
            if slot.get().is_none() {
                // Another thread may set it first: the row goes on to the next slot.
                match slot.set(value) {
                    Ok(()) => {
                        self.absorb(charge)?;
                        used.fetch_add(1, Ordering::Relaxed);
                        return slot
                            .get()
                            .map(|(_, row)| row)
                            .ok_or(NativeError::Capacity("read back slot"));
                    }
                    Err(back) => value = back,
                }
            }
            index = index.wrapping_add(1) & mask;
        }
        Err(NativeError::Capacity("read back table full"))
    }

    /// Refuses `bytes` more past what the reads may hold.
    fn within(&self, bytes: usize) -> Result<(), NativeError> {
        let held = self.bytes();
        if held
            .checked_add(bytes)
            .is_none_or(|total| total > READ_BACK_BYTES)
        {
            return Err(MemoryError::Capacity {
                requested: bytes,
                available: READ_BACK_BYTES.saturating_sub(held),
            }
            .into());
        }
        Ok(())
    }

    /// The table a row read back goes in, with its count: the first under half its slots, made
    /// (and charged) where it is the next.
    fn table(&self, budget: &MemoryBudget) -> Result<(&[Slot], &AtomicUsize), NativeError> {
        let mut slots = FIRST_SLOTS;
        for (table, used) in self.tables.iter().zip(&self.used) {
            if used.load(Ordering::Relaxed).saturating_mul(2) < slots {
                if table.get().is_none() {
                    let bytes = slots
                        .checked_mul(size_of::<Slot>())
                        .ok_or(NativeError::Capacity("read back table"))?;
                    self.within(bytes)?;
                    let charge = self.fund(budget, bytes)?;
                    let mut made = Vec::new();
                    made.try_reserve_exact(slots)
                        .map_err(|_| MemoryError::AllocationFailed)?;
                    made.resize_with(slots, OnceLock::new);
                    // Another thread may make it first: its table is the one, and this charge
                    // goes with this one.
                    if table.set(made).is_ok() {
                        self.absorb(charge)?;
                    }
                }
                let made = table
                    .get()
                    .map(Vec::as_slice)
                    .ok_or(NativeError::Capacity("read back table"))?;
                return Ok((made, used));
            }
            slots = slots
                .checked_mul(2)
                .ok_or(NativeError::Capacity("read back tables"))?;
        }
        Err(MemoryError::Capacity {
            requested: 1,
            available: 0,
        }
        .into())
    }

    fn absorb(&self, mut charge: Allocation) -> Result<(), NativeError> {
        let mut held = self
            .held
            .lock()
            .map_err(|_| NativeError::Capacity("read back charge lock poisoned"))?;
        match held.as_mut() {
            Some(held) => held.absorb(&mut charge)?,
            None => *held = Some(charge),
        }
        Ok(())
    }
}

/// The slot `at` is looked for first in a table of `mask + 1` slots: its segment and offset,
/// which name it among everything the log holds, mixed.
fn slot_of(at: PayloadRef, mask: usize) -> usize {
    let name = (u64::from(at.segment()) << 32) | u64::from(at.offset());
    let mixed = name.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
    usize::try_from(mixed).unwrap_or(0) & mask
}

/// The row read back from `at` in `slots`, probed from its first slot to the first empty one.
fn probe(slots: &[Slot], at: PayloadRef) -> Option<&Row> {
    let mask = slots.len().saturating_sub(1);
    let mut index = slot_of(at, mask);
    for _ in 0..slots.len() {
        match slots.get(index)?.get() {
            None => return None,
            Some((held, row)) if *held == at => return Some(row),
            Some(_) => index = index.wrapping_add(1) & mask,
        }
    }
    None
}

#[cfg(test)]
#[path = "read_back_tests.rs"]
mod tests;
