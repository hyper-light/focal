//! A part's log in memory: the batches of every stream the part holds, appended into fixed-size
//! segments, read in place, and let go a whole segment at a time.
//!
//! Interleaving the part's streams in one log, rather than giving each stream segments of its own,
//! is what lets millions of streams cost only what they hold: an idle stream holds no segment.
//! RAMCloud's log-structured memory keeps every object of a server in one log for the same reason
//! and reaches 80–90% memory utilization where general allocators fragment (Rumble, Kejriwal and
//! Ousterhout, FAST 2014); BookKeeper interleaves its ledgers' entries in one entry log.
//!
//! A segment counts the batches in it that are still held, and is given back to the shard's
//! [`SegmentPool`] the moment that count reaches zero, whatever its place in the log, so one
//! stream's long-held batch never keeps the segments around it: RAMCloud keeps the same per-segment
//! live count for its cleaner. A segment given back leaves an empty slot in the log's order until
//! every older slot is empty too. The pool keeps segments given back warm, already faulted in, as
//! many as its own demand has recently needed ([`WorkingSet`]): the next segment costs neither an
//! allocation nor a page fault, and a burst's segments are returned to the system once a busy
//! period that needs fewer has ended. Every segment held, in use or warm, is charged to the budget;
//! a part is refused room past it, never given more than the budget allows.
//!
//! A batch is never split across segments: one that does not fit the tail segment's room starts
//! the next, and the room it leaves is at most one batch less a byte, so with batches bounded to a
//! fifth of a segment (`wire::SEGMENT_BATCHES`) at least four fifths of every full segment hold
//! batches.

use std::collections::VecDeque;

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};

use crate::BusError;

/// Where a batch is in a part's log: its segment's number and its offset within it. The number
/// counts every segment the log has taken, so a position stays valid until its segment is let go,
/// and found in constant time while it is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    segment: u64,
    offset: u32,
}

impl Position {
    /// Bits of a word that carry the offset within the segment: segments are at most 16 MiB.
    const OFFSET_BITS: u32 = 24;
    /// The largest segment a position addresses.
    pub const MAX_SEGMENT: usize = 1 << Self::OFFSET_BITS;

    /// The position as one word, the segment's number above its offset; `None` past 2^40 segments.
    pub fn to_word(self) -> Option<u64> {
        if self.segment >= 1 << (64 - Self::OFFSET_BITS) || self.offset >= 1 << Self::OFFSET_BITS {
            return None;
        }
        Some(self.segment.wrapping_shl(Self::OFFSET_BITS) | u64::from(self.offset))
    }

    /// The position a word names.
    pub fn from_word(word: u64) -> Self {
        Self {
            segment: word.wrapping_shr(Self::OFFSET_BITS),
            offset: u32::try_from(word & ((1 << Self::OFFSET_BITS) - 1)).unwrap_or(0),
        }
    }

    /// The segment's number in its log.
    pub const fn segment(self) -> u64 {
        self.segment
    }

    /// The batch's offset within its segment.
    pub const fn offset(self) -> u32 {
        self.offset
    }
}

/// One segment: a buffer of fixed capacity that batches are copied into and never moved within.
#[derive(Debug)]
pub struct Segment {
    bytes: Vec<u8>,
    /// The budget's charge for the segment's bytes, given back as the segment drops.
    _charge: Allocation,
}

impl Segment {
    /// Room left for batches.
    fn room(&self) -> usize {
        self.bytes.capacity().saturating_sub(self.bytes.len())
    }
}

/// What a pool keeps warm, learned from its own demand: the most it had out at once in its current
/// period and in the period before. A period runs from one trough of demand to the next, a take
/// that follows gives, so it is the demand's own unit and the rule takes no time constant and no
/// fraction. A pool holds, out and warm, no more than that working set: a buffer given back is kept
/// only while demand this recent reached past what is out now, and a burst's are returned once a
/// period that needs fewer has ended. It never holds more than the pool's own peak, which its
/// budget allowed when the peak was reached (the working set of Bonwick and Adams's magazine depot,
/// USENIX 2001, delimited by demand's troughs rather than a reaping interval). A trough, not an
/// empty pool: a log's tail is out for as long as the log takes batches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkingSet {
    /// The most out at once in this period.
    peak: usize,
    /// The most out at once in the period before it.
    last: usize,
    /// Whether the last change was a give: the next take ends the period.
    falling: bool,
}

impl WorkingSet {
    /// One more is out, `out` with it.
    pub fn took(&mut self, out: usize) {
        if self.falling {
            // A trough: the period ends, its peak what the next one is kept warm for.
            self.falling = false;
            self.last = self.peak;
            self.peak = 0;
        }
        self.peak = self.peak.max(out);
    }

    /// One came back, `out` after it: how many may be kept warm.
    pub fn gave(&mut self, out: usize) -> usize {
        self.falling = true;
        self.peak.max(self.last).saturating_sub(out)
    }
}

/// The segments a shard's parts take and give back: one size, each charged while held.
#[derive(Debug)]
pub struct SegmentPool {
    size: usize,
    budget: MemoryBudget,
    warm: Vec<Segment>,
    in_use: usize,
    /// What it keeps warm.
    working: WorkingSet,
    /// Segments allocated from the system over the pool's life; a measurement's count.
    allocated: u64,
    /// Segments returned to the system.
    released: u64,
    /// Segments the parts gave back, each emptied of its batches: what the window let go.
    emptied: u64,
}

impl SegmentPool {
    /// A pool of segments of `size` bytes charged to `budget`.
    pub fn new(size: usize, budget: MemoryBudget) -> Result<Self, BusError> {
        if !(crate::batch::MIN_BATCH..=Position::MAX_SEGMENT).contains(&size) {
            return Err(BusError::Configuration(
                "a segment holds at least one batch and at most 16 MiB",
            ));
        }
        Ok(Self {
            size,
            budget,
            warm: Vec::new(),
            in_use: 0,
            working: WorkingSet::default(),
            allocated: 0,
            released: 0,
            emptied: 0,
        })
    }

    /// The segments' size, bytes.
    pub const fn segment_size(&self) -> usize {
        self.size
    }

    /// Segments held: in use and warm.
    pub fn held(&self) -> usize {
        self.in_use.saturating_add(self.warm.len())
    }

    /// Segments in use by parts.
    pub const fn in_use(&self) -> usize {
        self.in_use
    }

    /// Segments kept warm.
    pub fn warm(&self) -> usize {
        self.warm.len()
    }

    /// Segments allocated from the system and returned to it over the pool's life.
    pub const fn churn(&self) -> (u64, u64) {
        (self.allocated, self.released)
    }

    /// Segments the parts gave back over the pool's life, each emptied of the batches it held.
    pub const fn emptied(&self) -> u64 {
        self.emptied
    }

    fn take(&mut self) -> Result<Segment, BusError> {
        let segment = match self.warm.pop() {
            Some(segment) => segment,
            None => {
                // What is admitted is spent: the segment is what a committed fact requires where
                // it applies, a part's appends being its group's committed entries, held to the
                // budget's hold limit (`limits::GROUP_APPLY_HOLD`); a budget that holds nothing
                // past what it admits refuses it.
                let charge =
                    match self
                        .budget
                        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, self.size)
                    {
                        Ok(reserved) => reserved,
                        Err(_) => self
                            .budget
                            .reserve(BudgetKind::Payload, BudgetLane::Committed, self.size)
                            .map_err(BusError::Memory)?,
                    }
                    .commit();
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(self.size)
                    .map_err(|_| BusError::Memory(focal_memory::MemoryError::AllocationFailed))?;
                self.allocated = self.allocated.saturating_add(1);
                Segment {
                    bytes,
                    _charge: charge,
                }
            }
        };
        self.in_use = self.in_use.saturating_add(1);
        self.working.took(self.in_use);
        Ok(segment)
    }

    fn give(&mut self, mut segment: Segment) {
        self.in_use = self.in_use.saturating_sub(1);
        self.emptied = self.emptied.saturating_add(1);
        let keep = self.working.gave(self.in_use);
        if self.warm.len() < keep {
            segment.bytes.clear();
            self.warm.push(segment);
        } else {
            // Dropped: its bytes go back to the system and its charge to the budget.
            self.released = self.released.saturating_add(1);
            drop(segment);
        }
        // Demand fell: what is kept warm falls with it.
        while self.warm.len() > keep {
            self.warm.pop();
            self.released = self.released.saturating_add(1);
        }
    }

    /// Returns every warm segment to the system: what a node does first under memory pressure.
    pub fn shed(&mut self) {
        let shed = self.warm.len();
        self.warm.clear();
        self.released = self
            .released
            .saturating_add(u64::try_from(shed).unwrap_or(u64::MAX));
    }
}

/// A segment's place in a part's log: the segment while any batch in it is held, and how many are.
#[derive(Debug)]
struct Slot {
    segment: Option<Segment>,
    live: u32,
    /// Batches appended to it: what it held when it was the tail, `live` of them still held.
    appended: u32,
}

/// A part's log: its segments in order, the oldest first.
#[derive(Debug, Default)]
pub struct PartLog {
    slots: VecDeque<Slot>,
    /// The number of the oldest slot; the next segment taken is this plus `slots.len()`.
    first: u64,
    /// Segments held (slots not yet emptied).
    held: usize,
}

impl PartLog {
    /// An empty log, holding no segment.
    pub fn new() -> Self {
        Self::default()
    }

    /// Copies `batch` to the log's tail, taking a segment from `pool` when the tail has no room for
    /// it, and returns where it is.
    pub fn append(&mut self, pool: &mut SegmentPool, batch: &[u8]) -> Result<Position, BusError> {
        if batch.len() > pool.segment_size() {
            return Err(BusError::Batch(crate::batch::BatchError::TooLarge {
                bytes: batch.len(),
                limit: pool.segment_size(),
            }));
        }
        let fits = self
            .slots
            .back()
            .and_then(|slot| slot.segment.as_ref())
            .is_some_and(|tail| tail.room() >= batch.len());
        if !fits {
            // A tail that holds no batch and has no room for this one takes no more: it goes back
            // first, so the pool can hand it out again. Kept as the tail while it had room, it was
            // never looked at again, and a segment a rollover was held for good.
            self.seal(pool);
            let segment = pool.take()?;
            self.slots.push_back(Slot {
                segment: Some(segment),
                live: 0,
                appended: 0,
            });
            self.held = self.held.saturating_add(1);
        }
        let number = self
            .first
            .checked_add(u64::try_from(self.slots.len()).unwrap_or(u64::MAX))
            .and_then(|next| next.checked_sub(1))
            .ok_or(BusError::Capacity("segment numbers"))?;
        let slot = self
            .slots
            .back_mut()
            .ok_or(BusError::Capacity("segment taken"))?;
        slot.live = slot
            .live
            .checked_add(1)
            .ok_or(BusError::Capacity("batches in a segment"))?;
        slot.appended = slot.appended.saturating_add(1);
        let tail = slot
            .segment
            .as_mut()
            .ok_or(BusError::Capacity("segment taken"))?;
        let offset = u32::try_from(tail.bytes.len()).map_err(|_| BusError::Capacity("offset"))?;
        // Within the capacity reserved at the segment's start: never a reallocation.
        tail.bytes.extend_from_slice(batch);
        Ok(Position {
            segment: number,
            offset,
        })
    }

    /// The oldest batches to let go now so that the oldest segment is let go in the share the tail
    /// has filled: `segment_size`'s bytes appended to the tail, the oldest segment's batches given
    /// back by the same share, so it is empty when the tail is full. None while the oldest segment is
    /// the tail.
    pub fn recycle_need(&self, segment_size: usize) -> usize {
        if self.slots.len() < 2 {
            return 0;
        }
        let (Some(oldest), Some(tail)) = (self.slots.front(), self.slots.back()) else {
            return 0;
        };
        let Some(tail_segment) = tail.segment.as_ref() else {
            return 0;
        };
        let filled = segment_size.saturating_sub(tail_segment.room());
        let appended = usize::try_from(oldest.appended).unwrap_or(usize::MAX);
        let released = appended.saturating_sub(usize::try_from(oldest.live).unwrap_or(0));
        appended
            .saturating_mul(filled)
            .div_ceil(segment_size.max(1))
            .saturating_sub(released)
    }

    fn slot_index(&self, number: u64) -> Option<usize> {
        usize::try_from(number.checked_sub(self.first)?).ok()
    }

    fn segment_at(&self, number: u64) -> Option<&Segment> {
        self.slots.get(self.slot_index(number)?)?.segment.as_ref()
    }

    /// The `length` bytes at `at`, while its segment is held.
    pub fn read(&self, at: Position, length: usize) -> Option<&[u8]> {
        let start = usize::try_from(at.offset).ok()?;
        self.segment_at(at.segment)?
            .bytes
            .get(start..start.checked_add(length)?)
    }

    /// The stored batch at `at`, as long as its header states, while its segment is held.
    pub fn batch(&self, at: Position) -> Option<&[u8]> {
        let start = usize::try_from(at.offset).ok()?;
        let bytes = self.segment_at(at.segment)?.bytes.get(start..)?;
        bytes.get(..crate::batch::stated_length(bytes)?)
    }

    /// The stored batch at `at`, to stamp.
    pub(crate) fn batch_mut(&mut self, at: Position) -> Option<&mut [u8]> {
        let start = usize::try_from(at.offset).ok()?;
        let index = self.slot_index(at.segment)?;
        let bytes = self
            .slots
            .get_mut(index)?
            .segment
            .as_mut()?
            .bytes
            .get_mut(start..)?;
        let length = crate::batch::stated_length(bytes)?;
        bytes.get_mut(..length)
    }

    /// Gives the tail back if it holds no batch: the next batch starts a segment of its own. What
    /// memory pressure does with a tail whose batches were all let go (`Node::relieve`), and what a
    /// batch the tail has no room for does. Whether a segment went back.
    pub fn seal(&mut self, pool: &mut SegmentPool) -> bool {
        let Some(tail) = self.slots.back_mut() else {
            return false;
        };
        if tail.live != 0 {
            return false;
        }
        let Some(segment) = tail.segment.take() else {
            return false;
        };
        pool.give(segment);
        self.held = self.held.saturating_sub(1);
        self.trim();
        true
    }

    /// The batch at `at` is no longer held: its segment's count falls by one, and a segment that
    /// holds nothing more, and is not the tail still taking batches, goes back to `pool`.
    pub fn release(&mut self, pool: &mut SegmentPool, at: Position) -> Result<(), BusError> {
        let index = self
            .slot_index(at.segment)
            .ok_or(BusError::Capacity("release of a segment already let go"))?;
        let tail = self.slots.len().saturating_sub(1);
        let slot = self
            .slots
            .get_mut(index)
            .ok_or(BusError::Capacity("release of a segment already let go"))?;
        slot.live = slot
            .live
            .checked_sub(1)
            .ok_or(BusError::Capacity("release of a batch not held"))?;
        if slot.live == 0 && index != tail {
            if let Some(segment) = slot.segment.take() {
                pool.give(segment);
                self.held = self.held.saturating_sub(1);
            }
            self.trim();
        }
        Ok(())
    }

    /// Drops the empty slots at the front of the order.
    fn trim(&mut self) {
        while self
            .slots
            .front()
            .is_some_and(|slot| slot.segment.is_none())
        {
            self.slots.pop_front();
            self.first = self.first.saturating_add(1);
        }
    }

    /// Gives back every segment: the part holds nothing.
    pub fn release_all(&mut self, pool: &mut SegmentPool) {
        while let Some(slot) = self.slots.pop_front() {
            if let Some(segment) = slot.segment {
                pool.give(segment);
            }
            self.first = self.first.saturating_add(1);
        }
        self.held = 0;
    }

    /// Segments the log holds.
    pub const fn segments(&self) -> usize {
        self.held
    }

    /// Bytes of batches the log's held segments contain, the released batches in them included.
    pub fn bytes(&self) -> usize {
        self.slots
            .iter()
            .filter_map(|slot| slot.segment.as_ref())
            .map(|segment| segment.bytes.len())
            .fold(0, usize::saturating_add)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(size: usize) -> SegmentPool {
        SegmentPool::new(size, MemoryBudget::new(1 << 30, 0).unwrap()).unwrap()
    }

    #[test]
    fn batches_are_read_where_they_were_appended_across_segments() {
        let mut pool = pool(256);
        let mut log = PartLog::new();
        let mut written = Vec::new();
        for at in 0..100_u8 {
            let batch = vec![at; 60 + usize::from(at % 7)];
            let position = log.append(&mut pool, &batch).unwrap();
            written.push((position, batch));
        }
        for (position, batch) in &written {
            assert_eq!(log.read(*position, batch.len()), Some(&batch[..]));
        }
        // Each batch starts a new segment only where the tail's room is short of it.
        let mut expected = 0;
        let mut room = 0;
        for at in 0..100_u8 {
            let length = 60 + usize::from(at % 7);
            if room < length {
                expected += 1;
                room = 256;
            }
            room -= length;
        }
        assert_eq!(log.segments(), expected);
    }

    #[test]
    fn a_released_segment_is_gone_and_the_pool_keeps_what_its_demand_needed() {
        let mut pool = pool(128);
        let mut log = PartLog::new();
        let positions: Vec<_> = (0..40)
            .map(|_| log.append(&mut pool, &[1; 100]).unwrap())
            .collect();
        assert_eq!(pool.in_use(), 40);
        for position in &positions[..30] {
            log.release(&mut pool, *position).unwrap();
        }
        assert_eq!(log.read(positions[29], 100), None);
        assert_eq!(log.read(positions[30], 100), Some(&[1; 100][..]));
        // Demand reached forty in this busy period: the thirty given back are kept warm.
        assert_eq!((pool.in_use(), pool.held(), log.segments()), (10, 40, 10));
        assert_eq!(pool.churn(), (40, 0));
        // The next segments come from the warm ones, with no new allocation.
        let more: Vec<_> = (0..30)
            .map(|_| log.append(&mut pool, &[2; 100]).unwrap())
            .collect();
        assert_eq!(pool.churn(), (40, 0));
        // Everything is given back but the tail, which takes the next batch: the burst's forty
        // stay while demand this recent needed them.
        for position in positions[30..].iter().chain(&more) {
            log.release(&mut pool, *position).unwrap();
        }
        assert_eq!((pool.in_use(), pool.held()), (1, 40));
        // Then two periods that each need two: once one has ended, the pool keeps what they need
        // and returns the rest of the burst's.
        for round in 0..2 {
            let pair: Vec<_> = (0..2)
                .map(|_| log.append(&mut pool, &[3; 100]).unwrap())
                .collect();
            for position in &pair {
                log.release(&mut pool, *position).unwrap();
            }
            let _ = round;
        }
        assert!(pool.held() <= 2, "held {}", pool.held());
        assert_eq!(pool.churn().0, 40, "nothing allocated after the burst");
    }

    /// A tail emptied while it took batches is given back once a batch it has no room for comes,
    /// and the next segment is it again: nothing is held for good, nothing allocated anew.
    #[test]
    fn an_emptied_tail_goes_back_when_the_log_moves_past_it() {
        let mut pool = pool(128);
        let mut log = PartLog::new();
        for _ in 0..100 {
            let position = log.append(&mut pool, &[5; 100]).unwrap();
            log.release(&mut pool, position).unwrap();
        }
        assert_eq!((pool.in_use(), pool.held(), log.segments()), (1, 1, 1));
        assert_eq!(pool.churn(), (1, 0));
    }

    /// One taken and given back at a time, as a part writing steadily past its window does: one is
    /// kept warm, and none is allocated after the first.
    #[test]
    fn a_working_set_of_one_keeps_one() {
        let mut working = WorkingSet::default();
        for _ in 0..1_000 {
            working.took(1);
            assert_eq!(working.gave(0), 1);
        }
        let mut pool = pool(128);
        let mut log = PartLog::new();
        for _ in 0..100 {
            let position = log.append(&mut pool, &[4; 100]).unwrap();
            log.release(&mut pool, position).unwrap();
        }
        assert_eq!(pool.churn(), (1, 0));
    }

    #[test]
    fn a_segment_whose_batches_are_all_released_goes_back_out_of_order() {
        let mut pool = pool(128);
        let mut log = PartLog::new();
        let positions: Vec<_> = (0..6)
            .map(|_| log.append(&mut pool, &[3; 100]).unwrap())
            .collect();
        // The middle segments go; the first stays held, so they leave empty slots behind it.
        for position in &positions[1..4] {
            log.release(&mut pool, *position).unwrap();
        }
        assert_eq!(log.segments(), 3);
        assert_eq!(log.read(positions[2], 100), None);
        assert_eq!(log.read(positions[0], 100), Some(&[3; 100][..]));
        // Releasing the first drops it and the empty slots after it.
        log.release(&mut pool, positions[0]).unwrap();
        assert_eq!(log.segments(), 2);
        assert_eq!(log.read(positions[4], 100), Some(&[3; 100][..]));
        // The tail is never given back while it takes batches, even with nothing held in it.
        log.release(&mut pool, positions[5]).unwrap();
        assert_eq!(log.read(positions[5], 100), Some(&[3; 100][..]));
        // A release of a batch not held is refused, typed.
        assert!(log.release(&mut pool, positions[5]).is_err());
    }

    #[test]
    fn a_position_round_trips_through_its_word() {
        for (segment, offset) in [(0, 0), (1, 5), ((1 << 40) - 1, (1 << 24) - 1)] {
            let at = Position { segment, offset };
            assert_eq!(Position::from_word(at.to_word().unwrap()), at);
        }
        assert_eq!(
            Position {
                segment: 1 << 40,
                offset: 0
            }
            .to_word(),
            None
        );
        assert_eq!(
            Position {
                segment: 0,
                offset: 1 << 24
            }
            .to_word(),
            None
        );
        assert!(SegmentPool::new((16 << 20) + 1, MemoryBudget::new(1 << 30, 0).unwrap()).is_err());
    }

    #[test]
    fn a_batch_larger_than_a_segment_or_past_the_budget_is_refused_typed() {
        let mut pool = pool(128);
        let mut log = PartLog::new();
        assert!(matches!(
            log.append(&mut pool, &[0; 129]),
            Err(BusError::Batch(_))
        ));
        let mut small = SegmentPool::new(128, MemoryBudget::new(256, 0).unwrap()).unwrap();
        log.append(&mut small, &[0; 100]).unwrap();
        log.append(&mut small, &[0; 100]).unwrap();
        assert!(matches!(
            log.append(&mut small, &[0; 100]),
            Err(BusError::Memory(_))
        ));
    }
}
