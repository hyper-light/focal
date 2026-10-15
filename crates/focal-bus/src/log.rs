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
//! every older slot is empty too. The pool keeps a few given back warm,
//! already faulted in, as many as a quarter of those in use (at least [`SegmentPool::MIN_WARM`]):
//! the next segment costs neither an allocation nor a page fault, and a burst's segments are
//! returned to the system once it has passed. Every segment held, in use or warm, is charged to the
//! budget; a part is refused room past it, never given more than the budget allows.
//!
//! A batch is never split across segments: one that does not fit the tail segment's room starts
//! the next, and the room it leaves is at most one batch less a byte, so with batches bounded to an
//! eighth of a segment at least seven eighths of every full segment hold batches.

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

/// The segments a shard's parts take and give back: one size, each charged while held.
#[derive(Debug)]
pub struct SegmentPool {
    size: usize,
    budget: MemoryBudget,
    warm: Vec<Segment>,
    in_use: usize,
    /// Segments allocated from the system over the pool's life; a measurement's count.
    allocated: u64,
    /// Segments returned to the system.
    released: u64,
}

impl SegmentPool {
    /// The least the pool keeps warm, whatever is in use: one to take the next batch at once and
    /// one beside it, so a part writing steadily never waits on the allocator.
    pub const MIN_WARM: usize = 2;

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
            allocated: 0,
            released: 0,
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

    /// Segments allocated from the system and returned to it over the pool's life.
    pub const fn churn(&self) -> (u64, u64) {
        (self.allocated, self.released)
    }

    fn take(&mut self) -> Result<Segment, BusError> {
        let segment = match self.warm.pop() {
            Some(segment) => segment,
            None => {
                let charge = self
                    .budget
                    .reserve(BudgetKind::Payload, BudgetLane::Ordinary, self.size)
                    .map_err(BusError::Memory)?
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
        Ok(segment)
    }

    fn give(&mut self, mut segment: Segment) {
        self.in_use = self.in_use.saturating_sub(1);
        let keep = (self.in_use / 4).max(Self::MIN_WARM);
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
            let segment = pool.take()?;
            self.slots.push_back(Slot {
                segment: Some(segment),
                live: 0,
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
    fn a_released_segment_is_gone_and_the_pool_keeps_a_few_warm() {
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
        // Ten in use keep two warm; the other twenty-eight went back to the system.
        assert_eq!((pool.in_use(), pool.held(), log.segments()), (10, 12, 10));
        assert_eq!(pool.churn(), (40, 28));
        // The next segments come from the warm ones, with no new allocation.
        for _ in 0..2 {
            log.append(&mut pool, &[2; 100]).unwrap();
        }
        assert_eq!(pool.churn(), (40, 28));
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
