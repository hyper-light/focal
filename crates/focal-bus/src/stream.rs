//! The streams a part holds: for each, the next offset it gives out, the first it still holds, and
//! where its first and last held batches are in the part's log.
//!
//! A stream keeps no index of its own. Its batches are chained through the part's log by the
//! `next` each stored batch carries (`batch`), so a stream costs forty bytes and its entry in the
//! table, whatever it holds: a live tail follows the chain from the batch it delivered last, and
//! letting a stream's oldest batches go walks it from the first. Step 1 measured the alternative, a
//! block of index entries a stream, at 1 to 5.3 times an event's payload in memory once streams are
//! sparse (each holding one batch or a few), which is the common case of agents' inboxes.
//!
//! The table's own memory is charged to the budget as it grows, and a new stream past the part's
//! bound or the budget is refused, typed; a stream already held is never refused.

use std::collections::HashMap;

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};

use crate::BusError;

/// A stream as its part holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamState {
    /// The stream's identity.
    pub id: u64,
    /// The offset its next record takes.
    pub next: u64,
    /// The first offset it still holds; every offset before is let go.
    pub floor: u64,
    /// Where its oldest held batch is in the part's log (a position word), if it holds one.
    pub first: Option<u64>,
    /// Where its newest batch is, if it holds one: the batch whose `next` the stream's next append
    /// stamps.
    pub last: Option<u64>,
}

/// What one stream costs the table: its state, and its map entry with the map's control byte.
pub const STREAM_BYTES: usize =
    std::mem::size_of::<StreamState>() + std::mem::size_of::<(u64, usize)>() + 1;

/// The streams of a part, by identity, at most `limit` of them.
#[derive(Debug)]
pub struct StreamTable {
    slots: HashMap<u64, usize>,
    streams: Vec<StreamState>,
    limit: usize,
    budget: MemoryBudget,
    /// The charge for what the table's capacities hold.
    charge: Option<Allocation>,
}

impl StreamTable {
    /// A table that holds at most `limit` streams, its memory charged to `budget`.
    pub fn new(limit: usize, budget: MemoryBudget) -> Self {
        Self {
            slots: HashMap::new(),
            streams: Vec::new(),
            limit,
            budget,
            charge: None,
        }
    }

    /// Streams held.
    pub fn len(&self) -> usize {
        self.streams.len()
    }

    /// Whether the table holds no stream.
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// Bytes the table's capacities hold, as charged.
    pub fn bytes(&self) -> usize {
        self.charge.as_ref().map_or(0, Allocation::bytes)
    }

    /// What the table's capacities hold: the map's buckets (a power of two at least an eighth over
    /// its capacity) and the states' vector.
    fn capacity_bytes(&self) -> Option<usize> {
        let buckets = self
            .slots
            .capacity()
            .checked_mul(8)?
            .checked_div(7)?
            .checked_next_power_of_two()?;
        buckets
            .checked_mul(std::mem::size_of::<(u64, usize)>().checked_add(1)?)?
            .checked_add(
                self.streams
                    .capacity()
                    .checked_mul(std::mem::size_of::<StreamState>())?,
            )
    }

    /// Charges what the table's capacities hold now, beyond what was charged.
    fn charge_growth(&mut self) -> Result<(), BusError> {
        let held = self
            .capacity_bytes()
            .ok_or(BusError::Capacity("stream table bytes"))?;
        let charged = self.bytes();
        if held <= charged {
            return Ok(());
        }
        let more = held.saturating_sub(charged);
        match &mut self.charge {
            Some(charge) => charge.grow(more).map_err(BusError::Memory),
            None => {
                self.charge = Some(
                    self.budget
                        .reserve(BudgetKind::Index, BudgetLane::Ordinary, more)
                        .map_err(BusError::Memory)?
                        .commit(),
                );
                Ok(())
            }
        }
    }

    /// The stream `id`, made if the table has room for one more.
    pub fn get_or_insert(&mut self, id: u64) -> Result<&mut StreamState, BusError> {
        let slot = match self.slots.get(&id) {
            Some(slot) => *slot,
            None => {
                if self.streams.len() >= self.limit {
                    return Err(BusError::Capacity("streams in a part"));
                }
                // The room first, charged, so a refusal leaves the table as it was.
                let short = |_| BusError::Memory(focal_memory::MemoryError::AllocationFailed);
                self.slots.try_reserve(1).map_err(short)?;
                self.streams.try_reserve(1).map_err(short)?;
                self.charge_growth()?;
                let slot = self.streams.len();
                self.streams.push(StreamState {
                    id,
                    next: 0,
                    floor: 0,
                    first: None,
                    last: None,
                });
                self.slots.insert(id, slot);
                slot
            }
        };
        self.streams
            .get_mut(slot)
            .ok_or(BusError::Capacity("stream slot"))
    }

    /// Every stream held, in the order the table took them.
    pub fn iter(&self) -> impl Iterator<Item = &StreamState> {
        self.streams.iter()
    }

    /// Empties the table, keeping its room and its charge: a part replaced by an image.
    pub fn clear(&mut self) {
        self.slots.clear();
        self.streams.clear();
    }

    /// The stream `id`, if held.
    pub fn get(&self, id: u64) -> Option<&StreamState> {
        self.streams.get(*self.slots.get(&id)?)
    }

    /// The stream `id`, if held, to change.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut StreamState> {
        self.streams.get_mut(*self.slots.get(&id)?)
    }

    /// What a stream costs the table at least, bytes: its state and its map entry.
    pub const fn stream_bytes() -> usize {
        STREAM_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_refuses_a_stream_past_its_limit_or_its_budget() {
        let mut table = StreamTable::new(2, MemoryBudget::new(1 << 20, 0).unwrap());
        table.get_or_insert(10).unwrap().next = 5;
        table.get_or_insert(11).unwrap();
        assert_eq!(table.get_or_insert(10).unwrap().next, 5);
        assert!(matches!(
            table.get_or_insert(12),
            Err(BusError::Capacity(_))
        ));
        assert_eq!(table.len(), 2);
        assert!(table.bytes() >= 2 * std::mem::size_of::<StreamState>());

        let mut small = StreamTable::new(1_000_000, MemoryBudget::new(4_096, 0).unwrap());
        let mut made = 0;
        while small.get_or_insert(made).is_ok() {
            made += 1;
        }
        assert!(made > 0, "a 4 KiB budget holds some streams");
        assert!(small.bytes() <= 4_096);
        // Every stream made is still there, and still found.
        for id in 0..made {
            assert!(small.get(id).is_some());
        }
    }
}
