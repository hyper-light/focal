//! The world's pending events, held for the discipline in force (`docs/sim.md` §3.3).
//!
//! Payloads live in a slab, reused through a free list; their keys live in a binary heap under
//! the ordered discipline (the earliest first, ties by the order they were scheduled) and in a
//! dense vector under the free one (any of them taken in constant time by `swap_remove`). A switch
//! of discipline moves the keys from one to the other, once.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::error::SimError;

/// Which events are enabled at a choice point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discipline {
    /// Only the events at the earliest time, ties broken by the strategy: the timed family.
    Ordered,
    /// Every pending event, whatever its time: the untimed family, safety over orders.
    Free,
}

/// A pending event's place: when, its scheduling ordinal (unique, so keys order totally), the node
/// it is for and its payload's slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub(crate) at: u64,
    pub(crate) seq: u64,
    pub(crate) node: u32,
    pub(crate) slot: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct Queue<E> {
    slots: Vec<Option<E>>,
    free: Vec<u32>,
    heap: BinaryHeap<Reverse<Key>>,
    dense: Vec<Key>,
    discipline: Discipline,
    live: usize,
    bound: usize,
    next_seq: u64,
}

impl<E> Queue<E> {
    pub(crate) fn new(discipline: Discipline, bound: usize) -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            heap: BinaryHeap::new(),
            dense: Vec::new(),
            discipline,
            live: 0,
            bound,
            next_seq: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.live
    }

    pub(crate) fn push(&mut self, at: u64, node: u32, event: E) -> Result<(), SimError> {
        let full = SimError::Full {
            what: "events",
            bound: self.bound,
        };
        if self.live >= self.bound {
            return Err(full);
        }
        let slot = match self.free.pop() {
            Some(slot) => {
                let place = usize::try_from(slot)
                    .ok()
                    .and_then(|at| self.slots.get_mut(at))
                    .ok_or(full)?;
                *place = Some(event);
                slot
            }
            None => {
                let slot = u32::try_from(self.slots.len()).map_err(|_| full)?;
                self.slots.push(Some(event));
                slot
            }
        };
        let key = Key {
            at,
            seq: self.next_seq,
            node,
            slot,
        };
        self.next_seq = self.next_seq.saturating_add(1);
        self.live = self.live.saturating_add(1);
        self.insert(key);
        Ok(())
    }

    /// A key put back among the pending: a tie the strategy did not take.
    pub(crate) fn insert(&mut self, key: Key) {
        match self.discipline {
            Discipline::Ordered => self.heap.push(Reverse(key)),
            Discipline::Free => self.dense.push(key),
        }
    }

    /// The payload of a key taken from the pending.
    pub(crate) fn take(&mut self, key: Key) -> Option<E> {
        let event = usize::try_from(key.slot)
            .ok()
            .and_then(|at| self.slots.get_mut(at))
            .and_then(Option::take)?;
        self.free.push(key.slot);
        self.live = self.live.saturating_sub(1);
        Some(event)
    }

    /// Ordered: the earliest pending time.
    pub(crate) fn earliest(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse(key)| key.at)
    }

    /// Ordered: the key at `at`, taken, if no other is due then: the step without a tie.
    pub(crate) fn pop_alone(&mut self, at: u64) -> Option<Key> {
        let Reverse(first) = self.heap.pop()?;
        if first.at == at && self.heap.peek().is_none_or(|Reverse(next)| next.at != at) {
            return Some(first);
        }
        self.heap.push(Reverse(first));
        None
    }

    /// Ordered: every key at `at`, earliest scheduled first, moved into `into`.
    pub(crate) fn pop_at(&mut self, at: u64, into: &mut Vec<Key>) {
        while let Some(Reverse(key)) = self.heap.peek() {
            if key.at != at {
                break;
            }
            if let Some(Reverse(key)) = self.heap.pop() {
                into.push(key);
            }
        }
    }

    /// Free: every pending key, in the order the choice indexes them.
    pub(crate) fn dense(&self) -> &[Key] {
        &self.dense
    }

    /// Free: the key at `index`, taken from the pending.
    pub(crate) fn remove_dense(&mut self, index: usize) -> Option<Key> {
        (index < self.dense.len()).then(|| self.dense.swap_remove(index))
    }

    /// The discipline changed: the keys move to the other holder, in key order.
    pub(crate) fn switch(&mut self, discipline: Discipline) {
        if discipline == self.discipline {
            return;
        }
        self.discipline = discipline;
        match discipline {
            Discipline::Free => {
                self.dense.extend(
                    std::mem::take(&mut self.heap)
                        .into_sorted_vec()
                        .into_iter()
                        .rev()
                        .map(|Reverse(key)| key),
                );
            }
            Discipline::Ordered => {
                self.heap
                    .extend(std::mem::take(&mut self.dense).into_iter().map(Reverse));
            }
        }
    }
}
