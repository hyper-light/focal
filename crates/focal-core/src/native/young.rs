//! The newest spillable rows, held (doc 31 M3). A claim waiting to be delivered and an authored
//! definition are spilled to the payload log only once newer ones have pushed them out of the
//! young generation, as Kafka serves a log's tail from memory and lets its backlog go to disk: a
//! message taken soon after it is posted is never spilled, so it is never read back. A read back
//! decodes and checks the whole row as a replay does: at 6,000 writes a second, every delivery
//! spilled at its post and read back at its receipt, that was a quarter of the owner's time.
//!
//! The generation is bounded by count, sized from the session's memory (a sixteenth of it, at
//! the bytes a held claim takes), its keys' buffer allocated and charged when the state is made.
//! A key it pushes out is spilled if its row still may be (`OwnedClaim::spill` decides); a row
//! published again while young may hold two places, and the older one only spills it sooner.

use super::Key;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use std::collections::VecDeque;

/// The memory a held waiting claim takes, about: what sizes the generation.
const HELD_BYTES: usize = 8 << 10;
/// The generation takes at most this share of the session's memory: a sixteenth.
const SHARE: usize = 16;
/// The generation's bounds, whatever the memory: from a few hundred rows to 65,536.
const MIN_KEYS: usize = 256;
const MAX_KEYS: usize = 1 << 16;

pub(super) struct Young {
    /// Oldest first.
    keys: VecDeque<Key>,
    bound: usize,
    _charge: Option<Allocation>,
}

impl Young {
    /// No generation: each spillable row is spilled where it is published (a state without a
    /// payload log never spills).
    pub(super) fn none() -> Self {
        Self {
            keys: VecDeque::new(),
            bound: 0,
            _charge: None,
        }
    }
    /// A generation sized from `budget`, its buffer allocated and charged now.
    pub(super) fn for_budget(budget: &MemoryBudget) -> Result<Self, MemoryError> {
        let bound = budget
            .limit()
            .checked_div(SHARE)
            .and_then(|share| share.checked_div(HELD_BYTES))
            .unwrap_or(0)
            .clamp(MIN_KEYS, MAX_KEYS);
        Self::bounded(budget, bound)
    }
    /// A generation of `bound` keys charged to `budget`; none for a bound of none.
    pub(super) fn bounded(budget: &MemoryBudget, bound: usize) -> Result<Self, MemoryError> {
        if bound == 0 {
            return Ok(Self::none());
        }
        let bytes = bound
            .checked_mul(size_of::<Key>())
            .ok_or(MemoryError::InvalidConfiguration("young generation"))?;
        let charge = budget
            .reserve(BudgetKind::Index, BudgetLane::Ordinary, bytes)?
            .commit();
        let mut keys = VecDeque::new();
        keys.try_reserve_exact(bound)
            .map_err(|_| MemoryError::AllocationFailed)?;
        Ok(Self {
            keys,
            bound,
            _charge: Some(charge),
        })
    }
    /// `key` joins the generation; the oldest it pushes out is returned, to spill. With no
    /// generation, `key` itself is.
    pub(super) fn admit(&mut self, key: Key) -> Option<Key> {
        if self.bound == 0 {
            return Some(key);
        }
        let pushed = if self.keys.len() >= self.bound {
            self.keys.pop_front()
        } else {
            None
        };
        // Within the capacity allocated when the state was made: no reallocation.
        self.keys.push_back(key);
        pushed
    }
    /// The keys it holds now.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }
    #[cfg(test)]
    pub(super) fn bound(&self) -> usize {
        self.bound
    }
}
