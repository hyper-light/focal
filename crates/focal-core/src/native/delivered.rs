//! The recipients a core has filed deliveries for since its owner last took them: what wakes the
//! inbox reads that wait on them (the node's parked `Operation::NativeWait`s), so a commit wakes
//! the reads of the inboxes it delivered to, never every read that waits. Redis wakes a blocked
//! `XREAD` the same way, by the key the write signalled ready (`signalKeyAsReady`), not by a pass
//! over every blocked client; Kafka's purgatory checks a delayed fetch when a partition it
//! watches is appended to.
//!
//! The notes are distinct and sorted, at most the session's principals (every recipient is one),
//! each growth of their room charged to the core's budget as it is taken and given back with the
//! notes. Where they cannot be kept exactly (their room refused, the bound reached, the delivery
//! index let go or built again from the rows, which files nothing one at a time), the next take
//! says every inbox may have changed: every read that waits looks again. That costs a pass over
//! the reads, never a delivery missed.
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane};

/// What the deliveries filed since the last take changed: the inboxes, or every one.
#[derive(Debug, Default)]
pub struct Delivered {
    /// The recipients a delivery was filed for, distinct and sorted.
    pub recipients: Vec<ParticipantId>,
    /// Any inbox may have changed: the notes could not be kept exactly.
    pub all: bool,
    /// The notes' room, held while they are.
    _charge: Option<Allocation>,
}

impl Delivered {
    /// Whether `recipient`'s inbox may have changed.
    pub fn reaches(&self, recipient: ParticipantId) -> bool {
        self.all || self.recipients.binary_search(&recipient).is_ok()
    }
    /// Whether nothing was delivered.
    pub fn is_empty(&self) -> bool {
        !self.all && self.recipients.is_empty()
    }
}

#[derive(Debug)]
pub(super) struct Notes {
    noted: Delivered,
    /// The most distinct recipients noted: the session's principals.
    bound: usize,
}

impl Notes {
    pub(super) fn new(bound: usize) -> Self {
        Self {
            noted: Delivered::default(),
            bound,
        }
    }
    /// A delivery was filed for `recipient`.
    pub(super) fn note(&mut self, recipient: ParticipantId, budget: &MemoryBudget) {
        if self.noted.all {
            return;
        }
        let Err(at) = self.noted.recipients.binary_search(&recipient) else {
            return;
        };
        let len = self.noted.recipients.len();
        if len >= self.bound {
            self.all();
            return;
        }
        if len == self.noted.recipients.capacity() && !self.grow(budget) {
            self.all();
            return;
        }
        self.noted.recipients.insert(at, recipient);
    }
    /// Room for twice the notes, at most the bound, charged before it is taken.
    fn grow(&mut self, budget: &MemoryBudget) -> bool {
        let capacity = self.noted.recipients.capacity();
        // Twice the room, four at least, never past the bound; `clamp` would refuse a bound
        // under four.
        let wanted = capacity.saturating_mul(2).max(4).min(self.bound.max(1));
        let more = wanted.saturating_sub(capacity);
        let Some(bytes) = more.checked_mul(size_of::<ParticipantId>()) else {
            return false;
        };
        let charged = match &mut self.noted._charge {
            Some(charge) => charge.grow(bytes).is_ok(),
            None => match budget.reserve(BudgetKind::Index, BudgetLane::Ordinary, bytes) {
                Ok(reservation) => {
                    self.noted._charge = Some(reservation.commit());
                    true
                }
                Err(_) => false,
            },
        };
        let len = self.noted.recipients.len();
        charged
            && self
                .noted
                .recipients
                .try_reserve_exact(wanted.saturating_sub(len))
                .is_ok()
    }
    /// Any inbox may have changed; the notes and their room go.
    pub(super) fn all(&mut self) {
        self.noted = Delivered {
            all: true,
            ..Delivered::default()
        };
    }
    /// What was delivered since the last take, and from here on nothing.
    pub(super) fn take(&mut self) -> Delivered {
        std::mem::take(&mut self.noted)
    }
}

#[cfg(test)]
#[path = "delivered_tests.rs"]
mod tests;
