//! Each object's events, by where they were published (doc 31 M2): what a family's derivation
//! reads in place of a walk of every event the session holds, which made one retirement cost as
//! much as the whole live set. It is an index of what the committed rows already say, never
//! encoded: built from them where a core is restored, kept as each publication commits, and let
//! go whole where it cannot be kept exactly, to be built again from the rows; no derivation
//! depends on it being there, only on it being exact when it is. An event is filed under its
//! object's home, the affinity its object's rows sit under; a receipt's, whose rows share one
//! table, under its claim.
//!
//! It is charged as the pending index is (`pending_index::entry_charge`): each filed event its
//! entry's share of a B-tree node at least half full, which the publication that files it
//! funded when it was prepared, and the root once. Each funding stays charged where it was
//! (a funded pool's work to its pool, a replay's to the completion lane), the index keeping one
//! permit a source; a retirement gives back what it unfiles.
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane};
use std::collections::BTreeSet;

#[cfg(test)]
#[path = "event_index_tests.rs"]
mod tests;

/// Where one event was published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Position {
    pub(super) sequence: SessionSeq,
    pub(super) ordinal: u32,
}

/// An object's history or a participant's inbox, followed by its publication position.
/// The namespaces keep an object id equal to a participant id from mixing the two ranges.
type Filed = (u8, [u8; 16], u64, u32);
const HISTORY: u8 = 0;
const DELIVERY: u8 = 1;

/// What one filed event may take in the index: a B-tree node's slots for its entry with the
/// node at its least occupancy (half of its eleven, so twice the slot), its share of a node's
/// header and children, and an allocation's overhead.
pub(super) fn entry_charge() -> usize {
    size_of::<Filed>()
        .saturating_mul(2)
        .saturating_add(size_of::<usize>().saturating_mul(4))
}

/// The index's root node, which may hold fewer entries than [`entry_charge`] assumes of a node:
/// eleven slots, a header and twelve children.
fn root_charge() -> usize {
    size_of::<Filed>()
        .saturating_mul(11)
        .saturating_add(size_of::<usize>().saturating_mul(16))
}

/// The most sources the index keeps a permit of: the core's budget in each lane, and the
/// funded pools its owner prepares completion work in. A funding from one more lets the index
/// go, to be built again, all on the core's own budget.
const MAX_CHARGES: usize = 8;

/// What filing `events` events costs: what a publication funds as it is prepared.
pub(super) fn charge_for(events: u32) -> Result<usize, NativeError> {
    entry_charge()
        .checked_mul(2)
        .ok_or(NativeError::Capacity("event index charge"))?
        .checked_mul(usize::try_from(events).map_err(|_| NativeError::Capacity("events"))?)
        .ok_or(NativeError::Capacity("event index charge"))
}

/// The entries this particular event needs, split from its publication's own funding.
pub(super) fn event_charge(delivery: bool) -> usize {
    entry_charge().saturating_mul(if delivery { 2 } else { 1 })
}

/// The home an event is filed under.
pub(super) fn home(event: NativeEvent) -> [u8; 16] {
    match event.fact {
        NativeFact::Receipt { claim, .. } | NativeFact::ReceiptAdopted { claim, .. } => {
            claim.object.0
        }
        _ => layout::affinity(&record_codec::event_object(event)),
    }
}

pub(super) struct EventIndex {
    filed: BTreeSet<Filed>,
    /// The root's charge and every filed event's, while the index is built: one permit a
    /// source, at most [`MAX_CHARGES`].
    charges: Vec<Allocation>,
    /// Whether every committed event is filed: not until built, nor once let go.
    complete: bool,
}

impl std::fmt::Debug for EventIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventIndex")
            .field("filed", &self.filed.len())
            .field("complete", &self.complete)
            .finish_non_exhaustive()
    }
}

impl EventIndex {
    /// The index of a core with no events: complete, its root charged to `budget`.
    pub(super) fn empty(budget: &MemoryBudget) -> Result<Self, MemoryError> {
        let mut charges = Vec::new();
        charges
            .try_reserve_exact(MAX_CHARGES)
            .map_err(|_| MemoryError::AllocationFailed)?;
        charges.push(
            budget
                .reserve(BudgetKind::Index, BudgetLane::Ordinary, root_charge())?
                .commit(),
        );
        Ok(Self {
            filed: BTreeSet::new(),
            charges,
            complete: true,
        })
    }
    /// The index of a core whose rows came whole (restored, hydrated): built at its first use.
    pub(super) fn unbuilt() -> Self {
        Self {
            filed: BTreeSet::new(),
            charges: Vec::new(),
            complete: false,
        }
    }
    pub(super) fn complete(&self) -> bool {
        self.complete
    }
    /// Events filed, a diagnostic.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.filed
            .iter()
            .filter(|(kind, ..)| *kind == HISTORY)
            .count()
    }
    /// What the index is charged.
    #[cfg(test)]
    pub(super) fn charged(&self) -> usize {
        self.charges
            .iter()
            .fold(0usize, |total, charge| total.saturating_add(charge.bytes()))
    }
    /// The events filed under `home`, in the order they were published.
    pub(super) fn positions(&self, home: [u8; 16]) -> impl Iterator<Item = Position> + '_ {
        self.filed
            .range((HISTORY, home, 0, 0)..=(HISTORY, home, u64::MAX, u32::MAX))
            .map(|(_, _, sequence, ordinal)| Position {
                sequence: SessionSeq(*sequence),
                ordinal: *ordinal,
            })
    }
    /// Seek directly to a recipient's next delivery. Neither unrelated events nor the
    /// recipient's earlier history are visited to resume a tail.
    pub(super) fn deliveries(
        &self,
        recipient: ParticipantId,
        after: Option<(SessionSeq, u32)>,
    ) -> impl Iterator<Item = Position> + '_ {
        use std::ops::Bound::{Excluded, Included};
        let start = match after {
            Some((sequence, ordinal)) => Excluded((DELIVERY, recipient.0, sequence.0, ordinal)),
            None => Included((DELIVERY, recipient.0, 0, 0)),
        };
        self.filed
            .range((start, Included((DELIVERY, recipient.0, u64::MAX, u32::MAX))))
            .map(|(_, _, sequence, ordinal)| Position {
                sequence: SessionSeq(*sequence),
                ordinal: *ordinal,
            })
    }
    /// Take `funding`, what a publication's events were funded as it was prepared, for the
    /// events it files next. Not built, the index lets it go.
    pub(super) fn fund(&mut self, mut funding: Allocation) {
        if !self.complete {
            return;
        }
        // A permit absorbs another only of its own source; a mismatch leaves both as they were.
        if self
            .charges
            .iter_mut()
            .any(|charge| charge.absorb(&mut funding).is_ok())
        {
            return;
        }
        if self.charges.len() < self.charges.capacity() {
            // Within the capacity reserved when the index was made.
            self.charges.push(funding);
        } else {
            self.drop_all();
        }
    }
    /// File `position` under `home`, funded by [`Self::fund`]. One filed already is not an
    /// index of the rows: the index is let go.
    pub(super) fn file(
        &mut self,
        home: [u8; 16],
        recipient: Option<ParticipantId>,
        position: Position,
    ) {
        if !self.complete {
            return;
        }
        if !self
            .filed
            .insert((HISTORY, home, position.sequence.0, position.ordinal))
        {
            self.drop_all();
            return;
        }
        if let Some(recipient) = recipient
            && !self
                .filed
                .insert((DELIVERY, recipient.0, position.sequence.0, position.ordinal))
        {
            self.drop_all();
        }
    }
    /// Unfile `position` from `home`, giving back its charge. One not filed is not an index of
    /// the rows: the index is let go.
    pub(super) fn unfile(
        &mut self,
        home: [u8; 16],
        recipient: Option<ParticipantId>,
        position: Position,
    ) {
        if !self.complete {
            return;
        }
        if !self
            .filed
            .remove(&(HISTORY, home, position.sequence.0, position.ordinal))
        {
            self.drop_all();
            return;
        }
        self.release(entry_charge());
        if let Some(recipient) = recipient {
            if !self
                .filed
                .remove(&(DELIVERY, recipient.0, position.sequence.0, position.ordinal))
            {
                self.drop_all();
                return;
            }
            self.release(entry_charge());
        }
    }
    fn release(&mut self, bytes: usize) {
        let released = self
            .charges
            .iter_mut()
            .find(|charge| charge.bytes() >= bytes)
            .map(|charge| charge.split_off(bytes).map(drop));
        if !matches!(released, Some(Ok(()))) {
            self.drop_all();
        }
    }
    /// Let the index go, and its charge: the rows build it again.
    pub(super) fn drop_all(&mut self) {
        self.filed = BTreeSet::new();
        self.charges = Vec::new();
        self.complete = false;
    }
    /// The index of every event `rows` hold under `ledger`, charged to `budget` before it is
    /// built; where the charge is refused, or a row is not an event, it stays unbuilt.
    pub(super) fn build(state: &NativeState, payloads: Option<&dyn payloads::PayloadRead>) -> Self {
        let rows = &state.rows;
        let budget = &state.budget;
        let mut count = 0u32;
        for entry in rows.entries_from(&Key::Event(SessionSeq(0), 0), false) {
            let Key::Event(..) = entry.key else {
                break;
            };
            let Some(next) = count.checked_add(1) else {
                return Self::unbuilt();
            };
            count = next;
        }
        let Ok(mut index) = Self::empty(budget) else {
            return Self::unbuilt();
        };
        let funding = charge_for(count).ok().and_then(|bytes| {
            budget
                .reserve(BudgetKind::Index, BudgetLane::Ordinary, bytes)
                .ok()
        });
        let Some(funding) = funding else {
            return Self::unbuilt();
        };
        let mut funding = funding.commit();
        for entry in rows.entries_from(&Key::Event(SessionSeq(0), 0), false) {
            let Key::Event(sequence, ordinal) = entry.key else {
                break;
            };
            let event = match &entry.value {
                Row::Event(stored) => stored.get(state.ledger, payloads),
                _ => None,
            };
            let Some(event) = event else {
                return Self::unbuilt();
            };
            let Ok(recipient) = state.delivery_recipient_from(event, payloads) else {
                return Self::unbuilt();
            };
            let Ok(charge) = funding.split_off(event_charge(recipient.is_some())) else {
                return Self::unbuilt();
            };
            index.fund(charge);
            index.file(home(event), recipient, Position { sequence, ordinal });
            if !index.complete {
                return index;
            }
        }
        index
    }
}
