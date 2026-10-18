//! The producing sessions a part keeps (doc 34 P6): for each stream and each identity that
//! publishes to it, the sessions it publishes on, each with the sequence its next batch must begin
//! with. A batch is appended only in its session's order; one its session already took is
//! recognised when it comes again and answered with its offsets where the part still says them,
//! never appended twice; one past a gap is refused, naming the sequence the session waits for, so
//! its publisher sends again from there (Kafka's idempotent producer, KIP-98: duplicates answered
//! `DUPLICATE_SEQUENCE_NUMBER`, gaps refused `OUT_OF_ORDER_SEQUENCE_NUMBER`).
//!
//! An identity's sessions are its own: a batch is judged against the sessions of the identity
//! that published it (the transport's authenticated peer, which a group's leader states with the
//! entry), so no publisher can take another's session by naming it, whatever it read of the
//! sessions delivered batches carry. A new session begins at sequence zero, as Kafka's producer
//! does under a fresh producer id; a producer that restarts opens one. An identity keeps
//! [`SESSIONS_PER_IDENTITY`] sessions in a stream; a new one replaces the one it used least
//! recently, in the order the part appended their batches, alike at every replica.

use std::collections::HashMap;

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};

use crate::BusError;

/// Derived: the sessions an identity keeps in a stream: the one it publishes on, and the one it
/// replaced, whose batches sent before the replacement may still be committed after it (the
/// derivation of `limits::CONNECTIONS_PER_CLIENT`: the connection an identity is served on and the
/// one that replaces it).
pub const SESSIONS_PER_IDENTITY: usize = 2;

/// One session's place in a stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Session {
    /// The session, as its batches state it (`Batch::producer`).
    pub producer: u64,
    /// The sequence its next batch takes.
    pub next: u64,
    /// Its newest batch's first sequence.
    pub newest: u64,
    /// The offset its newest batch's first record took.
    pub base: u64,
}

/// What a batch's session says of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Next in its session's order, or the first batch of a new session: append it.
    Append,
    /// Its session took it already: not appended again. Its first offset where it was the
    /// session's newest batch; none where the part no longer says.
    Again {
        /// The offset its first record took.
        base: Option<u64>,
    },
    /// Past a gap in its session, or across its session's next sequence: refused, its publisher
    /// to send again from `expected`, which a session the part does not hold waits for at zero.
    OutOfOrder {
        /// The sequence its session's next batch must begin with.
        expected: u64,
    },
}

/// What one identity's sessions in one stream cost the table: the entry, and its map slot with the
/// map's control byte.
pub const ENTRY_BYTES: usize = std::mem::size_of::<Entry>()
    .saturating_add(std::mem::size_of::<((u64, u64), usize)>())
    .saturating_add(1);

/// An identity's sessions in a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The stream.
    pub stream: u64,
    /// The identity that published on them.
    pub owner: u64,
    /// Its sessions, most recently used first; none in a place it never filled.
    pub sessions: [Option<Session>; SESSIONS_PER_IDENTITY],
}

/// The part's sessions, at most `limit` identities' in all its streams together.
#[derive(Debug)]
pub struct ProducerTable {
    slots: HashMap<(u64, u64), usize>,
    entries: Vec<Entry>,
    limit: usize,
    budget: MemoryBudget,
    /// The charge for what the table's capacities hold.
    charge: Option<Allocation>,
}

impl ProducerTable {
    /// A table of at most `limit` entries, its memory charged to `budget`.
    pub fn new(limit: usize, budget: MemoryBudget) -> Self {
        Self {
            slots: HashMap::new(),
            entries: Vec::new(),
            limit,
            budget,
            charge: None,
        }
    }

    /// Entries held: identities in streams.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table holds no entry.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Bytes the table's capacities hold, as charged.
    pub fn bytes(&self) -> usize {
        self.charge.as_ref().map_or(0, Allocation::bytes)
    }

    /// Every entry, in the order the table took them.
    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    /// Empties the table, keeping its room and its charge.
    pub fn clear(&mut self) {
        self.slots.clear();
        self.entries.clear();
    }

    /// What `owner`'s batch on `stream` of session `producer`, its records `first..first+count`
    /// of the session's sequence, is to the part.
    pub fn judge(&self, stream: u64, owner: u64, producer: u64, first: u64, count: u64) -> Verdict {
        let session = self
            .slots
            .get(&(stream, owner))
            .and_then(|slot| self.entries.get(*slot))
            .and_then(|entry| {
                entry
                    .sessions
                    .iter()
                    .flatten()
                    .find(|session| session.producer == producer)
            });
        let end = first.saturating_add(count);
        match session {
            Some(session) if end <= session.next => Verdict::Again {
                base: (first == session.newest).then_some(session.base),
            },
            Some(session) if first == session.next => Verdict::Append,
            Some(session) => Verdict::OutOfOrder {
                expected: session.next,
            },
            None if first == 0 => Verdict::Append,
            None => Verdict::OutOfOrder { expected: 0 },
        }
    }

    /// Whether a batch of `owner` on `stream` can be recorded: the identity holds an entry in the
    /// stream, or the table has room for one more.
    pub fn admits(&self, stream: u64, owner: u64) -> bool {
        self.slots.contains_key(&(stream, owner)) || self.entries.len() < self.limit
    }

    /// Records an appended batch: `owner`'s session `producer` on `stream` takes `first + count`
    /// next, its newest batch's first record at `base`. A session the identity does not hold in
    /// the stream takes the place of the one it used least recently.
    pub fn record(
        &mut self,
        stream: u64,
        owner: u64,
        producer: u64,
        (first, count, base): (u64, u64, u64),
    ) -> Result<(), BusError> {
        let session = Session {
            producer,
            next: first.saturating_add(count),
            newest: first,
            base,
        };
        let slot = self.slot(stream, owner)?;
        let entry = self
            .entries
            .get_mut(slot)
            .ok_or(BusError::Capacity("producer slot"))?;
        // Most recently used first: the session moves to the front, and a new one pushes the
        // least recently used out of the last place.
        let at = entry
            .sessions
            .iter()
            .position(|held| held.is_some_and(|held| held.producer == producer))
            .unwrap_or(SESSIONS_PER_IDENTITY.saturating_sub(1));
        if let Some(place) = entry.sessions.get_mut(at) {
            *place = Some(session);
        }
        if let Some(moved) = entry.sessions.get_mut(..=at) {
            moved.rotate_right(1);
        }
        Ok(())
    }

    /// Room for `entries` entries in all, charged: a table fed what another records keeps the other's
    /// room, so recording what the other recorded never needs more.
    pub fn reserve(&mut self, entries: usize) -> Result<(), BusError> {
        let more = entries.saturating_sub(self.entries.len());
        let short = |_| BusError::Memory(focal_memory::MemoryError::AllocationFailed);
        self.slots.try_reserve(more).map_err(short)?;
        self.entries.try_reserve(more).map_err(short)?;
        self.charge_growth()
    }

    /// Replaces everything the table holds with `entries`: a part opened from an image.
    pub fn restore(&mut self, entries: impl IntoIterator<Item = Entry>) -> Result<(), BusError> {
        self.clear();
        for given in entries {
            let slot = self.slot(given.stream, given.owner)?;
            if let Some(entry) = self.entries.get_mut(slot) {
                entry.sessions = given.sessions;
            }
        }
        Ok(())
    }

    /// The place of `owner`'s entry in `stream`, made if the table has room for one more.
    fn slot(&mut self, stream: u64, owner: u64) -> Result<usize, BusError> {
        if let Some(slot) = self.slots.get(&(stream, owner)) {
            return Ok(*slot);
        }
        if self.entries.len() >= self.limit {
            return Err(BusError::Capacity("producing identities in a part"));
        }
        // The room first, charged, so a refusal leaves the table as it was.
        let short = |_| BusError::Memory(focal_memory::MemoryError::AllocationFailed);
        self.slots.try_reserve(1).map_err(short)?;
        self.entries.try_reserve(1).map_err(short)?;
        self.charge_growth()?;
        let slot = self.entries.len();
        self.entries.push(Entry {
            stream,
            owner,
            sessions: [None; SESSIONS_PER_IDENTITY],
        });
        self.slots.insert((stream, owner), slot);
        Ok(slot)
    }

    /// What the table's capacities hold: the map's buckets (a power of two at least an eighth over
    /// its capacity) and the entries' vector.
    fn capacity_bytes(&self) -> Option<usize> {
        let buckets = self
            .slots
            .capacity()
            .checked_mul(8)?
            .checked_div(7)?
            .checked_next_power_of_two()?;
        buckets
            .checked_mul(std::mem::size_of::<((u64, u64), usize)>().checked_add(1)?)?
            .checked_add(
                self.entries
                    .capacity()
                    .checked_mul(std::mem::size_of::<Entry>())?,
            )
    }

    /// Charges what the table's capacities hold now, beyond what was charged.
    fn charge_growth(&mut self) -> Result<(), BusError> {
        let held = self
            .capacity_bytes()
            .ok_or(BusError::Capacity("producer table bytes"))?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> ProducerTable {
        ProducerTable::new(8, MemoryBudget::new(1 << 20, 0).unwrap())
    }

    #[test]
    fn a_session_takes_its_batches_in_order_and_knows_each_again() {
        let mut t = table();
        assert_eq!(t.judge(1, 7, 42, 0, 3), Verdict::Append);
        t.record(1, 7, 42, (0, 3, 100)).unwrap();
        assert_eq!(t.judge(1, 7, 42, 3, 2), Verdict::Append);
        t.record(1, 7, 42, (3, 2, 103)).unwrap();
        // Its newest batch again: answered with its offsets; an older one: taken, offsets unsaid.
        assert_eq!(t.judge(1, 7, 42, 3, 2), Verdict::Again { base: Some(103) });
        assert_eq!(t.judge(1, 7, 42, 0, 3), Verdict::Again { base: None });
    }

    #[test]
    fn a_batch_past_a_gap_or_across_the_next_sequence_is_refused_naming_it() {
        let mut t = table();
        // A new session's first batch must begin at zero: one that does not waits for it.
        assert_eq!(t.judge(1, 7, 42, 3, 2), Verdict::OutOfOrder { expected: 0 });
        t.record(1, 7, 42, (0, 3, 0)).unwrap();
        assert_eq!(t.judge(1, 7, 42, 5, 1), Verdict::OutOfOrder { expected: 3 });
        // Overlapping what the session took and what it has not.
        assert_eq!(t.judge(1, 7, 42, 2, 2), Verdict::OutOfOrder { expected: 3 });
    }

    #[test]
    fn an_identitys_sessions_are_its_own_and_its_least_recent_is_replaced() {
        let mut t = table();
        t.record(1, 7, 42, (0, 3, 0)).unwrap();
        // Another identity naming the same session holds a session of its own.
        assert_eq!(t.judge(1, 8, 42, 3, 1), Verdict::OutOfOrder { expected: 0 });
        assert_eq!(t.judge(1, 8, 42, 0, 1), Verdict::Append);
        // A second session of the first identity, then a third: the least recently used goes.
        t.record(1, 7, 43, (0, 1, 3)).unwrap();
        t.record(1, 7, 42, (3, 1, 4)).unwrap();
        t.record(1, 7, 44, (0, 1, 5)).unwrap();
        assert_eq!(t.judge(1, 7, 42, 4, 1), Verdict::Append);
        assert_eq!(t.judge(1, 7, 43, 1, 1), Verdict::OutOfOrder { expected: 0 });
        assert_eq!(t.judge(1, 7, 44, 1, 1), Verdict::Append);
        // Streams are apart.
        assert_eq!(t.judge(2, 7, 42, 0, 1), Verdict::Append);
    }

    #[test]
    fn the_table_refuses_an_identity_past_its_limit_and_restores_whole() {
        let mut t = ProducerTable::new(2, MemoryBudget::new(1 << 20, 0).unwrap());
        t.record(1, 7, 42, (0, 1, 0)).unwrap();
        t.record(2, 7, 42, (0, 1, 0)).unwrap();
        assert!(t.admits(1, 7) && !t.admits(3, 7));
        assert!(matches!(
            t.record(3, 7, 42, (0, 1, 0)),
            Err(BusError::Capacity(_))
        ));
        let entries: Vec<Entry> = t.iter().copied().collect();
        let mut again = ProducerTable::new(2, MemoryBudget::new(1 << 20, 0).unwrap());
        again.restore(entries.iter().copied()).unwrap();
        assert_eq!(again.iter().copied().collect::<Vec<_>>(), entries);
        assert_eq!(again.judge(2, 7, 42, 1, 1), Verdict::Append);
    }
}
