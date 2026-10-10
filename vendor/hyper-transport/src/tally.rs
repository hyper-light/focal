//! The exchange table and its running sums: what each connection's exchanges have to send and
//! are owed, kept as the exchanges change rather than folded over all of them when read.
//!
//! The scheduling core asks three sums on every operation: what the classes above a rank still
//! have to send (`demand_above`, which a class's credit is shared against), what the connection's
//! exchanges hold unsent (`held`, the asking wait's charge), and what the peer has declared and not
//! delivered of a rank and the less urgent ones (`backlog`, the answering wait's charge). Each was
//! a fold over every exchange on the connection, and the core asks them for each exchange it
//! visits, so one operation cost O(n²) in the connection's open exchanges: 5.0 µs beside one open
//! exchange, 55.5 ms beside 1,023 (hyper-transport `benches/lookup.rs`, docs/benchmarks.md,
//! 2026-10-05).
//!
//! Here each exchange carries the share it was last counted with ([`Counted`]), and each
//! connection's [`Tally`] holds the sums of those shares by rank. Any mutable access to an
//! exchange through the [`Table`] marks it touched; [`Table::settle`] recounts the touched ones,
//! moving each one's old share out of its connection's tally and its new share in; a removed
//! exchange's share leaves with it. No mutation site has to remember to update a sum, so none can
//! be missed: what is not touched cannot have changed. A read after a settle is exact: the sums are
//! held in `u128` and clamped to `u64` when read, which is what the saturating fold gave
//! (every share is non-negative, so the fold saturates exactly when the true sum passes `u64`).
//!
//! The touched list holds each live exchange at most once (its `touched` flag), and the table
//! settles itself before the list would pass its capacity, so it never grows past the exchange
//! bound. With the `oracle` feature, an endpoint switched to it compares every settled tally with
//! the folds it replaced and counts any difference (`Endpoint::oracle_mismatches`); the
//! test harness switches every endpoint it builds to it.

use std::time::Instant;

use crate::Refusal;
use crate::arena::Arena;
use crate::deadlines;
use crate::exchange::{Exchange, Out};

/// One exchange's share of its connection's sums, as it was last counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counted {
    pub(crate) rank: u8,
    /// Whether it holds a stream: only then does its unsent count against less urgent classes.
    pub(crate) streamed: bool,
    /// What it has still to send (`Outgoing::pending`).
    pub(crate) pending: u64,
    /// What the peer has declared and not delivered of its message (`Incoming::owed`).
    pub(crate) owed: u64,
}

impl Counted {
    pub(crate) fn of<K>(exchange: &Exchange<K>) -> Self {
        Self {
            rank: exchange.rank,
            streamed: exchange.stream.is_some(),
            pending: exchange.out.pending(),
            owed: exchange.incoming.owed(),
        }
    }
}

/// One connection's sums, by rank. Exact: at most 2^32 exchanges (the arena's slot index) of at
/// most 2^64 bytes each sum below 2^96, so the `u128` sums never saturate.
#[derive(Debug)]
pub(crate) struct Tally {
    /// What exchanges holding a stream have to send, by rank.
    streamed: Vec<u128>,
    /// What every exchange has to send.
    held: u128,
    /// What the peer owes, by rank.
    owed: Vec<u128>,
}

impl Tally {
    fn new(ranks: u8) -> Self {
        Self {
            streamed: vec![0; usize::from(ranks)],
            held: 0,
            owed: vec![0; usize::from(ranks)],
        }
    }

    fn add(&mut self, share: Counted) {
        let rank = usize::from(share.rank);
        if share.streamed
            && let Some(sum) = self.streamed.get_mut(rank)
        {
            *sum = sum.saturating_add(u128::from(share.pending));
        }
        self.held = self.held.saturating_add(u128::from(share.pending));
        if let Some(sum) = self.owed.get_mut(rank) {
            *sum = sum.saturating_add(u128::from(share.owed));
        }
    }

    fn sub(&mut self, share: Counted) {
        let rank = usize::from(share.rank);
        if share.streamed
            && let Some(sum) = self.streamed.get_mut(rank)
        {
            *sum = sum.saturating_sub(u128::from(share.pending));
        }
        self.held = self.held.saturating_sub(u128::from(share.pending));
        if let Some(sum) = self.owed.get_mut(rank) {
            *sum = sum.saturating_sub(u128::from(share.owed));
        }
    }

    /// What exchanges holding a stream, of classes more urgent than `rank`, have to send.
    pub(crate) fn streamed_above(&self, rank: u8) -> u64 {
        clamp(self.streamed.iter().take(usize::from(rank)).sum())
    }

    /// What every exchange has to send.
    pub(crate) fn held(&self) -> u64 {
        clamp(self.held)
    }

    /// What the peer owes of the classes of `rank` and the less urgent ones.
    pub(crate) fn owed_from(&self, rank: u8) -> u64 {
        clamp(self.owed.iter().skip(usize::from(rank)).sum())
    }
}

fn clamp(sum: u128) -> u64 {
    u64::try_from(sum).unwrap_or(u64::MAX)
}

/// Whether a progress pass can move `exchange`: `start` gives a stream to one queued for it and
/// writes a head or trailer it holds; `wake` tells an owner refused a write that it can write
/// again. For any other exchange both do nothing, so the pass skips it.
pub(crate) fn visits<K>(exchange: &Exchange<K>) -> bool {
    exchange.stream.is_none()
        || matches!(exchange.out.state, Out::Head | Out::Trailer)
        || (exchange.out.state == Out::Body
            && exchange.wants_write
            && !exchange.writable_sent
            && !exchange.stream_blocked)
}

/// The endpoint's exchanges, each connection's sums over them, and the exchanges touched since
/// the sums were last settled.
#[derive(Debug)]
pub(crate) struct Table<K> {
    arena: Arena<Exchange<K>>,
    touched: Vec<u64>,
    /// Indexed by the connection's key; a key's tally is made when an exchange first counts on it.
    tallies: Vec<Tally>,
    ranks: u8,
    /// Indexed by the connection's key: the exchanges a progress pass visits, as (order, id),
    /// sorted by order.
    actives: Vec<Vec<(u64, u64)>>,
    /// The order the next exchange made takes.
    next_order: u64,
    /// Indexed by the connection's key: a binary min-heap of (due, id), one entry for each
    /// exchange whose wait has a due time (src/deadlines.rs).
    deadlines: Vec<Vec<(Instant, u64)>>,
    /// Each exchange's index in its connection's heap, by arena slot: sized to the bound once.
    positions: Vec<Option<usize>>,
    /// The frontier of a walk of a deadline heap: at most the heap's entries, so at most the
    /// exchange bound, reserved once.
    walk: Vec<usize>,
    /// Settles that found the sums by recounting (the non-vacuity counter for the oracle).
    #[cfg(feature = "oracle")]
    settled: u64,
}

impl<K> Table<K> {
    /// A table of at most `capacity` exchanges, of classes ranked below `ranks`.
    pub(crate) fn new(capacity: usize, ranks: u8) -> Self {
        Self {
            arena: Arena::new(capacity),
            touched: Vec::with_capacity(capacity),
            tallies: Vec::new(),
            ranks,
            actives: Vec::new(),
            next_order: 0,
            deadlines: Vec::new(),
            positions: vec![None; capacity],
            walk: Vec::with_capacity(capacity),
            #[cfg(feature = "oracle")]
            settled: 0,
        }
    }

    pub(crate) fn insert(&mut self, mut value: Exchange<K>) -> Result<u64, Refusal> {
        value.order = self.next_order;
        let id = self.arena.insert(value)?;
        self.next_order = self.next_order.saturating_add(1);
        self.mark(id);
        Ok(id)
    }

    pub(crate) fn get(&self, id: u64) -> Option<&Exchange<K>> {
        self.arena.get(id)
    }

    /// The exchange, marked touched: whatever the caller changes is recounted at the next settle.
    pub(crate) fn get_mut(&mut self, id: u64) -> Option<&mut Exchange<K>> {
        self.mark(id);
        self.arena.get_mut(id)
    }

    /// Removes the exchange, its counted share leaving its connection's sums with it.
    pub(crate) fn remove(&mut self, id: u64) -> Option<Exchange<K>> {
        let exchange = self.arena.remove(id)?;
        if let Some(tally) = self.tallies.get_mut(exchange.connection) {
            tally.sub(exchange.counted);
        }
        if let Some(heap) = self.deadlines.get_mut(exchange.connection) {
            deadlines::remove(heap, &mut self.positions, id);
        }
        if exchange.active
            && let Some(active) = self.actives.get_mut(exchange.connection)
            && let Ok(at) = active.binary_search(&(exchange.order, id))
        {
            active.remove(at);
        }
        Some(exchange)
    }

    /// Records that exchange `id` is at index `at` of its connection's list: bookkeeping that
    /// neither its share nor its visits depend on, so it is not a touch.
    pub(crate) fn place(&mut self, id: u64, at: usize) {
        if let Some(exchange) = self.arena.get_mut(id) {
            exchange.at = at;
        }
    }

    /// The exchanges of connection `key` whose wait is due at `now`, settled, in the order they
    /// were made, appended to `into`. The heap walk stops below any entry not yet due, so it costs
    /// the due entries and the entries bordering them, not the connection's exchanges.
    pub(crate) fn due(&mut self, key: usize, now: Instant, into: &mut Vec<u64>) {
        self.settle();
        let start = into.len();
        let Some(heap) = self.deadlines.get(key) else {
            return;
        };
        deadlines::due(heap, now, &mut self.walk, into);
        let arena = &self.arena;
        if let Some(found) = into.get_mut(start..) {
            found.sort_unstable_by_key(|id| {
                arena.get(*id).map_or(u64::MAX, |exchange| exchange.order)
            });
        }
    }

    /// The exchanges of connection `key` a progress pass visits, settled, in the order they were
    /// made, appended to `into`.
    pub(crate) fn active(&mut self, key: usize, into: &mut Vec<u64>) {
        self.settle();
        if let Some(active) = self.actives.get(key) {
            into.extend(active.iter().map(|(_, id)| *id));
        }
    }

    pub(crate) fn hold(&mut self, id: u64) -> bool {
        self.arena.hold(id)
    }

    pub(crate) fn hold_if(
        &mut self,
        id: u64,
        wanted: impl FnOnce(&mut Exchange<K>) -> bool,
    ) -> bool {
        self.mark(id);
        self.arena.hold_if(id, wanted)
    }

    pub(crate) fn release(&mut self, id: u64) {
        self.arena.release(id);
    }

    pub(crate) fn release_with(&mut self, id: u64, polled: impl FnOnce(&mut Exchange<K>)) {
        self.mark(id);
        self.arena.release_with(id, polled);
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &Exchange<K>> {
        self.arena.values()
    }

    pub(crate) fn len(&self) -> usize {
        self.arena.len()
    }

    /// Marks exchange `id` touched, once; settles first if the list is at its capacity (a removed
    /// exchange's entry stays until a settle, so remove-and-insert cycles could otherwise fill it).
    fn mark(&mut self, id: u64) {
        let Some(exchange) = self.arena.get(id) else {
            return;
        };
        if exchange.touched {
            return;
        }
        if self.touched.len() >= self.touched.capacity() {
            self.settle();
        }
        if let Some(exchange) = self.arena.get_mut(id) {
            exchange.touched = true;
            self.touched.push(id);
        }
    }

    /// Recounts every touched exchange, moving its old share out of its connection's sums and its
    /// new one in.
    pub(crate) fn settle(&mut self) {
        let mut touched = std::mem::take(&mut self.touched);
        for id in touched.drain(..) {
            let Some(exchange) = self.arena.get_mut(id) else {
                continue;
            };
            exchange.touched = false;
            let key = exchange.connection;
            let visited = visits(exchange);
            let revisit = (visited != exchange.active).then_some(exchange.order);
            exchange.active = visited;
            let due = exchange.carry.due();
            let deadline = due != exchange.deadline;
            exchange.deadline = due;
            let share = Counted::of(exchange);
            if let Some(order) = revisit {
                self.revisit(key, (order, id), visited);
            }
            if deadline {
                self.redeadline(key, id, due);
            }
            self.recount(key, id, share);
        }
        self.touched = touched;
        #[cfg(feature = "oracle")]
        {
            self.settled = self.settled.saturating_add(1);
        }
    }

    /// Puts `entry` (order, id) into connection `key`'s visit set, or takes it out.
    fn revisit(&mut self, key: usize, entry: (u64, u64), visited: bool) {
        if self.actives.len() <= key {
            self.actives.resize_with(key.saturating_add(1), Vec::new);
        }
        let Some(active) = self.actives.get_mut(key) else {
            return;
        };
        match (visited, active.binary_search(&entry)) {
            (true, Err(at)) => active.insert(at, entry),
            (false, Ok(at)) => {
                active.remove(at);
            }
            _ => {}
        }
    }

    /// Moves exchange `id`'s entry in connection `key`'s deadline heap to `due`.
    fn redeadline(&mut self, key: usize, id: u64, due: Option<Instant>) {
        if self.deadlines.len() <= key {
            self.deadlines.resize_with(key.saturating_add(1), Vec::new);
        }
        if let Some(heap) = self.deadlines.get_mut(key) {
            deadlines::set(heap, &mut self.positions, id, due);
        }
    }

    /// Moves exchange `id`'s counted share in connection `key`'s sums to `share`.
    fn recount(&mut self, key: usize, id: u64, share: Counted) {
        let Some(exchange) = self.arena.get_mut(id) else {
            return;
        };
        if share == exchange.counted {
            return;
        }
        let before = std::mem::replace(&mut exchange.counted, share);
        let ranks = self.ranks;
        if self.tallies.len() <= key {
            self.tallies
                .resize_with(key.saturating_add(1), || Tally::new(ranks));
        }
        if let Some(tally) = self.tallies.get_mut(key) {
            tally.sub(before);
            tally.add(share);
        }
    }

    /// Connection `key`'s sums, settled.
    pub(crate) fn tally(&mut self, key: usize) -> Option<&Tally> {
        self.settle();
        self.tallies.get(key)
    }

    /// How many times the sums were settled.
    #[cfg(feature = "oracle")]
    pub(crate) fn settled(&self) -> u64 {
        self.settled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn share(rank: u8, streamed: bool, pending: u64, owed: u64) -> Counted {
        Counted {
            rank,
            streamed,
            pending,
            owed,
        }
    }

    /// A sum past `u64` reads as the saturating fold reads it, and taking the share that carried
    /// it there back out leaves the exact sum: the `u128` sums never lose what the fold clamps.
    #[test]
    fn a_sum_past_u64_reads_as_the_fold_and_comes_back_exactly() {
        let mut tally = Tally::new(3);
        let shares = [
            share(0, true, u64::MAX, 5),
            share(0, true, 7, 0),
            share(2, false, 9, u64::MAX),
        ];
        for each in shares {
            tally.add(each);
        }
        let fold = |filter: &dyn Fn(&Counted) -> bool, of: &dyn Fn(&Counted) -> u64| {
            shares
                .iter()
                .filter(|each| filter(each))
                .fold(0u64, |sum, each| sum.saturating_add(of(each)))
        };
        assert_eq!(
            tally.streamed_above(1),
            fold(&|each| each.streamed && each.rank < 1, &|each| each.pending)
        );
        assert_eq!(tally.held(), fold(&|_| true, &|each| each.pending));
        assert_eq!(
            tally.owed_from(1),
            fold(&|each| each.rank >= 1, &|each| each.owed)
        );
        tally.sub(share(0, true, u64::MAX, 5));
        assert_eq!(tally.streamed_above(1), 7);
        assert_eq!(tally.held(), 16);
        assert_eq!(tally.owed_from(0), u64::MAX);
    }

    /// A share counted without a stream adds nothing to the demand of the classes below it, and a
    /// rank past the table's classes counts only in `held`, as the folds count them.
    #[test]
    fn only_streamed_shares_of_known_ranks_count_as_demand() {
        let mut tally = Tally::new(2);
        tally.add(share(0, false, 100, 0));
        tally.add(share(5, true, 30, 4));
        assert_eq!(tally.streamed_above(2), 0);
        assert_eq!(tally.held(), 130);
        assert_eq!(tally.owed_from(0), 0);
    }
}
