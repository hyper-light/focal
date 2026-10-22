//! What the connections' own state holds of an endpoint's memory, learned where the endpoint runs
//! (docs/transport.md §4x).
//!
//! A connection holds, besides its windows and the messages it carries (each charged as it is
//! held), its own state: its QUIC connection and what that allocates (streams, packets in flight,
//! keys, the congestion controller's model), its exchange tables, its rows in the endpoint's
//! tables, and the C library's allocations beside Rust's (aws-lc's keys). What that comes to is no
//! constant: it is the code's, the allocator's and the host's, measured 26.6 to 36.6 KB a connection
//! across three hosts and two allocators (docs/benchmarks.md). The endpoint's owner, which can run
//! the endpoint's work on a heap of its own (hyper-heap), observes what that heap holds at moments
//! nothing is in flight; this account charges the connections' state what the last observation
//! said, as one reservation, and a connection made between observations what one connection held
//! on average then.
//!
//! - **Before any observation** a connection is charged the least it can hold: its structures' own
//!   bytes ([`crate::state_charge`]), which no host's allocator rounds below.
//! - **The base** is what the endpoint's work held with no connection: its first observation, which
//!   its owner makes as it makes the endpoint. Failing that, two observations at different counts
//!   give it (their slope a connection's state).
//! - **An observation** says the connections' state is what the work holds past the base, less what
//!   the receive pool grew by since (charged as the pool, apart): the reservation is trued up to it,
//!   grown where the budget funds it, and what one connection holds is that over the connections
//!   held, never below the least.
//! - **When** an observation is wanted: first; then each time the connections held double or halve,
//!   or as many connections are made as were held at the last one (the population turned over).
//!   An observation walks the heap's pages, a cost that grows with the connections held; observed
//!   at each doubling, it is paid once for as many connections as the walk visits, a constant a
//!   connection however many there are.

use crate::Reservation;

/// An endpoint's account of its connections' state, as [`crate::Stats::state`] reports it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateStats {
    /// What one more connection is charged for its state now.
    pub each: u64,
    /// The bytes the connections' state is charged now, in all.
    pub charged: u64,
    /// The bytes the last observation found held past what the budget funded: what the endpoint
    /// holds and could not charge.
    pub unfunded: u64,
    /// The observations made.
    pub observations: u64,
    /// The bytes the endpoint's work held with no connection, once known.
    pub base: Option<u64>,
}

/// An observation's figures: the connections held, the bytes the endpoint's work held, and the
/// bytes its receive pool held then.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    connections: u64,
    held: u64,
    pool: u64,
}

impl Seen {
    /// What the work held past the receive pool, which is charged as the pool.
    fn past_pool(&self) -> u64 {
        self.held.saturating_sub(self.pool)
    }
}

/// An endpoint's account of its connections' state.
#[derive(Debug)]
pub(crate) struct StateAccount {
    /// What a connection's structures take in themselves: the least it is ever charged.
    least: u64,
    /// What the endpoint's work held with no connection, past its receive pool.
    base: Option<u64>,
    /// A first observation made with connections held, kept until a second at another count
    /// gives the base.
    anchor: Option<Seen>,
    /// What one more connection is charged.
    each: u64,
    /// The connections held: made and not yet gone.
    held: u64,
    /// The connections held at the last observation.
    observed_at: u64,
    /// The connections made since the last observation.
    made_since: u64,
    /// The one reservation the connections' state is charged in.
    charged: Reservation,
    unfunded: u64,
    observations: u64,
}

impl StateAccount {
    /// An account whose connections are charged `least` each until an observation says more.
    pub(crate) fn new(least: u64) -> Self {
        Self {
            least,
            base: None,
            anchor: None,
            each: least,
            held: 0,
            observed_at: 0,
            made_since: 0,
            charged: Reservation::default(),
            unfunded: 0,
            observations: 0,
        }
    }

    /// What one more connection is charged for its state.
    pub(crate) fn each(&self) -> u64 {
        self.each
    }

    /// The connections held.
    #[cfg(test)]
    pub(crate) fn held(&self) -> u64 {
        self.held
    }

    pub(crate) fn stats(&self) -> StateStats {
        StateStats {
            each: self.each,
            charged: self.charged.granted(),
            unfunded: self.unfunded,
            observations: self.observations,
            base: self.base,
        }
    }

    /// A connection was made, its state's `grant` (of [`StateAccount::each`]) taken into the one
    /// reservation. The connection is held whether or not the grant is taken: one the reservation
    /// cannot take (another mark than its own, a buffer) is handed back, to be given back to the
    /// budget, and counted unfunded until the next observation says what is held.
    pub(crate) fn made(&mut self, grant: Reservation) -> Result<(), Reservation> {
        self.held = self.held.saturating_add(1);
        self.made_since = self.made_since.saturating_add(1);
        let granted = grant.granted();
        self.take_in(grant).inspect_err(|_| {
            self.unfunded = self.unfunded.saturating_add(granted);
        })
    }

    /// `grant` taken into the one reservation: an empty one takes the grant whole, the accountant's
    /// mark with it (a reservation made empty carries no mark of the accountant's), a held one
    /// absorbs it.
    fn take_in(&mut self, grant: Reservation) -> Result<(), Reservation> {
        if self.charged.granted() == 0 {
            self.charged = grant;
            return Ok(());
        }
        self.charged.absorb(grant)
    }

    /// A connection went: its share of the reservation, to be given back to the budget. Its share
    /// is the reservation over the connections held, the last one the whole remainder.
    pub(crate) fn gone(&mut self) -> Option<Reservation> {
        let held = self.held;
        self.held = held.saturating_sub(1);
        // The last one gives back the rest: what the work holds with none (tables grown and kept)
        // is the next observation's to say.
        let share = if held <= 1 {
            self.charged.granted()
        } else {
            self.charged.granted().checked_div(held).unwrap_or(0)
        };
        self.charged.split(share)
    }

    /// Whether an observation is wanted now: the first; then once the connections held have doubled
    /// or halved since the last, or as many were made as were held at it.
    pub(crate) fn due(&self) -> bool {
        if self.base.is_none() {
            return self
                .anchor
                .is_none_or(|anchor| anchor.connections != self.held);
        }
        let at = self.observed_at;
        let doubled = self.held >= at.saturating_mul(2).max(1);
        let halved = at > 0 && self.held <= at.checked_div(2).unwrap_or(0);
        let turned = at > 0 && self.made_since >= at;
        doubled || halved || turned
    }

    /// An observation: the endpoint's work holds `held` bytes, its receive pool `pool` of them, with
    /// the connections now held. Returns the bytes the connections' state is to be charged, which
    /// the caller trues the reservation up to ([`StateAccount::grow`], [`StateAccount::shrink`]).
    pub(crate) fn observe(&mut self, held: u64, pool: u64) -> u64 {
        self.observations = self.observations.saturating_add(1);
        self.observed_at = self.held;
        self.made_since = 0;
        let seen = Seen {
            connections: self.held,
            held,
            pool,
        };
        let base = match (self.base, self.anchor) {
            (Some(base), _) => base,
            (None, _) if seen.connections == 0 => {
                self.base = Some(seen.past_pool());
                self.anchor = None;
                return 0;
            }
            (None, None) => {
                // Connections held at the first look: what they hold is told by a second look at
                // another count. Until then they are charged as they were.
                self.anchor = Some(seen);
                return self.charged.granted();
            }
            (None, Some(anchor)) => {
                let Some(base) = Self::base_of(anchor, seen) else {
                    self.anchor = Some(seen);
                    return self.charged.granted();
                };
                self.base = Some(base);
                self.anchor = None;
                base
            }
        };
        let state = seen.past_pool().saturating_sub(base);
        if let Some(each) = state.checked_div(seen.connections) {
            self.each = each.max(self.least);
        }
        state
    }

    /// The base two observations at different counts give: the first's holdings less its
    /// connections' at the slope between them; none where they hold the same count or what they say
    /// cannot be (a slope below nothing).
    fn base_of(first: Seen, second: Seen) -> Option<u64> {
        let (low, high) = if first.connections < second.connections {
            (first, second)
        } else {
            (second, first)
        };
        let connections = high.connections.checked_sub(low.connections)?;
        let bytes = high.past_pool().checked_sub(low.past_pool())?;
        let each = bytes.checked_div(connections)?;
        Some(
            low.past_pool()
                .saturating_sub(each.saturating_mul(low.connections)),
        )
    }

    /// The bytes the reservation is short of `target`, which the caller reserves and hands to
    /// [`StateAccount::grown`], or none where it holds as much.
    pub(crate) fn short_of(&self, target: u64) -> Option<u64> {
        target
            .checked_sub(self.charged.granted())
            .filter(|short| *short > 0)
    }

    /// The reservation grew by `grant` toward `target`; or, the budget refusing it (`None`), what
    /// the work holds past the charge is recorded unfunded.
    pub(crate) fn grown(
        &mut self,
        target: u64,
        grant: Option<Reservation>,
    ) -> Result<(), Reservation> {
        match grant {
            Some(grant) => {
                self.take_in(grant)?;
                self.unfunded = 0;
            }
            None => self.unfunded = target.saturating_sub(self.charged.granted()),
        }
        Ok(())
    }

    /// What the reservation holds past `target`, split off to be given back to the budget.
    pub(crate) fn shrink(&mut self, target: u64) -> Option<Reservation> {
        self.unfunded = 0;
        let past = self.charged.granted().checked_sub(target)?;
        if past == 0 {
            return None;
        }
        self.charged.split(past)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The least a connection is charged in these tests.
    const LEAST: u64 = 9_000;

    /// An account that has observed its base: `base` bytes, a receive pool of `pool`.
    fn based(base: u64, pool: u64) -> StateAccount {
        let mut account = StateAccount::new(LEAST);
        assert!(account.due());
        assert_eq!(account.observe(base, pool), 0);
        account
    }

    /// `n` connections made, each granted what the account charges one then.
    fn make(account: &mut StateAccount, n: u64) {
        for _ in 0..n {
            let grant = Reservation::new(account.each(), Vec::new());
            account.made(grant).unwrap();
        }
    }

    /// The reservation trued up to what an observation says.
    fn true_up(account: &mut StateAccount, target: u64) {
        match account.short_of(target) {
            Some(short) => account
                .grown(target, Some(Reservation::new(short, Vec::new())))
                .unwrap(),
            None => {
                account.shrink(target);
            }
        }
    }

    #[test]
    fn before_any_observation_a_connection_is_charged_the_least() {
        let mut account = StateAccount::new(LEAST);
        assert_eq!(account.each(), LEAST);
        make(&mut account, 3);
        assert_eq!(account.stats().charged, 3 * LEAST);
    }

    #[test]
    fn an_observation_charges_what_the_work_holds_past_its_base_and_the_pool() {
        let mut account = based(1_000_000, 64_000);
        make(&mut account, 1);
        assert!(account.due(), "the first connection doubles none");
        // The work holds 36 KB more, and its pool grew by 8 KB, which is the pool's to charge.
        let target = account.observe(1_000_000 + 36_000 + 8_000, 64_000 + 8_000);
        assert_eq!(target, 36_000);
        assert_eq!(account.each(), 36_000);
        true_up(&mut account, target);
        assert_eq!(account.stats().charged, 36_000);
        // Made at the learned charge; observed again once they double.
        make(&mut account, 1);
        assert!(account.due());
        let target = account.observe(1_000_000 + 2 * 35_000 + 8_000, 72_000);
        assert_eq!((target, account.each()), (70_000, 35_000));
        true_up(&mut account, target);
        assert_eq!(account.stats().charged, 70_000);
    }

    #[test]
    fn what_one_connection_is_charged_is_never_below_its_structures() {
        let mut account = based(1_000_000, 0);
        make(&mut account, 4);
        account.observe(1_000_000 + 4 * 2_000, 0);
        assert_eq!(account.each(), LEAST);
    }

    #[test]
    fn observations_come_as_the_connections_double_halve_or_turn_over() {
        let mut account = based(0, 0);
        make(&mut account, 1);
        assert!(account.due());
        account.observe(30_000, 0);
        make(&mut account, 1);
        assert!(account.due(), "2 doubles 1");
        account.observe(60_000, 0);
        make(&mut account, 1);
        assert!(!account.due(), "3 neither doubles nor halves 2");
        make(&mut account, 1);
        assert!(account.due(), "4 doubles 2");
        account.observe(120_000, 0);
        // Four gone and four made at four held: the population turned over.
        for _ in 0..3 {
            account.gone();
            make(&mut account, 1);
        }
        assert!(!account.due());
        account.gone();
        make(&mut account, 1);
        assert!(account.due(), "as many made as were held");
        account.observe(120_000, 0);
        account.gone();
        assert!(!account.due(), "3 does not halve 4");
        account.gone();
        assert!(account.due(), "2 halves 4");
    }

    #[test]
    fn a_connection_going_gives_back_its_share_and_the_last_the_rest() {
        let mut account = based(0, 0);
        make(&mut account, 3);
        let target = account.observe(100_000, 0);
        true_up(&mut account, target);
        let share = account.gone().unwrap();
        assert_eq!(share.granted(), 33_333);
        let share = account.gone().unwrap();
        assert_eq!(share.granted(), 33_333);
        let last = account.gone().unwrap();
        assert_eq!(last.granted(), 33_334);
        assert_eq!(account.stats().charged, 0);
        assert_eq!(account.held(), 0);
    }

    #[test]
    fn a_refused_true_up_is_recorded_unfunded_and_the_next_observation_clears_it() {
        let mut account = based(0, 0);
        make(&mut account, 2);
        let target = account.observe(80_000, 0);
        let short = account.short_of(target).unwrap();
        assert_eq!(short, 80_000 - 2 * LEAST);
        account.grown(target, None).unwrap();
        assert_eq!(account.stats().unfunded, short);
        assert_eq!(account.stats().charged, 2 * LEAST);
        make(&mut account, 2);
        let target = account.observe(80_000, 0);
        true_up(&mut account, target);
        assert_eq!(account.stats().unfunded, 0);
        assert_eq!(account.stats().charged, 80_000);
    }

    /// An accountant marks its grants (focal's, with the lane each is funded from): the reservation
    /// takes the mark of its first grant and every one after it, the connections counted as made.
    #[test]
    fn grants_of_an_accountants_mark_are_taken_and_counted() {
        let mut account = based(0, 0);
        for _ in 0..3 {
            account
                .made(Reservation::tagged(account.each(), Vec::new(), 1))
                .unwrap();
        }
        assert_eq!((account.held(), account.stats().charged), (3, 3 * LEAST));
        assert_eq!(account.gone().unwrap().tag(), 1);
        let target = account.observe(90_000, 0);
        let short = account.short_of(target).unwrap();
        account
            .grown(target, Some(Reservation::tagged(short, Vec::new(), 1)))
            .unwrap();
        assert_eq!(account.stats().charged, 90_000);
        // A grant of another mark is handed back, its connection held and its bytes unfunded.
        let refused = account
            .made(Reservation::tagged(LEAST, Vec::new(), 2))
            .unwrap_err();
        assert_eq!(refused.granted(), LEAST);
        assert_eq!((account.held(), account.stats().unfunded), (3, LEAST));
    }

    #[test]
    fn without_a_base_two_counts_give_it() {
        let mut account = StateAccount::new(LEAST);
        make(&mut account, 10);
        assert!(account.due());
        // 10 connections over a base of 500 KB, each 30 KB: the first look cannot say which.
        assert_eq!(account.observe(500_000 + 300_000, 0), 10 * LEAST);
        assert!(!account.due(), "the same count says nothing more");
        make(&mut account, 10);
        assert!(account.due());
        let target = account.observe(500_000 + 600_000, 0);
        assert_eq!(account.stats().base, Some(500_000));
        assert_eq!((target, account.each()), (600_000, 30_000));
    }
}
