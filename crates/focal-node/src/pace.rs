//! An owner's tick period, shared between the owner thread and whoever
//! derives it from measured round trips (27 §3.1 P2).
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// Zero means "as configured". The owner reads the period once per tick and
/// clamps it between its configured period and its ceiling, so nothing
/// written here can make it tick faster than configured or slower than its
/// ceiling. The handle is shared because the owner is a thread of its own
/// and the derivation runs beside the transport that measures.
#[derive(Clone, Default)]
pub(crate) struct TickPeriod(Arc<TickShared>);
#[derive(Default)]
struct TickShared {
    period_ns: AtomicU64,
    election_tick: AtomicUsize,
    /// The periods the owner has run: its progress, which a wait is
    /// charged in (27 §3.1 P8).
    periods: AtomicU64,
    /// The periods in which the replica was not ticked, because it was
    /// refused the room or still persisted what the tick before had left.
    refused: AtomicU64,
    /// When the owner last ran a period, as nanoseconds since the process
    /// began; and the longest a period took, from one to the next. An owner
    /// that stalls in a period (a disk that took long, a thread that was
    /// not scheduled) is seen here and nowhere else: its replica missed
    /// its heartbeats and its followers may have campaigned.
    last_advance_ns: AtomicU64,
    longest_ns: AtomicU64,
    /// The longest a period took beyond what the owner meant it to be: a
    /// stall, which the replica's patience covers ([`Self::patience`]) and
    /// which is remembered, with when it was seen, for the margin the
    /// paths are covered by times its own length (`ELECTION_MARGIN`): a
    /// stall that lasted a second stands for ten, and a longer one takes
    /// its place at once. The excess and not the period: a period is what
    /// the pace made it, and a pace fed its own periods would hold itself
    /// wherever it was.
    stall_ns: AtomicU64,
    stall_seen_ns: AtomicU64,
    /// The quorum's exchange tail with this group's other voters, in
    /// nanoseconds, zero while not enough of them are measured
    /// ([`quorum_tail`]): how late a commit's answers may come from
    /// followers whose owners stall, which the owner's own periods do not
    /// show (27 §8.4).
    quorum_tail_ns: AtomicU64,
    /// The last derivation as one value, for observers.
    derived: Mutex<Option<focal_timing::TickPace>>,
    /// What the session turned away for room, counted where it was turned
    /// away (`InputRefusals`): beside its periods because its host and its
    /// owner, on threads of their own, both turn input away and both hold
    /// this, and the node's metrics read it in place.
    frames_refused: AtomicU64,
    requests_refused: AtomicU64,
    frames_dropped: AtomicU64,
    requests_dropped: AtomicU64,
}
/// What a session turned away for room, of peers' frames and participants'
/// requests apart: refused by its host before it was queued — its queue or
/// its memory full — and dropped by its fleet's scheduler at its quota once
/// queued. A peer counts a refused frame lost; a dropped one it asks again
/// once, then counts lost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct InputRefusals {
    pub(crate) frames_refused: u64,
    pub(crate) requests_refused: u64,
    pub(crate) frames_dropped: u64,
    pub(crate) requests_dropped: u64,
}
/// When this process began, for the owners' periods to be timed against.
fn began() -> std::time::Instant {
    static BEGAN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    *BEGAN.get_or_init(std::time::Instant::now)
}
impl TickPeriod {
    #[cfg(test)]
    pub(crate) fn set(&self, period: Duration) {
        self.store(period);
    }
    fn store(&self, period: Duration) {
        self.0.period_ns.store(
            u64::try_from(period.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
    /// Put a derivation in force from the owner's next tick.
    pub(crate) fn publish(&self, pace: focal_timing::TickPace) {
        self.store(pace.period);
        if let Ok(mut derived) = self.0.derived.lock() {
            *derived = Some(pace);
        }
    }
    pub(crate) fn derived(&self) -> Option<focal_timing::TickPace> {
        self.0.derived.lock().ok().and_then(|derived| *derived)
    }
    /// Whether the period in force is longer than the configured one.
    pub(crate) fn stretched(&self, configured: Duration, ceiling: Duration) -> bool {
        self.get(configured, ceiling) > configured
    }
    pub(crate) fn get(&self, configured: Duration, ceiling: Duration) -> Duration {
        Duration::from_nanos(self.0.period_ns.load(Ordering::Relaxed))
            .clamp(configured, ceiling.max(configured))
    }
    /// The owner ran one period, which it meant to be `intended` long.
    pub(crate) fn advance(&self, intended: Duration) {
        self.0.periods.fetch_add(1, Ordering::Relaxed);
        let now = u64::try_from(began().elapsed().as_nanos()).unwrap_or(u64::MAX);
        let last = self.0.last_advance_ns.swap(now, Ordering::Relaxed);
        if last != 0 {
            let took = now.saturating_sub(last);
            self.0.longest_ns.fetch_max(took, Ordering::Relaxed);
            let meant = u64::try_from(intended.as_nanos()).unwrap_or(u64::MAX);
            let excess = took.saturating_sub(meant);
            if excess >= self.stall_at(now) {
                self.0.stall_ns.store(excess, Ordering::Relaxed);
                self.0.stall_seen_ns.store(now, Ordering::Relaxed);
            }
        }
    }
    /// The stall remembered at `now`: none once it has stood for the
    /// margin times its length.
    fn stall_at(&self, now: u64) -> u64 {
        let stall = self.0.stall_ns.load(Ordering::Relaxed);
        let seen = self.0.stall_seen_ns.load(Ordering::Relaxed);
        let held = now.saturating_sub(seen);
        if held >= focal_timing::ELECTION_MARGIN.saturating_mul(stall) {
            0
        } else {
            stall
        }
    }
    /// The longest a period took, from one to the next.
    pub(crate) fn longest(&self) -> Duration {
        Duration::from_nanos(self.0.longest_ns.load(Ordering::Relaxed))
    }
    /// The stall the owner remembers now, in nanoseconds.
    pub(crate) fn stall(&self) -> u64 {
        self.stall_at(u64::try_from(began().elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
    /// The ticks the stall is covered by: what the longest stall the owner
    /// remembers took, and a tail of the paths after it, in periods of the
    /// pace in force. The replica waits as many beyond its election
    /// timeout before it campaigns (`hyper_raft::Raft::set_patience`): a
    /// node that stalls cannot tell a leader that stalls as it does from
    /// one that died, and a leader's heartbeat leaves its tick, so the one
    /// after a stall takes a tail to arrive. In ticks and not in the
    /// period (27 §3.1 P2): what stretches is the election timeout, and
    /// nothing else that is counted in periods.
    pub(crate) fn patience(&self, configured: Duration, ceiling: Duration) -> usize {
        let stall = self.stall();
        if stall == 0 {
            return 0;
        }
        let tail = self.derived().map_or(0, |pace| pace.broadcast_tail_ns);
        let period = u64::try_from(self.get(configured, ceiling).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        usize::try_from(stall.saturating_add(tail).div_ceil(period)).unwrap_or(usize::MAX)
    }
    /// Put the quorum's exchange tail in force ([`quorum_tail`]); none
    /// while not enough voters are measured.
    pub(crate) fn publish_quorum_tail(&self, tail: Option<Duration>) {
        let ns = tail.map_or(0, |tail| u64::try_from(tail.as_nanos()).unwrap_or(u64::MAX));
        self.0.quorum_tail_ns.store(ns, Ordering::Relaxed);
    }
    /// The ticks the quorum's exchange tail takes, in periods of the pace
    /// in force: what a leader's followers may answer beyond its own
    /// periods. At most an election timeout at the tick ceiling, the
    /// longest a dead leader goes unnoticed; zero while not enough voters
    /// are measured. What a request waits for a commit is given these
    /// beyond its time.
    pub(crate) fn quorum_ticks(&self, configured: Duration, ceiling: Duration) -> usize {
        let tail = self.0.quorum_tail_ns.load(Ordering::Relaxed);
        if tail == 0 {
            return 0;
        }
        let period = u64::try_from(self.get(configured, ceiling).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let most = u64::try_from(ceiling.max(configured).as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(self.election_tick().max(1)).unwrap_or(u64::MAX));
        usize::try_from(tail.min(most).div_ceil(period)).unwrap_or(usize::MAX)
    }
    /// The period passed without a tick.
    pub(crate) fn refuse(&self) {
        self.0.refused.fetch_add(1, Ordering::Relaxed);
    }
    /// The host refused a peer's frame (`frame`) or a participant's request
    /// for room before it was queued.
    pub(crate) fn refuse_input(&self, frame: bool) {
        let counter = if frame {
            &self.0.frames_refused
        } else {
            &self.0.requests_refused
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    /// The fleet's scheduler dropped a peer's frame (`frame`) or a
    /// participant's request at its quota once queued.
    pub(crate) fn drop_input(&self, frame: bool) {
        let counter = if frame {
            &self.0.frames_dropped
        } else {
            &self.0.requests_dropped
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn input_refusals(&self) -> InputRefusals {
        InputRefusals {
            frames_refused: self.0.frames_refused.load(Ordering::Relaxed),
            requests_refused: self.0.requests_refused.load(Ordering::Relaxed),
            frames_dropped: self.0.frames_dropped.load(Ordering::Relaxed),
            requests_dropped: self.0.requests_dropped.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn refused(&self) -> u64 {
        self.0.refused.load(Ordering::Relaxed)
    }
    pub(crate) fn periods(&self) -> u64 {
        self.0.periods.load(Ordering::Relaxed)
    }
    /// The owner states its replica's election ticks once it has opened it.
    pub(crate) fn announce(&self, election_tick: usize) {
        self.0.election_tick.store(election_tick, Ordering::Relaxed);
    }
    /// Zero until the owner has opened its replica.
    pub(crate) fn election_tick(&self) -> usize {
        self.0.election_tick.load(Ordering::Relaxed)
    }
}
/// The exchange tail a commit waits on among a group of `voters` voters, this
/// one among them, from the `tails` measured with the others: a commit
/// needs a majority, this voter and the `voters / 2` others that answer
/// first, so the tail is the `voters / 2`-th smallest. None where fewer of
/// the others are measured, or where this voter is a majority alone: then
/// nothing is known, or nothing waited on.
pub(crate) fn quorum_tail(mut tails: Vec<Duration>, voters: usize) -> Option<Duration> {
    let needed = voters.checked_div(2)?;
    if needed == 0 {
        return None;
    }
    tails.sort_unstable();
    tails.get(needed.checked_sub(1)?).copied()
}
impl PartialEq for TickPeriod {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for TickPeriod {}
impl std::fmt::Debug for TickPeriod {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TickPeriod")
            .field("derived", &self.derived())
            .finish()
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
mod quorum_tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// A commit waits on the majority that answers first: of three voters
    /// the faster other, of five the second fastest of four, of four the
    /// second fastest of three (a majority of four is three).
    #[test]
    fn a_commit_waits_on_the_majority_that_answers_first() {
        assert_eq!(quorum_tail(vec![ms(30), ms(5)], 3), Some(ms(5)));
        assert_eq!(
            quorum_tail(vec![ms(40), ms(9), ms(700), ms(3)], 5),
            Some(ms(9))
        );
        assert_eq!(quorum_tail(vec![ms(40), ms(9), ms(700)], 4), Some(ms(40)));
    }

    /// Too few others measured says nothing, and a voter that is a
    /// majority alone waits on no one.
    #[test]
    fn too_few_measured_or_a_lone_voter_waits_on_nothing() {
        assert_eq!(quorum_tail(vec![], 3), None);
        assert_eq!(quorum_tail(vec![ms(5)], 5), None);
        assert_eq!(quorum_tail(vec![], 1), None);
        assert_eq!(quorum_tail(vec![ms(5)], 0), None);
    }

    /// The tail in ticks of the pace in force, rounded up, never past an
    /// election timeout at the ceiling, and none while unmeasured.
    #[test]
    fn the_quorum_tail_in_ticks_is_bounded_by_an_election_timeout_at_the_ceiling() {
        let pace = TickPeriod::default();
        pace.announce(10);
        let (tick, ceiling) = (ms(25), ms(100));
        assert_eq!(pace.quorum_ticks(tick, ceiling), 0);
        pace.publish_quorum_tail(Some(ms(301)));
        assert_eq!(pace.quorum_ticks(tick, ceiling), 13);
        pace.publish_quorum_tail(Some(Duration::from_secs(3600)));
        assert_eq!(
            pace.quorum_ticks(tick, ceiling),
            40,
            "ten ticks at 100 ms, in 25 ms ticks"
        );
        pace.publish_quorum_tail(None);
        assert_eq!(pace.quorum_ticks(tick, ceiling), 0);
    }
}
