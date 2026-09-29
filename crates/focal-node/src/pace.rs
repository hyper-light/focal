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
    /// The last derivation as one value, for observers.
    derived: Mutex<Option<focal_timing::TickPace>>,
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
    /// timeout before it campaigns (`focal_raft::Raft::set_patience`): a
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
    /// The period passed without a tick.
    pub(crate) fn refuse(&self) {
        self.0.refused.fetch_add(1, Ordering::Relaxed);
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
