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
    /// The last derivation as one value, for observers.
    derived: Mutex<Option<focal_timing::TickPace>>,
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
    /// The owner ran one period.
    pub(crate) fn advance(&self) {
        self.0.periods.fetch_add(1, Ordering::Relaxed);
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
