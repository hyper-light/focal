//! Time on a hyper-rt shard: the shard's monotonic clock (a simulated shard's virtual one), and the
//! waits kept by its timer wheel. Off a shard there is no clock to read and no timer to keep: a wait
//! there is refused `NotOnShardThread`, typed, as the wheel refuses it.
use std::future::Future;
use std::time::Duration;

use crate::RtError;

/// `period` in the shard clock's nanoseconds, saturating at the clock's end.
pub fn nanos(period: Duration) -> u64 {
    u64::try_from(period.as_nanos()).unwrap_or(u64::MAX)
}

/// An instant on the shard's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(u64);

impl Instant {
    /// Now, on the current shard's clock; the clock's origin off a shard.
    pub fn now() -> Self {
        Self(hyper_rt::futures::now_ns())
    }
    /// How long ago this instant was; zero for one not yet reached.
    pub fn elapsed(self) -> Duration {
        Self::now().duration_since(self)
    }
    /// How long after `earlier` this instant is; zero when it is not after it.
    pub fn duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
    /// The instant `period` after this one, if the clock can name it.
    pub fn checked_add(self, period: Duration) -> Option<Self> {
        u64::try_from(period.as_nanos())
            .ok()
            .and_then(|ns| self.0.checked_add(ns))
            .map(Self)
    }
    /// The instant `period` after this one, or the clock's end.
    pub fn saturating_add(self, period: Duration) -> Self {
        Self(self.0.saturating_add(nanos(period)))
    }
    /// How long until this instant: zero once it has passed.
    pub fn remaining(self) -> Duration {
        self.duration_since(Self::now())
    }
}

// Each wait below is the runtime's own future, made here and handed back: no wait adds a layer of
// its own around the work. A layer that made the work's future in its poll would keep a temporary
// the work's size in its frame, and a debug build keeps every layer's frame on the stack at once.

/// Waits `period` on the shard's wheel.
pub fn sleep(period: Duration) -> impl Future<Output = Result<(), RtError>> {
    hyper_rt::futures::sleep(nanos(period))
}

/// Waits until `deadline` on the shard's wheel; at once for one that has passed.
pub fn sleep_until(deadline: Instant) -> impl Future<Output = Result<(), RtError>> {
    sleep(deadline.remaining())
}

/// What `work` yields within `period` on the shard's clock, or `None` once the period is over (the
/// work dropped with it). The work is polled first, so one that is ready wins even at the deadline.
pub fn within<F: Future>(
    period: Duration,
    work: F,
) -> impl Future<Output = Result<Option<F::Output>, RtError>> {
    hyper_rt::futures::within(nanos(period), work)
}

/// What `work` yields by `deadline`, or `None` once it has passed.
pub fn within_until<F: Future>(
    deadline: Instant,
    work: F,
) -> impl Future<Output = Result<Option<F::Output>, RtError>> {
    within(deadline.remaining(), work)
}

/// Why a bounded wait gave no output: its period passed, or no wait could be kept (off a shard).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TimedOut {
    #[error("the wait's period passed")]
    Elapsed,
    #[error("no wait could be kept: {0}")]
    Refused(RtError),
}

/// What `work` yields within `period`, or why it did not: [`within`] in the shape of a result.
pub fn timeout<F: Future>(
    period: Duration,
    work: F,
) -> impl Future<Output = Result<F::Output, TimedOut>> {
    // Made before the block, which only awaits it in place.
    let waiting = within(period, work);
    async move {
        match waiting.await {
            Ok(Some(output)) => Ok(output),
            Ok(None) => Err(TimedOut::Elapsed),
            Err(refused) => Err(TimedOut::Refused(refused)),
        }
    }
}

/// What `work` yields by `deadline`, or why it did not.
pub fn timeout_at<F: Future>(
    deadline: Instant,
    work: F,
) -> impl Future<Output = Result<F::Output, TimedOut>> {
    timeout(deadline.remaining(), work)
}
