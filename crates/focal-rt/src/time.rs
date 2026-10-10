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

/// Waits `period` on the shard's wheel.
pub async fn sleep(period: Duration) -> Result<(), RtError> {
    hyper_rt::futures::sleep(nanos(period)).await
}

/// Waits until `deadline` on the shard's wheel; at once for one that has passed.
pub async fn sleep_until(deadline: Instant) -> Result<(), RtError> {
    sleep(deadline.remaining()).await
}

/// What `work` yields within `period` on the shard's clock, or `None` once the period is over (the
/// work dropped with it). The work is polled first, so one that is ready wins even at the deadline.
pub async fn within<F: Future>(period: Duration, work: F) -> Result<Option<F::Output>, RtError> {
    hyper_rt::futures::within(nanos(period), work).await
}

/// What `work` yields by `deadline`, or `None` once it has passed.
pub async fn within_until<F: Future>(
    deadline: Instant,
    work: F,
) -> Result<Option<F::Output>, RtError> {
    within(deadline.remaining(), work).await
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
pub async fn timeout<F: Future>(period: Duration, work: F) -> Result<F::Output, TimedOut> {
    match within(period, work).await {
        Ok(Some(output)) => Ok(output),
        Ok(None) => Err(TimedOut::Elapsed),
        Err(refused) => Err(TimedOut::Refused(refused)),
    }
}

/// What `work` yields by `deadline`, or why it did not.
pub async fn timeout_at<F: Future>(deadline: Instant, work: F) -> Result<F::Output, TimedOut> {
    timeout(deadline.remaining(), work).await
}

/// Waits `period`: on the shard's wheel, or — off any shard, where a plain thread's own loop drives
/// the future — by blocking that thread, the only wait it has. A loop that pauses between its tries
/// never spins on a refused wait.
pub async fn pause(period: Duration) {
    if sleep(period).await.is_err() {
        std::thread::sleep(period);
    }
}

/// Waits until `deadline`, as [`pause`] does.
pub async fn pause_until(deadline: Instant) {
    pause(deadline.remaining()).await;
}
