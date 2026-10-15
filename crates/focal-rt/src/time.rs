//! Time on a hyper-rt shard: the shard's monotonic clock (a simulated shard's virtual one), and the
//! waits kept by its timer wheel. Off a shard there is no clock to read and no timer to keep: a wait
//! there is refused `NotOnShardThread`, typed, as the wheel refuses it.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
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

// Each wait below is the runtime's own future, or a named one around it (`Timeout`), made here and
// handed back: no wait adds an `async` layer around the work. An `async` layer holds the work twice,
// the wait beside the local its `.await` moves it into (rust-lang/rust#62958), and a debug build
// keeps every layer's frame on the stack at once.

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
pub fn within<F: Future>(period: Duration, work: F) -> hyper_rt::futures::Within<F> {
    hyper_rt::futures::within(nanos(period), work)
}

/// What `work` yields by `deadline`, or `None` once it has passed.
pub fn within_until<F: Future>(deadline: Instant, work: F) -> hyper_rt::futures::Within<F> {
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
pub fn timeout<F: Future>(period: Duration, work: F) -> Timeout<F> {
    Timeout {
        waiting: hyper_rt::futures::within(nanos(period), work),
    }
}

pin_project_lite::pin_project! {
    /// The wait of [`timeout`]: [`within`]'s, its output put as a result. A named future, so the work
    /// is held once: an `async` block awaiting the wait held it twice, the wait beside the local its
    /// `.await` moved it into (a timed enrollment call measured 2.0 times its work's size).
    #[must_use = "a timed wait does nothing unless awaited"]
    pub struct Timeout<F> {
        #[pin]
        waiting: hyper_rt::futures::Within<F>,
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, TimedOut>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().waiting.poll(cx).map(|waited| match waited {
            Ok(Some(output)) => Ok(output),
            Ok(None) => Err(TimedOut::Elapsed),
            Err(refused) => Err(TimedOut::Refused(refused)),
        })
    }
}

impl<F> std::fmt::Debug for Timeout<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Timeout")
            .field("waiting", &self.waiting)
            .finish()
    }
}

/// What `work` yields by `deadline`, or why it did not.
pub fn timeout_at<F: Future>(deadline: Instant, work: F) -> Timeout<F> {
    timeout(deadline.remaining(), work)
}
