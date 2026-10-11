//! The client's clock and waits: the hyper-rt shard's its calls run on (a simulated shard's in a
//! simulation). Off a shard there is no timer to keep a wait with, so a call is refused there,
//! typed, before anything is sent.
use crate::ClientError;
use std::{future::Future, time::Duration};

/// `period` in the shard clock's nanoseconds.
pub(crate) fn nanos(period: Duration) -> u64 {
    u64::try_from(period.as_nanos()).unwrap_or(u64::MAX)
}

/// Whether this thread runs a shard, whose timers a wait is kept with.
pub(crate) fn on_a_shard() -> bool {
    hyper_rt::futures::shard_id().is_some()
}

/// An instant on the shard's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Instant(u64);
impl Instant {
    pub(crate) fn now() -> Self {
        Self(hyper_rt::futures::now_ns())
    }
    /// How long ago this instant was.
    pub(crate) fn elapsed(self) -> Duration {
        Duration::from_nanos(hyper_rt::futures::now_ns().saturating_sub(self.0))
    }
    /// The instant `period` after this one, if the clock can name it.
    pub(crate) fn checked_add(self, period: Duration) -> Option<Self> {
        u64::try_from(period.as_nanos())
            .ok()
            .and_then(|ns| self.0.checked_add(ns))
            .map(Self)
    }
    /// How long until this instant: zero once it has passed.
    pub(crate) fn remaining(self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(hyper_rt::futures::now_ns()))
    }
}

/// What `future` yields within `period`, or `None` once the period is over. Off a shard no
/// deadline can be kept: refused `Transport` before the work begins. Named waits around the
/// work, which they hold once (`focal_rt::adapt`).
pub(crate) fn within<F: Future>(
    period: Duration,
    future: F,
) -> impl Future<Output = Result<Option<F::Output>, ClientError>> {
    focal_rt::on_shard(
        focal_rt::map(hyper_rt::futures::within(nanos(period), future), |waited| {
            waited.map_err(|_| ClientError::Transport)
        }),
        || Err(ClientError::Transport),
    )
}

/// What `future` answers when polled once off any shard: a client's call is refused there at once,
/// typed, rather than waiting on a wake no timer could give.
#[cfg(test)]
pub(crate) fn off_a_shard<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("a call polled off a shard waited"),
    }
}

/// Waits `period` on the shard's clock; refused `Transport` off a shard.
pub(crate) async fn sleep(period: Duration) -> Result<(), ClientError> {
    hyper_rt::futures::sleep(nanos(period))
        .await
        .map_err(|_| ClientError::Transport)
}
