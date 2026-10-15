//! What an exchange with a peer is expected to take, and pauses spread so that callers that failed
//! together do not return together (mantle note 32 T40, T41; focal `peers.rs`).

use std::time::Duration;

use hyper_timing::ExchangeRtt;

/// The largest doubling a shift can apply to a `u64` tail. focal capped the doublings at six; note
/// 32 excludes that cap (T40): the caller's budget bounds the wait, so the estimate only stops
/// doubling where the arithmetic would.
const MAX_DOUBLINGS: u32 = u64::BITS - 1;

/// What exchanges with one peer take, its work included.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PeerTiming {
    taken: ExchangeRtt,
    /// Exchanges given up on since the last one answered. Each doubles what the peer is expected
    /// to take (RFC 9002 §6.2's backoff): an estimate that made a round give up too early is not
    /// fed by the exchange it gave up on, and would otherwise never grow.
    abandoned: u32,
}

impl PeerTiming {
    /// Karn's rule is the caller's: only an exchange the peer answered, timed on a connection
    /// already open, is a sample.
    pub(crate) fn answered(&mut self, taken: Duration) {
        self.taken
            .on_sample(u64::try_from(taken.as_nanos()).unwrap_or(u64::MAX));
        self.abandoned = 0;
    }
    /// An exchange was given up on.
    pub(crate) fn abandoned(&mut self) {
        self.abandoned = self.abandoned.saturating_add(1).min(MAX_DOUBLINGS);
    }
    /// The tail of the exchanges the peer answered, doubled for each given up on since; `None`
    /// while it has answered none. `granularity` is the caller's measured timer granularity
    /// (`hyper_timing::Lateness`), RFC 9002's `kGranularity` under the variation term.
    pub(crate) fn tail(&self, granularity: Duration) -> Option<Duration> {
        let granularity_ns = u64::try_from(granularity.as_nanos()).unwrap_or(u64::MAX);
        let tail = self.taken.tail_ns(granularity_ns)?;
        let factor = 1u64.checked_shl(self.abandoned)?;
        Some(Duration::from_nanos(tail.saturating_mul(factor)))
    }
}

/// One past the largest draw: what a draw is measured against.
const DRAWS: u128 = 1 << 64;

/// A pause spread by equal jitter (focal's F64): drawn uniformly between half of `delay` and the
/// whole of it from `random`, sixty-four random bits. Half the pause is kept whole because the
/// pause has a meaning of its own (an unreachable peer is not dialed again before its cooldown);
/// the other half is the spread, so peers that lost one node at once do not retry it in step.
pub fn spread(delay: Duration, random: u64) -> Duration {
    let half = delay.checked_div(2).unwrap_or(Duration::ZERO);
    let drawn = half
        .as_nanos()
        .saturating_mul(u128::from(random))
        .checked_div(DRAWS)
        .unwrap_or(0);
    half.saturating_add(Duration::from_nanos(
        u64::try_from(drawn).unwrap_or(u64::MAX),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ported from focal `tests.rs`, `a_peer_pause_is_spread_over_its_second_half`.
    #[test]
    fn a_peer_pause_is_spread_over_its_second_half() {
        let pause = Duration::from_millis(600);
        assert_eq!(spread(pause, 0), Duration::from_millis(300));
        let whole = spread(pause, u64::MAX);
        assert!(
            whole <= pause && whole >= pause - Duration::from_nanos(1),
            "{whole:?}"
        );
        let middle = spread(pause, u64::MAX / 2);
        assert!(
            middle >= Duration::from_millis(450) - Duration::from_nanos(1)
                && middle <= Duration::from_millis(450),
            "{middle:?}"
        );
        assert_eq!(spread(Duration::ZERO, u64::MAX), Duration::ZERO);
    }

    /// focal `peers.rs`'s `exchange_tail`: each exchange given up on doubles the expectation, and
    /// an answer resets it; without focal's cap of six.
    #[test]
    fn an_abandoned_exchange_doubles_what_the_peer_is_expected_to_take() {
        let g = Duration::from_micros(45);
        let mut timing = PeerTiming::default();
        assert_eq!(timing.tail(g), None);
        timing.answered(Duration::from_millis(10));
        let tail = timing.tail(g).unwrap();
        timing.abandoned();
        assert_eq!(timing.tail(g), Some(tail * 2));
        for _ in 0..7 {
            timing.abandoned();
        }
        assert_eq!(timing.tail(g), Some(tail * 256));
        for _ in 0..100 {
            timing.abandoned();
        }
        assert_eq!(timing.tail(g), Some(Duration::from_nanos(u64::MAX)));
        timing.answered(Duration::from_millis(10));
        assert!(timing.tail(g).unwrap() < tail * 2);
    }
}
