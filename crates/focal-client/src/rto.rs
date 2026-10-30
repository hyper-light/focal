//! How long a client waits on a route's answer before it sends the request by the next route as
//! well: the retransmission timeout of RFC 6298 over the answers its requests are given. A route
//! that gave no answer was waited on until its connection's idle timeout (10 s) said it was gone,
//! so a session whose leader was killed wrote again 10 s after the kill though its new leader was
//! ready within 2.1 s (fleet33-kill12, 2026-10-29). Each answer is a sample of the round trip its
//! request took, the timeout is the smoothed time and four of its deviations, and each timeout
//! that expires doubles it until the next answer, as a sender backs off its retransmissions (RFC
//! 6298 §2, §5.5). A request sent again so (a hedged request, Dean and Barroso, "The Tail at
//! Scale", CACM 56(2), 2013) is the same request, its identity, epochs and bytes, so its ledger
//! answers it once whichever node it reaches, and the first route goes on being waited on.
use std::time::Duration;

/// RFC 6298 §2.1: the timeout before any round trip is measured.
const INITIAL: Duration = Duration::from_secs(1);
/// RFC 6298 §2.4: a timeout under a second is rounded up to one, the margin it keeps against
/// timeouts that are not losses.
const FLOOR: Duration = Duration::from_secs(1);
/// RFC 6298 §2.5: a timeout may be capped at no less than sixty seconds.
const CEILING: Duration = Duration::from_secs(60);

/// The smoothed round trip and its deviation (RFC 6298 §2.2, §2.3), and how many timeouts have
/// expired since the last answer.
#[derive(Debug, Default)]
pub(crate) struct Rto {
    smoothed: Option<(Duration, Duration)>,
    expired: u32,
}

impl Rto {
    /// A round trip an answer took. The first sets the smoothed time to it and the deviation to
    /// half of it; each later one moves the deviation a quarter of the way to its distance from
    /// the smoothed time, then the smoothed time an eighth of the way to it (RFC 6298 §2.3: alpha
    /// 1/8, beta 1/4). An answer ends the backoff.
    pub(crate) fn sample(&mut self, round_trip: Duration) {
        let part = |time: Duration, of: u32| time.checked_div(of).unwrap_or(Duration::ZERO);
        self.smoothed = Some(match self.smoothed {
            None => (round_trip, part(round_trip, 2)),
            Some((smoothed, deviation)) => {
                let distance = smoothed.abs_diff(round_trip);
                let deviation =
                    part(deviation.saturating_mul(3), 4).saturating_add(part(distance, 4));
                let smoothed =
                    part(smoothed.saturating_mul(7), 8).saturating_add(part(round_trip, 8));
                (smoothed, deviation)
            }
        });
        self.expired = 0;
    }
    /// A timeout expired with no answer: the next is twice as long (RFC 6298 §5.5).
    pub(crate) fn expired(&mut self) {
        self.expired = self.expired.saturating_add(1);
    }
    /// How long a request is waited on before it is sent by the next route as well: the smoothed
    /// time and four deviations (RFC 6298 §2.3, K = 4), a second at least, doubled for each
    /// timeout expired since the last answer, sixty seconds at most.
    pub(crate) fn timeout(&self) -> Duration {
        let base = match self.smoothed {
            None => INITIAL,
            Some((smoothed, deviation)) => smoothed.saturating_add(deviation.saturating_mul(4)),
        }
        .max(FLOOR);
        let factor = 1u32.checked_shl(self.expired).unwrap_or(u32::MAX);
        base.saturating_mul(factor).min(CEILING)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The timeout follows RFC 6298: a second before any answer and at least a second after,
    /// the smoothed time and four deviations above that, doubled by each expiry until an answer,
    /// and sixty seconds at most.
    #[test]
    fn the_timeout_is_rfc_6298s() {
        let mut rto = Rto::default();
        assert_eq!(rto.timeout(), Duration::from_secs(1));
        // A fast path: 40 ms, deviation 20 ms, 120 ms in all, rounded up to the floor.
        rto.sample(Duration::from_millis(40));
        assert_eq!(rto.timeout(), Duration::from_secs(1));
        // A slow one: 800 ms. Deviation 3/4 * 20 + 760 / 4 = 205 ms; smoothed 7/8 * 40 + 100 = 135
        // ms; 135 + 4 * 205 = 955 ms, still the floor.
        rto.sample(Duration::from_millis(800));
        assert_eq!(rto.timeout(), Duration::from_secs(1));
        // Slower again: 2 s. Deviation 3/4 * 205 + 1,865 / 4 = 620 ms (153.75 + 466.25);
        // smoothed 7/8 * 135 + 250 = 368.125 ms; 368.125 + 2,480 = 2,848.125 ms.
        rto.sample(Duration::from_secs(2));
        assert_eq!(rto.timeout(), Duration::from_micros(2_848_125));
        // Each expiry doubles it, to the ceiling; an answer ends the backoff.
        rto.expired();
        assert_eq!(rto.timeout(), Duration::from_micros(5_696_250));
        for _ in 0..40 {
            rto.expired();
        }
        assert_eq!(rto.timeout(), Duration::from_secs(60));
        rto.sample(Duration::from_secs(2));
        assert!(rto.timeout() < Duration::from_secs(60));
    }
}
