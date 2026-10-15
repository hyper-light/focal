//! Pacing of packet transmissions.

use crate::{Duration, Instant};

use tracing::warn;

/// A simple token-bucket pacer
///
/// The pacer's capacity is derived on a fraction of the congestion window
/// which can be sent in regular intervals
/// Once the bucket is empty, further transmission is blocked.
/// The bucket refills at a rate slightly faster
/// than one congestion window per RTT, as recommended in
/// <https://tools.ietf.org/html/draft-ietf-quic-recovery-34#section-7.7>
pub(super) struct Pacer {
    capacity: u64,
    last_window: u64,
    last_mtu: u16,
    /// The rate the capacity was last derived from, when the controller stated one.
    last_rate: Option<u64>,
    tokens: u64,
    prev: Instant,
}

impl Pacer {
    /// Obtains a new [`Pacer`].
    pub(super) fn new(smoothed_rtt: Duration, window: u64, mtu: u16, now: Instant) -> Self {
        let capacity = optimal_capacity(smoothed_rtt, window, mtu);
        Self {
            capacity,
            last_window: window,
            last_mtu: mtu,
            last_rate: None,
            tokens: capacity,
            prev: now,
        }
    }

    /// Record that a packet has been transmitted.
    pub(super) fn on_transmit(&mut self, packet_length: u16) {
        self.tokens = self.tokens.saturating_sub(packet_length.into())
    }

    /// Return how long we need to wait before sending `bytes_to_send`
    ///
    /// If we can send a packet right away, this returns `None`. Otherwise, returns `Some(d)`,
    /// where `d` is the time before this function should be called again.
    ///
    /// The 5/4 ratio used here comes from the suggestion that N = 1.25 in the draft IETF RFC for
    /// QUIC. A `rate` the controller states, bytes a second, replaces that rule ([`Self::delay_at`]).
    pub(super) fn delay(
        &mut self,
        smoothed_rtt: Duration,
        rate: Option<u64>,
        bytes_to_send: u64,
        mtu: u16,
        window: u64,
        now: Instant,
    ) -> Option<Instant> {
        if let Some(rate) = rate {
            return self.delay_at(rate, bytes_to_send, mtu, now);
        }
        // A congestion window is never zero (upstream asserted that in debug builds); a zero one
        // paces nothing, below.
        if window != self.last_window || mtu != self.last_mtu || self.last_rate.is_some() {
            self.capacity = optimal_capacity(smoothed_rtt, window, mtu);

            // Clamp the tokens
            self.tokens = self.capacity.min(self.tokens);
            self.last_window = window;
            self.last_mtu = mtu;
            self.last_rate = None;
        }

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        // we disable pacing for extremely large windows, and for a zero one
        let Ok(window) = u32::try_from(window) else {
            return None;
        };
        if window == 0 {
            return None;
        }

        let time_elapsed = now.checked_duration_since(self.prev).unwrap_or_else(|| {
            warn!("received a timestamp early than a previous recorded time, ignoring");
            Default::default()
        });

        if smoothed_rtt.as_nanos() == 0 {
            return None;
        }

        let elapsed_rtts = time_elapsed.as_secs_f64() / smoothed_rtt.as_secs_f64();
        let new_tokens = f64::from(window) * 1.25 * elapsed_rtts;
        self.tokens = self
            .tokens
            .saturating_add(crate::float::saturating_u64(new_tokens))
            .min(self.capacity);

        self.prev = now;

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        // `tokens` is below `bytes_to_send` here; a deficit past u32::MAX saturates, as does the
        // product.
        let deficit = bytes_to_send.max(self.capacity).saturating_sub(self.tokens);
        let unscaled_delay = smoothed_rtt
            .checked_mul(u32::try_from(deficit).unwrap_or(u32::MAX))
            .unwrap_or(Duration::MAX)
            .checked_div(window)
            .unwrap_or(Duration::MAX);

        // divisions come before multiplications to prevent overflow
        // this is the time at which the pacing window becomes empty; a time past what `Instant`
        // represents does not pace (upstream's addition overflowed)
        let delay = unscaled_delay
            .checked_div(5)
            .and_then(|fifth| fifth.checked_mul(4))
            .unwrap_or(Duration::MAX);
        self.prev.checked_add(delay)
    }

    /// `delay` at a rate the controller states, bytes a second: the bucket holds what the rate
    /// sends in a burst interval, clamped as the window's is, and refills at the rate; a datagram
    /// it cannot cover waits until the bucket holds a burst again. A zero rate paces nothing, as a
    /// zero window does.
    fn delay_at(
        &mut self,
        rate: u64,
        bytes_to_send: u64,
        mtu: u16,
        now: Instant,
    ) -> Option<Instant> {
        if self.last_rate != Some(rate) || mtu != self.last_mtu {
            self.capacity = rate_capacity(rate, mtu);
            self.tokens = self.capacity.min(self.tokens);
            self.last_rate = Some(rate);
            self.last_mtu = mtu;
        }
        if self.tokens >= bytes_to_send || rate == 0 {
            return None;
        }
        let elapsed = now.checked_duration_since(self.prev).unwrap_or_else(|| {
            warn!("received a timestamp early than a previous recorded time, ignoring");
            Default::default()
        });
        // A u64 rate times a u64 nanosecond count fits a u128; the quotient saturates into a u64.
        let new_tokens = u128::from(rate)
            .saturating_mul(elapsed.as_nanos())
            .checked_div(NANOS_PER_SECOND)
            .map_or(u64::MAX, |tokens| u64::try_from(tokens).unwrap_or(u64::MAX));
        self.tokens = self.tokens.saturating_add(new_tokens).min(self.capacity);
        self.prev = now;
        if self.tokens >= bytes_to_send {
            return None;
        }
        let deficit = bytes_to_send.max(self.capacity).saturating_sub(self.tokens);
        let wait = u128::from(deficit)
            .saturating_mul(NANOS_PER_SECOND)
            .checked_div(u128::from(rate))
            .map_or(u64::MAX, |nanos| u64::try_from(nanos).unwrap_or(u64::MAX));
        // A time past what `Instant` represents does not pace, as the window's rule.
        self.prev.checked_add(Duration::from_nanos(wait))
    }
}

/// The bucket for a stated `rate`: what it sends in a burst interval, clamped as the window's is.
fn rate_capacity(rate: u64, mtu: u16) -> u64 {
    let capacity = u128::from(rate)
        .saturating_mul(BURST_INTERVAL_NANOS)
        .checked_div(NANOS_PER_SECOND)
        .map_or(u64::MAX, |capacity| {
            u64::try_from(capacity).unwrap_or(u64::MAX)
        });
    capacity.clamp(
        MIN_BURST_SIZE.saturating_mul(u64::from(mtu)),
        MAX_BURST_SIZE.saturating_mul(u64::from(mtu)),
    )
}

/// Nanoseconds in a second: the unit rates are stated per.
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Calculates a pacer capacity for a certain window and RTT
///
/// The goal is to emit a burst (of size `capacity`) in timer intervals
/// which compromise between
/// - ideally distributing datagrams over time
/// - constantly waking up the connection to produce additional datagrams
///
/// Too short burst intervals means we will never meet them since the timer
/// accuracy in user-space is not high enough. If we miss the interval by more
/// than 25%, we will lose that part of the congestion window since no additional
/// tokens for the extra-elapsed time can be stored.
///
/// Too long burst intervals make pacing less effective.
fn optimal_capacity(smoothed_rtt: Duration, window: u64, mtu: u16) -> u64 {
    let rtt = smoothed_rtt.as_nanos().max(1);

    // A u64 window times a nanosecond interval fits a u128; the quotient saturates into a u64.
    let capacity = u128::from(window)
        .saturating_mul(BURST_INTERVAL_NANOS)
        .checked_div(rtt)
        .map_or(u64::MAX, |capacity| {
            u64::try_from(capacity).unwrap_or(u64::MAX)
        });

    // Small bursts are less efficient (no GSO), could increase latency and don't effectively
    // use the channel's buffer capacity. Large bursts might block the connection on sending.
    // A u16 MTU times a small burst count does not saturate.
    capacity.clamp(
        MIN_BURST_SIZE.saturating_mul(u64::from(mtu)),
        MAX_BURST_SIZE.saturating_mul(u64::from(mtu)),
    )
}

/// The burst interval
///
/// The capacity will we refilled in 4/5 of that time.
/// 2ms is chosen here since framework timers might have 1ms precision.
/// If kernel-level pacing is supported later a higher time here might be
/// more applicable.
const BURST_INTERVAL_NANOS: u128 = 2_000_000; // 2ms

/// Allows some usage of GSO, and doesn't slow down the handshake.
const MIN_BURST_SIZE: u64 = 10;

/// Creating 256 packets took 1ms in a benchmark, so larger bursts don't make sense.
const MAX_BURST_SIZE: u64 = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_panic_on_bad_instant() {
        let old_instant = Instant::now();
        let new_instant = old_instant + Duration::from_micros(15);
        let rtt = Duration::from_micros(400);

        assert!(
            Pacer::new(rtt, 30000, 1500, new_instant)
                .delay(Duration::from_micros(0), None, 0, 1500, 1, old_instant)
                .is_none()
        );
        assert!(
            Pacer::new(rtt, 30000, 1500, new_instant)
                .delay(Duration::from_micros(0), None, 1600, 1500, 1, old_instant)
                .is_none()
        );
        assert!(
            Pacer::new(rtt, 30000, 1500, new_instant)
                .delay(
                    Duration::from_micros(0),
                    None,
                    1500,
                    1500,
                    3000,
                    old_instant
                )
                .is_none()
        );
    }

    #[test]
    fn a_stated_rate_paces_at_that_rate() {
        let rtt = Duration::from_millis(50);
        let mtu = 1_000;
        let start = Instant::now();
        let mut pacer = Pacer::new(rtt, 2_000_000, mtu, start);
        // A megabyte a second sends 2,000 bytes in a burst interval: below ten datagrams, so the
        // bucket holds ten.
        let rate = Some(1_000_000);
        assert_eq!(pacer.delay(rtt, rate, 1_000, mtu, 2_000_000, start), None);
        assert_eq!(pacer.capacity, 10_000);
        for _ in 0..10 {
            assert_eq!(pacer.delay(rtt, rate, 1_000, mtu, 2_000_000, start), None);
            pacer.on_transmit(mtu);
        }
        // Empty: it waits until the rate refills a burst, 10,000 bytes at a megabyte a second.
        assert_eq!(
            pacer.delay(rtt, rate, 1_000, mtu, 2_000_000, start),
            Some(start + Duration::from_millis(10))
        );
        // Half that later it holds half a burst, and sends.
        assert_eq!(
            pacer.delay(
                rtt,
                rate,
                1_000,
                mtu,
                2_000_000,
                start + Duration::from_millis(5)
            ),
            None
        );
        assert_eq!(pacer.tokens, 5_000);
        // The window's rule again: the capacity is the window's.
        pacer.delay(
            rtt,
            None,
            1_000,
            mtu,
            2_000_000,
            start + Duration::from_millis(5),
        );
        assert_eq!(
            pacer.capacity,
            (2_000_000u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        // A zero rate paces nothing.
        let mut idle = Pacer::new(rtt, 2_000_000, mtu, start);
        idle.tokens = 0;
        assert_eq!(idle.delay(rtt, Some(0), 1_000, mtu, 2_000_000, start), None);
    }

    #[test]
    fn derives_initial_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, now);
        assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(rtt, 1, mtu, now);
        assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn adjusts_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);
        let initial_tokens = pacer.tokens;

        pacer.delay(rtt, None, mtu as u64, mtu, window * 2, now);
        assert_eq!(
            pacer.capacity,
            (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens);

        pacer.delay(rtt, None, mtu as u64, mtu, window / 2, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens / 2);

        pacer.delay(rtt, None, mtu as u64, mtu * 2, window, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );

        pacer.delay(rtt, None, mtu as u64, 20_000, window, now);
        assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
    }

    #[test]
    fn computes_pause_correctly() {
        let window = 2_000_000u64;
        let mtu = 1000;
        let rtt = Duration::from_millis(50);
        let old_instant = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, old_instant);
        let packet_capacity = pacer.capacity / mtu as u64;

        for _ in 0..packet_capacity {
            assert_eq!(
                pacer.delay(rtt, None, mtu as u64, mtu, window, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        let pace_duration = Duration::from_nanos((BURST_INTERVAL_NANOS * 4 / 5) as u64);

        assert_eq!(
            pacer
                .delay(rtt, None, mtu as u64, mtu, window, old_instant)
                .expect("Send must be delayed")
                .duration_since(old_instant),
            pace_duration
        );

        // Refill half of the tokens
        assert_eq!(
            pacer.delay(
                rtt,
                None,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity / 2);

        for _ in 0..packet_capacity / 2 {
            assert_eq!(
                pacer.delay(rtt, None, mtu as u64, mtu, window, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        // Refill all capacity by waiting more than the expected duration
        assert_eq!(
            pacer.delay(
                rtt,
                None,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration * 3 / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity);
    }
}
