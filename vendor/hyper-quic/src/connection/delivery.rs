//! What a path delivers, and how fast: delivery-rate samples
//! (draft-cheng-iccrg-delivery-rate-estimation-02 §3), as Linux measures them for every TCP
//! connection (`net/ipv4/tcp_rate.c`) and Chromium's QUIC does for BBR (`BandwidthSampler`)
//!
//! A packet carries, from its send, what the path had delivered by then and when, and the send
//! time of the packet last delivered then ([`Snapshot`]). An acknowledgement measures, from its
//! newest packet, the bytes delivered since that packet's snapshot over the longer of two phases:
//! the time the path took to send them and the time it took to acknowledge them. Acknowledgements
//! that come together (one ACK frame for many packets, or ACKs held and released at once) shorten
//! the ack phase and not the send phase; packets sent together (a burst of the pacer's or of the
//! application's) shorten the send phase and not the ack phase. A flight is delivered no faster
//! than it crosses the path, so an interval shorter than the path's minimum round trip is no
//! sample (§3.3).
//!
//! The estimator it replaces, upstream's port of BBR, divided the bytes of each acknowledged
//! packet by the time since the previous acknowledgement, and the latest packet sent by the time
//! since the one before, whichever packets those were. On a path of 12.5 MB/s and 143 µs it
//! estimated 357 MB/s, and BBR's window grew to 65 times the path's bandwidth-delay product
//! (`VENDORED.md` §30).

use crate::congestion::RateSample;
use crate::{Duration, Instant};

/// What the path had delivered when a packet was sent (§3.2's per-packet state), sixteen bytes in
/// the packet's record: its times as nanoseconds before the packet's send. Whether the sender was
/// in a phase the application limited goes beside it (`SentPacket::app_limited`), in a byte the
/// record's other flags leave free.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot {
    /// The bytes the path had delivered
    delivered: u64,
    /// How long before the send the path's last delivery was acknowledged, or its current flight
    /// began: [`TOO_LONG`] for as long or longer
    delivered_ago: u32,
    /// How long before the send the packet last delivered was sent, or the packet that began the
    /// current flight: [`TOO_LONG`] for as long or longer
    first_sent_ago: u32,
}

/// An offset of 4.29 s or more: a packet whose flight began, or whose path last delivered, that
/// long before it went measures a stall, and gives no rate
const TOO_LONG: u32 = u32::MAX;

/// `earlier`'s distance before `later` in nanoseconds, [`TOO_LONG`] for as long or longer
fn ago(later: Instant, earlier: Instant) -> u32 {
    u32::try_from(later.saturating_duration_since(earlier).as_nanos()).unwrap_or(TOO_LONG)
}

/// The newest packet an acknowledgement delivered: it gives the acknowledgement's sample
#[derive(Debug, Clone, Copy)]
struct Newest {
    sent: Instant,
    snapshot: Snapshot,
    app_limited: bool,
}

/// What a path has delivered (§3.1's per-connection state, per path here as its congestion
/// controller is)
#[derive(Debug, Default)]
pub(crate) struct Delivery {
    /// The bytes acknowledged: every in-flight packet of the path's, once
    delivered: u64,
    /// When `delivered` last grew, or when the current flight began
    delivered_time: Option<Instant>,
    /// The send time of the newest packet delivered, or of the packet that began the flight
    first_sent_time: Option<Instant>,
    /// What `delivered` must pass to end the phase the application limited, zero outside one
    /// (§3.4's `C.app_limited`): the packets in flight when the sender found nothing to send, and
    /// any it sent until they are delivered, measure the application, not the path
    app_limited_until: u64,
    /// The newest packet the acknowledgement being processed has delivered so far
    newest: Option<Newest>,
}

impl Delivery {
    /// A packet goes at `now`, `in_flight` bytes of the path's ahead of it: what it carries, and
    /// whether it goes in a phase the application limited
    pub(crate) fn on_sent(&mut self, now: Instant, in_flight: u64) -> (Snapshot, bool) {
        // A packet sent with nothing in flight begins a flight: its phases are timed from it, not
        // from a delivery before an idle gap (§3.2)
        let (delivered_time, first_sent_time) = match (self.delivered_time, self.first_sent_time) {
            (Some(delivered_time), Some(first_sent_time)) if in_flight > 0 => {
                (delivered_time, first_sent_time)
            }
            _ => {
                self.delivered_time = Some(now);
                self.first_sent_time = Some(now);
                (now, now)
            }
        };
        let snapshot = Snapshot {
            delivered: self.delivered,
            delivered_ago: ago(now, delivered_time),
            first_sent_ago: ago(now, first_sent_time),
        };
        (snapshot, self.app_limited_until != 0)
    }

    /// The sender found nothing to send with its window open, `in_flight` bytes in flight: a
    /// phase the application limits begins, and ends once they are delivered (§3.4)
    pub(crate) fn on_app_limited(&mut self, in_flight: u64) {
        self.app_limited_until = self.delivered.saturating_add(in_flight).max(1);
    }

    /// A packet of `bytes`, sent at `sent` with `snapshot` (in a phase the application limited if
    /// `app_limited`), is acknowledged at `now` (§3.3's `UpdateRateSample`)
    pub(crate) fn on_acked(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        snapshot: Snapshot,
        app_limited: bool,
    ) {
        self.delivered = self.delivered.saturating_add(bytes);
        self.delivered_time = Some(now);
        // The newest packet, the one sent last, measures the most recent flight; of two sent at
        // once the later acknowledged (a larger packet number) is taken
        if self.newest.is_none_or(|newest| sent >= newest.sent) {
            self.newest = Some(Newest {
                sent,
                snapshot,
                app_limited,
            });
            self.first_sent_time = Some(sent);
        }
    }

    /// The acknowledgement at `now` is processed, `min_rtt` the path's least round trip: its
    /// sample, if it delivered an in-flight packet (§3.3's `GenerateRateSample`)
    pub(crate) fn sample(&mut self, now: Instant, min_rtt: Duration) -> Option<RateSample> {
        if self.app_limited_until != 0 && self.delivered > self.app_limited_until {
            self.app_limited_until = 0;
        }
        let Newest {
            sent,
            snapshot,
            app_limited,
        } = self.newest.take()?;
        let rtt = now.saturating_duration_since(sent);
        // The send phase is the newest packet's distance from its flight's first; the ack phase
        // runs from the delivery before its send to now
        let send_elapsed = Duration::from_nanos(u64::from(snapshot.first_sent_ago));
        let ack_elapsed =
            rtt.saturating_add(Duration::from_nanos(u64::from(snapshot.delivered_ago)));
        let interval = send_elapsed.max(ack_elapsed);
        let delivered = self.delivered.saturating_sub(snapshot.delivered);
        let stalled = snapshot.delivered_ago == TOO_LONG || snapshot.first_sent_ago == TOO_LONG;
        let rate = if stalled || interval < min_rtt {
            None
        } else {
            rate(delivered, interval)
        };
        Some(RateSample {
            delivered,
            interval,
            rate,
            app_limited,
            rtt,
        })
    }
}

/// `bytes` over `interval` in bytes a second, computed in u128 and saturating into a u64; none
/// over an empty interval
fn rate(bytes: u64, interval: Duration) -> Option<u64> {
    /// Nanoseconds in a second
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let rate = u128::from(bytes)
        .saturating_mul(NANOS_PER_SECOND)
        .checked_div(interval.as_nanos())?;
    Some(u64::try_from(rate).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MICRO: Duration = Duration::from_micros(1);
    /// A datagram of 1,200 bytes takes 96 µs at 12.5 MB/s (100 Mbit/s)
    const SIZE: u64 = 1_200;
    const SERIALIZE: Duration = Duration::from_micros(96);
    const RATE: u64 = 12_500_000;

    /// A flight of ten packets sent at once on an idle path, delivered by a bottleneck of 12.5
    /// MB/s and acknowledged a packet at a time 100 µs of propagation later: every sample is the
    /// bottleneck's rate, or under it, never the burst's, which is infinite
    #[test]
    fn a_burst_through_a_bottleneck_measures_the_bottleneck() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        let snapshots: Vec<_> = (0..10u64)
            .map(|i| (start, delivery.on_sent(start, i * SIZE).0))
            .collect();
        let propagation = 100 * MICRO;
        let mut rates = Vec::new();
        for (i, (sent, snapshot)) in snapshots.into_iter().enumerate() {
            let at = start + propagation + SERIALIZE * (i as u32 + 1);
            delivery.on_acked(at, sent, SIZE, snapshot, false);
            let sample = delivery.sample(at, propagation + SERIALIZE).unwrap();
            assert_eq!(sample.delivered, SIZE * (i as u64 + 1));
            assert!(!sample.app_limited);
            rates.push(sample.rate.unwrap());
        }
        // The first packet's interval is its own round trip; the last's, ten serializations and
        // the propagation: the rate climbs toward the bottleneck's and never passes it
        assert!(rates.windows(2).all(|pair| pair[0] <= pair[1]), "{rates:?}");
        assert!(*rates.last().unwrap() <= RATE, "{rates:?}");
        assert!(*rates.last().unwrap() > RATE * 9 / 10, "{rates:?}");
    }

    /// Ten packets paced 96 µs apart, their acknowledgements held by the peer and released
    /// together, one ACK frame for all ten: the ack phase is short, the send phase is not, and
    /// the sample is the send rate, not ten packets over the gap between two acknowledgements
    #[test]
    fn acknowledgements_that_come_together_measure_no_faster_than_the_packets_were_sent() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        // A first round trip delivered a packet, so the path has a delivery to time from
        let (first, rtt) = (delivery.on_sent(start, 0).0, 300 * MICRO);
        delivery.on_acked(start + rtt, start, SIZE, first, false);
        assert!(delivery.sample(start + rtt, rtt).is_some());
        let begin = start + rtt;
        let packets: Vec<_> = (0..10u32)
            .map(|i| {
                let at = begin + SERIALIZE * i;
                (at, delivery.on_sent(at, u64::from(i) * SIZE).0)
            })
            .collect();
        let at = begin + SERIALIZE * 9 + rtt;
        for (sent, snapshot) in packets {
            delivery.on_acked(at, sent, SIZE, snapshot, false);
        }
        let sample = delivery.sample(at, rtt).unwrap();
        assert_eq!(sample.delivered, 10 * SIZE);
        // The newest packet's flight began at the first's send, and its acknowledgement phase at
        // the first round trip's delivery: 9 serializations and a round trip
        assert_eq!(sample.interval, SERIALIZE * 9 + rtt);
        assert!(sample.rate.unwrap() < RATE, "{sample:?}");
        assert_eq!(sample.rtt, rtt);
    }

    /// No flight is delivered faster than it crosses the path: a sample over an interval shorter
    /// than the least round trip has no rate, and keeps its round trip
    #[test]
    fn an_interval_shorter_than_the_least_round_trip_is_no_rate() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        let (snapshot, _) = delivery.on_sent(start, 0);
        let at = start + 50 * MICRO;
        delivery.on_acked(at, start, SIZE, snapshot, false);
        let sample = delivery.sample(at, 100 * MICRO).unwrap();
        assert_eq!(sample.rate, None);
        assert_eq!(sample.rtt, 50 * MICRO);
        // An acknowledgement that delivered nothing in flight is no sample at all
        assert_eq!(delivery.sample(at, 100 * MICRO), None);
    }

    /// Packets sent after the sender found nothing to send, and those in flight then, measure the
    /// application until what was in flight is delivered; after that the path again
    #[test]
    fn a_phase_the_application_limits_ends_once_its_flight_is_delivered() {
        let start = Instant::now();
        let rtt = 100 * MICRO;
        let mut delivery = Delivery::default();
        let (a, limited) = delivery.on_sent(start, 0);
        assert!(!limited);
        delivery.on_app_limited(SIZE);
        let (b, limited) = delivery.on_sent(start + MICRO, SIZE);
        assert!(limited);
        delivery.on_acked(start + rtt, start, SIZE, a, false);
        let sample = delivery.sample(start + rtt, rtt).unwrap();
        assert!(!sample.app_limited, "sent before the phase began");
        delivery.on_acked(start + rtt + MICRO, start + MICRO, SIZE, b, true);
        let sample = delivery.sample(start + rtt + MICRO, rtt).unwrap();
        assert!(sample.app_limited, "sent in the phase");
        // Both delivered, past the phase's end: the next packet measures the path
        let (c, limited) = delivery.on_sent(start + rtt + MICRO, 0);
        assert!(!limited);
        delivery.on_acked(
            start + 2 * rtt + MICRO,
            start + rtt + MICRO,
            SIZE,
            c,
            limited,
        );
        assert!(
            !delivery
                .sample(start + 2 * rtt + MICRO, rtt)
                .unwrap()
                .app_limited
        );
    }

    /// A packet sent on an idle path times its flight from its own send: an idle gap is neither
    /// phase of it
    #[test]
    fn a_flight_after_an_idle_gap_is_timed_from_its_first_packet() {
        let start = Instant::now();
        let rtt = 100 * MICRO;
        let mut delivery = Delivery::default();
        let (a, _) = delivery.on_sent(start, 0);
        delivery.on_acked(start + rtt, start, SIZE, a, false);
        delivery.sample(start + rtt, rtt).unwrap();
        // A second idle later
        let later = start + Duration::from_secs(1);
        let (b, _) = delivery.on_sent(later, 0);
        delivery.on_acked(later + rtt, later, SIZE, b, false);
        let sample = delivery.sample(later + rtt, rtt).unwrap();
        assert_eq!(sample.interval, rtt);
        assert_eq!(sample.rate, Some(SIZE * 10_000));
    }

    /// A packet sent 4.29 s or more after its path last delivered, packets in flight all the
    /// while, measures the stall: it is delivered, and gives no rate
    #[test]
    fn a_packet_sent_after_a_stall_gives_no_rate() {
        let start = Instant::now();
        let rtt = 100 * MICRO;
        let mut delivery = Delivery::default();
        let (a, _) = delivery.on_sent(start, 0);
        let (b, _) = delivery.on_sent(start + MICRO, SIZE);
        delivery.on_acked(start + rtt, start, SIZE, a, false);
        delivery.sample(start + rtt, rtt).unwrap();
        // Five seconds on, the second packet still in flight
        let later = start + Duration::from_secs(5);
        let (c, _) = delivery.on_sent(later, SIZE);
        delivery.on_acked(later + rtt, start + MICRO, SIZE, b, false);
        delivery.on_acked(later + rtt, later, SIZE, c, false);
        let sample = delivery.sample(later + rtt, rtt).unwrap();
        assert_eq!(sample.delivered, 2 * SIZE);
        assert_eq!(sample.rate, None);
    }
}
