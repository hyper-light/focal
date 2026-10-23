//! A connection's tolerance of reordering, adapted to what its path does (RFC 9002 §6.1.1, which
//! lets an implementation raise its loss thresholds once it sees spurious losses, as RACK does;
//! RACK's reordering window, RFC 8985 §6.2). Ported from slates' `transport::reorder`.
//!
//! A path that reorders (jitter across parallel links, a reordering middlebox, a queue that varies)
//! delivers packets out of order by more than RFC 9002's fixed thresholds allow: three packets, and
//! 9/8 of the round trip. Every such packet is declared lost and its data sent again although it
//! arrives, and each declaration is a congestion event, which a recovering controller answers by
//! holding its window to what was acknowledged. slates' session plane at 10 Mbit/s and 20 ms with
//! 8 ms of jitter retransmitted 27,016 frames with no real loss and carried a quarter of the link;
//! BBR here at 10 Mbit/s and 100 ms with ±20 ms of jitter carried a fifth of it; focal's work among
//! 500 agents over a 40 ms path that reorders took seconds a write (compare/work-faults,
//! 2026-10-22).
//!
//! The evidence is the spurious loss: a packet declared lost that the peer acknowledges afterwards.
//! Each one shows how far out of order it arrived (the largest packet acknowledged when it was
//! declared, less its own number) and how long it took (its send to that acknowledgement), and the
//! tolerance widens to cover it:
//! - the packet threshold rises to that distance plus one (Chromium's adaptive reordering
//!   threshold; Linux raises `tp->reordering` so). A window does not bound it: a packet is
//!   overtaken by every packet acknowledged while it travels, and those leave the flight, so a
//!   path whose delay varies by more than the window takes to send is overtaken by more packets
//!   than the window holds (±40 ms of jitter at 10 Mbit/s and 100 ms overtook by 44 with a window
//!   of 16). The time threshold is what still finds a loss then, as RACK uses no count once it has
//!   seen reordering (RFC 8985 §6.2);
//! - the time threshold is at least the longest a spurious loss took (Chromium's adaptive time
//!   threshold raises its fraction of the round trip to cover each, `SpuriousLossDetected`), and
//!   gains a reordering window on top, a quarter of the least round trip for each round trip that
//!   saw a spurious loss (RACK's `reo_wnd_mult`), never past the longer of the smoothed round trip
//!   and the longest a spurious loss took.
//!
//! Where a share of the packets skips the path's delay, as netem's `reorder` sends them, QUIC's
//! round trips are those packets', a millisecond against 80 for the rest, and RFC 9002's threshold
//! of 9/8 of them declared the rest lost on each later packet acknowledged; the window, in steps of
//! a quarter of that millisecond, never caught up. BBR at 10 Mbit/s and 80 ms with 5% skipping
//! declared 921 packets lost, every one spurious, and with the threshold covering what they took,
//! 21.
//!
//! RACK bounds its window by the smoothed round trip, which TCP samples from the segment that fills
//! a gap, the late one (RFC 7323 §4.3). QUIC samples only the largest packet acknowledged (RFC 9002
//! §5.1), and under reordering that is the packet that overtook the rest: at ±40 ms of jitter on a
//! 100 ms path the smoothed round trip read 33 to 75 ms while packets took up to 180, and a window
//! bounded by it declared every late packet lost. What a spurious loss took is a sample of the late
//! packets themselves, so the bound is the longer of the two: still a round trip the path showed.
//!
//! A round trip is RACK's `dsack_round` in packet numbers: the window widens again only once a
//! packet sent after it last widened is acknowledged. The tolerance returns to the RFC's thresholds
//! once [`QUIET_RECOVERIES`] recovery periods (RFC 9002 §7.3.2) begin with no spurious loss between
//! them (RACK's `reo_wnd_persist`), so a path that stops reordering regains prompt loss detection;
//! a period, not a detection, is what counts, as RACK counts recoveries, or a path that also loses
//! would forget its reordering within a round trip of detections.
//!
//! A declared loss is remembered for one probe timeout (RFC 9002 §6.2.1: the smoothed round trip,
//! four variations of it and the peer's acknowledgement delay), the time within which a packet's
//! acknowledgement is expected after its send, which its declaration follows. The losses
//! remembered are bounded by that age and by count: the packets a window holds for each round trip
//! the probe timeout spans, as no more can be sent, and so declared, within it. The oldest goes
//! first past the count, which costs only the evidence it held.

use std::collections::BTreeMap;

use crate::{Duration, Instant};

/// The recovery periods without a spurious loss after which the tolerance returns to the RFC's
/// thresholds: RACK's `reo_wnd_persist`, 16 (RFC 8985 §6.2).
pub(super) const QUIET_RECOVERIES: u32 = 16;

/// The reordering window grows in steps of a quarter of the least round trip (RFC 8985 §6.2,
/// `min_RTT / 4`).
const WINDOW_STEP_DIVISOR: u32 = 4;

/// A loss this end declared: when its packet was sent, the largest packet acknowledged when it was
/// declared, and when that was.
#[derive(Clone, Copy, Debug)]
struct Declared {
    sent: Instant,
    largest_acked: u64,
    at: Instant,
}

/// One connection's tolerance of reordering, learned in its Data space.
#[derive(Debug, Default)]
pub(super) struct Reordering {
    /// The packet threshold above the configured one, learned from spurious losses.
    extra_packets: u64,
    /// Quarters of the least round trip added to the time threshold (RACK's `reo_wnd_mult`).
    window_quarters: u32,
    /// The longest a spurious loss took from its send to its acknowledgement: with the smoothed
    /// round trip, the window's bound.
    late: Duration,
    /// The next packet number when the window last widened: it widens again once a packet from
    /// there on is acknowledged (RACK's `dsack_round`).
    widen_round: Option<u64>,
    /// Recovery periods begun since the last spurious loss.
    quiet: u32,
    /// When the current recovery period began: a loss of a packet sent after it begins the next
    /// (RFC 9002 §7.3.2, `congestion_recovery_start_time`).
    recovery_start: Option<Instant>,
    /// Recently declared losses, by packet number.
    declared: BTreeMap<u64, Declared>,
}

impl Reordering {
    /// The packet threshold now: `configured`, raised by what spurious losses showed.
    pub(super) fn packet_threshold(&self, configured: u32) -> u64 {
        u64::from(configured).saturating_add(self.extra_packets)
    }

    /// The time threshold now: RFC 9002's, `rfc`, or the longest a spurious loss took if longer,
    /// and the reordering window on top. A packet late by what a spurious loss showed is not
    /// declared lost again, as Chromium's adaptive time threshold covers the time a spurious loss
    /// needed (`GeneralLossAlgorithm::SpuriousLossDetected`): RFC 9002's rests on the round trip
    /// QUIC samples from the packets that overtook, short of what the late ones take.
    pub(super) fn time_threshold(
        &self,
        rfc: Duration,
        min_rtt: Duration,
        srtt: Duration,
    ) -> Duration {
        rfc.max(self.late)
            .saturating_add(self.extra_delay(min_rtt, srtt))
    }

    /// What the time threshold gains now: the reordering window, `window_quarters · min_rtt / 4`,
    /// never past the longer of `srtt` and the longest a spurious loss took.
    fn extra_delay(&self, min_rtt: Duration, srtt: Duration) -> Duration {
        min_rtt
            .checked_div(WINDOW_STEP_DIVISOR)
            .unwrap_or(Duration::ZERO)
            .saturating_mul(self.window_quarters)
            .min(srtt.max(self.late))
    }

    /// Whether any declared loss is remembered: with none, an acknowledgement shows none spurious.
    pub(super) fn remembers_declared_losses(&self) -> bool {
        !self.declared.is_empty()
    }

    /// Records `packets`, each a number and when it was sent, declared lost at `now`, the largest
    /// packet acknowledged then being `largest_acked`; forgets declarations older than `memory`, a
    /// probe timeout, and keeps at most `cap`, the oldest going first.
    pub(super) fn on_declared_lost(
        &mut self,
        packets: impl Iterator<Item = (u64, Instant)>,
        largest_acked: u64,
        now: Instant,
        memory: Duration,
        cap: usize,
    ) {
        for (packet, sent) in packets {
            self.declared.insert(
                packet,
                Declared {
                    sent,
                    largest_acked,
                    at: now,
                },
            );
        }
        self.declared
            .retain(|_, declared| now.saturating_duration_since(declared.at) <= memory);
        while self.declared.len() > cap.max(1) {
            if self.declared.pop_first().is_none() {
                break;
            }
        }
    }

    /// A loss of an ack-eliciting packet sent at `largest_lost_sent` was a congestion event at
    /// `now`: one sent after the current recovery period began begins another, and
    /// [`QUIET_RECOVERIES`] of them with no spurious loss return the tolerance to the RFC's
    /// thresholds.
    pub(super) fn on_congestion_event(&mut self, largest_lost_sent: Instant, now: Instant) {
        if self
            .recovery_start
            .is_some_and(|start| largest_lost_sent <= start)
        {
            return;
        }
        self.recovery_start = Some(now);
        self.quiet = self.quiet.saturating_add(1);
        if self.quiet >= QUIET_RECOVERIES {
            self.extra_packets = 0;
            self.window_quarters = 0;
            self.late = Duration::ZERO;
            self.widen_round = None;
            self.quiet = 0;
        }
    }

    /// Folds an acknowledged range `low..=high`, received at `now`, of an acknowledgement whose
    /// largest packet is `largest`, `next` being the next packet number: every declared loss it
    /// covers was spurious, and widens the tolerance to cover it, above `configured`, the
    /// threshold's floor. Returns how many it found.
    pub(super) fn on_acknowledged(
        &mut self,
        (low, high): (u64, u64),
        now: Instant,
        largest: u64,
        next: u64,
        configured: u32,
    ) -> u64 {
        if low > high {
            return 0;
        }
        let configured = u64::from(configured);
        let mut found = 0u64;
        while let Some((&packet, &declared)) = self.declared.range(low..=high).next() {
            self.declared.remove(&packet);
            found = found.saturating_add(1);
            let distance = declared.largest_acked.saturating_sub(packet);
            let needed = distance.saturating_add(1).saturating_sub(configured);
            self.extra_packets = self.extra_packets.max(needed);
            self.late = self.late.max(now.saturating_duration_since(declared.sent));
        }
        if found > 0 {
            self.quiet = 0;
            if self.widen_round.is_none_or(|round| largest >= round) {
                self.window_quarters = self.window_quarters.saturating_add(1);
                self.widen_round = Some(next);
            }
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    /// Packets numbered `packets`, each sent at `sent`.
    fn sent_at(packets: &[u64], sent: Instant) -> impl Iterator<Item = (u64, Instant)> + '_ {
        packets.iter().map(move |&packet| (packet, sent))
    }

    /// RFC 8985 §6.2, RFC 9002 §6.1.1: a spurious loss widens the tolerance to cover what it
    /// showed, the packet threshold to its distance plus one and the time window by a quarter of
    /// the least round trip, and the window widens again only once a packet sent after it widened
    /// is acknowledged.
    #[test]
    fn a_spurious_loss_widens_the_tolerance_to_cover_it() {
        let start = Instant::now();
        let srtt = MS * 20;
        let mut reordering = Reordering::default();
        assert_eq!(reordering.packet_threshold(3), 3);
        assert_eq!(reordering.extra_delay(srtt, srtt), Duration::ZERO);
        reordering.on_declared_lost(sent_at(&[10], start), 18, start, srtt, 64);
        assert_eq!(reordering.on_acknowledged((9, 12), start, 20, 25, 3), 1);
        assert_eq!(
            reordering.packet_threshold(3),
            9,
            "distance 8 needs a threshold of 9"
        );
        assert_eq!(reordering.extra_delay(srtt, srtt), MS * 5);
        // Within the round trip: packets from 25 on are not yet acknowledged
        reordering.on_declared_lost(sent_at(&[21], start), 24, start + MS, srtt, 64);
        reordering.on_acknowledged((21, 21), start + MS, 24, 30, 3);
        assert_eq!(
            reordering.extra_delay(srtt, srtt),
            MS * 5,
            "once a round trip"
        );
        // A packet sent after the window widened is acknowledged: the next round trip
        reordering.on_declared_lost(sent_at(&[26], start), 29, start + MS * 2, srtt, 64);
        reordering.on_acknowledged((26, 26), start + MS * 2, 29, 35, 3);
        assert_eq!(reordering.extra_delay(srtt, srtt), MS * 10);
    }

    /// The tolerance is bounded by what the path showed: the packet threshold is the farthest
    /// distance plus one, past any window, and the time window never exceeds the longer of the
    /// smoothed round trip and the longest a spurious loss took.
    #[test]
    fn the_tolerance_is_bounded_by_what_the_path_showed() {
        let start = Instant::now();
        let srtt = MS * 20;
        let mut reordering = Reordering::default();
        for round in 0..100u32 {
            let packet = u64::from(round) * 1_000;
            let at = start + srtt * round;
            let distance = u64::from(round % 10) * 100;
            // Each packet took 30 ms, longer than the smoothed round trip says
            reordering.on_declared_lost(sent_at(&[packet], at), packet + distance, at, srtt, 64);
            reordering.on_acknowledged((packet, packet), at + MS * 30, packet + 1, packet + 1, 3);
        }
        assert_eq!(reordering.packet_threshold(3), 901);
        assert_eq!(reordering.extra_delay(srtt, srtt), MS * 30);
    }

    /// RFC 8985 §6.2: after [`QUIET_RECOVERIES`] recovery periods with no spurious loss the
    /// tolerance returns to the configured thresholds; losses within one period begin none, and
    /// the memory stays bounded by its cap and its age.
    #[test]
    fn a_quiet_path_returns_to_the_thresholds_and_the_memory_stays_bounded() {
        let start = Instant::now();
        let memory = MS * 50;
        let mut reordering = Reordering::default();
        reordering.on_declared_lost(sent_at(&[5], start), 12, start, memory, 64);
        reordering.on_acknowledged((5, 5), start + MS, 12, 13, 3);
        assert!(reordering.packet_threshold(3) > 3);
        // Many losses of packets sent before the first period began are one period
        let first = start + MS * 100;
        for loss in 0..u32::from(u8::MAX) {
            reordering.on_congestion_event(start + MS * 99, first + MS * loss);
        }
        assert!(reordering.packet_threshold(3) > 3, "one period");
        for recovery in 1..u64::from(QUIET_RECOVERIES) {
            let packet = 100 + recovery * 10;
            let at = first + MS * 100 * u32::try_from(recovery).unwrap_or(u32::MAX);
            let lost = [packet, packet + 1, packet + 2];
            reordering.on_declared_lost(sent_at(&lost, at), packet + 5, at, memory, 4);
            assert!(reordering.declared.len() <= 4, "bounded by its cap");
            reordering.on_congestion_event(at - MS, at);
        }
        assert_eq!(reordering.packet_threshold(3), 3);
        assert_eq!(reordering.extra_delay(memory, memory), Duration::ZERO);
        assert!(
            reordering.declared.len() <= 3,
            "older declarations forgotten"
        );
    }

    /// The time threshold covers the longest a spurious loss took, whatever RFC 9002's: a packet
    /// as late as one that was declared lost and arrived is not declared lost again.
    #[test]
    fn the_time_threshold_covers_the_longest_a_spurious_loss_took() {
        let start = Instant::now();
        let (rfc, min_rtt, srtt) = (MS * 9, MS, MS * 8);
        let mut reordering = Reordering::default();
        assert_eq!(reordering.time_threshold(rfc, min_rtt, srtt), rfc);
        reordering.on_declared_lost(sent_at(&[3], start), 9, start + MS * 9, MS * 200, 64);
        reordering.on_acknowledged((3, 3), start + MS * 80, 9, 10, 3);
        let threshold = reordering.time_threshold(rfc, min_rtt, srtt);
        assert!(threshold >= MS * 80, "{threshold:?}");
        // The window on top is a quarter of the least round trip a round trip
        assert_eq!(threshold, MS * 80 + min_rtt / 4);
    }

    /// An acknowledgement of packets never declared lost changes nothing.
    #[test]
    fn an_acknowledgement_of_what_was_not_declared_lost_changes_nothing() {
        let start = Instant::now();
        let srtt = MS * 20;
        let mut reordering = Reordering::default();
        reordering.on_declared_lost(sent_at(&[40], start), 50, start, srtt, 64);
        assert_eq!(reordering.on_acknowledged((41, 60), start, 60, 61, 3), 0);
        assert_eq!(reordering.packet_threshold(3), 3);
        assert!(reordering.remembers_declared_losses());
    }
}
