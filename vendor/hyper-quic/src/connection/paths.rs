use std::{cmp, net::SocketAddr};

use tracing::trace;

use super::{
    mtud::MtuDiscovery,
    pacing::Pacer,
    spaces::{PacketSpace, SentPacket},
};
use crate::{Duration, Instant, TIMER_GRANULARITY, TransportConfig, congestion, packet::SpaceId};

use qlog::events::quic::MetricsUpdated;

/// The RTT variation's weight in the probe timer: 2, Chromium's `kPtoRttvarMultiplier`
/// (quiche/quic/core/quic_constants.h), where RFC 9002 §6.2.1 has 4.
///
/// RFC 9002's 4 is RFC 6298's RTO, whose expiry collapses the window; a PTO expiry declares nothing
/// lost and leaves the window alone (§6.2), so a probe sent early costs one or two packets. With
/// the first sample's variation at half the sample (§5.3), 2 makes the first probe wait twice the
/// smoothed RTT, RACK-TLP's probe timeout (RFC 8985 §7.2, "under normal circumstances … an ACK
/// typically arrives in one SRTT"), where 4 makes it wait three times. Measured at 500 ms one
/// way in `docs/benchmarks.md` ("Probe timeouts").
const PROBE_VARIANCE_MULTIPLIER: u32 = 2;

/// Description of a particular network path
pub(super) struct PathData {
    pub(super) remote: SocketAddr,
    pub(super) rtt: RttEstimator,
    /// Whether we're enabling ECN on outgoing packets
    pub(super) sending_ecn: bool,
    /// Congestion controller state
    pub(super) congestion: Box<dyn congestion::Controller>,
    /// Pacing state
    pub(super) pacing: Pacer,
    pub(super) challenge: Option<u64>,
    pub(super) challenge_pending: bool,
    /// Whether we're certain the peer can both send and receive on this address
    ///
    /// Initially equal to `use_stateless_retry` for servers, and becomes false again on every
    /// migration. Always true for clients.
    pub(super) validated: bool,
    /// Total size of all UDP datagrams sent on this path
    pub(super) total_sent: u64,
    /// Total size of all UDP datagrams received on this path
    pub(super) total_recvd: u64,
    /// The state of the MTU discovery process
    pub(super) mtud: MtuDiscovery,
    /// Packet number of the first packet sent after an RTT sample was collected on this path
    ///
    /// Used in persistent congestion determination.
    pub(super) first_packet_after_rtt_sample: Option<(SpaceId, u64)>,
    pub(super) in_flight: InFlight,
    /// Number of the first packet sent on this path
    ///
    /// Used to determine whether a packet was sent on an earlier path. Insufficient to determine if
    /// a packet was sent on a later path.
    first_packet: Option<u64>,

    /// Snapshot of the qlog recovery metrics
    recovery_metrics: RecoveryMetrics,

    /// Tag uniquely identifying a path in a connection
    generation: u64,
}

impl PathData {
    pub(super) fn new(
        remote: SocketAddr,
        allow_mtud: bool,
        peer_max_udp_payload_size: Option<u16>,
        generation: u64,
        now: Instant,
        config: &TransportConfig,
    ) -> Self {
        let congestion = config.congestion.build(now, config.get_initial_mtu());
        Self {
            remote,
            rtt: RttEstimator::new(config.initial_rtt),
            sending_ecn: true,
            pacing: Pacer::new(
                config.initial_rtt,
                congestion.initial_window(),
                config.get_initial_mtu(),
                now,
            ),
            congestion,
            challenge: None,
            challenge_pending: false,
            validated: false,
            total_sent: 0,
            total_recvd: 0,
            mtud: config
                .mtu_discovery_config
                .as_ref()
                .filter(|_| allow_mtud)
                .map_or(
                    MtuDiscovery::disabled(config.get_initial_mtu(), config.min_mtu),
                    |mtud_config| {
                        MtuDiscovery::new(
                            config.get_initial_mtu(),
                            config.min_mtu,
                            peer_max_udp_payload_size,
                            mtud_config.clone(),
                        )
                    },
                ),
            first_packet_after_rtt_sample: None,
            in_flight: InFlight::new(),
            first_packet: None,
            recovery_metrics: RecoveryMetrics::default(),
            generation,
        }
    }

    pub(super) fn from_previous(
        remote: SocketAddr,
        prev: &Self,
        generation: u64,
        now: Instant,
    ) -> Self {
        let congestion = prev.congestion.clone_box();
        let smoothed_rtt = prev.rtt.get();
        Self {
            remote,
            rtt: prev.rtt,
            pacing: Pacer::new(smoothed_rtt, congestion.window(), prev.current_mtu(), now),
            sending_ecn: true,
            congestion,
            challenge: None,
            challenge_pending: false,
            validated: false,
            total_sent: 0,
            total_recvd: 0,
            mtud: prev.mtud.clone(),
            first_packet_after_rtt_sample: prev.first_packet_after_rtt_sample,
            in_flight: InFlight::new(),
            first_packet: None,
            recovery_metrics: prev.recovery_metrics.clone(),
            generation,
        }
    }

    /// Resets RTT, congestion control, pacing and MTU states.
    ///
    /// This is useful when it is known the underlying path has changed.
    pub(super) fn reset(&mut self, now: Instant, config: &TransportConfig) {
        self.rtt = RttEstimator::new(config.initial_rtt);
        self.congestion = config.congestion.build(now, config.get_initial_mtu());
        self.mtud.reset(config.get_initial_mtu(), config.min_mtu);
        self.pacing = Pacer::new(
            self.rtt.get(),
            self.congestion.initial_window(),
            self.current_mtu(),
            now,
        );
    }

    /// Indicates whether we're a server that hasn't validated the peer's address and hasn't
    /// received enough data from the peer to permit sending `bytes_to_send` additional bytes
    pub(super) fn anti_amplification_blocked(&self, bytes_to_send: u64) -> bool {
        // Byte counts: the budget saturates at a total no path reaches, and the bytes to send
        // saturate upward, which blocks rather than over-sends
        !self.validated
            && self.total_recvd.saturating_mul(AMPLIFICATION_FACTOR)
                < self.total_sent.saturating_add(bytes_to_send)
    }

    /// The bytes RFC 9000 §8.1 still lets a server send to the peer's address beyond the `created`
    /// ones not yet counted in `total_sent`, or `None` once the address is validated and no limit
    /// applies: "an endpoint MUST limit the amount of data it sends to the unvalidated address to
    /// three times the amount of data received from that address"
    pub(super) fn amplification_room(&self, created: u64) -> Option<u64> {
        if self.validated {
            return None;
        }
        // Byte counts: the allowance saturates at a total no path reaches, and what is spent
        // saturates upward, which leaves less room rather than more
        Some(
            self.total_recvd
                .saturating_mul(AMPLIFICATION_FACTOR)
                .saturating_sub(self.total_sent.saturating_add(created)),
        )
    }

    /// Returns the path's current MTU
    pub(super) fn current_mtu(&self) -> u16 {
        self.mtud.current_mtu()
    }

    /// Account for transmission of `packet` with number `pn` in `space`
    pub(super) fn sent(&mut self, pn: u64, packet: SentPacket, space: &mut PacketSpace) {
        self.in_flight.insert(&packet);
        if self.first_packet.is_none() {
            self.first_packet = Some(pn);
        }
        if let Some(forgotten) = space.sent(pn, packet) {
            self.remove_in_flight(&forgotten);
        }
    }

    /// Remove `packet` with number `pn` from this path's congestion control counters, or return
    /// `false` if `pn` was sent before this path was established.
    pub(super) fn remove_in_flight(&mut self, packet: &SentPacket) -> bool {
        if packet.path_generation != self.generation {
            return false;
        }
        self.in_flight.remove(packet);
        true
    }

    pub(super) fn qlog_recovery_metrics(&mut self, pto_count: u32) -> Option<MetricsUpdated> {
        let controller_metrics = self.congestion.metrics();

        let metrics = RecoveryMetrics {
            min_rtt: Some(self.rtt.min),
            smoothed_rtt: Some(self.rtt.get()),
            latest_rtt: Some(self.rtt.latest),
            rtt_variance: Some(self.rtt.var),
            pto_count: Some(pto_count),
            bytes_in_flight: Some(self.in_flight.bytes),
            packets_in_flight: Some(self.in_flight.ack_eliciting),

            congestion_window: Some(controller_metrics.congestion_window),
            ssthresh: controller_metrics.ssthresh,
            pacing_rate: controller_metrics.pacing_rate,
        };

        let event = metrics.to_qlog_event(&self.recovery_metrics);
        self.recovery_metrics = metrics;
        event
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
}

/// Congestion metrics as described in [`recovery_metrics_updated`].
///
/// [`recovery_metrics_updated`]: https://datatracker.ietf.org/doc/html/draft-ietf-quic-qlog-quic-events.html#name-recovery_metrics_updated
#[derive(Default, Clone, PartialEq)]
#[non_exhaustive]
struct RecoveryMetrics {
    pub min_rtt: Option<Duration>,
    pub smoothed_rtt: Option<Duration>,
    pub latest_rtt: Option<Duration>,
    pub rtt_variance: Option<Duration>,
    pub pto_count: Option<u32>,
    pub bytes_in_flight: Option<u64>,
    pub packets_in_flight: Option<u64>,
    pub congestion_window: Option<u64>,
    pub ssthresh: Option<u64>,
    pub pacing_rate: Option<u64>,
}

impl RecoveryMetrics {
    /// Retain only values that have been updated since the last snapshot.
    fn retain_updated(&self, previous: &Self) -> Self {
        macro_rules! keep_if_changed {
            ($name:ident) => {
                keep_if_changed(previous.$name, self.$name)
            };
        }

        Self {
            min_rtt: keep_if_changed!(min_rtt),
            smoothed_rtt: keep_if_changed!(smoothed_rtt),
            latest_rtt: keep_if_changed!(latest_rtt),
            rtt_variance: keep_if_changed!(rtt_variance),
            pto_count: keep_if_changed!(pto_count),
            bytes_in_flight: keep_if_changed!(bytes_in_flight),
            packets_in_flight: keep_if_changed!(packets_in_flight),
            congestion_window: keep_if_changed!(congestion_window),
            ssthresh: keep_if_changed!(ssthresh),
            pacing_rate: keep_if_changed!(pacing_rate),
        }
    }

    /// Emit a `MetricsUpdated` event containing only updated values
    fn to_qlog_event(&self, previous: &Self) -> Option<MetricsUpdated> {
        let updated = self.retain_updated(previous);

        if updated == Self::default() {
            return None;
        }

        Some(MetricsUpdated {
            min_rtt: updated.min_rtt.map(|rtt| rtt.as_secs_f32()),
            smoothed_rtt: updated.smoothed_rtt.map(|rtt| rtt.as_secs_f32()),
            latest_rtt: updated.latest_rtt.map(|rtt| rtt.as_secs_f32()),
            rtt_variance: updated.rtt_variance.map(|rtt| rtt.as_secs_f32()),
            pto_count: updated
                .pto_count
                .map(|count| count.try_into().unwrap_or(u16::MAX)),
            bytes_in_flight: updated.bytes_in_flight,
            packets_in_flight: updated.packets_in_flight,
            congestion_window: updated.congestion_window,
            ssthresh: updated.ssthresh,
            pacing_rate: updated.pacing_rate,
        })
    }
}

/// RTT estimation for a particular network path
#[derive(Copy, Clone)]
pub struct RttEstimator {
    /// The most recent RTT measurement made when receiving an ack for a previously unacked packet
    latest: Duration,
    /// The smoothed RTT of the connection, computed as described in RFC6298
    smoothed: Option<Duration>,
    /// The RTT variance, computed as described in RFC6298
    var: Duration,
    /// The minimum RTT seen in the connection, ignoring ack delay.
    min: Duration,
}

impl RttEstimator {
    pub(crate) fn new(initial_rtt: Duration) -> Self {
        Self {
            latest: initial_rtt,
            smoothed: None,
            var: half(initial_rtt),
            min: initial_rtt,
        }
    }

    /// The current best RTT estimation.
    pub fn get(&self) -> Duration {
        self.smoothed.unwrap_or(self.latest)
    }

    /// Conservative estimate of RTT
    ///
    /// Takes the maximum of smoothed and latest RTT, as recommended
    /// in 6.1.2 of the recovery spec (draft 29).
    pub fn conservative(&self) -> Duration {
        self.get().max(self.latest)
    }

    /// Minimum RTT registered so far for this estimator.
    pub fn min(&self) -> Duration {
        self.min
    }

    /// How far the path's delay varies: four mean deviations (RFC 6298 §2's `K`), and at least the
    /// timer's granularity, the term RFC 9002 §6.2.1 adds to the PTO
    pub(crate) fn variation_bound(&self) -> Duration {
        cmp::max(self.var.saturating_mul(4), TIMER_GRANULARITY)
    }

    // PTO computed as described in RFC9002#6.2.1
    pub(crate) fn pto_base(&self) -> Duration {
        // Durations: saturating can only lengthen them
        self.get()
            .saturating_add(cmp::max(self.var.saturating_mul(4), TIMER_GRANULARITY))
    }

    /// The loss-detection timer's interval before backoff and the peer's ACK delay: the smoothed
    /// RTT plus [`PROBE_VARIANCE_MULTIPLIER`] times its variation once a sample exists, and
    /// RFC 9002 §6.2.1's PTO before one (§6.2.2, from kInitialRtt).
    ///
    /// Only the probe timer runs on it. The periods RFC 9002 and RFC 9000 state as multiples of
    /// the PTO (persistent congestion, §7.6.1; key discard and draining, RFC 9000 §10.2) keep
    /// [`pto_base`](Self::pto_base), so no congestion response and no state's lifetime changes.
    pub(crate) fn probe_base(&self) -> Duration {
        if self.smoothed.is_none() {
            return self.pto_base();
        }
        // Durations: saturating can only lengthen them
        self.get().saturating_add(cmp::max(
            self.var.saturating_mul(PROBE_VARIANCE_MULTIPLIER),
            TIMER_GRANULARITY,
        ))
    }

    pub(crate) fn update(&mut self, ack_delay: Duration, rtt: Duration) {
        self.latest = rtt;
        // min_rtt ignores ack delay.
        self.min = cmp::min(self.min, self.latest);
        // Based on RFC6298.
        if let Some(smoothed) = self.smoothed {
            // Durations: saturating can only lengthen them; the ACK delay is the peer's, capped
            // at its max_ack_delay
            let adjusted_rtt = if self.min.saturating_add(ack_delay) <= self.latest {
                self.latest.saturating_sub(ack_delay)
            } else {
                self.latest
            };
            let var_sample = smoothed.abs_diff(adjusted_rtt);
            self.var = quarter(self.var.saturating_mul(3).saturating_add(var_sample));
            self.smoothed = Some(eighth(
                smoothed.saturating_mul(7).saturating_add(adjusted_rtt),
            ));
        } else {
            self.smoothed = Some(self.latest);
            self.var = half(self.latest);
            self.min = self.latest;
        }
    }
}

#[derive(Default)]
pub(crate) struct PathResponses {
    pending: Vec<PathResponse>,
}

impl PathResponses {
    pub(crate) fn push(&mut self, packet: u64, token: u64, remote: SocketAddr) {
        /// Arbitrary permissive limit to prevent abuse
        const MAX_PATH_RESPONSES: usize = 16;
        let response = PathResponse {
            packet,
            token,
            remote,
        };
        let existing = self.pending.iter_mut().find(|x| x.remote == remote);
        if let Some(existing) = existing {
            // Update a queued response
            if existing.packet <= packet {
                *existing = response;
            }
            return;
        }
        if self.pending.len() < MAX_PATH_RESPONSES {
            self.pending.push(response);
        } else {
            // We don't expect to ever hit this with well-behaved peers, so we don't bother dropping
            // older challenges.
            trace!("ignoring excessive PATH_CHALLENGE");
        }
    }

    pub(crate) fn pop_off_path(&mut self, remote: SocketAddr) -> Option<(u64, SocketAddr)> {
        let response = *self.pending.last()?;
        if response.remote == remote {
            // We don't bother searching further because we expect that the on-path response will
            // get drained in the immediate future by a call to `pop_on_path`
            return None;
        }
        self.pending.pop();
        Some((response.token, response.remote))
    }

    pub(crate) fn pop_on_path(&mut self, remote: SocketAddr) -> Option<u64> {
        let response = *self.pending.last()?;
        if response.remote != remote {
            // We don't bother searching further because we expect that the off-path response will
            // get drained in the immediate future by a call to `pop_off_path`
            return None;
        }
        self.pending.pop();
        Some(response.token)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether the next response to send is for `remote`, the current path's: the packet it goes
    /// in is not held by the pacer (RFC 9000 §8.2.2)
    pub(crate) fn has_on_path(&self, remote: SocketAddr) -> bool {
        self.pending.last().is_some_and(|r| r.remote == remote)
    }
}

#[derive(Copy, Clone)]
struct PathResponse {
    /// The packet number the corresponding PATH_CHALLENGE was received in
    packet: u64,
    token: u64,
    /// The address the corresponding PATH_CHALLENGE was received from
    remote: SocketAddr,
}

/// Summary statistics of packets that have been sent on a particular path, but which have not yet
/// been acked or deemed lost
pub(super) struct InFlight {
    /// Sum of the sizes of all sent packets considered "in flight" by congestion control
    ///
    /// The size does not include IP or UDP overhead. Packets only containing ACK frames do not
    /// count towards this to ensure congestion control does not impede congestion feedback.
    pub(super) bytes: u64,
    /// Number of packets in flight containing frames other than ACK and PADDING
    ///
    /// This can be 0 even when bytes is not 0 because PADDING frames cause a packet to be
    /// considered "in flight" by congestion control. However, if this is nonzero, bytes will always
    /// also be nonzero.
    pub(super) ack_eliciting: u64,
}

impl InFlight {
    fn new() -> Self {
        Self {
            bytes: 0,
            ack_eliciting: 0,
        }
    }

    // Counts of what is in flight: each removal is of a packet inserted before, so neither
    // saturates in use
    fn insert(&mut self, packet: &SentPacket) {
        self.bytes = self.bytes.saturating_add(u64::from(packet.size));
        self.ack_eliciting = self
            .ack_eliciting
            .saturating_add(u64::from(packet.ack_eliciting));
    }

    /// Update counters to account for a packet becoming acknowledged, lost, or abandoned
    fn remove(&mut self, packet: &SentPacket) {
        self.bytes = self.bytes.saturating_sub(u64::from(packet.size));
        self.ack_eliciting = self
            .ack_eliciting
            .saturating_sub(u64::from(packet.ack_eliciting));
    }
}

/// A metric when it changed since `previous`
fn keep_if_changed<T: PartialEq>(previous: Option<T>, current: Option<T>) -> Option<T> {
    if previous == current { None } else { current }
}

/// `d / 2`; the divisor is a nonzero constant, so `checked_div` never refuses
fn half(d: Duration) -> Duration {
    d.checked_div(2).unwrap_or(Duration::ZERO)
}

/// `d / 4`; the divisor is a nonzero constant, so `checked_div` never refuses
fn quarter(d: Duration) -> Duration {
    d.checked_div(4).unwrap_or(Duration::ZERO)
}

/// `d / 8`; the divisor is a nonzero constant, so `checked_div` never refuses
fn eighth(d: Duration) -> Duration {
    d.checked_div(8).unwrap_or(Duration::ZERO)
}

/// RFC 9000 §8.1's anti-amplification factor: before validating the peer's address, an endpoint
/// sends at most three times the bytes it has received from it
const AMPLIFICATION_FACTOR: u64 = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_refreshes_pacer_budget() {
        let now = Instant::now();
        let config = TransportConfig::default();
        let remote = "203.0.113.1:4433".parse().unwrap();
        let mut path = PathData::new(remote, true, None, 0, now, &config);
        let mtu = path.current_mtu();
        let window = path.congestion.window();

        for _ in 0..1000 {
            if path
                .pacing
                .delay(
                    path.rtt.get(),
                    path.congestion.pacing_rate(),
                    mtu.into(),
                    mtu,
                    window,
                    now,
                )
                .is_some()
            {
                break;
            }
            path.pacing.on_transmit(mtu);
        }
        assert!(
            path.pacing
                .delay(
                    path.rtt.get(),
                    path.congestion.pacing_rate(),
                    mtu.into(),
                    mtu,
                    window,
                    now
                )
                .is_some()
        );

        path.reset(now, &config);

        assert_eq!(
            path.pacing.delay(
                path.rtt.get(),
                path.congestion.pacing_rate(),
                path.current_mtu().into(),
                path.current_mtu(),
                path.congestion.window(),
                now
            ),
            None
        );
    }
}
