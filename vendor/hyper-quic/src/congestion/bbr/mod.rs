use std::any::Any;
use std::fmt::Debug;

use rand::{RngExt, SeedableRng};
use rand_pcg::Pcg32;

use crate::congestion::bbr::min_max::MinMax;
use crate::congestion::{ControllerMetrics, RateSample};
use crate::connection::RttEstimator;
use crate::{Duration, Instant, float};

use super::{Controller, initial_window};

mod min_max;

/// Experimental! Use at your own risk.
///
/// Aims for reduced buffer bloat and improved performance over high bandwidth-delay product networks.
/// Based on google's quiche implementation <https://source.chromium.org/chromium/chromium/src/+/master:net/third_party/quiche/src/quic/core/congestion_control/bbr_sender.cc>
/// of BBR <https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control>.
/// More discussion and links at <https://groups.google.com/g/bbr-dev>.
///
/// Its bandwidth is the windowed maximum of the path's delivery-rate samples
/// (draft-cheng-iccrg-delivery-rate-estimation, [`RateSample`]), as Chromium's `BandwidthSampler`
/// gives its BBR, and its least round trip theirs (`VENDORED.md` §30).
#[derive(Debug, Clone)]
pub struct Bbr {
    config: BbrConfig,
    current_mtu: u64,
    /// The most a delivery-rate sample measured in the last ten round trips, bytes a second
    /// (Chromium's `max_bandwidth_`, its window `kBandwidthWindowSize`)
    max_bandwidth: MinMax,
    /// The bytes the acknowledgement being processed acknowledged
    acked_in_ack: u64,
    /// The delivery-rate sample of the acknowledgement being processed, taken into the model in
    /// `on_end_acks`, after the round trip it may begin
    sample: Option<RateSample>,
    /// Whether the last sample was of a phase the application limited (Chromium's
    /// `last_sample_is_app_limited_`)
    last_sample_app_limited: bool,
    acked_bytes: u64,
    mode: Mode,
    loss_state: LossState,
    recovery_state: RecoveryState,
    recovery_window: u64,
    is_at_full_bandwidth: bool,
    pacing_gain: f32,
    high_gain: f32,
    drain_gain: f32,
    cwnd_gain: f32,
    high_cwnd_gain: f32,
    last_cycle_start: Option<Instant>,
    current_cycle_offset: u8,
    init_cwnd: u64,
    min_cwnd: u64,
    prev_in_flight_count: u64,
    exit_probe_rtt_at: Option<Instant>,
    /// When the minimum round trip held now was sampled: it expires ten seconds after.
    min_rtt_timestamp: Option<Instant>,
    min_rtt: Duration,
    exiting_quiescence: bool,
    pacing_rate: u64,
    max_acked_packet_number: u64,
    max_sent_packet_number: u64,
    end_recovery_at_packet_number: u64,
    cwnd: u64,
    current_round_trip_end_packet_number: u64,
    round_count: u64,
    bw_at_last_round: u64,
    round_wo_bw_gain: u64,
    ack_aggregation: AckAggregationState,
    random_number_generator: Pcg32,
}

impl Bbr {
    /// Construct a state using the given `config` and current time `now`
    pub fn new(config: BbrConfig, current_mtu: u16) -> Self {
        let window = config
            .initial_window
            .unwrap_or_else(|| initial_window(current_mtu.into()));
        Self {
            config,
            current_mtu: current_mtu as u64,
            max_bandwidth: MinMax::default(),
            acked_in_ack: 0,
            sample: None,
            last_sample_app_limited: false,
            acked_bytes: 0,
            mode: Mode::Startup,
            loss_state: Default::default(),
            recovery_state: RecoveryState::NotInRecovery,
            recovery_window: 0,
            is_at_full_bandwidth: false,
            pacing_gain: K_DEFAULT_HIGH_GAIN,
            high_gain: K_DEFAULT_HIGH_GAIN,
            drain_gain: 1.0 / K_DEFAULT_HIGH_GAIN,
            cwnd_gain: K_DEFAULT_HIGH_GAIN,
            high_cwnd_gain: K_DEFAULT_HIGH_GAIN,
            last_cycle_start: None,
            current_cycle_offset: 0,
            init_cwnd: window,
            min_cwnd: calculate_min_window(current_mtu as u64),
            prev_in_flight_count: 0,
            exit_probe_rtt_at: None,
            min_rtt_timestamp: None,
            min_rtt: Default::default(),
            exiting_quiescence: false,
            pacing_rate: 0,
            max_acked_packet_number: 0,
            max_sent_packet_number: 0,
            end_recovery_at_packet_number: 0,
            cwnd: window,
            current_round_trip_end_packet_number: 0,
            round_count: 0,
            bw_at_last_round: 0,
            round_wo_bw_gain: 0,
            ack_aggregation: AckAggregationState::default(),
            random_number_generator: Pcg32::from_rng(&mut rand::rng()),
        }
    }

    fn enter_startup_mode(&mut self) {
        self.mode = Mode::Startup;
        self.pacing_gain = self.high_gain;
        self.cwnd_gain = self.high_cwnd_gain;
    }

    fn enter_probe_bandwidth_mode(&mut self, now: Instant) {
        self.mode = Mode::ProbeBw;
        self.cwnd_gain = K_DERIVED_HIGH_CWNDGAIN;
        self.last_cycle_start = Some(now);
        // Pick a random offset for the gain cycle out of {0, 2..7} range. 1 is
        // excluded because in that case increased gain and decreased gain would not
        // follow each other.
        let mut rand_index = self
            .random_number_generator
            .random_range(0..GAIN_CYCLE_LENGTH - 1);
        if rand_index >= 1 {
            // At most the cycle's last offset
            rand_index = rand_index.saturating_add(1);
        }
        self.current_cycle_offset = rand_index;
        self.pacing_gain = pacing_gain_at(rand_index);
    }

    fn update_recovery_state(&mut self, is_round_start: bool) {
        // Exit recovery when there are no losses for a round.
        if self.loss_state.has_losses() {
            self.end_recovery_at_packet_number = self.max_sent_packet_number;
        }
        match self.recovery_state {
            // Enter conservation on the first loss.
            RecoveryState::NotInRecovery if self.loss_state.has_losses() => {
                self.recovery_state = RecoveryState::Conservation;
                // This will cause the |recovery_window| to be set to the
                // correct value in CalculateRecoveryWindow().
                self.recovery_window = 0;
                // Since the conservation phase is meant to be lasting for a whole
                // round, extend the current round as if it were started right now.
                self.current_round_trip_end_packet_number = self.max_sent_packet_number;
            }
            RecoveryState::Growth | RecoveryState::Conservation => {
                if self.recovery_state == RecoveryState::Conservation && is_round_start {
                    self.recovery_state = RecoveryState::Growth;
                }
                // Exit recovery if appropriate.
                if !self.loss_state.has_losses()
                    && self.max_acked_packet_number > self.end_recovery_at_packet_number
                {
                    self.recovery_state = RecoveryState::NotInRecovery;
                }
            }
            _ => {}
        }
    }

    fn update_gain_cycle_phase(&mut self, now: Instant, in_flight: u64) {
        // In most cases, the cycle is advanced after an RTT passes.
        let mut should_advance_gain_cycling = self
            .last_cycle_start
            .map(|last_cycle_start| now.duration_since(last_cycle_start) > self.min_rtt)
            .unwrap_or(false);
        // If the pacing gain is above 1.0, the connection is trying to probe the
        // bandwidth by increasing the number of bytes in flight to at least
        // pacing_gain * BDP.  Make sure that it actually reaches the target, as
        // long as there are no losses suggesting that the buffers are not able to
        // hold that much.
        if self.pacing_gain > 1.0
            && !self.loss_state.has_losses()
            && self.prev_in_flight_count < self.get_target_cwnd(self.pacing_gain)
        {
            should_advance_gain_cycling = false;
        }

        // If pacing gain is below 1.0, the connection is trying to drain the extra
        // queue which could have been incurred by probing prior to it.  If the
        // number of bytes in flight falls down to the estimated BDP value earlier,
        // conclude that the queue has been successfully drained and exit this cycle
        // early.
        if self.pacing_gain < 1.0 && in_flight <= self.get_target_cwnd(1.0) {
            should_advance_gain_cycling = true;
        }

        if should_advance_gain_cycling {
            self.current_cycle_offset = self
                .current_cycle_offset
                .checked_add(1)
                .filter(|&next| next < GAIN_CYCLE_LENGTH)
                .unwrap_or(0);
            self.last_cycle_start = Some(now);
            // Stay in low gain mode until the target BDP is hit.  Low gain mode
            // will be exited immediately when the target BDP is achieved.
            if DRAIN_TO_TARGET
                && self.pacing_gain < 1.0
                && (pacing_gain_at(self.current_cycle_offset) - 1.0).abs() < f32::EPSILON
                && in_flight > self.get_target_cwnd(1.0)
            {
                return;
            }
            self.pacing_gain = pacing_gain_at(self.current_cycle_offset);
        }
    }

    fn maybe_exit_startup_or_drain(&mut self, now: Instant, in_flight: u64) {
        if self.mode == Mode::Startup && self.is_at_full_bandwidth {
            self.mode = Mode::Drain;
            self.pacing_gain = self.drain_gain;
            self.cwnd_gain = self.high_cwnd_gain;
        }
        if self.mode == Mode::Drain && in_flight <= self.get_target_cwnd(1.0) {
            self.enter_probe_bandwidth_mode(now);
        }
    }

    /// Whether the minimum round trip held now is older than ten seconds (Chromium's
    /// `kMinRttExpiry`), timed from when it was sampled, as Chromium's `min_rtt_timestamp_` is. The
    /// port timed it from when ProbeRtt last began and, before any had, called it expired: every
    /// connection entered ProbeRtt, its window four datagrams, at its first acknowledgement not
    /// app-limited, and stayed 200 ms. A sample of a phase the application limited does not
    /// expire it: such a sender keeps no queue of its own to drain (Chromium's option
    /// `probe_rtt_disabled_if_app_limited_`).
    fn is_min_rtt_expired(&self, now: Instant, app_limited: bool) -> bool {
        /// How long a minimum round trip stands before it is measured again (Chromium's value)
        const MIN_RTT_EXPIRY: Duration = Duration::from_secs(10);
        !app_limited
            && self
                .min_rtt_timestamp
                .is_some_and(|sampled| now.saturating_duration_since(sampled) > MIN_RTT_EXPIRY)
    }

    /// The acknowledgement's delivery-rate sample, taken into the model (Chromium's
    /// `UpdateBandwidthAndMinRtt`): its rate into the windowed maximum, unless it measured the
    /// application and is below it, and its round trip into the minimum, which is taken anew from
    /// the sample once it expires, so a path whose round trip grew is measured as it is now.
    /// Returns whether the minimum had expired.
    fn update_bandwidth_and_min_rtt(&mut self, now: Instant, sample: &RateSample) -> bool {
        self.last_sample_app_limited = sample.app_limited;
        if let Some(rate) = sample.rate
            && (!sample.app_limited || self.max_bandwidth.get() < rate)
        {
            self.max_bandwidth.update_max(self.round_count, rate);
        }
        if sample.rtt.is_zero() {
            return false;
        }
        let expired = self.is_min_rtt_expired(now, sample.app_limited);
        if expired || self.min_rtt.is_zero() || sample.rtt < self.min_rtt {
            self.min_rtt = sample.rtt;
            self.min_rtt_timestamp = Some(now);
        }
        expired
    }

    fn maybe_enter_or_exit_probe_rtt(
        &mut self,
        now: Instant,
        is_round_start: bool,
        bytes_in_flight: u64,
        min_rtt_expired: bool,
    ) {
        if min_rtt_expired && !self.exiting_quiescence && self.mode != Mode::ProbeRtt {
            self.mode = Mode::ProbeRtt;
            self.pacing_gain = 1.0;
            // Do not decide on the time to exit ProbeRtt until the
            // |bytes_in_flight| is at the target small value.
            self.exit_probe_rtt_at = None;
        }

        if self.mode == Mode::ProbeRtt {
            match self.exit_probe_rtt_at {
                None => {
                    // If the window has reached the appropriate size, schedule exiting
                    // ProbeRtt.  The CWND during ProbeRtt is
                    // kMinimumCongestionWindow, but we allow an extra packet since QUIC
                    // checks CWND before sending a packet.
                    if bytes_in_flight < self.get_probe_rtt_cwnd().saturating_add(self.current_mtu)
                    {
                        /// How long ProbeRtt lasts (upstream's value, from the BBR draft)
                        const K_PROBE_RTT_TIME: Duration = Duration::from_millis(200);
                        self.exit_probe_rtt_at = now.checked_add(K_PROBE_RTT_TIME);
                    }
                }
                Some(exit_time) if is_round_start && now >= exit_time => {
                    if !self.is_at_full_bandwidth {
                        self.enter_startup_mode();
                    } else {
                        self.enter_probe_bandwidth_mode(now);
                    }
                }
                Some(_) => {}
            }
        }

        self.exiting_quiescence = false;
    }

    fn get_target_cwnd(&self, gain: f32) -> u64 {
        let bw = self.max_bandwidth.get();
        // Byte counts: saturating holds them at a size no connection reaches
        let bdp = u64::try_from(self.min_rtt.as_micros())
            .unwrap_or(u64::MAX)
            .saturating_mul(bw);
        let bdpf = bdp as f64;
        let cwnd = float::saturating_u64((gain as f64 * bdpf) / 1_000_000f64);
        // BDP estimate will be zero if no bandwidth samples are available yet.
        if cwnd == 0 {
            return self.init_cwnd;
        }
        cwnd.max(self.min_cwnd)
    }

    fn get_probe_rtt_cwnd(&self) -> u64 {
        /// ProbeRTT's window as a fraction of the BDP (upstream's value, after Chromium's BBR)
        const K_MODERATE_PROBE_RTT_MULTIPLIER: f32 = 0.75;
        if PROBE_RTT_BASED_ON_BDP {
            return self.get_target_cwnd(K_MODERATE_PROBE_RTT_MULTIPLIER);
        }
        self.min_cwnd
    }

    fn calculate_pacing_rate(&mut self) {
        let bw = self.max_bandwidth.get();
        if bw == 0 {
            return;
        }
        let target_rate = float::saturating_u64(bw as f64 * self.pacing_gain as f64);
        if self.is_at_full_bandwidth {
            self.pacing_rate = target_rate;
            return;
        }

        // Pace at the rate of initial_window / RTT as soon as RTT measurements are
        // available.
        // With no RTT measured yet there is no rate
        if self.pacing_rate == 0
            && let Some(rate) = bw_from_delta(self.init_cwnd, self.min_rtt)
        {
            self.pacing_rate = rate;
            return;
        }

        // Do not decrease the pacing rate during startup.
        if self.pacing_rate < target_rate {
            self.pacing_rate = target_rate;
        }
    }

    fn calculate_cwnd(&mut self, bytes_acked: u64, excess_acked: u64) {
        if self.mode == Mode::ProbeRtt {
            return;
        }
        let mut target_window = self.get_target_cwnd(self.cwnd_gain);
        if self.is_at_full_bandwidth {
            // Add the max recently measured ack aggregation to CWND.
            target_window = target_window.saturating_add(self.ack_aggregation.max_ack_height.get());
        } else {
            // Add the most recent excess acked.  Because CWND never decreases in
            // STARTUP, this will automatically create a very localized max filter.
            target_window = target_window.saturating_add(excess_acked);
        }
        // Instead of immediately setting the target CWND as the new one, BBR grows
        // the CWND towards |target_window| by only increasing it |bytes_acked| at a
        // time.
        if self.is_at_full_bandwidth {
            self.cwnd = target_window.min(self.cwnd.saturating_add(bytes_acked));
        } else if self.cwnd < target_window || self.acked_bytes < self.init_cwnd {
            // If the connection is not yet out of startup phase, do not decrease the window; grow
            // it towards the target and no further. Chromium's `CalculateCongestionWindow` compares
            // the window with the target (`congestion_window_ < target_window`); the port compared
            // the gain (about 2.89) with the target in bytes, true of any target, so a sender that
            // stays in startup (one its peer's flow control holds is app-limited, and never sees
            // full bandwidth) grew its window by every byte acked, without end: a bus publisher's
            // reached 409 MB on a path of 72 µs, its lane bound to it holding seconds of bytes.
            // RFC 9002 §7.8: a window not used to its size is not grown.
            self.cwnd = self.cwnd.saturating_add(bytes_acked);
        }

        // Enforce the limits on the congestion window.
        if self.cwnd < self.min_cwnd {
            self.cwnd = self.min_cwnd;
        }
    }

    fn calculate_recovery_window(&mut self, bytes_acked: u64, bytes_lost: u64, in_flight: u64) {
        if !self.recovery_state.in_recovery() {
            return;
        }
        // Set up the initial recovery window.
        if self.recovery_window == 0 {
            self.recovery_window = self.min_cwnd.max(in_flight.saturating_add(bytes_acked));
            return;
        }

        // Remove losses from the recovery window, while accounting for a potential
        // integer underflow.
        if let Some(left) = self.recovery_window.checked_sub(bytes_lost) {
            self.recovery_window = left;
        } else {
            // k_max_segment_size = current_mtu
            self.recovery_window = self.current_mtu;
        }
        // In CONSERVATION mode, just subtracting losses is sufficient.  In GROWTH,
        // release additional |bytes_acked| to achieve a slow-start-like behavior.
        if self.recovery_state == RecoveryState::Growth {
            self.recovery_window = self.recovery_window.saturating_add(bytes_acked);
        }

        // Sanity checks.  Ensure that we always allow to send at least an MSS or
        // |bytes_acked| in response, whichever is larger.
        self.recovery_window = self
            .recovery_window
            .max(in_flight.saturating_add(bytes_acked))
            .max(self.min_cwnd);
    }

    /// <https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control#section-4.3.2.2>
    fn check_if_full_bw_reached(&mut self, app_limited: bool) {
        if app_limited {
            return;
        }
        let target =
            float::saturating_u64(self.bw_at_last_round as f64 * K_STARTUP_GROWTH_TARGET as f64);
        let bw = self.max_bandwidth.get();
        if bw >= target {
            self.bw_at_last_round = bw;
            self.round_wo_bw_gain = 0;
            self.ack_aggregation.max_ack_height.reset();
            return;
        }

        self.round_wo_bw_gain = self.round_wo_bw_gain.saturating_add(1);
        if self.round_wo_bw_gain >= K_ROUND_TRIPS_WITHOUT_GROWTH_BEFORE_EXITING_STARTUP as u64
            || (self.recovery_state.in_recovery())
        {
            self.is_at_full_bandwidth = true;
        }
    }
}

impl Controller for Bbr {
    fn on_sent(&mut self, _now: Instant, _bytes: u64, last_packet_number: u64) {
        self.max_sent_packet_number = last_packet_number;
    }

    fn on_ack(
        &mut self,
        _now: Instant,
        _sent: Instant,
        bytes: u64,
        _app_limited: bool,
        _rtt: &RttEstimator,
    ) {
        self.acked_in_ack = self.acked_in_ack.saturating_add(bytes);
        self.acked_bytes = self.acked_bytes.saturating_add(bytes);
    }

    fn on_delivered(&mut self, _now: Instant, sample: &RateSample) {
        self.sample = Some(*sample);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        _app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        let bytes_acked = core::mem::take(&mut self.acked_in_ack);
        if let Some(largest_acked_packet) = largest_packet_num_acked {
            self.max_acked_packet_number = largest_acked_packet;
        }

        // Chromium's order (`BbrSender::OnCongestionEvent`): the round trip the acknowledgement
        // may begin, the sample taken into the model, recovery, then ack aggregation by the
        // bandwidth just updated
        let mut is_round_start = false;
        if bytes_acked > 0 {
            is_round_start =
                self.max_acked_packet_number > self.current_round_trip_end_packet_number;
            if is_round_start {
                self.current_round_trip_end_packet_number = self.max_sent_packet_number;
                self.round_count = self.round_count.saturating_add(1);
            }
        }
        let min_rtt_expired = self
            .sample
            .take()
            .is_some_and(|sample| self.update_bandwidth_and_min_rtt(now, &sample));

        self.update_recovery_state(is_round_start);

        let excess_acked = self.ack_aggregation.update_ack_aggregation_bytes(
            bytes_acked,
            now,
            self.round_count,
            self.max_bandwidth.get(),
        );

        if self.mode == Mode::ProbeBw {
            self.update_gain_cycle_phase(now, in_flight);
        }

        // A sample of a phase the application limited says nothing of whether the pipe is full
        if is_round_start && !self.is_at_full_bandwidth {
            self.check_if_full_bw_reached(self.last_sample_app_limited);
        }

        self.maybe_exit_startup_or_drain(now, in_flight);

        self.maybe_enter_or_exit_probe_rtt(now, is_round_start, in_flight, min_rtt_expired);

        // After the model is updated, recalculate the pacing rate and congestion window.
        self.calculate_pacing_rate();
        self.calculate_cwnd(bytes_acked, excess_acked);
        self.calculate_recovery_window(bytes_acked, self.loss_state.lost_bytes, in_flight);

        self.prev_in_flight_count = in_flight;
        self.loss_state.reset();
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.loss_state.lost_bytes = self.loss_state.lost_bytes.saturating_add(lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.current_mtu = new_mtu as u64;
        self.min_cwnd = calculate_min_window(self.current_mtu);
        self.init_cwnd = self.initial_window().max(self.min_cwnd);
        self.cwnd = self.cwnd.max(self.min_cwnd);
    }

    fn window(&self) -> u64 {
        if self.mode == Mode::ProbeRtt {
            return self.get_probe_rtt_cwnd();
        } else if self.recovery_state.in_recovery() && self.mode != Mode::Startup {
            return self.cwnd.min(self.recovery_window);
        }
        self.cwnd
    }

    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            // Bits per second
            pacing_rate: Some(self.pacing_rate.saturating_mul(8)),
        }
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config
            .initial_window
            .unwrap_or_else(|| initial_window(self.current_mtu))
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Configuration for the [`Bbr`] congestion controller
#[derive(Debug, Clone, Default)]
pub struct BbrConfig {
    /// `None`: RFC 9002 §7.2's, by the path's datagram size
    initial_window: Option<u64>,
}

impl BbrConfig {
    /// Limit on the amount of outstanding data in bytes before any is acknowledged
    ///
    /// By default RFC 9002 §7.2's, `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`,
    /// recalculated as the path's datagram size changes, as CUBIC's and NewReno's are; a value set
    /// here is fixed. Upstream's default was 200 datagrams of 1,200 bytes, 240,000 bytes: Chromium's
    /// bound on a window resumed from a cached bandwidth (`kMaxInitialCongestionWindow`), not a
    /// start, and the bound the path's warm-up spends to scales with it (`VENDORED.md` §21).
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_window = Some(value);
        self
    }
}

#[derive(Debug, Default, Copy, Clone)]
struct AckAggregationState {
    max_ack_height: MinMax,
    aggregation_epoch_start_time: Option<Instant>,
    aggregation_epoch_bytes: u64,
}

impl AckAggregationState {
    fn update_ack_aggregation_bytes(
        &mut self,
        newly_acked_bytes: u64,
        now: Instant,
        round: u64,
        max_bandwidth: u64,
    ) -> u64 {
        // Compute how many bytes are expected to be delivered, assuming max
        // bandwidth is correct.
        // Byte counts: saturating holds them at a size no connection reaches
        let elapsed_micros = u64::try_from(
            now.saturating_duration_since(self.aggregation_epoch_start_time.unwrap_or(now))
                .as_micros(),
        )
        .unwrap_or(u64::MAX);
        let expected_bytes_acked = max_bandwidth.saturating_mul(elapsed_micros) / 1_000_000;

        // Reset the current aggregation epoch as soon as the ack arrival rate is
        // less than or equal to the max bandwidth.
        if self.aggregation_epoch_bytes <= expected_bytes_acked {
            // Reset to start measuring a new aggregation epoch.
            self.aggregation_epoch_bytes = newly_acked_bytes;
            self.aggregation_epoch_start_time = Some(now);
            return 0;
        }

        // Compute how many extra bytes were delivered vs max bandwidth.
        // Include the bytes most recently acknowledged to account for stretch acks.
        self.aggregation_epoch_bytes = self
            .aggregation_epoch_bytes
            .saturating_add(newly_acked_bytes);
        // Above the expected bytes, as checked just before
        let diff = self
            .aggregation_epoch_bytes
            .saturating_sub(expected_bytes_acked);
        self.max_ack_height.update_max(round, diff);
        diff
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Mode {
    // Startup phase of the connection.
    Startup,
    // After achieving the highest possible bandwidth during the startup, lower
    // the pacing rate in order to drain the queue.
    Drain,
    // Cruising mode.
    ProbeBw,
    // Temporarily slow down sending in order to empty the buffer and measure
    // the real minimum RTT.
    ProbeRtt,
}

// Indicates how the congestion control limits the amount of bytes in flight.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RecoveryState {
    // Do not limit.
    NotInRecovery,
    // Allow an extra outstanding byte for each byte acknowledged.
    Conservation,
    // Allow two extra outstanding bytes for each byte acknowledged (slow
    // start).
    Growth,
}

impl RecoveryState {
    pub(super) fn in_recovery(&self) -> bool {
        !matches!(self, Self::NotInRecovery)
    }
}

#[derive(Debug, Clone, Default)]
struct LossState {
    lost_bytes: u64,
}

impl LossState {
    pub(super) fn reset(&mut self) {
        self.lost_bytes = 0;
    }

    pub(super) fn has_losses(&self) -> bool {
        self.lost_bytes != 0
    }
}

/// Bytes a second from `bytes` over `delta`, computed in u128 (upstream's u64 product overflowed
/// past 18 GB) and saturating into a u64; none over an empty interval
fn bw_from_delta(bytes: u64, delta: Duration) -> Option<u64> {
    /// Nanoseconds in a second
    const NANOS_PER_SEC: u128 = 1_000_000_000;
    let bytes_per_second = u128::from(bytes)
        .saturating_mul(NANOS_PER_SEC)
        .checked_div(delta.as_nanos())?;
    Some(u64::try_from(bytes_per_second).unwrap_or(u64::MAX))
}

fn calculate_min_window(current_mtu: u64) -> u64 {
    4u64.saturating_mul(current_mtu)
}

/// The pacing gain at `offset` in the gain cycle
fn pacing_gain_at(offset: u8) -> f32 {
    let [g0, g1, g2, g3, g4, g5, g6, g7] = K_PACING_GAIN;
    // The mask keeps the offset within the cycle's eight phases
    match offset & (GAIN_CYCLE_LENGTH - 1) {
        0 => g0,
        1 => g1,
        2 => g2,
        3 => g3,
        4 => g4,
        5 => g5,
        6 => g6,
        _ => g7,
    }
}

// The gain used for the STARTUP, equal to 2/ln(2).
/// The gain used for STARTUP, 2/ln(2) (draft-cardwell-iccrg-bbr-congestion-control §4.6.1)
const K_DEFAULT_HIGH_GAIN: f32 = 2.885;
// The newly derived CWND gain for STARTUP, 2.
/// The newly derived CWND gain for STARTUP, 2 (BBR draft §4.6.1)
const K_DERIVED_HIGH_CWNDGAIN: f32 = 2.0;
// The cycle of gains used during the ProbeBw stage.
/// The cycle of gains used during ProbeBW (BBR draft §4.3.4)
const K_PACING_GAIN: [f32; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
/// Phases in the gain cycle, a power of two
const GAIN_CYCLE_LENGTH: u8 = 8;

/// The bandwidth growth per round that keeps STARTUP going, 25% (BBR draft §4.3.2.2)
const K_STARTUP_GROWTH_TARGET: f32 = 1.25;
/// Rounds without that growth before STARTUP ends, 3 (BBR draft §4.3.2.2)
const K_ROUND_TRIPS_WITHOUT_GROWTH_BEFORE_EXITING_STARTUP: u8 = 3;

/// ProbeRTT sizes its window from the BDP (upstream's choice)
const PROBE_RTT_BASED_ON_BDP: bool = true;
/// Low gain holds until the BDP target is reached (upstream's choice)
const DRAIN_TO_TARGET: bool = true;

#[cfg(test)]
mod tests {
    use super::super::BASE_DATAGRAM_SIZE;
    use super::*;
    use crate::connection::{Delivery, RttEstimator, Snapshot};
    use std::collections::VecDeque;
    use std::time::Duration;

    const PACKET: u64 = 1_200;

    /// An acknowledgement due: when, the packet's send, its snapshot, whether it went app-limited,
    /// and its number
    type Due = (Instant, Instant, Snapshot, bool, u64);

    /// A connection's side of the controller: the path's delivery and round trip, sampled as a
    /// connection samples them, an acknowledgement a packet
    struct Sender {
        bbr: Bbr,
        delivery: Delivery,
        rtt: RttEstimator,
        in_flight: u64,
        sent: u64,
    }

    impl Sender {
        fn new() -> Self {
            Self {
                bbr: Bbr::new(BbrConfig::default(), BASE_DATAGRAM_SIZE as u16),
                delivery: Delivery::default(),
                rtt: RttEstimator::new(Duration::from_millis(333)),
                in_flight: 0,
                sent: 0,
            }
        }

        /// A packet goes at `now`: its snapshot, whether it went app-limited, and its number
        fn send(&mut self, now: Instant) -> (Snapshot, bool, u64) {
            let (snapshot, app_limited) = self.delivery.on_sent(now, self.in_flight);
            let number = self.sent;
            self.bbr.on_sent(now, PACKET, number);
            self.in_flight += PACKET;
            self.sent += 1;
            (snapshot, app_limited, number)
        }

        /// The packet `number`, sent at `sent` with `snapshot`, is acknowledged at `now`: its
        /// round trip
        fn ack(&mut self, (now, sent, snapshot, app_limited, number): Due) -> Duration {
            self.in_flight -= PACKET;
            self.bbr.on_ack(now, sent, PACKET, false, &self.rtt);
            let round_trip = now - sent;
            self.rtt.update(Duration::ZERO, round_trip);
            self.delivery
                .on_acked(now, sent, PACKET, snapshot, app_limited);
            if let Some(sample) = self.delivery.sample(now, self.rtt.min()) {
                self.bbr.on_delivered(now, &sample);
            }
            self.bbr
                .on_end_acks(now, self.in_flight, false, Some(number));
            round_trip
        }
    }

    fn start() -> Instant {
        hyper_sim::Anchor::new().instant(0).unwrap()
    }

    /// A sender its peer's flow control holds is app-limited, and stays in startup: it never sees
    /// full bandwidth. Paced at four packets a millisecond over a round trip of one, its window
    /// grows towards the target, the gain times the bandwidth-delay product it measures, and no
    /// further, however many bytes are acknowledged. The port grew it by every byte acknowledged:
    /// it compared the gain, not the window, with the target (Chromium's
    /// `CalculateCongestionWindow`), and its estimate, fed no app-limited sample, stayed zero, so
    /// every byte acknowledged was ack aggregation's excess (RFC 9002 §7.8).
    #[test]
    fn an_app_limited_sender_in_startup_grows_its_window_to_the_target_and_no_further() {
        let (gap, round_trip) = (Duration::from_micros(250), Duration::from_millis(1));
        let mut sender = Sender::new();
        let start = start();
        let mut due: VecDeque<Due> = VecDeque::new();
        let mut halfway = 0;
        for number in 0..8_000_u32 {
            let now = start + gap * number;
            while due.front().is_some_and(|(at, ..)| *at <= now) {
                sender.ack(due.pop_front().unwrap());
            }
            let (snapshot, app_limited, number) = sender.send(now);
            due.push_back((now + round_trip, now, snapshot, app_limited, number));
            // Nothing more to send, the window open
            assert!(sender.in_flight < sender.bbr.window());
            sender.delivery.on_app_limited(sender.in_flight);
            if number == 3_999 {
                halfway = sender.bbr.window();
            }
        }
        // 8,000 packets, 9.6 MB; the path carried four a millisecond: 4.8 KB a round trip. The
        // window is the target, the startup gain (2.89) times that and ack aggregation's excess,
        // and the second half of the bytes acknowledged grew it not at all.
        let bdp = 4 * PACKET;
        let window = sender.bbr.window();
        assert!(
            window <= 6 * bdp,
            "the window grew past the target: {window} B for a BDP of {bdp} B"
        );
        assert_eq!(
            window, halfway,
            "4.8 MB more acknowledged, the window unmoved"
        );
        assert_eq!(sender.bbr.mode, Mode::Startup);
    }

    /// What a bulk sender saw through a bottleneck of `RATE` and `PROPAGATION` each way, its
    /// sends held by the window alone or also paced at the rate the controller states: the
    /// controller, and the longest round trip of the second half of its packets
    fn bulk(paced: bool) -> (Bbr, Duration) {
        const PACKETS: u64 = 4_000;
        let mut sender = Sender::new();
        let mut now = start();
        let (mut free, mut next_send) = (now, now);
        let mut due: VecDeque<Due> = VecDeque::new();
        let (mut acked, mut most) = (0, Duration::ZERO);
        while acked < PACKETS {
            let open = sender.in_flight + PACKET <= sender.bbr.window() && sender.sent < PACKETS;
            if open && now >= next_send {
                let (snapshot, app_limited, number) = sender.send(now);
                free = free.max(now) + SERIALIZE;
                due.push_back((free + 2 * PROPAGATION, now, snapshot, app_limited, number));
                if let Some(rate) = sender.bbr.pacing_rate().filter(|_| paced) {
                    next_send = now + Duration::from_nanos(PACKET * 1_000_000_000 / rate);
                }
                continue;
            }
            // The next event: the pacer's release, if the window is open and it comes first, or
            // the next acknowledgement
            if open && due.front().is_none_or(|(at, ..)| next_send < *at) {
                now = next_send;
                continue;
            }
            let next = due.pop_front().unwrap();
            now = now.max(next.0);
            let round_trip = sender.ack(next);
            acked += 1;
            if acked > PACKETS / 2 {
                most = most.max(round_trip);
            }
        }
        (sender.bbr, most)
    }

    /// A bottleneck of 12.5 MB/s (100 Mbit/s)
    const RATE: u64 = 12_500_000;
    /// 40 µs of propagation each way
    const PROPAGATION: Duration = Duration::from_micros(40);
    /// A packet's time at the bottleneck: 96 µs
    const SERIALIZE: Duration = Duration::from_nanos(PACKET * 1_000_000_000 / RATE);

    /// A packet's round trip alone, 176 µs, and the path's bandwidth-delay product, 2.2 kB
    fn alone() -> (Duration, u64) {
        let alone = 2 * PROPAGATION + SERIALIZE;
        let bdp = RATE * u64::try_from(alone.as_nanos()).unwrap() / 1_000_000_000;
        (alone, bdp)
    }

    /// A sender with all it can send, its window alone holding it, through the bottleneck: its
    /// bandwidth is the bottleneck's, and its window a few times the product, so the queue it
    /// keeps is a few packets. With the port's estimator, the bytes of each packet acknowledged
    /// over the gap since the acknowledgement before and of the latest packet sent over the gap
    /// since the one before, whichever packets those were, a pair on this path estimated 357 MB/s
    /// and its window grew to 147 kB, 65 times the product, its queue lengthening the round trip
    /// from 143 µs to 5 ms (`VENDORED.md` §30).
    #[test]
    fn a_bulk_sender_measures_its_bottleneck_and_keeps_a_window_of_a_few_bandwidth_delay_products()
    {
        let (bbr, most) = bulk(false);
        let (alone, bdp) = alone();
        let estimate = bbr.max_bandwidth.get();
        assert!(
            (RATE * 9 / 10..=RATE).contains(&estimate),
            "estimated {estimate} B/s of a {RATE} B/s bottleneck"
        );
        assert_eq!(bbr.min_rtt, alone);
        let window = bbr.window();
        assert!(
            window <= 4 * bdp,
            "a window of {window} B for a product of {bdp} B"
        );
        assert!(
            most <= alone * 4,
            "round trips to {most:?} on a path of {alone:?}"
        );
    }

    /// RFC 9002 §7.2's initial window by the path's datagram size, as CUBIC's and NewReno's are; a
    /// window set in the configuration is kept whatever the datagram size
    #[test]
    fn the_initial_window_is_rfc_9002s_by_the_datagram_size_unless_set() {
        let mut bbr = Bbr::new(BbrConfig::default(), BASE_DATAGRAM_SIZE as u16);
        assert_eq!((bbr.window(), bbr.initial_window()), (12_000, 12_000));
        bbr.on_mtu_update(1_452);
        assert_eq!(bbr.initial_window(), 14_520);
        let mut config = BbrConfig::default();
        config.initial_window(30_000);
        let mut set = Bbr::new(config, BASE_DATAGRAM_SIZE as u16);
        assert_eq!((set.window(), set.initial_window()), (30_000, 30_000));
        set.on_mtu_update(1_452);
        assert_eq!(set.initial_window(), 30_000);
    }
}
