//! Copa as a congestion controller for quinn (27 §3.1 P10, stage G).
//!
//! Copa (Arun and Balakrishnan, NSDI 2018) aims at the rate `1/(δ·d_q)`,
//! where `d_q` is the queueing delay it measures: the least round trip of
//! the last half smoothed round trip (`RTTstanding`) less the least of the
//! last ten seconds (`RTTmin`). Below that rate the window grows, above it
//! it shrinks, by `v/(δ·cwnd)` packets for each packet acknowledged; the
//! velocity `v` doubles once the window has moved one way for three round
//! trips. A loss is no signal by itself. When the queue never nearly
//! empties over four round trips another sender is filling it, and Copa
//! competes: `1/δ` grows by one each round trip without loss and halves on
//! a loss, until the queue empties again.
//!
//! The law is slates' (`crates/transport/src/congestion/copa.rs`), which
//! fixed its details from the paper, the authors' reference implementation
//! and mvfst, and found by measurement that RFC 9002 §7.8 must bound only
//! the window's growth: a window the sender does not fill still shrinks.
//! It is integer arithmetic throughout, reads the caller's clock, and keeps
//! three samples for each of its four windows.
//!
//! What differs under quinn: quinn paces by its own rule, at five quarters
//! of the window per smoothed round trip, where Copa would pace at twice
//! the window per `RTTstanding`; the window is Copa's and the pacing
//! quinn's. Whether Copa is what focal's connections run is decided by
//! measurement (`tests/congestion.rs`), not here.
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};
use std::{
    any::Any,
    sync::Arc,
    time::{Duration, Instant},
};

/// RFC 9002 §7.2: the window a connection begins with, in datagrams.
const INITIAL_WINDOW_DATAGRAMS: u64 = 10;
/// RFC 9002 §7.2: the least window, in datagrams.
const MINIMUM_WINDOW_DATAGRAMS: u64 = 2;
/// Copa §2.1: the window of the least round trip, 10 s.
const MIN_RTT_WINDOW_NS: u64 = 10_000_000_000;
/// Copa §2.2: the window the mode is judged over, in smoothed round trips.
const MODE_WINDOW_SRTTS: u64 = 4;
/// Copa §2.2: nearly empty is within a tenth of the spread above the least.
const NEARLY_EMPTY_FRACTION: u64 = 10;
/// Copa §2.1: the velocity doubles once a direction held this often.
const VELOCITY_DIRECTION_THRESHOLD: u32 = 3;
/// Copa §4.3: δ = 1/2, as `1/δ`.
pub const DEFAULT_INV_DELTA: u64 = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Sample {
    time: u64,
    value: u64,
}
/// The greatest of the samples of the last `window` of the caller's clock,
/// in three samples (Nichols' filter, as Linux `lib/win_minmax.c`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WindowedMax {
    best: Sample,
    second: Sample,
    third: Sample,
}
impl WindowedMax {
    fn new(time: u64, value: u64) -> Self {
        let sample = Sample { time, value };
        Self {
            best: sample,
            second: sample,
            third: sample,
        }
    }
    fn get(&self) -> u64 {
        self.best.value
    }
    fn update(&mut self, time: u64, window: u64, value: u64) -> u64 {
        let sample = Sample { time, value };
        if value >= self.best.value || time.saturating_sub(self.third.time) > window {
            *self = Self::new(time, value);
            return value;
        }
        if value >= self.second.value {
            self.second = sample;
            self.third = sample;
        } else if value >= self.third.value {
            self.third = sample;
        }
        let elapsed = time.saturating_sub(self.best.time);
        if elapsed > window {
            // The best has aged out: the next two move up.
            self.best = self.second;
            self.second = self.third;
            self.third = sample;
            if time.saturating_sub(self.best.time) > window {
                self.best = self.second;
                self.second = self.third;
                self.third = sample;
            }
        } else if self.second.time == self.best.time && elapsed > window.checked_div(4).unwrap_or(0)
        {
            self.second = sample;
            self.third = sample;
        } else if self.third.time == self.second.time
            && elapsed > window.checked_div(2).unwrap_or(0)
        {
            self.third = sample;
        }
        self.best.value
    }
}
/// The least of the samples of the last `window`: the same filter over the
/// order turned round.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WindowedMin(WindowedMax);
impl WindowedMin {
    fn new(time: u64, value: u64) -> Self {
        Self(WindowedMax::new(time, u64::MAX.saturating_sub(value)))
    }
    fn get(&self) -> u64 {
        u64::MAX.saturating_sub(self.0.get())
    }
    fn update(&mut self, time: u64, window: u64, value: u64) -> u64 {
        u64::MAX.saturating_sub(self.0.update(time, window, u64::MAX.saturating_sub(value)))
    }
}
fn least(filter: &mut Option<WindowedMin>, now: u64, window: u64, value: u64) -> u64 {
    match filter {
        Some(filter) => filter.update(now, window, value),
        None => {
            *filter = Some(WindowedMin::new(now, value));
            value
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Up,
    Down,
}

/// One acknowledged packet, as the law sees it. Times are nanoseconds of
/// the caller's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Acked {
    pub now: u64,
    /// From the packet's sending to now.
    pub rtt: u64,
    /// The smoothed round trip.
    pub srtt: u64,
    pub bytes: u64,
    /// The sender had nothing more to send than its window let it.
    pub window_limited: bool,
}

/// The law.
#[derive(Clone, Debug)]
pub struct Copa {
    datagram: u64,
    initial: u64,
    window: u64,
    default_inv_delta: u64,
    inv_delta: u64,
    competitive: bool,
    slow_start: bool,
    last_double: Option<u64>,
    min_rtt: Option<WindowedMin>,
    standing_rtt: Option<WindowedMin>,
    mode_min: Option<WindowedMin>,
    mode_max: Option<WindowedMax>,
    velocity: u64,
    direction: Direction,
    same_direction: u32,
    /// When the direction was last judged, and the window then.
    direction_mark: Option<(u64, u64)>,
    /// What of a step was less than a byte, kept for the next.
    step_remainder: u64,
    last_loss_update: u64,
    last_increase_update: u64,
}
impl Copa {
    pub fn new(datagram: u64, inv_delta: u64) -> Self {
        let datagram = datagram.max(1);
        let initial = INITIAL_WINDOW_DATAGRAMS.saturating_mul(datagram);
        Self {
            datagram,
            initial,
            window: initial,
            default_inv_delta: inv_delta.max(1),
            inv_delta: inv_delta.max(1),
            competitive: false,
            slow_start: true,
            last_double: None,
            min_rtt: None,
            standing_rtt: None,
            mode_min: None,
            mode_max: None,
            velocity: 1,
            direction: Direction::Up,
            same_direction: 0,
            direction_mark: None,
            step_remainder: 0,
            last_loss_update: 0,
            last_increase_update: 0,
        }
    }
    pub fn window(&self) -> u64 {
        self.window
    }
    pub fn initial_window(&self) -> u64 {
        self.initial
    }
    pub fn in_slow_start(&self) -> bool {
        self.slow_start
    }
    pub fn competitive(&self) -> bool {
        self.competitive
    }
    pub fn inv_delta(&self) -> u64 {
        self.inv_delta
    }
    /// The least round trip of the last half smoothed round trip.
    pub fn standing_rtt(&self) -> Option<u64> {
        self.standing_rtt.map(|filter| filter.get())
    }
    fn minimum_window(&self) -> u64 {
        MINIMUM_WINDOW_DATAGRAMS.saturating_mul(self.datagram)
    }
    /// The path takes datagrams of another size (RFC 9002 §7.2): the
    /// window stays in bytes, and is the least window at that size at
    /// least.
    pub fn set_datagram(&mut self, datagram: u64) {
        self.datagram = datagram.max(1);
        self.window = self.window.max(self.minimum_window());
    }
    pub fn on_ack(&mut self, acked: Acked) {
        let Acked {
            now,
            rtt,
            bytes,
            window_limited,
            ..
        } = acked;
        let srtt = acked.srtt.max(1);
        let rtt_min = least(&mut self.min_rtt, now, MIN_RTT_WINDOW_NS, rtt);
        let standing = least(
            &mut self.standing_rtt,
            now,
            srtt.checked_div(2).unwrap_or(0),
            rtt,
        );
        let mode_window = srtt.saturating_mul(MODE_WINDOW_SRTTS);
        let recent_min = least(&mut self.mode_min, now, mode_window, rtt);
        let recent_max = match &mut self.mode_max {
            Some(filter) => filter.update(now, mode_window, rtt),
            None => {
                self.mode_max = Some(WindowedMax::new(now, rtt));
                rtt
            }
        };
        self.update_mode(now, srtt, rtt_min, recent_min, recent_max);
        let queueing = standing.saturating_sub(rtt_min);
        // The rate `cwnd/RTTstanding` is at or below the target
        // `1/(δ·d_q)`, in bytes: `cwnd·d_q <= (1/δ)·datagram·RTTstanding`.
        // The products of three 64-bit values fit 192 bits and not 128:
        // they are compared as quotients where they would not fit.
        let increase = queueing == 0 || {
            let held = u128::from(self.window).saturating_mul(u128::from(queueing));
            let target = u128::from(self.inv_delta)
                .saturating_mul(u128::from(self.datagram))
                .saturating_mul(u128::from(standing));
            held <= target
        };
        if increase && !window_limited {
            // RFC 9002 §7.8: a window that is not used does not grow. It
            // still shrinks (slates, 2026-09-28).
            return;
        }
        if self.slow_start && increase {
            match self.last_double {
                None => self.last_double = Some(now),
                Some(then) if now.saturating_sub(then) > srtt => {
                    self.window = self.window.saturating_mul(2);
                    self.last_double = Some(now);
                }
                Some(_) => {}
            }
            return;
        }
        self.update_direction(now, srtt);
        let wanted = if increase {
            Direction::Up
        } else {
            Direction::Down
        };
        if wanted != self.direction && self.velocity > 1 {
            self.direction = wanted;
            self.velocity = 1;
            self.same_direction = 0;
            self.direction_mark = Some((now, self.window));
        }
        // `v/(δ·cwnd)` packets for a packet acknowledged, in bytes.
        let numerator = u128::from(bytes)
            .saturating_mul(u128::from(self.datagram))
            .saturating_mul(u128::from(self.velocity))
            .saturating_mul(u128::from(self.inv_delta))
            .saturating_add(u128::from(self.step_remainder));
        let denominator = u128::from(self.window.max(1));
        let step = numerator
            .checked_div(denominator)
            .and_then(|step| u64::try_from(step).ok())
            .unwrap_or(u64::MAX);
        self.step_remainder = numerator
            .checked_rem(denominator)
            .and_then(|rest| u64::try_from(rest).ok())
            .unwrap_or(0);
        if increase {
            self.window = self.window.saturating_add(step);
        } else {
            self.slow_start = false;
            self.window = self.window.saturating_sub(step).max(self.minimum_window());
        }
    }
    /// Once a smoothed round trip: a direction held three times doubles the
    /// velocity and a change makes it one; it is `cwnd·δ` packets at most.
    fn update_direction(&mut self, now: u64, srtt: u64) {
        let Some((then, window_then)) = self.direction_mark else {
            self.direction_mark = Some((now, self.window));
            return;
        };
        if now.saturating_sub(then) < srtt {
            return;
        }
        let direction = if self.window > window_then {
            Direction::Up
        } else {
            Direction::Down
        };
        if direction == self.direction {
            self.same_direction = self.same_direction.saturating_add(1);
            if self.same_direction >= VELOCITY_DIRECTION_THRESHOLD {
                self.velocity = self.velocity.saturating_mul(2);
            }
        } else {
            self.velocity = 1;
            self.same_direction = 0;
        }
        let cap = self
            .window
            .checked_div(self.datagram)
            .and_then(|packets| packets.checked_div(self.inv_delta))
            .unwrap_or(1)
            .max(1);
        self.velocity = self.velocity.min(cap);
        self.direction = direction;
        self.direction_mark = Some((now, self.window));
    }
    fn update_mode(&mut self, now: u64, srtt: u64, rtt_min: u64, recent_min: u64, recent_max: u64) {
        let spread = recent_max.saturating_sub(rtt_min);
        let nearly_empty = recent_min == rtt_min
            || recent_min
                < rtt_min.saturating_add(spread.checked_div(NEARLY_EMPTY_FRACTION).unwrap_or(0));
        if nearly_empty {
            self.competitive = false;
            self.inv_delta = self.default_inv_delta;
            return;
        }
        if !self.competitive {
            self.competitive = true;
            self.last_increase_update = now;
        }
        if now.saturating_sub(self.last_increase_update) > srtt
            && now.saturating_sub(self.last_loss_update) > srtt
        {
            self.inv_delta = self.inv_delta.saturating_add(1);
            self.last_increase_update = now;
        }
    }
    /// A loss halves `1/δ` while Copa competes, once a round trip at most;
    /// otherwise it is no signal. Persistent congestion leaves the least
    /// window.
    pub fn on_loss(&mut self, now: u64, srtt: u64, persistent: bool) {
        if self.competitive && now.saturating_sub(self.last_loss_update) > srtt {
            self.inv_delta = self
                .inv_delta
                .checked_div(2)
                .unwrap_or(0)
                .max(self.default_inv_delta);
            self.last_loss_update = now;
        }
        if persistent {
            self.window = self.minimum_window();
            self.slow_start = false;
        }
    }
}

/// How Copa is set for a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopaConfig {
    /// `1/δ`: how many packets of queue a sender aims to keep, about.
    pub inv_delta: u64,
}
impl Default for CopaConfig {
    fn default() -> Self {
        Self {
            inv_delta: DEFAULT_INV_DELTA,
        }
    }
}
impl ControllerFactory for CopaConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(CopaController {
            law: Copa::new(u64::from(current_mtu), self.inv_delta),
            began: now,
            srtt: 0,
        })
    }
}

/// Copa under quinn's clock.
#[derive(Clone, Debug)]
pub struct CopaController {
    law: Copa,
    /// What the law's clock counts from.
    began: Instant,
    /// The smoothed round trip at the last acknowledgement, for a loss.
    srtt: u64,
}
impl CopaController {
    pub fn law(&self) -> &Copa {
        &self.law
    }
    fn at(&self, now: Instant) -> u64 {
        nanos(now.saturating_duration_since(self.began))
    }
}
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
impl Controller for CopaController {
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.srtt = nanos(rtt.get());
        let acked = Acked {
            now: self.at(now),
            rtt: nanos(now.saturating_duration_since(sent)),
            srtt: self.srtt,
            bytes,
            window_limited: !app_limited,
        };
        self.law.on_ack(acked);
    }
    fn on_congestion_event(
        &mut self,
        now: Instant,
        _sent: Instant,
        is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {
        let now = self.at(now);
        self.law.on_loss(now, self.srtt, is_persistent_congestion);
    }
    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.law.set_datagram(u64::from(new_mtu));
    }
    fn window(&self) -> u64 {
        self.law.window()
    }
    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.law.window();
        metrics
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn initial_window(&self) -> u64 {
        self.law.initial_window()
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATAGRAM: u64 = 1_000;
    const MS: u64 = 1_000_000;

    fn ack(law: &mut Copa, now: u64, rtt: u64, window_limited: bool) {
        law.on_ack(Acked {
            now,
            rtt,
            srtt: 100 * MS,
            bytes: DATAGRAM,
            window_limited,
        });
    }

    #[test]
    fn an_empty_queue_doubles_the_window_each_round_trip_of_slow_start() {
        let mut law = Copa::new(DATAGRAM, 2);
        let start = law.window();
        assert_eq!(start, 10 * DATAGRAM);
        for step in 0..=21 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert_eq!(law.window(), 2 * start);
        assert!(law.in_slow_start());
    }
    #[test]
    fn a_queue_past_the_target_shrinks_the_window() {
        let mut law = Copa::new(DATAGRAM, 2);
        ack(&mut law, 0, 100 * MS, true);
        let before = law.window();
        // 100 ms of queue at ten packets: the target is 20 packets a
        // second, and ten packets in 200 ms are 50.
        for step in 1..=5 {
            ack(&mut law, 100 * MS + step * MS, 200 * MS, true);
        }
        assert!(!law.in_slow_start());
        assert!(law.window() < before);
        assert_eq!(law.standing_rtt(), Some(200 * MS));
    }
    #[test]
    fn a_window_the_sender_does_not_fill_shrinks_and_never_grows() {
        let mut law = Copa::new(DATAGRAM, 2);
        ack(&mut law, 0, 100 * MS, false);
        let before = law.window();
        for step in 1..=5 {
            ack(&mut law, 100 * MS + step * MS, 200 * MS, false);
        }
        assert!(law.window() < before);
        let shrunk = law.window();
        for step in 1..=50 {
            ack(&mut law, 10_000 * MS + step * MS, 100 * MS, false);
        }
        assert!(law.window() <= shrunk);
    }
    #[test]
    fn a_queue_that_never_empties_is_competed_for() {
        let mut law = Copa::new(DATAGRAM, 2);
        ack(&mut law, 0, 100 * MS, true);
        for step in 1..=200 {
            let rtt = if step % 2 == 0 { 180 * MS } else { 220 * MS };
            ack(&mut law, 500 * MS + step * 10 * MS, rtt, true);
        }
        assert!(law.competitive());
        let raised = law.inv_delta();
        assert!(raised > 2, "{raised}");
        law.on_loss(3_000 * MS, 100 * MS, false);
        assert_eq!(law.inv_delta(), (raised / 2).max(2));
        // Within the same round trip a second loss says nothing more.
        law.on_loss(3_001 * MS, 100 * MS, false);
        assert_eq!(law.inv_delta(), (raised / 2).max(2));
        // The queue empties: the default again.
        for step in 1..=100 {
            ack(&mut law, 4_000 * MS + step * 10 * MS, 100 * MS, true);
        }
        assert!(!law.competitive());
        assert_eq!(law.inv_delta(), 2);
    }
    #[test]
    fn a_loss_is_no_signal_unless_it_persists() {
        let mut law = Copa::new(DATAGRAM, 2);
        for step in 0..=50 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        let window = law.window();
        law.on_loss(600 * MS, 100 * MS, false);
        assert_eq!(law.window(), window);
        law.on_loss(900 * MS, 100 * MS, true);
        assert_eq!(law.window(), 2 * DATAGRAM);
        assert!(!law.in_slow_start());
    }
    #[test]
    fn the_window_is_never_below_two_datagrams_and_follows_their_size() {
        let mut law = Copa::new(DATAGRAM, 2);
        ack(&mut law, 0, 100 * MS, true);
        // 4.9 s of queue: the target is two packets and a little.
        for step in 1..=90 {
            ack(&mut law, 100 * MS + step * MS, 5_000 * MS, true);
            assert!(law.window() >= 2 * DATAGRAM);
        }
        assert!(law.window() < 3 * DATAGRAM, "{}", law.window());
        law.on_loss(200 * MS, 100 * MS, true);
        assert_eq!(law.window(), 2 * DATAGRAM);
        law.set_datagram(1_400);
        assert_eq!(law.window(), 2_800);
        law.set_datagram(0);
        assert_eq!(law.window(), 2_800);
    }
    #[test]
    fn nothing_overflows_at_the_bounds_of_what_it_is_told() {
        for (datagram, inv_delta) in [(0, 0), (u64::MAX, u64::MAX), (1, u64::MAX), (u64::MAX, 1)] {
            let mut law = Copa::new(datagram, inv_delta);
            for (now, rtt, srtt, bytes) in [
                (0, 0, 0, 0),
                (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
                (1, u64::MAX, 1, u64::MAX),
                (u64::MAX, 1, u64::MAX, 1),
                (u64::MAX, 0, 1, u64::MAX),
            ] {
                for window_limited in [true, false] {
                    law.on_ack(Acked {
                        now,
                        rtt,
                        srtt,
                        bytes,
                        window_limited,
                    });
                    law.on_loss(now, srtt, window_limited);
                    assert!(law.window() >= 2);
                }
            }
        }
    }
    #[test]
    fn the_filters_forget_what_has_aged_out_of_their_window() {
        let mut most = WindowedMax::new(0, 10);
        assert_eq!(most.update(1, 100, 5), 10);
        assert_eq!(most.update(50, 100, 7), 10);
        assert_eq!(most.update(90, 100, 6), 10);
        // The best is a hundred old and more: the next best stands.
        assert_eq!(most.update(101, 100, 1), 7);
        assert_eq!(most.update(151, 100, 1), 6);
        assert_eq!(most.update(400, 100, 2), 2);
        assert_eq!(most.update(401, 100, 9), 9);
        let mut least = WindowedMin::new(0, 10);
        assert_eq!(least.update(1, 100, 50), 10);
        assert_eq!(least.update(60, 100, 20), 10);
        assert_eq!(least.update(101, 100, 90), 20);
        assert_eq!(least.update(102, 100, 3), 3);
        assert_eq!(least.get(), 3);
        // A window of nothing keeps the last sample alone.
        let mut none = WindowedMax::new(0, 10);
        assert_eq!(none.update(1, 0, 4), 4);
    }
    #[test]
    fn the_controller_counts_from_when_it_was_built() {
        let began = Instant::now();
        let mut controller = Arc::new(CopaConfig::default()).build(began, 1_200);
        assert_eq!(controller.window(), 12_000);
        assert_eq!(controller.initial_window(), 12_000);
        controller.on_mtu_update(1_400);
        assert_eq!(controller.window(), 12_000);
        // A clock behind the beginning counts as the beginning.
        controller.on_congestion_event(began, began, true, 1_200);
        assert_eq!(controller.window(), 2_800);
        assert_eq!(controller.metrics().congestion_window, 2_800);
        let copy = controller.clone_box();
        assert_eq!(copy.window(), 2_800);
        let law = controller.into_any().downcast::<CopaController>().unwrap();
        assert!(!law.law().in_slow_start());
    }
}
