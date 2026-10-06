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
//!
//! An explicit mark (ECN-CE, RFC 9002 §7.1) is a signal Copa's paper does
//! not answer: a queue manager on the path judged its queue longer than it
//! wants it. It is never random, so Copa answers it as a classic sender
//! answers congestion ([`Copa::on_mark`]), and after a mark past slow start
//! grows as one for ten seconds: the manager keeps the queue short for
//! every sender, and its marks are what Copa sees of the others.
//!
//! It is integer arithmetic throughout, reads the caller's clock, and keeps
//! three samples for each of its four windows.
//!
//! What differs from the law as slates has it, by measurement
//! (`tests/congestion.rs`): slow start judges a doubling by what was sent
//! after the last one, and a round trip moves the window by half of itself
//! at most ([`DEFAULT_STRIDE`]).
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
/// A round trip moves the window by half of itself at most. The velocity
/// of the paper and of slates is bounded by `cwnd·δ` packets, by which one
/// round trip moves the window by all of itself; what the window's change
/// does to the queue is heard of a round trip later, so a window that
/// falls for the queue it built falls to its floor, and one that grows to
/// the path's rate grows past it. Over the paths of `tests/congestion.rs`
/// half is what answers soonest, and carries within a thousandth of the
/// most: at 100 Mbit/s and 100 ms 95% of the path where the whole carries
/// 67%.
pub const DEFAULT_STRIDE: u64 = 2;
/// What a mark multiplies the window by: a classic sender's answer to
/// congestion, which a sender of ECT(0) gives a mark (RFC 3168 §5; RFC 9002
/// §B.2's `kLossReductionFactor`). Of the backoffs the RFCs give (RFC 3168
/// and RFC 9002: 1/2; RFC 9438: 7/10; RFC 8511's experimental β_ecn: 4/5),
/// the gentler ones were measured to leave NewReno and CUBIC under CoDel
/// less than nine tenths of what they carry beside their own kind or CUBIC
/// — at 1 Mbit/s, 100 ms over eight seeds, 7/10 left NewReno 0.84 of it and
/// 4/5 left CUBIC 0.79 (`tests/congestion.rs`,
/// `the_mark_backoff_of_the_law_is_the_one_that_was_measured`).
pub const DEFAULT_MARK_BACKOFF: MarkBackoff = MarkBackoff {
    numerator: 1,
    denominator: 2,
};
/// A fraction the window is multiplied by: `numerator/denominator`, at
/// most one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkBackoff {
    pub numerator: u64,
    pub denominator: u64,
}

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
    /// What part of the window a round trip moves it by at most, as its
    /// inverse.
    stride: u64,
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
    /// When the last mark was answered: a mark of what was sent before
    /// then is part of the round trip it answered (RFC 9002 §7.3.2).
    mark_recovery: Option<u64>,
    /// When a mark last came after slow start: what the window grows as a
    /// classic sender's for (`grows_as_a_classic_sender`).
    marked_after_slow_start: Option<u64>,
    /// Whether a round trip went down since `1/δ` was last raised, or the
    /// competitive mode began: the target held the window back.
    held_back: bool,
    mark_backoff: MarkBackoff,
}
impl Copa {
    pub fn new(datagram: u64, inv_delta: u64) -> Self {
        Self::with_stride(datagram, inv_delta, DEFAULT_STRIDE)
    }
    /// A law whose window a round trip moves by a `stride`th of itself at
    /// most.
    pub fn with_stride(datagram: u64, inv_delta: u64, stride: u64) -> Self {
        Self::with(datagram, inv_delta, stride, DEFAULT_MARK_BACKOFF)
    }
    /// A law of the stride given whose window a mark multiplies by
    /// `mark_backoff`.
    pub fn with(datagram: u64, inv_delta: u64, stride: u64, mark_backoff: MarkBackoff) -> Self {
        let datagram = datagram.max(1);
        let initial = INITIAL_WINDOW_DATAGRAMS.saturating_mul(datagram);
        Self {
            datagram,
            initial,
            window: initial,
            default_inv_delta: inv_delta.max(1),
            inv_delta: inv_delta.max(1),
            stride: stride.max(1),
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
            mark_recovery: None,
            marked_after_slow_start: None,
            held_back: false,
            mark_backoff,
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
    /// How many times the step a packet acknowledged moves the window by.
    pub fn velocity(&self) -> u64 {
        self.velocity
    }
    /// The least round trip of the last ten seconds.
    pub fn min_rtt(&self) -> Option<u64> {
        self.min_rtt.map(|filter| filter.get())
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
            // The queue a doubling builds is seen by what was sent after
            // it, a round trip of sending later and a round trip of
            // acknowledging after that. A doubling judged by what was sent
            // before the last one doubles once more than the path holds:
            // at 100 Mbit/s and 100 ms the window came to 3.07 MB where the
            // path and its queue hold 2.5 MB, the queue overflowed for as
            // long as the transfer lasted, and what was asked beside it
            // took four round trips (`tests/congestion.rs`).
            let sent = now.saturating_sub(rtt);
            match self.last_double {
                None => self.last_double = Some(now),
                Some(then) if sent > then.saturating_add(srtt) => {
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
        // `v/(δ·cwnd)` packets for a packet acknowledged, in bytes; for ten
        // seconds after a mark past slow start, a classic sender's datagram a
        // round trip (RFC 9002 §B.5's `max_datagram_size · acked / cwnd`,
        // `grows_as_a_classic_sender`).
        let gain = if increase && self.grows_as_a_classic_sender(now) {
            1
        } else {
            self.velocity.saturating_mul(self.inv_delta)
        };
        let numerator = u128::from(bytes)
            .saturating_mul(u128::from(self.datagram))
            .saturating_mul(u128::from(gain))
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
        if self.window < window_then {
            self.held_back = true;
        }
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
            .and_then(|packets| packets.checked_div(self.stride))
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
            self.held_back = false;
        }
        // While the window grows as a classic sender's, a datagram a round
        // trip, the target grows only after a round trip it held the window
        // back. Raised a packet a round trip beside it, the target is never
        // reached, the window never falls, and the queue Copa keeps never
        // empties for the mode to see Copa alone: alone at 100 Mbit/s, 20 ms
        // under CoDel, Copa took itself for competing (§2.2's test misjudges
        // Copa's own queue now and then), raised `1/δ` to 58 and filled the
        // queue to CoDel's target, a mark every three to six seconds
        // (2026-10-03, 27 §7). Held back, the window falls by Copa's own
        // step, which empties the queue Copa alone keeps.
        if now.saturating_sub(self.last_increase_update) > srtt
            && now.saturating_sub(self.last_loss_update) > srtt
            && (self.held_back || !self.grows_as_a_classic_sender(now))
        {
            self.inv_delta = self.inv_delta.saturating_add(1);
            self.last_increase_update = now;
            self.held_back = false;
        }
    }
    /// Whether the window grows as a classic sender's does, a datagram a
    /// round trip: within the window Copa keeps its least round trip over
    /// (Copa §2.1, ten seconds) of a mark that came after slow start. A
    /// queue manager keeps the queue short for every sender, so the senders
    /// filling it to the manager's target leave Copa's queue nearly empty
    /// and its competing mode unseen; the marks are what Copa sees of them,
    /// and Copa's own growth, `v/δ` datagrams a round trip, took back after
    /// every mark the share a classic sender regrows a datagram a round trip
    /// — more of it the longer the round trip (F39's measurement, 27 §7).
    /// So in either mode: competing, Copa's own growth after its marks left
    /// NewReno and CUBIC 0.89 of what they carry beside their own kind at
    /// 1 Mbit/s, 100 ms under CoDel. A mark in slow start is Copa's own
    /// doubling past the manager's target, which ending slow start answers.
    /// A window of four round trips (the mode's) let Copa grow by its own
    /// step between the marks of a long path and take from NewReno more than
    /// CUBIC does (2026-10-03).
    fn grows_as_a_classic_sender(&self, now: u64) -> bool {
        self.marked_after_slow_start
            .is_some_and(|answered| now.saturating_sub(answered) <= MIN_RTT_WINDOW_NS)
    }
    /// A loss halves `1/δ` while Copa competes, once a round trip at most;
    /// otherwise it is no signal (§2.2: a loss may be noise, and a mode
    /// judged competing on a lossy path is no proof of a competitor).
    /// Persistent congestion leaves the least window.
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
    /// A mark (ECN-CE) of what was sent at `sent`: a queue manager on the
    /// path judged its queue longer than it wants it. A loss Copa may take
    /// for noise (§2.2), but a mark is never random: it is congestion, and a
    /// sender that marks its datagrams ECN-capable (quinn: ECT(0)) answers
    /// it as a classic sender answers congestion (RFC 3168 §5, RFC 9002
    /// §7.1). A mark of what was sent before the last one was answered is
    /// part of that round trip (RFC 9002 §7.3.2); otherwise slow start ends
    /// (§7.3.1), `1/δ` halves while Copa competes, a direction Up turns Down
    /// at velocity one, and the window is multiplied by the law's backoff
    /// ([`DEFAULT_MARK_BACKOFF`]), never below the least window, the next
    /// round trip's direction judged from there; after a mark past slow
    /// start the window grows as a classic sender's for ten seconds
    /// (`grows_as_a_classic_sender`). Marks round trip after round trip
    /// halve the window each round trip until they stop or the window is
    /// the least: the response to persistent marking.
    pub fn on_mark(&mut self, now: u64, sent: u64) {
        if self.mark_recovery.is_some_and(|answered| sent <= answered) {
            return;
        }
        self.mark_recovery = Some(now);
        // A mark in slow start is Copa's own doubling past the manager's
        // target, which ending slow start answers; one after it says the
        // manager's queue stands above its target while Copa aims at its
        // own short queue — other senders fill it.
        if !self.slow_start {
            self.marked_after_slow_start = Some(now);
        }
        self.slow_start = false;
        if self.competitive {
            // Competing, Copa also halves `1/δ`, its own rule for
            // congestion there.
            self.inv_delta = self
                .inv_delta
                .checked_div(2)
                .unwrap_or(0)
                .max(self.default_inv_delta);
            self.last_loss_update = now;
        }
        if self.direction == Direction::Up && self.velocity > 1 {
            self.direction = Direction::Down;
            self.velocity = 1;
            self.same_direction = 0;
        }
        self.back_off(self.mark_backoff);
        // The next round trip's direction is the law's own, judged from the
        // window the mark left.
        self.direction_mark = Some((now, self.window));
    }
    /// The window multiplied by `backoff`, never above what it was nor
    /// below the least window.
    fn back_off(&mut self, backoff: MarkBackoff) {
        let backed_off = u128::from(self.window)
            .saturating_mul(u128::from(backoff.numerator))
            .checked_div(u128::from(backoff.denominator))
            .and_then(|window| u64::try_from(window).ok())
            .unwrap_or(self.window);
        self.window = backed_off.min(self.window).max(self.minimum_window());
    }
}

/// How Copa is set for a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopaConfig {
    /// `1/δ`: how many packets of queue a sender aims to keep, about.
    pub inv_delta: u64,
    /// What part of the window a round trip moves it by at most, as its
    /// inverse.
    pub stride: u64,
    /// What a mark multiplies the window by.
    pub mark_backoff: MarkBackoff,
}
impl Default for CopaConfig {
    fn default() -> Self {
        Self {
            inv_delta: DEFAULT_INV_DELTA,
            stride: DEFAULT_STRIDE,
            mark_backoff: DEFAULT_MARK_BACKOFF,
        }
    }
}
impl ControllerFactory for CopaConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(CopaController {
            law: Copa::with(
                u64::from(current_mtu),
                self.inv_delta,
                self.stride,
                self.mark_backoff,
            ),
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
    /// quinn raises an explicit mark with no bytes lost and no persistence
    /// (`Connection::process_ecn`), once for each acknowledgement whose
    /// count of marks grew, naming the latest packet it acknowledges as
    /// `sent`; a loss names the bytes it lost.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        let at = self.at(now);
        if lost_bytes == 0 && !is_persistent_congestion {
            self.law.on_mark(at, self.at(sent));
        } else {
            self.law.on_loss(at, self.srtt, is_persistent_congestion);
        }
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
    fn an_empty_queue_doubles_the_window_once_what_was_sent_after_the_last_doubling_is_heard_of() {
        let mut law = Copa::new(DATAGRAM, 2);
        let start = law.window();
        assert_eq!(start, 10 * DATAGRAM);
        // Heard of at 0: what is acknowledged until 200 ms was sent
        // within a round trip of it.
        for step in 0..=20 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
            assert_eq!(law.window(), start, "at {step}");
        }
        ack(&mut law, 210 * MS, 100 * MS, true);
        assert_eq!(law.window(), 2 * start);
        for step in 22..=41 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
            assert_eq!(law.window(), 2 * start, "at {step}");
        }
        ack(&mut law, 420 * MS, 100 * MS, true);
        assert_eq!(law.window(), 4 * start);
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
                    law.on_mark(now, rtt);
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
    #[test]
    fn a_mark_ends_slow_start_and_steps_the_window_down_once_a_round_trip() {
        let mut law = Copa::new(DATAGRAM, 2);
        for step in 0..=25 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert!(law.in_slow_start());
        let before = law.window();
        // The backoff of a mark: half the window.
        law.on_mark(300 * MS, 250 * MS);
        assert!(!law.in_slow_start());
        assert_eq!(law.window(), before / 2);
        // A mark of what was sent before that answer is the same round trip.
        law.on_mark(320 * MS, 300 * MS);
        assert_eq!(law.window(), before / 2);
        // One of what was sent after it is the next.
        law.on_mark(420 * MS, 310 * MS);
        assert_eq!(law.window(), before / 4);
        // Never below the least window.
        for round in 0..100 {
            law.on_mark(500 * MS + round * 100 * MS, 450 * MS + round * 100 * MS);
            assert!(law.window() >= 2 * DATAGRAM);
        }
        assert_eq!(law.window(), 2 * DATAGRAM);
    }
    #[test]
    fn a_mark_while_competing_halves_the_target_and_the_window_as_a_classic_sender_does() {
        let mut law = Copa::new(DATAGRAM, 2);
        ack(&mut law, 0, 100 * MS, true);
        for step in 1..=200 {
            let rtt = if step % 2 == 0 { 180 * MS } else { 220 * MS };
            ack(&mut law, 500 * MS + step * 10 * MS, rtt, true);
        }
        assert!(law.competitive());
        let raised = law.inv_delta();
        assert!(raised > 2, "{raised}");
        let window = law.window();
        law.on_mark(3_000 * MS, 2_900 * MS);
        assert_eq!(law.inv_delta(), (raised / 2).max(2));
        // Copa halves its window as a classic sender does for a mark (RFC
        // 9002 §B.2).
        assert_eq!(law.window(), (window / 2).max(2 * DATAGRAM));
        // The same round trip says nothing more.
        law.on_mark(3_010 * MS, 2_950 * MS);
        assert_eq!(law.inv_delta(), (raised / 2).max(2));
        assert_eq!(law.window(), (window / 2).max(2 * DATAGRAM));
    }
    #[test]
    fn marks_held_round_trip_after_round_trip_take_the_window_down_by_the_backoff_to_the_least() {
        // A law whose queue empties (the default mode), marked once each
        // round trip: the declared response to persistent marking halves
        // the window each round trip, to the least window.
        let mut law = Copa::new(DATAGRAM, 2);
        for step in 0..=40 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert!(!law.competitive());
        let mut window = law.window();
        for round in 0..40_u64 {
            let begins = 1_000 * MS + round * 100 * MS;
            law.on_mark(begins + 99 * MS, begins);
            window = (window / 2).max(2 * DATAGRAM);
            assert_eq!(law.window(), window, "round {round}");
        }
        assert_eq!(law.window(), 2 * DATAGRAM);
        assert!(!law.in_slow_start() && !law.competitive());
    }
    #[test]
    fn after_a_mark_past_slow_start_the_window_grows_a_datagram_a_round_trip_for_ten_seconds() {
        // What a round trip of acknowledgements, the window's bytes from
        // `from`, grows the window by.
        fn round_trip(law: &mut Copa, from: u64) -> u64 {
            let before = law.window();
            let whole = before / DATAGRAM;
            for step in 0..whole {
                ack(law, from + step * MS, 100 * MS, true);
            }
            law.on_ack(Acked {
                now: from + whole * MS,
                rtt: 100 * MS,
                srtt: 100 * MS,
                bytes: before % DATAGRAM,
                window_limited: true,
            });
            law.window() - before
        }
        let mut law = Copa::new(DATAGRAM, 2);
        for step in 0..=80 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert_eq!(law.window(), 80 * DATAGRAM);
        // A mark in slow start is Copa's own doubling past the manager's
        // target: slow start ends and Copa grows by its own step after it,
        // `v/δ` datagrams a round trip, more than a classic sender's one.
        law.on_mark(900 * MS, 850 * MS);
        assert!(!law.in_slow_start());
        let grown = round_trip(&mut law, 1_000 * MS);
        assert!(grown > DATAGRAM + DATAGRAM / 10, "Copa's own step: {grown}");
        // One after slow start: for ten seconds a datagram a round trip.
        law.on_mark(1_100 * MS, 1_050 * MS);
        let grown = round_trip(&mut law, 1_200 * MS);
        assert!(
            (DATAGRAM - DATAGRAM / 10..=DATAGRAM + DATAGRAM / 10).contains(&grown),
            "a datagram a round trip: {grown}"
        );
        let grown = round_trip(&mut law, 11_000 * MS);
        assert!(
            (DATAGRAM - DATAGRAM / 10..=DATAGRAM + DATAGRAM / 10).contains(&grown),
            "a datagram a round trip: {grown}"
        );
        // Past ten seconds from it, Copa's own step again.
        let grown = round_trip(&mut law, 11_200 * MS);
        assert!(grown > DATAGRAM + DATAGRAM / 10, "Copa's own step: {grown}");
    }
    #[test]
    fn competing_as_a_classic_sender_copa_raises_its_target_only_after_a_round_trip_held_back() {
        let mut law = Copa::new(DATAGRAM, 2);
        ack(&mut law, 0, 100 * MS, true);
        // A mark in slow start ends it, and begins no classic growth.
        law.on_mark(10 * MS, 5 * MS);
        // A queue a millisecond or three over the least that never nearly
        // empties: Copa competes, and raises `1/δ` a round trip.
        let low = |step: u64| {
            if step.is_multiple_of(2) {
                101 * MS
            } else {
                103 * MS
            }
        };
        for step in 2..=150 {
            ack(&mut law, step * 10 * MS, low(step), true);
        }
        assert!(law.competitive());
        let raised = law.inv_delta();
        assert!(raised > 4, "{raised}");
        // A mark past slow start: `1/δ` halves, and for ten seconds the
        // window grows as a classic sender's.
        law.on_mark(1_510 * MS, 1_505 * MS);
        let halved = law.inv_delta();
        assert_eq!(halved, (raised / 2).max(2));
        // Below its target the window grows round trip after round trip and
        // the target stays: raised a packet a round trip beside a window
        // that grows a datagram a round trip, it would never be reached, and
        // the queue never empty (F39, 27 §7). A queue at most ten
        // milliseconds over the least keeps the mode competing as the
        // samples rise.
        for step in 152..=300 {
            let rtt = match step {
                ..=260 => low(step),
                _ if step.is_multiple_of(2) => 109 * MS,
                _ => 110 * MS,
            };
            ack(&mut law, step * 10 * MS, rtt, true);
            assert!(law.competitive(), "{step}");
            assert_eq!(law.inv_delta(), halved, "{step}");
        }
        // The queue rises past what the target allows: a round trip goes
        // down, and the round trip after it `1/δ` rises by one.
        let before = law.window();
        for step in 301..=360_u64 {
            let rtt = if step.is_multiple_of(2) {
                170 * MS
            } else {
                180 * MS
            };
            ack(&mut law, step * 10 * MS, rtt, true);
            assert!(law.competitive(), "{step}");
            if law.inv_delta() > halved {
                assert!(law.window() < before, "{} {before}", law.window());
                assert_eq!(law.inv_delta(), halved + 1);
                return;
            }
        }
        panic!("held back, and `1/δ` stayed {}", law.inv_delta());
    }
    #[test]
    fn the_controller_takes_an_event_without_lost_bytes_for_a_mark() {
        let began = Instant::now();
        let mut controller = Arc::new(CopaConfig::default()).build(began, 1_200);
        let window = controller.window();
        let later = began + Duration::from_millis(100);
        controller.on_congestion_event(later, began, false, 0);
        assert_eq!(controller.window(), window / 2);
        // The same round trip again: nothing more.
        controller.on_congestion_event(later + Duration::from_millis(1), began, false, 0);
        assert_eq!(controller.window(), window / 2);
        // A loss in the default mode is still no signal.
        controller.on_congestion_event(later + Duration::from_millis(200), later, false, 1_200);
        assert_eq!(controller.window(), window / 2);
        let law = controller.into_any().downcast::<CopaController>().unwrap();
        assert!(!law.law().in_slow_start());
    }
}
