//! Copa (Arun and Balakrishnan, "Copa: Practical Delay-Based Congestion Control for the Internet",
//! NSDI 2018), with the changes slates' and focal's measurements made to it and a test of whose the
//! queue is (docs/transport.md §4d).
//!
//! Copa aims at the rate `1/(δ·d_q)` packets a second, where `d_q` is the queueing delay it measures:
//! the least round trip of the last half smoothed round trip (`RTTstanding`) less the least of the last
//! ten seconds (`RTTmin`). Below that rate the window grows and above it the window shrinks, by
//! `v/(δ·cwnd)` packets for each packet acknowledged; the velocity `v` doubles once the window has moved
//! one way for three round trips (§2.1). Alone, the queue cycles from empty to about `2.5/δ` packets and
//! back every five round trips (§3). A loss is no signal by itself: it may be noise.
//!
//! When the queue has not nearly emptied over five round trips ([`MODE_WINDOW_SRTTS`]), the paper
//! takes another sender to be filling it (§2.2). Here Copa first looks: it cuts its window to its
//! share of the path, `r·RTTmin` for its delivery rate `r`, which withdraws its own bytes in the queue
//! (`r·d_q`, Little's law). Alone, the packets sent under the cut find the queue empty, and Copa keeps
//! the default mode with its own excess dropped; beside another sender, its bytes stay, and Copa
//! competes. Competing, the window grows as NewReno's and halves on a loss or a mark once a recovery
//! period, and Copa cuts again every [`CUT_INTERVAL_SRTTS`] round trips: the mode ends on a cut that
//! finds the queue empty, never on the one empty moment a competitor's backoff leaves
//! (docs/research/congestion.md, "Whose queue is it").
//!
//! From slates' law, which fixed its details from the paper, the authors' implementation (genericCC)
//! and mvfst: integer arithmetic throughout, Nichols' windowed filters, and RFC 9002 §7.8 bounding only
//! the window's growth, so a window the sender does not fill still shrinks.
//!
//! From focal's measurements (focal's record F39):
//! - slow start judges a doubling by what was sent after the last one;
//! - a round trip moves the window by half of itself at most ([`CopaConfig::stride`]);
//! - an explicit congestion mark (ECN-CE) is answered as a classic sender answers congestion, and for
//!   ten seconds after a mark past slow start the window grows as a classic sender's does.
//!
//! From focal's derivations (focal b18) and the harness's: the mode is judged over the five round
//! trips of Copa's cycle (A1); a delay sample is judged by the window its packet was sent under (A2);
//! and a queue of a datagram is nearly empty (G).
//!
//! The law states its pacing, `2·cwnd/RTTstanding` (§2.1), and the connection's pacer sends at it
//! (`Controller::pacing_rate`): the paper's analysis of Copa's own cycle (§3) assumes it, and paced
//! at the connection's own five quarters of the window a smoothed round trip, Copa alone emptied its
//! queue every five round trips on one of focal's five paths.
//!
//! Measured over focal's grids by the rule fixed before the runs, the law is admissible: no stall,
//! ECN kept, CoDel marking Copa, every incumbent at nine tenths of its bar or more with a manager or
//! without, Copa alone never competing and keeping a shorter queue than NewReno's, and no competing
//! after the competitor leaves (docs/benchmarks.md, "Copa's competing mode over focal's grids").

use std::any::Any;

use super::{Controller, ControllerMetrics};
use crate::connection::RttEstimator;
use crate::{Duration, Instant};

/// RFC 9002 §7.2: the initial window is ten datagrams...
const INITIAL_WINDOW_DATAGRAMS: u64 = 10;
/// ...limited to the larger of 14,720 bytes and two datagrams.
const INITIAL_WINDOW_LIMIT: u64 = 14_720;
/// RFC 9002 §7.2: the least window, in datagrams.
const MINIMUM_WINDOW_DATAGRAMS: u64 = 2;
/// Copa §2.1: `RTTmin` is the least round trip of the last ten seconds.
const MIN_RTT_WINDOW_NS: u64 = 10_000_000_000;
/// Copa §2.2: the queue must have been nearly empty "in the last 5 RTTs", the period of Copa's own
/// oscillation (§3: the queue oscillates "between having 0 and 2.5/δ̂ packets every five RTTs"). The
/// paper measures `RTTmax`, which scales "nearly", over four, and genericCC (`rtt-window.cc`), slates
/// and focal took four for both. A window shorter than the cycle can miss its trough or its peak:
/// both windows cover the cycle here (focal's A1).
const MODE_WINDOW_SRTTS: u64 = 5;
/// Copa §2.2: the queue is nearly empty when the least round trip of the mode's window is within a
/// tenth of that window's spread above `RTTmin`.
const NEARLY_EMPTY_FRACTION: u64 = 10;
/// How many samples of its window the law keeps...
const WINDOW_SAMPLES: usize = 32;
/// ...and how far apart at least, as a part of the smoothed round trip. Thirty-two samples a sixteenth
/// of a round trip apart reach two round trips back, past the acknowledged packet's sending, and the
/// sample in force then is within a sixteenth of a round trip's movement of the window then.
const WINDOW_SAMPLE_SPACING: u64 = 16;
/// What the cut withdraws beyond Copa's own bytes in the queue, in datagrams: a delivery rate counted
/// in whole packets over an interval is off by up to a datagram at the interval's end, and the cut,
/// that rate times `RTTmin` (no longer than the interval), by up to a datagram; and a queue of one
/// datagram is the packet in service (docs/research/congestion.md, "Whose queue is it").
const CUT_MARGIN_DATAGRAMS: u64 = 2;
/// How many datagrams' time at Copa's rate the least round trip under a cut may stand above `RTTmin`
/// for a queue that was Copa's alone: one for the least round trip being a smaller packet's, up to a
/// datagram's time on the link sooner (at 1 Mbit/s and 20 ms the least was 22.9 ms where a full
/// datagram's was 30.0 ms), and one for the second of two packets in flight waiting out the first's.
const CUT_QUANTA_DATAGRAMS: u64 = 2;
/// The least window a cut leaves, in datagrams. A receiver acknowledges at once the second of two
/// ack-eliciting packets and holds a lone one up to its `max_ack_delay` (RFC 9000 §13.2.2, 25 ms by
/// default, §18.2), and the path sends only while its bytes in flight and the next datagram stay
/// under the window: at the least window, two datagrams, one packet is in flight, every sample under
/// the cut waits out the peer's delay, and Copa alone at 1 Mbit/s and 20 ms read its empty queue as
/// 25 ms standing. Three keep two in flight.
const CUT_FLOOR_DATAGRAMS: u64 = 3;
/// Competing, Copa cuts once this many smoothed round trips, to see whether its competitor has gone.
/// A cut costs Copa at most its own share of the queue for about two round trips, so a longer
/// interval keeps more of what Copa competes for, and a competitor gone is seen later. Measured over
/// focal's grids by the rule of `docs/benchmarks.md` ("Copa's competing mode over focal's grids"):
/// at 10 CoDel marked none of Copa's datagrams in one run; 20 and 40 met the rule, 40 carrying more
/// (35.5% against 31.8%); at 80 Copa still competed in the last quarter after its competitor left in
/// four runs at 100 ms.
const CUT_INTERVAL_SRTTS: u64 = 40;
/// Copa §2.1: the velocity doubles once the window has moved one way this many round trips running.
const VELOCITY_DIRECTION_THRESHOLD: u32 = 3;
/// Copa §2.2: the default mode's δ = 0.5, held as `1/δ`.
const DEFAULT_INV_DELTA: u64 = 2;
/// A round trip moves the window by half of itself at most. The velocity of the paper and of slates is
/// bounded by `cwnd·δ` packets, by which one round trip moves the window by all of itself; what the
/// window's change does to the queue is heard of a round trip later, so a window that falls for the
/// queue it built falls to its floor, and one that grows to the path's rate grows past it. focal
/// measured half to answer soonest over its paths, and to carry within a thousandth of the most: at
/// 100 Mbit/s and 100 ms, 95% of the path where the whole carried 67%.
const DEFAULT_STRIDE: u64 = 2;
/// What a mark multiplies the window by: a classic sender's answer to congestion, which a sender of
/// ECT(0) gives a mark (RFC 3168 §5; RFC 9002 §B.1's `kLossReductionFactor`). Of the backoffs the RFCs
/// give (RFC 3168 and RFC 9002: 1/2; RFC 9438: 7/10; RFC 8511's experimental β_ecn: 4/5), focal
/// measured the gentler ones to leave NewReno and CUBIC under CoDel less than nine tenths of what they
/// carry beside their own kind or CUBIC: at 1 Mbit/s and 100 ms over eight seeds, 7/10 left NewReno
/// 0.84 of it and 4/5 left CUBIC 0.79.
const DEFAULT_MARK_BACKOFF: MarkBackoff = MarkBackoff {
    numerator: 1,
    denominator: 2,
};

/// Copa §2.1: "the sender paces packets at a rate of 2·cwnd/RTTstanding packets per second", double
/// the window's rate "to accommodate imperfections in pacing".
const PACING_MULTIPLE: u128 = 2;
/// Nanoseconds in a second: the unit rates are stated per.
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// RFC 9002 §7.2's initial window for datagrams of `datagram` bytes.
fn initial_window(datagram: u64) -> u64 {
    INITIAL_WINDOW_DATAGRAMS
        .saturating_mul(datagram)
        .min(INITIAL_WINDOW_LIMIT.max(MINIMUM_WINDOW_DATAGRAMS.saturating_mul(datagram)))
}

/// How long `amount` bytes take at `bytes` over `elapsed` nanoseconds, the most a `u64` holds where
/// it holds more.
fn time_at(amount: u64, bytes: u64, elapsed: u64) -> u64 {
    u128::from(amount)
        .saturating_mul(u128::from(elapsed))
        .checked_div(u128::from(bytes.max(1)))
        .map_or(u64::MAX, |time| u64::try_from(time).unwrap_or(u64::MAX))
}

/// One sample of a filter: when, and what.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Sample {
    time: u64,
    value: u64,
}

/// The greatest sample of the last `window` of the caller's clock, kept in three samples (Nichols'
/// filter, as Linux `lib/win_minmax.c`): constant space whatever the window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        self.age(sample, window);
        self.best.value
    }

    /// As time passes, a best older than the window gives way to the next; and a quarter of the window
    /// without a new second, or half without a new third, takes the sample for it, so the three stay
    /// spread over the window.
    fn age(&mut self, sample: Sample, window: u64) {
        let elapsed = sample.time.saturating_sub(self.best.time);
        if elapsed > window {
            self.shift(sample);
            if sample.time.saturating_sub(self.best.time) > window {
                self.shift(sample);
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
    }

    fn shift(&mut self, sample: Sample) {
        self.best = self.second;
        self.second = self.third;
        self.third = sample;
    }
}

/// The least sample of the last `window`: the same filter over the order turned round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// The least of `filter`'s window with `value` taken in, the filter begun with it if it had none.
fn least(filter: &mut Option<WindowedMin>, now: u64, window: u64, value: u64) -> u64 {
    match filter {
        Some(filter) => filter.update(now, window, value),
        None => filter.insert(WindowedMin::new(now, value)).get(),
    }
}

/// The greatest of `filter`'s window with `value` taken in, the filter begun with it if it had none.
fn most(filter: &mut Option<WindowedMax>, now: u64, window: u64, value: u64) -> u64 {
    match filter {
        Some(filter) => filter.update(now, window, value),
        None => filter.insert(WindowedMax::new(now, value)).get(),
    }
}

/// The window as it was: a sample whenever it changed, a sixteenth of a smoothed round trip apart at
/// least (a change sooner is taken at the first event past the spacing), the last [`WINDOW_SAMPLES`]
/// kept. A sample holds from its time to the next.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WindowHistory {
    samples: [Sample; WINDOW_SAMPLES],
    /// Where the next sample goes.
    next: usize,
    /// How many are kept.
    kept: usize,
}

impl WindowHistory {
    fn newest(&self) -> Option<Sample> {
        if self.kept == 0 {
            return None;
        }
        let at = self
            .next
            .checked_sub(1)
            .unwrap_or(WINDOW_SAMPLES.saturating_sub(1));
        self.samples.get(at).copied()
    }

    /// Takes the window at `now`, unless it is the newest sample's or that sample is less than
    /// `srtt/16` old (`srtt` zero takes every change).
    fn record(&mut self, now: u64, srtt: u64, window: u64) {
        if let Some(newest) = self.newest()
            && (newest.value == window
                || now.saturating_sub(newest.time)
                    < srtt.checked_div(WINDOW_SAMPLE_SPACING).unwrap_or(0))
        {
            return;
        }
        if let Some(slot) = self.samples.get_mut(self.next) {
            *slot = Sample {
                time: now,
                value: window,
            };
        }
        self.next = self
            .next
            .saturating_add(1)
            .checked_rem(WINDOW_SAMPLES)
            .unwrap_or(0);
        self.kept = self.kept.saturating_add(1).min(WINDOW_SAMPLES);
    }

    /// The window in force at `time`: the newest sample at or before it, or the oldest kept where
    /// every sample is later.
    fn at(&self, time: u64) -> Option<u64> {
        let mut oldest = None;
        for back in 1..=self.kept {
            let at = self
                .next
                .checked_add(WINDOW_SAMPLES)
                .and_then(|at| at.checked_sub(back))
                .and_then(|at| at.checked_rem(WINDOW_SAMPLES))?;
            let sample = self.samples.get(at)?;
            if sample.time <= time {
                return Some(sample.value);
            }
            oldest = Some(sample.value);
        }
        oldest
    }
}

/// Bytes acknowledged over the last interval of a smoothed round trip at least: the delivery rate
/// (draft-cheng-iccrg-delivery-rate-estimation), over a round trip so that acknowledgements arriving
/// in bunches average out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct DeliveryRate {
    /// Every byte acknowledged.
    delivered: u64,
    /// When the interval running began, and `delivered` then.
    mark: Option<Sample>,
    /// The last whole interval: bytes, over nanoseconds.
    last: Option<(u64, u64)>,
    /// The most bytes a second of the intervals of the last ten seconds: the bottleneck's rate as
    /// the most the path delivered, as `RTTmin` is the least it took (BBR's bottleneck-bandwidth
    /// filter, Cardwell et al., ACM Queue 2016, over Copa's window for `RTTmin`, §2.1).
    most: Option<WindowedMax>,
}

impl DeliveryRate {
    fn on_ack(&mut self, now: u64, bytes: u64, srtt: u64) {
        self.delivered = self.delivered.saturating_add(bytes);
        match self.mark {
            Some(mark) if now.saturating_sub(mark.time) >= srtt => {
                let elapsed = now.saturating_sub(mark.time);
                if elapsed > 0 {
                    let bytes = self.delivered.saturating_sub(mark.value);
                    self.last = Some((bytes, elapsed));
                    let rate = u64::try_from(
                        u128::from(bytes)
                            .saturating_mul(NANOS_PER_SECOND)
                            .checked_div(u128::from(elapsed))
                            .unwrap_or(0),
                    )
                    .unwrap_or(u64::MAX);
                    most(&mut self.most, now, MIN_RTT_WINDOW_NS, rate);
                }
                self.mark = Some(Sample {
                    time: now,
                    value: self.delivered,
                });
            }
            Some(_) => {}
            None => {
                self.mark = Some(Sample {
                    time: now,
                    value: self.delivered,
                });
            }
        }
    }
}

/// Copa's window cut to its share of the path, its own bytes in the queue withdrawn, to see whose the
/// queue is (docs/research/congestion.md, "Whose queue is it").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cut {
    began: u64,
    /// The window while the cut holds.
    window: u64,
    /// How far above `RTTmin` the least round trip of the packets sent under the cut may stand for a
    /// queue that was Copa's alone.
    allowance: u64,
    /// When the first packet under the cut was sent.
    first_sent: Option<u64>,
    /// The least round trip of the packets sent under the cut.
    least: Option<u64>,
}

/// Which way the window moved over the last round trip (Copa §2.1's velocity rule).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Up,
    Down,
}

/// A fraction the window is multiplied by: `numerator/denominator`, at most one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarkBackoff {
    numerator: u64,
    denominator: u64,
}

/// One acknowledged packet, as the law sees it. Times are nanoseconds of the law's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Acked {
    now: u64,
    /// From the packet's sending to now.
    rtt: u64,
    /// The smoothed round trip.
    srtt: u64,
    bytes: u64,
    /// The sender had nothing more to send than its window let it.
    window_limited: bool,
}

/// What the filters make of one round-trip sample.
#[derive(Clone, Copy, Debug)]
struct Delays {
    /// The least round trip of the last ten seconds.
    rtt_min: u64,
    /// The least of the last half smoothed round trip.
    standing: u64,
    /// The least and the greatest of the mode's window.
    recent_min: u64,
    recent_max: u64,
}

impl Delays {
    /// The queueing delay `d_q`: `RTTstanding` less `RTTmin`.
    fn queueing(&self) -> u64 {
        self.standing.saturating_sub(self.rtt_min)
    }

    /// Copa §2.2: the queue nearly emptied in the mode's window, within a tenth of the window's spread
    /// or within `packet`, one datagram's time at Copa's rate, whichever is more. The paper's queue is
    /// a fluid; a link's is packets, and Copa alone keeps about `2.5/δ` of them (§3), five at the
    /// default, so a tenth of their spread is half a datagram's time and asks for an idle link
    /// (docs/research/congestion.md, "The paper's emptying is continuous").
    fn nearly_empty(&self, packet: u64) -> bool {
        let spread = self.recent_max.saturating_sub(self.rtt_min);
        let near = spread
            .checked_div(NEARLY_EMPTY_FRACTION)
            .unwrap_or(0)
            .max(packet);
        self.recent_min == self.rtt_min || self.recent_min <= self.rtt_min.saturating_add(near)
    }
}

/// The law, on a clock of nanoseconds the caller counts.
#[derive(Clone, Debug)]
struct Law {
    datagram: u64,
    initial: u64,
    /// The law's window. While a cut holds, the path's is the cut's where that is less
    /// ([`Law::effective_window`]).
    window: u64,
    /// `1/δ` in whole packets, the default mode's (§2.2).
    inv_delta: u64,
    /// The window as it was, for the rate a delay sample measured.
    history: WindowHistory,
    /// What part of the window a round trip moves it by at most, as its inverse.
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
    /// When congestion was last answered: a mark, or while competing a loss, of what was sent before
    /// then is part of the recovery period it began (RFC 9002 §7.3.2).
    recovery: Option<u64>,
    /// When a mark last came after slow start: what the window grows as a classic sender's for
    /// ([`Law::grows_as_a_classic_sender`]).
    marked_after_slow_start: Option<u64>,
    mark_backoff: MarkBackoff,
    delivery: DeliveryRate,
    /// The cut in progress.
    cut: Option<Cut>,
    /// When the last cut ended.
    last_cut: Option<u64>,
    /// Competing: bytes acknowledged toward the next datagram of NewReno's growth (RFC 9002 §B.5).
    classic_acked: u64,
}

impl Law {
    fn new(datagram: u64, config: &CopaConfig) -> Self {
        let datagram = datagram.max(1);
        let initial = initial_window(datagram);
        let mut history = WindowHistory::default();
        history.record(0, 0, initial);
        Self {
            datagram,
            initial,
            window: initial,
            inv_delta: config.inv_delta.max(1),
            history,
            stride: config.stride.max(1),
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
            recovery: None,
            marked_after_slow_start: None,
            mark_backoff: config.mark_backoff,
            delivery: DeliveryRate::default(),
            cut: None,
            last_cut: None,
            classic_acked: 0,
        }
    }

    fn minimum_window(&self) -> u64 {
        MINIMUM_WINDOW_DATAGRAMS.saturating_mul(self.datagram)
    }

    /// The window the path sends under: the law's, or the cut's while one holds and is less.
    fn effective_window(&self) -> u64 {
        self.cut
            .map_or(self.window, |cut| self.window.min(cut.window))
    }

    /// `2·cwnd/RTTstanding` in bytes a second (§2.1), once a standing round trip is measured; the
    /// connection's own rule until then.
    fn pacing_rate(&self) -> Option<u64> {
        let standing = self.standing_rtt?.get().max(1);
        let rate = u128::from(self.effective_window())
            .saturating_mul(PACING_MULTIPLE)
            .saturating_mul(NANOS_PER_SECOND)
            .checked_div(u128::from(standing))
            .map_or(u64::MAX, |rate| u64::try_from(rate).unwrap_or(u64::MAX));
        Some(rate)
    }

    /// The path takes datagrams of another size (RFC 9002 §7.2): the window stays in bytes, and is the
    /// least window at that size at least.
    fn set_datagram(&mut self, datagram: u64) {
        self.datagram = datagram.max(1);
        self.window = self.window.max(self.minimum_window());
    }

    /// A packet left at `now`: the first under a cut begins its evidence.
    fn on_sent(&mut self, now: u64) {
        if let Some(cut) = &mut self.cut
            && cut.first_sent.is_none()
        {
            cut.first_sent = Some(now);
        }
    }

    fn on_ack(&mut self, acked: Acked) {
        self.ack(acked);
        self.history
            .record(acked.now, acked.srtt.max(1), self.effective_window());
    }

    fn ack(&mut self, acked: Acked) {
        let srtt = acked.srtt.max(1);
        let delays = self.delays(acked.now, acked.rtt, srtt);
        self.delivery.on_ack(acked.now, acked.bytes, srtt);
        let sent = acked.now.saturating_sub(acked.rtt);
        if self.cut.is_some() {
            self.judge_cut(acked.now, sent, acked.rtt, srtt, delays.rtt_min);
            return;
        }
        if self.competitive {
            if self.cut_due(acked.now, srtt, CUT_INTERVAL_SRTTS) {
                self.begin_cut(acked.now, delays.rtt_min);
            }
            self.grow_as_new_reno(acked);
            return;
        }
        if !self.slow_start
            && !delays.nearly_empty(self.packet_time(delays.standing))
            && self.cut_due(acked.now, srtt, MODE_WINDOW_SRTTS)
            && self.begin_cut(acked.now, delays.rtt_min)
        {
            return;
        }
        let queueing = delays.queueing();
        let increase = self.below_target(sent, queueing, delays.standing);
        if increase && !acked.window_limited {
            // RFC 9002 §7.8: a window that is not used does not grow. It still shrinks: slates' guard
            // once skipped every update, and a window slow start had overshot stayed frozen while
            // losses kept the sender from looking window-limited (1.5 MB at a 250 kB product).
            return;
        }
        if self.slow_start && increase {
            self.double(acked.now, sent, srtt);
            return;
        }
        self.steer(acked, srtt, increase);
    }

    /// The round-trip sample through the four filters.
    fn delays(&mut self, now: u64, rtt: u64, srtt: u64) -> Delays {
        let mode_window = srtt.saturating_mul(MODE_WINDOW_SRTTS);
        Delays {
            rtt_min: least(&mut self.min_rtt, now, MIN_RTT_WINDOW_NS, rtt),
            standing: least(
                &mut self.standing_rtt,
                now,
                srtt.checked_div(2).unwrap_or(0),
                rtt,
            ),
            recent_min: least(&mut self.mode_min, now, mode_window, rtt),
            recent_max: most(&mut self.mode_max, now, mode_window, rtt),
        }
    }

    /// One datagram's time at the rate of the window over the standing round trip.
    fn packet_time(&self, standing: u64) -> u64 {
        u128::from(self.datagram)
            .saturating_mul(u128::from(standing))
            .checked_div(u128::from(self.window.max(1)))
            .map_or(u64::MAX, |time| u64::try_from(time).unwrap_or(u64::MAX))
    }

    /// Whether a cut may begin: `srtts` smoothed round trips since the last ended, or none yet.
    fn cut_due(&self, now: u64, srtt: u64, srtts: u64) -> bool {
        self.last_cut
            .is_none_or(|then| now.saturating_sub(then) >= srtt.saturating_mul(srtts))
    }

    /// Cuts the window to Copa's share of the path, `r·RTTmin` less [`CUT_MARGIN_DATAGRAMS`]: by
    /// Little's law Copa's bytes in the queue are `r·d_q`, its delivery rate times the queueing delay,
    /// alone or not, and the cut withdraws them and a little more. Alone, the queue was all Copa's and
    /// the packets sent under the cut find it empty; beside another sender, its bytes stay. Returns
    /// whether a cut began: none does before a delivery rate is measured.
    fn begin_cut(&mut self, now: u64, rtt_min: u64) -> bool {
        let Some((bytes, elapsed)) = self.delivery.last else {
            return false;
        };
        let share = u128::from(bytes)
            .saturating_mul(u128::from(rtt_min))
            .checked_div(u128::from(elapsed.max(1)))
            .map_or(u64::MAX, |share| u64::try_from(share).unwrap_or(u64::MAX));
        let window = share
            .saturating_sub(CUT_MARGIN_DATAGRAMS.saturating_mul(self.datagram))
            .max(CUT_FLOOR_DATAGRAMS.saturating_mul(self.datagram))
            .min(self.window);
        // Two datagrams' time on the link, at the most the path delivered: the least round trip may be
        // a smaller packet's, sooner by up to a datagram's time on the link, and of two packets in
        // flight the second waits out the first's time. Above them, what the floor keeps beyond
        // Copa's share queues alone too, at Copa's rate.
        let link = self.delivery.most.map_or(0, |most| most.get());
        let allowance = time_at(
            CUT_QUANTA_DATAGRAMS.saturating_mul(self.datagram),
            link.max(1),
            u64::try_from(NANOS_PER_SECOND).unwrap_or(u64::MAX),
        )
        .min(time_at(
            CUT_QUANTA_DATAGRAMS.saturating_mul(self.datagram),
            bytes,
            elapsed,
        ))
        .saturating_add(time_at(window.saturating_sub(share), bytes, elapsed));
        self.cut = Some(Cut {
            began: now,
            window,
            allowance,
            first_sent: None,
            least: None,
        });
        true
    }

    /// The evidence of the packets sent under the cut, over half a smoothed round trip from the first
    /// (the paper's window for the standing round trip, §2.1: τ = srtt/2). Their least round trip
    /// within a datagram's time of `RTTmin` is a queue that was Copa's alone: Copa keeps the default
    /// mode, or leaves competing, its window at the cut, its own excess dropped. Above it, another
    /// sender's bytes stood: Copa competes, at the window it had. A cut that sees no evidence over the
    /// mode's window decides nothing.
    fn judge_cut(&mut self, now: u64, sent: u64, rtt: u64, srtt: u64, rtt_min: u64) {
        let Some(cut) = &mut self.cut else {
            return;
        };
        let Some(first) = cut.first_sent.filter(|first| sent >= *first) else {
            if now.saturating_sub(cut.began) > srtt.saturating_mul(MODE_WINDOW_SRTTS) {
                self.cut = None;
                self.last_cut = Some(now);
            }
            return;
        };
        let least = cut.least.map_or(rtt, |least| least.min(rtt));
        cut.least = Some(least);
        if sent < first.saturating_add(srtt.checked_div(2).unwrap_or(0)) {
            return;
        }
        let (window, alone) = (
            cut.window.min(self.window),
            least <= rtt_min.saturating_add(cut.allowance),
        );
        self.cut = None;
        self.last_cut = Some(now);
        if alone {
            self.competitive = false;
            self.window = window;
            self.velocity = 1;
            self.same_direction = 0;
            self.direction_mark = Some((now, self.window));
        } else if !self.competitive {
            self.competitive = true;
            self.slow_start = false;
            self.classic_acked = 0;
        }
    }

    /// Competing, the window grows as NewReno's in congestion avoidance: a datagram for each window
    /// acknowledged (RFC 9002 §B.5), and only a window the sender fills (§7.8). §2.2 leaves the law
    /// open ("whatever buffer-filling algorithm one wishes to emulate"); emulated on `1/δ`, Copa's rate
    /// moved with the queueing delay a classic sender's sawtooth moves (docs/research/congestion.md,
    /// "B holds the queueing delay fixed").
    fn grow_as_new_reno(&mut self, acked: Acked) {
        if !acked.window_limited {
            return;
        }
        self.classic_acked = self.classic_acked.saturating_add(acked.bytes);
        if self.classic_acked >= self.window {
            self.classic_acked = self.classic_acked.saturating_sub(self.window);
            self.window = self.window.saturating_add(self.datagram);
        }
    }

    /// Whether the rate the sample measured, the window over `RTTstanding`, is at or below the target
    /// `1/(δ·d_q)`; in bytes, `window·d_q ≤ (1/δ)·datagram·RTTstanding`. An empty queue is below any
    /// target. The products of three 64-bit values fit 192 bits and not 128: the target saturates
    /// where it would not fit.
    ///
    /// Past slow start the window is the one the packet sent at `sent` went under: the queue a sample
    /// shows is what that window built (§3: `q(t) = w(t − RTTmin) − BDP`), and the window now differs
    /// from it by a round trip's movement. Where the queue is short beside the path the two judge
    /// alike. Where it is not (a path of a packet or two), the target barely moves with the queue, and
    /// judged by the window now the window locks to it an acknowledgement either side and the queue
    /// stands at `1/δ` (focal's A2). In slow start, the window now: it holds the doubling the sample
    /// has not seen, and judged by the one before, slow start doubles once more than the path holds.
    fn below_target(&self, sent: u64, queueing: u64, standing: u64) -> bool {
        if queueing == 0 {
            return true;
        }
        let window = if self.slow_start {
            self.window
        } else {
            self.history.at(sent).unwrap_or(self.window)
        };
        let held = u128::from(window).saturating_mul(u128::from(queueing));
        let target = u128::from(self.inv_delta)
            .saturating_mul(u128::from(self.datagram))
            .saturating_mul(u128::from(standing));
        held <= target
    }

    /// Slow start doubles the window once what was sent after the last doubling is heard of. The queue
    /// a doubling builds is seen by what was sent after it, a round trip of sending later and a round
    /// trip of acknowledging after that. A doubling judged by what was sent before the last one doubles
    /// once more than the path holds: at 100 Mbit/s and 100 ms focal measured the window to come to
    /// 3.07 MB where the path and its queue hold 2.5 MB, the queue overflowing for as long as the
    /// transfer lasted, and what was asked beside it taking four round trips.
    fn double(&mut self, now: u64, sent: u64, srtt: u64) {
        match self.last_double {
            None => self.last_double = Some(now),
            Some(then) if sent > then.saturating_add(srtt) => {
                self.window = self.window.saturating_mul(2);
                self.last_double = Some(now);
            }
            Some(_) => {}
        }
    }

    /// Past slow start, or on a decrease: the window moves toward the target by `v/(δ·cwnd)` packets
    /// for the packet acknowledged; for ten seconds after a mark past slow start, it grows by a classic
    /// sender's datagram a round trip instead (RFC 9002 §B.5's `max_datagram_size · acked / cwnd`).
    fn steer(&mut self, acked: Acked, srtt: u64, increase: bool) {
        self.update_direction(acked.now, srtt);
        let wanted = if increase {
            Direction::Up
        } else {
            Direction::Down
        };
        if wanted != self.direction && self.velocity > 1 {
            // A reversal at speed starts over at one (mvfst `changeDirection`).
            self.direction = wanted;
            self.velocity = 1;
            self.same_direction = 0;
            self.direction_mark = Some((acked.now, self.window));
        }
        let gain = if increase && self.grows_as_a_classic_sender(acked.now) {
            1
        } else {
            self.velocity.saturating_mul(self.inv_delta)
        };
        let step = self.step(acked.bytes, gain);
        if increase {
            self.window = self.window.saturating_add(step);
        } else {
            self.slow_start = false;
            self.window = self.window.saturating_sub(step).max(self.minimum_window());
        }
    }

    /// `bytes` acknowledged move the window by `bytes·datagram·gain/cwnd`, the gain in packets; what
    /// is less than a byte is kept for the next step.
    fn step(&mut self, bytes: u64, gain: u64) -> u64 {
        let numerator = u128::from(bytes)
            .saturating_mul(u128::from(self.datagram))
            .saturating_mul(u128::from(gain))
            .saturating_add(u128::from(self.step_remainder));
        let denominator = u128::from(self.window.max(1));
        self.step_remainder = numerator
            .checked_rem(denominator)
            .and_then(|rest| u64::try_from(rest).ok())
            .unwrap_or(0);
        numerator
            .checked_div(denominator)
            .and_then(|step| u64::try_from(step).ok())
            .unwrap_or(u64::MAX)
    }

    /// Once a smoothed round trip: a direction held three times doubles the velocity and a change makes
    /// it one ([`Law::velocity_cap`] bounds it).
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
        self.velocity = self.velocity.min(self.velocity_cap());
        self.direction = direction;
        self.direction_mark = Some((now, self.window));
    }

    /// The velocity at most: `cwnd·δ` packets (genericCC `update_amt`), by which one round trip moves
    /// the window by all of itself, divided by the stride.
    fn velocity_cap(&self) -> u64 {
        u128::from(self.window.checked_div(self.datagram).unwrap_or(0))
            .checked_div(u128::from(self.inv_delta))
            .and_then(|packets| packets.checked_div(u128::from(self.stride)))
            .map_or(1, |cap| u64::try_from(cap).unwrap_or(u64::MAX))
            .max(1)
    }

    /// Whether the window grows as a classic sender's does, a datagram a round trip: within the window
    /// Copa keeps its least round trip over (§2.1, ten seconds) of a mark that came after slow start. A
    /// queue manager keeps the queue short for every sender, so the senders filling it to the manager's
    /// target leave Copa's queue nearly empty and its competing mode unseen; the marks are what Copa
    /// sees of them. Copa's own growth, `v/δ` datagrams a round trip, took back after every mark the
    /// share a classic sender regrows a datagram a round trip, more of it the longer the round trip.
    /// Growing as a classic sender after its marks, competing Copa left NewReno and CUBIC 0.89 of what
    /// they carry beside their own kind at 1 Mbit/s and 100 ms under CoDel in focal's measurement. A
    /// mark in slow start is Copa's own doubling past the manager's target, which ending slow start
    /// answers. A window of four round trips (the mode's) let Copa grow by its own step between the
    /// marks of a long path and take from NewReno more than CUBIC does.
    fn grows_as_a_classic_sender(&self, now: u64) -> bool {
        self.marked_after_slow_start
            .is_some_and(|answered| now.saturating_sub(answered) <= MIN_RTT_WINDOW_NS)
    }

    /// A loss is no signal in the default mode (§2.2: a loss may be noise). Competing, Copa answers it
    /// as NewReno does: the window halves (RFC 9002 §B.1's `kLossReductionFactor`), once a recovery
    /// period (§7.3.2), a loss of what was sent before the last answer being part of it. Persistent
    /// congestion leaves the least window.
    fn on_loss(&mut self, now: u64, sent: u64, persistent: bool) {
        if self.competitive && !self.recovering(sent) {
            self.recovery = Some(now);
            self.window = self
                .window
                .checked_div(2)
                .unwrap_or(0)
                .max(self.minimum_window());
            self.history.record(now, 0, self.effective_window());
        }
        if persistent {
            self.window = self.minimum_window();
            self.slow_start = false;
            self.history.record(now, 0, self.effective_window());
        }
    }

    /// Whether what was sent at `sent` went before congestion was last answered (RFC 9002 §7.3.2).
    fn recovering(&self, sent: u64) -> bool {
        self.recovery.is_some_and(|answered| sent <= answered)
    }

    /// A mark (ECN-CE) of what was sent at `sent`: a queue manager on the path judged its queue longer
    /// than it wants it. A loss Copa may take for noise (§2.2), but a mark is never random: it is
    /// congestion, and a sender that marks its datagrams ECN-capable answers it as a classic sender
    /// answers congestion (RFC 3168 §5, RFC 9002 §7.1). A mark of what was sent before the last answer
    /// is part of that recovery period (RFC 9002 §7.3.2). Otherwise slow start ends (§7.3.1), a
    /// direction up turns down at velocity one, and the window is multiplied by the backoff, never
    /// below the least window, the next round trip's direction judged from there. Marks round trip
    /// after round trip take the window down by the backoff each round trip until they stop or the
    /// window is the least: the response to persistent marking.
    fn on_mark(&mut self, now: u64, sent: u64) {
        if self.recovering(sent) {
            return;
        }
        self.recovery = Some(now);
        // A mark in slow start is Copa's own doubling past the manager's target, which ending slow
        // start answers; one after it says the manager's queue stands above its target while Copa aims
        // at its own short queue: other senders fill it.
        if !self.slow_start {
            self.marked_after_slow_start = Some(now);
        }
        self.slow_start = false;
        if self.direction == Direction::Up && self.velocity > 1 {
            self.direction = Direction::Down;
            self.velocity = 1;
            self.same_direction = 0;
        }
        self.back_off();
        self.direction_mark = Some((now, self.window));
        self.history.record(now, 0, self.effective_window());
    }

    /// The window multiplied by the mark backoff, never above what it was nor below the least window.
    fn back_off(&mut self) {
        let backed_off = u128::from(self.window)
            .saturating_mul(u128::from(self.mark_backoff.numerator))
            .checked_div(u128::from(self.mark_backoff.denominator))
            .and_then(|window| u64::try_from(window).ok())
            .unwrap_or(self.window);
        self.window = backed_off.min(self.window).max(self.minimum_window());
    }
}

/// Copa as a path's congestion controller: the law on the connection's clock
#[derive(Debug, Clone)]
pub struct Copa {
    law: Law,
    /// What the law's clock counts from.
    began: Instant,
}

impl Copa {
    /// A controller for one path, its clock counting from `now`
    pub fn new(config: CopaConfig, now: Instant, current_mtu: u16) -> Self {
        Self {
            law: Law::new(u64::from(current_mtu), &config),
            began: now,
        }
    }

    /// Whether the law is in slow start
    pub fn in_slow_start(&self) -> bool {
        self.law.slow_start
    }

    /// Whether the law judges another sender to be filling the queue and competes for it
    pub fn competitive(&self) -> bool {
        self.law.competitive
    }

    /// `1/δ`, in whole packets: about how many packets of queue the default mode aims to keep
    pub fn inv_delta(&self) -> u64 {
        self.law.inv_delta
    }

    /// How many times its step a packet acknowledged moves the window by
    pub fn velocity(&self) -> u64 {
        self.law.velocity
    }

    fn at(&self, now: Instant) -> u64 {
        nanos(now.saturating_duration_since(self.began))
    }
}

/// A duration in nanoseconds, the most a `u64` holds where it holds more.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl Controller for Copa {
    fn on_sent(&mut self, now: Instant, _bytes: u64, _last_packet_number: u64) {
        let at = self.at(now);
        self.law.on_sent(at);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        let acked = Acked {
            now: self.at(now),
            rtt: nanos(now.saturating_duration_since(sent)),
            srtt: nanos(rtt.get()),
            bytes,
            window_limited: !app_limited,
        };
        self.law.on_ack(acked);
    }

    /// The connection raises a mark with no bytes lost and no persistence (`process_ecn`), once for
    /// each acknowledgement whose count of marks grew, naming the latest packet it acknowledges as
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
            self.law
                .on_loss(at, self.at(sent), is_persistent_congestion);
        }
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.law.set_datagram(u64::from(new_mtu));
    }

    fn window(&self) -> u64 {
        self.law.effective_window()
    }

    fn pacing_rate(&self) -> Option<u64> {
        self.law.pacing_rate()
    }

    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.law.effective_window(),
            ssthresh: None,
            // qlog states it in bits a second.
            pacing_rate: self.law.pacing_rate().map(|bytes| bytes.saturating_mul(8)),
        }
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.law.initial
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Configuration for the [`Copa`] congestion controller
#[derive(Debug, Clone)]
pub struct CopaConfig {
    inv_delta: u64,
    stride: u64,
    mark_backoff: MarkBackoff,
}

impl CopaConfig {
    /// `1/δ`: about how many packets of queue the sender aims to keep (Copa §2.1)
    ///
    /// Copa §2.2's default mode takes 2, the default; mvfst runs 25 (δ = 0.04) for live video.
    pub fn inv_delta(&mut self, value: u64) -> &mut Self {
        self.inv_delta = value;
        self
    }

    /// What part of the window a round trip moves it by at most, as its inverse
    ///
    /// The paper's velocity bound moves the window by all of itself in a round trip (1). The default,
    /// 2, is focal's measurement: half answered soonest over its paths.
    pub fn stride(&mut self, value: u64) -> &mut Self {
        self.stride = value;
        self
    }

    /// What an explicit congestion mark multiplies the window by, `numerator/denominator`, at most one
    ///
    /// The default, 1/2, is a classic sender's answer to congestion (RFC 3168 §5, RFC 9002 §B.1), and
    /// focal's measurement: the gentler backoffs the RFCs give left NewReno and CUBIC under CoDel less
    /// than nine tenths of what they carry beside their own kind or CUBIC.
    pub fn mark_backoff(&mut self, numerator: u64, denominator: u64) -> &mut Self {
        self.mark_backoff = MarkBackoff {
            numerator,
            denominator,
        };
        self
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::congestion::Congestion;

    const DATAGRAM: u64 = 1_000;
    const MS: u64 = 1_000_000;
    /// `1/δ` at the default.
    const DEFAULT: u64 = DEFAULT_INV_DELTA;

    fn new_law() -> Law {
        Law::new(DATAGRAM, &CopaConfig::default())
    }

    fn ack(law: &mut Law, now: u64, rtt: u64, window_limited: bool) {
        law.on_ack(Acked {
            now,
            rtt,
            srtt: 100 * MS,
            bytes: DATAGRAM,
            window_limited,
        });
    }

    /// An acknowledgement at `now` of a datagram sent at `sent`, the sender window-limited.
    fn heard(law: &mut Law, now: u64, sent: u64) {
        law.on_ack(Acked {
            now,
            rtt: now - sent,
            srtt: 100 * MS,
            bytes: DATAGRAM,
            window_limited: true,
        });
    }

    /// A least round trip of 100 ms at 0, then from 10 ms a datagram each 10 ms over a queue of 20
    /// and 30 ms by turns: about Copa's target at ten datagrams, so the window holds near them, slow
    /// start ends at the first, and the queue never nearly empties once the least leaves the mode's
    /// window. Returns when the cut began.
    fn stand(law: &mut Law) -> u64 {
        ack(law, 0, 100 * MS, true);
        for step in 1..=100 {
            let now = step * 10 * MS;
            let rtt = if step % 2 == 1 { 130 * MS } else { 120 * MS };
            ack(law, now, rtt, true);
            if law.cut.is_some() {
                return now;
            }
        }
        panic!("no cut began: {law:?}");
    }

    /// Packets sent under the cut from `from`, a datagram each 10 ms, each heard of `rtt` later, until
    /// the cut is judged; returns when it was.
    fn under_cut(law: &mut Law, from: u64, rtt: u64) -> u64 {
        for step in 0..=10 {
            let sent = from + step * 10 * MS;
            law.on_sent(sent);
            heard(law, sent + rtt, sent);
            if law.cut.is_none() {
                return sent + rtt;
            }
        }
        panic!("the cut was not judged: {law:?}");
    }

    #[test]
    fn a_queue_that_does_not_nearly_empty_over_five_round_trips_is_cut_to_copas_share() {
        let mut law = new_law();
        let began = stand(&mut law);
        // The least of 100 ms leaves the mode's window of five smoothed round trips (500 ms) at the
        // first sample past it: over four it would have left at 410 ms.
        assert_eq!(began, 510 * MS);
        assert!(!law.competitive);
        // A datagram each 10 ms is 100,000 bytes a second, and over the least round trip of 100 ms
        // Copa's share of the path is 10,000 bytes: the cut is two datagrams under it. The path sends
        // under the cut; the law keeps its own window.
        let cut = law.cut.unwrap();
        assert_eq!(cut.window, 8 * DATAGRAM);
        assert_eq!(law.effective_window(), 8 * DATAGRAM);
        assert!(law.window > 8 * DATAGRAM, "{}", law.window);
        // Two datagrams' time at the most the path delivered, 100,000 bytes a second.
        assert_eq!(cut.allowance, 20 * MS);
        assert_eq!(law.pacing_rate(), Some(2 * 8 * DATAGRAM * 1_000 / 120));
    }

    #[test]
    fn a_cut_that_finds_the_queue_empty_keeps_the_default_mode_at_the_cut() {
        let mut law = new_law();
        let began = stand(&mut law);
        // The packets sent under the cut find no queue: it was Copa's own. Judged once the packets of
        // half a smoothed round trip from the first are heard of, the sixth.
        let judged = under_cut(&mut law, began + 10 * MS, 100 * MS);
        assert_eq!(judged, began + 160 * MS);
        assert!(!law.competitive);
        assert_eq!(law.window, 8 * DATAGRAM);
        assert_eq!(law.effective_window(), 8 * DATAGRAM);
        assert_eq!(law.last_cut, Some(judged));
    }

    #[test]
    fn a_cut_that_finds_another_senders_queue_competes_at_the_window_copa_had() {
        let mut law = new_law();
        let began = stand(&mut law);
        let before = law.window;
        // 21 ms of queue stays when Copa withdraws its own bytes: one more than the allowance.
        under_cut(&mut law, began + 10 * MS, 121 * MS);
        assert!(law.competitive);
        assert!(!law.slow_start);
        assert_eq!(law.window, before);
        assert_eq!(law.effective_window(), before);
        // Within the allowance: Copa's own.
        let mut law = new_law();
        let began = stand(&mut law);
        under_cut(&mut law, began + 10 * MS, 120 * MS);
        assert!(!law.competitive);
    }

    #[test]
    fn competing_ends_only_on_a_cut_that_finds_the_queue_empty_never_on_one_empty_moment() {
        let mut law = new_law();
        let began = stand(&mut law);
        let judged = under_cut(&mut law, began + 10 * MS, 150 * MS);
        assert!(law.competitive);
        // The queue empties at once and stays empty, as a competitor's backoff leaves it, or its
        // leaving: Copa competes on until the next cut, forty smoothed round trips (4 s) after the
        // last.
        let mut now = judged;
        loop {
            now += 10 * MS;
            ack(&mut law, now, 100 * MS, true);
            if law.cut.is_some() {
                break;
            }
            assert!(law.competitive, "{now}");
        }
        assert_eq!(now, judged + 4_000 * MS);
        assert!(law.competitive);
        // The cut finds the queue empty: the competitor has gone.
        under_cut(&mut law, now + 10 * MS, 100 * MS);
        assert!(!law.competitive);
        assert_eq!(law.inv_delta, DEFAULT);
    }

    #[test]
    fn competing_the_window_grows_as_new_renos_and_halves_once_a_recovery_period() {
        let mut law = new_law();
        let began = stand(&mut law);
        let judged = under_cut(&mut law, began + 10 * MS, 150 * MS);
        assert!(law.competitive);
        // A window's bytes acknowledged grow it a datagram (RFC 9002 §B.5), and a window not filled
        // does not grow.
        let window = law.window;
        let mut now = judged;
        let mut acked = 0;
        while acked + DATAGRAM <= window {
            now += MS;
            ack(&mut law, now, 150 * MS, true);
            acked += DATAGRAM;
        }
        assert_eq!(law.window, window);
        now += MS;
        law.on_ack(Acked {
            now,
            rtt: 150 * MS,
            srtt: 100 * MS,
            bytes: window - acked,
            window_limited: true,
        });
        assert_eq!(law.window, window + DATAGRAM);
        for _ in 0..100 {
            now += MS;
            ack(&mut law, now, 150 * MS, false);
        }
        assert_eq!(law.window, window + DATAGRAM);
        // A loss halves it; a loss of what was sent before that answer is the same recovery period;
        // one of what was sent after it halves it again.
        let grown = law.window;
        law.on_loss(now, now - 150 * MS, false);
        assert_eq!(law.window, grown / 2);
        law.on_loss(now + MS, now, false);
        assert_eq!(law.window, grown / 2);
        law.on_loss(now + 200 * MS, now + 50 * MS, false);
        assert_eq!(law.window, grown / 4);
    }

    #[test]
    fn a_cut_that_sees_no_packet_of_its_own_decides_nothing_after_the_modes_window() {
        let mut law = new_law();
        let began = stand(&mut law);
        // Nothing is sent under the cut: only what left before it is heard of.
        for step in 1..=50 {
            ack(&mut law, began + step * 10 * MS, 130 * MS, true);
            assert!(law.cut.is_some(), "{step}");
        }
        ack(&mut law, began + 510 * MS, 130 * MS, true);
        assert!(law.cut.is_none());
        assert!(!law.competitive);
        assert_eq!(law.last_cut, Some(began + 510 * MS));
    }

    #[test]
    fn a_cut_leaves_three_datagrams_at_least_and_allows_for_what_they_hold_beyond_copas_share() {
        let mut law = new_law();
        law.window = 10 * DATAGRAM;
        // 2,000 bytes over 100 ms, 20,000 bytes a second: a share of 2,000 bytes over a least round
        // trip of 100 ms, under the floor of three datagrams with the margin.
        law.delivery.last = Some((2 * DATAGRAM, 100 * MS));
        assert!(law.begin_cut(0, 100 * MS));
        let cut = law.cut.unwrap();
        assert_eq!(cut.window, 3 * DATAGRAM);
        // No rate of the path's yet: two datagrams at Copa's, 100 ms, and the datagram the floor holds
        // beyond its share, 50 ms.
        assert_eq!(cut.allowance, 150 * MS);
        assert_eq!(law.effective_window(), 3 * DATAGRAM);
        assert_eq!(law.window, 10 * DATAGRAM);
        // A window under the floor is not raised by the cut.
        let mut law = new_law();
        law.window = 2 * DATAGRAM;
        law.delivery.last = Some((2 * DATAGRAM, 100 * MS));
        assert!(law.begin_cut(0, 100 * MS));
        assert_eq!(law.effective_window(), 2 * DATAGRAM);
        // Without a delivery rate there is no cut.
        let mut law = new_law();
        assert!(!law.begin_cut(0, 100 * MS));
        assert!(law.cut.is_none());
    }

    #[test]
    fn the_delivery_rate_is_taken_over_a_smoothed_round_trip_and_its_most_over_ten_seconds() {
        let mut rate = DeliveryRate::default();
        rate.on_ack(0, 1_000, 100);
        assert_eq!(rate.last, None);
        rate.on_ack(50, 1_000, 100);
        assert_eq!(rate.last, None);
        rate.on_ack(100, 2_000, 100);
        // What came after the interval's first acknowledgement, over the interval.
        assert_eq!(rate.last, Some((3_000, 100)));
        assert_eq!(rate.most.map(|most| most.get()), Some(30_000_000_000));
        rate.on_ack(300, 1_000, 100);
        assert_eq!(rate.last, Some((1_000, 200)));
        // The most stands for ten seconds.
        assert_eq!(rate.most.map(|most| most.get()), Some(30_000_000_000));
        rate.on_ack(MIN_RTT_WINDOW_NS + 500, 1_000, 100);
        assert_eq!(
            rate.most.map(|most| most.get()),
            Some(1_000 * 1_000_000_000 / (MIN_RTT_WINDOW_NS + 200))
        );
    }

    #[test]
    fn a_queue_of_one_datagram_is_nearly_empty() {
        // The least round trip 100 ms, the most of the mode's window 102 ms: a tenth of the spread is
        // 0.2 ms, below one datagram's time at the window's rate (1,000 bytes of a 10,000-byte window
        // over 101 ms: 10.1 ms). A least of the window 1 ms over the least round trip is one datagram
        // queued at most: nearly empty, where a tenth of the spread alone says not.
        let mut law = new_law();
        ack(&mut law, 0, 100 * MS, true);
        let delays = Delays {
            rtt_min: 100 * MS,
            standing: 101 * MS,
            recent_min: 101 * MS,
            recent_max: 102 * MS,
        };
        let packet = law.packet_time(delays.standing);
        assert_eq!(packet, DATAGRAM * 101 * MS / (10 * DATAGRAM));
        assert!(delays.nearly_empty(packet));
        assert!(!delays.nearly_empty(0));
        let full = Delays {
            recent_min: 100 * MS + packet + 1,
            ..delays
        };
        assert!(!full.nearly_empty(packet));
    }

    #[test]
    fn past_slow_start_the_window_a_packet_was_sent_under_judges_its_delay() {
        // 10 ms of queue over a standing round trip of 110 ms: the target is two packets over 10 ms,
        // 22 packets a round trip of 110 ms. The window was 20 packets when the acknowledged packet
        // left and is 40 now: the queue the sample shows is what 20 packets built, under the target,
        // and the window grows. Judged by the window now it would shrink.
        let mut law = new_law();
        ack(&mut law, 0, 100 * MS, true);
        law.slow_start = false;
        law.window = 20 * DATAGRAM;
        law.history.record(100 * MS, 0, law.window);
        law.window = 40 * DATAGRAM;
        law.history.record(200 * MS, 0, law.window);
        ack(&mut law, 250 * MS, 110 * MS, true);
        assert_eq!(
            law.window,
            40 * DATAGRAM + 2 * DATAGRAM * DATAGRAM / (40 * DATAGRAM)
        );
        // A packet sent under the 40 is judged by them: over the target.
        let mut law = new_law();
        ack(&mut law, 0, 100 * MS, true);
        law.slow_start = false;
        law.window = 40 * DATAGRAM;
        law.history.record(100 * MS, 0, law.window);
        ack(&mut law, 250 * MS, 110 * MS, true);
        assert_eq!(
            law.window,
            40 * DATAGRAM - 2 * DATAGRAM * DATAGRAM / (40 * DATAGRAM)
        );
    }

    #[test]
    fn the_history_keeps_the_window_a_sixteenth_of_a_round_trip_apart_and_the_last_thirty_two() {
        let mut history = WindowHistory::default();
        assert_eq!(history.at(5), None);
        history.record(0, 160, 10);
        // The same window again, or a change sooner than a sixteenth of the round trip (10), is not
        // kept as it comes.
        history.record(5, 160, 10);
        history.record(9, 160, 11);
        assert_eq!(history.kept, 1);
        history.record(10, 160, 12);
        assert_eq!(history.at(9), Some(10));
        assert_eq!(history.at(10), Some(12));
        assert_eq!(history.at(u64::MAX), Some(12));
        // A time before every sample kept takes the oldest.
        for at in 1..=40_u64 {
            history.record(10 + at * 10, 160, 100 + at);
        }
        assert_eq!(history.kept, WINDOW_SAMPLES);
        assert_eq!(history.at(0), Some(100 + 40 - 31));
        assert_eq!(history.at(415), Some(140));
        assert_eq!(history.at(405), Some(139));
    }

    #[test]
    fn the_controller_sends_under_the_cut_and_hears_of_its_first_packet() {
        let began = Instant::now();
        let mut copa = Copa::new(CopaConfig::default(), began, 1_200);
        copa.law.delivery.last = Some((2_400, 100 * MS));
        assert!(copa.law.begin_cut(0, 100 * MS));
        assert_eq!(copa.window(), 3_600);
        assert_eq!(copa.metrics().congestion_window, 3_600);
        copa.on_sent(began + Duration::from_millis(5), 1_200, 7);
        copa.on_sent(began + Duration::from_millis(6), 1_200, 8);
        assert_eq!(copa.law.cut.and_then(|cut| cut.first_sent), Some(5 * MS));
    }

    #[test]
    fn the_initial_window_is_rfc_9002s() {
        assert_eq!(initial_window(1_200), 12_000);
        assert_eq!(initial_window(1_472), 14_720);
        // Large datagrams: limited to the larger of 14,720 bytes and two datagrams.
        assert_eq!(initial_window(9_000), 18_000);
        assert_eq!(initial_window(1_600), 14_720);
        let copa = Copa::new(CopaConfig::default(), Instant::now(), 1_200);
        assert_eq!(copa.initial_window(), 12_000);
        assert_eq!(copa.window(), 12_000);
    }

    #[test]
    fn copa_paces_at_twice_its_window_over_the_standing_round_trip() {
        let mut law = new_law();
        // No standing round trip yet: the connection's own rule.
        assert_eq!(law.pacing_rate(), None);
        ack(&mut law, 0, 100 * MS, true);
        // 10,000 bytes over a standing 100 ms, doubled: 200,000 bytes a second.
        assert_eq!(law.window, 10 * DATAGRAM);
        assert_eq!(law.pacing_rate(), Some(200_000));
        let copa = Copa::new(CopaConfig::default(), Instant::now(), 1_200);
        assert_eq!(copa.pacing_rate(), None);
        assert_eq!(copa.metrics().pacing_rate, None);
    }

    #[test]
    fn an_empty_queue_doubles_the_window_once_what_was_sent_after_the_last_doubling_is_heard_of() {
        let mut law = new_law();
        let start = law.window;
        assert_eq!(start, 10 * DATAGRAM);
        // Heard of at 0: what is acknowledged until 200 ms was sent within a round trip of it.
        for step in 0..=20 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
            assert_eq!(law.window, start, "at {step}");
        }
        ack(&mut law, 210 * MS, 100 * MS, true);
        assert_eq!(law.window, 2 * start);
        for step in 22..=41 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
            assert_eq!(law.window, 2 * start, "at {step}");
        }
        ack(&mut law, 420 * MS, 100 * MS, true);
        assert_eq!(law.window, 4 * start);
        assert!(law.slow_start);
    }

    #[test]
    fn a_queue_past_the_target_shrinks_the_window() {
        let mut law = new_law();
        ack(&mut law, 0, 100 * MS, true);
        let before = law.window;
        // 100 ms of queue at ten packets: the target is 20 packets a second, and ten packets in
        // 200 ms are 50.
        for step in 1..=5 {
            ack(&mut law, 100 * MS + step * MS, 200 * MS, true);
        }
        assert!(!law.slow_start);
        assert!(law.window < before);
        assert_eq!(law.standing_rtt.map(|filter| filter.get()), Some(200 * MS));
    }

    #[test]
    fn a_window_the_sender_does_not_fill_shrinks_and_never_grows() {
        let mut law = new_law();
        ack(&mut law, 0, 100 * MS, false);
        let before = law.window;
        for step in 1..=5 {
            ack(&mut law, 100 * MS + step * MS, 200 * MS, false);
        }
        assert!(law.window < before);
        let shrunk = law.window;
        for step in 1..=50 {
            ack(&mut law, 10_000 * MS + step * MS, 100 * MS, false);
        }
        assert!(law.window <= shrunk);
    }

    #[test]
    fn a_loss_is_no_signal_unless_it_persists() {
        let mut law = new_law();
        for step in 0..=50 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        let window = law.window;
        law.on_loss(600 * MS, 100 * MS, false);
        assert_eq!(law.window, window);
        law.on_loss(900 * MS, 100 * MS, true);
        assert_eq!(law.window, 2 * DATAGRAM);
        assert!(!law.slow_start);
    }

    #[test]
    fn the_window_is_never_below_two_datagrams_and_follows_their_size() {
        let mut law = new_law();
        ack(&mut law, 0, 100 * MS, true);
        // 4.9 s of queue: the target is two packets and a little.
        for step in 1..=90 {
            ack(&mut law, 100 * MS + step * MS, 5_000 * MS, true);
            assert!(law.window >= 2 * DATAGRAM);
        }
        assert!(law.window < 3 * DATAGRAM, "{}", law.window);
        law.on_loss(200 * MS, 100 * MS, true);
        assert_eq!(law.window, 2 * DATAGRAM);
        law.set_datagram(1_400);
        assert_eq!(law.window, 2_800);
        law.set_datagram(0);
        assert_eq!(law.window, 2_800);
    }

    #[test]
    fn nothing_overflows_at_the_bounds_of_what_it_is_told() {
        for (datagram, inv_delta) in [(0, 0), (u64::MAX, u64::MAX), (1, u64::MAX), (u64::MAX, 1)] {
            let mut config = CopaConfig::default();
            config.inv_delta(inv_delta).stride(0).mark_backoff(0, 0);
            let mut law = Law::new(datagram, &config);
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
                    assert!(law.window >= 2);
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
        let mut controller = Congestion::Copa(CopaConfig::default()).build(began, 1_200);
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
        let copa = controller.into_any().downcast::<Copa>().unwrap();
        assert!(!copa.in_slow_start());
        assert!(!copa.competitive());
        assert_eq!(copa.inv_delta(), DEFAULT_INV_DELTA);
        assert_eq!(copa.velocity(), 1);
    }

    #[test]
    fn a_mark_ends_slow_start_and_steps_the_window_down_once_a_round_trip() {
        let mut law = new_law();
        for step in 0..=25 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert!(law.slow_start);
        let before = law.window;
        // The backoff of a mark: half the window.
        law.on_mark(300 * MS, 250 * MS);
        assert!(!law.slow_start);
        assert_eq!(law.window, before / 2);
        // A mark of what was sent before that answer is the same round trip.
        law.on_mark(320 * MS, 300 * MS);
        assert_eq!(law.window, before / 2);
        // One of what was sent after it is the next.
        law.on_mark(420 * MS, 310 * MS);
        assert_eq!(law.window, before / 4);
        // Never below the least window.
        for round in 0..100 {
            law.on_mark(500 * MS + round * 100 * MS, 450 * MS + round * 100 * MS);
            assert!(law.window >= 2 * DATAGRAM);
        }
        assert_eq!(law.window, 2 * DATAGRAM);
    }

    #[test]
    fn a_mark_while_competing_halves_the_window_as_a_classic_sender_does() {
        let mut law = new_law();
        let began = stand(&mut law);
        let judged = under_cut(&mut law, began + 10 * MS, 150 * MS);
        assert!(law.competitive);
        let window = law.window;
        law.on_mark(judged + 10 * MS, judged);
        // Copa halves its window as a classic sender does for a mark (RFC 9002 §B.1), and `1/δ` is the
        // default mode's alone.
        assert_eq!(law.window, (window / 2).max(2 * DATAGRAM));
        assert_eq!(law.inv_delta, DEFAULT);
        // The same recovery period says nothing more, a loss in it included.
        law.on_mark(judged + 20 * MS, judged + 5 * MS);
        law.on_loss(judged + 30 * MS, judged + 10 * MS, false);
        assert_eq!(law.window, (window / 2).max(2 * DATAGRAM));
    }

    #[test]
    fn marks_held_round_trip_after_round_trip_take_the_window_down_by_the_backoff_to_the_least() {
        // A law whose queue empties (the default mode), marked once each round trip: the declared
        // response to persistent marking halves the window each round trip, to the least window.
        let mut law = new_law();
        for step in 0..=40 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert!(!law.competitive);
        let mut window = law.window;
        for round in 0..40_u64 {
            let begins = 1_000 * MS + round * 100 * MS;
            law.on_mark(begins + 99 * MS, begins);
            window = (window / 2).max(2 * DATAGRAM);
            assert_eq!(law.window, window, "round {round}");
        }
        assert_eq!(law.window, 2 * DATAGRAM);
        assert!(!law.slow_start && !law.competitive);
    }

    /// What a round trip of acknowledgements, the window's bytes from `from`, grows the window by, and
    /// the window it began from.
    fn round_trip(law: &mut Law, from: u64) -> (u64, u64) {
        let before = law.window;
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
        (law.window - before, before)
    }

    /// Whether a round trip that began at `before` and grew by `grown` grew by `packets` datagrams a
    /// round trip: a window's bytes acknowledged, each step `packets·datagram²/cwnd` with the window
    /// between `before` and `before + packets·datagram` as it grows, the remainder carried, so the
    /// growth lies within a byte of `[packets·datagram·before/(before + packets·datagram),
    /// packets·datagram]`.
    fn grew_by(grown: u64, before: u64, packets: u64) -> bool {
        let most = packets * DATAGRAM;
        let least = most * before / (before + most);
        (least.saturating_sub(1)..=most + 1).contains(&grown)
    }

    #[test]
    fn after_a_mark_past_slow_start_the_window_grows_a_datagram_a_round_trip_for_ten_seconds() {
        let mut law = new_law();
        for step in 0..=80 {
            ack(&mut law, step * 10 * MS, 100 * MS, true);
        }
        assert_eq!(law.window, 80 * DATAGRAM);
        // A mark in slow start is Copa's own doubling past the manager's target: slow start ends and
        // Copa grows by its own step after it, `v/δ` datagrams a round trip, two at velocity one.
        law.on_mark(900 * MS, 850 * MS);
        assert!(!law.slow_start);
        let (grown, before) = round_trip(&mut law, 1_000 * MS);
        assert!(
            grew_by(grown, before, 2),
            "Copa's own step: {grown} from {before}"
        );
        // One after slow start: for ten seconds a datagram a round trip.
        law.on_mark(1_100 * MS, 1_050 * MS);
        let (grown, before) = round_trip(&mut law, 1_200 * MS);
        assert!(
            grew_by(grown, before, 1),
            "a datagram: {grown} from {before}"
        );
        let (grown, before) = round_trip(&mut law, 11_000 * MS);
        assert!(
            grew_by(grown, before, 1),
            "a datagram: {grown} from {before}"
        );
        // Past ten seconds from it, Copa's own step again.
        let (grown, before) = round_trip(&mut law, 11_200 * MS);
        assert!(
            grew_by(grown, before, 2),
            "Copa's own step: {grown} from {before}"
        );
    }

    #[test]
    fn the_controller_takes_an_event_without_lost_bytes_for_a_mark() {
        let began = Instant::now();
        let mut controller = Congestion::Copa(CopaConfig::default()).build(began, 1_200);
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
        let copa = controller.into_any().downcast::<Copa>().unwrap();
        assert!(!copa.in_slow_start());
    }
}
