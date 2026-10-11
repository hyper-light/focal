//! A session's write window, learned: how many writes its leader holds
//! admitted at once, queued and pending together, so that a write waits for
//! the pipeline it enters and not for a queue of the leader's own making.
//!
//! The pipeline is a path in Little's law's sense: a write enters at the
//! host's ingress and leaves when it is answered, committed. A window of `w`
//! writes over a base latency `L` admits at most `w / L` writes a second, and
//! past the pipeline's bandwidth-delay product (its delivery rate times its
//! base latency) every write admitted adds only queueing delay: 24 queued and
//! 32 pending, fixed, held a committed write 73 ms at the median where the
//! pipeline answers one in 3 (the Linux comparison at 1,000/s offered and at
//! 100/s, 2026-10-11).
//!
//! The window is learned as BBR learns a path's (Cardwell, Cheng, Gunn,
//! Hassas Yeganeh and Jacobson, "BBR: Congestion-Based Congestion Control",
//! ACM Queue 14(5), 2016), in the pipeline's own units: rounds, each of the
//! writes admitted and answered while the round's first write is, and never
//! seconds picked for one machine.
//! - The delivery rate is sampled once a round: the writes answered over the
//!   round's span. Its maximum is kept over the last ten rounds (BBR's
//!   bottleneck-bandwidth filter) by Kathleen Nichols' windowed filter, and a
//!   round whose window never filled (its load, not the pipeline, set its
//!   rate) lowers it no further than a full round would.
//! - The base latency is the least a write took from admission to answer. A
//!   window that stays full keeps a queue that hides it, so once it is fifty
//!   rounds old the window holds four writes for a round and measures it
//!   again (BBR's ProbeRTT, which holds its least window 200 ms in every
//!   10 s: one round in fifty, the same share of the pipeline's time).
//! - The window is the delivery rate times the base latency, times a gain:
//!   2/ln 2 while it starts (the least gain that doubles the delivery rate a
//!   round: BBR's startup), the product alone while it drains what starting
//!   queued, and 2 after (BBR's window gain: commits are answered in batches,
//!   as acknowledgements aggregate), cycled by 5/4 and then 3/4 one round
//!   each in eight (BBR's probing for more, then draining what that probe
//!   queued).
//! - It starts at ten writes (RFC 6928's initial window) and leaves its start
//!   once three rounds in a row grew the delivery rate by less than a quarter
//!   (BBR's full-pipe test). It holds no fewer than four writes (BBR's least
//!   window) and no more than its ceiling: what the node's memory holds of
//!   the writes, each of which reserves its room as it is admitted.

/// The rounds the delivery rate's maximum is kept over (BBR's bottleneck
/// bandwidth filter length).
const RATE_ROUNDS: u64 = 10;
/// The rounds of one probe cycle (BBR's eight pacing gains).
const CYCLE_ROUNDS: u8 = 8;
/// The rounds the base latency stands before it is measured again: BBR
/// measures it for 200 ms in every 10 s, a fiftieth of the path's time, and
/// one round in fifty is the same share of the pipeline's.
const LATENCY_ROUNDS: u64 = 50;
/// The least window, and the window that measures the base latency again
/// (BBR's minimum window of four packets).
const LEAST_WINDOW: u64 = 4;
/// The window a session starts with (RFC 6928's initial window of ten).
const INITIAL_WINDOW: u64 = 10;
/// The gain while starting: 2/ln 2, the least that doubles the delivery
/// rate each round (BBR's high gain).
const STARTUP_GAIN: Gain = Gain(2885, 1000);
/// The window's gain over the bandwidth-delay product (BBR's window gain).
const WINDOW_GAIN: Gain = Gain(2, 1);
/// The growth a round's rate must show for the pipeline not to be full
/// (BBR's full-pipe threshold of a quarter), and the rounds without it that
/// say it is.
const FULL_GROWTH: Gain = Gain(5, 4);
const FULL_ROUNDS: u8 = 3;
/// The delivery rate's fixed point: writes a second times this.
const RATE_SCALE: u128 = 1 << 10;
const NANOS: u128 = 1_000_000_000;

/// A rational gain, applied in 128 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Gain(u64, u64);
impl Gain {
    fn of(self, value: u64) -> u64 {
        u128::from(value)
            .saturating_mul(u128::from(self.0))
            .checked_div(u128::from(self.1))
            .and_then(|product| u64::try_from(product).ok())
            .unwrap_or(u64::MAX)
    }
}
/// The gain of the probe cycle's `index`th round: 5/4 to probe for more,
/// 3/4 to drain what that probe queued, then 1 for six.
fn cycle_gain(index: u8) -> Gain {
    match index {
        0 => Gain(5, 4),
        1 => Gain(3, 4),
        _ => Gain(1, 1),
    }
}

/// One sample of the windowed filter: its value and the round it was taken.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Sample {
    round: u64,
    value: u64,
}
/// The most of a stream over the last `RATE_ROUNDS` rounds, in constant
/// space: Kathleen Nichols' windowed filter (as BBR keeps its bandwidth),
/// the best, second and third choices, each no older than the one before it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MaxFilter {
    best: Sample,
    second: Sample,
    third: Sample,
}
impl MaxFilter {
    fn get(&self) -> u64 {
        self.best.value
    }
    fn update(&mut self, round: u64, value: u64) {
        let sample = Sample { round, value };
        if self.best.value == 0
            || value >= self.best.value
            || round.saturating_sub(self.third.round) > RATE_ROUNDS
        {
            *self = Self {
                best: sample,
                second: sample,
                third: sample,
            };
            return;
        }
        if value >= self.second.value {
            self.second = sample;
            self.third = sample;
        } else if value >= self.third.value {
            self.third = sample;
        }
        let elapsed = round.saturating_sub(self.best.round);
        if elapsed > RATE_ROUNDS {
            // The best aged out: the second and third move up, and the
            // second may have aged out as well.
            self.best = self.second;
            self.second = self.third;
            self.third = sample;
            if round.saturating_sub(self.best.round) > RATE_ROUNDS {
                self.best = self.second;
                self.second = self.third;
                self.third = sample;
            }
        } else if self.second.round == self.best.round
            && elapsed > RATE_ROUNDS.checked_div(4).unwrap_or(0)
        {
            // A quarter of the span passed with no second choice of its own:
            // take one from the second quarter.
            self.second = sample;
            self.third = sample;
        } else if self.third.round == self.second.round
            && elapsed > RATE_ROUNDS.checked_div(2).unwrap_or(0)
        {
            self.third = sample;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Growing the window by the startup gain each round until the rate
    /// stops growing.
    Startup,
    /// Holding the window at the product until what starting queued drains.
    Drain,
    /// Probing: the `index`th round of the gain cycle.
    Probe { index: u8 },
    /// Holding the least window for a round to measure the base latency;
    /// `back` is the phase it returns to.
    Latency { back: Back },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Back {
    Startup,
    Probe(u8),
}

/// A write's admission as the window samples it: when it was admitted, on
/// the owner's clock in nanoseconds, and how many writes had been answered
/// when its owner took it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Admitted {
    pub(crate) at_ns: u64,
    pub(crate) delivered: u64,
}

/// A session leader's write window ([module](self)).
#[derive(Clone, Debug)]
pub(crate) struct WriteWindow {
    ceiling: u64,
    phase: Phase,
    window: u64,
    /// Writes answered, committed.
    delivered: u64,
    round: u64,
    /// The writes answered when this round began: a write taken at or past
    /// it, answered, ends the round.
    round_delivered: u64,
    round_started_ns: u64,
    /// Whether the window filled during the round: its rate is the
    /// pipeline's, not its load's.
    round_full: bool,
    rate: MaxFilter,
    /// The least latency of its span, nanoseconds, and the round it was
    /// measured in; zero before any.
    latency_ns: u64,
    latency_round: u64,
    /// The least latency of a latency probe, while one is under way.
    probing_ns: u64,
    full_rate: u64,
    full_rounds: u8,
}

impl WriteWindow {
    /// A window starting at RFC 6928's ten writes, held at most `ceiling`.
    pub(crate) fn new(ceiling: u64, now_ns: u64) -> Self {
        let mut window = Self {
            ceiling,
            phase: Phase::Startup,
            window: INITIAL_WINDOW,
            delivered: 0,
            round: 0,
            round_delivered: 0,
            round_started_ns: now_ns,
            round_full: false,
            rate: MaxFilter::default(),
            latency_ns: 0,
            latency_round: 0,
            probing_ns: 0,
            full_rate: 0,
            full_rounds: 0,
        };
        window.window = window.bounded(INITIAL_WINDOW);
        window
    }
    /// The writes the window holds now.
    pub(crate) fn window(&self) -> u64 {
        self.window
    }
    /// Writes answered so far, for the next write's [`Admitted`].
    pub(crate) fn delivered(&self) -> u64 {
        self.delivered
    }
    /// The window was full: a write was refused at it, or `inflight`
    /// reached it. The round's rate is then the pipeline's.
    pub(crate) fn filled(&mut self, inflight: u64) {
        if inflight >= self.window {
            self.round_full = true;
        }
    }
    /// A write admitted at `admitted` was answered, committed, at `now_ns`,
    /// with `inflight` writes still admitted.
    pub(crate) fn answered(&mut self, admitted: Admitted, now_ns: u64, inflight: u64) {
        self.delivered = self.delivered.saturating_add(1);
        self.filled(inflight.saturating_add(1));
        let latency = now_ns.saturating_sub(admitted.at_ns).max(1);
        if let Phase::Latency { .. } = self.phase {
            self.probing_ns = match self.probing_ns {
                0 => latency,
                least => least.min(latency),
            };
        }
        if self.latency_ns == 0 || latency <= self.latency_ns {
            self.latency_ns = latency;
            self.latency_round = self.round;
        }
        if admitted.delivered >= self.round_delivered {
            self.end_round(now_ns, inflight);
        }
        self.window = self.bounded(self.target());
    }

    /// The round that just ended: sample its rate, and move the phase.
    fn end_round(&mut self, now_ns: u64, inflight: u64) {
        let span = now_ns.saturating_sub(self.round_started_ns).max(1);
        let answered = self.delivered.saturating_sub(self.round_delivered);
        let rate = u128::from(answered)
            .saturating_mul(RATE_SCALE)
            .saturating_mul(NANOS)
            .checked_div(u128::from(span))
            .and_then(|rate| u64::try_from(rate).ok())
            .unwrap_or(u64::MAX);
        let full = self.round_full;
        self.round = self.round.saturating_add(1);
        // A round its load limited lowers the rate no further than a full
        // round would; it may still raise it (BBR's application-limited
        // samples).
        if full || rate >= self.rate.get() {
            self.rate.update(self.round, rate);
        }
        self.round_delivered = self.delivered;
        self.round_started_ns = now_ns;
        self.round_full = false;
        self.phase = match self.phase {
            Phase::Startup => {
                if full {
                    if self.rate.get() >= FULL_GROWTH.of(self.full_rate) {
                        self.full_rate = self.rate.get();
                        self.full_rounds = 0;
                    } else {
                        self.full_rounds = self.full_rounds.saturating_add(1);
                    }
                }
                if self.full_rounds >= FULL_ROUNDS {
                    Phase::Drain
                } else {
                    Phase::Startup
                }
            }
            Phase::Drain => {
                if inflight <= self.product() {
                    Phase::Probe { index: 0 }
                } else {
                    Phase::Drain
                }
            }
            Phase::Probe { index } => Phase::Probe {
                index: index
                    .checked_add(1)
                    .and_then(|next| next.checked_rem(CYCLE_ROUNDS))
                    .unwrap_or(0),
            },
            Phase::Latency { back } => {
                // One round at the least window measured it again.
                if self.probing_ns != 0 {
                    self.latency_ns = self.probing_ns;
                    self.latency_round = self.round;
                }
                self.probing_ns = 0;
                match back {
                    Back::Startup => Phase::Startup,
                    Back::Probe(index) => Phase::Probe { index },
                }
            }
        };
        if self.round.saturating_sub(self.latency_round) > LATENCY_ROUNDS {
            let back = match self.phase {
                Phase::Startup | Phase::Drain => Some(Back::Startup),
                Phase::Probe { index } => Some(Back::Probe(index)),
                Phase::Latency { .. } => None,
            };
            if let Some(back) = back {
                self.phase = Phase::Latency { back };
                self.probing_ns = 0;
            }
        }
    }

    /// The bandwidth-delay product, in writes: the delivery rate's maximum
    /// times the base latency; zero before either is measured.
    fn product(&self) -> u64 {
        u128::from(self.rate.get())
            .saturating_mul(u128::from(self.latency_ns))
            .checked_div(RATE_SCALE.saturating_mul(NANOS))
            .and_then(|product| u64::try_from(product).ok())
            .unwrap_or(u64::MAX)
    }

    /// The window the phase asks for, before its bounds.
    fn target(&self) -> u64 {
        let product = self.product();
        match self.phase {
            Phase::Startup => self.window.max(STARTUP_GAIN.of(product)),
            Phase::Drain => product,
            Phase::Probe { index } => cycle_gain(index).of(WINDOW_GAIN.of(product)),
            Phase::Latency { .. } => LEAST_WINDOW,
        }
    }

    /// `window` within the least window and the ceiling; a ceiling under the
    /// least window is the window.
    fn bounded(&self, window: u64) -> u64 {
        window.max(LEAST_WINDOW).min(self.ceiling.max(1))
    }
}

#[cfg(test)]
#[path = "write_window_tests.rs"]
mod tests;
