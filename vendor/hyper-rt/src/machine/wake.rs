//! The wake probe (§4.1 "park→unpark latency"; D-10's spin-then-park; the derived constants of §4.1, §4.3
//! and §4.7): the **expected cost of parking** — how long a thread asleep in the kernel takes to run again
//! after another thread wakes it — measured with the placement the product runs under, and converged on
//! the statistic its consumers read.
//!
//! Each choice below is what the 2026-09-22 measurements decided
//! (`docs/bugs/2026-09-22-wake-probe-mixes-two-events-and-reports-an-unconverged-tail.md`):
//!
//! - **One event: a sleeping thread woken.** Before each wake the waiter is confirmed asleep in the
//!   kernel — its `/proc` state `S` on Linux, `thread_info`'s `TH_STATE_WAITING` on macOS; Windows has no
//!   per-thread query short of a system-wide snapshot, so there the waiter's own announcement stands in and
//!   the result says so. The probe it replaces timed every park/unpark round trip, whether the waiter was
//!   still running or asleep, and on the waker's CPU or another, so it mixed an on-CPU handoff (about
//!   0.45 µs) with a real wake (about 10 µs); thread placement holds for a whole run, so whole runs split
//!   between the two (a median of 417 ns and of 10,041 ns on the same two cores, a minute apart).
//! - **The placement production runs under** ([`crate::machine::placement`], the rule the runtime places its shards
//!   by). Where the runtime fixes its shards — the process owns its cores and the OS pins (Linux, Windows)
//!   — the waiter is pinned to a shard core and the waker to the control core, one shard core per round; a
//!   sample the OS still ran on one CPU is not the event and is dropped (counted). Pinned this way, on a
//!   quiet host, the probe's median held at 8.9–10.8 µs over ten container runs (two and four CPUs,
//!   2026-09-25) where the old probe's flipped between 416 ns and 10,041 ns. Where production is not fixed
//!   — the process shares its cores in time (a CPU quota below its cpuset), or the OS will not pin (macOS:
//!   an affinity hint, refused on Apple silicon) — the OS places the pair as it places the shards, so every
//!   sample is kept and the same-CPU share is recorded (63–92 % of wakes on Apple silicon ran on the
//!   waker's CPU across the prototype's and the probe's runs; the probe's pooled mean held at 2.03–2.49 µs
//!   over five runs).
//! - **The mean, not the median.** The spin-then-park rule spins for the expected cost of parking [A:
//!   Karlin, Manasse, McGeoch & Owicki, "Competitive randomized algorithms for nonuniform problems",
//!   Algorithmica 1994], and the heavy tail is part of that cost: on the same two pinned cores a spinning
//!   waiter's p99 was at most 209 ns while a parked one's was 285–639 µs (a halted virtual CPU waits on the
//!   host's scheduler), so the median understated the expected cost about threefold (mean 31–44 µs, median
//!   11–13 µs). The probe stops when the mean's 95 % bootstrap interval is within the crate's stopping-rule
//!   width ([`crate::machine::stats::CONVERGED_WIDTH_PERMILLE`]) — the statistic consumed, not the median — or at
//!   its wall budget, reported `quick`. A heavy tail converges slowly (a coefficient of variation of five
//!   to seven on a virtual machine needs 38,000–75,000 wakes for an interval ten percent wide, where
//!   250 ms buys three to six thousand), which is why the
//!   runtime refines this estimate from its own wakes after boot (`slates-rt`'s wake-cost estimate).
//! - **Rounds.** [`WAKE_ROUNDS`] rounds, each with a fresh waiter thread (and, pinned, its own shard core).
//!   A run-long mode is invisible to a within-run interval — every old run converged tightly and runs still
//!   differed 23× — so the rounds are compared, and disagree when a round's median interval misses the
//!   pooled one by more than the probe's own precision ([`rounds_agree`]). Medians, not means: a placement
//!   or state mode moves the median by multiples (0.4 µs against 10 µs), while a heavy tail's rare events
//!   scatter a short round's mean without moving its median — on the Linux containers the round means
//!   disagreed in six runs of eight with the pair pinned, and no mode in sight. Round medians still
//!   disagree when the host's own load drifts during the probe (five runs of ten on a desktop VM): that is
//!   reported, the profile lists the probe as degraded (`wake.rounds`), and nothing is sized from it.
//! - **The p99 is kept for its one consumer**: the runtime's inbound ring, sized against the tail at its
//!   overflow target (§4.1 "ring depth"). Everything else derives from the mean.
#![allow(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::machine::bench::{MIN_SAMPLES, nanos};
use crate::machine::error::MachineError;
use crate::machine::placement::Placement;
use crate::machine::probes::{
    Pinning, SavedAffinity, pin_current_thread, pinning_code, pinning_from_code, weaker,
};
use crate::machine::stats::{
    CONVERGED_WIDTH_PERMILLE, Interval, MeanInterval, Percentile, Sample, Xorshift,
    bootstrap_interval, bootstrap_mean_interval, converged_mean, standard_deviation,
};

/// The wake latency the profile reports (§4.1) and every derivation reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeLatency {
    /// The mean wake, nanoseconds: the expected cost of parking, the statistic every consumer derives from.
    pub mean_ns: u64,
    /// The lower edge of the mean's 95 % bootstrap interval.
    pub mean_lower_ns: u64,
    /// The upper edge.
    pub mean_upper_ns: u64,
    /// The median of the kept samples, for the record.
    pub p50_ns: u64,
    /// The p99 of the kept samples: what the inbound ring is sized against at its overflow target.
    pub p99_ns: u64,
    /// The population standard deviation of the kept samples: with the mean, how many wakes an online
    /// estimate needs to reach the probe's precision ([`WakeLatency::estimate_shift`]).
    pub sd_ns: u64,
    /// Samples kept, pooled over the rounds.
    pub samples: u32,
    /// Wakes whose waker and waiter ran on one CPU: dropped when the pair was pinned to two cores (a pin
    /// the OS did not honour is not the event), kept when the OS placed the threads itself.
    pub same_cpu_samples: u32,
    /// Rounds measured, each with a fresh waiter thread: [`WAKE_ROUNDS`], or more when the planned rounds
    /// kept fewer than [`MIN_SAMPLES`] between them ([`wake`]).
    pub rounds: u32,
    /// Whether every round's median interval reaches the pooled one within the probe's precision
    /// ([`rounds_agree`]); `None` when fewer than two rounds kept enough samples to judge.
    pub rounds_agree: Option<bool>,
    /// How the waker and waiter were placed, the weakest over the rounds.
    pub placement: Pinning,
    /// Whether the waiter was confirmed asleep in the kernel before every wake, rather than having only
    /// announced its park (Windows, or a thread query the OS refused).
    pub asleep_confirmed: bool,
    /// True when the budget ended before the pooled mean's interval converged.
    pub quick: bool,
}

/// Format: the standard normal quantile of the two-sided 95 % interval every probe reports
/// ([`Percentile::LOWER_95`] and [`Percentile::UPPER_95`]), in thousandths: 1.960.
const Z_95_PERMILLE: u128 = 1960;

impl WakeLatency {
    /// How many wakes an online estimate of the mean needs to reach the probe's own precision:
    /// `N = (z · sd / (h · mean))²`, with `z` the 95 % normal quantile and `h` half the stopping-rule width
    /// ([`CONVERGED_WIDTH_PERMILLE`]) — the sample size at which the mean's interval is as narrow as the
    /// probe asks of itself. At least [`MIN_SAMPLES`]; a probe with no spread (or no mean) asks no more.
    /// A coefficient of variation of six (a virtual machine's) needs about 55,000 wakes; one and a half
    /// (Apple silicon) about 3,500.
    pub fn estimate_window(&self) -> u64 {
        let half_width_permille = u128::from(CONVERGED_WIDTH_PERMILLE / 2).max(1);
        let spread = u128::from(self.sd_ns).saturating_mul(Z_95_PERMILLE);
        let scale = u128::from(self.mean_ns).saturating_mul(half_width_permille);
        let floor = u64::try_from(MIN_SAMPLES).unwrap_or(u64::MAX);
        if scale == 0 {
            return floor;
        }
        let ratio = spread.div_ceil(scale);
        u64::try_from(ratio.saturating_mul(ratio))
            .unwrap_or(u64::MAX)
            .max(floor)
    }

    /// The exponential-weighting shift of an online estimate over [`estimate_window`](Self::estimate_window)
    /// wakes: `⌈log₂ N⌉`, capped so the fixed-point accumulator of [`WakeEstimate`] (a mean shifted left by
    /// it) fits 128 bits.
    pub fn estimate_shift(&self) -> u32 {
        let window = self.estimate_window();
        let shift = window
            .saturating_sub(1)
            .checked_ilog2()
            .map_or(0, |bits| bits.saturating_add(1));
        shift.min(WakeEstimate::MAX_SHIFT)
    }
}

/// An online estimate of the mean wake (§4.1, §4.3, §4.7): an exponentially weighted mean over about
/// `2^shift` wakes, seeded with the boot probe's mean and fed each wake the product measures afterwards
/// (the runtime's kicked parks, a client's woken waits), so a machine whose neighbours change after boot
/// is tracked rather than frozen at one 250 ms sample. Kept in fixed point — the accumulator is the mean
/// shifted left by `shift` — so a small correction is never rounded away: an integer mean updated by
/// `(sample − mean) >> shift` drops every deviation under `2^shift`, keeps the tail's large ones, and
/// drifts upward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeEstimate {
    scaled: u128,
    shift: u32,
    samples: u64,
}

impl WakeEstimate {
    /// Format: the largest shift whose accumulator — a `u64` mean shifted left — fits 128 bits.
    pub const MAX_SHIFT: u32 = 63;

    /// An estimate seeded with `prior_ns`, weighting about `2^shift` wakes.
    pub fn new(prior_ns: u64, shift: u32) -> WakeEstimate {
        let shift = shift.min(Self::MAX_SHIFT);
        WakeEstimate {
            scaled: u128::from(prior_ns) << shift,
            shift,
            samples: 0,
        }
    }

    /// Folds one measured wake in.
    pub fn record(&mut self, wake_ns: u64) {
        self.scaled = self
            .scaled
            .saturating_sub(self.scaled >> self.shift)
            .saturating_add(u128::from(wake_ns));
        self.samples = self.samples.saturating_add(1);
    }

    /// The estimated mean wake, nanoseconds.
    pub fn mean_ns(&self) -> u64 {
        u64::try_from(self.scaled >> self.shift).unwrap_or(u64::MAX)
    }

    /// Wakes folded in since the seed (a non-vacuity count).
    pub fn samples(&self) -> u64 {
        self.samples
    }
}

/// Shape: the rounds a wake probe splits its budget into. Five, from the split it must expose: the
/// 2026-09-22 container runs landed in the fast placement mode in two runs of five, and five independent
/// rounds show both modes with probability `1 − (0.6⁵ + 0.4⁵) ≈ 91 %`.
pub const WAKE_ROUNDS: u32 = 5;

/// Format: the CPU number a thread could not learn.
const UNKNOWN_CPU: u32 = u32::MAX;

/// Format: the thread id a waiter has not yet published (no OS numbers a thread `u64::MAX`).
const UNPUBLISHED: u64 = u64::MAX;

/// Format: the `turn` word's first state — the waiter idle.
const IDLE: u32 = 0;
/// Format: the `turn` word's second state — the waker's go.
const GO: u32 = 1;
/// Format: the `turn` word's third state — the waiter's done.
const DONE: u32 = 2;

/// Measures the wake latency within `budget`, placed as `placement` places the shards (the profile's facts).
///
/// The [`WAKE_ROUNDS`] rounds share the budget. A round whose setup — a fresh waiter scheduled and
/// confirmed asleep — outlasts its share on a loaded machine keeps nothing, and five such rounds once left
/// the probe with no sample at all, reported as a zero mean that every consumer derives from (a zero spin
/// window, a one-nanosecond step quantum; CI run 36201648573, a 30 ms budget on a macOS runner). So while
/// the pooled sample is under the stopping rule's floor ([`MIN_SAMPLES`]), up to [`WAKE_ROUNDS`] more rounds
/// run, each with twice the last one's budget: a slow machine gets exponentially more time, within
/// `2 + 4 + 8 + 16 + 32 = 62` more shares. Short of the floor even then, the probe refuses
/// `MeasurementTimeout` rather than report a mean it did not measure.
pub fn wake(budget: Duration, placement: &Placement) -> Result<WakeLatency, MachineError> {
    let pairs = core_pairs(placement);
    let saved = SavedAffinity::of_calling_thread();
    let mut round_budget = budget.checked_div(WAKE_ROUNDS).unwrap_or(budget);
    let mut rng = Xorshift::new(Xorshift::SEED);
    let mut kept: Vec<u64> = Vec::new();
    let mut round_medians: Vec<Interval> = Vec::new();
    let mut placement = Pinning::Pinned;
    let mut same_cpu_samples = 0u32;
    let mut asleep_confirmed = true;
    let mut rounds = 0u32;
    while rounds < WAKE_ROUNDS
        || (kept.len() < MIN_SAMPLES && rounds < WAKE_ROUNDS.saturating_mul(2))
    {
        if rounds >= WAKE_ROUNDS {
            round_budget = round_budget.saturating_mul(2);
        }
        let pair = usize::try_from(rounds)
            .ok()
            .and_then(|round| pairs.get(round.checked_rem(pairs.len().max(1)).unwrap_or(0)))
            .copied();
        rounds = rounds.saturating_add(1);
        let outcome = wake_round(pair, round_budget, &mut rng);
        placement = weaker(placement, outcome.placement);
        same_cpu_samples = same_cpu_samples.saturating_add(outcome.same_cpu);
        asleep_confirmed &= outcome.asleep_confirmed;
        if outcome.samples.len() >= MIN_SAMPLES
            && let Some(interval) =
                bootstrap_interval(&Sample::new(outcome.samples.clone()), &mut rng)
        {
            round_medians.push(interval);
        }
        kept.extend(outcome.samples);
    }
    if !saved.restore() {
        // The calling thread (the anchor's) could not be given its mask back: nothing measured after this
        // can vouch for its placement, and the profile says so (the core matrix's rule).
        placement = Pinning::Refused;
    }
    let pooled = (kept.len() >= MIN_SAMPLES)
        .then(|| bootstrap_mean_interval(&kept, &mut rng))
        .flatten()
        .ok_or(MachineError::MeasurementTimeout { probe: "wake" })?;
    Ok(summarize(
        &kept,
        pooled,
        &round_medians,
        Tally {
            placement,
            same_cpu_samples,
            asleep_confirmed,
            rounds,
        },
        &mut rng,
    ))
}

/// What the rounds found beside their samples.
struct Tally {
    placement: Pinning,
    same_cpu_samples: u32,
    asleep_confirmed: bool,
    /// The rounds run: [`WAKE_ROUNDS`], or more when the planned ones stayed under the floor.
    rounds: u32,
}

/// The pooled result: the mean with its interval, the quantiles for the record, and the rounds' verdict.
fn summarize(
    kept: &[u64],
    pooled: MeanInterval,
    round_medians: &[Interval],
    tally: Tally,
    rng: &mut Xorshift,
) -> WakeLatency {
    let sorted = Sample::new(kept.to_vec());
    let pooled_median = bootstrap_interval(&sorted, rng);
    WakeLatency {
        mean_ns: pooled.mean,
        mean_lower_ns: pooled.lower,
        mean_upper_ns: pooled.upper,
        p50_ns: sorted.median().unwrap_or(0),
        p99_ns: sorted.percentile(Percentile::P99).unwrap_or(0),
        sd_ns: standard_deviation(kept).unwrap_or(0),
        samples: u32::try_from(kept.len()).unwrap_or(u32::MAX),
        same_cpu_samples: tally.same_cpu_samples,
        rounds: tally.rounds,
        rounds_agree: pooled_median.and_then(|pooled| rounds_agree(round_medians, &pooled)),
        placement: tally.placement,
        asleep_confirmed: tally.asleep_confirmed,
        quick: !converged_mean(&pooled),
    }
}

/// Whether the rounds measured one thing: every round's median interval reaches the pooled median's,
/// widened by the probe's own precision — the stopping-rule width ([`CONVERGED_WIDTH_PERMILLE`] of the
/// pooled median), a difference the probe does not claim to resolve. `None` when fewer than two rounds
/// could be judged. Sampling noise, a mixture that shifts a few percent between rounds (tight Apple-silicon
/// rounds about ten percent apart) and a heavy tail's rare events (which scatter a short round's mean, not
/// its median) do not flag it; a run-long mode — a round several times faster or slower — does.
pub fn rounds_agree(round_medians: &[Interval], pooled: &Interval) -> Option<bool> {
    let tolerance = u64::try_from(
        u128::from(pooled.median)
            .saturating_mul(u128::from(CONVERGED_WIDTH_PERMILLE))
            .checked_div(u128::from(PERMILLE))
            .unwrap_or(0),
    )
    .unwrap_or(u64::MAX);
    let lower = pooled.lower.saturating_sub(tolerance);
    let upper = pooled.upper.saturating_add(tolerance);
    (round_medians.len() >= 2).then(|| {
        round_medians
            .iter()
            .all(|round| round.lower <= upper && lower <= round.upper)
    })
}

/// Format: parts per thousand.
const PERMILLE: u64 = 1000;

/// The (waker, waiter) core pairs, one per round in turn, where production fixes its shards: the waiter on
/// each shard core, the waker where production's wakes come from: the first reserved core when the
/// consumer reserves one, else the next shard's core (a wake from a sibling shard), both on the one core a
/// one-core class shares. Empty where production does not fix them: the OS then places the pair as it
/// places the shards.
pub fn core_pairs(placement: &Placement) -> Vec<(u32, u32)> {
    placement.fixed.as_ref().map_or_else(Vec::new, |fixed| {
        let mut next = fixed.shards.iter().cycle().skip(1);
        fixed
            .shards
            .iter()
            .map(|shard| {
                let waker = fixed
                    .reserved
                    .first()
                    .copied()
                    .or_else(|| next.next().copied())
                    .unwrap_or(*shard);
                (waker, *shard)
            })
            .collect()
    })
}

/// One round's outcome.
struct Round {
    samples: Vec<u64>,
    same_cpu: u32,
    placement: Pinning,
    asleep_confirmed: bool,
}

/// The words the waker and the waiter share for one round.
struct Shared {
    /// The waker's send stamp, replaced by the waiter with the latency it measured.
    stamp: AtomicU64,
    /// [`IDLE`], [`GO`] or [`DONE`].
    turn: AtomicU32,
    /// Tells the waiter to leave.
    stop: AtomicBool,
    /// The waiter's announcement that it is about to park (the stand-in where the OS cannot be asked).
    parking: AtomicBool,
    /// The waiter's thread id for the asleep query, [`UNPUBLISHED`] until it runs.
    waiter: AtomicU64,
    /// The CPU the waiter woke on, [`UNKNOWN_CPU`] when the OS does not say.
    waiter_cpu: AtomicU32,
    /// The waiter's pin, as a code.
    waiter_pin: AtomicU32,
}

/// One round: a fresh waiter, pinned with the waker to `pair` when there is one, timed until its mean
/// converges or `budget` ends.
#[allow(
    clippy::disallowed_methods,
    reason = "the wake probe measures the host: a real thread parked and woken on the host clock (docs/runtime.md §10.2)"
)]
fn wake_round(pair: Option<(u32, u32)>, budget: Duration, rng: &mut Xorshift) -> Round {
    let started = Instant::now();
    let waker_pin = pair.map_or(Pinning::Scheduled, |(waker, _)| pin_current_thread(waker));
    let shared = Shared {
        stamp: AtomicU64::new(0),
        turn: AtomicU32::new(IDLE),
        stop: AtomicBool::new(false),
        parking: AtomicBool::new(false),
        waiter: AtomicU64::new(UNPUBLISHED),
        waiter_cpu: AtomicU32::new(UNKNOWN_CPU),
        waiter_pin: AtomicU32::new(pinning_code(Pinning::Refused)),
    };
    let epoch = Instant::now();
    let waker = std::thread::current();
    let mut round = Round {
        samples: Vec::new(),
        same_cpu: 0,
        placement: waker_pin,
        asleep_confirmed: true,
    };
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| waiter_loop(&shared, pair, epoch, &waker));
        let Some(id) = await_waiter(&shared, started, budget) else {
            shared.stop.store(true, Ordering::Release);
            waiter.thread().unpark();
            return;
        };
        let waiter_pin = pinning_from_code(shared.waiter_pin.load(Ordering::Acquire));
        round.placement = weaker(waker_pin, waiter_pin);
        // Only a pair pinned to two distinct cores defines the event as cross-CPU; unpinned (or one core),
        // wherever the OS ran the waiter is the placement production gets.
        let two_cores = pair.is_some_and(|(a, b)| a != b) && round.placement == Pinning::Pinned;
        let mut next_check = MIN_SAMPLES;
        while started.elapsed() < budget {
            let Some(confirmed) = await_asleep(&shared, id, started, budget) else {
                break;
            };
            round.asleep_confirmed &= confirmed;
            let Some((latency, one_cpu)) =
                time_one_wake(&shared, waiter.thread(), started, budget, epoch)
            else {
                break;
            };
            if one_cpu {
                round.same_cpu = round.same_cpu.saturating_add(1);
                if two_cores {
                    continue;
                }
            }
            round.samples.push(latency);
            if round.samples.len() >= next_check {
                next_check = next_check.saturating_mul(2);
                if bootstrap_mean_interval(&round.samples, rng).is_some_and(|i| converged_mean(&i))
                {
                    break;
                }
            }
        }
        shared.stop.store(true, Ordering::Release);
        waiter.thread().unpark();
    });
    round
}

/// The waiter: pins itself, publishes its id, then wakes on each [`GO`], stamps the latency and hands
/// the turn back, parking in between; leaves on `stop`.
fn waiter_loop(
    shared: &Shared,
    pair: Option<(u32, u32)>,
    epoch: Instant,
    waker: &std::thread::Thread,
) {
    let pin = pair.map_or(Pinning::Scheduled, |(_, waiter)| pin_current_thread(waiter));
    shared
        .waiter_pin
        .store(pinning_code(pin), Ordering::Release);
    shared
        .waiter
        .store(platform::thread_id(), Ordering::Release);
    loop {
        while shared.turn.load(Ordering::Acquire) != GO {
            if shared.stop.load(Ordering::Acquire) {
                return;
            }
            shared.parking.store(true, Ordering::Release);
            std::thread::park();
            shared.parking.store(false, Ordering::Release);
        }
        let woke = nanos(epoch.elapsed());
        shared.waiter_cpu.store(
            platform::current_cpu().unwrap_or(UNKNOWN_CPU),
            Ordering::Release,
        );
        let sent = shared.stamp.load(Ordering::Acquire);
        shared
            .stamp
            .store(woke.saturating_sub(sent), Ordering::Release);
        shared.turn.store(DONE, Ordering::Release);
        waker.unpark();
    }
}

/// Waits for the waiter to publish its id; `None` when the round's budget ends first.
fn await_waiter(shared: &Shared, started: Instant, budget: Duration) -> Option<u64> {
    loop {
        let id = shared.waiter.load(Ordering::Acquire);
        if id != UNPUBLISHED {
            return Some(id);
        }
        if started.elapsed() >= budget {
            return None;
        }
        std::thread::yield_now();
    }
}

/// Waits until the waiter is asleep: confirmed by the OS (`Some(true)`), or — where the OS cannot be
/// asked — announced by the waiter itself (`Some(false)`). `None` when the round's budget ends first.
fn await_asleep(shared: &Shared, id: u64, started: Instant, budget: Duration) -> Option<bool> {
    loop {
        match platform::is_waiting(id) {
            Some(true) => return Some(true),
            Some(false) => {}
            None if shared.parking.load(Ordering::Acquire) => return Some(false),
            None => {}
        }
        if started.elapsed() >= budget {
            return None;
        }
        std::thread::yield_now();
    }
}

/// Wakes the sleeping waiter and waits for its answer: the latency it measured and whether it woke on
/// the waker's CPU. `None` when the answer does not come within the round's budget (the waker's wait is
/// bounded, never an open park).
fn time_one_wake(
    shared: &Shared,
    waiter: &std::thread::Thread,
    started: Instant,
    budget: Duration,
    epoch: Instant,
) -> Option<(u64, bool)> {
    let waker_cpu = platform::current_cpu();
    shared
        .stamp
        .store(nanos(epoch.elapsed()), Ordering::Release);
    shared.turn.store(GO, Ordering::Release);
    waiter.unpark();
    while shared.turn.load(Ordering::Acquire) != DONE {
        let remaining = budget.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return None;
        }
        std::thread::park_timeout(remaining);
    }
    let latency = shared.stamp.load(Ordering::Acquire);
    let woke_on = shared.waiter_cpu.load(Ordering::Acquire);
    shared.turn.store(IDLE, Ordering::Release);
    let one_cpu = waker_cpu.is_some_and(|cpu| cpu != UNKNOWN_CPU && cpu == woke_on);
    Some((latency, one_cpu))
}

#[cfg(target_os = "linux")]
mod platform {
    /// The CPU the calling thread runs on.
    pub(super) fn current_cpu() -> Option<u32> {
        u32::try_from(rustix::thread::sched_getcpu()).ok()
    }

    /// The calling thread's kernel id (what `/proc/self/task` is keyed by).
    pub(super) fn thread_id() -> u64 {
        u64::try_from(rustix::thread::gettid().as_raw_nonzero().get()).unwrap_or(0)
    }

    /// Whether thread `id` is asleep in the kernel: its `/proc` state is `S` (interruptible sleep, where a
    /// futex wait sits). The state follows the command name, which may itself hold `)`, so the last `") "`
    /// is the boundary. A read of a kernel pseudo-file, never a disk read (R1).
    pub(super) fn is_waiting(id: u64) -> Option<bool> {
        let text = std::fs::read_to_string(format!("/proc/self/task/{id}/stat")).ok()?;
        let (_, rest) = text.rsplit_once(") ")?;
        Some(rest.starts_with('S'))
    }
}

#[cfg(target_os = "macos")]
mod platform {
    /// The CPU the calling thread runs on (`pthread_cpu_number_np`, macOS 11 and later).
    pub(super) fn current_cpu() -> Option<u32> {
        let mut cpu: libc::size_t = 0;
        // SAFETY: a writable size_t receives the calling thread's CPU number; the call has no other effect.
        let rc = unsafe { libc::pthread_cpu_number_np(&raw mut cpu) };
        if rc == 0 {
            u32::try_from(cpu).ok()
        } else {
            None
        }
    }

    /// The calling thread's mach port, which `thread_info` takes; the pthread owns it (no new right).
    pub(super) fn thread_id() -> u64 {
        // SAFETY: the calling thread's own pthread handle, alive for the duration of the call.
        u64::from(unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) })
    }

    /// Whether thread `id` (a mach port) is waiting in the kernel: `thread_info`'s basic run state is
    /// `TH_STATE_WAITING`.
    pub(super) fn is_waiting(id: u64) -> Option<bool> {
        let port = libc::thread_act_t::try_from(id).ok()?;
        let flavor = libc::thread_flavor_t::try_from(libc::THREAD_BASIC_INFO).ok()?;
        let idle = libc::time_value_t {
            seconds: 0,
            microseconds: 0,
        };
        let mut info = libc::thread_basic_info {
            user_time: idle,
            system_time: idle,
            cpu_usage: 0,
            policy: 0,
            run_state: 0,
            flags: 0,
            suspend_count: 0,
            sleep_time: 0,
        };
        let mut count = libc::THREAD_BASIC_INFO_COUNT;
        // SAFETY: `port` names a live thread of this process (the waiter joins only after the waker stops
        // asking); `info` is a writable thread_basic_info and `count` its size in integers, as the flavor
        // requires.
        let rc = unsafe {
            libc::thread_info(
                port,
                flavor,
                (&raw mut info).cast::<libc::integer_t>(),
                &raw mut count,
            )
        };
        (rc == libc::KERN_SUCCESS).then_some(info.run_state == libc::TH_STATE_WAITING)
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
mod platform {
    /// No query for the running CPU here.
    pub(super) fn current_cpu() -> Option<u32> {
        None
    }

    /// No thread query here, so no id to give one.
    pub(super) fn thread_id() -> u64 {
        0
    }

    /// No per-thread state query here: the waiter's announcement stands in.
    pub(super) fn is_waiting(_id: u64) -> Option<bool> {
        None
    }
}

#[cfg(windows)]
mod platform {
    /// The processor the calling thread runs on.
    pub(super) fn current_cpu() -> Option<u32> {
        // SAFETY: no preconditions; returns the calling thread's processor number within its group.
        Some(unsafe { windows_sys::Win32::System::Threading::GetCurrentProcessorNumber() })
    }

    /// No per-thread state query is used on Windows, so no id is needed.
    pub(super) fn thread_id() -> u64 {
        0
    }

    /// Windows names a thread's wait state only in a system-wide process snapshot; the waiter's own
    /// announcement stands in, and the result reports the wait as unconfirmed.
    pub(super) fn is_waiting(_id: u64) -> Option<bool> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::facts::{CoreClass, CoreFacts, CpuBudget, Facts};

    fn core(id: u32, level: u32) -> CoreFacts {
        CoreFacts {
            id,
            class: CoreClass::Unknown,
            level,
            numa: 0,
            l2_bytes: 0,
        }
    }

    /// The placement the profile measures the wake under: the facts' own.
    fn placement_of(facts: &Facts) -> Placement {
        Placement::of(&facts.cores, facts.cpu_budget, 1)
    }

    /// §4.1, D-10 (the placement production runs under): where the process owns its cores, the waker takes
    /// the fastest class's first core (the control core) and the waiter each other core of that class in
    /// turn (the shard cores), as the runtime places them; a one-core class shares its core; slower classes
    /// are never used. Where the process shares its cores in time (a quota below its cpuset) production
    /// fixes nothing, so neither does the probe; no facts, no pairs.
    #[test]
    fn the_pairs_follow_the_runtimes_shard_placement() {
        let mixed = [core(4, 1), core(2, 0), core(0, 0), core(5, 1), core(1, 0)];
        assert_eq!(
            core_pairs(&Placement::of(&mixed, None, 1)),
            vec![(0, 1), (0, 2)]
        );
        assert_eq!(
            core_pairs(&Placement::of(&[core(3, 0), core(1, 1)], None, 1)),
            vec![(3, 3)]
        );
        let shared = CpuBudget {
            quota_us: 200_000,
            period_us: 100_000,
        };
        assert_eq!(
            core_pairs(&Placement::of(&mixed, Some(shared), 1)),
            Vec::<(u32, u32)>::new()
        );
        assert_eq!(
            core_pairs(&Placement::of(&[], None, 1)),
            Vec::<(u32, u32)>::new()
        );
    }

    /// The rounds' verdict by use: rounds drawn from one distribution agree; a round stuck in another mode
    /// (a decade faster, with a tight interval — the fast handoff the old probe mixed in) does not; a shift
    /// within the probe's own precision (tight rounds ten percent apart) is not a split, while a round at
    /// half or twice the pooled median is; one round alone cannot be judged.
    #[test]
    fn rounds_in_two_modes_disagree_and_rounds_of_one_do_not() {
        let pooled = Interval {
            median: 10_000,
            lower: 9_000,
            upper: 11_500,
        };
        let near = |median: u64| Interval {
            median,
            lower: median - median / 10,
            upper: median + median / 10,
        };
        assert_eq!(
            rounds_agree(&[near(9_800), near(10_400), near(10_900)], &pooled),
            Some(true)
        );
        assert_eq!(
            rounds_agree(&[near(10_100), near(450)], &pooled),
            Some(false)
        );
        assert_eq!(rounds_agree(&[near(10_100)], &pooled), None);
        let tight = |median: u64| Interval {
            median,
            lower: median - median / 50,
            upper: median + median / 50,
        };
        assert_eq!(
            rounds_agree(&[tight(9_400), tight(10_400), tight(12_200)], &pooled),
            Some(true)
        );
        assert_eq!(
            rounds_agree(&[tight(10_000), tight(20_000)], &pooled),
            Some(false)
        );
        assert_eq!(
            rounds_agree(&[tight(10_000), tight(5_000)], &pooled),
            Some(false)
        );
    }

    /// The online estimate by use: a constant stream converges to its value exactly (no rounding drift);
    /// from a stale prior it moves to the stream's mean; a stream with a rare large wake settles at the
    /// stream's true mean, not above it (the drift an integer update without fixed point shows).
    #[test]
    fn the_online_estimate_tracks_the_mean_without_rounding_drift() {
        let mut steady = WakeEstimate::new(10_000, 8);
        for _ in 0..10_000 {
            steady.record(10_000);
        }
        assert_eq!(steady.mean_ns(), 10_000);
        let mut moved = WakeEstimate::new(1_000, 6);
        for _ in 0..4_096 {
            moved.record(50_000);
        }
        assert!(moved.mean_ns() > 49_000, "{}", moved.mean_ns());
        // One wake in sixteen held up to 160 µs, the rest 10 µs: a true mean of 19,375 ns.
        let mut tailed = WakeEstimate::new(10_000, 10);
        for k in 0..400_000u64 {
            tailed.record(if k % 16 == 0 { 160_000 } else { 10_000 });
        }
        let mean = tailed.mean_ns();
        assert!((18_500..=20_500).contains(&mean), "{mean}");
        assert_eq!(tailed.samples(), 400_000);
    }

    /// The window follows the probe's spread: `(1.96 · cv / 0.05)²` wakes, at least the probe's minimum,
    /// its shift the ceiling of its binary logarithm.
    #[test]
    fn the_estimate_window_follows_the_probes_spread() {
        let probe = |mean_ns, sd_ns| WakeLatency {
            mean_ns,
            mean_lower_ns: mean_ns,
            mean_upper_ns: mean_ns,
            p50_ns: mean_ns,
            p99_ns: mean_ns,
            sd_ns,
            samples: 1,
            same_cpu_samples: 0,
            rounds: WAKE_ROUNDS,
            rounds_agree: None,
            placement: Pinning::Pinned,
            asleep_confirmed: true,
            quick: false,
        };
        // A coefficient of variation of six: (1.96 × 6 / 0.05)² = 235.2² → 236² = 55,696.
        assert_eq!(probe(10_000, 60_000).estimate_window(), 55_696);
        assert_eq!(probe(10_000, 60_000).estimate_shift(), 16);
        // One and a half: 58.8² → 59² = 3,481.
        assert_eq!(probe(2_000, 3_000).estimate_window(), 3_481);
        assert_eq!(probe(2_000, 3_000).estimate_shift(), 12);
        // No spread, or no mean: the probe's minimum.
        let floor = u64::try_from(MIN_SAMPLES).unwrap();
        assert_eq!(probe(10_000, 0).estimate_window(), floor);
        assert_eq!(probe(0, 5_000).estimate_window(), floor);
    }

    /// §4.1 (docs/bugs/2026-09-22-wake-probe-mixes-two-events-and-reports-an-unconverged-tail.md) by use on
    /// this machine: the probe reports a mean inside its interval from its rounds, with the median and p99
    /// beside it. Where the OS pins and the fastest class has two cores (Linux, Windows) the pair was pinned
    /// and every kept wake crossed CPUs — so the handoff the old probe mixed in cannot enter; where it can
    /// query a thread's state (Linux, macOS) every waiter was confirmed asleep before its wake. The calling
    /// thread gets its mask back (the core matrix's rule).
    #[test]
    fn a_wake_is_a_confirmed_sleepers_wake_on_the_production_placement() {
        let facts = Facts::query();
        let before = std::thread::available_parallelism().map_or(1, |n| n.get());
        let placement = placement_of(&facts);
        let w = wake(Duration::from_millis(100), &placement).expect("the probe measures a wake");
        let after = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(
            after, before,
            "the calling thread's usable parallelism is unchanged"
        );
        assert_a_measured_mean(&w);
        let pairs = core_pairs(&placement);
        if pairs.is_empty() {
            // Production fixes nothing here (a pool shared in time), so the probe pinned nothing and says so,
            // rather than calling the OS's placement a refusal.
            assert_eq!(w.placement, Pinning::Scheduled, "{w:?}");
        }
        let pinnable = cfg!(any(target_os = "linux", windows));
        if pinnable && pairs.first().is_some_and(|(a, b)| a != b) && w.placement == Pinning::Pinned
        {
            // A hard pin to two distinct cores leaves the waiter no way onto the waker's CPU: no wake the
            // probe kept or dropped ran on one CPU.
            assert_eq!(w.same_cpu_samples, 0, "{w:?}");
        }
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(w.asleep_confirmed, "{w:?}");
        }
    }

    /// A wake that was measured: at least the planned rounds, at least the stopping rule's floor of samples,
    /// a positive mean inside its own interval, and quantiles in order.
    fn assert_a_measured_mean(w: &WakeLatency) {
        assert!(w.rounds >= WAKE_ROUNDS, "{w:?}");
        assert!(w.samples >= u32::try_from(MIN_SAMPLES).unwrap(), "{w:?}");
        assert!(
            w.mean_lower_ns <= w.mean_ns && w.mean_ns <= w.mean_upper_ns && w.mean_ns > 0,
            "{w:?}"
        );
        assert!(w.p50_ns > 0 && w.p99_ns >= w.p50_ns, "{w:?}");
    }

    /// §4.1 refusals: a probe that measured no wake reports none. Given no time, no round can confirm a
    /// sleeper, the extension rounds (twice nothing is nothing) cannot either, and the probe refuses by name
    /// rather than report a zero mean for its consumers to derive from; the calling thread's mask comes back
    /// all the same.
    #[test]
    fn a_probe_that_measured_no_wake_refuses_rather_than_report_a_mean() {
        let facts = Facts::query();
        let before = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(
            wake(Duration::ZERO, &placement_of(&facts)),
            Err(MachineError::MeasurementTimeout { probe: "wake" })
        );
        let after = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(after, before);
    }
}
