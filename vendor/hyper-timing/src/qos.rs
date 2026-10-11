//! A link's failure detector and a group's election span, chosen from measurement to minimize the
//! time a group cannot commit (`docs/timing.md` §2.2–§2.3).
//!
//! The detector is Chen, Toueg and Aguilera's NFD-E (DSN 2000; IEEE Transactions on Computers
//! 51(5), 2002): heartbeats every `η`, trusted while one is fresh, freshness at the expected arrival
//! plus a margin `α`. A crash is detected within `E(D) + α + η` (their Theorem 4). Its mistakes are
//! bounded from the measured arrivals alone, through the one-sided (Cantelli) inequality
//! `Pr(X − E ≥ x) ≤ V / (V + x²)`, which holds for any distribution with that mean and variance,
//! **per arrival** ([`configure_arrivals`], [`arrival_detector_at`], [`lateness_bound`]): a
//! freshness point errs only when the heartbeat taken after it comes past it, so the detector errs
//! at most once a heartbeat taken, with chance at most `β(α) = u + (1 − u)·V/(V + (α − μ)²)` for
//! `α > μ`, over the lateness `ℓ` of each heartbeat taken past its expected arrival (mean `μ`,
//! variance `V`) and the chance `u` that one is past every lateness seen ([`Arrivals`],
//! `docs/timing.md` §2.2). One factor, whatever the heartbeats the margin holds: it assumes no
//! independence between them, so any margin is admitted, and a heartbeat the sender skipped or the
//! network lost is the next one's lateness. The node-pair stream's detector is configured so, and
//! a SWIM member's probe detector too (`hyper-swim`, §2.7): a pair's probes never overlap, so each
//! is judged alone, with the round trip's lateness from its mean and the loss in place of `u`.
//!
//! Theorem 7's product, `β = Π_{j=0}^{k₀} (V + p_L·x_j²) / (V + x_j²)`, `x_j = α − jη`, over the
//! heartbeats still fresh, `k₀ = ⌈α/η⌉ − 1` ([`mistake_bound`]), takes the heartbeats in the
//! margin as independent, which the traces refute below the correlation time `T_c`; it configures
//! nothing, and the trace analyser reports it beside the per-arrival bound.
//!
//! Chen et al. configure `η` and `α` from requirements an application states. Here they minimize
//! what those requirements stand for, a group's expected unavailability
//! `U = (E(D) + α + η + T_E) / MTBF + T_E · β / η`: an election `T_E` after each detected crash of
//! the leader's node, and one after each false suspicion, at most one an interval. By Little's law
//! (Little 1961) `U` is the mean number of elections in progress, so it bounds the share of time
//! one is: at one or more it promises no availability at all, and is no configuration.
//!
//! The election span `W` minimizes the expected time to a leader. A suspicion starts each of the
//! `s` available voters' campaigns after a delay drawn uniformly from `[0, W)`; the vote splits when
//! `c = s − ⌊n/2⌋ + 1` or more of them start within the one-way latency `l` of the first (Ongaro,
//! dissertation §9.2). The spacing of uniform order statistics `T_(c) − T_(1)` is
//! `Beta(c − 1, s − c + 2)` (David and Nagaraja, *Order Statistics*, 3rd ed., 2003, §2.5), whose
//! distribution function at integer parameters is a binomial tail:
//! `Pr(split) = Pr(Binomial(s, l/W) ≥ c − 1)`. Elections needed are geometric (§9.3).
//!
//! Times are seconds as `f64` inside the arithmetic and `Duration` at the edges.

use std::time::Duration;

/// What a link's heartbeats were measured to do.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LinkBehaviour {
    /// The probability a heartbeat is lost, `p_L`, in `[0, 1)`.
    pub loss: f64,
    /// The mean one-way delay `E(D)`.
    pub mean_delay: Duration,
    /// The standard deviation of the one-way delay, `√V(D)`.
    pub delay_deviation: Duration,
}

/// What a link's arrivals were measured to do, for NFD-E judged where it can err
/// (`docs/timing.md` §2.2): the lateness `ℓ` of each heartbeat taken past the expected arrival its
/// predecessor's freshness point was set from. The detector suspects a live sender at that
/// freshness point exactly when `ℓ` passes the margin, once for every heartbeat taken; a heartbeat
/// the sender skipped or the network lost lengthens the next one's `ℓ` and is nothing else.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Arrivals {
    /// The chance the next lateness is past every one the window holds, `1/(m + 1)` over its `m`
    /// independent arrivals (Rényi 1962): what no deviation within the range seen can bound.
    pub unseen: f64,
    /// The mean lateness `μ`, at least zero: a mean below it only tightens Cantelli's bound.
    pub lateness: Duration,
    /// Its standard deviation `√V`.
    pub deviation: Duration,
    /// `E(D)` where the sender's schedule is on this clock, for the detection bound; zero where it
    /// is not.
    pub mean_delay: Duration,
}

/// What an election costs and how often the leader's node fails.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Costs {
    /// The expected time from a suspicion to a new leader, `T_E` ([`election_span`]).
    pub election: Duration,
    /// The mean time between failures of a node, measured from the membership's history.
    pub mtbf: Duration,
}

/// A detector's parameters and what they promise.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Detector {
    /// The interval between heartbeats, `η`.
    pub interval: Duration,
    /// The margin past a heartbeat's expected arrival before it is stale, `α`.
    pub margin: Duration,
    /// The bound on detecting a crash, `E(D) + α + η` (Theorem 4).
    pub detection: Duration,
    /// The bound on how often a false suspicion recurs, `η / β` (Theorem 7).
    pub mistake_recurrence: Duration,
    /// The expected share of time a group cannot commit, `U`.
    pub unavailability: f64,
}

/// A group's election span and the expected time to a leader with it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Span {
    /// The span campaigns are delayed within, `W`.
    pub span: Duration,
    /// The expected time from a suspicion to a leader, `T_E(W)`.
    pub election: Duration,
    /// The probability one attempt splits the vote.
    pub split: f64,
}

/// The golden ratio's inverse, `(√5 − 1) / 2`: the share of a bracket a golden-section search
/// keeps each step (Kiefer 1953).
const GOLDEN: f64 = 0.618_033_988_749_894_9;

/// Golden-section steps that can still shrink a bracket: from the widest `f64` bracket to the
/// narrowest a step at a time by [`GOLDEN`], `(MAX_EXP − MIN_EXP + MANTISSA_DIGITS) · ln 2 /
/// ln(1/GOLDEN)`, about 3,022. A search ends sooner, once the bracket is within its resolution.
fn golden_steps() -> u32 {
    let bits = f64::from(f64::MAX_EXP - f64::MIN_EXP) + f64::from(f64::MANTISSA_DIGITS);
    whole(bits * std::f64::consts::LN_2 / (1.0 / GOLDEN).ln())
}

/// `value` rounded up to a whole count, 0 when it is not a finite non-negative count a `u32`
/// holds.
fn whole(value: f64) -> u32 {
    let up = value.ceil();
    if !(0.0..=f64::from(u32::MAX)).contains(&up) {
        return 0;
    }
    let mut count = 0u32;
    // `up` is a whole number in range: bisect it into a u32 without a narrowing cast.
    let mut bit = 1u32 << 31;
    while bit > 0 {
        let candidate = count | bit;
        if f64::from(candidate) <= up {
            count = candidate;
        }
        bit >>= 1;
    }
    count
}

/// The minimum of `f` on `[low, high]` to within `resolution`, by golden-section search, and the
/// bracket's ends, where the search's interior points never land: the argument and the value. `f`
/// is assumed unimodal on the bracket.
fn minimize(low: f64, high: f64, resolution: f64, f: impl Fn(f64) -> f64) -> (f64, f64) {
    let inside = golden(low, high, resolution, &f);
    [(low, f(low)), (high, f(high))]
        .into_iter()
        .fold(inside, |best, end| if end.1 < best.1 { end } else { best })
}

/// Golden-section search for the minimum of `f` inside `[low, high]`, to within `resolution`.
fn golden(mut low: f64, mut high: f64, resolution: f64, f: &impl Fn(f64) -> f64) -> (f64, f64) {
    let mut a = high - GOLDEN * (high - low);
    let mut b = low + GOLDEN * (high - low);
    let (mut fa, mut fb) = (f(a), f(b));
    for _ in 0..golden_steps() {
        if high - low <= resolution {
            break;
        }
        if fa <= fb {
            high = b;
            b = a;
            fb = fa;
            a = high - GOLDEN * (high - low);
            fa = f(a);
        } else {
            low = a;
            a = b;
            fa = fb;
            b = low + GOLDEN * (high - low);
            fb = f(b);
        }
    }
    if fa <= fb { (a, fa) } else { (b, fb) }
}

/// Theorem 7's `β`: the bound on the probability that every heartbeat still fresh at a freshness
/// point is late or lost, `Π_{j≥0, x_j>0} (V + p_L x_j²)/(V + x_j²)` with `x_j = α − jη`, for margin
/// `alpha`, interval `eta`, variance `variance` and loss `loss`. The product is a ratio of squares, so
/// any one unit serves for the times (seconds, nanoseconds) with its square for the variance. The
/// trace analyser (`crates/hyper-timing-trace`) reports it; no configurator minimizes it, since the
/// product's independence fails below the correlation time (`docs/timing.md` §2.2).
pub fn mistake_bound(loss: f64, variance: f64, eta: f64, alpha: f64) -> f64 {
    beta(loss, variance, eta, alpha)
}

fn beta(loss: f64, variance: f64, eta: f64, alpha: f64) -> f64 {
    let mut product = 1.0;
    let mut x = alpha;
    // The factors grow towards 1 as `x` falls, so the first is the smallest: the product reaches
    // zero early when it does, and nothing after changes it.
    while x > 0.0 && product > 0.0 {
        let square = x * x;
        let denominator = variance + square;
        if denominator <= 0.0 {
            return 0.0;
        }
        product *= (variance + loss * square) / denominator;
        x -= eta;
    }
    product
}

/// A nanosecond in seconds: the resolution of every stamp the estimates are taken from, the
/// nearest margin past a lateness that never varies.
const NANOSECOND: f64 = 1e-9;

/// Bisections that can still shrink a bracket: from the widest `f64` bracket to the narrowest a
/// halving at a time, `MAX_EXP − MIN_EXP + MANTISSA_DIGITS`, 2,098. A search ends sooner, once the
/// bracket is within its resolution.
fn halvings() -> u32 {
    let bits = f64::MAX_EXP
        .saturating_sub(f64::MIN_EXP)
        .saturating_add_unsigned(f64::MANTISSA_DIGITS);
    u32::try_from(bits).unwrap_or(0)
}

/// The bound on the chance that a heartbeat taken comes later than `margin` past its expected
/// arrival (`docs/timing.md` §2.2): the unseen share, and within the range seen Cantelli's one-sided
/// inequality `Pr(m − μ ≥ x) ≤ V/(V + x²)`, which holds for any distribution with that mean and
/// variance and assumes no independence between heartbeats. At or below the mean it bounds
/// nothing. Seconds.
pub fn lateness_bound(arrivals: &Arrivals, margin: Duration) -> f64 {
    arrival_beta(arrivals, margin.as_secs_f64())
}

fn arrival_beta(arrivals: &Arrivals, alpha: f64) -> f64 {
    let unseen = arrivals.unseen.clamp(0.0, 1.0);
    let past = alpha - arrivals.lateness.as_secs_f64();
    if past <= 0.0 {
        return 1.0;
    }
    let variance = arrivals.deviation.as_secs_f64().powi(2);
    let tail = variance / (variance + past * past);
    (unseen + (1.0 - unseen) * tail).clamp(0.0, 1.0)
}

/// The expected share of time a group cannot commit with detector `(eta, alpha)` on `arrivals`,
/// seconds: a crash detected within `E(D) + α + η` and then an election, once per MTBF; and an
/// election after each mistake, at most one a heartbeat taken, so at most one every `η`, each with
/// chance at most `β(α)`. By Little's law (Little 1961) it is the mean number of elections in
/// progress, so it also bounds the share of time one is: one or more promises nothing.
fn arrival_unavailability(arrivals: &Arrivals, costs: &Costs, eta: f64, alpha: f64) -> f64 {
    let mtbf = costs.mtbf.as_secs_f64();
    let election = costs.election.as_secs_f64();
    let detected = arrivals.mean_delay.as_secs_f64() + alpha + eta + election;
    detected / mtbf + election * arrival_beta(arrivals, alpha) / eta
}

/// The margin at interval `eta` that minimizes `U` on `arrivals`, and that `U`, the margin to
/// within `resolution` past its exact minimum.
///
/// With `y = α − μ`, `U = const + y/MTBF + c·V/(V + y²)` past the mean, `c = T_E(1 − unseen)/η`,
/// and at or below it `U` only grows with `α`, so `α = 0` is the best there. Past it the slope
/// `1/MTBF − c·h(y)`, `h(y) = 2Vy/(V + y²)²`, is positive until `h` (which rises to its peak at
/// `y = √(V/3)` and falls after) passes `1/(c·MTBF)`, and positive again from where it falls back:
/// `U` has one valley, whose floor is the root of `c·h(y) = 1/MTBF` on `h`'s falling side,
/// bracketed by the peak and `(2cV·MTBF)^{1/3}`, where `c·h(y) < 2cV/y³` meets it, and found by
/// bisection, `h` being monotone there. The better of `α = 0` and that floor is the minimum.
fn arrival_margin(arrivals: &Arrivals, costs: &Costs, eta: f64, resolution: f64) -> (f64, f64) {
    let at = |alpha: f64| (alpha, arrival_unavailability(arrivals, costs, eta, alpha));
    let none = at(0.0);
    let better = |candidate: (f64, f64)| {
        if candidate.1 < none.1 {
            candidate
        } else {
            none
        }
    };
    let mu = arrivals.lateness.as_secs_f64();
    let variance = arrivals.deviation.as_secs_f64().powi(2);
    let mtbf = costs.mtbf.as_secs_f64();
    let cost = costs.election.as_secs_f64() * (1.0 - arrivals.unseen.clamp(0.0, 1.0)) / eta;
    if cost <= 0.0 || !cost.is_finite() {
        return none;
    }
    if variance <= 0.0 {
        // Every lateness is the mean: a margin past it is passed only by the unseen.
        return better(at(mu + NANOSECOND));
    }
    let h = |y: f64| 2.0 * variance * y / (variance + y * y).powi(2);
    let target = 1.0 / mtbf;
    let peak = (variance / 3.0).sqrt();
    if cost * h(peak) <= target {
        return none;
    }
    let (mut low, mut high) = (peak, (2.0 * cost * variance * mtbf).cbrt().max(peak));
    for _ in 0..halvings() {
        let middle = 0.5 * (low + high);
        if high - low <= resolution || middle <= low || middle >= high {
            break;
        }
        if cost * h(middle) > target {
            low = middle;
        } else {
            high = middle;
        }
    }
    better(at(mu + high))
}

/// The detector `(eta, alpha)` on `arrivals`, seconds, with its unavailability `value`.
fn arrival_detector(arrivals: &Arrivals, eta: f64, alpha: f64, value: f64) -> Option<Detector> {
    let beta = arrival_beta(arrivals, alpha);
    Some(Detector {
        interval: Duration::try_from_secs_f64(eta).ok()?,
        margin: Duration::try_from_secs_f64(alpha).ok()?,
        detection: Duration::try_from_secs_f64(arrivals.mean_delay.as_secs_f64() + alpha + eta)
            .ok()?,
        mistake_recurrence: if beta > 0.0 {
            Duration::try_from_secs_f64(eta / beta).unwrap_or(Duration::MAX)
        } else {
            Duration::MAX
        },
        unavailability: value,
    })
}

/// The search's resolution for `arrivals`, the granularity in seconds, or `None` when nothing can
/// be configured: an unseen share of one, or a granularity, MTBF or election time that is not a
/// positive finite time.
fn arrival_resolution(arrivals: &Arrivals, costs: &Costs, granularity: Duration) -> Option<f64> {
    let resolution = granularity.as_secs_f64();
    let mtbf = costs.mtbf.as_secs_f64();
    let election = costs.election.as_secs_f64();
    if !(0.0..1.0).contains(&arrivals.unseen) || resolution <= 0.0 || mtbf <= 0.0 || election <= 0.0
    {
        return None;
    }
    Some(resolution)
}

/// The detector that minimizes a group's expected unavailability on `arrivals`, its interval at
/// or above the sender's `floor` and the timer's `granularity` (the search's resolution too).
/// Any margin is admitted: the bound is one Cantelli factor on the arrival's lateness, whatever
/// the heartbeats the margin holds, so no correlation time enters. `None` when nothing can be
/// configured ([`arrival_resolution`]).
pub fn configure_arrivals(
    arrivals: &Arrivals,
    costs: &Costs,
    granularity: Duration,
    floor: Duration,
) -> Option<Detector> {
    let resolution = arrival_resolution(arrivals, costs, granularity)?;
    let base = resolution.max(floor.as_secs_f64());
    let mtbf = costs.mtbf.as_secs_f64();
    // `U ≥ η / MTBF`, so an interval past `MTBF · U` at the floor costs more than any it could
    // save.
    let (_, at_floor) = arrival_margin(arrivals, costs, base, resolution);
    let high = (mtbf * at_floor).max(base);
    let (eta, _) = minimize(base, high, resolution, |eta| {
        arrival_margin(arrivals, costs, eta, resolution).1
    });
    let eta = eta.max(base);
    let (alpha, value) = arrival_margin(arrivals, costs, eta, resolution);
    arrival_detector(arrivals, eta, alpha, value)
}

/// The detector that minimizes a group's expected unavailability on `arrivals` at a given
/// `interval`: the margin alone is searched. `None` where [`configure_arrivals`] would give none,
/// or for a zero interval.
pub fn arrival_detector_at(
    arrivals: &Arrivals,
    costs: &Costs,
    granularity: Duration,
    interval: Duration,
) -> Option<Detector> {
    let resolution = arrival_resolution(arrivals, costs, granularity)?;
    let eta = interval.as_secs_f64();
    if eta <= 0.0 {
        return None;
    }
    let (alpha, value) = arrival_margin(arrivals, costs, eta, resolution);
    arrival_detector(arrivals, eta, alpha, value)
}

/// `Pr(Binomial(trials, p) ≥ at_least)`.
fn binomial_tail(trials: u32, p: f64, at_least: u32) -> f64 {
    let mut tail = 0.0;
    let mut choose = 1.0;
    for k in 0..=trials {
        if k >= at_least {
            tail += choose
                * p.powi(i32::try_from(k).unwrap_or(i32::MAX))
                * (1.0 - p).powi(i32::try_from(trials.saturating_sub(k)).unwrap_or(i32::MAX));
        }
        // C(trials, k + 1) = C(trials, k) · (trials − k) / (k + 1).
        choose *= f64::from(trials.saturating_sub(k)) / f64::from(k.saturating_add(1));
    }
    tail.min(1.0)
}

/// The probability an attempt splits the vote: `available` voters of `voters` campaign within
/// `span` and the first reaches the others in `latency` (seconds).
fn split(voters: u32, available: u32, latency: f64, span: f64) -> f64 {
    // The vote splits when `c = s − ⌊n/2⌋ + 1` or more start within `l` of the first.
    let crowd = available.saturating_sub(voters / 2).saturating_add(1);
    if crowd <= 1 || span <= 0.0 {
        return 1.0;
    }
    binomial_tail(
        available,
        (latency / span).min(1.0),
        crowd.saturating_sub(1),
    )
}

/// The expected time from a suspicion to a leader with span `span`: the first campaign, expected
/// `W / (s + 1)` after the suspicion, and its vote `round`; each split, with probability `p`, costs
/// a further span and round, so `(p / (1 − p)) · (W + round)` more in expectation.
fn election_time(voters: u32, available: u32, latency: f64, round: f64, span: f64) -> f64 {
    let p = split(voters, available, latency, span);
    if p >= 1.0 {
        return f64::INFINITY;
    }
    span / f64::from(available.saturating_add(1)) + round + p / (1.0 - p) * (span + round)
}

/// The span that minimizes the expected time to a leader for a group of `voters` with
/// `available` of them up, one-way latency `latency` and vote round `round`, searched to within
/// `floor`. `None` when no election can succeed: fewer than a majority available.
pub fn election_span(
    voters: u32,
    available: u32,
    latency: Duration,
    round: Duration,
    floor: Duration,
) -> Option<Span> {
    let (l, b, resolution) = (
        latency.as_secs_f64(),
        round.as_secs_f64(),
        floor.as_secs_f64(),
    );
    if available <= voters / 2 || resolution <= 0.0 {
        return None;
    }
    let low = l.max(resolution);
    // `T_E(W) ≥ W / (s + 1)`, so a span past `(s + 1) · T_E(W₀)` costs more than `W₀` for any
    // `W₀` with a finite time: the search's upper end. The narrowest span is one, unless every
    // attempt at it splits (`W = l`: every start falls within the latency of the first); then
    // `(s + 1) · low` is, a span past the latency, at which an attempt splits with probability
    // below one.
    let first = f64::from(available.saturating_add(1));
    let at_low = election_time(voters, available, l, b, low);
    let (bounding, at) = if at_low.is_finite() {
        (low, at_low)
    } else {
        let wider = first * low;
        (wider, election_time(voters, available, l, b, wider))
    };
    if !at.is_finite() {
        return None;
    }
    let high = (first * at).max(bounding);
    let (w, time) = minimize(low, high, resolution, |w| {
        election_time(voters, available, l, b, w)
    });
    if !time.is_finite() {
        return None;
    }
    Some(Span {
        span: Duration::try_from_secs_f64(w).ok()?,
        election: Duration::try_from_secs_f64(time).ok()?,
        split: split(voters, available, l, w),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(value: f64) -> Duration {
        Duration::from_secs_f64(value / 1e3)
    }

    /// Where every attempt at the narrowest span splits, the search's upper end is bounded by a
    /// span with a finite time. A vote round long against the latency, as one with a flush in it
    /// is, puts the best span past `(s + 1)² · l`, where a fixed widening stopped: three voters,
    /// two up, a round a hundred times the latency, best near 26 latencies, cut at 9.
    #[test]
    fn a_long_vote_round_is_searched_past_a_fixed_widening() {
        let found = election_span(3, 2, ms(1.0), ms(100.0), Duration::from_micros(1)).unwrap();
        assert!(found.span > ms(25.0), "{found:?}");
        let at_fixed_edge = election_time(3, 2, 1e-3, 0.1, 9e-3);
        assert!(found.election.as_secs_f64() < at_fixed_edge, "{found:?}");
    }

    /// A rational `numerator / denominator`, exact in `i128` for the small groups checked here.
    #[derive(Clone, Copy, Debug)]
    struct Ratio(i128, i128);

    impl Ratio {
        fn add(self, other: Self) -> Self {
            Self(self.0 * other.1 + other.0 * self.1, self.1 * other.1).reduced()
        }
        fn times(self, other: Self) -> Self {
            Self(self.0 * other.0, self.1 * other.1).reduced()
        }
        fn reduced(self) -> Self {
            let (mut a, mut b) = (self.0.abs(), self.1.abs());
            while b != 0 {
                (a, b) = (b, a % b);
            }
            let g = a.max(1) * self.1.signum();
            Self(self.0 / g, self.1 / g)
        }
        fn equals(self, other: Self) -> bool {
            self.0 * other.1 == other.0 * self.1
        }
        fn value(self) -> f64 {
            self.0 as f64 / self.1 as f64
        }
    }

    fn choose(n: i128, k: i128) -> i128 {
        (0..k).fold(1, |acc, i| acc * (n - i) / (i + 1))
    }

    fn power(x: Ratio, n: i128) -> Ratio {
        (0..n).fold(Ratio(1, 1), |acc, _| acc.times(x))
    }

    /// `Pr(T_(c) − T_(1) < x)` for `s` uniform starts, from the order statistics' joint density
    /// (David and Nagaraja, *Order Statistics*, 2003, §2.2): the spacing has density
    /// `s!/((c−2)!(s−c+1)!) · w^{c−2}(1 − w)^{s−c+1}` (the density of `(T_(1), T_(c))` with the
    /// first integrated out), integrated term by term from `0` to `x`, exactly.
    fn spacing_below(s: i128, c: i128, x: Ratio) -> Ratio {
        let factorial = |n: i128| (1..=n).product::<i128>();
        let scale = factorial(s) / (factorial(c - 2) * factorial(s - c + 1));
        (0..=s - c + 1).fold(Ratio(0, 1), |sum, k| {
            let sign = if k % 2 == 0 { 1 } else { -1 };
            let term =
                Ratio(sign * scale * choose(s - c + 1, k), c - 1 + k).times(power(x, c - 1 + k));
            sum.add(term)
        })
    }

    /// `Pr(Binomial(s, x) ≥ at_least)`, exactly.
    fn binomial_at_least(s: i128, x: Ratio, at_least: i128) -> Ratio {
        let rest = Ratio(x.1 - x.0, x.1);
        (at_least..=s).fold(Ratio(0, 1), |sum, j| {
            sum.add(
                Ratio(choose(s, j), 1)
                    .times(power(x, j))
                    .times(power(rest, s - j)),
            )
        })
    }

    /// The split probability is Ongaro's order statistic in closed form: the spacing's law,
    /// integrated exactly from the order statistics' density, equals the binomial tail the code
    /// computes, as rationals, for every group of two to seven voters with every count up and
    /// latencies on a grid of the span; and the code's floating value is that rational to within
    /// its rounding. The two smallest have textbook forms: two starts within `x` of each other,
    /// `1 − (1 − x)²`; the second of three within `x` of the first, `1 − (1 − x)³`.
    #[test]
    fn the_split_probability_is_ongaros_order_statistic() {
        assert_eq!(
            split(5, 4, 1.0, 1.0),
            1.0,
            "at l = W every start is within l"
        );
        for voters in 2..=7u32 {
            for available in (voters / 2 + 1)..=voters {
                let crowd = i128::from(available - voters / 2 + 1);
                for x in (1..10).map(|p| Ratio(p, 10)) {
                    let s = i128::from(available);
                    let tail = binomial_at_least(s, x, crowd - 1);
                    if crowd >= 2 {
                        assert!(
                            spacing_below(s, crowd, x).equals(tail),
                            "{voters} voters, {available} up, x {x:?}"
                        );
                    }
                    let closed = split(voters, available, x.value(), 1.0);
                    let exact = if crowd <= 1 { 1.0 } else { tail.value() };
                    assert!(
                        (closed - exact).abs() <= 8.0 * f64::EPSILON,
                        "{voters} voters, {available} up, x {x:?}: {closed} against {exact}"
                    );
                }
            }
        }
        let x = Ratio(3, 10);
        let one = Ratio(1, 1);
        let rest = Ratio(7, 10);
        assert!(spacing_below(2, 2, x).equals(one.add(power(rest, 2).times(Ratio(-1, 1)))));
        assert!(spacing_below(3, 2, x).equals(one.add(power(rest, 3).times(Ratio(-1, 1)))));
    }

    #[test]
    fn no_election_without_a_majority() {
        assert_eq!(election_span(5, 2, ms(1.0), ms(2.0), ms(0.05)), None);
        assert_eq!(election_span(3, 1, ms(1.0), ms(2.0), ms(0.05)), None);
    }

    #[test]
    fn the_span_is_the_minimum_of_the_expected_election() {
        for (voters, available) in [(3u32, 2u32), (3, 3), (5, 4), (5, 5), (7, 6)] {
            let (l, b, g) = (ms(0.5), ms(2.0), ms(0.01));
            let found = election_span(voters, available, l, b, g).unwrap();
            // No span on a fine grid does better than the search, to within its resolution.
            let best = (1..20_000)
                .map(|i| f64::from(i) * 1e-5)
                .filter(|w| *w >= l.as_secs_f64())
                .map(|w| election_time(voters, available, 5e-4, 2e-3, w))
                .fold(f64::INFINITY, f64::min);
            let time = found.election.as_secs_f64();
            assert!(
                time <= best * 1.001,
                "{voters}/{available}: {time} against {best}"
            );
            // The span is wider than the latency, as a split would otherwise be certain.
            assert!(found.span > l);
        }
    }

    #[test]
    fn ongaros_rule_of_thumb_is_near_the_optimum_on_his_assumptions() {
        // Ongaro §9.2–9.3: a span 10–20 times the one-way latency keeps splits under 40 % and
        // elects within 20 latencies on average. On a five-server cluster with all up, the
        // optimum here, with a vote round of two latencies, should land in that band.
        let l = ms(1.0);
        let found = election_span(5, 5, l, ms(2.0), ms(0.001)).unwrap();
        let ratio = found.span.as_secs_f64() / l.as_secs_f64();
        assert!(found.split < 0.4, "split {}", found.split);
        assert!(found.election < l * 20, "election {:?}", found.election);
        assert!(ratio > 2.0 && ratio < 40.0, "span {ratio} latencies");
    }

    #[test]
    fn beta_is_a_product_of_cantelli_bounds() {
        // One heartbeat in the margin: β is Cantelli's bound with loss, (V + p x²) / (V + x²).
        let (p, v, x) = (0.01, 4e-6, 3e-3);
        let one = beta(p, v, 1.0, x);
        assert!((one - (v + p * x * x) / (v + x * x)).abs() < 1e-15);
        // More heartbeats inside the margin only lower it.
        assert!(beta(p, v, x / 4.0, x) < one);
        // No margin: nothing is fresh past its expected arrival, every check may be a mistake.
        assert_eq!(beta(p, v, 1e-3, 0.0), 1.0);
        // No variance and no loss: a heartbeat is never late.
        assert_eq!(beta(0.0, 0.0, 1e-3, 1e-3), 0.0);
    }

    /// The configured detector does at least as well as its neighbours, and a node that fails
    /// more often is worth detecting sooner: on a LAN-like link (0.2 ms delay, 0.1 ms deviation, one
    /// arrival in a hundred past every one seen, 10 ms elections), a month's MTBF against a day's.
    #[test]
    fn the_detector_trades_detection_against_mistakes() {
        let link = Arrivals {
            unseen: 0.01,
            lateness: Duration::ZERO,
            deviation: ms(0.1),
            mean_delay: ms(0.2),
        };
        let floor = ms(0.05);
        let monthly = Costs {
            election: ms(10.0),
            mtbf: Duration::from_secs(30 * 24 * 3600),
        };
        let found = configure_arrivals(&link, &monthly, floor, floor).unwrap();
        let u = |eta: f64, alpha: f64| arrival_unavailability(&link, &monthly, eta, alpha);
        let (eta, alpha) = (found.interval.as_secs_f64(), found.margin.as_secs_f64());
        for (de, da) in [(1.1, 1.0), (0.9, 1.0), (1.0, 1.1), (1.0, 0.9)] {
            let other = (eta * de).max(floor.as_secs_f64());
            assert!(found.unavailability <= u(other, alpha * da) * 1.0001);
        }
        let daily = Costs {
            mtbf: Duration::from_secs(24 * 3600),
            ..monthly
        };
        let sooner = configure_arrivals(&link, &daily, floor, floor).unwrap();
        assert!(sooner.detection <= found.detection);
    }

    /// A xorshift stream (Marsaglia 2003) of `[0, 1)` draws: deterministic test noise.
    fn draws(mut state: u64) -> impl FnMut() -> f64 {
        move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    /// The margin found is the minimum of `U` over every margin, to within the resolution: against
    /// a grid finer than the resolution from zero to far past the deviation, on arrivals from tight
    /// to stalled, with means from none to past the deviation.
    #[test]
    fn the_arrival_margin_is_the_minimum_of_its_unavailability() {
        let mut draw = draws(0x9E37_79B9_7F4A_7C15);
        for _ in 0..200 {
            let arrivals = Arrivals {
                unseen: draw() * 0.2,
                lateness: Duration::from_secs_f64(draw() * draw() * 0.05),
                deviation: Duration::from_secs_f64(1e-5 + draw() * draw() * 0.2),
                mean_delay: Duration::ZERO,
            };
            let costs = Costs {
                election: Duration::from_secs_f64(1e-4 + draw() * 0.1),
                mtbf: Duration::from_secs_f64(1.0 + draw() * 3e5),
            };
            let resolution = 1e-5 + draw() * 2e-3;
            let eta = resolution * (1.0 + draw() * 400.0);
            let (alpha, value) = arrival_margin(&arrivals, &costs, eta, resolution);
            assert!((value - arrival_unavailability(&arrivals, &costs, eta, alpha)).abs() <= 1e-12);
            let reach = 20.0 * arrivals.deviation.as_secs_f64() + arrivals.lateness.as_secs_f64();
            let steps = 20_000;
            let least = (0..=steps)
                .map(|i| reach * f64::from(i) / f64::from(steps))
                .map(|a| arrival_unavailability(&arrivals, &costs, eta, a))
                .fold(f64::INFINITY, f64::min);
            // Within one resolution past the minimum: U's slope there is at most 1/MTBF plus the
            // mistake term's, whose magnitude the step bounds.
            let slack = resolution / costs.mtbf.as_secs_f64()
                + arrival_unavailability(&arrivals, &costs, eta, alpha + resolution)
                - arrival_unavailability(&arrivals, &costs, eta, alpha);
            assert!(
                value <= least + slack.abs() + 1e-12,
                "{arrivals:?} {costs:?} η {eta}: {value} at {alpha} against {least}"
            );
        }
    }

    /// The bound per arrival: one at or below the mean, falling with the margin to the unseen
    /// share, never below it.
    #[test]
    fn the_lateness_bound_is_cantellis_with_the_unseen_share() {
        let arrivals = Arrivals {
            unseen: 0.01,
            lateness: ms(2.0),
            deviation: ms(3.0),
            mean_delay: Duration::ZERO,
        };
        assert_eq!(lateness_bound(&arrivals, ms(1.0)), 1.0);
        assert_eq!(lateness_bound(&arrivals, ms(2.0)), 1.0);
        let at = |margin: f64| lateness_bound(&arrivals, ms(margin));
        let expected = 0.01 + 0.99 * 9.0 / (9.0 + 16.0);
        assert!((at(6.0) - expected).abs() < 1e-12);
        assert!(at(6.0) < at(4.0) && at(60.0) < at(6.0) && at(6_000.0) >= 0.01);
    }

    /// A margin is not capped by the interval: on a link whose deviation is several intervals and
    /// whose receiver's granularity is past the interval (an E2E pair of `docs/timing.md` §2.9 that
    /// the single-heartbeat cap configured with `α = 0` and an unavailability past one: `η` 1.64 ms,
    /// `G` 5.2 ms, deviation 8 ms past the mean), the margin covers the deviation and the
    /// unavailability is far below one.
    #[test]
    fn a_margin_covers_a_deviation_of_several_intervals() {
        let costs = Costs {
            election: ms(22.6),
            mtbf: Duration::from_secs(60),
        };
        let granularity = ms(5.195);
        let interval = ms(1.640);
        let arrivals = Arrivals {
            unseen: 0.01,
            lateness: ms(1.6),
            deviation: ms(8.0),
            mean_delay: Duration::ZERO,
        };
        let found = arrival_detector_at(&arrivals, &costs, granularity, interval).unwrap();
        assert!(found.margin > interval * 4, "{found:?}");
        assert!(found.unavailability < 1.0, "{found:?}");
    }

    /// The best interval does at least as well as its neighbours and as the interval in force.
    #[test]
    fn the_best_arrival_detector_is_no_worse_than_its_neighbours() {
        let arrivals = Arrivals {
            unseen: 0.004,
            lateness: ms(0.4),
            deviation: ms(2.5),
            mean_delay: Duration::ZERO,
        };
        let costs = Costs {
            election: ms(40.0),
            mtbf: Duration::from_secs(3_600),
        };
        let (granularity, floor) = (ms(1.0), ms(4.0));
        let best = configure_arrivals(&arrivals, &costs, granularity, floor).unwrap();
        assert!(best.interval >= floor);
        for scale in [0.8, 0.95, 1.05, 1.25, 2.0] {
            let other = Duration::from_secs_f64(best.interval.as_secs_f64() * scale).max(floor);
            let at = arrival_detector_at(&arrivals, &costs, granularity, other).unwrap();
            assert!(
                best.unavailability <= at.unavailability * 1.0001,
                "{at:?} {best:?}"
            );
        }
        assert!(best.mistake_recurrence > costs.election);
        let none = Arrivals {
            unseen: 1.0,
            ..arrivals
        };
        assert_eq!(configure_arrivals(&none, &costs, granularity, floor), None);
        assert_eq!(
            arrival_detector_at(&arrivals, &costs, Duration::ZERO, floor),
            None
        );
    }

    #[test]
    fn nothing_is_configured_on_a_dead_link_or_a_zero_floor() {
        let costs = Costs {
            election: ms(10.0),
            mtbf: Duration::from_secs(3600),
        };
        let dead = Arrivals {
            unseen: 1.0,
            lateness: Duration::ZERO,
            deviation: ms(1.0),
            mean_delay: ms(1.0),
        };
        assert_eq!(arrival_detector_at(&dead, &costs, ms(1.0), ms(1.0)), None);
        let link = Arrivals {
            unseen: 0.0,
            ..dead
        };
        assert_eq!(
            arrival_detector_at(&link, &costs, Duration::ZERO, ms(1.0)),
            None
        );
        assert_eq!(
            arrival_detector_at(&link, &costs, ms(1.0), Duration::ZERO),
            None
        );
    }
}
