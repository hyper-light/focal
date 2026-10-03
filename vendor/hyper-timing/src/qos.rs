//! A link's failure detector and a group's election span, chosen from measurement to minimize the
//! time a group cannot commit (`docs/timing.md` §2.2–§2.3).
//!
//! The detector is Chen, Toueg and Aguilera's NFD-E (DSN 2000; IEEE Transactions on Computers
//! 51(5), 2002): heartbeats every `η`, trusted while one is fresh, freshness at the expected arrival
//! plus a margin `α`. Their Theorems 7–8 bound its quality from the loss probability and the
//! delay's mean and variance alone, through the one-sided (Cantelli) inequality
//! `Pr(D > t) ≤ V / (V + (t − E)²)` for `t > E`:
//! - a crash is detected within `E(D) + α + η`;
//! - mistakes recur no more often than every `η / β`, with
//!   `β = Π_{j=0}^{k₀} (V + p_L·x_j²) / (V + x_j²)`, `x_j = α − jη`, over the heartbeats still
//!   fresh, `k₀ = ⌈α/η⌉ − 1`.
//!
//! Chen et al. configure `η` and `α` from requirements an application states. Here they minimize
//! what those requirements stand for, a group's expected unavailability
//! `U = (E(D) + α + η + T_E) / MTBF + T_E · β / η`: an election `T_E` after each detected crash of
//! the leader's node, and one after each false suspicion.
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
/// configurator and the trace analyser (`crates/hyper-timing-trace`) both use this one, so the bound
/// they report and the one the configurator minimizes cannot differ.
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

/// The normal distribution's two-sided 95 % point, `Φ⁻¹(0.975)`.
pub const Z95: f64 = 1.959_963_984_540_054;

/// The 95 % score interval of a Poisson count `k`, `k + z²/2 ∓ z√(k + z²/4)` (Brown, Cai and
/// DasGupta, "Interval estimation in exponential families", Statistica Sinica 13, 2003), whose
/// coverage is close to nominal down to small counts. A run refutes Theorem 7's allowance for its
/// mistakes only when the interval's lower end passes it: the rule the replay, the trace analyser
/// and the cluster tests of hyper-swim and hyper-liveness all apply (`docs/timing.md` §2.6).
pub fn poisson95(k: u64) -> (f64, f64) {
    // u64 → f64 rounds only past 2⁵³ mistakes.
    let k = k as f64;
    let centre = k + Z95 * Z95 / 2.0;
    let half = Z95 * (k + Z95 * Z95 / 4.0).sqrt();
    ((centre - half).max(0.0), centre + half)
}

/// The expected share of time a group cannot commit with detector `(eta, alpha)`, seconds.
fn unavailability(link: &LinkBehaviour, costs: &Costs, eta: f64, alpha: f64) -> f64 {
    let variance = link.delay_deviation.as_secs_f64().powi(2);
    let mtbf = costs.mtbf.as_secs_f64();
    let election = costs.election.as_secs_f64();
    let detected = link.mean_delay.as_secs_f64() + alpha + eta + election;
    detected / mtbf + election * beta(link.loss, variance, eta, alpha) / eta
}

/// The measured floors under a detector's interval (`docs/timing.md` §2.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Floors {
    /// The timer's granularity `G`: how late a timed wait ends. The search's resolution too, as
    /// nothing finer can be kept.
    pub granularity: Duration,
    /// The sender's stability floor, `E[flush] + G`: a sender that flushes before each heartbeat
    /// cannot keep a shorter interval (Lindley's condition).
    pub sender: Duration,
    /// The link's correlation time `T_c`: heartbeats closer than this are late together, so a margin
    /// holding two or more of them may count on Theorem 7's product only at `η ≥ T_c`.
    pub correlation: Duration,
}

/// The detector that minimizes a group's expected unavailability on `link`, within `floors`.
///
/// Two regimes, the better kept. With one heartbeat in the margin (`α < η`) Theorem 7's product has
/// one factor and assumes no independence, so `η` need only clear `G` and the sender's floor. With
/// more, the heartbeats it multiplies must be independent, so `η ≥ T_c` as well. `None` when nothing
/// can be configured: a link that loses every heartbeat, or a granularity, MTBF or election time that
/// is not a positive finite time.
pub fn configure(link: &LinkBehaviour, costs: &Costs, floors: &Floors) -> Option<Detector> {
    let resolution = resolution(link, costs, floors)?;
    let base = resolution.max(floors.sender.as_secs_f64());
    let independent = base.max(floors.correlation.as_secs_f64());
    // One heartbeat in the margin: `α` below `η`.
    let single = search(link, costs, resolution, base, true);
    // Any margin, its heartbeats independent.
    let any = search(link, costs, resolution, independent, false);
    let (eta, alpha, value) = if single.2 <= any.2 { single } else { any };
    detector(link, eta, alpha, value)
}

/// The detector that minimizes a group's expected unavailability on `link` at a given `interval`:
/// the margin alone is searched. It is the detector for the heartbeats a link is sending now, while
/// [`configure`]'s interval, where it differs, is the one to move the link to. Below the correlation
/// time the margin holds one heartbeat (`α < η`), as in [`configure`]. `None` where [`configure`]
/// would give none, or for a zero interval.
pub fn detector_at(
    link: &LinkBehaviour,
    costs: &Costs,
    floors: &Floors,
    interval: Duration,
) -> Option<Detector> {
    let resolution = resolution(link, costs, floors)?;
    let eta = interval.as_secs_f64();
    if eta <= 0.0 {
        return None;
    }
    let single = eta < floors.correlation.as_secs_f64();
    let (alpha, value) = best_margin(link, costs, resolution, eta, single);
    detector(link, eta, alpha, value)
}

/// The search's resolution, the granularity in seconds, or `None` when nothing can be configured: a
/// link that loses every heartbeat, or a granularity, MTBF or election time that is not a positive
/// finite time.
fn resolution(link: &LinkBehaviour, costs: &Costs, floors: &Floors) -> Option<f64> {
    let resolution = floors.granularity.as_secs_f64();
    let mtbf = costs.mtbf.as_secs_f64();
    let election = costs.election.as_secs_f64();
    if !(0.0..1.0).contains(&link.loss) || resolution <= 0.0 || mtbf <= 0.0 || election <= 0.0 {
        return None;
    }
    Some(resolution)
}

/// The detector `(eta, alpha)` on `link`, seconds, with its unavailability `value`.
fn detector(link: &LinkBehaviour, eta: f64, alpha: f64, value: f64) -> Option<Detector> {
    let variance = link.delay_deviation.as_secs_f64().powi(2);
    let beta = beta(link.loss, variance, eta, alpha);
    Some(Detector {
        interval: Duration::try_from_secs_f64(eta).ok()?,
        margin: Duration::try_from_secs_f64(alpha).ok()?,
        detection: Duration::try_from_secs_f64(link.mean_delay.as_secs_f64() + alpha + eta).ok()?,
        mistake_recurrence: if beta > 0.0 {
            Duration::try_from_secs_f64(eta / beta).unwrap_or(Duration::MAX)
        } else {
            Duration::MAX
        },
        unavailability: value,
    })
}

/// The best margin for interval `eta` and its `U`, to within `resolution`. With `single`, the
/// margin stays below the interval.
///
/// `U ≥ α / MTBF`, so a margin past `MTBF · U*` for any `U*` reached costs more than the margin
/// that reached it. The bracket is therefore closed from above by probing `α = η, 2η, 4η, …` while
/// `α / MTBF` is below the least `U` seen: each probe can only lower that least `U`, so the bracket
/// keeps every margin that could do better. On a lossy link, where `U(η, η)` is large (one
/// heartbeat in the margin bounds a mistake by little more than the loss), it is far tighter than
/// `MTBF · U(η, η)`, whose margins hold thousands of heartbeats for `β`'s product to multiply. The
/// probes end once a doubling would pass `MTBF · U*`, at most the doublings an `f64` holds.
fn best_margin(
    link: &LinkBehaviour,
    costs: &Costs,
    resolution: f64,
    eta: f64,
    single: bool,
) -> (f64, f64) {
    let high = if single {
        (eta - resolution).max(0.0)
    } else {
        let mtbf = costs.mtbf.as_secs_f64();
        let mut least = unavailability(link, costs, eta, eta);
        let mut alpha = eta;
        loop {
            let next = alpha * 2.0;
            if !next.is_finite() || next >= mtbf * least {
                break;
            }
            least = least.min(unavailability(link, costs, eta, next));
            alpha = next;
        }
        (mtbf * least).max(eta)
    };
    minimize(0.0, high, resolution, |alpha| {
        unavailability(link, costs, eta, alpha)
    })
}

/// The interval at or above `floor` and its margin that minimize `U`, to within `resolution`: the
/// interval, the margin and `U`. With `single`, the margin stays below the interval.
fn search(
    link: &LinkBehaviour,
    costs: &Costs,
    resolution: f64,
    floor: f64,
    single: bool,
) -> (f64, f64, f64) {
    let mtbf = costs.mtbf.as_secs_f64();
    // `U ≥ η / MTBF`, so an interval past `MTBF · U` at the floor costs more than any it could save.
    let (_, at_floor) = best_margin(link, costs, resolution, floor, single);
    let high = (mtbf * at_floor).max(floor);
    let (eta, _) = minimize(floor, high, resolution, |eta| {
        best_margin(link, costs, resolution, eta, single).1
    });
    let eta = eta.max(floor);
    let (alpha, value) = best_margin(link, costs, resolution, eta, single);
    (eta, alpha, value)
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

    #[test]
    fn the_split_probability_is_ongaros_order_statistic() {
        // Five voters, four up: the vote splits when three of four start within l of the first
        // (Ongaro §9.2, Figure 9.4). At l = W every start is within l: always split.
        assert_eq!(split(5, 4, 1.0, 1.0), 1.0);
        // A Monte Carlo of the same event agrees with the closed form.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut draw = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        for (voters, available, x) in [(5u32, 4u32, 0.1), (5, 5, 0.2), (3, 2, 0.3), (7, 6, 0.05)] {
            let crowd = available - voters / 2 + 1;
            let trials = 200_000;
            let mut splits = 0;
            for _ in 0..trials {
                let mut times: Vec<f64> = (0..available).map(|_| draw()).collect();
                times.sort_by(f64::total_cmp);
                if times[crowd as usize - 1] - times[0] < x {
                    splits += 1;
                }
            }
            let simulated = f64::from(splits) / f64::from(trials);
            let closed = split(voters, available, x, 1.0);
            assert!(
                (simulated - closed).abs() < 0.005,
                "{voters} voters, {available} up, l/W {x}: simulated {simulated}, closed {closed}"
            );
        }
    }

    /// The margin's bracket closed by probing finds the margin the whole bracket `[0, MTBF·U(η,η)]`
    /// finds, to within the search's resolution, on links from lossless to lossy.
    #[test]
    fn the_probed_bracket_keeps_the_best_margin() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut draw = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        for _ in 0..24 {
            let link = LinkBehaviour {
                loss: draw() * 0.6,
                mean_delay: Duration::ZERO,
                delay_deviation: Duration::from_secs_f64(1e-5 + draw() * 2e-2),
            };
            let costs = Costs {
                election: Duration::from_secs_f64(1e-4 + draw() * 1e-2),
                mtbf: Duration::from_secs_f64(10.0 + draw() * 3e6),
            };
            let resolution = 1e-5 + draw() * 1e-3;
            let eta = resolution * (1.0 + draw() * 500.0);
            let (alpha, value) = best_margin(&link, &costs, resolution, eta, false);
            let whole =
                (costs.mtbf.as_secs_f64() * unavailability(&link, &costs, eta, eta)).max(eta);
            let (_, before) = minimize(0.0, whole, resolution, |a| {
                unavailability(&link, &costs, eta, a)
            });
            // Within the resolution: the better of the two is no more than a step of `G` better.
            let step = unavailability(&link, &costs, eta, alpha + resolution).min(unavailability(
                &link,
                &costs,
                eta,
                (alpha - resolution).max(0.0),
            ));
            assert!(
                value <= before.max(step) * (1.0 + 1e-9),
                "{link:?} {costs:?} η {eta}: {value} against {before}"
            );
        }
    }

    #[test]
    fn the_poisson_interval_is_the_score_interval() {
        // k = 0: the lower end is zero and the upper z², the score interval's own closed form.
        let (low, high) = poisson95(0);
        assert_eq!(low, 0.0);
        assert!((high - Z95 * Z95).abs() < 1e-12);
        // Around a large count it is close to the normal interval k ± z√k.
        let (low, high) = poisson95(10_000);
        assert!((low - (10_000.0 - Z95 * 100.0)).abs() < 2.0);
        assert!((high - (10_000.0 + Z95 * 100.0)).abs() < 2.0);
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

    #[test]
    fn the_detector_trades_detection_against_mistakes() {
        let link = LinkBehaviour {
            loss: 0.01,
            mean_delay: ms(0.2),
            delay_deviation: ms(0.1),
        };
        let floor = ms(0.05);
        let monthly = Costs {
            election: ms(10.0),
            mtbf: Duration::from_secs(30 * 24 * 3600),
        };
        let floors = Floors {
            granularity: floor,
            sender: floor,
            correlation: floor,
        };
        let found = configure(&link, &monthly, &floors).unwrap();
        // It does at least as well as the neighbours of its choice.
        let u = |eta: f64, alpha: f64| unavailability(&link, &monthly, eta, alpha);
        let (eta, alpha) = (found.interval.as_secs_f64(), found.margin.as_secs_f64());
        for (de, da) in [(1.1, 1.0), (0.9, 1.0), (1.0, 1.1), (1.0, 0.9)] {
            let other = (eta * de).max(floor.as_secs_f64());
            assert!(found.unavailability <= u(other, alpha * da) * 1.0001);
        }
        // Mistakes are rarer than failures: a false suspicion must not cost more than a crash.
        assert!(found.mistake_recurrence > monthly.mtbf / 10);
        // A node that fails more often is worth detecting sooner.
        let daily = Costs {
            mtbf: Duration::from_secs(24 * 3600),
            ..monthly
        };
        let sooner = configure(&link, &daily, &floors).unwrap();
        assert!(sooner.detection <= found.detection);
    }

    #[test]
    fn heartbeats_counted_together_are_a_correlation_time_apart() {
        // A link with a heavy tail, as the traces measured on a flushing macOS link (mean 7 ms,
        // deviation 10.8 ms), and correlation times from none to past any useful interval.
        let link = LinkBehaviour {
            loss: 0.0005,
            mean_delay: ms(7.0),
            delay_deviation: ms(10.8),
        };
        let costs = Costs {
            election: ms(5.7),
            mtbf: Duration::from_secs(30 * 24 * 3600),
        };
        for correlation in [0.0, 50.0, 200.0, 2_000.0, 60_000.0] {
            let floors = Floors {
                granularity: ms(1.6),
                sender: ms(7.0),
                correlation: ms(correlation),
            };
            let found = configure(&link, &costs, &floors).unwrap();
            assert!(
                found.margin < found.interval || found.interval >= floors.correlation,
                "T_c {correlation} ms: {found:?}"
            );
            assert!(found.interval >= floors.sender && found.interval >= floors.granularity);
        }
        // With no correlation the floors alone bind, and a correlation time can only cost.
        let free = Floors {
            granularity: ms(1.6),
            sender: ms(7.0),
            correlation: Duration::ZERO,
        };
        let bound = Floors {
            correlation: ms(200.0),
            ..free
        };
        let (a, b) = (
            configure(&link, &costs, &free).unwrap(),
            configure(&link, &costs, &bound).unwrap(),
        );
        assert!(a.unavailability <= b.unavailability);
    }

    #[test]
    fn nothing_is_configured_on_a_dead_link_or_a_zero_floor() {
        let costs = Costs {
            election: ms(10.0),
            mtbf: Duration::from_secs(3600),
        };
        let dead = LinkBehaviour {
            loss: 1.0,
            mean_delay: ms(1.0),
            delay_deviation: ms(1.0),
        };
        let floors = Floors {
            granularity: ms(1.0),
            sender: ms(1.0),
            correlation: ms(1.0),
        };
        assert_eq!(configure(&dead, &costs, &floors), None);
        let link = LinkBehaviour { loss: 0.0, ..dead };
        let zero = Floors {
            granularity: Duration::ZERO,
            ..floors
        };
        assert_eq!(configure(&link, &costs, &zero), None);
    }
}
