//! A quantile of a run's samples with its distribution-free interval (`docs/tails.md` §3.3).
//!
//! Of `n` independent samples of a continuous distribution, the count `B` below its `q`-quantile is
//! Binomial(`n`, `q`) whatever the distribution. The sample of rank `r` (the `r`-th smallest) lies
//! above the quantile exactly when fewer than `r` samples are below it, with probability
//! `P(B ≤ r − 1)`, and the samples of ranks `l < u` bracket it with probability `P(l ≤ B < u)`
//! (David and Nagaraja, *Order Statistics*, 3rd ed., 2003, §7.1). [`ranks`] leaves at most half of
//! what the coverage leaves out to each side: `l` is the highest rank with `P(B ≤ l − 1)` within
//! that half, `u` the lowest with `P(B ≥ u)` within it. A rank that would fall outside the samples is
//! none: the quantile is unresolved on that side, the run too short to state it, and
//! [`samples_for`] is the run length that resolves it above.
//!
//! The binomial's probabilities are summed from its mode outward, each term from the last by the
//! ratio of consecutive terms, and normalised by their sum; a side's walk ends once what it has not
//! summed is below the resolution of the tail it is compared with. No term is computed through a
//! factorial, so no count of samples overflows it.

use std::fmt;

/// A probability strictly between 0 and 1, as a ratio of integers: a quantile's level or an
/// interval's coverage, exact where a rank is computed from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    per: u64,
    of: u64,
}

impl Level {
    /// The median.
    pub const P50: Self = Self { per: 1, of: 2 };
    /// The 99th percentile.
    pub const P99: Self = Self { per: 99, of: 100 };
    /// The 99.9th percentile.
    pub const P999: Self = Self { per: 999, of: 1000 };
    /// The 99.99th percentile.
    pub const P9999: Self = Self {
        per: 9999,
        of: 10_000,
    };

    /// `per / of`; refused unless `0 < per < of`.
    pub const fn new(per: u64, of: u64) -> Result<Self, QuantileError> {
        if per == 0 || per >= of {
            return Err(QuantileError::NotBetweenZeroAndOne { per, of });
        }
        Ok(Self { per, of })
    }

    /// The level as a float.
    #[must_use]
    pub fn value(self) -> f64 {
        self.per as f64 / self.of as f64
    }

    /// One minus the level, as a float, computed from the integers so a level near one keeps its
    /// difference exactly.
    fn complement(self) -> f64 {
        self.of.saturating_sub(self.per) as f64 / self.of as f64
    }
}

/// Why a quantile could not be stated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuantileError {
    /// A level or a coverage of `per / of` outside (0, 1).
    NotBetweenZeroAndOne {
        /// The numerator given.
        per: u64,
        /// The denominator given.
        of: u64,
    },
    /// No samples to state a quantile of.
    NoSamples,
    /// A count past what the arithmetic holds (more samples than `u64` counts).
    Overflow,
}

impl fmt::Display for QuantileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBetweenZeroAndOne { per, of } => {
                write!(f, "{per}/{of} is not strictly between 0 and 1")
            }
            Self::NoSamples => f.write_str("no samples"),
            Self::Overflow => f.write_str("a count past u64"),
        }
    }
}

impl std::error::Error for QuantileError {}

/// The 1-based ranks among `n` sorted samples that a quantile and its interval are read at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ranks {
    /// The estimate: the nearest rank, `⌈n·q⌉`, at least 1.
    pub estimate: usize,
    /// The interval's lower end; none when even the smallest sample lies above the quantile more
    /// often than the coverage allows.
    pub lower: Option<usize>,
    /// The interval's upper end; none when even the largest sample lies below the quantile more
    /// often than the coverage allows: unresolved above.
    pub upper: Option<usize>,
}

/// A quantile's estimate and its interval, read from sorted samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quantile<T> {
    /// The sample at the estimate's rank.
    pub estimate: T,
    /// The sample at the interval's lower rank, if there is one.
    pub lower: Option<T>,
    /// The sample at the interval's upper rank, if there is one.
    pub upper: Option<T>,
}

/// The ranks of the `q`-quantile of `n` samples and of its interval of at least `coverage`.
pub fn ranks(n: usize, q: Level, coverage: Level) -> Result<Ranks, QuantileError> {
    if n == 0 {
        return Err(QuantileError::NoSamples);
    }
    let count = u64::try_from(n).map_err(|_| QuantileError::Overflow)?;
    let estimate = usize::try_from(ceil_ratio(count, q)?.clamp(1, count))
        .map_err(|_| QuantileError::Overflow)?;
    let window = Window::of(count, q, coverage)?;
    Ok(Ranks {
        estimate,
        lower: window
            .lower()
            .map(usize::try_from)
            .transpose()
            .ok()
            .flatten(),
        upper: window
            .upper()
            .map(usize::try_from)
            .transpose()
            .ok()
            .flatten(),
    })
}

/// The `q`-quantile of `sorted` (ascending) and its interval of at least `coverage`.
pub fn quantile<T: Copy>(
    sorted: &[T],
    q: Level,
    coverage: Level,
) -> Result<Quantile<T>, QuantileError> {
    let r = ranks(sorted.len(), q, coverage)?;
    let at = |rank: usize| rank.checked_sub(1).and_then(|i| sorted.get(i)).copied();
    Ok(Quantile {
        estimate: at(r.estimate).ok_or(QuantileError::NoSamples)?,
        lower: r.lower.and_then(at),
        upper: r.upper.and_then(at),
    })
}

/// A quantile and its interval as a table cell, `estimate [lower–upper]`, each sample written by
/// `show` and each end the samples do not reach written `unresolved`; the refusal's words when
/// there is no quantile to state.
pub fn cell<T: Copy>(
    sorted: &[T],
    q: Level,
    coverage: Level,
    show: impl Fn(T) -> String,
) -> String {
    let end = |v: Option<T>| v.map_or_else(|| "unresolved".to_owned(), &show);
    match quantile(sorted, q, coverage) {
        Ok(v) => format!("{} [{}–{}]", show(v.estimate), end(v.lower), end(v.upper)),
        Err(e) => e.to_string(),
    }
}

/// The fewest samples whose `q`-quantile has an upper end at `coverage`: the least `n` with
/// `q^n` within half of what the coverage leaves out, the chance that every sample lies below the
/// quantile (`docs/tails.md` §3.3's run length).
pub fn samples_for(q: Level, coverage: Level) -> Result<u64, QuantileError> {
    let tail = coverage.complement() / 2.0;
    // ln q as ln(1 − (1 − q)), the complement exact from the integers, so a level near one keeps
    // its digits.
    let ln_q = (-q.complement()).ln_1p();
    let resolved = |n: u64| n as f64 * ln_q <= tail.ln();
    let mut high = 1u64;
    while !resolved(high) {
        high = high.checked_mul(2).ok_or(QuantileError::Overflow)?;
    }
    let mut low = high / 2;
    // `resolved(low)` is false (or `low` is 0) and `resolved(high)` true: halve the gap.
    while high.saturating_sub(low) > 1 {
        let mid = low.saturating_add(high.saturating_sub(low) / 2);
        if resolved(mid) {
            high = mid;
        } else {
            low = mid;
        }
    }
    Ok(high)
}

/// `⌈n·q⌉`, exactly.
fn ceil_ratio(n: u64, q: Level) -> Result<u64, QuantileError> {
    let product = u128::from(n)
        .checked_mul(u128::from(q.per))
        .ok_or(QuantileError::Overflow)?;
    u64::try_from(product.div_ceil(u128::from(q.of))).map_err(|_| QuantileError::Overflow)
}

/// The binomial's terms over the counts that carry its mass, normalised by their sum.
struct Window {
    /// The count of the first term.
    first: u64,
    /// The terms, each relative to the mode's, from `first` up.
    terms: Vec<f64>,
    /// Their sum.
    sum: f64,
    /// The samples.
    n: u64,
    /// Half of what the coverage leaves out: the most each side of the interval may miss by.
    tail: f64,
}

impl Window {
    fn of(n: u64, q: Level, coverage: Level) -> Result<Self, QuantileError> {
        // The mode, ⌊(n + 1)·q⌋, at most n.
        let mode = u64::try_from(
            u128::from(n)
                .checked_add(1)
                .and_then(|m| m.checked_mul(u128::from(q.per)))
                .and_then(|m| m.checked_div(u128::from(q.of)))
                .ok_or(QuantileError::Overflow)?,
        )
        .map_err(|_| QuantileError::Overflow)?
        .min(n);
        let tail = coverage.complement() / 2.0;
        // What a side may leave unsummed: below the resolution of the tail it is compared with,
        // relative to the mode's term of one, which the sum is at least.
        let negligible = tail * f64::EPSILON;
        let odds = q.value() / q.complement();
        let mut below = Vec::new();
        let mut term = 1.0f64;
        let mut k = mode;
        // Down from the mode: term(k − 1) = term(k) · k / (n − k + 1) / odds, the ratio falling as
        // k does (the binomial's terms are log-concave), so once it is below one what is left is
        // at most term · r / (1 − r).
        while k > 0 {
            let r = k as f64 / (n.saturating_sub(k).saturating_add(1)) as f64 / odds;
            term *= r;
            k = k.saturating_sub(1);
            below.push(term);
            if r < 1.0 && term * r / (1.0 - r) <= negligible {
                break;
            }
        }
        let first = k;
        below.reverse();
        let mut terms = below;
        terms.push(1.0);
        // Up from the mode: term(k + 1) = term(k) · (n − k) / (k + 1) · odds.
        let mut term = 1.0f64;
        let mut k = mode;
        while k < n {
            let r = n.saturating_sub(k) as f64 / k.saturating_add(1) as f64 * odds;
            term *= r;
            k = k.saturating_add(1);
            terms.push(term);
            if r < 1.0 && term * r / (1.0 - r) <= negligible {
                break;
            }
        }
        let sum = terms.iter().sum();
        Ok(Self {
            first,
            terms,
            sum,
            n,
            tail,
        })
    }

    /// The highest rank `l ≥ 1` with `P(B ≤ l − 1) ≤ tail`.
    fn lower(&self) -> Option<u64> {
        // Counts below the window carry less than the tail's resolution: P(B ≤ first − 1) is
        // within it, so rank `first` qualifies, and each count up adds its term.
        let mut below = 0.0f64;
        let mut rank = self.first;
        for &term in &self.terms {
            below += term / self.sum;
            if below > self.tail {
                break;
            }
            rank = rank.saturating_add(1);
        }
        (rank >= 1).then_some(rank.min(self.n))
    }

    /// The lowest rank `u ≤ n` with `P(B ≥ u) ≤ tail`.
    fn upper(&self) -> Option<u64> {
        // Counts above the window carry less than the tail's resolution: P(B ≥ last + 1) is
        // within it, and each count down adds its term.
        let last = self
            .first
            .saturating_add(u64::try_from(self.terms.len()).ok()?)
            .saturating_sub(1);
        let mut above = 0.0f64;
        let mut rank = last.saturating_add(1);
        for &term in self.terms.iter().rev() {
            above += term / self.sum;
            if above > self.tail {
                break;
            }
            rank = rank.saturating_sub(1);
        }
        (rank >= 1 && rank <= self.n).then_some(rank)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const C95: Level = Level { per: 95, of: 100 };

    fn level(per: u64, of: u64) -> Level {
        Level::new(per, of).unwrap()
    }

    /// Every rank against the exact binomial sums, computed in integers (`P(B ≤ k)` as
    /// `Σ C(n, i)·per^i·(of − per)^(n − i) / of^n`) for the classic cases: the median of ten
    /// samples is bracketed by ranks 2 and 9, of a hundred by 40 and 61 (David and Nagaraja's
    /// worked cases); p99 of a hundred has no upper end, of a thousand 983 and 997, of two thousand
    /// 1,971 and 1,989; p99.9 needs 3,688 samples for an upper end, and has none at 3,687 or at the
    /// two thousand of hyper-raft-compare's multilog table.
    #[test]
    fn ranks_are_the_exact_binomial_ranks() {
        let cases = [
            (10, level(1, 2), 5, Some(2), Some(9)),
            (100, level(1, 2), 50, Some(40), Some(61)),
            (100, Level::P99, 99, Some(97), None),
            (1000, Level::P99, 990, Some(983), Some(997)),
            (20, level(1, 10), 2, None, Some(6)),
            (5, level(1, 2), 3, None, None),
            (6, level(1, 2), 3, Some(1), Some(6)),
            (1, level(1, 2), 1, None, None),
            (3688, Level::P999, 3685, Some(3680), Some(3688)),
            (3687, Level::P999, 3684, Some(3679), None),
            (2000, Level::P99, 1980, Some(1971), Some(1989)),
            (2000, Level::P999, 1998, Some(1995), None),
        ];
        for (n, q, estimate, lower, upper) in cases {
            assert_eq!(
                ranks(n, q, C95).unwrap(),
                Ranks {
                    estimate,
                    lower,
                    upper
                },
                "n {n}, q {q:?}"
            );
        }
    }

    /// The run length for an upper end: the least `n` with `q^n ≤ 0.025` (3,688 for p99.9, as
    /// hyper-multilog-e2e states it; 368 for p99; 6 for the median), and the ranks agree with it at
    /// that length and one less.
    #[test]
    fn samples_for_is_where_the_upper_end_appears() {
        for (q, n) in [(Level::P999, 3688), (Level::P99, 368), (Level::P50, 6)] {
            assert_eq!(samples_for(q, C95).unwrap(), n, "{q:?}");
            let at = usize::try_from(n).unwrap();
            assert!(ranks(at, q, C95).unwrap().upper.is_some());
            assert!(ranks(at - 1, q, C95).unwrap().upper.is_none());
        }
    }

    /// A million samples, where no factorial fits a float: the median's ranks are the exact ones,
    /// 499,020 and 500,981, from the integer sums `Σ C(10⁶, i)` against `2^(10⁶) / 40` (the half
    /// below the middle less the terms from the boundary up, in Python's integers).
    #[test]
    fn a_million_samples_give_the_exact_ranks_of_the_median() {
        assert_eq!(
            ranks(1_000_000, Level::P50, C95).unwrap(),
            Ranks {
                estimate: 500_000,
                lower: Some(499_020),
                upper: Some(500_981)
            }
        );
    }

    #[test]
    fn quantile_reads_the_samples_at_the_ranks() {
        let sorted: Vec<u64> = (1..=100).collect();
        let q = quantile(&sorted, Level::P50, C95).unwrap();
        assert_eq!(
            q,
            Quantile {
                estimate: 50,
                lower: Some(40),
                upper: Some(61)
            }
        );
        assert_eq!(
            quantile::<u64>(&[], Level::P50, C95),
            Err(QuantileError::NoSamples)
        );
    }

    #[test]
    fn a_cell_writes_the_estimate_and_each_end_or_unresolved() {
        let sorted: Vec<u64> = (1..=100).collect();
        let show = |v: u64| v.to_string();
        assert_eq!(cell(&sorted, Level::P50, C95, show), "50 [40–61]");
        assert_eq!(cell(&sorted, Level::P99, C95, show), "99 [97–unresolved]");
        assert_eq!(
            cell(&sorted[..5], Level::P50, C95, show),
            "3 [unresolved–unresolved]"
        );
        assert_eq!(cell::<u64>(&[], Level::P50, C95, show), "no samples");
    }

    #[test]
    fn a_level_outside_zero_and_one_is_refused() {
        assert!(Level::new(0, 2).is_err());
        assert!(Level::new(2, 2).is_err());
        assert!(Level::new(3, 2).is_err());
        assert!(Level::new(1, 2).is_ok());
    }
}
