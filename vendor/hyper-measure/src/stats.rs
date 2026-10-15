//! What repeated runs say, and how far apart two sets of runs of the same
//! thing fall: the noise band a difference must clear before it is one.
//!
//! The band is measured, never assumed. For `n` runs of one implementation
//! on one workload, every way of dividing them into two disjoint halves is
//! taken, and the ratio of the halves' medians is computed; the band is the
//! least and the most of those ratios. Two implementations differ on a
//! workload only when the ratio of their medians falls outside both of their
//! bands. slates measures its band the same way across disjoint seed sets
//! (0.79–1.22 between seed sets in its BENCHMARKS); this repository measures
//! its own (`docs/benchmarks.md`).

/// The median, least and most of a set of samples.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Summary {
    /// How many samples.
    pub count: usize,
    /// The middle sample, or the mean of the two middle ones.
    pub median: f64,
    /// The least sample.
    pub min: f64,
    /// The most.
    pub max: f64,
}

/// The median of `samples`, which it sorts; `None` for no sample.
pub fn median(samples: &mut [f64]) -> Option<f64> {
    samples.sort_by(f64::total_cmp);
    let middle = samples.len() / 2;
    let upper = *samples.get(middle)?;
    if samples.len() % 2 == 1 {
        return Some(upper);
    }
    let lower = *samples.get(middle.checked_sub(1)?)?;
    Some(f64::midpoint(lower, upper))
}

impl Summary {
    /// What `samples` say; `None` for no sample.
    pub fn of(samples: &[f64]) -> Option<Self> {
        let mut sorted = samples.to_vec();
        let median = median(&mut sorted)?;
        Some(Self {
            count: sorted.len(),
            median,
            min: *sorted.first()?,
            max: *sorted.last()?,
        })
    }
}

/// The most runs [`band`] divides. Every division of `n` runs into halves is
/// taken both ways round, `C(n, n/2)` ratios: 184,756 at twenty, which a
/// bench computes in well under a second. The mask that names a half is a
/// `u32`, and the work doubles with each run past this.
pub const BAND_RUNS: usize = 20;

/// The least and the most ratio of medians between two disjoint halves of
/// `samples`, over every way of halving them (an odd sample out is left out
/// of both halves in turn). `None` for fewer than four samples or more than
/// [`BAND_RUNS`].
pub fn band(samples: &[f64]) -> Option<(f64, f64)> {
    let count = samples.len();
    if !(4..=BAND_RUNS).contains(&count) {
        return None;
    }
    let half = count / 2;
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    let mut first = Vec::with_capacity(half);
    let mut second = Vec::with_capacity(count);
    // Every subset of `half` members, as a bit mask over the samples.
    let all: u32 = (1u32 << count).wrapping_sub(1);
    for mask in 0..=all {
        if usize::try_from(mask.count_ones()).ok() != Some(half) {
            continue;
        }
        first.clear();
        second.clear();
        for (at, sample) in samples.iter().enumerate() {
            if mask & (1u32 << at) != 0 {
                first.push(*sample);
            } else {
                second.push(*sample);
            }
        }
        // An odd count leaves one more in the second half: each of its
        // members is the one left out in turn, so both halves hold `half`.
        let rest = second.len();
        for out in 0..rest {
            let mut other: Vec<f64> = second
                .iter()
                .enumerate()
                .filter(|(at, _)| rest == half || *at != out)
                .map(|(_, sample)| *sample)
                .collect();
            let (Some(a), Some(b)) = (median(&mut first), median(&mut other)) else {
                continue;
            };
            if b > 0.0 {
                low = low.min(a / b);
                high = high.max(a / b);
            }
            if rest == half {
                break;
            }
        }
    }
    low.is_finite().then_some((low, high))
}
