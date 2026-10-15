//! A histogram of nanoseconds in log-linear buckets, eight a doubling: what an owner's metrics keep
//! of a latency (a log's flushes and group-commit waits, an owner's queue, quorum and apply delays)
//! in fixed room, and merge bucket by bucket across owners (`docs/timing.md` §2.10).
//!
//! A value below eight nanoseconds has a bucket of its own; from eight, each doubling `[2^e,
//! 2^(e+1))` is cut into eight equal buckets, so a bucket is at most an eighth of its least value
//! wide and a value is placed within 12.5% of it. focal's metrics view (F26) set the precision from
//! what the numbers are used for: its measured claims compare runs that differ by 10–30%, and the
//! smallest regression worth paging on is a quarter of p99, which at an eighth always lands a bucket
//! apart, where in `log2` buckets it can sit in one.

/// Buckets a doubling: the precision, an eighth.
const SUB_BUCKETS: u64 = 8;
/// `log2(SUB_BUCKETS)`: the doubling at which the first eight one-nanosecond buckets end.
const SUB_BITS: u32 = 3;

/// Nanoseconds counted in log-linear buckets, with their count and sum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Histogram {
    count: u64,
    sum_ns: u64,
    buckets: [u64; Histogram::BUCKETS],
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    /// Eight buckets of one nanosecond, then the 61 doublings from `2^3` to `2^63`, eight each:
    /// 3,968 bytes of counts.
    pub const BUCKETS: usize = 496;

    /// An empty histogram.
    pub const fn new() -> Self {
        Self {
            count: 0,
            sum_ns: 0,
            buckets: [0; Self::BUCKETS],
        }
    }

    /// The bucket `ns` falls in.
    pub fn bucket(ns: u64) -> usize {
        if ns < SUB_BUCKETS {
            return usize::try_from(ns).unwrap_or(0);
        }
        // The doubling `[2^e, 2^(e+1))` that holds `ns`, `e` at least 3 here.
        let e = 63u32.saturating_sub(ns.leading_zeros());
        let shift = e.saturating_sub(SUB_BITS);
        let sub = ns.checked_shr(shift).unwrap_or(0) & (SUB_BUCKETS - 1);
        let bucket = u64::from(shift)
            .saturating_mul(SUB_BUCKETS)
            .saturating_add(SUB_BUCKETS)
            .saturating_add(sub);
        usize::try_from(bucket).unwrap_or(Self::BUCKETS - 1)
    }

    /// A bucket's least value, and the least value past it (`u64::MAX` for the last, which holds
    /// it).
    pub fn bounds(bucket: usize) -> (u64, u64) {
        let index = u64::try_from(bucket).unwrap_or(u64::MAX);
        if index < SUB_BUCKETS {
            return (index, index.saturating_add(1));
        }
        let past_first = index.saturating_sub(SUB_BUCKETS);
        let shift =
            u32::try_from(past_first.checked_div(SUB_BUCKETS).unwrap_or(0)).unwrap_or(u32::MAX);
        let sub = past_first.checked_rem(SUB_BUCKETS).unwrap_or(0);
        let scale = |step: u64| {
            u128::from(SUB_BUCKETS.saturating_add(step))
                .checked_shl(shift)
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(u64::MAX)
        };
        (scale(sub), scale(sub.saturating_add(1)))
    }

    /// One value counted.
    pub fn record(&mut self, ns: u64) {
        self.count = self.count.saturating_add(1);
        self.sum_ns = self.sum_ns.saturating_add(ns);
        if let Some(slot) = self.buckets.get_mut(Self::bucket(ns)) {
            *slot = slot.saturating_add(1);
        }
    }

    /// Every value `other` counted counted here too, bucket by bucket: what an owner's metrics
    /// gather of several logs or owners.
    pub fn merge(&mut self, other: &Self) {
        self.count = self.count.saturating_add(other.count);
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
        for (slot, theirs) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *slot = slot.saturating_add(*theirs);
        }
    }

    /// The largest value the bucket holding the `q_ppm` quantile holds (by the nearest rank): the
    /// quantile is at most this, and less by an eighth of it at most. `None` when nothing is
    /// counted.
    pub fn quantile(&self, q_ppm: u32) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = u128::from(self.count)
            .saturating_mul(u128::from(q_ppm.min(1_000_000)))
            .div_ceil(1_000_000)
            .clamp(1, u128::from(self.count));
        let mut seen = 0u128;
        for (bucket, held) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(u128::from(*held));
            if seen >= rank {
                let (_, past) = Self::bounds(bucket);
                return Some(if bucket == Self::BUCKETS - 1 {
                    past
                } else {
                    past.saturating_sub(1)
                });
            }
        }
        None
    }

    /// How many values were counted.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Their sum, saturating.
    pub fn sum_ns(&self) -> u64 {
        self.sum_ns
    }

    /// The counts, bucket by bucket ([`Histogram::bounds`] names each).
    pub fn buckets(&self) -> &[u64; Self::BUCKETS] {
        &self.buckets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_below_eight_has_a_bucket_of_its_own() {
        for ns in 0..8u64 {
            assert_eq!(Histogram::bucket(ns), ns as usize);
            assert_eq!(Histogram::bounds(ns as usize), (ns, ns + 1));
        }
        assert_eq!(Histogram::bucket(8), 8);
        assert_eq!(Histogram::bounds(8), (8, 9));
        // From 16 a bucket is two nanoseconds wide.
        assert_eq!(Histogram::bounds(16), (16, 18));
    }

    #[test]
    fn every_bucket_holds_its_bounds_and_is_an_eighth_of_its_least_value_wide_at_most() {
        for bucket in 0..Histogram::BUCKETS {
            let (least, past) = Histogram::bounds(bucket);
            assert_eq!(Histogram::bucket(least), bucket, "{bucket}: {least}");
            if bucket == Histogram::BUCKETS - 1 {
                assert_eq!(past, u64::MAX);
                assert_eq!(Histogram::bucket(u64::MAX), bucket);
                continue;
            }
            assert_eq!(Histogram::bucket(past - 1), bucket, "{bucket}: {past}");
            assert_eq!(Histogram::bucket(past), bucket + 1, "{bucket}: {past}");
            if least >= 8 {
                assert!((past - least) * 8 <= least, "{bucket}: {least}..{past}");
            }
        }
        assert_eq!(Histogram::bucket(u64::MAX), Histogram::BUCKETS - 1);
        assert_eq!(std::mem::size_of::<[u64; Histogram::BUCKETS]>(), 3_968);
    }

    #[test]
    fn counts_sums_and_quantiles_are_what_was_recorded() {
        let mut histogram = Histogram::new();
        assert_eq!(histogram.quantile(500_000), None);
        for ns in 1..=100u64 {
            histogram.record(ns * 1_000);
        }
        assert_eq!(histogram.count(), 100);
        assert_eq!(histogram.sum_ns(), 5_050_000);
        // The 50th of a hundred is 50,000 ns, in [49,152, 53,248): its upper bound.
        let (least, past) = Histogram::bounds(Histogram::bucket(50_000));
        assert_eq!((least, past), (49_152, 53_248));
        assert_eq!(histogram.quantile(500_000), Some(past - 1));
        // The most: the bucket of 100,000 ns.
        let (_, past) = Histogram::bounds(Histogram::bucket(100_000));
        assert_eq!(histogram.quantile(1_000_000), Some(past - 1));
        // The least by the nearest rank: the first value's bucket.
        let (_, past) = Histogram::bounds(Histogram::bucket(1_000));
        assert_eq!(histogram.quantile(0), Some(past - 1));
        assert_eq!(histogram.buckets().iter().sum::<u64>(), 100);
    }

    #[test]
    fn a_merge_adds_bucket_by_bucket_and_saturates() {
        let mut first = Histogram::new();
        let mut second = Histogram::new();
        first.record(5);
        first.record(1_000);
        second.record(1_000);
        second.record(u64::MAX);
        first.merge(&second);
        assert_eq!(first.count(), 4);
        assert_eq!(first.sum_ns(), u64::MAX);
        assert_eq!(first.buckets()[Histogram::bucket(5)], 1);
        assert_eq!(first.buckets()[Histogram::bucket(1_000)], 2);
        assert_eq!(first.buckets()[Histogram::BUCKETS - 1], 1);
        assert_eq!(first.quantile(1_000_000), Some(u64::MAX));
        // Recorded one at a time or merged: the same histogram.
        let mut once = Histogram::new();
        for ns in [5, 1_000, 1_000, u64::MAX] {
            once.record(ns);
        }
        assert_eq!(once, first);
    }
}
