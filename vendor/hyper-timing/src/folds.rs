//! What a node measures of itself and of the fleet for its detectors (`docs/timing.md` §2.4,
//! §2.6): the lateness of its own timed waits (the granularity `G`), the cost of the flush its
//! sender makes before each heartbeat (the stability floor `E[flush] + G`), and the fleet's
//! failures over its exposure (the MTBF).
//!
//! Each is a mean, held as a count and an exact sum: two words, whatever the number of samples. The
//! means are what the floors need. `G` floors the queues the waits feed (the sender's schedule, the
//! detector's checks), whose stability and expected delay depend on the mean service time
//! (Lindley 1952), and a sender that flushes before each heartbeat is stable only while its mean
//! service is below the interval.

use std::time::Duration;

use crate::qos::Floors;

/// An exact mean of nanosecond samples: their count and their sum. The sum is a `u128`, so it
/// cannot overflow before the count does, at 2⁶⁴ samples; a sample past that is refused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Mean {
    count: u64,
    sum: u128,
}

impl Mean {
    fn add(&mut self, sample_ns: u64) -> Result<(), FoldFull> {
        let count = self.count.checked_add(1).ok_or(FoldFull)?;
        let sum = self
            .sum
            .checked_add(u128::from(sample_ns))
            .ok_or(FoldFull)?;
        self.count = count;
        self.sum = sum;
        Ok(())
    }

    /// The mean, read at every poll and heartbeat of a detector: divided in `u64` while the sum
    /// fits one (2⁶⁴ ns is 584 years of summed samples), which the hardware divides, and in `u128`
    /// past it, which is a call into the compiler's runtime (`__udivti3`); the quotient is the same.
    fn mean(&self) -> Option<Duration> {
        let mean = match u64::try_from(self.sum) {
            Ok(sum) => sum.checked_div(self.count)?,
            Err(_) => u64::try_from(self.sum.checked_div(u128::from(self.count))?).ok()?,
        };
        Some(Duration::from_nanos(mean))
    }
}

/// A fold has taken 2⁶⁴ samples and takes no more: its mean stands as it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FoldFull;

impl std::fmt::Display for FoldFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the fold holds 2^64 samples and takes no more")
    }
}

impl std::error::Error for FoldFull {}

/// The timer's granularity `G`: the mean lateness of the detector's own timed waits, asked against
/// woke (`docs/timing.md` §2.4). A wait never ends early (`PR_SET_TIMERSLACK(2const)`; XNU's leeway
/// only delays), so a wake before its deadline counts as on time.
///
/// On macOS `G` is half of each wait (timer coalescing), so it is a property of the wait length:
/// a link that changes its interval changes the waits, and the fold is started again
/// ([`Lateness::new`]) for the new length.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lateness {
    late: Mean,
}

impl Lateness {
    /// A fold with no wait.
    pub const fn new() -> Self {
        Self {
            late: Mean { count: 0, sum: 0 },
        }
    }

    /// A timed wait asked to end at `deadline_ns` that ended at `woke_ns`, on the same monotonic
    /// clock.
    pub fn on_wait(&mut self, deadline_ns: u64, woke_ns: u64) -> Result<(), FoldFull> {
        // Saturating: a wake before its deadline was on time, not negatively late.
        self.late.add(woke_ns.saturating_sub(deadline_ns))
    }

    /// The waits folded in.
    pub const fn waits(&self) -> u64 {
        self.late.count
    }

    /// `G`, the mean lateness, or `None` before a wait. A wait that was exactly on time on a clock
    /// that cannot resolve finer gives zero, which no floor can use; [`Floors::measured`] refuses it.
    pub fn granularity(&self) -> Option<Duration> {
        self.late.mean()
    }
}

/// The wakes a sans-io detector asks of its owner and how late each came: the fold of `G`
/// ([`Lateness`]) and the latest lateness seen, which a detection bound adds (Lifeguard's local
/// health, measured: `docs/timing.md` §2.7). The detector says which wake it asked
/// ([`ask`](Self::ask)) each time it is polled and tells the fold when it was next polled
/// ([`woke`](Self::woke)): a poll at or past the wake asked is that wait's end, and one before it is
/// a poll for something else (a message), which measures nothing. hyper-swim's detector and
/// hyper-liveness's streams both measure their owner's timer through it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Wakes {
    lateness: Lateness,
    asked: Option<u64>,
    latest: u64,
}

impl Wakes {
    /// No wake asked or measured.
    pub const fn new() -> Self {
        Self {
            lateness: Lateness::new(),
            asked: None,
            latest: 0,
        }
    }

    /// The detector was polled at `now_ns`: a wake asked for at or before it ended there, and its
    /// lateness is folded.
    pub fn woke(&mut self, now_ns: u64) {
        if let Some(at) = self.asked
            && now_ns >= at
        {
            // A full fold keeps its mean: `G` stands as measured.
            let _ = self.lateness.on_wait(at, now_ns);
            self.latest = self.latest.max(now_ns.saturating_sub(at));
            self.asked = None;
        }
    }

    /// The wake the detector now asks of its owner, if any.
    pub fn ask(&mut self, at_ns: Option<u64>) {
        self.asked = at_ns;
    }

    /// The wake asked and not yet measured.
    pub const fn asked(&self) -> Option<u64> {
        self.asked
    }

    /// `G`, the mean lateness of the wakes, once measured and not zero.
    pub fn granularity(&self) -> Option<Duration> {
        self.lateness.granularity().filter(|g| !g.is_zero())
    }

    /// The fold itself.
    pub const fn lateness(&self) -> &Lateness {
        &self.lateness
    }

    /// The latest any wake has come past the one asked, or the owner is past it at `now_ns`:
    /// a stall the detector is in when it states a bound is in the bound.
    pub fn latest_ns(&self, now_ns: u64) -> u64 {
        self.asked
            .map_or(0, |at| now_ns.saturating_sub(at))
            .max(self.latest)
    }
}

/// The sender's flush before each heartbeat: the mean time from waking to send, the write and the
/// platform's full flush (`docs/timing.md` §2.1, §2.6).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Flushes {
    cost: Mean,
}

impl Flushes {
    /// A fold with no flush.
    pub const fn new() -> Self {
        Self {
            cost: Mean { count: 0, sum: 0 },
        }
    }

    /// A flush that started at `started_ns` and was durable at `durable_ns`, on one monotonic clock.
    pub fn on_flush(&mut self, started_ns: u64, durable_ns: u64) -> Result<(), FoldFull> {
        // Saturating: a clock read out of order is a flush of no measurable length.
        self.cost.add(durable_ns.saturating_sub(started_ns))
    }

    /// The flushes folded in.
    pub const fn flushes(&self) -> u64 {
        self.cost.count
    }

    /// `E[flush]`, or `None` before a flush.
    pub fn mean(&self) -> Option<Duration> {
        self.cost.mean()
    }
}

impl Floors {
    /// The floors from what was measured: the receiver's granularity `G`, the sender's mean flush
    /// (its stability floor is `E[flush] + G`, Lindley's condition with the sender's wait in its
    /// service), and the link's correlation time `T_c`, which the traces measure (§2.6). `None`
    /// while either fold is empty or `G` is zero: no floor is invented.
    pub fn measured(
        granularity: &Lateness,
        sender: &Flushes,
        correlation: Duration,
    ) -> Option<Self> {
        let granularity = granularity.granularity().filter(|g| !g.is_zero())?;
        let flush = sender.mean()?;
        Some(Self {
            granularity,
            sender: flush.saturating_add(granularity),
            correlation,
        })
    }
}

/// The fleet's node failures over its exposure, for the MTBF (`docs/timing.md` §2.6, item 3): the
/// Jeffreys posterior for a Poisson rate after `k` failures in node exposure `T` is
/// `Gamma(k + ½, T)`, mean rate `(k + ½)/T` (Jeffreys 1946), so `MTBF = T / (k + ½)`. Before the
/// first failure it is `2T`: a fleet that has run little is treated as failing as often as its
/// exposure cannot exclude.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Exposure {
    exposure: Duration,
    failures: u64,
}

impl Exposure {
    /// No exposure and no failure.
    pub const fn new() -> Self {
        Self {
            exposure: Duration::ZERO,
            failures: 0,
        }
    }

    /// Node time run: the sum over nodes of the time each was up. Saturating: past `Duration::MAX`
    /// of exposure, more changes nothing measurable.
    pub fn on_exposure(&mut self, node_time: Duration) {
        self.exposure = self.exposure.saturating_add(node_time);
    }

    /// One node failed. Saturating at 2⁶⁴ failures.
    pub fn on_failure(&mut self) {
        self.failures = self.failures.saturating_add(1);
    }

    /// The failures counted.
    pub const fn failures(&self) -> u64 {
        self.failures
    }

    /// `T / (k + ½)`, or `None` before any exposure.
    pub fn mtbf(&self) -> Option<Duration> {
        if self.exposure.is_zero() {
            return None;
        }
        // u64 → f64 rounds past 2⁵³ failures, far below any count a fleet reaches.
        let events = self.failures as f64 + 0.5;
        Duration::try_from_secs_f64(self.exposure.as_secs_f64() / events).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mean_past_a_u64_sum_is_the_wide_quotient() {
        let mut mean = Mean::default();
        mean.add(u64::MAX).unwrap();
        mean.add(u64::MAX).unwrap();
        mean.add(1).unwrap();
        let wide = (2 * u128::from(u64::MAX) + 1) / 3;
        assert_eq!(mean.mean(), Some(Duration::from_nanos(wide as u64)));
        let mut small = Mean::default();
        for sample in [3, 4, 4] {
            small.add(sample).unwrap();
        }
        assert_eq!(small.mean(), Some(Duration::from_nanos(3)));
        assert_eq!(Mean::default().mean(), None);
    }

    #[test]
    fn the_granularity_is_the_mean_lateness_and_never_negative() {
        let mut late = Lateness::new();
        assert_eq!(late.granularity(), None);
        late.on_wait(1_000, 1_400).unwrap();
        late.on_wait(2_000, 2_200).unwrap();
        // Early: on time.
        late.on_wait(3_000, 2_900).unwrap();
        assert_eq!(late.waits(), 3);
        assert_eq!(late.granularity(), Some(Duration::from_nanos(200)));
    }

    #[test]
    fn the_floors_need_both_folds_and_a_granularity() {
        let mut late = Lateness::new();
        let mut flush = Flushes::new();
        let tc = Duration::from_millis(50);
        assert_eq!(Floors::measured(&late, &flush, tc), None);
        late.on_wait(0, 0).unwrap();
        flush.on_flush(10, 4_010).unwrap();
        assert_eq!(Floors::measured(&late, &flush, tc), None, "G of zero");
        late.on_wait(0, 2_000).unwrap();
        let floors = Floors::measured(&late, &flush, tc).unwrap();
        assert_eq!(floors.granularity, Duration::from_nanos(1_000));
        assert_eq!(floors.sender, Duration::from_nanos(5_000));
        assert_eq!(floors.correlation, tc);
    }

    #[test]
    fn a_full_fold_refuses_and_keeps_its_mean() {
        let mut mean = Mean {
            count: u64::MAX,
            sum: 0,
        };
        assert_eq!(mean.add(5), Err(FoldFull));
        assert_eq!(mean.count, u64::MAX);
    }

    #[test]
    fn a_wake_is_measured_once_and_only_at_or_past_its_ask() {
        let mut wakes = Wakes::new();
        wakes.woke(5);
        assert_eq!(wakes.granularity(), None, "nothing asked");
        wakes.ask(Some(1_000));
        wakes.woke(900);
        assert_eq!(
            wakes.asked(),
            Some(1_000),
            "a poll for a message measures nothing"
        );
        assert_eq!(wakes.latest_ns(1_500), 500, "late now counts");
        wakes.woke(1_300);
        assert_eq!(wakes.granularity(), Some(Duration::from_nanos(300)));
        assert_eq!(wakes.asked(), None);
        wakes.woke(9_000);
        assert_eq!(wakes.lateness().waits(), 1, "one wake, one sample");
        wakes.ask(Some(10_000));
        wakes.woke(10_100);
        assert_eq!(wakes.granularity(), Some(Duration::from_nanos(200)));
        assert_eq!(wakes.latest_ns(0), 300);
    }

    #[test]
    fn the_mtbf_is_the_jeffreys_posterior_mean() {
        let mut fleet = Exposure::new();
        assert_eq!(fleet.mtbf(), None);
        fleet.on_exposure(Duration::from_secs(3_600));
        assert_eq!(fleet.mtbf(), Some(Duration::from_secs(7_200)), "2T");
        fleet.on_failure();
        fleet.on_exposure(Duration::from_secs(3_600));
        assert_eq!(fleet.mtbf(), Some(Duration::from_secs(4_800)), "T / 1.5");
        assert_eq!(fleet.failures(), 1);
    }
}
