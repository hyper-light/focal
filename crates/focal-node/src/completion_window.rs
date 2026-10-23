//! How many independent jobs a pipeline runs at once, learned from their completions as a
//! session learns its write window (`pace::TickPeriod::write_window`): by Little's law the jobs
//! a pipeline holds are its rate times the time each takes, and what it completes within the
//! least time a job takes is that product counted directly, its bandwidth-delay product. The
//! window is BBR's: twice that product (its `cwnd_gain`), the most counted over its bandwidth
//! filter's window, with the least time a job takes as BBR's min-RTT filter keeps it (Cardwell et
//! al., "BBR: Congestion-Based Congestion Control", ACM Queue 2016). Never below its initial
//! window. Owned by the one task that runs the jobs: no lock, no atomic.
//!
//! A pipeline whose jobs are of different kinds keeps a filter a kind ([`CompletionWindow`]): the
//! least time a job takes is a kind's, as a round trip is a path's, and a kind that ends in a
//! millisecond (an obligation whose copies already hold receipts) beside one that takes its
//! copies' round trips (an artifact's seal) made the least the fast kind's, its interval too short
//! to count the slow kind's completions, and the window stood at its floor whatever the slow kind
//! needed. The jobs held are the sum over the kinds of each kind's rate times its time (Little's
//! law holds class by class), so the window is the sum of the kinds' products, each counted over
//! its own least.

use std::time::{Duration, Instant};

/// The window's gain over the bandwidth-delay product: BBR's `cwnd_gain`, so completions that
/// come late or bunched (delayed acknowledgements, in BBR's case) do not hold the pipeline below
/// what it can complete.
const WINDOW_GAIN: u32 = 2;

/// One kind's filter: the least time its jobs take and the most of them that ended within twice
/// that.
#[derive(Debug, Default)]
struct Filter {
    /// The least time a job took and when it was measured: a sample at or under it replaces it,
    /// and so does any once it is older than the filter's window (BBR's min-RTT filter).
    base: Option<(Duration, Instant)>,
    /// When the interval being counted began, and the jobs that ended within it.
    interval: Option<(Instant, u64)>,
    /// The most jobs that ended within one interval, and when it was counted: a lower count
    /// replaces it once it is older than the filter's window (BBR's bandwidth filter).
    most: (u64, Option<Instant>),
    /// When its last job ended: a kind that has ended none within the filter's window holds no
    /// share of the window.
    last: Option<Instant>,
}

impl Filter {
    fn ended(&mut self, service: Duration, now: Instant) {
        let service = service.max(Duration::from_nanos(1));
        let filter = crate::pace::BASE_WINDOW;
        self.last = Some(now);
        let base = match self.base {
            Some((least, at)) if service > least && now.saturating_duration_since(at) <= filter => {
                least
            }
            _ => {
                self.base = Some((service, now));
                service
            }
        };
        let bound = base.saturating_mul(WINDOW_GAIN);
        match self.interval {
            None => self.interval = Some((now, 1)),
            Some((began, count)) if now.saturating_duration_since(began) < bound => {
                self.interval = Some((began, count.saturating_add(1)));
            }
            Some((began, count)) => {
                // The interval ran its bound: what ended within it is a sample, dated from when
                // it began (an interval a quiet spell closed is as old as its jobs), and this
                // job begins the next.
                let (most, seen) = self.most;
                let stale = seen.is_none_or(|seen| now.saturating_duration_since(seen) > filter);
                if count >= most || stale {
                    self.most = (count, Some(began));
                }
                self.interval = Some((now, 1));
            }
        }
    }

    /// Its share of the window at `now`: the most it counted, while it has ended a job within the
    /// filter's window.
    fn share(&self, now: Instant) -> u64 {
        match self.last {
            Some(last) if now.saturating_duration_since(last) <= crate::pace::BASE_WINDOW => {
                self.most.0
            }
            _ => 0,
        }
    }
}

/// A pipeline's window, as the completions of its `KINDS` kinds of job teach it.
#[derive(Debug)]
pub(crate) struct CompletionWindow<const KINDS: usize> {
    /// The window before any job has completed, and its least.
    floor: usize,
    kinds: [Filter; KINDS],
}

impl<const KINDS: usize> CompletionWindow<KINDS> {
    /// A window of `floor` jobs until the first completions say otherwise, and never fewer.
    pub(crate) fn new(floor: usize) -> Self {
        Self {
            floor: floor.max(1),
            kinds: std::array::from_fn(|_| Filter::default()),
        }
    }

    /// A job of kind `kind` that took `service` ended at `now`. A kind past the pipeline's is
    /// none of its jobs: no sample.
    pub(crate) fn ended(&mut self, kind: usize, service: Duration, now: Instant) {
        if let Some(filter) = self.kinds.get_mut(kind) {
            filter.ended(service, now);
        }
    }

    /// The least time a job of kind `kind` took within the filter's window, once any has ended.
    pub(crate) fn least(&self, kind: usize) -> Option<Duration> {
        self.kinds
            .get(kind)
            .and_then(|filter| filter.base.map(|(least, _)| least))
    }

    /// The most jobs of kind `kind` that ended within one interval of its filter.
    #[cfg(test)]
    pub(crate) fn most(&self, kind: usize) -> u64 {
        self.kinds.get(kind).map_or(0, |filter| filter.most.0)
    }

    /// The jobs the pipeline may run at once at `now`: the kinds' shares together, never under
    /// the floor.
    pub(crate) fn window(&self, now: Instant) -> usize {
        let shares = self
            .kinds
            .iter()
            .fold(0u64, |sum, filter| sum.saturating_add(filter.share(now)));
        usize::try_from(shares)
            .unwrap_or(usize::MAX)
            .max(self.floor)
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::arithmetic_side_effects,
        clippy::indexing_slicing
    )
)]
mod tests {
    use super::*;

    /// The drain after an outage, replayed: thirty seconds of jobs refused in microseconds, then
    /// a backlog served by jobs that each take half a second, as many at once as the window
    /// holds. Returns the window a second from the outage's end on, and how many jobs ended in
    /// those twenty seconds, with the refusals sampled or not.
    fn drain(sample_refusals: bool) -> (Vec<usize>, u64) {
        let start = Instant::now();
        let mut window = CompletionWindow::<1>::new(10);
        let mut at = Duration::ZERO;
        // Before the outage: jobs of 3 ms, ten at once.
        while at < Duration::from_secs(5) {
            at += Duration::from_micros(300);
            window.ended(0, Duration::from_millis(3), start + at);
        }
        // The outage: every job refused at once, a thousand a second.
        while at < Duration::from_secs(35) {
            at += Duration::from_millis(1);
            if sample_refusals {
                window.ended(0, Duration::from_micros(20), start + at);
            }
        }
        // The drain: as many jobs in flight as the window allows, each half a second.
        let mut windows = Vec::new();
        let mut ended = 0u64;
        let service = Duration::from_millis(500);
        let mut next_second = at + Duration::from_secs(1);
        let end = at + Duration::from_secs(20);
        while at < end {
            let running = window.window(start + at);
            // A turn of half a second: the jobs running end together.
            at += service;
            for _ in 0..running {
                window.ended(0, service, start + at);
                ended += 1;
            }
            while at >= next_second {
                windows.push(window.window(start + at));
                next_second += Duration::from_secs(1);
            }
        }
        (windows, ended)
    }

    /// A refusal before a job's work is no sample of the pipeline's service: sampled, the
    /// microseconds it took held the window at its floor through the first ten seconds of a
    /// half-second backlog; not, the window grows with what the pipeline completes.
    #[test]
    fn refusals_sampled_hold_the_window_at_its_floor_through_a_drain() {
        let (sampled, sampled_ended) = drain(true);
        let (served, served_ended) = drain(false);
        // Sampled, the refusals' twenty microseconds stand as the least a job takes for the
        // filter's ten seconds, and the window stays at its floor until they go.
        assert_eq!(sampled.get(..10), Some(&[10; 10][..]));
        // Not sampled, the half second the backlog's jobs take is the least, and the window
        // grows with what they complete from the drain's first seconds.
        assert!(
            served.get(1).is_some_and(|window| *window > 10),
            "{served:?}"
        );
        assert!(served_ended > sampled_ended);
    }

    /// A pipeline that completes a job every millisecond, each taking 10 ms, holds ten at once
    /// by Little's law: its window is twice that, learned from the completions alone.
    #[test]
    fn a_window_is_twice_what_the_pipeline_completes_in_the_least_time_a_job_takes() {
        let start = Instant::now();
        let mut window = CompletionWindow::<1>::new(1);
        assert_eq!(window.window(start), 1);
        for n in 0..200u64 {
            window.ended(
                0,
                Duration::from_millis(10),
                start + Duration::from_millis(n),
            );
        }
        // 20 ms intervals (twice the 10 ms each job takes) of a job a millisecond.
        assert_eq!(window.window(start + Duration::from_millis(200)), 20);
    }

    /// The floor holds until completions say more, and a window learned under load falls once
    /// its count is older than the filter's window and a lighter load is counted.
    #[test]
    fn a_window_never_falls_below_its_floor_and_a_stale_most_gives_way() {
        let start = Instant::now();
        let mut window = CompletionWindow::<1>::new(10);
        for n in 0..100u64 {
            window.ended(
                0,
                Duration::from_millis(10),
                start + Duration::from_millis(n),
            );
        }
        assert_eq!(window.window(start + Duration::from_millis(100)), 20);
        // Eleven seconds on, a job every 10 ms: two within each 20 ms interval, under the floor.
        let later = start + Duration::from_secs(11);
        for n in 0..100u64 {
            window.ended(
                0,
                Duration::from_millis(10),
                later + Duration::from_millis(10 * n),
            );
        }
        assert_eq!(window.window(later + Duration::from_secs(1)), 10);
    }

    /// The least time a job takes is what the bound is made of: one slow job does not stretch
    /// the interval while a faster one stands within the filter's window.
    #[test]
    fn a_slow_job_does_not_stretch_the_bound_while_a_faster_one_stands() {
        let start = Instant::now();
        let mut window = CompletionWindow::<1>::new(1);
        window.ended(0, Duration::from_millis(10), start);
        window.ended(0, Duration::from_secs(5), start + Duration::from_millis(1));
        for n in 2..200u64 {
            window.ended(
                0,
                Duration::from_millis(10),
                start + Duration::from_millis(n),
            );
        }
        assert_eq!(window.window(start + Duration::from_millis(200)), 20);
    }

    /// Two kinds of job at once, a hundred a second each: one ends in a millisecond (an
    /// obligation whose copies hold receipts), the other takes 250 ms (an artifact's seal). Kept
    /// apart, the slow kind's product is counted over its own least, twice its 25 in flight, and
    /// the fast kind's beside it; counted as one kind, the fast kind's least made each interval
    /// 2 ms and the window stood at its floor (the evidence window at ten in every work run, the
    /// leader holding 4,201 artifact requests at its door over a lossy path).
    #[test]
    fn a_fast_kind_does_not_hold_a_slow_kinds_window_at_its_floor() {
        let start = Instant::now();
        let mut apart = CompletionWindow::<2>::new(10);
        let mut together = CompletionWindow::<1>::new(10);
        for n in 0..500u64 {
            let at = start + Duration::from_millis(10 * n);
            apart.ended(0, Duration::from_millis(1), at);
            apart.ended(1, Duration::from_millis(250), at);
            together.ended(0, Duration::from_millis(1), at);
            together.ended(0, Duration::from_millis(250), at);
        }
        let end = start + Duration::from_secs(5);
        assert_eq!(together.window(end), 10);
        // The slow kind: 500 ms intervals of a job every 10 ms, fifty; the fast kind: 2 ms
        // intervals of one.
        assert_eq!(apart.most(1), 50);
        assert_eq!(apart.most(0), 1);
        assert_eq!(apart.window(end), 51);
        assert_eq!(apart.least(1), Some(Duration::from_millis(250)));
    }

    /// A kind that has ended no job within the filter's window holds no share: the window a busy
    /// kind taught gives way to the floor once that kind's jobs stop.
    #[test]
    fn a_kind_gone_quiet_gives_its_share_back() {
        let start = Instant::now();
        let mut window = CompletionWindow::<2>::new(10);
        for n in 0..500u64 {
            window.ended(
                1,
                Duration::from_millis(250),
                start + Duration::from_millis(10 * n),
            );
        }
        let last = start + Duration::from_millis(4_990);
        assert_eq!(window.window(last), 50);
        assert_eq!(window.window(last + crate::pace::BASE_WINDOW), 50);
        assert_eq!(
            window.window(last + crate::pace::BASE_WINDOW + Duration::from_millis(1)),
            10
        );
        // A kind past the window's is no sample.
        window.ended(7, Duration::from_millis(1), last);
        assert_eq!(window.most(7), 0);
        assert_eq!(window.least(7), None);
    }
}
