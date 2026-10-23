//! How many independent jobs a pipeline runs at once, learned from their completions as a
//! session learns its write window (`pace::TickPeriod::write_window`): by Little's law the jobs
//! a pipeline holds are its rate times the time each takes, and what it completes within the
//! least time a job takes is that product counted directly, its bandwidth-delay product. The
//! window is BBR's: twice that product (its `cwnd_gain`), the most counted over its bandwidth
//! filter's window, with the least time a job takes as BBR's min-RTT filter keeps it (Cardwell et
//! al., "BBR: Congestion-Based Congestion Control", ACM Queue 2016). Never below its initial
//! window. Owned by the one task that runs the jobs: no lock, no atomic.

use std::time::{Duration, Instant};

/// The window's gain over the bandwidth-delay product: BBR's `cwnd_gain`, so completions that
/// come late or bunched (delayed acknowledgements, in BBR's case) do not hold the pipeline below
/// what it can complete.
const WINDOW_GAIN: u32 = 2;

/// A pipeline's window, as its jobs' completions teach it.
#[derive(Debug)]
pub(crate) struct CompletionWindow {
    /// The window before any job has completed, and its least.
    floor: usize,
    /// The least time a job took and when it was measured: a sample at or under it replaces it,
    /// and so does any once it is older than the filter's window (BBR's min-RTT filter).
    base: Option<(Duration, Instant)>,
    /// When the interval being counted began, and the jobs that ended within it.
    interval: Option<(Instant, u64)>,
    /// The most jobs that ended within one interval, and when it was counted: a lower count
    /// replaces it once it is older than the filter's window (BBR's bandwidth filter).
    most: (u64, Option<Instant>),
}

impl CompletionWindow {
    /// A window of `floor` jobs until the first completions say otherwise, and never fewer.
    pub(crate) fn new(floor: usize) -> Self {
        Self {
            floor: floor.max(1),
            base: None,
            interval: None,
            most: (0, None),
        }
    }

    /// A job that took `service` ended at `now`.
    pub(crate) fn ended(&mut self, service: Duration, now: Instant) {
        let service = service.max(Duration::from_nanos(1));
        let filter = crate::pace::BASE_WINDOW;
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

    /// The jobs the pipeline may run at once.
    pub(crate) fn window(&self) -> usize {
        usize::try_from(self.most.0)
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
        let mut window = CompletionWindow::new(10);
        let mut at = Duration::ZERO;
        // Before the outage: jobs of 3 ms, ten at once.
        while at < Duration::from_secs(5) {
            at += Duration::from_micros(300);
            window.ended(Duration::from_millis(3), start + at);
        }
        // The outage: every job refused at once, a thousand a second.
        while at < Duration::from_secs(35) {
            at += Duration::from_millis(1);
            if sample_refusals {
                window.ended(Duration::from_micros(20), start + at);
            }
        }
        // The drain: as many jobs in flight as the window allows, each half a second.
        let mut windows = Vec::new();
        let mut ended = 0u64;
        let service = Duration::from_millis(500);
        let mut next_second = at + Duration::from_secs(1);
        let end = at + Duration::from_secs(20);
        while at < end {
            let running = window.window();
            // A turn of half a second: the jobs running end together.
            at += service;
            for _ in 0..running {
                window.ended(service, start + at);
                ended += 1;
            }
            while at >= next_second {
                windows.push(window.window());
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
        let mut window = CompletionWindow::new(1);
        assert_eq!(window.window(), 1);
        for n in 0..200u64 {
            window.ended(Duration::from_millis(10), start + Duration::from_millis(n));
        }
        // 20 ms intervals (twice the 10 ms each job takes) of a job a millisecond.
        assert_eq!(window.window(), 20);
    }

    /// The floor holds until completions say more, and a window learned under load falls once
    /// its count is older than the filter's window and a lighter load is counted.
    #[test]
    fn a_window_never_falls_below_its_floor_and_a_stale_most_gives_way() {
        let start = Instant::now();
        let mut window = CompletionWindow::new(10);
        for n in 0..100u64 {
            window.ended(Duration::from_millis(10), start + Duration::from_millis(n));
        }
        assert_eq!(window.window(), 20);
        // Eleven seconds on, a job every 10 ms: two within each 20 ms interval, under the floor.
        let later = start + Duration::from_secs(11);
        for n in 0..100u64 {
            window.ended(
                Duration::from_millis(10),
                later + Duration::from_millis(10 * n),
            );
        }
        assert_eq!(window.window(), 10);
    }

    /// The least time a job takes is what the bound is made of: one slow job does not stretch
    /// the interval while a faster one stands within the filter's window.
    #[test]
    fn a_slow_job_does_not_stretch_the_bound_while_a_faster_one_stands() {
        let start = Instant::now();
        let mut window = CompletionWindow::new(1);
        window.ended(Duration::from_millis(10), start);
        window.ended(Duration::from_secs(5), start + Duration::from_millis(1));
        for n in 2..200u64 {
            window.ended(Duration::from_millis(10), start + Duration::from_millis(n));
        }
        assert_eq!(window.window(), 20);
    }
}
