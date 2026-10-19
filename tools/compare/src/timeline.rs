//! Each second of a run, by its records' intended sends: how many were acknowledged, and their
//! latencies' p50, p99 and largest. With the wall clock the run began at, a tail can be read
//! against the nodes' own lines (`focal start bus` states each event of its group with the wall
//! clock's time).

use crate::{CompareError, HIGHEST_NS};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The seconds of a run, as acknowledgements fill them.
#[derive(Debug, Default)]
pub struct Timeline {
    /// One histogram a second from the run's start, at most one for each second the run lasts.
    seconds: Vec<Histogram<u64>>,
    /// The seconds the run lasts: no record is intended past them.
    bound: usize,
    /// The wall clock's time as the run began, milliseconds since the Unix epoch.
    started_unix_ms: u64,
}

/// One second of a run, as its report states it.
#[derive(Debug, Serialize)]
pub struct Second {
    /// Seconds from the run's start to the second's.
    pub second: usize,
    pub acknowledged: u64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
}

impl Timeline {
    /// A run of `total`, beginning now.
    pub fn new(total: Duration) -> Result<Self, CompareError> {
        let bound = usize::try_from(total.as_secs())
            .ok()
            .and_then(|seconds| seconds.checked_add(usize::from(total.subsec_nanos() > 0)))
            .ok_or(CompareError::Argument("duration"))?;
        let started_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|since| u64::try_from(since.as_millis()).ok())
            .unwrap_or(0);
        Ok(Self {
            seconds: Vec::new(),
            bound,
            started_unix_ms,
        })
    }

    /// The wall clock's time as the run began, milliseconds since the Unix epoch.
    pub const fn started_unix_ms(&self) -> u64 {
        self.started_unix_ms
    }

    /// A record intended `intended` from the run's start, acknowledged `latency` after it.
    pub fn record(&mut self, intended: Duration, latency: Duration) -> Result<(), CompareError> {
        let last = self.bound.saturating_sub(1);
        let second = usize::try_from(intended.as_secs())
            .unwrap_or(usize::MAX)
            .min(last);
        // Bounded: the run's seconds, each made once.
        while self.seconds.len() <= second {
            self.seconds.push(
                Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 2)
                    .map_err(|error| CompareError::Histogram(error.to_string()))?,
            );
        }
        let nanos = u64::try_from(latency.as_nanos())
            .unwrap_or(u64::MAX)
            .clamp(1_000, HIGHEST_NS);
        self.seconds
            .get_mut(second)
            .ok_or(CompareError::Argument("timeline"))?
            .record(nanos)
            .map_err(|error| CompareError::Histogram(error.to_string()))
    }

    /// Every second a record was acknowledged in, in order.
    pub fn seconds(&self) -> Vec<Second> {
        let micros = |nanos: u64| nanos as f64 / 1_000.0;
        self.seconds
            .iter()
            .enumerate()
            .filter(|(_, histogram)| !histogram.is_empty())
            .map(|(second, histogram)| Second {
                second,
                acknowledged: histogram.len(),
                p50_us: micros(histogram.value_at_quantile(0.50)),
                p99_us: micros(histogram.value_at_quantile(0.99)),
                max_us: micros(histogram.max()),
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn each_record_is_counted_in_the_second_it_was_intended_in() {
        let mut timeline = Timeline::new(Duration::from_secs(3)).unwrap();
        timeline.record(10 * MS, 2 * MS).unwrap();
        timeline.record(999 * MS, 40 * MS).unwrap();
        timeline.record(2_500 * MS, MS).unwrap();
        let seconds = timeline.seconds();
        // The second with nothing acknowledged is left out.
        assert_eq!(seconds.len(), 2);
        assert_eq!((seconds[0].second, seconds[0].acknowledged), (0, 2));
        assert!((seconds[0].max_us - 40_000.0).abs() < 400.0);
        assert_eq!((seconds[1].second, seconds[1].acknowledged), (2, 1));
    }

    #[test]
    fn a_record_past_the_run_is_counted_in_its_last_second() {
        let mut timeline = Timeline::new(Duration::from_millis(1_500)).unwrap();
        timeline.record(Duration::from_secs(9), MS).unwrap();
        let seconds = timeline.seconds();
        assert_eq!(seconds.len(), 1);
        assert_eq!(seconds[0].second, 1);
    }
}
