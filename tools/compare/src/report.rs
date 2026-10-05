//! One run's report: its terms, its percentiles and its counts, as JSON.
use crate::{Args, CompareError, System, Tally};
use hdrhistogram::Histogram;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Report {
    system: System,
    durable: bool,
    offered_per_second: u32,
    payload_bytes: usize,
    measured_seconds: u64,
    redis_lanes: usize,
    sent: u64,
    acknowledged: u64,
    refused_by_generator: u64,
    failed: u64,
    clamped_above_120s: u64,
    first_error: Option<String>,
    achieved_per_second: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
}

impl Report {
    pub fn new(args: &Args, histogram: &Histogram<u64>, tally: &Tally) -> Self {
        let micros = |nanos: u64| nanos as f64 / 1_000.0;
        let acknowledged = histogram.len();
        Self {
            system: args.system,
            durable: args.durable,
            offered_per_second: args.rate,
            payload_bytes: args.payload,
            measured_seconds: args.seconds,
            redis_lanes: args.lanes,
            sent: tally.sent,
            acknowledged,
            refused_by_generator: tally.refused,
            failed: tally.failed,
            clamped_above_120s: tally.clamped,
            first_error: tally.first_error.clone(),
            achieved_per_second: acknowledged as f64 / args.seconds as f64,
            p50_us: micros(histogram.value_at_quantile(0.50)),
            p99_us: micros(histogram.value_at_quantile(0.99)),
            p999_us: micros(histogram.value_at_quantile(0.999)),
            max_us: micros(histogram.max()),
        }
    }

    pub fn write(&self, path: &std::path::Path) -> Result<(), CompareError> {
        let text = serde_json::to_string_pretty(self)
            .map_err(|error| CompareError::Report(error.to_string()))?;
        std::fs::write(path, text).map_err(|error| CompareError::Report(error.to_string()))?;
        use std::io::Write as _;
        let line =
            serde_json::to_string(self).map_err(|error| CompareError::Report(error.to_string()))?;
        writeln!(std::io::stdout().lock(), "{line}")
            .map_err(|error| CompareError::Report(error.to_string()))
    }
}
