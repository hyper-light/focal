//! The node log's counts as focal reports them (doc 27 §15.10): what a node on the shell exports in
//! place of focal-log's writer counts, taken from hyper-log's own statistics without holding
//! them. Every latency is in nanoseconds; a quantile is the largest value of the bucket that holds
//! it, at most an eighth above the value itself (`hyper_timing::Histogram::quantile`).
use hyper_log::LogStats;

/// One of the log's latencies: how many were timed, their sum, and three quantiles, each `None`
/// while nothing was timed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogLatency {
    pub count: u64,
    pub sum_ns: u64,
    pub p50_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
}

/// The node log's counts at one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogMetrics {
    /// Frames written and flushed.
    pub frames: u64,
    /// Updates those frames carried.
    pub updates: u64,
    /// Bytes written: frames, persist records and confirmations.
    pub bytes: u64,
    /// Flushes of the file.
    pub flushes: u64,
    /// Each flush.
    pub flush: LogLatency,
    /// Each frame's writes.
    pub write: LogLatency,
    /// Each update, from its submission to the flush that let it be answered.
    pub commit_wait: LogLatency,
    /// How long the flush now in progress has run, if one is: a stalled device shows here before
    /// any histogram hears of it.
    pub flushing_ns: Option<u64>,
    /// Confirmations the device made durable on their own (a FUA write where it has FUA), each
    /// without a flush of its cache.
    pub durable_writes: u64,
    /// Durable writes made with a write and a flush instead: a file that is not direct, off Linux,
    /// or a kernel without `RWF_DSYNC`.
    pub durable_fallbacks: u64,
    /// Confirmations written and flushed where the device measured that cheaper than its durable
    /// write.
    pub flushed_confirmations: u64,
    /// Bytes of zeros written over new slots ahead of their first frames.
    pub zeroed_bytes: u64,
    /// Bytes sweeps copied a second time to free the log's tail.
    pub swept_bytes: u64,
}

impl LogMetrics {
    /// The counts `stats` holds.
    pub fn of(stats: &LogStats) -> Self {
        let latency = |count: u64, sum_ns: u64, q: &dyn Fn(u32) -> Option<u64>| LogLatency {
            count,
            sum_ns,
            p50_ns: q(500_000),
            p99_ns: q(990_000),
            p999_ns: q(999_000),
        };
        Self {
            frames: stats.frames,
            updates: stats.updates,
            bytes: stats.bytes,
            flushes: stats.flushes,
            flush: latency(stats.flush.count(), stats.flush.sum_ns(), &|q| {
                stats.flush.quantile(q)
            }),
            write: latency(stats.write.count(), stats.write.sum_ns(), &|q| {
                stats.write.quantile(q)
            }),
            commit_wait: latency(
                stats.commit_wait.count(),
                stats.commit_wait.sum_ns(),
                &|q| stats.commit_wait.quantile(q),
            ),
            flushing_ns: stats.flushing_since.map(|since| {
                u64::try_from(stats.at.saturating_duration_since(since).as_nanos())
                    .unwrap_or(u64::MAX)
            }),
            durable_writes: stats.durable_writes,
            durable_fallbacks: stats.durable_fallbacks,
            flushed_confirmations: stats.flushed_confirmations,
            zeroed_bytes: stats.zeroed_bytes,
            swept_bytes: stats.swept_bytes,
        }
    }
}
