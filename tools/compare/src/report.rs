//! One run's report: its terms, its percentiles and its counts, as JSON.
use crate::{Args, CompareError, Mode, System, Tally};
use hdrhistogram::Histogram;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Report {
    system: System,
    mode: Mode,
    durable: bool,
    /// Whether the clients spoke TLS 1.3 to the node (`--tls-ca`; the bus always does).
    tls: bool,
    /// Zero: no schedule, each answer sending the next record (a closed loop).
    offered_per_second: u32,
    /// Requests outstanding at most (`--inflight`).
    window: usize,
    payload_bytes: usize,
    measured_seconds: u64,
    redis_lanes: usize,
    sent: u64,
    acknowledged: u64,
    /// focal's bus: of those, acknowledged replicated (a majority across the group's fates) and
    /// persisted (a majority's disks).
    acknowledged_replicated: u64,
    acknowledged_persisted: u64,
    refused_by_generator: u64,
    /// Messages the schedule still held when its time was over, never offered: a rate past what
    /// the generator publishes.
    behind_schedule: u64,
    /// The times the publisher was held (its lane full), what it was held in all and the longest,
    /// milliseconds.
    publisher_holds: u64,
    publisher_held_ms: f64,
    publisher_held_most_ms: f64,
    failed: u64,
    clamped_above_120s: u64,
    first_error: Option<String>,
    /// Seconds from the schedule's close to the last answer.
    drain_seconds: f64,
    /// Acknowledgements over the measured window plus the drain.
    achieved_per_second: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    /// `deliver`: each record from its intended send time to its consumer's
    /// acknowledgement confirmed.
    deliver: Option<Delivery>,
    /// `pubsub`: each message from its publish to its arrival at the subscriber.
    pubsub: Option<Pubsub>,
}

#[derive(Debug, Serialize)]
struct Pubsub {
    /// The mean bytes of a message's payload.
    payload_bytes_mean: f64,
    /// Messages the subscriber received, the warm-up's included.
    received: u64,
    /// Messages published that never arrived (at most once).
    lost: u64,
    /// Messages received out of their publisher's order.
    reordered: u64,
    /// Messages whose stamp could not be read.
    unreadable: u64,
    /// Messages the node told the subscriber it missed (the bus's gap notices).
    missed_told: u64,
    /// Deliveries that failed their producer's check.
    corrupt: u64,
    /// Arrivals over the measured window plus the tail (the schedule's close to the last
    /// arrival): a drain waited out for messages lost is not counted as time spent delivering.
    delivered_per_second: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    /// How far behind its intended time each measured message was published: the generator's
    /// schedule, not the system.
    schedule_lag_p50_us: f64,
    schedule_lag_p99_us: f64,
    schedule_lag_max_us: f64,
    /// The bus clients' connections as their QUIC stacks counted them.
    publisher_path: Option<crate::pubsub::Path>,
    subscriber_path: Option<crate::pubsub::Path>,
}

#[derive(Debug, Serialize)]
struct Delivery {
    /// Records acknowledged by the consumer whose intended send time falls
    /// after the warm-up.
    delivered: u64,
    /// Records the producers stored that the consumer never acknowledged
    /// within its drain.
    undelivered: u64,
    failed: u64,
    first_error: Option<String>,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
}

impl Report {
    pub fn new(
        args: &Args,
        histogram: &Histogram<u64>,
        tally: &Tally,
        delivered: Option<&crate::deliver::Delivered>,
    ) -> Self {
        let micros = |nanos: u64| nanos as f64 / 1_000.0;
        let acknowledged = histogram.len();
        Self {
            system: args.system,
            mode: args.mode,
            durable: args.durable,
            tls: args.tls_ca.is_some() || args.system == System::FocalBus,
            offered_per_second: args.rate,
            window: args.inflight,
            payload_bytes: args.payload,
            measured_seconds: args.seconds,
            redis_lanes: args.lanes,
            sent: tally.sent,
            acknowledged,
            acknowledged_replicated: tally.replicated,
            acknowledged_persisted: tally.persisted,
            refused_by_generator: tally.refused,
            behind_schedule: tally.behind,
            publisher_holds: tally.holds,
            publisher_held_ms: tally.held_ns as f64 / 1e6,
            publisher_held_most_ms: tally.held_most_ns as f64 / 1e6,
            failed: tally.failed,
            clamped_above_120s: tally.clamped,
            first_error: tally.first_error.clone(),
            drain_seconds: tally.drain.as_secs_f64(),
            achieved_per_second: acknowledged as f64
                / (args.seconds as f64 + tally.drain.as_secs_f64()),
            p50_us: micros(histogram.value_at_quantile(0.50)),
            p99_us: micros(histogram.value_at_quantile(0.99)),
            p999_us: micros(histogram.value_at_quantile(0.999)),
            max_us: micros(histogram.max()),
            deliver: delivered.map(|delivered| Delivery {
                delivered: delivered.histogram.len(),
                undelivered: tally.produced.saturating_sub(delivered.acknowledged),
                failed: delivered.failed,
                first_error: delivered.first_error.clone(),
                p50_us: micros(delivered.histogram.value_at_quantile(0.50)),
                p99_us: micros(delivered.histogram.value_at_quantile(0.99)),
                p999_us: micros(delivered.histogram.value_at_quantile(0.999)),
                max_us: micros(delivered.histogram.max()),
            }),
            pubsub: None,
        }
    }

    /// A `pubsub` run's report: the publisher's counts, and the subscriber's timings.
    pub fn pubsub(
        args: &Args,
        tally: &Tally,
        received: &crate::pubsub::Received,
        lag: &crate::pubsub::Lag,
        payload_bytes_mean: f64,
    ) -> Self {
        let micros = |nanos: u64| nanos as f64 / 1_000.0;
        let timed = received.histogram.len();
        Self {
            system: args.system,
            mode: args.mode,
            durable: false,
            tls: args.tls_ca.is_some() || args.system == System::FocalBus,
            offered_per_second: args.rate,
            window: args.inflight,
            payload_bytes: args.payload,
            measured_seconds: args.seconds,
            redis_lanes: 0,
            sent: tally.sent,
            acknowledged: tally.produced,
            acknowledged_replicated: tally.replicated,
            acknowledged_persisted: tally.persisted,
            refused_by_generator: tally.refused,
            behind_schedule: tally.behind,
            publisher_holds: tally.holds,
            publisher_held_ms: tally.held_ns as f64 / 1e6,
            publisher_held_most_ms: tally.held_most_ns as f64 / 1e6,
            failed: tally.failed,
            clamped_above_120s: tally.clamped,
            first_error: tally
                .first_error
                .clone()
                .or_else(|| received.first_error.clone()),
            drain_seconds: received.drain.as_secs_f64(),
            achieved_per_second: tally.produced as f64 / (args.seconds as f64 + args.warmup as f64),
            p50_us: micros(received.histogram.value_at_quantile(0.50)),
            p99_us: micros(received.histogram.value_at_quantile(0.99)),
            p999_us: micros(received.histogram.value_at_quantile(0.999)),
            max_us: micros(received.histogram.max()),
            deliver: None,
            pubsub: Some(Pubsub {
                payload_bytes_mean,
                received: received.received,
                lost: tally.produced.saturating_sub(received.received),
                reordered: received.reordered,
                unreadable: received.unreadable,
                missed_told: received.missed_told,
                corrupt: received.corrupt,
                delivered_per_second: timed as f64
                    / (args.seconds as f64 + received.tail.as_secs_f64()),
                p50_us: micros(received.histogram.value_at_quantile(0.50)),
                p99_us: micros(received.histogram.value_at_quantile(0.99)),
                p999_us: micros(received.histogram.value_at_quantile(0.999)),
                max_us: micros(received.histogram.max()),
                schedule_lag_p50_us: micros(lag.histogram.value_at_quantile(0.50)),
                schedule_lag_p99_us: micros(lag.histogram.value_at_quantile(0.99)),
                schedule_lag_max_us: micros(lag.histogram.max()),
                publisher_path: lag.path,
                subscriber_path: received.path,
            }),
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
