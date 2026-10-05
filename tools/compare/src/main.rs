//! Open-loop tail-latency measurement of one durable replicated append against
//! Kafka, NATS JetStream or Redis, on the terms of
//! `docs/qualification/competitive-p99.md`. Requests leave on a fixed
//! schedule; each latency counts from the request's intended send time, so a
//! stall is charged to every request it delays (coordinated omission is
//! corrected by construction, not after the fact). Results are one JSON
//! report per run.
mod report;
mod target;

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Debug, thiserror::Error)]
pub enum CompareError {
    #[error("argument: {0}")]
    Argument(&'static str),
    #[error("target: {0}")]
    Target(String),
    #[error("histogram: {0}")]
    Histogram(String),
    #[error("report: {0}")]
    Report(String),
}

#[derive(Clone, Copy, Debug, ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum System {
    Kafka,
    Nats,
    Redis,
}

#[derive(Debug, Parser)]
#[command(about = "Open-loop durable-append latency against Kafka, NATS JetStream or Redis")]
pub struct Args {
    #[arg(long, value_enum)]
    pub system: System,
    /// Every write acknowledged only once on disk at a quorum (the durable row
    /// of the plan); without it, the system as shipped.
    #[arg(long)]
    pub durable: bool,
    /// Comma-separated host:port of the three nodes.
    #[arg(long)]
    pub endpoints: String,
    /// Offered records a second.
    #[arg(long)]
    pub rate: u32,
    /// Bytes of each record's payload.
    #[arg(long, default_value_t = 128)]
    pub payload: usize,
    /// Seconds measured, after the warm-up.
    #[arg(long, default_value_t = 60)]
    pub seconds: u64,
    /// Seconds sent and not measured, first.
    #[arg(long, default_value_t = 15)]
    pub warmup: u64,
    /// Requests outstanding at most; a request due while all are out is
    /// counted as refused by the generator, never queued in it.
    #[arg(long, default_value_t = 4096)]
    pub inflight: usize,
    /// Redis only: connections the writes are spread over (WAITAOF blocks the
    /// connection it is sent on until the replicas answer).
    #[arg(long, default_value_t = 64)]
    pub lanes: usize,
    /// Where the JSON report goes.
    #[arg(long)]
    pub out: std::path::PathBuf,
}

/// The longest latency recorded: anything slower is clamped there and counted.
pub const HIGHEST_NS: u64 = 120_000_000_000;

#[tokio::main]
async fn main() -> Result<(), CompareError> {
    let args = Args::parse();
    if args.rate == 0 || args.payload == 0 || args.inflight == 0 || args.seconds == 0 {
        return Err(CompareError::Argument(
            "rate, payload, inflight and seconds must be positive",
        ));
    }
    let endpoints: Vec<String> = args.endpoints.split(',').map(str::to_owned).collect();
    let target =
        target::Target::connect(args.system, args.durable, &endpoints, args.lanes.max(1)).await?;
    let interval = Duration::from_secs(1)
        .checked_div(args.rate)
        .ok_or(CompareError::Argument("rate"))?;
    let warmup = Duration::from_secs(args.warmup);
    let total = warmup
        .checked_add(Duration::from_secs(args.seconds))
        .ok_or(CompareError::Argument("duration"))?;
    let payload = vec![0x5a_u8; args.payload];
    let mut histogram = Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
        .map_err(|error| CompareError::Histogram(error.to_string()))?;
    let mut tally = Tally::default();
    // The requests in flight, owned here: their count is the bound, and a
    // request due while it is full is refused, never queued.
    let mut flying = JoinSet::new();
    let started = Instant::now();
    let mut next = started;
    loop {
        let open = next.saturating_duration_since(started) < total;
        if !open && flying.is_empty() {
            break;
        }
        tokio::select! {
            () = tokio::time::sleep_until(next.into()), if open => {
                let intended = next;
                next = next.checked_add(interval).ok_or(CompareError::Argument("schedule"))?;
                let measured = intended.saturating_duration_since(started) >= warmup;
                if flying.len() >= args.inflight {
                    if measured {
                        tally.refused = tally.refused.saturating_add(1);
                    }
                    continue;
                }
                tally.sent = tally.sent.saturating_add(1);
                let target = target.lane(tally.sent);
                let payload = payload.clone();
                let sequence = tally.sent;
                flying.spawn(async move {
                    let result = target.append(sequence, payload).await;
                    (intended.elapsed(), measured, result)
                });
            }
            Some(joined) = flying.join_next() => {
                let (latency, measured, result) =
                    joined.map_err(|error| CompareError::Target(error.to_string()))?;
                if measured {
                    tally.record(&mut histogram, latency, result)?;
                }
            }
        }
    }
    report::Report::new(&args, &histogram, &tally).write(&args.out)
}

/// What a run counts beside the histogram.
#[derive(Debug, Default)]
pub struct Tally {
    pub sent: u64,
    pub refused: u64,
    pub failed: u64,
    pub clamped: u64,
    pub first_error: Option<String>,
}
impl Tally {
    fn record(
        &mut self,
        histogram: &mut Histogram<u64>,
        latency: Duration,
        result: Result<(), String>,
    ) -> Result<(), CompareError> {
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        if nanos > HIGHEST_NS {
            self.clamped = self.clamped.saturating_add(1);
        }
        match result {
            Ok(()) => histogram
                .record(nanos.clamp(1_000, HIGHEST_NS))
                .map_err(|error| CompareError::Histogram(error.to_string())),
            Err(error) => {
                self.failed = self.failed.saturating_add(1);
                self.first_error.get_or_insert(error);
                Ok(())
            }
        }
    }
}
