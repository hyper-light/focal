//! Open-loop tail-latency measurement of one durable replicated append against
//! Kafka, NATS JetStream or Redis, on the terms of
//! `docs/qualification/competitive-p99.md`. Requests leave on a fixed
//! schedule; each latency counts from the request's intended send time, so a
//! stall is charged to every request it delays (coordinated omission is
//! corrected by construction, not after the fact). Results are one JSON
//! report per run. In `deliver` mode each record is also taken by a
//! consumer and acknowledged durably, and timed from its intended send time
//! to that acknowledgement (`deliver.rs`). At a rate of zero there is no schedule: each answer sends
//! the next record, the window held full (a closed loop, Kafka's `producer-perf-test` with
//! `--throughput -1`), each record timed from its send, for a system's ceiling at that window.
mod bus;
mod bus_append;
mod certs;
mod deliver;
mod pubsub;
mod report;
mod schedule;
mod target;
mod timeline;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum System {
    Kafka,
    Nats,
    Redis,
    /// focal's event bus (doc 32): `pubsub` only, its ephemeral class.
    FocalBus,
}

/// What becomes of each record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Appended and acknowledged as stored: the durable append alone.
    #[default]
    Append,
    /// Appended, then taken by a consumer and acknowledged by it durably: a
    /// work queue's message end to end, focal's `deliver` lifecycle.
    Deliver,
    /// Published at most once to a subscriber placed first, timed from the
    /// intended send to the delivery: NATS core, Redis Pub/Sub and focal's
    /// event bus, each as shipped, on one node.
    Pubsub,
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
    /// Offered records a second; `0`, no schedule: each answer sends the next record, `inflight`
    /// requests out at all times (a closed loop), each timed from its send.
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
    /// Requests outstanding at most (the window); a request due while all are
    /// out is counted as refused by the generator, never queued in it.
    #[arg(long, default_value_t = 4096)]
    pub inflight: usize,
    /// Redis only: connections the writes are spread over (WAITAOF blocks the
    /// connection it is sent on until the replicas answer).
    #[arg(long, default_value_t = 64)]
    pub lanes: usize,
    /// What becomes of each record: `append` (stored), or `deliver`
    /// (consumed and acknowledged as well).
    #[arg(long, value_enum, default_value_t = Mode::Append)]
    pub mode: Mode,
    /// Where the JSON report goes.
    #[arg(long)]
    pub out: std::path::PathBuf,
    /// `pubsub`: each message one of a shape of focal's dense JSON corpus (claim, event,
    /// testament or entropy), compressed as a claimant sends it (zstd 3 with a 64 KiB
    /// dictionary trained on another seed), its first 16 bytes the stamp; without it, `payload`
    /// bytes of one value.
    #[arg(long)]
    pub corpus: Option<String>,
    /// `focal-bus`: the node's local identity directory (`focal start bus --pki`), shared with it.
    #[arg(long)]
    pub pki: Option<std::path::PathBuf>,
    /// `nats`: connect over TLS 1.3, trusting only this CA certificate (PEM), as a production NATS
    /// deployment runs; without it the clients connect in plaintext.
    #[arg(long)]
    pub tls_ca: Option<std::path::PathBuf>,
}

/// `focal-compare certs DIR`: the bench's own certificates instead of a run.
#[derive(Debug, Parser)]
#[command(about = "Write the bench's own CA and the nats-tls node's certificate")]
struct Certs {
    /// Where `ca.pem`, `server.pem` and `server.key` (PKCS #8) are written.
    dir: std::path::PathBuf,
}

/// The longest latency recorded: anything slower is clamped there and counted.
pub const HIGHEST_NS: u64 = 120_000_000_000;

#[tokio::main]
async fn main() -> Result<(), CompareError> {
    // `certs` is a task of its own, its arguments none of a run's.
    if std::env::args_os()
        .nth(1)
        .is_some_and(|first| first == "certs")
    {
        let task = Certs::parse_from(std::env::args_os().skip(1));
        return certs::write(&task.dir);
    }
    let args = Args::parse();
    if args.payload == 0 || args.inflight == 0 || args.seconds == 0 {
        return Err(CompareError::Argument(
            "payload, inflight and seconds must be positive",
        ));
    }
    if args.mode == Mode::Pubsub {
        if args.rate == 0 {
            return Err(CompareError::Argument(
                "pubsub runs on a schedule: a positive rate",
            ));
        }
        return pubsub_main(args).await;
    }
    if args.system == System::FocalBus {
        if args.mode != Mode::Append {
            return Err(CompareError::Argument(
                "focal-bus runs the pubsub and append modes",
            ));
        }
        return bus_append_main(args).await;
    }
    let deliver = args.mode == Mode::Deliver;
    if deliver && args.payload < deliver::HEADER {
        return Err(CompareError::Argument(
            "deliver needs a payload of 16 bytes or more",
        ));
    }
    let endpoints: Vec<String> = args.endpoints.split(',').map(str::to_owned).collect();
    let target = target::Target::connect(
        args.system,
        args.durable,
        &endpoints,
        args.lanes.max(1),
        deliver,
    )
    .await?;
    // The consumer is placed before the first record is sent, so it takes every one.
    let consumer = match args.mode {
        Mode::Deliver => {
            Some(deliver::Consumer::connect(args.system, args.durable, &endpoints, &target).await?)
        }
        // `pubsub` returned above, with its own subscriber.
        Mode::Append | Mode::Pubsub => None,
    };
    let (produced, stop) = tokio::sync::watch::channel(None);
    // One record due each `interval`; none (a rate of zero), a closed loop.
    let interval = match args.rate {
        0 => None,
        rate => Some(
            Duration::from_secs(1)
                .checked_div(rate)
                .filter(|interval| !interval.is_zero())
                .ok_or(CompareError::Argument("rate"))?,
        ),
    };
    let warmup = Duration::from_secs(args.warmup);
    let total = warmup
        .checked_add(Duration::from_secs(args.seconds))
        .ok_or(CompareError::Argument("duration"))?;
    let mut histogram = Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
        .map_err(|error| CompareError::Histogram(error.to_string()))?;
    let mut tally = Tally::default();
    // The requests in flight, owned here: their count is the bound, and a
    // request due while it is full is refused, never queued.
    let mut flying: JoinSet<Answer> = JoinSet::new();
    let started = Instant::now();
    tally.timeline = timeline::Timeline::new(total)?;
    let sender = Sender {
        target,
        payload: vec![0x5a_u8; args.payload],
        deliver,
        started,
    };
    let consuming = consumer
        .map(|consumer| tokio::spawn(consumer.run(started, warmup, deliver::Stop::new(stop))));
    let mut next = started;
    // When the schedule closed and when its last answer came: a system that
    // answers long after the window is charged the time it took.
    let mut closed = None;
    let mut last_answer = started;
    loop {
        let open = match interval {
            Some(_) => next.saturating_duration_since(started) < total,
            None => started.elapsed() < total,
        };
        if !open && closed.is_none() {
            closed = Some(Instant::now());
        }
        if !open && flying.is_empty() {
            break;
        }
        let room = flying.len() < args.inflight;
        tokio::select! {
            // Answers first: the window a due record is judged against has every answer's place back.
            biased;
            Some(joined) = flying.join_next() => {
                let (intended, latency, measured, result) =
                    joined.map_err(|error| CompareError::Target(error.to_string()))?;
                last_answer = Instant::now();
                if result.is_ok() {
                    tally.produced = tally.produced.saturating_add(1);
                    tally
                        .timeline
                        .record(intended.saturating_duration_since(started), latency)?;
                }
                if measured {
                    tally.record(&mut histogram, latency, result)?;
                }
            }
            // A closed loop: a place free, the next record goes now.
            () = std::future::ready(()), if open && room && interval.is_none() => {
                let intended = Instant::now();
                let measured = intended.saturating_duration_since(started) >= warmup;
                sender.send(&mut flying, &mut tally, intended, measured)?;
            }
            () = tokio::time::sleep_until(next.into()), if open && interval.is_some() => {
                let Some(interval) = interval else {
                    continue;
                };
                if !room {
                    // Every record due by now finds the window full: all counted refused at once,
                    // the schedule moved past them.
                    let skipped = schedule::overdue(
                        next.saturating_duration_since(started),
                        started.elapsed(),
                        interval,
                        warmup,
                        total,
                    )?;
                    tally.refused = tally.refused.saturating_add(skipped.measured);
                    next = started
                        .checked_add(skipped.next)
                        .ok_or(CompareError::Argument("schedule"))?;
                    continue;
                }
                let intended = next;
                next = next.checked_add(interval).ok_or(CompareError::Argument("schedule"))?;
                let measured = intended.saturating_duration_since(started) >= warmup;
                sender.send(&mut flying, &mut tally, intended, measured)?;
            }
        }
    }
    tally.drain = closed.map_or(Duration::ZERO, |closed| {
        last_answer.saturating_duration_since(closed)
    });
    // Every record the producers stored is the consumer's to acknowledge before it stops.
    let _ = produced.send(Some(tally.produced));
    let delivered = match consuming {
        Some(consuming) => Some(
            consuming
                .await
                .map_err(|error| CompareError::Target(error.to_string()))??,
        ),
        None => None,
    };
    report::Report::new(&args, &histogram, &tally, delivered.as_ref()).write(&args.out)
}

/// The `append` mode against focal's event bus: a part replicated by the nodes `--endpoints` names,
/// node 1 first, each record timed from its intended send to its persisted acknowledgement
/// (`bus_append`).
async fn bus_append_main(args: Args) -> Result<(), CompareError> {
    let mut nodes = Vec::new();
    for endpoint in args.endpoints.split(',') {
        let address = tokio::net::lookup_host(endpoint)
            .await
            .map_err(|error| CompareError::Target(error.to_string()))?
            .next()
            .ok_or(CompareError::Argument("a bus node's address"))?;
        nodes.push(address);
    }
    let run = bus_append::Run {
        nodes,
        pki: args
            .pki
            .clone()
            .ok_or(CompareError::Argument("focal-bus needs --pki"))?,
        rate: args.rate,
        warmup: Duration::from_secs(args.warmup),
        total: Duration::from_secs(args.warmup)
            .checked_add(Duration::from_secs(args.seconds))
            .ok_or(CompareError::Argument("duration"))?,
        payload: args.payload,
        inflight: args.inflight,
    };
    let (histogram, tally) = tokio::task::spawn_blocking(move || bus_append::run(run))
        .await
        .map_err(|error| CompareError::Target(error.to_string()))??;
    report::Report::new(&args, &histogram, &tally, None).write(&args.out)
}

/// The `pubsub` mode's messages: a shape of focal's corpus, compressed as a claimant sends it, or
/// `payload` bytes of one value; each at least the stamp's 16 bytes.
fn payloads(args: &Args) -> Result<Vec<Vec<u8>>, CompareError> {
    let Some(shape) = args.corpus.as_deref() else {
        return Ok(vec![vec![0x5a_u8; args.payload.max(deliver::HEADER)]]);
    };
    let shape = focal_bus::corpus::Shape::ALL
        .into_iter()
        .find(|known| known.label() == shape)
        .ok_or(CompareError::Argument(
            "corpus: claim, event, testament or entropy",
        ))?;
    let fail = |error: std::io::Error| {
        CompareError::Argument(if error.kind() == std::io::ErrorKind::Other {
            "corpus codec"
        } else {
            "corpus"
        })
    };
    let mut text = String::new();
    let mut train = focal_bus::corpus::Corpus::new(0x5EED_0001);
    let mut samples = Vec::new();
    for _ in 0..2_000 {
        train
            .next(shape, &mut text)
            .map_err(|_| CompareError::Argument("corpus"))?;
        samples.push(text.as_bytes().to_vec());
    }
    let dictionary = zstd::dict::from_samples(&samples, 64 * 1024).map_err(fail)?;
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &dictionary).map_err(fail)?;
    let mut test = focal_bus::corpus::Corpus::new(0x5EED_0002);
    let mut payloads = Vec::new();
    for _ in 0..1_024 {
        test.next(shape, &mut text)
            .map_err(|_| CompareError::Argument("corpus"))?;
        let mut compressed = compressor.compress(text.as_bytes()).map_err(fail)?;
        if compressed.len() < deliver::HEADER {
            compressed.resize(deliver::HEADER, 0);
        }
        payloads.push(compressed);
    }
    Ok(payloads)
}

/// The `pubsub` mode: a publisher on the open-loop schedule, a subscriber placed first.
async fn pubsub_main(args: Args) -> Result<(), CompareError> {
    let payloads = payloads(&args)?;
    let mean = payloads.iter().map(Vec::len).sum::<usize>() as f64 / payloads.len().max(1) as f64;
    let warmup = Duration::from_secs(args.warmup);
    let total = warmup
        .checked_add(Duration::from_secs(args.seconds))
        .ok_or(CompareError::Argument("duration"))?;
    let endpoint = args
        .endpoints
        .split(',')
        .next()
        .ok_or(CompareError::Argument("endpoints"))?
        .to_owned();
    if args.system == System::FocalBus {
        let node = tokio::net::lookup_host(endpoint.as_str())
            .await
            .map_err(|error| CompareError::Target(error.to_string()))?
            .next()
            .ok_or(CompareError::Argument("the bus node's address"))?;
        let run = bus::Run {
            node,
            pki: args
                .pki
                .clone()
                .ok_or(CompareError::Argument("focal-bus needs --pki"))?,
            rate: args.rate,
            warmup,
            total,
            payloads,
        };
        let (tally, received, lag) = tokio::task::spawn_blocking(move || bus::run(run))
            .await
            .map_err(|error| CompareError::Target(error.to_string()))??;
        return report::Report::pubsub(&args, &tally, &received, &lag, mean).write(&args.out);
    }
    let run = pubsub::Run {
        system: args.system,
        endpoint,
        rate: args.rate,
        warmup,
        total,
        payloads,
        inflight: args.inflight,
        tls_ca: args.tls_ca.clone(),
    };
    let (tally, received, lag) = tokio::task::spawn_blocking(move || pubsub::run(run))
        .await
        .map_err(|error| CompareError::Target(error.to_string()))??;
    report::Report::pubsub(&args, &tally, &received, &lag, mean).write(&args.out)
}

/// A request's answer: its intended send, its latency from it, whether it is measured, and its
/// result.
type Answer = (Instant, Duration, bool, Result<(), String>);

/// What every record of a run is sent with.
struct Sender {
    target: target::Target,
    payload: Vec<u8>,
    deliver: bool,
    started: Instant,
}

impl Sender {
    /// One record, intended at `intended`: counted sent, stamped where a consumer takes it, its
    /// request owned by `flying` until it answers.
    fn send(
        &self,
        flying: &mut JoinSet<Answer>,
        tally: &mut Tally,
        intended: Instant,
        measured: bool,
    ) -> Result<(), CompareError> {
        tally.sent = tally.sent.saturating_add(1);
        let target = self.target.lane(tally.sent);
        let mut payload = self.payload.clone();
        let sequence = tally.sent;
        if self.deliver {
            let intended_ns =
                u64::try_from(intended.saturating_duration_since(self.started).as_nanos())
                    .map_err(|_| CompareError::Argument("schedule"))?;
            deliver::stamp(&mut payload, intended_ns, sequence)?;
        }
        flying.spawn(async move {
            let result = target.append(sequence, payload).await;
            (intended, intended.elapsed(), measured, result)
        });
        Ok(())
    }
}

/// What a run counts beside the histogram.
#[derive(Debug, Default)]
pub struct Tally {
    /// Messages the schedule still held when its time was over: the generator could not offer
    /// them (a rate past what it publishes).
    pub behind: u64,
    pub sent: u64,
    /// Records stored, the warm-up's included: what a consumer takes.
    pub produced: u64,
    pub refused: u64,
    pub failed: u64,
    pub clamped: u64,
    pub first_error: Option<String>,
    /// From the schedule's close to the last answer.
    pub drain: Duration,
    /// The times a publisher was held (its lane full), what it was held in all, and the longest
    /// one, nanoseconds.
    pub holds: u64,
    pub held_ns: u64,
    pub held_most_ns: u64,
    /// When the hold in progress began.
    pub held_since: Option<std::time::Instant>,
    /// focal's bus: records acknowledged replicated (a majority across the group's fates) and
    /// persisted (a majority's disks).
    pub replicated: u64,
    pub persisted: u64,
    /// Each second's acknowledgements, by their records' intended sends.
    pub timeline: timeline::Timeline,
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
