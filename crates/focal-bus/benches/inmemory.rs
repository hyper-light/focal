// A measurement printed as a table, not a criterion run: `cargo bench -p focal-bus --bench inmemory`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_macros,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::indexing_slicing
)]
//! Step 1 of the event bus: the in-memory core, on the payloads step 0 measured (each shape's
//! messages compressed by zstd 3 with a 64 KiB dictionary trained on another seed).
//!
//! **Quiet.** One thread appends each batch to a part, reads it back as a delivery would, and
//! acknowledges it once `in_flight` later batches have followed, which lets its stream's floor up:
//! the steady state of a part whose consumers keep up. Measured per message, over passes after a
//! warm-up pass: time, allocations and page faults, and the memory the part holds (segments in use
//! and warm, index blocks) against the payload bytes it holds.
//!
//! **Contended.** Producer threads build batches and hand them to one owner thread over hyper-rt's
//! bounded channel (the ring the shards use, `sync::channel`); the owner drains what has arrived,
//! appends and reads back each, and acknowledges as above. Closed loop for the throughput one owner
//! sustains; open loop at fixed rates (Poisson arrivals, hyper-measure's schedule) for the time a
//! batch takes from its producer's hand to its append, with each quantile's interval.
//!
//! `FOCAL_BENCH_MESSAGES` (default 100,000) sets the messages a pass, `FOCAL_BENCH_ROUNDS` (default
//! 5) the timed passes, `FOCAL_BENCH_RATE_SECONDS` (default 2) each open-loop run's length.

use std::collections::VecDeque;
use std::hint::black_box;
use std::time::{Duration, Instant};

use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::corpus::{Corpus, Shape};
use focal_bus::log::SegmentPool;
use focal_bus::part::{Part, PartLimits};
use focal_memory::MemoryBudget;
use hyper_measure::alloc::{self, Counting};
use hyper_measure::quantile::{Level, cell};
use hyper_measure::stats::Summary;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const TRAIN_SEED: u64 = 0x5EED_0001;
const TEST_SEED: u64 = 0x5EED_0002;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `count` messages of `shape`, compressed as a claimant or respondent sends them.
fn payloads(shape: Shape, count: usize) -> Vec<Vec<u8>> {
    let mut text = String::new();
    let mut train = Corpus::new(TRAIN_SEED);
    let samples: Vec<Vec<u8>> = (0..2_000)
        .map(|_| {
            train.next(shape, &mut text).unwrap();
            text.as_bytes().to_vec()
        })
        .collect();
    let dictionary = zstd::dict::from_samples(&samples, 64 * 1024).unwrap();
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &dictionary).unwrap();
    compressor
        .set_parameter(zstd::zstd_safe::CParameter::DictIdFlag(false))
        .unwrap();
    let mut test = Corpus::new(TEST_SEED);
    (0..count)
        .map(|_| {
            test.next(shape, &mut text).unwrap();
            compressor.compress(text.as_bytes()).unwrap()
        })
        .collect()
}

/// A stream for message `at` of `streams`: Zipf-weighted (rank `r` weighs `1/r`) over many
/// streams, as agents' inboxes are, or every message to the one stream.
struct Streams {
    cumulative: Vec<f64>,
    state: u64,
}

impl Streams {
    fn new(streams: u64) -> Self {
        let mut total = 0.0;
        let cumulative = (1..=streams)
            .map(|rank| {
                total += 1.0 / rank as f64;
                total
            })
            .collect();
        Self {
            cumulative,
            state: 0x9E37_79B9,
        }
    }

    fn next(&mut self) -> u64 {
        // SplitMix64, as the corpus draws.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let total = *self.cumulative.last().unwrap();
        let draw = (z >> 11) as f64 / (1_u64 << 53) as f64 * total;
        self.cumulative.partition_point(|&edge| edge < draw) as u64
    }
}

fn batch(builder: &mut BatchBuilder, stream: u64, producer: u64, sequence: u32, payload: &[u8]) {
    builder.begin(&Producing {
        stream,
        producer,
        sequence,
        flags: 1,
        timestamp: 1_792_022_400_000_000 + u64::from(sequence),
    });
    builder.push(1_792_022_400_000_000, payload).unwrap();
    builder.finish().unwrap();
}

struct Owner {
    pool: SegmentPool,
    part: Part,
    /// Delivered and not yet acknowledged: (stream, the offset after the batch).
    in_flight: VecDeque<(u64, u64)>,
    window: usize,
    delivered_bytes: u64,
}

impl Owner {
    fn new(segment: usize, streams: usize, window: usize) -> Self {
        let budget = MemoryBudget::new(1 << 33, 0).unwrap();
        let mut in_flight = VecDeque::new();
        in_flight.reserve_exact(window + 1);
        Self {
            pool: SegmentPool::new(segment, budget.clone()).unwrap(),
            part: Part::new(
                PartLimits {
                    streams,
                    batch_bytes: segment / 8,
                    producers: streams,
                },
                budget,
            ),
            in_flight,
            window,
            delivered_bytes: 0,
        }
    }

    /// Appends one batch, delivers it, and acknowledges the oldest past the window.
    fn take(&mut self, bytes: &[u8]) {
        let appended = self.part.append(&mut self.pool, bytes).unwrap();
        // A live tail delivers the batch where it was stored, as the chain leads it there.
        let delivered = self.part.batch(appended.position).unwrap();
        self.delivered_bytes += delivered.bytes().len() as u64;
        black_box(delivered.bytes());
        self.in_flight
            .push_back((appended.stream, appended.base + u64::from(appended.count)));
        if self.in_flight.len() > self.window {
            let (stream, end) = self.in_flight.pop_front().unwrap();
            self.part
                .release_below(&mut self.pool, stream, end, None)
                .unwrap();
        }
    }

    /// Bytes held: segments in use and warm, and the stream table.
    fn held_bytes(&self) -> usize {
        self.pool.held() * self.pool.segment_size() + self.part.stream_table().bytes()
    }
}

struct QuietRow {
    ns: Summary,
    allocs: f64,
    faults: f64,
    held_per_message: f64,
    payload_per_message: f64,
    churn: (u64, u64),
}

fn quiet(
    payloads: &[Vec<u8>],
    streams: u64,
    window: usize,
    segment: usize,
    messages: usize,
    rounds: usize,
) -> QuietRow {
    let mut owner = Owner::new(segment, streams as usize, window);
    let mut builder = BatchBuilder::new();
    let mut pick = Streams::new(streams);
    let batches: Vec<Vec<u8>> = (0..messages)
        .map(|at| {
            let payload = &payloads[at % payloads.len()];
            batch(&mut builder, pick.next(), 1, at as u32, payload);
            builder.finish().unwrap().to_vec()
        })
        .collect();
    // Warm-up: every stream made, the window full, the pool at its steady size.
    for bytes in &batches {
        owner.take(bytes);
    }
    let faults_before = hyper_measure::faults::read().map(|f| f.minor).unwrap_or(0);
    alloc::begin();
    for bytes in &batches {
        owner.take(bytes);
    }
    let counts = alloc::end();
    let faults = hyper_measure::faults::read()
        .map(|f| f.minor)
        .unwrap_or(0)
        .saturating_sub(faults_before);
    let mut ns = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = Instant::now();
        for bytes in &batches {
            owner.take(black_box(bytes));
        }
        ns.push(started.elapsed().as_nanos() as f64 / messages as f64);
    }
    let held = owner.held_bytes() as f64;
    let held_messages = owner.in_flight.len().max(1) as f64;
    let payload: usize = batches[batches.len() - window.min(batches.len())..]
        .iter()
        .map(Vec::len)
        .sum();
    QuietRow {
        ns: Summary::of(&ns).unwrap(),
        allocs: counts.calls() as f64 / messages as f64,
        faults: faults as f64 / messages as f64,
        held_per_message: held / held_messages,
        payload_per_message: payload as f64 / window.min(batches.len()).max(1) as f64,
        churn: owner.pool.churn(),
    }
}

/// Closed loop: `producers` threads send `messages` batches each as fast as the channel takes
/// them; the owner's throughput and its share of busy time per message.
fn saturate(payloads: &[Vec<u8>], producers: usize, messages: usize, streams: u64) -> (f64, f64) {
    let (sender, mut receiver) = hyper_rt::sync::channel::<Vec<u8>>(4_096).unwrap();
    let mut owner = Owner::new(1 << 20, streams as usize, 10_000);
    let total = producers * messages;
    let started = Instant::now();
    let mut busy = Duration::ZERO;
    std::thread::scope(|scope| {
        for producer in 0..producers {
            let sender = sender.clone();
            scope.spawn(move || {
                let mut builder = BatchBuilder::new();
                let mut pick = Streams::new(streams);
                for at in 0..messages {
                    let payload = &payloads[(at + producer) % payloads.len()];
                    batch(
                        &mut builder,
                        pick.next(),
                        producer as u64,
                        at as u32,
                        payload,
                    );
                    sender
                        .blocking_send(builder.finish().unwrap().to_vec())
                        .unwrap();
                }
            });
        }
        drop(sender);
        let mut taken = 0;
        while taken < total {
            let first = receiver.blocking_recv().unwrap();
            let work = Instant::now();
            owner.take(&first);
            taken += 1;
            while let Ok(Some(bytes)) = receiver.try_recv() {
                owner.take(&bytes);
                taken += 1;
            }
            busy += work.elapsed();
        }
    });
    let elapsed = started.elapsed().as_secs_f64();
    (
        total as f64 / elapsed,
        busy.as_nanos() as f64 / total as f64,
    )
}

/// Open loop: one producer sends at `rate` a second, Poisson arrivals, for `seconds`; the time
/// from each batch's scheduled send to its append, sorted.
fn open_loop(payloads: &[Vec<u8>], rate: u64, seconds: u64, streams: u64) -> (Vec<u64>, f64) {
    let (sender, mut receiver) = hyper_rt::sync::channel::<(u64, Vec<u8>)>(65_536).unwrap();
    let mut owner = Owner::new(1 << 20, streams as usize, 10_000);
    let count = (rate * seconds) as usize;
    let epoch = Instant::now();
    let mut latencies = Vec::with_capacity(count);
    let mut busy = Duration::ZERO;
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut schedule =
                hyper_measure::open_loop::Arrivals::poisson(rate as f64, 0x0000_FEED_5EED, 0)
                    .unwrap();
            let mut builder = BatchBuilder::new();
            let mut pick = Streams::new(streams);
            for at in 0..count {
                let due_ns = schedule.next_arrival().unwrap();
                let payload = &payloads[at % payloads.len()];
                batch(&mut builder, pick.next(), 1, at as u32, payload);
                let bytes = builder.finish().unwrap().to_vec();
                // Wait for the arrival's time: a short spin, as the schedule is microseconds apart.
                while (epoch.elapsed().as_nanos() as u64) < due_ns {
                    std::hint::spin_loop();
                }
                sender.blocking_send((due_ns, bytes)).unwrap();
            }
        });
        let mut taken = 0;
        while taken < count {
            let (due, first) = receiver.blocking_recv().unwrap();
            let work = Instant::now();
            owner.take(&first);
            latencies.push(epoch.elapsed().as_nanos() as u64 - due);
            taken += 1;
            while let Ok(Some((due, bytes))) = receiver.try_recv() {
                owner.take(&bytes);
                latencies.push(epoch.elapsed().as_nanos() as u64 - due);
                taken += 1;
            }
            busy += work.elapsed();
        }
    });
    latencies.sort_unstable();
    (latencies, busy.as_nanos() as f64 / count as f64)
}

fn us(ns: u64) -> String {
    format!("{:.1}", ns as f64 / 1_000.0)
}

fn main() {
    let messages = env("FOCAL_BENCH_MESSAGES", 100_000);
    let rounds = env("FOCAL_BENCH_ROUNDS", 5).max(1);
    let rate_seconds = env("FOCAL_BENCH_RATE_SECONDS", 2) as u64;
    println!(
        "focal-bus step 1: the in-memory core ({messages} messages a pass, {rounds} timed passes; host load {:.1})",
        hyper_measure::usage::load().unwrap_or(f64::NAN)
    );
    println!();
    println!(
        "Quiet: append, deliver, acknowledge once 10,000 later batches followed (ns a message, median [least–most])"
    );
    println!(
        "   {:<10} {:>8} {:>9} {:>22} {:>8} {:>8} {:>11} {:>11} {:>14}",
        "shape",
        "streams",
        "segment",
        "ns",
        "allocs",
        "faults",
        "payload B",
        "held B",
        "segments +/-"
    );
    let mut shapes = Vec::new();
    for shape in Shape::ALL {
        shapes.push((shape, payloads(shape, 4_000)));
    }
    for (shape, payloads) in &shapes {
        for (streams, segment) in [
            (1_u64, 1 << 20),
            (10_000, 1 << 20),
            (10_000, 64 << 10),
            (1_000_000, 1 << 20),
        ] {
            let row = quiet(payloads, streams, 10_000, segment, messages, rounds);
            println!(
                "   {:<10} {:>8} {:>8}K {:>22} {:>8.3} {:>8.3} {:>11.0} {:>11.0} {:>14}",
                shape.label(),
                streams,
                segment >> 10,
                format!("{:.0} [{:.0}–{:.0}]", row.ns.median, row.ns.min, row.ns.max),
                row.allocs,
                row.faults,
                row.payload_per_message,
                row.held_per_message,
                format!("{}/{}", row.churn.0, row.churn.1)
            );
        }
    }
    println!();
    println!(
        "Contended, closed loop: producers hand batches to one owner (claims, 10,000 streams)"
    );
    let claims = &shapes[1].1;
    for producers in [1, 2, 4] {
        let (rate, busy) = saturate(claims, producers, messages, 10_000);
        println!(
            "   {producers} producer(s): {:>10.0} messages/s, owner busy {:.0} ns a message",
            rate, busy
        );
    }
    println!();
    println!(
        "Open loop: one producer at a fixed rate, Poisson arrivals, {rate_seconds} s; send to append, µs: estimate [99% interval]"
    );
    for rate in [1_000, 10_000, 100_000, 400_000] {
        let (sorted, busy) = open_loop(claims, rate, rate_seconds, 10_000);
        let coverage = Level::new(99, 100).unwrap();
        println!(
            "   {:>7}/s  p50 {:<28} p99 {:<28} p99.9 {:<28} max {:>9}  owner {:.0} ns a message",
            rate,
            cell(&sorted, Level::P50, coverage, us),
            cell(&sorted, Level::P99, coverage, us),
            cell(&sorted, Level::P999, coverage, us),
            us(*sorted.last().unwrap()),
            busy
        );
    }
}
