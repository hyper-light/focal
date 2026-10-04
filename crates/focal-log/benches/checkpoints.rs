// Dependency-free bench: a plain `harness = false` binary, like `append`.
// A measurement tool, not a pass/fail test.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing
)]
//! What a group's checkpoint costs the shared log (the audit's F14): many
//! groups share one writer; most are cold — written once, never again — and
//! a few are hot, appending and checkpointing at a cadence. Reported: the
//! bytes the checkpoints and the cleaning they cause write, against the
//! bytes freed; the most disk a checkpoint needed beyond what the log held
//! before it; what the log holds against what is live; and the latency of
//! the appends and of the checkpoints, and of one more group's appends
//! issued from another thread all the while — what an unrelated group waits
//! when its neighbours checkpoint. Every append and checkpoint is a real
//! disk sync, so the latencies are the disk's: a same-host signal.
use focal_log::{LogicalLogId, Record, RecordKind, SharedWal, WalIdentity, WalOptions};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

const COLD: u8 = 56;
const HOT: u8 = 8;
const COLD_RECORDS: u64 = 256;
const CADENCE: u64 = 256;
const BATCH: u64 = 16;
const ROUNDS: u64 = 16;
const PAYLOAD: usize = 256;
const SNAPSHOT: usize = 4096;

fn options() -> WalOptions {
    WalOptions {
        segment_bytes: 1 << 20,
        max_record_bytes: 64 << 10,
        max_batch_bytes: 1 << 20,
        ..WalOptions::new(WalIdentity {
            cluster: [7; 16],
            node: 1,
            stream: 0,
        })
    }
}
fn entry(log: u8, index: u64) -> Record {
    Record {
        log: LogicalLogId([log; 16]),
        kind: RecordKind::Entry,
        index,
        term: 1,
        payload: vec![log; PAYLOAD],
    }
}
/// The bytes of the segment files on disk. The writer removes segments
/// while this reads the directory: one gone between the two holds nothing.
fn held(directory: &Path) -> u64 {
    std::fs::read_dir(directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".seg"))
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}
fn percentile(sorted: &[u128], of: f64) -> f64 {
    let at = ((sorted.len() as f64 - 1.0) * of).round() as usize;
    sorted[at] as f64 / 1e6
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut cold: Vec<_> = (1..=COLD)
        .map(|log| shared.lease(LogicalLogId([log; 16])).unwrap())
        .collect();
    let mut hot: Vec<_> = (COLD + 1..=COLD + HOT)
        .map(|log| shared.lease(LogicalLogId([log; 16])).unwrap())
        .collect();
    // The old log: every cold group's history, interleaved batch by batch.
    for start in (1..=COLD_RECORDS).step_by(BATCH as usize) {
        for (at, lease) in cold.iter_mut().enumerate() {
            let log = at as u8 + 1;
            let batch: Vec<Record> = (start..start + BATCH)
                .map(|index| entry(log, index))
                .collect();
            lease.append(&batch).unwrap();
        }
    }
    let old = held(dir.path());
    let before = shared.stats().unwrap();
    let mut appends = Vec::new();
    let mut checkpoints = Vec::new();
    let mut most = old;
    let mut most_extra = 0u64;
    let mut foreground = 0u64;
    // An unrelated group, appending from its own thread the whole time; the
    // scope lends it the flag that ends it.
    let stop = AtomicBool::new(false);
    let mut neighbour_lease = shared.lease(LogicalLogId([COLD + HOT + 1; 16])).unwrap();
    let (elapsed, neighbours) = std::thread::scope(|scope| {
        let neighbour = scope.spawn(|| {
            let mut waits = Vec::new();
            let mut index = 0;
            while !stop.load(Ordering::Acquire) {
                index += 1;
                let begun = Instant::now();
                neighbour_lease
                    .append(&[entry(COLD + HOT + 1, index)])
                    .unwrap();
                waits.push(begun.elapsed().as_nanos());
            }
            waits
        });
        let started = Instant::now();
        for round in 0..ROUNDS {
            for start in (1..=CADENCE).step_by(BATCH as usize) {
                for (at, lease) in hot.iter_mut().enumerate() {
                    let log = COLD + 1 + at as u8;
                    let batch: Vec<Record> = (start..start + BATCH)
                        .map(|index| entry(log, round * CADENCE + index))
                        .collect();
                    let begun = Instant::now();
                    lease.append(&batch).unwrap();
                    appends.push(begun.elapsed().as_nanos());
                    foreground += BATCH * (PAYLOAD as u64 + 40);
                }
                most = most.max(held(dir.path()));
            }
            for (at, lease) in hot.iter_mut().enumerate() {
                let log = COLD + 1 + at as u8;
                let snapshot = Record {
                    kind: RecordKind::Snapshot,
                    payload: vec![log; SNAPSHOT],
                    ..entry(log, (round + 1) * CADENCE)
                };
                let had = held(dir.path());
                let begun = Instant::now();
                lease.rewrite_checkpoint(&[snapshot]).unwrap();
                checkpoints.push(begun.elapsed().as_nanos());
                let has = held(dir.path());
                most = most.max(has);
                most_extra = most_extra.max(has.saturating_sub(had));
            }
        }
        let elapsed = started.elapsed();
        stop.store(true, Ordering::Release);
        let mut neighbours = neighbour.join().unwrap();
        neighbours.sort_unstable();
        (elapsed, neighbours)
    });
    let after = shared.stats().unwrap();
    appends.sort_unstable();
    checkpoints.sort_unstable();
    let written = (after.checkpoint_bytes - before.checkpoint_bytes)
        + (after.relocated_bytes - before.relocated_bytes);
    let freed = after.reclaimed_bytes - before.reclaimed_bytes;
    println!(
        "groups {} ({} hot), old log {:.1} MiB, {} checkpoints over {:.1} s",
        COLD + HOT,
        HOT,
        old as f64 / (1 << 20) as f64,
        checkpoints.len(),
        elapsed.as_secs_f64()
    );
    println!(
        "foreground appends        {:>10.1} MiB",
        foreground as f64 / (1 << 20) as f64
    );
    println!(
        "checkpoints wrote         {:>10.1} MiB (their records and floors {:.1}, frames written again {:.1})",
        written as f64 / (1 << 20) as f64,
        (after.checkpoint_bytes - before.checkpoint_bytes) as f64 / (1 << 20) as f64,
        (after.relocated_bytes - before.relocated_bytes) as f64 / (1 << 20) as f64
    );
    println!(
        "freed                     {:>10.1} MiB ({:.3} bytes written a byte freed)",
        freed as f64 / (1 << 20) as f64,
        written as f64 / freed.max(1) as f64
    );
    println!(
        "most disk held            {:>10.1} MiB; most a checkpoint added {:.3} MiB",
        most as f64 / (1 << 20) as f64,
        most_extra as f64 / (1 << 20) as f64
    );
    println!(
        "at the end: on disk {:.1} MiB, frames {:.1} MiB, live {:.1} MiB",
        held(dir.path()) as f64 / (1 << 20) as f64,
        after.physical_bytes as f64 / (1 << 20) as f64,
        after.live_bytes as f64 / (1 << 20) as f64
    );
    println!(
        "append ms      p50 {:>8.3}  p99 {:>8.3}  max {:>8.3}",
        percentile(&appends, 0.5),
        percentile(&appends, 0.99),
        percentile(&appends, 1.0)
    );
    println!(
        "checkpoint ms  p50 {:>8.3}  p99 {:>8.3}  max {:>8.3}",
        percentile(&checkpoints, 0.5),
        percentile(&checkpoints, 0.99),
        percentile(&checkpoints, 1.0)
    );
    println!(
        "neighbour ms   p50 {:>8.3}  p99 {:>8.3}  max {:>8.3}  ({} appends)",
        percentile(&neighbours, 0.5),
        percentile(&neighbours, 0.99),
        percentile(&neighbours, 1.0),
        neighbours.len()
    );
}
