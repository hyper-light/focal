// Dependency-free bench: a plain `harness = false` binary, no criterion (the
// workspace's deny.toml forbids unmaintained/unvetted deps). It reports the
// durable-append latency and throughput so a regression is visible; a
// measurement tool, not a pass/fail test.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Performance bench for durable WAL append (`focal_log::Wal::append`) — the
//! fsync-bound path every committed log entry passes through (P-gate, R11 §5).
//! A single `append` is one durable batch (one fsync), so the interesting axes
//! are the per-append (per-fsync) latency and how batching amortizes it across
//! records. Counts are small because each append is a real disk sync; the
//! numbers are disk-dependent (this is a durability-latency measurement, not a
//! CPU microbench), so treat them as a same-host regression signal.
use focal_log::{LogicalLogId, Record, RecordKind, Wal, WalIdentity, WalOptions};
use std::time::Instant;

const LOG: LogicalLogId = LogicalLogId([9; 16]);

fn options() -> WalOptions {
    WalOptions::new(WalIdentity {
        cluster: [7; 16],
        node: 1,
        stream: 0,
    })
}

fn batch(index: u64, count: u64, payload: usize) -> Vec<Record> {
    (0..count)
        .map(|i| Record {
            log: LOG,
            kind: RecordKind::Entry,
            index: index + i,
            term: 1,
            payload: vec![0xABu8; payload],
        })
        .collect()
}

fn run(batch_n: u64, payload: usize, appends: u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), options()).unwrap();
    let mut index = 1u64;
    // Warm up the first segment and the writer's buffers.
    for _ in 0..5 {
        wal.append(&batch(index, batch_n, payload)).unwrap();
        index += batch_n;
    }
    let start = Instant::now();
    for _ in 0..appends {
        wal.append(&batch(index, batch_n, payload)).unwrap();
        index += batch_n;
    }
    let elapsed = start.elapsed();
    let records = appends * batch_n;
    let bytes = records * payload as u64;
    let per_append = elapsed.as_nanos() as f64 / appends as f64;
    let recs = records as f64 / elapsed.as_secs_f64();
    let mib_s = bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "append batch={batch_n:<4} payload={payload:<5}B  {per_append:11.0} ns/append  {recs:>11.0} rec/s  {mib_s:8.1} MiB/s  ({appends} appends)"
    );
}

fn main() {
    let appends: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    println!("focal-log durable append bench ({appends} appends/config)\n");
    for (batch_n, payload) in [(1u64, 64usize), (16, 64), (256, 64), (1, 4096), (16, 4096)] {
        run(batch_n, payload, appends);
    }
}
