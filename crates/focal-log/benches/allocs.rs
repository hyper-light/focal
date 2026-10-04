// Dependency-free allocation-count bench: a plain `harness = false` binary
// with a counting global allocator (focal-memory/benches/support/alloc_count.rs).
// It reports heap allocations, reallocations, bytes and peak growth per
// durable append, which are stable under machine load; no wall-clock.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Allocation counts for `Wal::append` (the same configurations as
//! benches/append.rs). The record batches are built before the gate opens, so
//! what is counted is the writer's own framing, buffering and syncing. Each
//! append is a real `F_FULLFSYNC`, so the counts are small in number.
#[path = "../../focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;

use alloc_count::Meter;
use focal_log::{LogicalLogId, Record, RecordKind, Wal, WalIdentity, WalOptions};

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

fn main() {
    alloc_count::configure(1, 1, 50_000);
    let appends: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    println!("focal-log durable append allocation counts ({appends} appends/config)\n");
    println!("{}", alloc_count::header());
    let mut phases = Vec::new();

    // Opening a fresh WAL: the fixed cost, once.
    {
        let dir = tempfile::tempdir().unwrap();
        alloc_count::reset_sites();
        let mut meter = Meter::start("Wal::open (fresh directory)");
        let before = meter.open();
        let wal = Wal::open(dir.path(), options()).unwrap();
        meter.close(before);
        drop(wal);
        phases.push(meter.finish());
        println!("\ntop sites: Wal::open");
        print!("{}", alloc_count::sites_report(6));
    }

    for (batch_n, payload) in [(1u64, 64usize), (16, 64), (256, 64), (1, 4096), (16, 4096)] {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), options()).unwrap();
        let mut index = 1u64;
        for _ in 0..3 {
            wal.append(&batch(index, batch_n, payload)).unwrap();
            index += batch_n;
        }
        // Every batch is built before the gate opens.
        let batches: Vec<Vec<Record>> = (0..appends)
            .map(|i| batch(index + i * batch_n, batch_n, payload))
            .collect();
        alloc_count::reset_sites();
        let mut meter = Meter::start(&format!("append batch={batch_n} payload={payload}B"));
        for records in &batches {
            let before = meter.open();
            let position = wal.append(records).unwrap();
            std::hint::black_box(&position);
            meter.close(before);
        }
        let phase = meter.finish();
        println!(
            "\n{} -> per record: {:.2} allocs, {:.3} reallocs, {:.1} bytes",
            phase.name,
            alloc_count::per_op(phase.counts.allocs, appends * batch_n),
            alloc_count::per_op(phase.counts.reallocs, appends * batch_n),
            alloc_count::per_op(
                phase.counts.bytes + phase.counts.realloc_bytes,
                appends * batch_n
            )
        );
        print!("{}", alloc_count::sites_report(8));
        phases.push(phase);
        drop(wal);
    }

    println!();
    println!("{}", alloc_count::header());
    for phase in &phases {
        println!("{}", alloc_count::row(phase));
    }
}
