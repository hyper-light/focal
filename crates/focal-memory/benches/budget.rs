// Dependency-free bench: a plain `harness = false` binary, no criterion (the
// workspace's deny.toml forbids unmaintained/unvetted deps). It reports ns/op
// and ops/s for the allocation-admission hot path so a regression is visible;
// it is a measurement tool, not a pass/fail test.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Performance bench for `focal_memory::MemoryBudget` — the admission path every
//! operation charges through before it does any work (P-gate, R11 §5). The
//! interesting costs are the uncontended atomic reserve/commit/release, the
//! per-child (tenant/node) budget creation those admissions nest under, and the
//! contended counters when workers reserve concurrently.
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use std::time::Instant;

fn bench(name: &str, iters: u64, mut op: impl FnMut()) {
    for _ in 0..(iters / 10).max(1) {
        op();
    }
    let start = Instant::now();
    for _ in 0..iters {
        op();
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_nanos() as f64 / iters as f64;
    let ops = iters as f64 / elapsed.as_secs_f64();
    println!("{name:34} {per:8.1} ns/op  {ops:>13.0} ops/s  ({iters} iters)");
}

fn main() {
    let iters: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);
    println!("focal-memory budget admission bench ({iters} iterations)\n");

    let budget = MemoryBudget::new(1 << 30, 1 << 20).unwrap();

    // The whole point of the admission path: reserve capacity, commit it, and
    // release on drop. One uncontended reserve/commit/release round trip.
    bench("reserve + commit + drop", iters, || {
        let alloc = budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap()
            .commit();
        std::hint::black_box(&alloc);
    });

    // A refused/abandoned admission: reserve then drop without committing must
    // return every byte, and is on the hot path of every capacity check.
    bench("reserve + drop (uncommitted)", iters, || {
        let reservation = budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap();
        std::hint::black_box(&reservation);
    });

    // A tenant/node child budget is created per session and threads its parent
    // limits; measure creation plus one nested admission through it.
    let parent = MemoryBudget::new(1 << 30, 1 << 20).unwrap();
    bench("child + nested reserve/commit", iters / 5, || {
        let child = parent.child(1 << 20, 1 << 16).unwrap();
        let alloc = child
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap()
            .commit();
        std::hint::black_box((&child, &alloc));
    });

    // Concurrent workers charging the same envelope: the cost the atomic
    // counters pay under contention (the reason admission is lock-free), on
    // one level, through a node/tenant/session chain of three, and with each
    // permit dropped by another thread than the one that reserved it, as a
    // worker or response task releases what its owner admitted. Each case runs
    // `FOCAL_BENCH_ROUNDS` times (default 5): on a loaded machine the spread
    // is the noise, and the minimum is the closest to the uncontended cost.
    let rounds: usize = std::env::var("FOCAL_BENCH_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    for threads in [1usize, 2, 4, 8] {
        let per_thread = (iters / 4) / threads as u64;
        let root = MemoryBudget::new(1 << 30, 1 << 20).unwrap();
        contended("one level", threads, per_thread, rounds, &root);
        let node = root.child(1 << 30, 1 << 20).unwrap();
        let tenant = node.child(1 << 29, 1 << 19).unwrap();
        let session = tenant.child(1 << 28, 1 << 18).unwrap();
        contended("three levels", threads, per_thread, rounds, &session);
        handed_off(threads, per_thread, rounds, &session);
        // A working budget is seldom empty: the same with a standing charge on
        // every level, so no release takes one back to nothing.
        let standing = session
            .reserve(BudgetKind::Control, BudgetLane::Completion, 1 << 16)
            .unwrap()
            .commit();
        contended("one, charged", threads, per_thread, rounds, &root);
        contended("three, charged", threads, per_thread, rounds, &session);
        drop(standing);
    }
}

fn report(name: &str, threads: usize, total: u64, mut per: Vec<f64>) {
    per.sort_by(f64::total_cmp);
    let min = per.first().copied().unwrap_or(0.0);
    let median = per.get(per.len() / 2).copied().unwrap_or(0.0);
    let max = per.last().copied().unwrap_or(0.0);
    println!(
        "{name:13} x{threads:<2} threads  min {min:7.1}  median {median:7.1}  max {max:7.1} ns/op  ({total} ops a round, {} rounds)",
        per.len()
    );
}

/// `threads` workers reserve, commit and drop on `budget`, borrowed by each.
fn contended(name: &str, threads: usize, per_thread: u64, rounds: usize, budget: &MemoryBudget) {
    let total = per_thread * threads as u64;
    let per = (0..rounds)
        .map(|_| {
            let start = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    scope.spawn(|| {
                        for _ in 0..per_thread {
                            let alloc = budget
                                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
                                .unwrap()
                                .commit();
                            std::hint::black_box(&alloc);
                        }
                    });
                }
            });
            start.elapsed().as_nanos() as f64 / total as f64
        })
        .collect();
    report(name, threads, total, per);
}

/// `threads` pairs: one reserves and commits on `budget` and hands each
/// permit over a bounded channel to the other, which drops it.
fn handed_off(threads: usize, per_thread: u64, rounds: usize, budget: &MemoryBudget) {
    let total = per_thread * threads as u64;
    let per = (0..rounds)
        .map(|_| {
            let start = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    let (send, receive) = std::sync::mpsc::sync_channel(64);
                    scope.spawn(move || {
                        for alloc in receive {
                            drop(std::hint::black_box(alloc));
                        }
                    });
                    scope.spawn(move || {
                        for _ in 0..per_thread {
                            let alloc = budget
                                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
                                .unwrap()
                                .commit();
                            send.send(alloc).unwrap();
                        }
                    });
                }
            });
            start.elapsed().as_nanos() as f64 / total as f64
        })
        .collect();
    report("handed off", threads, total, per);
}
