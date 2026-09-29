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
    // counters pay under contention (the reason admission is lock-free).
    for threads in [2usize, 4, 8] {
        let shared = MemoryBudget::new(1 << 30, 1 << 20).unwrap();
        let per_thread = (iters / 4) / threads as u64;
        let start = Instant::now();
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let budget = shared.clone();
                scope.spawn(move || {
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
        let elapsed = start.elapsed();
        let total = per_thread * threads as u64;
        let ops = total as f64 / elapsed.as_secs_f64();
        println!(
            "reserve/commit x{threads:<2} threads          {:8.1} ns/op  {ops:>13.0} ops/s  ({total} iters)",
            elapsed.as_nanos() as f64 / total as f64
        );
    }
}
