// Dependency-free allocation-count bench: a plain `harness = false` binary
// with a counting global allocator (benches/support/alloc_count.rs). It
// reports heap allocations, reallocations, bytes and peak growth per
// operation, which are stable under machine load; it reports no wall-clock.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Allocation counts for the admission path: `MemoryBudget::reserve` /
//! `commit` / drop must be zero-alloc (the budget is atomic counters behind
//! one `Arc`; a permit is a plain struct), a child budget is one `Arc`, and an
//! `Arena` page is one boxed slice plus its directory.
#[path = "support/alloc_count.rs"]
mod alloc_count;

use alloc_count::Meter;
use focal_memory::{Arena, ArenaConfig, ArenaId, BudgetKind, BudgetLane, MemoryBudget};

fn main() {
    alloc_count::configure(97, 1, 50_000);
    let iters: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    println!("focal-memory allocation counts ({iters} iterations)\n");
    println!("{}", alloc_count::header());
    let mut phases = Vec::new();

    let budget = MemoryBudget::new(1 << 30, 1 << 20).unwrap();
    let mut meter = Meter::start("budget reserve+commit+drop 4 KiB");
    for _ in 0..iters {
        let before = meter.open();
        let alloc = budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap()
            .commit();
        std::hint::black_box(&alloc);
        drop(alloc);
        meter.close(before);
    }
    phases.push(meter.finish());

    let mut meter = Meter::start("budget reserve+drop (uncommitted)");
    for _ in 0..iters {
        let before = meter.open();
        let reservation = budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap();
        std::hint::black_box(&reservation);
        drop(reservation);
        meter.close(before);
    }
    phases.push(meter.finish());

    let small = MemoryBudget::new(4096, 0).unwrap();
    let mut meter = Meter::start("budget refusal (over limit)");
    for _ in 0..iters {
        let before = meter.open();
        let refused = small.reserve(BudgetKind::Payload, BudgetLane::Ordinary, 8192);
        std::hint::black_box(&refused);
        drop(refused);
        meter.close(before);
    }
    phases.push(meter.finish());

    let parent = MemoryBudget::new(1 << 30, 1 << 20).unwrap();
    let mut meter = Meter::start("child + nested reserve/commit");
    for _ in 0..iters / 10 {
        let before = meter.open();
        let child = parent.child(1 << 20, 1 << 16).unwrap();
        let alloc = child
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap()
            .commit();
        std::hint::black_box((&child, &alloc));
        drop(alloc);
        drop(child);
        meter.close(before);
    }
    phases.push(meter.finish());

    let mut meter = Meter::start("funded_child + reserve/commit");
    for _ in 0..iters / 10 {
        let before = meter.open();
        let child = parent.funded_child(BudgetLane::Ordinary, 1 << 20).unwrap();
        let alloc = child
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 4096)
            .unwrap()
            .commit();
        std::hint::black_box((&child, &alloc));
        drop(alloc);
        drop(child);
        meter.close(before);
    }
    phases.push(meter.finish());

    // Arena insert: a page of 128 slots is one boxed slice, and the page
    // directory is rebuilt (one Vec) per page — 2 allocations per 128 inserts.
    let arena_budget = MemoryBudget::new(1 << 30, 0).unwrap();
    let mut arena: Arena<u64> =
        Arena::new(ArenaId(1), ArenaConfig::default(), arena_budget).unwrap();
    let mut meter = Meter::start("arena insert u64 (page_slots 128)");
    for i in 0..iters {
        let before = meter.open();
        let handle = arena.insert(i, 0, BudgetLane::Ordinary).unwrap();
        std::hint::black_box(&handle);
        meter.close(before);
    }
    phases.push(meter.finish());

    for phase in &phases {
        println!("{}", alloc_count::row(phase));
    }
    println!("\ntop sites (all phases):");
    print!("{}", alloc_count::sites_report(8));
}
