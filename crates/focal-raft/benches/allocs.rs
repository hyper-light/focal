// Dependency-free allocation-count bench: a plain `harness = false` binary
// with a counting global allocator (focal-memory/benches/support/alloc_count.rs).
// It reports heap allocations, reallocations, bytes and peak growth per
// committed entry, which are stable under machine load; no wall-clock.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Allocation counts for one replication cycle on the test harness that the
//! replicate bench drives (benches/replicate.rs): proposals replicated to
//! every member and committed, storage in memory, a lossless network. The
//! harness itself clones every message and committed entry it records, so the
//! site table separates the core's frames (`focal_raft::` / raft-rs `raft::`)
//! from the harness's (`support::`), and both shares are printed.
#[path = "../../focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;
#[path = "../tests/support/mod.rs"]
mod support;

use alloc_count::Meter;
use support::{Cluster, New, Old, Op, Replica, Settings};

/// benches/replicate.rs delivers newest-first (`at: len - 1`), which reorders
/// a follower's messages: it sees the commit-only append before the one
/// carrying the entry, rejects it, and the leader re-probes and re-sends from
/// storage. `FOCAL_RAFT_FIFO=1` delivers oldest-first instead, the order an
/// in-order transport gives, so both schedules can be counted.
fn fifo() -> bool {
    std::env::var("FOCAL_RAFT_FIFO").is_ok_and(|value| value == "1")
}

fn quiet<R: Replica>(group: &mut Cluster<R>) {
    let fifo = fifo();
    while !group.net.is_empty() {
        group.act(&Op::Deliver {
            at: if fifo { 0 } else { group.net.len() - 1 },
            keep: false,
            lose: false,
        });
    }
}

/// Counts for each entry committed by every member.
fn replicate<R: Replica>(
    label: &str,
    members: u64,
    batch: usize,
    bytes: usize,
    rounds: usize,
) -> alloc_count::Phase {
    let voters: Vec<u64> = (1..=members).collect();
    let mut group: Cluster<R> = Cluster::new(members, &voters, Settings::shell(), 1);
    group.act(&Op::Campaign(1));
    quiet(&mut group);
    assert_eq!(group.leaders_now(), vec![1]);
    // The proposals are built before the gate opens: the harness's clone of
    // the payload inside `act` is still counted, as it is the proposal's copy.
    let proposals: Vec<Op> = (0..batch)
        .map(|_| Op::Propose(1, vec![0xa5u8; bytes]))
        .collect();
    let run = |group: &mut Cluster<R>, rounds: usize| {
        for _ in 0..rounds {
            for proposal in &proposals {
                group.act(proposal);
            }
            quiet(group);
        }
    };
    run(&mut group, rounds / 10 + 1);
    group.chosen.clear();
    let before_count = group.peek(members).unwrap().app().count;
    alloc_count::reset_sites();
    let mut meter = Meter::start(label);
    let before = meter.open();
    run(&mut group, rounds);
    let committed = group.peek(members).unwrap().app().count - before_count;
    assert_eq!(committed as usize, rounds * batch);
    meter.close_many(before, committed);
    let phase = meter.finish();
    // A site belongs to whichever of these the innermost matching frame names:
    // the core (this crate, or raft-rs for `Old`) or the harness.
    print!(
        "\n{label}:\n{}",
        alloc_count::shares_report(
            &phase,
            &["core", "harness", "other"],
            &[
                &[
                    "crates/focal-raft/src/",
                    "focal_raft::",
                    "raft-rs-",
                    "raft::raft",
                    "raft::raw_node",
                ],
                &["tests/support/", "benches/allocs.rs", "support::"],
            ],
            2,
        )
    );
    print!("{}", alloc_count::sites_report(10));
    phase
}

fn main() {
    alloc_count::configure(17, 1, 60_000);
    println!(
        "focal-raft replication allocation counts (per entry committed by every member; delivery {})\n",
        if fifo() {
            "oldest-first"
        } else {
            "newest-first, as benches/replicate.rs"
        }
    );
    println!("{}", alloc_count::header());
    let mut phases = Vec::new();
    for (members, batch, bytes, rounds) in [
        (3u64, 1usize, 64usize, 2_000usize),
        (3, 16, 64, 200),
        (3, 1, 4096, 1_000),
        (5, 1, 64, 1_000),
        (5, 16, 1024, 100),
    ] {
        phases.push(replicate::<Old>(
            &format!("raft-rs {members}m x{batch} {bytes}B"),
            members,
            batch,
            bytes,
            rounds,
        ));
        phases.push(replicate::<New>(
            &format!("focal-raft {members}m x{batch} {bytes}B"),
            members,
            batch,
            bytes,
            rounds,
        ));
    }
    println!();
    println!("{}", alloc_count::header());
    for phase in &phases {
        println!("{}", alloc_count::row(phase));
    }
}
