// Dependency-free bench: a plain `harness = false` binary, no criterion (the
// workspace's deny.toml forbids unmaintained/unvetted deps). A measurement
// tool, not a pass/fail test.
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
//! What the core costs a group: proposals replicated to every member and
//! committed, with storage in memory and a network that loses nothing, so
//! that what is measured is the core and the copies it makes. `raft-rs` runs
//! the same schedule, driven the same way.
#[path = "../tests/support/mod.rs"]
mod support;

use std::time::Instant;

use support::{Cluster, New, Old, Op, Replica, Settings};

fn quiet<R: Replica>(group: &mut Cluster<R>) {
    while !group.net.is_empty() {
        group.act(&Op::Deliver {
            at: group.net.len() - 1,
            keep: false,
            lose: false,
        });
    }
}
/// Nanoseconds for each entry committed by every member.
fn replicate<R: Replica>(members: u64, batch: usize, bytes: usize, rounds: usize) -> f64 {
    let voters: Vec<u64> = (1..=members).collect();
    let mut group: Cluster<R> = Cluster::new(members, &voters, Settings::shell(), 1);
    group.act(&Op::Campaign(1));
    quiet(&mut group);
    assert_eq!(group.leaders_now(), vec![1]);
    let payload = vec![0xa5u8; bytes];
    let run = |group: &mut Cluster<R>, rounds: usize| {
        for _ in 0..rounds {
            for _ in 0..batch {
                group.act(&Op::Propose(1, payload.clone()));
            }
            quiet(group);
        }
    };
    run(&mut group, rounds / 10 + 1);
    // What is kept for the comparison of what was committed is no part of
    // the core.
    group.chosen.clear();
    let before = group.peek(members).unwrap().app().count;
    let started = Instant::now();
    run(&mut group, rounds);
    let elapsed = started.elapsed();
    let committed = group.peek(members).unwrap().app().count - before;
    assert_eq!(committed as usize, rounds * batch);
    elapsed.as_nanos() as f64 / committed as f64
}

fn main() {
    println!(
        "{:<44} {:>14} {:>14} {:>8}",
        "entries committed by every member", "raft-rs ns", "focal-raft ns", "ratio"
    );
    for (members, batch, bytes, rounds) in [
        (3u64, 1usize, 64usize, 20_000usize),
        (3, 16, 64, 2_000),
        (3, 1, 4096, 10_000),
        (3, 16, 4096, 1_000),
        (5, 1, 64, 10_000),
        (5, 16, 1024, 1_000),
        (3, 1, 262_144, 400),
    ] {
        // The better of three: the machine does other things.
        let best =
            |measure: &dyn Fn() -> f64| (0..3).map(|_| measure()).fold(f64::INFINITY, f64::min);
        let old = best(&|| replicate::<Old>(members, batch, bytes, rounds));
        let new = best(&|| replicate::<New>(members, batch, bytes, rounds));
        println!(
            "{:<44} {:>14.0} {:>14.0} {:>8.2}",
            format!("{members} members, {batch} at a time, {bytes} B"),
            old,
            new,
            new / old
        );
    }
}
