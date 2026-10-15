//! A follower serves a linearizable read (27 §5; the KIND campaign of
//! 2026-09-29, D4): the read asks the leader for its commit index through
//! Raft's ReadIndex, waits until this copy has applied it, and answers from
//! its own committed state — where a follower refused the read and the
//! client resent it for its whole 30 s ceiling before reporting
//! `unavailable`. Three voters on real processes, written through the leader,
//! read through each host's own operator socket.
#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/fleet.rs"]
mod fleet;

use fleet::*;
use std::time::{Duration, Instant};

#[test]
fn a_follower_answers_a_linearizable_read_with_what_the_leader_committed() {
    let founder = Node::new("founder");
    let hosts = [Node::new("host-a"), Node::new("host-b")];
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let client = Node::new("client");
    let alice = enroll_client(&founder, &client, "alice");
    let (_server_a, node_a) = join_start(&founder, &hosts[0], "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &hosts[1], "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    plan_and_settle(&founder, &tenant, &ledger, None, 1, &all);
    // Written through the leader; read through every host at once, where a
    // host that does not lead serves the read from its own copy at the
    // leader's commit index. Each read is charged to the hosts' progress by
    // the harness; what is asserted is the answer, not its time.
    for round in 0..3 {
        let claim = write_claim(
            &founder,
            &alice,
            &format!("read from a follower, round {round}"),
        );
        for host in &hosts {
            let started = Instant::now();
            let object = read_claim(host, &claim);
            eprintln!(
                "round {round}: {} answered in {:?}",
                host.root().display(),
                started.elapsed()
            );
            assert_eq!(claim_id(&object), claim, "{object}");
        }
    }
}
