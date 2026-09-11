#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Lowering a session's `max_failures` shrinks the voter set (24 §4, §19).
//! A voter the placement drops keeps voting until activation retires it, so
//! a shrink commits no membership change before its cut-over and its
//! membership epoch holds until activation; the controller and the session,
//! directory and authority checks agree on that (see doc 09, the
//! "shrinking the voter set" note). This exercises the whole path: three
//! voters, a workload, then `max_failures 0` reducing to one voter with the
//! earlier and later claims still readable.
use std::time::Duration;
#[path = "support/fleet.rs"]
mod fleet;
use fleet::*;

#[test]
fn lowering_max_failures_shrinks_the_voter_set_and_keeps_the_history() {
    let founder = Node::new("founder");
    let host_a = Node::new("host-a");
    let host_b = Node::new("host-b");
    let client = Node::new("client");
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let alice = fleet::enroll_client(&founder, &client, "alice");
    let before = write_claim(&founder, &alice, "before the shrink");
    let (_server_a, node_a) = join_start(&founder, &host_a, "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &host_b, "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    // Three voters, one tolerated failure.
    plan_and_settle(&founder, &tenant, &ledger, None, 1, &all);
    let during = write_claim(&founder, &alice, "at three voters");
    let voters = ids(&session_row(&placement(&founder).unwrap(), &ledger).unwrap()["voters"]);
    assert_eq!(voters.len(), 3, "three voters before the shrink");
    // Lower the tolerance: one voter. A voter the plan drops keeps voting
    // until activation retires it, so the cut-over commits with the epoch
    // unchanged and the downgrade completes.
    let planned = admin(
        &founder,
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--max-failures",
            "0",
        ],
    );
    assert_eq!(planned["result"]["state"], "planned", "{planned}");
    let view = wait_for(&founder, "the shrink", Duration::from_secs(200), |view| {
        session_row(view, &ledger).is_some_and(|session| {
            session["pending"].is_null()
                && session["achieved_max_failures"] == 0
                && ids(&session["voters"]).len() == 1
        })
    });
    let shrunk = ids(&session_row(&view, &ledger).unwrap()["voters"]);
    assert_eq!(shrunk.len(), 1, "one voter after the shrink: {view}");
    // The sole voter serves the session. Read the earlier claims from it
    // directly (its own socket), retrying while leadership settles.
    let sole = shrunk[0];
    let voter: &Node = if sole == node_a { &host_a } else { &host_b };
    let read_from_voter = |claim: &str| -> String {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let output = fleet::run(voter, None, &["get", "claim", claim, "--format", "json"]);
            if output.status.success()
                && let Ok(page) = serde_json::from_slice::<serde_json::Value>(&output.stdout)
                && page["result"]["kind"] == "native_read"
            {
                return claim_id(&objects(&page)[0]);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the sole voter never served: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            std::thread::sleep(Duration::from_millis(500));
        }
    };
    assert_eq!(read_from_voter(&before), before);
    assert_eq!(read_from_voter(&during), during);
    let _ = &client;
}
