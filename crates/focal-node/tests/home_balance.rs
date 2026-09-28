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
//! Seats that move toward home, across five real `focal` processes over
//! QUIC ([27](../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md)
//! §3.1 P4). A session whose policy names a home region is placed while
//! that region has one node: one voter at home, which leads, and two that
//! are not. Two nodes join at home. Nothing died, so no heal moves
//! anything; the controller gives one seat to a node at home, and when
//! that placement is active, the other. Then every voter is at home and
//! nothing moves.
use serde_json::Value;
use std::time::Duration;

#[path = "support/fleet.rs"]
mod fleet;
use fleet::*;

const BALANCED: &[(&str, &str)] = &[("FOCAL_HOME_BALANCE_HOLD_SECS", "2")];

fn sorted(mut ids: Vec<u64>) -> Vec<u64> {
    ids.sort_unstable();
    ids
}
fn session(view: &Value, ledger: &str) -> Value {
    session_row(view, ledger).unwrap().clone()
}

#[test]
fn seats_move_home_one_at_a_time_and_then_rest() {
    let home = "version: 1\ntopology:\n  region: r1\nplacement:\n  home_regions: [r1]\n";
    let away = "version: 1\ntopology:\n  region: r2\n";
    let founder = Node::with_config("founder", home);
    let hosts = [
        Node::with_config("host-a", away),
        Node::with_config("host-b", away),
        Node::with_config("host-c", "version: 1\ntopology:\n  region: r1\n"),
        Node::with_config("host-d", "version: 1\ntopology:\n  region: r1\n"),
    ];
    let addresses: Vec<String> = (0..5).map(|_| address()).collect();
    let _founder_server = start_with(&founder, &["--advertise", &addresses[0]], BALANCED);
    let (founder_node, tenant, ledger) = identity(&founder);
    let (_server_a, node_a) = join_start(&founder, &hosts[0], "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &hosts[1], "host-b", &addresses[2]);
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &[founder_node, node_a, node_b], 0)
    });
    // Placed while home has one node: it leads, and two voters are away.
    let view = plan_and_settle(
        &founder,
        &tenant,
        &ledger,
        None,
        1,
        &[founder_node, node_a, node_b],
    );
    let placed = session(&view, &ledger);
    assert_eq!(
        sorted(ids(&placed["voters"])),
        sorted(vec![founder_node, node_a, node_b])
    );
    assert_eq!(placed["preferred_leader"], founder_node);
    let epoch = placed["placement_epoch"].as_u64().unwrap();
    // It rests as it is: there is no node at home to give a seat to.
    let began = periods(&founder).unwrap();
    let mut wait = Progress::begin(&[&founder], Duration::from_secs(120));
    while periods(&founder).is_none_or(|now| now < began + 60) {
        assert!(wait.spent().is_none(), "the founder stopped");
        std::thread::sleep(Duration::from_millis(250));
    }
    let rested = session(&placement(&founder).unwrap(), &ledger);
    assert_eq!(rested["placement_epoch"], epoch, "{rested}");
    assert!(rested["pending"].is_null(), "{rested}");

    // Two nodes join at home.
    let (_server_c, node_c) = join_start(&founder, &hosts[2], "host-c", &addresses[3]);
    let (_server_d, node_d) = join_start(&founder, &hosts[3], "host-d", &addresses[4]);
    let all = [founder_node, node_a, node_b, node_c, node_d];
    // One seat, then the other: each move a placement of its own.
    let first = wait_for(
        &founder,
        "the first seat at home",
        Duration::from_secs(480),
        |view| {
            settled(view, &ledger, &all, 1)
                && session_row(view, &ledger)
                    .is_some_and(|session| session["placement_epoch"] == epoch + 1)
        },
    );
    let first = session(&first, &ledger);
    let voters = ids(&first["voters"]);
    assert_eq!(voters.len(), 3, "{first}");
    assert!(voters.contains(&founder_node), "{first}");
    assert_eq!(
        voters
            .iter()
            .filter(|voter| [node_c, node_d].contains(voter))
            .count(),
        1,
        "one seat moved: {first}"
    );
    assert_eq!(first["preferred_leader"], founder_node);
    let second = wait_for(
        &founder,
        "every seat at home",
        Duration::from_secs(480),
        |view| {
            settled(view, &ledger, &all, 1)
                && session_row(view, &ledger).is_some_and(|session| {
                    sorted(ids(&session["voters"])) == sorted(vec![founder_node, node_c, node_d])
                })
        },
    );
    let second = session(&second, &ledger);
    assert_eq!(second["placement_epoch"], epoch + 2, "{second}");
    assert_eq!(second["preferred_leader"], founder_node);
    assert_eq!(second["max_failures"], 1);
    // The copies that were left are drained and retired, and the session
    // serves.
    for left in [node_a, node_b] {
        assert!(!ids(&second["content_copies"]).contains(&left), "{second}");
    }
    let status = run(&founder, None, &["status"]);
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    // At rest.
    let plan = admin(&founder, &["cluster", "plan"])["result"]["actions"].clone();
    assert_eq!(plan, serde_json::json!([]));
    let began = periods(&founder).unwrap();
    let mut wait = Progress::begin(&[&founder], Duration::from_secs(120));
    while periods(&founder).is_none_or(|now| now < began + 80) {
        assert!(wait.spent().is_none(), "the founder stopped");
        std::thread::sleep(Duration::from_millis(250));
    }
    let rested = session(&placement(&founder).unwrap(), &ledger);
    assert_eq!(rested["placement_epoch"], epoch + 2, "{rested}");
    assert!(rested["pending"].is_null(), "{rested}");
}
