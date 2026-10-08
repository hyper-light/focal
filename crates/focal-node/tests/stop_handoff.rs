//! A leader that is told to stop hands its log off before it goes (27 §5):
//! the voter that holds the whole log is asked to campaign at once, so the
//! group is led again within a couple of round trips, where a leader that
//! went silent cost the survivors their whole election timeout. Measured on
//! real processes: three voters, the session's leader sent SIGTERM, the
//! succession charged to the survivors' own periods — the unit the election
//! timer counts.
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
use std::process::Command;
use std::time::{Duration, Instant};

/// The session's leader and term as `node` sees them now, from its own
/// replica (`inspect replicas --replicas` answers locally, unsampled).
fn leadership(node: &Node, ledger: &str) -> Option<(u64, u64)> {
    let output = run(
        node,
        None,
        &["inspect", "replicas", "--replicas", "--session", ledger],
    );
    if !output.status.success() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let diagnostics = &value["result"]["diagnostics"];
    Some((
        diagnostics["leader"].as_u64()?,
        diagnostics["term"].as_u64()?,
    ))
}

#[test]
fn a_leader_told_to_stop_hands_its_log_off_before_an_election_could_start() {
    let founder = Node::new("founder");
    let hosts = [Node::new("host-a"), Node::new("host-b")];
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let (server_a, node_a) = join_start(&founder, &hosts[0], "host-a", &addresses[1]);
    let (server_b, node_b) = join_start(&founder, &hosts[1], "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    plan_and_settle(&founder, &tenant, &ledger, None, 1, &all);
    let mut servers = [Some(founder_server), Some(server_a), Some(server_b)];
    let nodes = [&founder, &hosts[0], &hosts[1]];

    // The session's leader, once every copy reports the same one.
    let mut wait = Progress::begin(&nodes, Duration::from_secs(60));
    let (leader, term) = loop {
        let seen: Vec<Option<(u64, u64)>> =
            nodes.iter().map(|node| leadership(node, &ledger)).collect();
        if let [Some(a), Some(b), Some(c)] = seen[..]
            && a == b
            && b == c
            && a.0 != 0
        {
            break a;
        }
        assert!(
            wait.spent().is_none(),
            "the copies never agreed on a leader: {seen:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let leading = all.iter().position(|id| *id == leader).unwrap();
    eprintln!(
        "nodes founder={founder_node} host-a={node_a} host-b={node_b}; leader {leader} (index {leading}) at term {term}"
    );
    let survivors: Vec<&Node> = (0..3).filter(|i| *i != leading).map(|i| nodes[i]).collect();

    // A planned stop: SIGTERM, as a process manager sends it.
    let pid = servers[leading].as_ref().unwrap().pid();
    let started = Instant::now();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    let mut wait = Progress::begin(&survivors, Duration::from_secs(60));
    let (successor, new_term) = loop {
        let seen: Vec<Option<(u64, u64)>> = survivors
            .iter()
            .map(|node| leadership(node, &ledger))
            .collect();
        if let Some(found) = seen
            .iter()
            .flatten()
            .find(|(id, _)| *id != 0 && *id != leader)
        {
            break *found;
        }
        assert!(wait.spent().is_none(), "no successor was seen: {seen:?}");
        std::thread::sleep(Duration::from_millis(20));
    };
    let took = started.elapsed();
    eprintln!("succession from {leader} to {successor} in {took:?}, term {term} -> {new_term}");
    // Handed off, not elected after a timeout: the stopped node says so in
    // its last status line, the successor is a survivor, and the log
    // advanced one term for it.
    assert!(all.contains(&successor) && successor != leader);
    assert_eq!(
        new_term,
        term + 1,
        "one term for a hand-off from term {term}"
    );
    let stopped = servers[leading]
        .as_ref()
        .unwrap()
        .1
        .as_ref()
        .unwrap()
        .recv_timeout(Duration::from_secs(60))
        .expect("the stopped leader printed no Stopped line");
    assert_eq!(stopped["condition"], "Stopped", "{stopped}");
    assert_eq!(
        (
            stopped["sessions_led"].as_u64(),
            stopped["sessions_handed_off"].as_u64()
        ),
        (Some(1), Some(1)),
        "{stopped}"
    );
    // The stopped leader exits as a planned stop does, and the successor
    // serves the session: authoritative, in the new term, as every survivor
    // reports it.
    let status = servers[leading].take().unwrap().0.wait().unwrap();
    assert!(status.success(), "the stopped leader exited {status}");
    let successor_node = survivors
        .iter()
        .find(|node| identity(node).0 == successor)
        .copied()
        .expect("the successor is a survivor");
    let mut wait = Progress::begin(&survivors, Duration::from_secs(60));
    loop {
        let diagnostics = admin(
            successor_node,
            &["inspect", "replicas", "--replicas", "--session", &ledger],
        )["result"]["diagnostics"]
            .clone();
        if diagnostics["authoritative"] == true && diagnostics["term"] == new_term {
            break;
        }
        assert!(
            wait.spent().is_none(),
            "the successor never became authoritative: {diagnostics}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
