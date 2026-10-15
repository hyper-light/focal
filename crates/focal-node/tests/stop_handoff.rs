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

/// Founded clusters a run may take before one's observed leader still leads its session when the
/// stop reaches it. Between the observation and the signal an election under load can move
/// leadership (a busy machine's run, 2026-10-08), and the stopped node then truthfully leads
/// nothing: that attempt cannot show a hand-off and is never counted as one. Such an election in
/// that window is rare, so three fresh clusters bound the run, and a run in which none stopped a
/// leader fails.
const ATTEMPTS: usize = 3;

#[test]
fn a_leader_told_to_stop_hands_its_log_off_before_an_election_could_start() {
    for attempt in 1..=ATTEMPTS {
        if stop_the_leader() {
            return;
        }
        eprintln!(
            "attempt {attempt}: leadership moved before the stop reached it; the stopped node led \
             nothing, so this cluster shows no hand-off"
        );
    }
    panic!("no attempt of {ATTEMPTS} stopped a node that still led its session");
}

/// One founded cluster, its session's leader sent SIGTERM. True once the stopped node led its
/// session at the signal and every claim of the hand-off held; false where it led nothing.
fn stop_the_leader() -> bool {
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
    // The stopped node's own word on what it led when the signal reached it: the premise of every
    // claim below. Leading nothing, leadership had moved before the stop.
    let stopped = servers[leading]
        .as_ref()
        .unwrap()
        .1
        .as_ref()
        .unwrap()
        .recv_timeout(Duration::from_secs(60))
        .expect("the stopped leader printed no Stopped line");
    assert_eq!(stopped["condition"], "Stopped", "{stopped}");
    if stopped["sessions_led"].as_u64() == Some(0) {
        let status = servers[leading].take().unwrap().0.wait().unwrap();
        assert!(status.success(), "the stopped node exited {status}");
        return false;
    }
    // Handed off, not elected after a timeout: the stopped node says so in
    // its last status line, the successor is a survivor, and the log
    // advanced one term for it.
    assert!(all.contains(&successor) && successor != leader);
    assert_eq!(
        new_term,
        term + 1,
        "one term for a hand-off from term {term}"
    );
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
    true
}

/// A joined member told to stop stops every owner it runs, the directory
/// partition it was seated in among them: it says it stopped and exits
/// cleanly. (2026-10-08: the stop skipped the first partition on a member,
/// which hosts it as a seated replica rather than as the founder's slot, and
/// every member waited out the 30-second shutdown deadline and exited with
/// its timeout.)
#[test]
fn a_joined_member_told_to_stop_stops_its_directory_replica_and_exits_cleanly() {
    let founder = Node::new("founder");
    let hosts = [Node::new("host-a"), Node::new("host-b")];
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let (server_a, node_a) = join_start(&founder, &hosts[0], "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &hosts[1], "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    // A deployment seats the root and the directory partition on the
    // members before the session (F24): host-a then runs a directory replica.
    let _ = tenant;
    let intent = founder.root().join("node-1.yaml");
    std::fs::write(
        &intent,
        "version: 1\ndurability:\n  survive: node\n  max_failures: 1\n",
    )
    .unwrap();
    let plan_file = founder.root().join("node-1.plan");
    admin(
        &founder,
        &[
            "plan",
            "deployment",
            "--config",
            intent.to_str().unwrap(),
            "--output",
            plan_file.to_str().unwrap(),
        ],
    );
    let applied = admin(
        &founder,
        &[
            "apply",
            "deployment",
            "--plan-file",
            plan_file.to_str().unwrap(),
            "--wait",
            "300",
        ],
    );
    assert_eq!(applied["result"]["outcome"], "Complete", "{applied}");
    wait_for(
        &founder,
        "node survival",
        Duration::from_secs(180),
        |view| settled(view, &ledger, &all, 1),
    );
    let mut server_a = server_a;
    let signalled = Instant::now();
    assert!(
        Command::new("kill")
            .args(["-TERM", &server_a.pid().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let stopped = server_a
        .1
        .as_ref()
        .unwrap()
        .recv_timeout(Duration::from_secs(60))
        .expect("the stopped member printed no Stopped line");
    assert_eq!(stopped["condition"], "Stopped", "{stopped}");
    let status = server_a.0.wait().unwrap();
    eprintln!("member stop took {:?}", signalled.elapsed());
    assert!(status.success(), "the stopped member exited {status}");
}
