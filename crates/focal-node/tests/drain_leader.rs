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
//! Draining the host that leads a session
//! ([27](../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §5),
//! on real `focal` processes: a leader does not remove itself, so the
//! controller moves the session's leadership to a voter that stays and that
//! leader removes the drained host. The session never follows a host outside
//! its configuration, and it serves before, during and after.
use std::time::Duration;

#[path = "support/fleet.rs"]
mod fleet;
use fleet::*;

/// Where `node` says the session leads, from its metrics; none while it
/// does not answer or does not host the session.
fn session_leader(node: &Node, ledger: &str) -> Option<u64> {
    let output = run(node, None, &["diagnose", "node", "--metrics"]);
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.starts_with("focal_session_leader{"))
        .find(|line| line.contains(&format!("session=\"{ledger}\"")))
        .and_then(|line| line.rsplit(' ').next()?.trim().parse().ok())
}
fn wait_leader(
    nodes: &[&Node],
    ledger: &str,
    what: &str,
    allowance: Duration,
    mut holds: impl FnMut(u64) -> bool,
) -> u64 {
    let mut wait = Progress::begin(nodes, allowance);
    loop {
        // Every host that answers names the same leader, and it is the one
        // waited for.
        let named: Vec<u64> = nodes
            .iter()
            .filter_map(|node| session_leader(node, ledger))
            .collect();
        if let Some(first) = named.first().copied()
            && first != 0
            && named.iter().all(|leader| *leader == first)
            && holds(first)
        {
            return first;
        }
        if let Some(spent) = wait.spent() {
            panic!("{what}: {spent}; leaders named {named:?}");
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Ask the host that leads to hand leadership to `target`. A transfer is a
/// request: it is asked again until every host says it happened.
fn transfer(leading: &Node, nodes: &[&Node], ledger: &str, target: u64) {
    let mut wait = Progress::begin(nodes, Duration::from_secs(120));
    loop {
        let _ = run(
            leading,
            None,
            &[
                "cluster",
                "replicas",
                "transfer",
                "--session",
                ledger,
                "--node",
                &target.to_string(),
            ],
        );
        std::thread::sleep(Duration::from_millis(500));
        if nodes
            .iter()
            .all(|node| session_leader(node, ledger) == Some(target))
        {
            return;
        }
        if let Some(spent) = wait.spent() {
            panic!("leadership never moved to {target}: {spent}");
        }
    }
}

#[test]
fn a_drained_session_leader_hands_leadership_on_before_it_is_removed() {
    let founder = Node::new("founder");
    let hosts = [
        Node::new("host-a"),
        Node::new("host-b"),
        Node::new("host-c"),
    ];
    let addresses: Vec<String> = (0..4).map(|_| address()).collect();
    activate_native(&founder);
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let client = Node::new("client");
    let alice = enroll_client(&founder, &client, "alice");
    let before = write_claim(&founder, &alice, "before the drain");
    let (_server_a, node_a) = join_start(&founder, &hosts[0], "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &hosts[1], "host-b", &addresses[2]);
    let first = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &first, 0)
    });
    plan_and_settle(&founder, &tenant, &ledger, None, 1, &first);
    let voters = [&founder, &hosts[0], &hosts[1]];
    wait_leader(
        &voters,
        &ledger,
        "the founder leads its session",
        Duration::from_secs(60),
        |leader| leader == founder_node,
    );

    // The operator moves the session's leadership to host a.
    transfer(&founder, &voters, &ledger, node_a);
    // The participant's client knows the founder's address; it is sent on
    // to the host that leads and commits there.
    let artifact = deliver_artifact(&client, "alice", &before);
    assert!(!artifact.is_empty());

    // A replacement joins, and the host that leads is drained.
    let (_server_c, node_c) = join_start(&founder, &hosts[2], "host-c", &addresses[3]);
    let drained = admin(
        &founder,
        &["cluster", "nodes", "drain", "--node", &node_a.to_string()],
    );
    assert_eq!(drained["result"]["eligible"], false, "{drained}");
    let healed = [founder_node, node_b, node_c];
    let everyone = [&founder, &hosts[0], &hosts[1], &hosts[2]];
    let mut wait = Progress::begin(&[&founder, &hosts[1], &hosts[2]], Duration::from_secs(300));
    let view = loop {
        // Settled on the hosts that stay: the drain is committed at once,
        // and the plan that answers it a controller round later.
        if let Some(view) = placement(&founder)
            && settled(&view, &ledger, &healed, 1)
            && session_row(&view, &ledger).is_some_and(|session| {
                let mut voters = ids(&session["voters"]);
                voters.sort_unstable();
                let mut expected = healed.to_vec();
                expected.sort_unstable();
                voters == expected
            })
        {
            break view;
        }
        if let Some(spent) = wait.spent() {
            let seen: Vec<String> = everyone
                .iter()
                .map(|node| {
                    format!(
                        "leader {:?}; health {}; replicas {}; plan {}; periods {}",
                        session_leader(node, &ledger),
                        String::from_utf8_lossy(
                            &run(node, None, &["diagnose", "node", "--health"]).stdout
                        ),
                        // Which replica of the session each node runs, with
                        // its leader, commit and apply: a replacement stuck
                        // at `Installed` is one that never caught up, or never
                        // learned a leader (three CI runs of 2026-10-01/02).
                        String::from_utf8_lossy(
                            &run(node, None, &["diagnose", "cluster", "--replicas"]).stdout
                        ),
                        String::from_utf8_lossy(&run(node, None, &["cluster", "plan"]).stdout),
                        String::from_utf8_lossy(
                            &run(node, None, &["diagnose", "node", "--metrics"]).stdout
                        )
                        .lines()
                        .filter(|line| line.contains("period") || line.contains("pace"))
                        .collect::<Vec<_>>()
                        .join("\n")
                    )
                })
                .collect();
            panic!(
                "the heal did not happen: {spent}: {seen:#?}; view {:#}",
                placement(&founder)
                    .as_ref()
                    .and_then(|view| session_row(view, &ledger))
                    .cloned()
                    .unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    let session = session_row(&view, &ledger).unwrap();
    assert!(!ids(&session["voters"]).contains(&node_a), "{session}");

    // The session leads at a voter that stays, by every host that stays.
    let staying = [&founder, &hosts[1], &hosts[2]];
    let leader = wait_leader(
        &staying,
        &ledger,
        "the session leads at a host that stays",
        Duration::from_secs(120),
        |leader| healed.contains(&leader),
    );
    assert_ne!(leader, node_a);
    // The drained host stops claiming the session: it follows the leader
    // that removed it, or its copy is retired and it reports none.
    let mut wait = Progress::begin(&[&hosts[0]], Duration::from_secs(120));
    while session_leader(&hosts[0], &ledger) == Some(node_a) {
        if let Some(spent) = wait.spent() {
            let metrics = run(&hosts[0], None, &["diagnose", "node", "--metrics"]);
            panic!(
                "the drained host still claims the session: {spent}: {:?}",
                String::from_utf8_lossy(&metrics.stdout)
                    .lines()
                    .filter(|line| line.starts_with("focal_session_"))
                    .collect::<Vec<_>>()
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    // It serves the participant wherever it leads.
    let read = cli(&client, Some("alice"), &["get", "claim", &before]);
    assert_eq!(claim_id(&objects(&read)[0]), before, "{read}");
    // A node's own socket reaches the log where that node leads it (24
    // §14): the operator brings leadership home to write there.
    if leader != founder_node {
        let leading = if leader == node_b {
            &hosts[1]
        } else {
            &hosts[2]
        };
        transfer(leading, &staying, &ledger, founder_node);
    }
    let after = write_claim(&founder, &alice, "after the drain");
    for claim in [&before, &after] {
        assert_eq!(claim_id(&read_claim(&founder, claim)), *claim);
    }

    // The drained host holds nothing and is removed.
    let mut wait = Progress::begin(&staying, Duration::from_secs(180));
    let removed = loop {
        let output = run(
            &founder,
            None,
            &["cluster", "nodes", "remove", "--node", &node_a.to_string()],
        );
        if output.status.success() {
            break serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["result"]
                .clone();
        }
        if let Some(spent) = wait.spent() {
            panic!(
                "removal never succeeded: {spent}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    assert_eq!(removed["node"], node_a, "{removed}");
    let last = write_claim(&founder, &alice, "after the removal");
    assert_eq!(claim_id(&read_claim(&founder, &last)), last);
}
