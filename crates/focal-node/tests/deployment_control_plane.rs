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
//! The control plane survives what the data does (the audit's F24). A
//! deployment that asks for zone survival seats the root group's voters
//! across the zones as it seats the session's; `cluster placement` states
//! what the root, the directory partitions and the issuer survive, apart
//! from the data; readiness holds the committed policy to the root and the
//! partition groups as well; and with the founder's zone silent the root
//! and the directory's partition are led by another zone and answer — a
//! session is created there — while the issuer the founder alone holds is
//! reported as what it is. Before, the plan placed the session's three
//! voters across the zones and left the root with the founder's single
//! vote (the KIND campaign of 2026-09-29, D5), and the partition group,
//! hosted by the founder alone, fell silent with it (batch 2).
#[path = "support/fleet.rs"]
mod fleet;
use fleet::*;
use serde_json::Value;
use std::time::Duration;

fn control(view: &Value) -> &Value {
    &view["control"]
}
fn configuration(node: &Node) -> Value {
    admin(node, &["cluster", "membership", "show"])["result"]["configuration"].clone()
}

#[test]
fn zone_survival_seats_the_root_across_the_zones_and_states_what_the_control_plane_survives() {
    let founder = Node::with_config(
        "founder",
        "version: 1\ntopology:\n  region: ra\n  zone: a1\n",
    );
    let host_b = Node::with_config(
        "host-b",
        "version: 1\ntopology:\n  region: ra\n  zone: a2\n",
    );
    let host_c = Node::with_config(
        "host-c",
        "version: 1\ntopology:\n  region: ra\n  zone: a3\n",
    );
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let (_server_b, node_b) = join_start(&founder, &host_b, "host-b", &addresses[1]);
    let (_server_c, node_c) = join_start(&founder, &host_c, "host-c", &addresses[2]);
    let all = [founder_node, node_b, node_c];
    let view = wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0) && ids(&control(view)["root"]["learners"]).len() == 2
    });
    // Before the plan: the root is the founder's vote alone, measured and
    // said so; the partition and the issuer are the founder's alone.
    let root = &control(&view)["root"];
    assert_eq!(ids(&root["voters"]), vec![founder_node], "{view}");
    assert_eq!(root["tolerates_zone"], 0, "{view}");
    assert_eq!(root["tolerates_node"], 0, "{view}");
    assert_eq!(control(&view)["partitions"].as_array().unwrap().len(), 1);
    assert_eq!(
        ids(&control(&view)["partitions"][0]["voters"]),
        vec![founder_node]
    );
    assert_eq!(
        ids(&control(&view)["issuer"]["holders"]),
        vec![founder_node]
    );
    assert_eq!(control(&view)["issuer"]["tolerates_node"], 0);
    // The plan seats the root before the session, and promises for both.
    let policy = founder.root().join("zone-1.yaml");
    std::fs::write(
        &policy,
        "version: 1\ndurability:\n  survive: zone\n  max_failures: 1\n",
    )
    .unwrap();
    let plan_file = founder.root().join("zone-1.plan");
    let planned = run_bare(
        &founder,
        &[
            "--config",
            policy.to_str().unwrap(),
            "deployment",
            "plan",
            "--output",
            plan_file.to_str().unwrap(),
        ],
    );
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let planned: Value = serde_json::from_slice(&planned.stdout).unwrap();
    let plan = &planned["result"]["plan"];
    assert!(plan["blocked"].as_array().unwrap().is_empty(), "{planned}");
    assert!(plan["blocked_control"].is_null(), "{planned}");
    let changes = plan["changes"].as_array().unwrap();
    assert_eq!(changes[0]["change"], "commit_policy", "{planned}");
    assert_eq!(changes[1]["change"], "plan_root", "{planned}");
    let mut root_voters = ids(&changes[1]["voters"]);
    root_voters.sort_unstable();
    let mut expected = all.to_vec();
    expected.sort_unstable();
    assert_eq!(root_voters, expected, "{planned}");
    assert_eq!(changes[2]["change"], "plan_partition", "{planned}");
    let mut partition_voters = ids(&changes[2]["voters"]);
    partition_voters.sort_unstable();
    assert_eq!(partition_voters, expected, "{planned}");
    assert_eq!(changes[3]["change"], "plan_session", "{planned}");
    assert_eq!(plan["control_guarantee"]["before"]["max_failures"], 0);
    assert_eq!(plan["control_guarantee"]["after"]["survive"], "zone");
    assert_eq!(plan["control_guarantee"]["after"]["max_failures"], 1);
    assert_eq!(plan["guarantee"]["after"]["max_failures"], 1);
    // Applied: every planned root voter votes, the session's copies are
    // placed, and readiness holds the policy — root included.
    let applied = admin(
        &founder,
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
            "--wait",
            // The apply seats the root and the partition group (three
            // voters each, admitted, caught up and promoted) before the
            // session; its allowance covers that whole sequence on a
            // starved runner (macOS CI ran out of 240 s with the session
            // step committed after both groups were seated, 2026-10-02).
            "360",
        ],
    );
    if applied["result"]["outcome"] != "Complete" {
        // What the placement and the founder say when an apply runs out of
        // its allowance (macOS CI, 2026-10-02: root and partition seated,
        // the session step committed and not complete at 360 s).
        let view = placement(&founder);
        let health = admin(&founder, &["cluster", "node", "health"]);
        panic!("apply not complete: {applied}\nplacement: {view:?}\nhealth: {health}");
    }
    let mut voters = ids(&configuration(&founder)["voters"]);
    voters.sort_unstable();
    assert_eq!(voters, expected);
    let view = wait_for(
        &founder,
        "zone survival",
        Duration::from_secs(240),
        |view| {
            settled(view, &ledger, &all, 1)
                && control(view)["root"]["tolerates_zone"] == 1
                && control(view)["partitions"][0]["tolerates_zone"] == 1
        },
    );
    assert_eq!(control(&view)["root"]["tolerates_node"], 1, "{view}");
    assert_eq!(
        control(&view)["root"]["blocked_by"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    // The partition group's voters are the three as well: its replicas on
    // the hosts were seated by the root's grant, caught up and promoted.
    // The issuer is still the founder's alone, and said to be: the data's
    // promise is not the whole product's.
    let mut partition_voters = ids(&control(&view)["partitions"][0]["voters"]);
    partition_voters.sort_unstable();
    assert_eq!(partition_voters, expected, "{view}");
    assert_eq!(control(&view)["issuer"]["tolerates_node"], 0);
    let ready = readiness(&founder);
    assert_eq!(ready["control_satisfied"], true, "{ready}");
    assert_eq!(ready["policy_satisfied"], true, "{ready}");
    assert!(
        ready["root"]["peers"].as_array().unwrap().len() >= 2,
        "{ready}"
    );
    // The same plan applied again is exact: nothing moves, all complete.
    let again = admin(
        &founder,
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
        ],
    );
    assert_eq!(again["result"]["outcome"], "Complete", "{again}");
    // ---- The founder's zone falls silent. The root is led from another
    // zone and answers the operator there; the session's writes continue
    // through the hosts' copies; what the founder alone hosts is reported
    // as it is, not as surviving.
    founder_server.pause();
    let mut wait = Progress::begin(&[&host_b], Duration::from_secs(180));
    let led = loop {
        let configuration = configuration(&host_b);
        let view = placement(&host_b);
        let leader = view
            .as_ref()
            .map(|view| control(view)["root"]["leader"].as_u64().unwrap_or(0))
            .unwrap_or(0);
        if leader != 0 && leader != founder_node && ids(&configuration["voters"]).len() == 3 {
            break leader;
        }
        if let Some(spent) = wait.spent() {
            panic!("the root was not led from another zone: {spent}; {configuration}; {view:?}");
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    assert!(all.contains(&led) && led != founder_node);
    let view = placement(&host_b).unwrap();
    assert_eq!(control(&view)["root"]["tolerates_zone"], 1, "{view}");
    let founder_down = readiness(&host_b);
    assert_eq!(founder_down["alive"], true, "{founder_down}");
    assert_eq!(founder_down["root"]["leader"], led, "{founder_down}");
    // The directory's partition is led from another zone too, and serves:
    // a session is created on a host while the founder is silent, placed
    // by the partition the hosts now lead.
    let mut wait = Progress::begin(&[&host_b], Duration::from_secs(180));
    let partition_leader = loop {
        let view = placement(&host_b);
        let leader = view
            .as_ref()
            .map(|view| {
                control(view)["partitions"][0]["leader"]
                    .as_u64()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        if leader != 0 && leader != founder_node {
            break leader;
        }
        if let Some(spent) = wait.spent() {
            panic!("the partition was not led from another zone: {spent}; {view:?}");
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    assert!(all.contains(&partition_leader) && partition_leader != founder_node);
    let created = admin(
        &host_b,
        &[
            "cluster",
            "sessions",
            "create",
            "--tenant",
            &tenant,
            "--name",
            "f24-while-founder-silent",
        ],
    );
    assert_eq!(created["result"]["kind"], "session_created", "{created}");
    assert_eq!(created["result"]["existing"], false, "{created}");
    // The founder returns: it follows or leads again, and the policy holds.
    founder_server.resume();
    let mut wait = Progress::begin(&[&founder], Duration::from_secs(180));
    loop {
        let ready = readiness(&founder);
        if ready["policy_satisfied"] == true && ready["root"]["leader"].as_u64().unwrap_or(0) != 0 {
            break;
        }
        if let Some(spent) = wait.spent() {
            panic!("the founder did not rejoin: {spent}; {ready}");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let view = wait_for(
        &founder,
        "the whole promise",
        Duration::from_secs(120),
        |view| settled(view, &ledger, &all, 1) && control(view)["root"]["tolerates_zone"] == 1,
    );
    assert_eq!(
        control(&view)["root"]["blocked_by"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}
