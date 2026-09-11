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
//! Stage 2, VMs or bare metal (08 §3; DC03, DC05, DC06, DC16, DC19): hosts
//! enroll from invitations and start in one command, a replayed, tampered
//! or revoked invitation is refused; a durability intent planned and
//! applied derives the placement and advertises protection only once the
//! copies are ready, with the demo unchanged before and after; a policy
//! the hosts cannot provide is planned as blocked and leaves the contract
//! intact; a plan made on an earlier policy revision is refused as stale;
//! a host that holds copies cannot be removed, a drained one leaves the
//! guarantee visibly short until a replacement arrives, and is removed
//! only once nothing names it.
use serde_json::Value;
use std::{os::unix::fs::PermissionsExt, time::Duration};

#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/journey.rs"]
mod journey;
use fleet::*;
use journey::*;

fn guarantee(view: &Value, ledger: &str) -> (Option<u64>, usize) {
    let session = session_row(view, ledger).unwrap();
    (
        session["achieved_max_failures"].as_u64(),
        session["blocked_by"].as_array().map_or(0, Vec::len),
    )
}

#[test]
fn the_vm_stage_adds_addresses_invitations_and_a_durability_intent() {
    let mut journey = Journey::building_on("fleet", "deployment_fleet", &["laptop"]);
    let region = "version: 1\ntopology:\n  region: ra\n";
    let founder = Node::with_config("founder", region);
    let host_a = Node::with_config("host-a", region);
    let host_b = Node::with_config("host-b", region);
    let host_c = Node::with_config("host-c", region);
    let addresses: Vec<String> = (0..6).map(|_| address()).collect();
    journey.admin(
        &founder,
        "native engine",
        &["data directory"],
        &["cluster", "replicas", "activate-native"],
    );
    let _founder_server = journey.start(
        &founder,
        "start",
        &["data directory", "address"],
        &["--advertise", &addresses[0]],
    );
    let (founder_node, _tenant, ledger) = identity(&founder);
    let demo = journey.enroll(&founder, "alice");
    let token = Journey::client_token(&demo);
    let first = journey.demo(&founder, &demo, "before the fleet");
    // ---- DC03: a host enrolls and starts in one command. The same
    // invitation again, a tampered one and a revoked one enroll nothing.
    let (_server_a, node_a) = journey.join(&founder, &host_a, "host-a", &addresses[1]);
    let replay = Node::new("replay");
    let used = host_a.root().join("host-a.invite");
    let node_token = Journey::node_token(&used);
    let (code, report) = journey.failure(
        &replay,
        None,
        "join",
        &["invitation file", "address"],
        &[
            "start",
            "--advertise",
            &addresses[3],
            "--invite-file",
            used.to_str().unwrap(),
        ],
    );
    assert_ne!(code, 0, "{report}");
    assert!(!replay.root().join("IDENTITY").exists(), "{report}");
    let invite_b = journey.invite(&founder, &host_b, "host-b");
    let mut bytes = std::fs::read(&invite_b).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    let tampered = Node::new("tampered");
    let tampered_file = tampered.root().join("tampered.invite");
    std::fs::write(&tampered_file, &bytes).unwrap();
    std::fs::set_permissions(&tampered_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let (code, report) = journey.failure(
        &tampered,
        None,
        "join",
        &["invitation file", "address"],
        &[
            "start",
            "--advertise",
            &addresses[4],
            "--invite-file",
            tampered_file.to_str().unwrap(),
        ],
    );
    assert_ne!(code, 0, "{report}");
    assert!(!tampered.root().join("IDENTITY").exists(), "{report}");
    let _server_b = journey.start(
        &host_b,
        "join",
        &["invitation file", "address"],
        &[
            "--advertise",
            &addresses[2],
            "--invite-file",
            invite_b.to_str().unwrap(),
        ],
    );
    let node_b = identity(&host_b).0;
    let revoked_host = Node::new("revoked");
    let invite_r = journey.invite(&founder, &revoked_host, "host-r");
    let listed = journey.admin(
        &founder,
        "invitation",
        &[],
        &["cluster", "invitations", "list"],
    );
    let pending: Vec<String> = listed["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["credential"].is_null() && entry["revoked"] == false)
        .map(|entry| entry["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(pending.len(), 1, "{listed}");
    let revoked = journey.admin(
        &founder,
        "invitation",
        &["invitation id"],
        &["cluster", "invitations", "revoke", &pending[0]],
    );
    assert!(revoked["result"]["operation_id"].is_string(), "{revoked}");
    let (code, report) = journey.failure(
        &revoked_host,
        None,
        "join",
        &["invitation file", "address"],
        &[
            "start",
            "--advertise",
            &addresses[5],
            "--invite-file",
            invite_r.to_str().unwrap(),
        ],
    );
    assert_ne!(code, 0, "{report}");
    assert!(!revoked_host.root().join("IDENTITY").exists(), "{report}");
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    let view = journey.admin(&founder, "placement view", &[], &["cluster", "placement"]);
    assert_eq!(
        guarantee(&view["result"]["placement"], &ledger),
        (Some(0), 0),
        "{view}"
    );
    // ---- DC05: one intent, node survival with one failure. The plan
    // derives three voters; applying it commits the policy and requests the
    // placement; the guarantee is advertised once the copies are ready.
    let node_1 = "version: 1\ndurability:\n  survive: node\n  max_failures: 1\n";
    let plan_file = founder.root().join("node-1.plan");
    let planned = journey.plan(&founder, "node-1", node_1, Some(&plan_file));
    let plan = &planned["result"]["plan"];
    assert!(plan["blocked"].as_array().unwrap().is_empty(), "{planned}");
    assert_eq!(plan["guarantee"]["before"]["max_failures"], 0);
    assert_eq!(plan["guarantee"]["after"]["max_failures"], 1);
    let voters = plan["changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["change"] == "plan_session")
        .map(|change| ids(&change["voters"]))
        .unwrap();
    assert_eq!(voters.len(), 3, "{planned}");
    let applied = journey.admin(
        &founder,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
            "--wait",
            "180",
        ],
    );
    assert_eq!(applied["result"]["outcome"], "Complete", "{applied}");
    let view = wait_for(
        &founder,
        "node survival",
        Duration::from_secs(180),
        |view| settled(view, &ledger, &all, 1),
    );
    assert_eq!(guarantee(&view, &ledger), (Some(1), 0));
    journey.same(&founder, &first);
    let second = journey.demo(&founder, &demo, "on the fleet");
    assert_ne!(second.claim, first.claim);
    // The same records read the same through the participant's own
    // connection, which finds the session wherever the fleet serves it.
    journey.same_via(&demo, &first);
    // ---- DC06: two failures need five hosts. The plan names the session
    // it cannot place and keeps the contract: applying it is refused and
    // the achieved level is what it was.
    let node_2 = "version: 1\ndurability:\n  survive: node\n  max_failures: 2\n";
    let blocked_file = founder.root().join("node-2.plan");
    let blocked = journey.plan(&founder, "node-2", node_2, Some(&blocked_file));
    let plan = &blocked["result"]["plan"];
    assert_eq!(plan["blocked"].as_array().unwrap().len(), 1, "{blocked}");
    assert_eq!(plan["blocked"][0]["session"], ledger);
    assert_eq!(plan["guarantee"]["after"]["max_failures"], 1);
    let (code, report) = journey.failure(
        &founder,
        None,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            blocked_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 6, "{report}");
    assert!(report.contains("[guarantee_unsatisfied]"), "{report}");
    assert_eq!(
        guarantee(&placement(&founder).unwrap(), &ledger),
        (Some(1), 0)
    );
    // ---- DC16: the same request composed again is the same plan and
    // resumes as complete; a plan whose policy moved on under it is refused
    // as stale before any side effect. Two node-survival policies that
    // differ only by home region are two distinct valid plans.
    let node_1_home = "version: 1\ndurability:\n  survive: node\n  max_failures: 1\nplacement:\n  home_regions: [ra]\n";
    let again_file = founder.root().join("node-1-again.plan");
    journey.plan(&founder, "node-1-again", node_1, Some(&again_file));
    let resumed = journey.admin(
        &founder,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            again_file.to_str().unwrap(),
        ],
    );
    assert_eq!(resumed["result"]["outcome"], "Complete", "{resumed}");
    let node_1_res = "version: 1\ndurability:\n  survive: node\n  max_failures: 1\nplacement:\n  residency: [ra]\n";
    // A plan made now, before another distinct policy commits under it.
    let early_home = founder.root().join("node-1-home-early.plan");
    let early = journey.plan(
        &founder,
        "node-1-home-early",
        node_1_home,
        Some(&early_home),
    );
    assert_eq!(
        early["result"]["plan"]["observed"]["policy_revision"], 2,
        "{early}"
    );
    // A different valid policy commits and moves the revision on.
    let res_file = founder.root().join("node-1-res.plan");
    journey.plan(&founder, "node-1-res", node_1_res, Some(&res_file));
    let applied = journey.admin(
        &founder,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            res_file.to_str().unwrap(),
            "--wait",
            "180",
        ],
    );
    assert_eq!(applied["result"]["outcome"], "Complete", "{applied}");
    let explained = journey.admin(&founder, "explain", &[], &["deployment", "explain"]);
    assert_eq!(explained["committed_revision"], 3, "{explained}");
    // The early plan was made at revision 2; the policy is at 3 now, so it
    // is refused as stale before any side effect.
    let (code, report) = journey.failure(
        &founder,
        None,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            early_home.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[stale_plan]"), "{report}");
    assert!(
        !founder
            .root()
            .join("cluster/apply")
            .join(early["result"]["plan"]["plan_id"].as_str().unwrap())
            .exists()
    );
    // ---- DC19: shrinking. A host that holds copies is not removed; drained,
    // it leaves the guarantee visibly short until a replacement joins; then
    // it is removed and the demo still reads.
    let (code, report) = journey.failure(
        &founder,
        None,
        "remove",
        &["node id"],
        &["cluster", "nodes", "remove", "--node", &node_b.to_string()],
    );
    assert_ne!(code, 0, "{report}");
    assert!(node_row(&placement(&founder).unwrap(), node_b).is_some());
    let drained = journey.admin(
        &founder,
        "drain",
        &["node id"],
        &["cluster", "nodes", "drain", "--node", &node_b.to_string()],
    );
    assert_eq!(drained["result"]["eligible"], false, "{drained}");
    let view = wait_for(
        &founder,
        "a short guarantee",
        Duration::from_secs(120),
        |view| guarantee(view, &ledger).1 > 0,
    );
    assert!(
        guarantee(&view, &ledger).0 < Some(1) || guarantee(&view, &ledger).1 > 0,
        "{view}"
    );
    journey.same(&founder, &second);
    let (_server_c, node_c) = journey.join(&founder, &host_c, "host-c", &addresses[3]);
    let healed = [founder_node, node_a, node_c];
    wait_for(&founder, "the heal", Duration::from_secs(240), |view| {
        settled(view, &ledger, &healed, 1)
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let removed = loop {
        let output = fleet::run(
            &founder,
            None,
            &["cluster", "nodes", "remove", "--node", &node_b.to_string()],
        );
        if output.status.success() {
            break serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "removal never succeeded: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(500));
    };
    // Record the removal that succeeded (the retry loop above used the bare
    // runner so the recording shows the operator's one command).
    journey.steps_record_remove(&founder, node_b);
    assert_eq!(removed["membership_removed"], true, "{removed}");
    journey.same(&founder, &second);
    // The heal settled the session; wait for it to be quiet, then a fresh
    // claim written and read back proves the shrunk fleet still serves
    // (the full participant round-trip ran before and on the fleet).
    wait_for(
        &founder,
        "a quiet session",
        Duration::from_secs(120),
        |view| settled(view, &ledger, &healed, 1),
    );
    let third = journey.write(&founder, &demo, "after the shrink");
    assert_ne!(third, second.claim);
    assert_eq!(claim_id(&journey.read(&founder, "claim", &third)), third);
    journey.assert_redacted(&[&token, &node_token]);
    journey.not_executed(
        "an expired invitation: lifetimes are a day by default and at most seven; expiry is the enrollment registry's unit tests",
    );
    journey.finish();
}
