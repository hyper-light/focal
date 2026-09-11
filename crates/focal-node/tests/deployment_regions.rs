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
//! Stage 5, regions (08 §7; DC10, DC11, DC12): nodes announce a region, a
//! region-survival policy places one voter in each of three regions, the
//! node's metrics show the measured inter-node round-trip time (the latency
//! an operator weighs against remote acknowledgement), losing a region
//! leaves the promised writes readable and the guarantee degraded until the
//! region returns, and a home-region and residency policy is a hard fence:
//! a move outside the residency is refused, and a policy that needs a region
//! the residency excludes cannot be placed. The region stage adds the region
//! fact, home regions, the measured latency and the residency fence over the
//! zone stage.
use serde_json::Value;
use std::time::Duration;

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
fn regional(view: &Value, id: u64, region: &str) -> bool {
    node_row(view, id).is_some_and(|row| row["alive"] == true && row["region"] == region)
}
/// The founder's metrics text once a measured peer RTT is present.
fn wait_for_peer_rtt(founder: &Node) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = fleet::run(founder, None, &["cluster", "node", "metrics"]);
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout).into_owned();
            if text.contains("focal_peer_rtt_ms{") {
                return text;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no peer RTT metric within 60 s"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn the_region_stage_spans_regions_shows_latency_and_fences_residency() {
    let mut journey = Journey::building_on(
        "regions",
        "deployment_regions",
        &["laptop", "fleet", "zones"],
    );
    // A host in each of three regions and a fourth region the policy excludes;
    // the founder homes writes in r1 and fences every copy to r1, r2, r3.
    let founder = Node::with_config(
        "founder",
        "version: 1\ntopology:\n  region: r1\nplacement:\n  home_regions: [r1]\n  residency: [r1, r2, r3]\n",
    );
    let host_b = Node::with_config("host-b", "version: 1\ntopology:\n  region: r2\n");
    let host_c = Node::with_config("host-c", "version: 1\ntopology:\n  region: r3\n");
    let host_d = Node::with_config("host-d", "version: 1\ntopology:\n  region: r4\n");
    let addresses: Vec<String> = (0..4).map(|_| address()).collect();
    journey.admin(
        &founder,
        "native engine",
        &["data directory"],
        &["cluster", "replicas", "activate-native"],
    );
    let _founder_server = journey.start(
        &founder,
        "start",
        &["data directory", "address", "region fact"],
        &["--advertise", &addresses[0]],
    );
    let (founder_node, tenant, ledger) = identity(&founder);
    let demo = journey.enroll(&founder, "alice");
    let token = Journey::client_token(&demo);
    let (_server_b, node_b) = journey.join(&founder, &host_b, "host-b", &addresses[1]);
    let (server_c, node_c) = journey.join(&founder, &host_c, "host-c", &addresses[2]);
    let (_server_d, node_d) = journey.join(&founder, &host_d, "host-d", &addresses[3]);
    wait_for(
        &founder,
        "declared regions",
        Duration::from_secs(120),
        |view| {
            regional(view, founder_node, "r1")
                && regional(view, node_b, "r2")
                && regional(view, node_c, "r3")
                && regional(view, node_d, "r4")
                && settled(view, &ledger, &[founder_node, node_b, node_c, node_d], 0)
        },
    );
    let before = journey.demo(&founder, &demo, "before the region loss");
    // ---- DC10: region survival across three regions; the r4 host is never
    // chosen (home regions and the residency exclude it).
    let planned = journey.admin(
        &founder,
        "region survival",
        &["tenant", "session", "survive region"],
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--survive",
            "region",
            "--max-failures",
            "1",
        ],
    );
    assert_eq!(planned["result"]["state"], "planned", "{planned}");
    let view = wait_for(
        &founder,
        "region activation",
        Duration::from_secs(240),
        |view| {
            session_row(view, &ledger).is_some_and(|session| {
                session["pending"].is_null()
                    && session["achieved_survive"] == "Region"
                    && session["achieved_max_failures"] == 1
            })
        },
    );
    let active = session_row(&view, &ledger).unwrap().clone();
    let voters = ids(&active["voters"]);
    assert_eq!(voters.len(), 3, "{active}");
    assert!(voters.contains(&founder_node) && voters.contains(&node_b) && voters.contains(&node_c));
    assert!(
        !voters.contains(&node_d),
        "the excluded region is never chosen: {active}"
    );
    // ---- DC10: the measured round-trip time to each peer is shown.
    let metrics = wait_for_peer_rtt(&founder);
    assert!(metrics.contains("focal_peer_rtt_ms{"), "{metrics}");
    journey.manual(
        "peer latency",
        &[],
        &[
            "focal",
            "--data-dir",
            "<founder>",
            "cluster",
            "node",
            "metrics",
        ],
    );
    // ---- DC11: lose region r3 (pause its host; it returns with its disk).
    // The promised writes survive, the guarantee degrades, writes continue.
    journey.manual("region loss", &[], &["kill", "-STOP", "<host in r3>"]);
    server_c.pause();
    let view = wait_for(
        &founder,
        "the region's loss",
        Duration::from_secs(120),
        |view| !node_row(view, node_c).is_some_and(|row| row["alive"] == true),
    );
    assert!(
        guarantee(&view, &ledger).1 > 0,
        "the degraded guarantee is visible: {view}"
    );
    let during = journey.write(&founder, &demo, "while a region is down");
    server_c.resume();
    journey.manual("region return", &[], &["kill", "-CONT", "<host in r3>"]);
    let view = wait_for(
        &founder,
        "the region rejoins",
        Duration::from_secs(180),
        |view| regional(view, node_c, "r3") && guarantee(view, &ledger) == (Some(1), 0),
    );
    assert_eq!(
        session_row(&view, &ledger).unwrap()["achieved_survive"],
        "Region"
    );
    // ---- DC12: the residency fence is hard. A range member moved to the r4
    // host (outside the residency) is refused by name before anything moves.
    let ranges = journey.admin(
        &founder,
        "ranges",
        &["session"],
        &[
            "cluster",
            "replicas",
            "ranges",
            "--session",
            &ledger,
            "list",
        ],
    );
    let member = ranges["result"]["ranges"]["members"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (code, report) = journey.failure(
        &founder,
        None,
        "residency fence",
        &["session", "member", "node outside residency"],
        &[
            "cluster",
            "replicas",
            "ranges",
            "--session",
            &ledger,
            "move",
            "--member",
            &member,
            "--node",
            &node_d.to_string(),
        ],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[outside_residency]"), "{report}");
    assert!(report.contains("r4"), "{report}");
    // The region survival still holds and both claims read.
    assert_eq!(
        guarantee(&placement(&founder).unwrap(), &ledger),
        (Some(1), 0)
    );
    assert_eq!(claim_id(&read_claim(&founder, &before.claim)), before.claim);
    assert_eq!(claim_id(&read_claim(&founder, &during)), during);
    journey.assert_redacted(&[&token]);
    journey.not_executed(
        "measured cross-region RTT under real latency: the fleet is one host, so the RTT gauge is present and near zero; a real trial qualifies the adapter",
    );
    journey.finish();
}
