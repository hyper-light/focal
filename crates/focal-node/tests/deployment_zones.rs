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
//! Stage 4, availability zones (08 §6; DC09): nodes announce a zone as a
//! topology fact, a zone-survival policy places one voter in each of three
//! zones, losing a zone leaves the promised writes readable and the
//! guarantee visibly degraded until the zone returns, and a policy the
//! declared zones cannot provide (region survival with one region, or a
//! node that announced no zone) is refused rather than placed. The zone
//! stage adds exactly one operator concept over the fleet: the zone fact.
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
fn labeled(view: &Value, id: u64, region: &str, zone: &str) -> bool {
    node_row(view, id)
        .is_some_and(|row| row["alive"] == true && row["region"] == region && row["zone"] == zone)
}

#[test]
fn the_zone_stage_adds_a_zone_fact_and_survives_a_zone_loss() {
    let mut journey = Journey::building_on("zones", "deployment_zones", &["laptop", "fleet"]);
    // Three hosts, one per zone of region ra; the founder keeps every copy
    // in ra with a residency fence.
    let founder = Node::with_config(
        "founder",
        "version: 1\ntopology:\n  region: ra\n  zone: a1\nplacement:\n  residency: [ra]\n",
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
    journey.admin(
        &founder,
        "native engine",
        &["data directory"],
        &["cluster", "replicas", "activate-native"],
    );
    let _founder_server = journey.start(
        &founder,
        "start",
        &["data directory", "address", "zone fact"],
        &["--advertise", &addresses[0]],
    );
    let (founder_node, tenant, ledger) = identity(&founder);
    let demo = journey.enroll(&founder, "alice");
    let token = Journey::client_token(&demo);
    let (_server_b, node_b) = journey.join(&founder, &host_b, "host-b", &addresses[1]);
    let (server_c, node_c) = journey.join(&founder, &host_c, "host-c", &addresses[2]);
    let all = [founder_node, node_b, node_c];
    // Every node is granted with the zone it announced.
    journey.admin(&founder, "placement view", &[], &["cluster", "placement"]);
    wait_for(
        &founder,
        "declared zones",
        Duration::from_secs(120),
        |view| {
            labeled(view, founder_node, "ra", "a1")
                && labeled(view, node_b, "ra", "a2")
                && labeled(view, node_c, "ra", "a3")
                && settled(view, &ledger, &all, 0)
        },
    );
    let before = journey.demo(&founder, &demo, "before the zone loss");
    // ---- Zone survival: one voter per zone. Planned by hand (the fleet
    // stage showed plan/apply; here the new fact is `--survive zone`).
    let planned = journey.admin(
        &founder,
        "zone survival",
        &["tenant", "session", "survive zone"],
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--survive",
            "zone",
            "--max-failures",
            "1",
        ],
    );
    assert_eq!(planned["result"]["state"], "planned", "{planned}");
    let view = wait_for(
        &founder,
        "zone activation",
        Duration::from_secs(240),
        |view| {
            session_row(view, &ledger).is_some_and(|session| {
                session["pending"].is_null()
                    && session["achieved_survive"] == "Zone"
                    && session["achieved_max_failures"] == 1
            })
        },
    );
    let voters = ids(&session_row(&view, &ledger).unwrap()["voters"]);
    assert_eq!(voters.len(), 3, "{view}");
    // ---- DC09: lose zone a3. The promised writes survive, the guarantee
    // degrades visibly, writes continue, and the zone's return restores it.
    // A zone outage is the host going silent (SIGSTOP): kill would drop its
    // disk, and the zone returns with its disk.
    journey.manual("zone loss", &[], &["kill", "-STOP", "<host in a3>"]);
    server_c.pause();
    let view = wait_for(
        &founder,
        "the zone's loss",
        Duration::from_secs(120),
        |view| !node_row(view, node_c).is_some_and(|row| row["alive"] == true),
    );
    assert!(
        guarantee(&view, &ledger).1 > 0,
        "the degraded guarantee is visible: {view}"
    );
    let during = journey.write(&founder, &demo, "while a zone is down");
    assert_eq!(
        claim_id(&journey.read(&founder, "claim", &before.claim)),
        before.claim
    );
    // The zone returns.
    server_c.resume();
    journey.manual("zone return", &[], &["kill", "-CONT", "<host in a3>"]);
    let view = wait_for(
        &founder,
        "the zone rejoins",
        Duration::from_secs(180),
        |view| labeled(view, node_c, "ra", "a3") && guarantee(view, &ledger) == (Some(1), 0),
    );
    assert_eq!(
        session_row(&view, &ledger).unwrap()["achieved_survive"],
        "Zone"
    );
    assert_eq!(claim_id(&journey.read(&founder, "claim", &during)), during);
    // ---- False or missing topology prevents qualification. A region
    // survival policy needs three regions; this fleet has one, so it is
    // refused, not placed.
    let output = journey.run(
        &founder,
        None,
        "region survival",
        &["survive region"],
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
            "--dry-run",
        ],
    );
    if output.status.success() {
        let regional: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_ne!(regional["result"]["state"], "planned", "{regional}");
    } else {
        let report = String::from_utf8_lossy(&output.stderr);
        assert!(
            report.contains("[invalid_input]") || report.contains("[unavailable]"),
            "{report}"
        );
    }
    // The zone survival still holds and the demo still reads.
    assert_eq!(
        guarantee(&placement(&founder).unwrap(), &ledger),
        (Some(1), 0)
    );
    journey.same(&founder, &before);
    journey.assert_redacted(&[&token]);
    journey.not_executed(
        "a host that announced no zone: the directory grants no zone domain to it, so a zone-survival plan never counts it; the cli_zones unit covers the domain grant",
    );
    journey.finish();
}
