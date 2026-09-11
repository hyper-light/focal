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
//! Rolling upgrade at the version fence (08 §11, DC18; 24 §21): every node
//! reports its binary's capability level, the founder raises the fence only
//! once every node reports the level, the workload keeps committing across
//! the activation, and a binary announcing below the fence refuses to serve
//! (an unsupported downgrade is rejected before it opens for write). Built
//! on the fleet stage; it adds the capability level, the upgrade status and
//! the fence activation.
use serde_json::Value;
use std::time::Duration;

#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/journey.rs"]
mod journey;
use fleet::*;
use journey::*;

fn upgrade(node: &Node) -> Value {
    admin(node, &["cluster", "upgrade", "status"])["result"]["upgrade"].clone()
}
fn wait_upgrade(node: &Node, what: &str, cond: impl Fn(&Value) -> bool) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = fleet::run(node, None, &["cluster", "upgrade", "status"]);
        if output.status.success()
            && let Ok(value) = serde_json::from_slice::<Value>(&output.stdout)
        {
            let view = value["result"]["upgrade"].clone();
            if cond(&view) {
                return view;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what} did not happen"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}
fn capability(view: &Value, node: u64) -> u64 {
    view["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["node"] == node)
        .and_then(|entry| entry["capability"].as_u64())
        .unwrap_or(u64::MAX)
}

#[test]
fn the_upgrade_stage_raises_the_fence_and_refuses_a_below_fence_binary() {
    let mut journey = Journey::building_on("upgrade", "deployment_upgrade", &["laptop", "fleet"]);
    let founder = Node::new("founder");
    let host = Node::new("host");
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
        &["data directory", "address"],
        &["--advertise", &addresses[0]],
    );
    let (founder_node, _tenant, ledger) = identity(&founder);
    let demo = journey.enroll(&founder, "alice");
    let token = Journey::client_token(&demo);
    let (host_server, node) = journey.join(&founder, &host, "host", &addresses[1]);
    wait_for(&founder, "two nodes", Duration::from_secs(120), |view| {
        settled(view, &ledger, &[founder_node, node], 0)
    });
    let before = journey.demo(&founder, &demo, "before the upgrade");
    // ---- Both nodes report the binary's level; the fence starts at zero.
    let view = journey.admin(
        &founder,
        "upgrade status",
        &[],
        &["cluster", "upgrade", "status"],
    )["result"]["upgrade"]
        .clone();
    let level = view["binary_level"].as_u64().unwrap();
    assert!(level >= 1, "{view}");
    wait_upgrade(&founder, "both nodes reporting", |v| {
        capability(v, founder_node) == level && capability(v, node) == level
    });
    assert_eq!(upgrade(&founder)["fence_level"], 0);
    // Only the founder raises the fence, and only to a level all support.
    let (code, _) = journey.failure(
        &host,
        None,
        "upgrade activate",
        &["fence level"],
        &[
            "cluster",
            "upgrade",
            "activate",
            "--fence",
            &level.to_string(),
        ],
    );
    assert_eq!(code, 3, "a host cannot raise the fence");
    let (code, report) = journey.failure(
        &founder,
        None,
        "upgrade activate",
        &["fence level"],
        &[
            "cluster",
            "upgrade",
            "activate",
            "--fence",
            &(level + 1).to_string(),
        ],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[members_behind]"), "{report}");
    // ---- The fence rises to the level every node supports, and the demo
    // still reads across the activation (quorum and policy maintained).
    let activated = journey.admin(
        &founder,
        "upgrade activate",
        &["fence level"],
        &[
            "cluster",
            "upgrade",
            "activate",
            "--fence",
            &level.to_string(),
        ],
    );
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    assert_eq!(activated["result"]["upgrade"]["fence_level"], level);
    journey.same(&founder, &before);
    wait_upgrade(&host, "the host sees the fence", |v| {
        v["fence_level"] == level
    });
    // ---- A binary announcing below the fence refuses to serve: an
    // unsupported downgrade is rejected before it opens for write.
    drop(host_server);
    let (code, _) = journey.start_refused(
        &host,
        "capability level",
        &["address", "capability level"],
        &["--advertise", &addresses[2]],
        &[("FOCAL_CAPABILITY_LEVEL", "0")],
    );
    assert_eq!(code, 5, "a below-fence binary refused to serve");
    // ---- Restarted at or above the fence, the host serves again.
    let _host_server = journey.start(
        &host,
        "start",
        &["address"],
        &["--advertise", &addresses[2]],
    );
    wait_for(
        &founder,
        "the host serving under the fence",
        Duration::from_secs(120),
        |view| settled(view, &ledger, &[founder_node, node], 0),
    );
    let after = journey.write(&founder, &demo, "after the upgrade");
    assert_ne!(after, before.claim);
    journey.assert_redacted(&[&token]);
    journey.not_executed(
        "a real newer binary at a higher level: the fence mechanism is exercised with the levels one binary announces; a second binary at a higher capability qualifies a real version step",
    );
    journey.finish();
}
