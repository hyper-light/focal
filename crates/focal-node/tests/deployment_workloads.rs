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
//! Saturating and mixed workloads (08 §8, DC14): a sustained workload runs
//! within the node's fixed RAM and disk budgets, the fleet keeps the
//! guarantee it was given, and nothing weakens the policy or spends new
//! resources on its own. Built on the fleet stage; it adds the sustained
//! workload and the budget metrics.
use serde_json::Value;
use std::time::Duration;

#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/journey.rs"]
mod journey;
use fleet::*;
use journey::*;

/// A named gauge's value from the founder's Prometheus text.
fn gauge(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let line = line.trim();
        if line.starts_with(name)
            && (line.as_bytes().get(name.len()) == Some(&b'{')
                || line.as_bytes().get(name.len()) == Some(&b' '))
        {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
        } else {
            None
        }
    })
}
fn metrics(node: &Node) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let output = fleet::run(node, None, &["cluster", "node", "metrics"]);
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout).into_owned();
            if text.contains("focal_memory_used_bytes") {
                return text;
            }
        }
        assert!(std::time::Instant::now() < deadline, "no metrics snapshot");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn a_sustained_workload_stays_within_budget_and_keeps_the_guarantee() {
    let mut journey =
        Journey::building_on("workloads", "deployment_workloads", &["laptop", "fleet"]);
    let founder = Node::new("founder");
    let host_a = Node::new("host-a");
    let host_b = Node::new("host-b");
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
    let (founder_node, tenant, ledger) = identity(&founder);
    let demo = journey.enroll(&founder, "alice");
    let token = Journey::client_token(&demo);
    let (_server_a, node_a) = journey.join(&founder, &host_a, "host-a", &addresses[1]);
    let (_server_b, node_b) = journey.join(&founder, &host_b, "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    plan_and_settle(&founder, &tenant, &ledger, None, 1, &all);
    // ---- DC14: a sustained workload. A run of claims committed through the
    // founder, and one delivered with a stored artifact through the
    // participant's connection (a mixed read/write shape). The completion
    // lane is bounded, so the fleet paces the work rather than exceeding its
    // budget; a claim refused under pressure is the budget holding, not a
    // loss, so the run tolerates a capacity refusal and keeps going.
    let mut committed = 0u32;
    let mut last = String::new();
    for round in 0..16 {
        let document = claim_document(&demo.principal, &format!("sustained round {round}"));
        let output = journey.run(
            &founder,
            None,
            "claim",
            &["claim document"],
            &[
                "submit",
                "claim",
                "--json",
                &document.to_string(),
                "--format",
                "json",
            ],
        );
        if !output.status.success() {
            assert_eq!(
                output.status.code(),
                Some(6),
                "only a capacity refusal is tolerated"
            );
            continue;
        }
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let claim = created(&value["result"], "Claim").remove(0);
        fleet::committed(&journey.cli(&founder, None, "claim", &[], &["claim", "post", &claim]));
        committed += 1;
        last = claim;
    }
    assert!(
        committed >= 8,
        "a substantial run committed: {committed} of 16"
    );
    // One delivered with a stored artifact: the mixed shape.
    let delivered = journey.write(&founder, &demo, "the delivered claim");
    journey.deliver(&founder, &demo, &delivered);
    assert_eq!(claim_id(&read_claim(&founder, &last)), last);
    assert_eq!(claim_id(&read_claim(&founder, &delivered)), delivered);
    // ---- The node stayed within its fixed RAM budget: used never exceeds
    // the limit, and the completion reserve is held back.
    let text = metrics(&founder);
    journey.manual(
        "budget metrics",
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
    let limit = gauge(&text, "focal_memory_limit_bytes").expect("memory limit gauge");
    let used = gauge(&text, "focal_memory_used_bytes").expect("memory used gauge");
    assert!(used <= limit, "memory used {used} within limit {limit}");
    assert!(
        gauge(&text, "focal_disk_free_bytes").is_some()
            || text.contains("focal_memory_completion_reserve_bytes"),
        "the budgets are reported: {text}"
    );
    // ---- The guarantee the fleet was given is unchanged: the sustained
    // workload never weakened the policy or moved the placement on its own.
    let view = placement(&founder).unwrap();
    let session = session_row(&view, &ledger).unwrap();
    assert_eq!(session["max_failures"], 1, "the policy is unchanged");
    assert_eq!(
        session["achieved_max_failures"], 1,
        "the guarantee holds under load"
    );
    assert_eq!(
        session["pending"],
        Value::Null,
        "no placement moved on its own"
    );
    assert_eq!(ids(&session["voters"]).len(), 3);
    journey.assert_redacted(&[&token]);
    journey.not_executed(
        "disk-growth and checkpoint/recovery envelopes under a long mixed load: measured by the R11 workload generator, not this correctness walk",
    );
    journey.finish();
}
