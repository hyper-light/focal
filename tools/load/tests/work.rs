#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! The `work` lifecycle end to end on an embedded node, among agents: every claim's eleven writes
//! commit across its issuer, its worker and its evaluator, three principals of their own, each
//! finding the step before its own only in its inbox — the claim posted, the testament posted, the
//! evaluation due, the claim settled — and every claim ends satisfied by its evaluator's verdict
//! on the artifact its worker submitted and its testament cited (02 §3, §6).
use serde_json::Value;
use std::process::Command;

#[test]
fn every_work_claim_among_agents_is_judged_by_its_evaluator_and_satisfied() {
    let directory = tempfile::tempdir().unwrap();
    let shape = directory.path().join("shape.yaml");
    let report = directory.path().join("report.json");
    std::fs::write(
        &shape,
        "claims: 32\ntransport: embedded\nprofile: authored_v1\nlifecycle: work\n\
         concurrency: 4\ninflight: 4\nrate: 60\nagents: 16\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal-load"))
        .args([
            "--shape",
            shape.to_str().unwrap(),
            "--out",
            report.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(report["refused"], 0, "{}", report["refusals"]);
    assert_eq!(report["unknown"], 0, "{report}");
    assert_eq!(
        report["committed"],
        32 * 11,
        "eleven writes a claim: {report}"
    );
    assert_eq!(report["lifecycles"], 32, "{report}");
    let work = &report["deliver"];
    assert_eq!(work["delivered"], 32, "every claim satisfied: {work}");
    assert_eq!(work["undelivered"], 0, "{work}");
    assert_eq!(work["tail_failures"], 0, "{work}");
    assert_eq!(work["agents"], 16);
    // Four events a claim reach its three agents: the post (worker), the testament (issuer),
    // the evaluation due (evaluator) and the claim settled (issuer), at least.
    assert!(work["agent_events"].as_u64().unwrap() >= 4 * 32, "{work}");
}

/// A run of `work` names three principals at least: an issuer, a worker and an evaluator.
#[test]
fn work_needs_three_agents() {
    let directory = tempfile::tempdir().unwrap();
    let shape = directory.path().join("shape.yaml");
    std::fs::write(
        &shape,
        "claims: 4\ntransport: embedded\nprofile: authored_v1\nlifecycle: work\nagents: 2\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal-load"))
        .args([
            "--shape",
            shape.to_str().unwrap(),
            "--out",
            directory.path().join("report.json").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("3 at least"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
