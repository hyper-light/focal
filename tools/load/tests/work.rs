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
    // A run of one ledger names it, its row the run's totals.
    let rows = report["ledgers"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{report}");
    assert_eq!(rows[0]["callers"], 4);
    assert_eq!(rows[0]["agents"], 16);
    for field in ["started", "written", "judged"] {
        assert_eq!(rows[0][field], 32, "{field}: {report}");
        assert_eq!(report["totals"][field], 32, "{field}: {report}");
    }
}

/// A run of `work` over two ledgers, each an embedded node of its own: its callers, its agents
/// and its share of the lifecycles divided between them, each ledger's lifecycles driven among
/// its own agents through its own inboxes and counted apart, the report's totals their sums.
/// Thirty-three claims divide seventeen and sixteen, so each row's counts are its own ledger's.
#[test]
fn work_over_two_ledgers_on_embedded_nodes_is_driven_and_counted_apart() {
    let directory = tempfile::tempdir().unwrap();
    let shape = directory.path().join("shape.yaml");
    let report = directory.path().join("report.json");
    std::fs::write(
        &shape,
        "claims: 33\ntransport: embedded\nprofile: authored_v1\nlifecycle: work\n\
         concurrency: 4\ninflight: 4\nrate: 60\nagents: 8\nledgers: [{}, {}]\n",
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
    let rows = report["ledgers"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{report}");
    // Two nodes, two ledgers: each named, neither the other.
    for row in rows {
        for part in ["tenant", "session"] {
            let id = row["ledger"][part].as_str().unwrap();
            assert!(
                id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()),
                "{row}"
            );
        }
    }
    assert_ne!(rows[0]["ledger"], rows[1]["ledger"]);
    for (row, lifecycles) in rows.iter().zip([17u64, 16]) {
        assert_eq!(row["callers"], 2, "{row}");
        assert_eq!(row["agents"], 4, "{row}");
        assert_eq!(row["started"], lifecycles, "{row}");
        assert_eq!(row["written"], lifecycles, "{row}");
        assert_eq!(row["judged"], lifecycles, "{row}");
        for ended in ["refused", "unknown", "abandoned", "undelivered"] {
            assert_eq!(row[ended], 0, "{ended}: {row}");
        }
        let refused: u64 = row["refused_by_cause"]
            .as_object()
            .unwrap()
            .values()
            .map(|count| count.as_u64().unwrap())
            .sum();
        assert_eq!(refused, 0, "{row}");
        assert_eq!(
            row["committed"],
            lifecycles * 11,
            "eleven writes a claim: {row}"
        );
        // Each hand-off found in the ledger's own agents' inboxes: the post, the testament, the
        // evaluation due and the settlement, four a claim at least.
        assert!(
            row["tail_events"].as_u64().unwrap() >= 4 * lifecycles,
            "{row}"
        );
        assert_eq!(row["tail_failures"], 0, "{row}");
        assert_eq!(row["refusals"], Value::Array(Vec::new()), "{row}");
    }
    // The totals are the rows' sums, and the run's own counts are the totals'.
    let totals = &report["totals"];
    for field in [
        "started",
        "written",
        "judged",
        "refused",
        "unknown",
        "abandoned",
        "undelivered",
        "committed",
    ] {
        let sum: u64 = rows.iter().map(|row| row[field].as_u64().unwrap()).sum();
        assert_eq!(totals[field], sum, "{field}: {report}");
    }
    for (cause, total) in totals["refused_by_cause"].as_object().unwrap() {
        let sum: u64 = rows
            .iter()
            .map(|row| row["refused_by_cause"][cause].as_u64().unwrap())
            .sum();
        assert_eq!(*total, sum, "{cause}");
    }
    assert_eq!(totals["started"], 33);
    assert_eq!(report["committed"], totals["committed"]);
    assert_eq!(report["lifecycles"], totals["written"]);
    assert_eq!(report["abandoned"], totals["abandoned"]);
    assert_eq!(report["unknown"], 0);
    let work = &report["deliver"];
    assert_eq!(work["delivered"], totals["judged"], "{work}");
    assert_eq!(work["undelivered"], 0, "{work}");
    assert_eq!(work["agents"], 8);
    let events: u64 = rows
        .iter()
        .map(|row| row["tail_events"].as_u64().unwrap())
        .sum();
    assert_eq!(work["agent_events"], events, "{work}");
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
