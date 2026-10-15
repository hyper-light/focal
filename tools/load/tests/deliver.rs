#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! The `deliver` lifecycle end to end on an embedded node: every message's eight writes commit,
//! each side finding the other's step only in the ledger's events, and each message is measured
//! from its place on the schedule to its testament posted and to its claim satisfied.
use serde_json::Value;
use std::{collections::BTreeMap, process::Command};

#[test]
fn every_message_is_delivered_through_the_events_and_measured_end_to_end() {
    let directory = tempfile::tempdir().unwrap();
    let shape = directory.path().join("shape.yaml");
    let report = directory.path().join("report.json");
    let csv = directory.path().join("writes.csv");
    std::fs::write(
        &shape,
        "claims: 12\ntransport: embedded\nprofile: authored_v1\nlifecycle: deliver\n\
         concurrency: 2\ninflight: 4\nrate: 60\npoll_ms: 2\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal-load"))
        .args([
            "--shape",
            shape.to_str().unwrap(),
            "--out",
            report.to_str().unwrap(),
        ])
        .env("FOCAL_LOAD_WRITES_CSV", &csv)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(report["committed"], 12 * 8, "eight writes a message");
    assert_eq!(report["refused"], 0, "{}", report["refusals"]);
    assert_eq!(report["unknown"], 0);
    assert_eq!(report["lifecycles"], 12);
    assert_eq!(report["abandoned"], 0);
    let deliver = &report["deliver"];
    assert_eq!(deliver["delivered"], 12);
    assert_eq!(deliver["undelivered"], 0);
    assert_eq!(deliver["tail_failures"], 0);
    // Each side read the other's steps from the events: at least a claim posted and a testament
    // posted a message.
    assert!(deliver["consumer_events"].as_u64().unwrap() >= 12);
    assert!(deliver["issuer_events"].as_u64().unwrap() >= 12);
    let handled = deliver["handled_latency_ns"]["max"].as_u64().unwrap();
    let satisfied = deliver["satisfied_latency_ns"]["max"].as_u64().unwrap();
    assert!(handled > 0 && handled <= satisfied, "{handled} {satisfied}");

    // Every message's steps, each committed once, in order.
    let text = std::fs::read_to_string(csv).unwrap();
    let mut lines = text.lines();
    let header: Vec<_> = lines.next().unwrap().split(',').collect();
    let position = |name| header.iter().position(|field| *field == name).unwrap();
    let mut steps: BTreeMap<(u64, u64), Vec<(u128, u64)>> = BTreeMap::new();
    for line in lines {
        let row: Vec<_> = line.split(',').collect();
        assert_eq!(row[position("outcome")], "committed");
        let message = (
            row[position("worker")].parse().unwrap(),
            row[position("write")].parse().unwrap(),
        );
        steps.entry(message).or_default().push((
            row[position("finished_ns")].parse().unwrap(),
            row[position("step")].parse().unwrap(),
        ));
    }
    assert_eq!(steps.len(), 12);
    for (message, mut written) in steps {
        written.sort_unstable();
        let order: Vec<u64> = written.iter().map(|(_, step)| *step).collect();
        assert_eq!(order, (0..8).collect::<Vec<_>>(), "{message:?}");
    }
}
