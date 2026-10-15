#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Measurement boundaries over the real client, node and commit path.
use serde_json::Value;
use std::{collections::BTreeSet, process::Command};

#[test]
fn paced_trace_and_success_statistics_cover_only_the_measured_requests() {
    let directory = tempfile::tempdir().unwrap();
    let shape = directory.path().join("shape.yaml");
    let report = directory.path().join("report.json");
    let csv = directory.path().join("writes.csv");
    std::fs::write(
        &shape,
        "claims: 32\nconcurrency: 2\nrate: 200\nwarmup_ms: 40\nreads: 4\n",
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
    assert_eq!(report["committed"], 32, "whole-phase count retains warm-up");
    assert_eq!(report["read_hits"], 4, "the read phase still serves");
    let measured = &report["measured_writes"];
    assert_eq!(
        measured["committed"], 24,
        "eight scheduled warm-up writes excluded"
    );
    assert_eq!(measured["refused"], 0);
    assert_eq!(measured["unknown"], 0);
    assert_eq!(measured["expired"], 0);
    assert_eq!(measured["committed_latency_ns"], report["latency_ns"]);

    let text = std::fs::read_to_string(csv).unwrap();
    let mut lines = text.lines();
    let header: Vec<_> = lines.next().unwrap().split(',').collect();
    let position = |name| header.iter().position(|field| *field == name).unwrap();
    let mut identities = BTreeSet::new();
    let mut starts = Vec::new();
    let mut samples = 0;
    for line in lines {
        let row: Vec<_> = line.split(',').collect();
        let number = |name| row[position(name)].parse::<u128>().unwrap();
        let start = number("start_ns");
        let sent = number("sent_ns");
        let finished = number("finished_ns");
        assert!(start <= sent && sent <= finished);
        assert_eq!(number("latency_ns"), finished - start);
        assert!(number("phase_start_ns") >= 40_000_000);
        assert!(number("run_start_epoch_ns") > 0);
        assert_eq!(row[position("outcome")], "committed");
        assert!(identities.insert(row[position("request")].to_string()));
        starts.push(start);
        samples += 1;
    }
    assert_eq!(samples, 24);
    assert_eq!(starts.first(), Some(&0));
    assert!(starts.windows(2).all(|pair| pair[0] <= pair[1]));
}

/// The offered rate is aggregate, including its remainder across callers. Integer division
/// used to label 50/s while scheduling only 48/s, and 250/s while scheduling only 192/s.
#[test]
fn offered_rate_includes_the_remainder_across_callers() {
    assert_offered_schedule(50, 16);
    assert_offered_schedule(250, 64);
    assert_offered_schedule(7, 3);
}

/// A slow aggregate stream can still use many callers. Callers take successive places on the
/// same schedule rather than requiring at least one request per second for every caller.
#[test]
fn offered_rate_can_be_lower_than_the_caller_count() {
    assert_offered_schedule(5, 16);
}

fn assert_offered_schedule(rate: u64, callers: u16) {
    let directory = tempfile::tempdir().unwrap();
    let shape = directory.path().join("shape.yaml");
    let report = directory.path().join("report.json");
    let csv = directory.path().join("writes.csv");
    std::fs::write(
        &shape,
        format!("claims: {rate}\nconcurrency: {callers}\nrate: {rate}\n"),
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
    assert_eq!(report["committed"], rate);
    assert_eq!(report["refused"], 0);
    assert_eq!(report["unknown"], 0);
    let text = std::fs::read_to_string(csv).unwrap();
    let mut lines = text.lines();
    let header: Vec<_> = lines.next().unwrap().split(',').collect();
    let phase = header
        .iter()
        .position(|field| *field == "phase_start_ns")
        .unwrap();
    let mut starts: Vec<u128> = lines
        .map(|line| line.split(',').nth(phase).unwrap().parse().unwrap())
        .collect();
    starts.sort_unstable();
    let expected: Vec<u128> = (0..rate)
        .map(|ordinal| u128::from(ordinal) * 1_000_000_000 / u128::from(rate))
        .collect();
    assert_eq!(starts, expected, "rate={rate}, callers={callers}");
}
