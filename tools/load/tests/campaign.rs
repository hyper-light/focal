// Nightly seeded campaign (R11 §4). Ignored by default; the nightly workflow
// runs it with `--ignored` and a seed range. Measurement/test code, so it
// allows the production no-panic lints.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Runs the workload generator across a range of seeds and asserts every
//! end-to-end run commits its whole workload with nothing refused or left
//! unknown — i.e. the full client→node→consensus→commit path stays bounded and
//! loss-free under repeated, reproducible load. Seeds and size come from the
//! environment so the nightly can widen the sweep without a code change.

use serde_json::Value;
use std::process::Command;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "nightly campaign; run with --ignored (FOCAL_SEED_START/COUNT, FOCAL_CAMPAIGN_CLAIMS)"]
fn seeded_workloads_commit_without_loss() {
    let start = env_u64("FOCAL_SEED_START", 0);
    let count = env_u64("FOCAL_SEED_COUNT", 4).max(1);
    let claims = env_u64("FOCAL_CAMPAIGN_CLAIMS", 100).max(1);
    let reads = env_u64("FOCAL_CAMPAIGN_READS", 0);

    for seed in start..start.saturating_add(count) {
        let dir = tempfile::tempdir().unwrap();
        let shape_path = dir.path().join("shape.yaml");
        let report_path = dir.path().join("report.json");
        std::fs::write(
            &shape_path,
            format!("claims: {claims}\nseed: {seed}\nreads: {reads}\n"),
        )
        .unwrap();

        let status = Command::new(env!("CARGO_BIN_EXE_focal-load"))
            .args([
                "--shape",
                shape_path.to_str().unwrap(),
                "--out",
                report_path.to_str().unwrap(),
            ])
            .status()
            .unwrap();
        assert!(status.success(), "focal-load failed for seed {seed}");

        let report: Value = serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
        assert_eq!(
            report["committed"].as_u64(),
            Some(claims),
            "seed {seed}: {report}"
        );
        assert_eq!(report["refused"].as_u64(), Some(0), "seed {seed}: {report}");
        assert_eq!(report["unknown"].as_u64(), Some(0), "seed {seed}: {report}");
    }
}
