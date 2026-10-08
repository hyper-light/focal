// A session filled to admission's bound must still checkpoint (rule 2 of the
// native session: what it admits it must be able to checkpoint). Ignored by
// default, since a debug build takes minutes to fill a session; the nightly
// runs it by name. Measurement/test code, so it allows the production
// no-panic lints.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! An embedded node is offered more authored claims than one session holds.
//! Admission refuses the excess, typed, and the node then closes, which takes
//! a checkpoint, and reopens from it. A session that admitted what it could
//! not checkpoint failed exactly there: closed at 14,000 offered, its
//! checkpoint was refused ("consensus: entry, read context, or pending
//! proposal capacity exceeded"), and a replicated one stopped on every
//! replica at its next checkpoint (268,771,520 visits past 268,435,456).

use serde_json::Value;
use std::process::Command;

#[test]
#[ignore = "fills a whole session; run with --ignored (the nightly does)"]
fn a_session_filled_to_admissions_bound_checkpoints_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let shape = dir.path().join("shape.yaml");
    let report = dir.path().join("report.json");
    std::fs::write(
        &shape,
        format!(
            "claims: 14000\ntransport: embedded\ndata_dir: {}\nprofile: authored_v1\nconcurrency: 8\nreopen: true\n",
            dir.path().join("node").display()
        ),
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
        "the filled session did not close and reopen: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    let committed = report["committed"].as_u64().unwrap();
    let refused = report["refused"].as_u64().unwrap();
    assert!(refused > 0, "the session was not filled: {report}");
    assert_eq!(
        committed + refused,
        14_000,
        "every claim committed or refused, typed"
    );
    assert_eq!(report["unknown"].as_u64(), Some(0));
    assert_eq!(report["reopen_read_hit"].as_bool(), Some(true), "{report}");
}
