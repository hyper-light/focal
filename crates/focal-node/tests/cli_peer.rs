#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
#![cfg(unix)]
//! A participant invokes a validator outside Focal, then records exact evidence.
use focal_client::validation_context::ValidationContext;
use focal_evidence::{TestReportValidator, Validator};
use focal_model::{ClaimStatus, ValidationResultValue, VerdictValue};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::Duration,
};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(root: &Path) -> Server {
    let mut process = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .arg("start")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = process.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            text.push_str(&line);
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                break;
            }
        }
    });
    let server = Server(process);
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap()["condition"],
        "Ready"
    );
    server
}
fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let result = run(root, &args);
    assert!(
        result.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        result.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
fn context(root: &Path, id: &str) -> ValidationContext {
    serde_json::from_value(cli(root, &["get", "validation", id, "--context"])["context"].clone())
        .unwrap()
}
fn id(value: u128) -> String {
    format!("{value:032x}")
}

#[test]
fn actual_cli_external_verdict_completes_after_pinned_evidence_and_preserves_history() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let claim_id = id(100);
    let validation_id = id(103);
    let handler_id = id(104);
    let version = blake3::hash(b"cli participant test-report contract v1")
        .to_hex()
        .to_string();
    let schema = focal_evidence::test_report_schema().to_string();
    let handler = json!({"id":handler_id,"version":version,"agentic":false});
    let claim = json!({"id":claim_id,"occurrence":id(101),"target":"self","action":"handoff",
    "description":"Deliver verified report","validations":[
        {"id":id(102),"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive response","evaluator":"self"},
        {"id":validation_id,"kind":"test","phase":"whole_work","mode":"required","description":"Report has no failed tests","evaluator":"self","handlers":[handler],"evidence_schemas":[schema]}
    ]});
    cli(
        root.path(),
        &["submit", "claim", "--json", &claim.to_string()],
    );
    let contracts = cli(root.path(), &["validator", "list", "--claim", &claim_id]);
    assert_eq!(contracts["execution"], "participant_owned");
    assert_eq!(contracts["page"]["objects"].as_array().unwrap().len(), 1);
    let inspected = cli(
        root.path(),
        &[
            "validator",
            "get",
            &handler_id,
            "--version",
            &version,
            "--schema-hash",
            &schema,
        ],
    );
    assert_eq!(inspected["page"]["objects"], contracts["page"]["objects"]);
    let absent = cli(
        root.path(),
        &[
            "validator",
            "get",
            &handler_id,
            "--version",
            &"44".repeat(32),
        ],
    );
    assert!(absent["page"]["objects"].as_array().unwrap().is_empty());
    let graph_root = format!("claim:{claim_id}");
    let graph_args = [
        "ledger",
        "traverse",
        &graph_root,
        "--edge",
        "requirement",
        "--depth",
        "1",
        "--limit",
        "1",
    ];
    let first = cli(root.path(), &graph_args);
    assert_eq!(first["stop"], "PageLimit");
    assert_eq!(first["results"].as_array().unwrap().len(), 1);
    cli(root.path(), &["claim", "post", &claim_id]);
    let mut graph_cursor = first["cursor"].as_str().unwrap().to_owned();
    let mut graph_count = 1;
    for _ in 0..8 {
        let mut args = graph_args.to_vec();
        args.extend(["--cursor", &graph_cursor]);
        let next = cli(root.path(), &args);
        assert_eq!(
            cli(root.path(), &args),
            next,
            "an exact graph cursor is repeatable"
        );
        assert_eq!(next["token"], first["token"]);
        graph_count += next["results"].as_array().unwrap().len();
        match next["cursor"].as_str() {
            Some(cursor) => graph_cursor = cursor.to_owned(),
            None => {
                assert_eq!(next["stop"], "Complete");
                break;
            }
        }
    }
    assert_eq!(graph_count, 3);
    let received = cli(root.path(), &["receipt", "acquire", &claim_id]);
    let receipt = received["result"]["receipt"].as_str().unwrap();
    let opened = cli(
        root.path(),
        &[
            "evidence",
            "begin",
            "--claim",
            &claim_id,
            "--receipt",
            receipt,
            "--receipt-epoch",
            "1",
        ],
    );
    let evidence_set = opened["result"]["evidence_set"].as_str().unwrap();
    let report = r#"{"passed":3,"failed":0,"skipped":0}"#;
    let artifact = cli(
        root.path(),
        &[
            "submit",
            "artifact",
            "--claim",
            &claim_id,
            "--receipt",
            receipt,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            evidence_set,
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--text",
            report,
        ],
    );
    let reference = format!(
        "{}:{}",
        artifact["result"]["artifact"].as_str().unwrap(),
        artifact["result"]["hash"].as_str().unwrap()
    );
    let closed = cli(
        root.path(),
        &[
            "submit",
            "testament",
            "--claim",
            &claim_id,
            "--receipt",
            receipt,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            evidence_set,
            "--artifact",
            &reference,
            "--summary",
            "Report produced",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
        ],
    );
    let testament = closed["result"]["testament"].as_str().unwrap();
    cli(
        root.path(),
        &["testament", "receive", testament, "--claim", &claim_id],
    );
    cli(root.path(), &["validation", "begin", "--claim", &claim_id]);
    let before = context(root.path(), &validation_id);
    assert_eq!(before.claim.lifecycle().status, ClaimStatus::Validating);
    let run = before
        .records
        .iter()
        .find_map(|record| match &record.value {
            ValidationResultValue::Run(run) => Some(run),
            _ => None,
        })
        .unwrap();
    assert_eq!(run.final_verdict, None); // Beginning did not execute the external validator.
    assert_eq!(run.attempt_count, 0);

    // This call runs in the test participant process, never in the Focal daemon.
    let evaluation = TestReportValidator
        .evaluate(report.as_bytes(), None)
        .unwrap();
    assert_eq!(evaluation.value, VerdictValue::Pass);
    let proof = cli(
        root.path(),
        &[
            "artifact",
            "register",
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--text",
            report,
        ],
    );
    let proof_ref = format!(
        "{}:{}",
        proof["result"]["artifact"].as_str().unwrap(),
        proof["result"]["hash"].as_str().unwrap()
    );
    let target_hash = run.id.target_hash.to_string();
    let manifest = run.manifest.to_string();
    let epoch = run.id.epoch.to_string();
    cli(
        root.path(),
        &[
            "submit",
            "validation",
            "--validation",
            &validation_id,
            "--target-hash",
            &target_hash,
            "--phase",
            "whole_work",
            "--epoch",
            &epoch,
            "--handler",
            &handler_id,
            "--handler-version",
            &version,
            "--attempt",
            "0",
            "--manifest",
            &manifest,
            "--receipt",
            receipt,
            "--receipt-epoch",
            "1",
            "--value",
            "pass",
            "--evidence",
            &proof_ref,
        ],
    );
    cli(
        root.path(),
        &["validation", "complete", "--claim", &claim_id],
    );
    let completed = context(root.path(), &validation_id);
    assert_eq!(completed.claim.lifecycle().status, ClaimStatus::Satisfied);
    assert!(completed.records.iter().any(|record| matches!(&record.value,ValidationResultValue::Attempt(value) if value.value==VerdictValue::Pass)));
    assert_eq!(
        completed
            .testament
            .as_ref()
            .unwrap()
            .value
            .content()
            .artifacts,
        before.testament.as_ref().unwrap().value.content().artifacts
    );

    let successor = json!({"id":id(110),"occurrence":id(111),"target":"self","action":"handoff","description":"Follow-up report",
        "validations":[{"id":id(112),"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive follow-up","evaluator":"self"}]});
    cli(
        root.path(),
        &[
            "claim",
            "supersede",
            &claim_id,
            "--json",
            &successor.to_string(),
        ],
    );
    assert_eq!(
        context(root.path(), &validation_id)
            .claim
            .lifecycle()
            .status,
        ClaimStatus::Satisfied
    );
    let make = |base: u128| json!({"id":id(base),"occurrence":id(base+1),"target":"self","action":"handoff","description":"Atomic plan member","validations":[{"id":id(base+2),"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive response","evaluator":"self"}]});
    let mut first = make(500);
    let second = make(510);
    first["relations"] = json!([{"kind":"depends_on","target":id(510)}]);
    let batch = cli(
        root.path(),
        &[
            "submit",
            "claims",
            "--claim-json",
            &first.to_string(),
            "--claim-json",
            &second.to_string(),
        ],
    );
    assert_eq!(batch["result"]["claims"], json!([id(500), id(510)]));
    let mut invalid = make(610);
    invalid["relations"] = json!([{"kind":"depends_on","target":id(9999)}]);
    let rejected = self::run(
        root.path(),
        &[
            "submit",
            "claims",
            "--json",
            &json!({"claims":[make(600),invalid]}).to_string(),
        ],
    );
    assert!(!rejected.status.success());
    assert_eq!(
        self::run(root.path(), &["get", "claim", &id(600)])
            .status
            .code(),
        Some(4),
        "a refused batch cannot publish its first member"
    );
    drop(server);
    let _server = start(root.path());
    let recovered = context(root.path(), &validation_id);
    assert_eq!(recovered.records, completed.records);
    assert_eq!(recovered.claim.lifecycle().status, ClaimStatus::Satisfied);
    assert_eq!(
        cli(root.path(), &["get", "claim", &id(500)])["result"]["id"],
        id(500)
    );
}
