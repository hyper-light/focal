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
//! Operator workflows through the real executable and OS-authenticated socket.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "start"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            text.push_str(&line);
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                break;
            }
        }
    });
    let server = Server(child);
    let ready = receive
        .recv_timeout(Duration::from_secs(20))
        .expect("server readiness");
    assert_eq!(ready["condition"], "Ready");
    server
}
fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{args:?}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stdout)))
}
fn mutation(root: &Path, args: &[&str], operation: &Path) -> Value {
    let mut args = args.to_vec();
    args.extend([
        "--operation",
        operation.to_str().unwrap(),
        "--format",
        "json",
    ]);
    let result = cli(root, &args);
    assert_eq!(result["stage"], "Completed");
    assert!(result["receipt"].is_object());
    result
}
fn id(value: u128) -> String {
    format!("{value:032x}")
}
fn validation(value: u128) -> Value {
    json!({"id":id(value),"kind":"receipt","phase":"whole_work","mode":"required",
        "description":"Review evidence","evaluator":"self"})
}
fn claim(value: u128) -> Value {
    json!({"id":id(value),"occurrence":id(value+1),"description":"Manual durable claim",
        "target":"self","action":"handoff","scopes":[{"kind":"file","key":"manual.txt"}],
        "validations":[validation(value+2)]})
}
fn sequence(root: &Path) -> Value {
    cli(root, &["status"])["result"]["Read"]["token"]["sequence"].clone()
}
fn list(root: &Path, family: &str) -> Value {
    cli(root, &["list", family, "--format", "json"])
}

#[test]
fn remote_recovery_reads_are_authenticated_preserve_journals_and_survive_restart() {
    use focal_client::pending::{OperationContext, OperationJournal};
    use focal_model::{RequestEpoch, RequestId};
    use focal_wire::{Operation, RequestEnvelope};
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let server = start(root.path());
    let before = sequence(root.path());
    let empty = cli(root.path(), &["request", "epoch", "--format", "json"]);
    assert_eq!(
        empty["reply"]["page"]["result"]["Epoch"]["minimum"],
        Value::Null
    );
    let absent = cli(
        root.path(),
        &[
            "request",
            "status",
            "--request-id",
            &id(555),
            "--format",
            "json",
        ],
    );
    assert_eq!(
        absent["reply"]["page"]["result"]["Receipt"]["resolution"],
        "Unknown"
    );
    assert_eq!(sequence(root.path()), before);
    for args in [
        vec!["request", "epoch", "--epoch", "0"],
        vec!["request", "status", "--request-id", "bad"],
    ] {
        assert_eq!(run(root.path(), &args).status.code(), Some(2));
    }
    let path = root.path().join("submitted");
    let result = mutation(
        root.path(),
        &["submit", "claim", "--json", &claim(100).to_string()],
        &path,
    );
    let identity = focal_node::embedded::decode_identity(&root.path().join("IDENTITY")).unwrap();
    let context = OperationContext {
        cluster: identity.cluster,
        ledger: identity.ledger,
        principal: identity.issuer,
    };
    let journal = OperationJournal::open(&path, &context).unwrap();
    let business = journal.business_request().unwrap().clone();
    let open = RequestEnvelope {
        request_id: RequestId::from_u128(999),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
        ..business.clone()
    };
    // This journal has the exact business request whose replies were lost. Its
    // first local step is epoch admission, so inspection must ignore next_request.
    let lost_path = root.path().join("lost-replies");
    drop(OperationJournal::create(&lost_path, context, open, business.clone()).unwrap());
    drop(journal);
    let committed_sequence = sequence(root.path());
    for operation in [&path, &lost_path] {
        let saved = std::fs::read(operation.join("state.bin")).unwrap();
        let inspected = cli(
            root.path(),
            &[
                "request",
                "inspect",
                operation.to_str().unwrap(),
                "--remote",
                "--format",
                "json",
            ],
        );
        assert_eq!(
            inspected["reply"]["page"]["result"]["Receipt"]["resolution"]["Committed"],
            result["receipt"]
        );
        assert_eq!(std::fs::read(operation.join("state.bin")).unwrap(), saved);
    }
    assert_eq!(sequence(root.path()), committed_sequence);
    drop(server);
    let _restarted = start(root.path());
    let inspected = cli(
        root.path(),
        &[
            "request",
            "status",
            "--request-id",
            &business.request_id.to_string(),
            "--format",
            "json",
        ],
    );
    assert_eq!(
        inspected["reply"]["page"]["result"]["Receipt"]["resolution"]["Committed"],
        result["receipt"]
    );
    let local = cli(
        root.path(),
        &[
            "request",
            "inspect",
            lost_path.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(local["stage"], "OpenEpoch");
}

#[test]
fn authored_forms_lifecycle_lists_download_and_exact_journal_retry_survive_restart() {
    let root = tempfile::Builder::new()
        .prefix("focal-manual-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = start(root.path());
    let identity = cli(root.path(), &["identity"]);
    let claim_id = id(100);
    let occurrence = id(101);
    let validation_id = id(102);
    let validation_json = validation(102).to_string();
    let flags_operation = root.path().join("flags-operation");
    let flags = mutation(
        root.path(),
        &[
            "submit",
            "claim",
            "--id",
            &claim_id,
            "--occurrence",
            &occurrence,
            "--description",
            "Manual durable claim",
            "--target",
            "self",
            "--action",
            "handoff",
            "--scope",
            "file:manual.txt",
            "--validation-json",
            &validation_json,
        ],
        &flags_operation,
    );
    let generated = cli(
        root.path(),
        &["get", "claim", &claim_id, "--format", "json"],
    );
    assert_eq!(generated["result"]["id"], claim_id);
    let json_input = claim(100).to_string();
    let from_json = mutation(
        root.path(),
        &["submit", "claim", "--json", &json_input],
        &root.path().join("json-operation"),
    );
    let yaml = format!(
        "id: '{claim_id}'\noccurrence: '{occurrence}'\ndescription: Manual durable claim\ntarget: self\naction: handoff\nscopes:\n  - kind: file\n    key: manual.txt\nvalidations:\n  - id: '{validation_id}'\n    kind: receipt\n    phase: whole_work\n    mode: required\n    description: Review evidence\n    evaluator: self\n"
    );
    let from_yaml = mutation(
        root.path(),
        &["submit", "claim", "--yaml", &yaml],
        &root.path().join("yaml-operation"),
    );
    assert_eq!(
        flags["receipt"]["command_hash"],
        from_json["receipt"]["command_hash"]
    );
    assert_eq!(
        flags["receipt"]["command_hash"],
        from_yaml["receipt"]["command_hash"]
    );
    assert_eq!(flags["result"]["claims"], json!([claim_id]));
    assert_eq!(from_json["result"]["claims"], json!([claim_id]));
    assert_eq!(
        list(root.path(), "claims")["results"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        cli(
            root.path(),
            &["get", "claim", "--source", "self", "--format", "json"]
        )["result"]["id"],
        claim_id
    );
    assert_eq!(
        list(root.path(), "validations")["results"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        list(root.path(), "testaments")["results"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        list(root.path(), "artifacts")["results"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        cli(
            root.path(),
            &["get", "validation", &validation_id, "--format", "json"]
        )["result"]["id"],
        validation_id
    );

    mutation(
        root.path(),
        &["claim", "post", &claim_id],
        &root.path().join("post-operation"),
    );
    let receipt_id = id(104);
    let acquired = mutation(
        root.path(),
        &["receipt", "acquire", &claim_id, "--id", &receipt_id],
        &root.path().join("receipt-operation"),
    );
    assert_eq!(acquired["result"]["receipt"], receipt_id);
    assert_eq!(acquired["result"]["receipt_epoch"], 1);
    let evidence_id = id(105);
    let begun = mutation(
        root.path(),
        &[
            "evidence",
            "begin",
            "--claim",
            &claim_id,
            "--receipt",
            &receipt_id,
            "--receipt-epoch",
            "1",
            "--id",
            &evidence_id,
        ],
        &root.path().join("evidence-operation"),
    );
    assert_eq!(begun["result"]["evidence_set"], evidence_id);
    let artifact_id = id(106);
    let schema = focal_evidence::test_report_schema().to_string();
    let payload = br#"{"passed":1,"failed":0,"skipped":0}"#;
    let payload_file = root.path().join("report.json");
    std::fs::write(&payload_file, payload).unwrap();
    let artifact = mutation(
        root.path(),
        &[
            "submit",
            "artifact",
            "--id",
            &artifact_id,
            "--claim",
            &claim_id,
            "--receipt",
            &receipt_id,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            &evidence_id,
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--payload-file",
            payload_file.to_str().unwrap(),
        ],
        &root.path().join("artifact-operation"),
    );
    let manifest = format!(
        "{artifact_id}:{}",
        artifact["result"]["hash"].as_str().unwrap()
    );
    let testament_id = id(107);
    let testament = mutation(
        root.path(),
        &[
            "submit",
            "testament",
            "--id",
            &testament_id,
            "--claim",
            &claim_id,
            "--receipt",
            &receipt_id,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            &evidence_id,
            "--artifact",
            &manifest,
            "--summary",
            "One passing test",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
        ],
        &root.path().join("testament-operation"),
    );
    assert_eq!(testament["result"]["testament"], testament_id);
    for (kind, object) in [
        ("claim", &claim_id),
        ("testament", &testament_id),
        ("artifact", &artifact_id),
        ("validation", &validation_id),
    ] {
        assert_eq!(
            cli(root.path(), &["get", kind, object, "--format", "json"])["result"]["id"],
            *object
        );
    }
    assert_eq!(
        list(root.path(), "testaments")["results"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        list(root.path(), "artifacts")["results"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for family in ["claims", "testaments", "validations", "artifacts"] {
        let page = cli(
            root.path(),
            &["list", family, "--claim", &claim_id, "--format", "json"],
        );
        assert_eq!(page["results"].as_array().unwrap().len(), 1);
    }
    let manifest_page = cli(
        root.path(),
        &[
            "list",
            "artifacts",
            "--testament",
            &testament_id,
            "--producer",
            "self",
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--format",
            "json",
        ],
    );
    assert_eq!(manifest_page["results"].as_array().unwrap().len(), 1);
    assert_eq!(manifest_page["results"][0]["id"], artifact_id);
    let validation_page = cli(
        root.path(),
        &[
            "list",
            "validations",
            "--claim",
            &claim_id,
            "--evaluator",
            "self",
            "--kind",
            "receipt",
            "--phase",
            "whole_work",
            "--mode",
            "required",
            "--format",
            "json",
        ],
    );
    assert_eq!(validation_page["results"].as_array().unwrap().len(), 1);
    let downloaded = root.path().join("downloaded.json");
    cli(
        root.path(),
        &[
            "get",
            "artifact",
            &artifact_id,
            "--output",
            downloaded.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(std::fs::read(&downloaded).unwrap(), payload);
    assert!(
        !run(
            root.path(),
            &[
                "get",
                "artifact",
                &artifact_id,
                "--output",
                downloaded.to_str().unwrap()
            ]
        )
        .status
        .success()
    );
    assert_eq!(std::fs::read(&downloaded).unwrap(), payload);

    let second = claim(200).to_string();
    mutation(
        root.path(),
        &["submit", "claim", "--json", &second],
        &root.path().join("second-operation"),
    );
    let second_claim = id(200);
    let second_receipt = id(204);
    let second_set = id(205);
    mutation(
        root.path(),
        &["claim", "post", &second_claim],
        &root.path().join("second-post"),
    );
    mutation(
        root.path(),
        &["receipt", "acquire", &second_claim, "--id", &second_receipt],
        &root.path().join("second-receipt"),
    );
    mutation(
        root.path(),
        &[
            "evidence",
            "begin",
            "--claim",
            &second_claim,
            "--receipt",
            &second_receipt,
            "--receipt-epoch",
            "1",
            "--id",
            &second_set,
        ],
        &root.path().join("second-evidence"),
    );
    let related = json!({"id":id(206),"claim":second_claim,
        "receipt":{"id":second_receipt,"epoch":1},"evidence_set":second_set,
        "kind":"test-report","schema_hash":schema,
        "payload":{"type":"text","text":std::str::from_utf8(payload).unwrap()},
        "inputs":[{"kind":"claim","id":claim_id}]})
    .to_string();
    mutation(
        root.path(),
        &["submit", "artifact", "--json", &related],
        &root.path().join("related-artifact"),
    );
    assert_eq!(
        list(root.path(), "artifacts")["results"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let manifest_only = cli(
        root.path(),
        &[
            "list",
            "artifacts",
            "--testament",
            &testament_id,
            "--format",
            "json",
        ],
    );
    assert_eq!(manifest_only["results"].as_array().unwrap().len(), 1);
    assert_eq!(manifest_only["results"][0]["id"], artifact_id);
    let claim_evidence = cli(
        root.path(),
        &[
            "list",
            "artifacts",
            "--claim",
            &claim_id,
            "--format",
            "json",
        ],
    );
    assert_eq!(claim_evidence["results"].as_array().unwrap().len(), 1);
    let ambiguous = run(
        root.path(),
        &["get", "claim", "--source", "self", "--format", "json"],
    );
    assert_eq!(ambiguous.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&ambiguous.stderr).contains("more than one claim"));
    let page = cli(
        root.path(),
        &[
            "list", "claims", "--source", "self", "--limit", "1", "--format", "json",
        ],
    );
    assert_eq!(page["results"].as_array().unwrap().len(), 1);
    let continuation = cli(
        root.path(),
        &[
            "list",
            "claims",
            "--source",
            "self",
            "--limit",
            "1",
            "--cursor",
            page["cursor"].as_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(continuation["token"], page["token"]);
    assert_eq!(continuation["results"].as_array().unwrap().len(), 1);
    assert_ne!(continuation["results"][0]["id"], page["results"][0]["id"]);
    assert!(continuation["cursor"].is_null());
    let before = sequence(root.path());
    let inspected = cli(
        root.path(),
        &[
            "request",
            "inspect",
            flags_operation.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(inspected, flags);
    drop(server); // no graceful owner checkpoint; all acknowledged facts are WAL-durable
    let _server = start(root.path());
    assert_eq!(cli(root.path(), &["identity"]), identity);
    let retried = cli(
        root.path(),
        &[
            "request",
            "retry",
            flags_operation.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(retried, flags);
    assert_eq!(sequence(root.path()), before);
    assert_eq!(
        cli(
            root.path(),
            &["get", "testament", &testament_id, "--format", "json"]
        )["result"]["id"],
        testament_id
    );
}

#[test]
fn parse_failures_and_conflicting_inputs_create_no_operation_journal() {
    let root = tempfile::Builder::new()
        .prefix("focal-parse-")
        .tempdir_in("/tmp")
        .unwrap();
    let _server = start(root.path());
    let before = sequence(root.path());
    for (name, args) in [
        ("invalid-json", vec!["submit", "claim", "--json", "{"]),
        (
            "mixed",
            vec![
                "submit",
                "claim",
                "--json",
                "{}",
                "--description",
                "ignored",
            ],
        ),
        (
            "unknown-field",
            vec![
                "submit",
                "claim",
                "--json",
                r#"{"description":"invalid","target":"self","validations":[],"runtime":true}"#,
            ],
        ),
        ("invalid-id", vec!["claim", "post", "not-a-typed-id"]),
    ] {
        let operation = root.path().join(name);
        let mut args = args;
        args.extend([
            "--operation",
            operation.to_str().unwrap(),
            "--format",
            "json",
        ]);
        let result = run(root.path(), &args);
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!operation.exists());
        assert!(!String::from_utf8_lossy(&result.stderr).contains("operation:"));
    }
    assert!(!root.path().join("client/operations").exists());
    assert_eq!(sequence(root.path()), before);
}

#[test]
fn validation_get_pages_actual_committed_run_and_verdict_after_restart() {
    let root = tempfile::Builder::new()
        .prefix("focal-verdict-")
        .tempdir_in("/tmp")
        .unwrap();
    cli(root.path(), &["demo"]);
    let server = start(root.path());
    let definitions = cli(
        root.path(),
        &["list", "validations", "--kind", "test", "--format", "json"],
    );
    assert_eq!(definitions["results"].as_array().unwrap().len(), 1);
    let validation = definitions["results"][0]["id"].as_str().unwrap();
    let first = cli(
        root.path(),
        &[
            "get",
            "validation",
            validation,
            "--limit",
            "1",
            "--format",
            "json",
        ],
    );
    let records = &first["result"]["object"]["ValidationResults"]["records"];
    assert_eq!(records.as_array().unwrap().len(), 1);
    assert_eq!(records[0]["value"]["Run"]["attempt_count"], 1);
    assert_eq!(
        records[0]["value"]["Run"]["final_verdict"],
        serde_json::to_value(focal_model::VerdictValue::Pass).unwrap()
    );
    let cursor = first["cursor"].as_str().unwrap();
    let second = cli(
        root.path(),
        &[
            "get",
            "validation",
            validation,
            "--limit",
            "1",
            "--cursor",
            cursor,
            "--format",
            "json",
        ],
    );
    assert_eq!(second["token"], first["token"]);
    assert!(second["cursor"].is_null());
    let attempts = &second["result"]["object"]["ValidationResults"]["records"];
    assert_eq!(attempts.as_array().unwrap().len(), 1);
    assert_eq!(
        attempts[0]["value"]["Attempt"]["value"],
        serde_json::to_value(focal_model::VerdictValue::Pass).unwrap()
    );
    assert_eq!(
        attempts[0]["value"]["Attempt"]["evidence"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    drop(server);
    let _server = start(root.path());
    let recovered = cli(
        root.path(),
        &["get", "validation", validation, "--format", "json"],
    );
    assert_eq!(recovered["token"], first["token"]);
    let recovered = &recovered["result"]["object"]["ValidationResults"]["records"];
    assert_eq!(
        recovered.as_array().unwrap(),
        &vec![records[0].clone(), attempts[0].clone()]
    );
}
