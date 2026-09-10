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
//! The second product gate on the native engine through the MCP adapter:
//! failed respondent work and an evaluator that fails to execute remain
//! distinct, inspectable evidence through two `mcp serve` processes, across
//! an adapter lost before its reply was consumed and a killed and restarted
//! node (REMAINING §9 A2).
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, ChildStdin, Command, Output, Stdio},
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
fn private(path: &Path) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn start(root: &Path, advertise: &str) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args([
            "--data-dir",
            root.to_str().unwrap(),
            "start",
            "--advertise",
            advertise,
        ])
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
    let status = receive
        .recv_timeout(Duration::from_secs(25))
        .expect("server did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    server
}
fn run(root: &Path, context: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap()]);
    if let Some(context) = context {
        command.args(["--client-context", context]);
    }
    command.args(args).output().unwrap()
}
fn admin(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let output = run(root, context, args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{args:?}: {error}: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

/// One `mcp serve` process on the modern profile for one participant.
struct Mcp {
    _process: Server,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    next: u64,
}
impl Mcp {
    fn open(root: &Path, context: Option<&str>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
        command.args(["--data-dir", root.to_str().unwrap()]);
        if let Some(context) = context {
            command.args(["--client-context", context]);
        }
        let mut child = command
            .args(["mcp", "serve"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if send.send(serde_json::from_str(&line).unwrap()).is_err() {
                    break;
                }
            }
        });
        Self {
            _process: Server(child),
            input,
            output,
            next: 1,
        }
    }
    fn rpc(&mut self, method: &str, mut params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}});
        let input = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        serde_json::to_writer(&mut self.input, &input).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        let result = self.output.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(result["id"], id, "{result}");
        assert!(result.get("error").is_none(), "{result}");
        result["result"].clone()
    }
    fn raw(&mut self, name: &str, args: Value) -> Value {
        self.rpc("tools/call", json!({"name":name,"arguments":args}))
    }
    fn call(&mut self, name: &str, args: Value) -> Value {
        let result = self.raw(name, args);
        assert_eq!(result["isError"], false, "tool {name}: {result}");
        let content = &result["structuredContent"];
        assert_eq!(
            serde_json::from_str::<Value>(result["content"][0]["text"].as_str().unwrap()).unwrap(),
            *content
        );
        content.clone()
    }
    /// A committed native mutation, acknowledged unless `keep` says otherwise.
    fn committed(&mut self, name: &str, args: Value, keep: bool) -> (String, Value) {
        let value = self.call(name, args);
        assert_eq!(value["condition"], "Committed", "{name}: {value}");
        assert_eq!(value["schema_version"], 2);
        assert_eq!(value["result"]["kind"], "native");
        let id = value["operation_id"].as_str().unwrap().to_string();
        assert!(id.starts_with("n1:"));
        if !keep {
            let acknowledged = self.call("request.acknowledge", json!({"operation_id": id}));
            assert_eq!(acknowledged["condition"], "Consumed");
        }
        (id, value["result"].clone())
    }
    fn read(&mut self, name: &str, args: Value) -> Value {
        let value = self.call(name, args);
        assert_eq!(value["condition"], "Read", "{name}: {value}");
        assert_eq!(value["schema_version"], 2);
        assert_eq!(value["result"]["kind"], "native_read", "{value}");
        value
    }
}
fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| hex_hash(&entry["id"]))
        .collect()
}
fn objects(page: &Value) -> &Vec<Value> {
    page["result"]["page"]["objects"].as_array().unwrap()
}
fn hex_hash(value: &Value) -> String {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;
const ERROR: &str = r#"{"code":"build","message":"The build failed before any test ran."}"#;
const FAR: u64 = 4_102_444_800_000;
/// Codes of the frozen claim status, outcome and verdict vocabularies.
const POSTED: u64 = 2;
const VALIDATING: u64 = 7;
const SATISFIED: u64 = 8;
const VALIDATION_INCOMPLETE: u64 = 12;
const OUTCOME_COMPLETE: u64 = 1;
const OUTCOME_FAILED: u64 = 6;
const VERDICT_PASS: u64 = 1;
const VERDICT_ERROR: u64 = 4;

fn claim_document(target: &str, description: &str) -> Value {
    json!({
        "description": description,
        "target": target,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77), "attempts": 2}],
             "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    })
}
fn inline_text(artifact: &Value) -> String {
    let bytes: Vec<u8> = artifact["payload"]["Inline"]
        .as_array()
        .unwrap_or_else(|| panic!("{artifact}"))
        .iter()
        .map(|byte| byte.as_u64().unwrap() as u8)
        .collect();
    String::from_utf8(bytes).unwrap()
}
impl Mcp {
    fn claim(&mut self, id: &str) -> Value {
        objects(&self.read("claim.get", json!({"id": id})))[0]["Claim"].clone()
    }
    fn artifact(&mut self, id: &str) -> Value {
        objects(&self.read("artifact.get", json!({"id": id})))[0]["Artifact"].clone()
    }
    fn response(&mut self, id: &str) -> Value {
        let page = self.read("testament.get", json!({"id": id}));
        objects(&page)
            .iter()
            .find_map(|object| object.get("Response"))
            .unwrap_or_else(|| panic!("{page}"))
            .clone()
    }
    fn evaluations(&mut self, id: &str) -> Vec<Value> {
        objects(&self.read("validation.get", json!({"id": id})))
            .iter()
            .filter_map(|object| object.get("Evaluation").cloned())
            .collect()
    }
    /// A refused tool call: the structured failure with its code.
    fn refused(&mut self, name: &str, args: Value) -> (String, Value) {
        let result = self.raw(name, args);
        assert_eq!(result["isError"], true, "tool {name}: {result}");
        let content = result["structuredContent"].clone();
        assert_eq!(content["condition"], "Error", "{content}");
        (
            content["result"]["code"].as_str().unwrap().to_string(),
            content,
        )
    }
}

#[test]
fn failed_work_and_evaluator_errors_stay_distinct_inspectable_evidence_through_mcp() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-mcp-a2-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-mcp-a2-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let invitation = client.path().join("alice.invite");
    admin(
        root,
        None,
        &[
            "cluster",
            "client",
            "invite",
            "--name",
            "alice",
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    let enrolled = run(
        client.path(),
        None,
        &[
            "context",
            "enroll",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(
        enrolled.status.success(),
        "enroll: {}
{}",
        String::from_utf8_lossy(&enrolled.stderr),
        String::from_utf8_lossy(&enrolled.stdout)
    );
    let mut issuer = Mcp::open(root, None);
    let mut alice = Mcp::open(client.path(), Some("alice"));
    let alice_standing = alice.read("ledger.standing", json!({}));
    let alice_id = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);

    // ---- Claim A: the respondent's work fails ----
    let (_, result) = issuer.committed(
        "claim.submit",
        claim_document(&alice_id, "Run the suite and deliver the report."),
        false,
    );
    let a = created(&result, "Claim").remove(0);
    let a_check = created(&result, "Validation").remove(1);
    issuer.committed("claim.post", json!({"claim": a}), false);
    alice.committed("receipt.acquire", json!({"claim": a}), false);
    // Failed testimony without its diagnostic is refused before anything is
    // sent, with a typed input failure.
    let (code, refusal) = alice.refused(
        "testament.submit",
        json!({"claim": a, "summary": "Could not build.", "confidence": "committed", "outcome": "failed"}),
    );
    assert_eq!(code, "invalid_input", "{refusal}");
    let (_, result) = alice.committed(
        "artifact.diagnostic",
        json!({"claim": a, "reason": "work", "payload": {"type": "text", "text": ERROR}}),
        false,
    );
    let diagnostic = created(&result, "Artifact").remove(0);
    let diagnostic_object = alice.artifact(&diagnostic);
    let diagnostic_hash = hex_hash(&diagnostic_object["content_hash"]);
    assert_eq!(diagnostic_object["kind"], "error", "{diagnostic_object}");
    assert_eq!(inline_text(&diagnostic_object), ERROR);

    // The failed testament's result is committed but the adapter dies before
    // the agent consumes it: a fresh adapter on the same journal lists it,
    // its exact retry returns the same receipt, and no second testament is
    // authored.
    let (lost_id, lost) = alice.committed(
        "testament.submit",
        json!({"claim": a, "summary": "Could not build.", "confidence": "committed", "outcome": "failed",
               "diagnostics": [{"id": diagnostic, "hash": diagnostic_hash}]}),
        true,
    );
    drop(alice);
    let mut alice = Mcp::open(client.path(), Some("alice"));
    let pending = alice.call("request.pending", json!({}));
    assert_eq!(
        pending["result"]["operation_ids"],
        json!([lost_id]),
        "{pending}"
    );
    let replayed = alice.call("request.retry", json!({"operation_id": lost_id}));
    assert_eq!(replayed["result"], lost);
    let consumed = alice.call("request.acknowledge", json!({"operation_id": lost_id}));
    assert_eq!(consumed["condition"], "Consumed");
    let testament_a = created(&lost, "Response").remove(0);
    alice.committed(
        "testament.post",
        json!({"claim": a, "testament": testament_a}),
        false,
    );
    issuer.committed(
        "testament.receive",
        json!({"claim": a, "testament": testament_a}),
        false,
    );
    let responses = objects(&issuer.read("claim.get", json!({"id": a})))
        .iter()
        .filter(|object| object.get("Response").is_some())
        .count();
    assert_eq!(responses, 1);

    // The testimony is the respondent's own: Failed with the exact diagnostic
    // reference and an empty manifest; the claimant reads the diagnostic
    // bytes through its own adapter.
    let response = issuer.response(&testament_a);
    assert_eq!(response["outcome"], OUTCOME_FAILED, "{response}");
    assert_eq!(response["state"], "Received", "{response}");
    assert_eq!(hex_hash(&response["respondent"]), alice_id, "{response}");
    assert!(
        response["manifest"].as_array().unwrap().is_empty(),
        "{response}"
    );
    let cited = &response["diagnostics"][0];
    assert_eq!(hex_hash(&cited["producer"]), alice_id, "{response}");
    assert_eq!(cited["diagnostic"]["reason"], "Work", "{response}");
    assert_eq!(
        hex_hash(&cited["diagnostic"]["artifact"]["id"]),
        diagnostic,
        "{response}"
    );
    assert_eq!(
        hex_hash(&cited["diagnostic"]["artifact"]["hash"]),
        diagnostic_hash,
        "{response}"
    );
    let read_by_claimant = issuer.artifact(&diagnostic);
    assert_eq!(inline_text(&read_by_claimant), ERROR);
    assert_eq!(hex_hash(&read_by_claimant["producer"]), alice_id);

    // The evaluator's context names the missing slot as the exact target; the
    // delivery check passed, the registration is ineligible, and no check on
    // the missing slot can be begun or reported.
    let page = issuer.read("validation.context", json!({"validation": a_check}));
    let context = &objects(&page)[0]["Context"];
    assert_eq!(
        hex_hash(&context["claim"]["binding"]["object"]),
        a,
        "{page}"
    );
    assert_eq!(context["manifest"][0]["slot"], 0, "{page}");
    assert!(context["manifest"][0]["artifact"].is_null(), "{page}");
    assert_eq!(
        context["evaluation"]["key"]["target"]["MissingSlot"]["slot"], 0,
        "{page}"
    );
    assert_eq!(context["registration"]["eligible"], false, "{page}");
    assert_eq!(context["delivery"]["verdict"], VERDICT_PASS, "{page}");
    let (code, _) = issuer.refused(
        "validation.begin",
        json!({"claim": a, "validation": a_check}),
    );
    assert_eq!(code, "not_found");
    let (code, _) = issuer.refused(
        "validation.report",
        json!({"claim": a, "validation": a_check, "verdict": "incomplete", "payload": {"type": "text", "text": ERROR}}),
    );
    assert_eq!(code, "not_found");
    // Explicit whole-work entry assesses the missing slot: the required check
    // ends ValidationIncomplete without any manufactured verdict and the
    // claim is never satisfied.
    let (_, entered) = issuer.committed(
        "validation.enter_whole_work",
        json!({"claim": a, "testament": testament_a}),
        false,
    );
    assert!(created(&entered, "Artifact").is_empty(), "{entered}");
    let a_evaluations = issuer.evaluations(&a_check);
    let missing = a_evaluations
        .iter()
        .find(|evaluation| evaluation["key"]["target"].get("MissingSlot").is_some())
        .unwrap_or_else(|| panic!("{a_evaluations:?}"));
    assert_eq!(missing["state"], "ValidationIncomplete", "{missing}");
    assert!(missing["attempt_index"].is_null(), "{missing}");
    assert_eq!(missing["has_begun"], false, "{missing}");
    let a_before = issuer.claim(&a);
    assert_eq!(a_before["status"], VALIDATION_INCOMPLETE, "{a_before}");
    let response_after_entry = issuer.response(&testament_a);
    assert_eq!(response_after_entry["state"], "ValidationIncomplete");
    assert_eq!(response_after_entry["outcome"], OUTCOME_FAILED);

    // ---- Claim B: the work succeeds and the evaluator fails to execute ----
    let (_, result) = issuer.committed(
        "claim.submit",
        claim_document(&alice_id, "Run the suite again and deliver the report."),
        false,
    );
    let b = created(&result, "Claim").remove(0);
    let b_check = created(&result, "Validation").remove(1);
    issuer.committed("claim.post", json!({"claim": b}), false);
    assert_eq!(issuer.claim(&b)["status"], POSTED);
    alice.committed("receipt.acquire", json!({"claim": b}), false);
    let (_, result) = alice.committed(
        "artifact.submit",
        json!({"claim": b, "slot": 0, "payload": {"type": "text", "text": PROOF}}),
        false,
    );
    let output = created(&result, "Artifact").remove(0);
    let output_hash = hex_hash(&alice.artifact(&output)["content_hash"]);
    let (_, result) = alice.committed(
        "testament.submit",
        json!({"claim": b, "summary": "Suite passed.", "confidence": "committed", "outcome": "complete",
               "manifest": [{"slot": 0, "artifact": {"id": output, "hash": output_hash}}]}),
        false,
    );
    let testament_b = created(&result, "Response").remove(0);
    alice.committed(
        "testament.post",
        json!({"claim": b, "testament": testament_b}),
        false,
    );
    issuer.committed(
        "testament.receive",
        json!({"claim": b, "testament": testament_b}),
        false,
    );
    issuer.committed(
        "validation.begin",
        json!({"claim": b, "validation": b_check}),
        false,
    );
    // The evaluator could not run its handler: the Error result names the
    // exact target and attempt zero; the evaluation stays open on the second
    // attempt, nothing is satisfied and the work is untouched. The result is
    // kept unacknowledged so the restart below can reprint it.
    let (error_id, error_result) = issuer.committed(
        "validation.report",
        json!({"claim": b, "validation": b_check, "verdict": "error", "payload": {"type": "text", "text": ERROR}}),
        true,
    );
    let error_report = created(&error_result, "Artifact").remove(0);
    let errored = issuer.artifact(&error_report);
    assert_eq!(errored["kind"], "error", "{errored}");
    assert_eq!(inline_text(&errored), ERROR);
    assert_eq!(
        hex_hash(&errored["result_provenance"]["claim"]),
        b,
        "{errored}"
    );
    assert_eq!(
        hex_hash(&errored["result_provenance"]["validation"]),
        b_check,
        "{errored}"
    );
    assert_eq!(
        errored["result_provenance"]["value"], VERDICT_ERROR,
        "{errored}"
    );
    assert_eq!(
        errored["result_provenance"]["attempt"]["index"], 0,
        "{errored}"
    );
    assert_eq!(
        hex_hash(&errored["result_provenance"]["target"]["Artifact"]["artifact"]["object"]),
        output,
        "{errored}"
    );
    let b_evaluations = issuer.evaluations(&b_check);
    let open = b_evaluations
        .iter()
        .find(|evaluation| evaluation["state"] == "Validating")
        .unwrap_or_else(|| panic!("{b_evaluations:?}"));
    assert_eq!(open["attempt_index"], 1, "{open}");
    assert_eq!(open["attempt_bound"], 2, "{open}");
    assert_eq!(issuer.claim(&b)["status"], VALIDATING);
    assert_eq!(inline_text(&issuer.artifact(&output)), PROOF);
    let response_b = issuer.response(&testament_b);
    assert_eq!(response_b["state"], "Validating", "{response_b}");
    assert_eq!(response_b["outcome"], OUTCOME_COMPLETE, "{response_b}");

    // The retry on the second attempt passes: the claim is satisfied and the
    // error report remains as evidence beside the passing one.
    let (_, result) = issuer.committed(
        "validation.report",
        json!({"claim": b, "validation": b_check, "verdict": "pass", "payload": {"type": "text", "text": PROOF}}),
        false,
    );
    let pass_report = created(&result, "Artifact").remove(0);
    let passed = issuer.artifact(&pass_report);
    assert_eq!(
        passed["result_provenance"]["value"], VERDICT_PASS,
        "{passed}"
    );
    assert_eq!(
        passed["result_provenance"]["attempt"]["index"], 1,
        "{passed}"
    );
    assert_eq!(issuer.claim(&b)["status"], SATISFIED);
    assert!(
        issuer
            .evaluations(&b_check)
            .iter()
            .any(|evaluation| evaluation["state"] == "Validated")
    );
    assert_eq!(issuer.artifact(&error_report), errored);
    assert_eq!(issuer.response(&testament_a)["outcome"], OUTCOME_FAILED);
    assert_eq!(issuer.response(&testament_b)["outcome"], OUTCOME_COMPLETE);
    assert_eq!(issuer.claim(&a)["status"], VALIDATION_INCOMPLETE);

    // Kill the node without warning: both histories, the diagnostic bytes,
    // the report artifacts and the unconsumed result survive unchanged, and
    // the human CLI observes the adapter's error report through the owner.
    let b_before = issuer.claim(&b);
    let a_check_before = issuer.read("validation.get", json!({"id": a_check}));
    let b_check_before = issuer.read("validation.get", json!({"id": b_check}));
    drop(server);
    let server = start(root, &advertise);
    assert_eq!(issuer.claim(&a), a_before);
    assert_eq!(issuer.claim(&b), b_before);
    assert_eq!(issuer.response(&testament_a), response_after_entry);
    assert_eq!(issuer.artifact(&diagnostic), read_by_claimant);
    assert_eq!(issuer.artifact(&error_report), errored);
    assert_eq!(issuer.artifact(&pass_report), passed);
    assert_eq!(
        objects(&issuer.read("validation.get", json!({"id": a_check}))),
        objects(&a_check_before)
    );
    assert_eq!(
        objects(&issuer.read("validation.get", json!({"id": b_check}))),
        objects(&b_check_before)
    );
    let pending = issuer.call("request.pending", json!({}));
    assert_eq!(
        pending["result"]["operation_ids"],
        json!([error_id]),
        "{pending}"
    );
    let inspected = issuer.call("request.inspect", json!({"operation_id": error_id}));
    assert_eq!(inspected["condition"], "Committed");
    assert_eq!(inspected["result"], error_result);
    let observed = admin(
        root,
        None,
        &[
            "request",
            "inspect",
            "--operation-id",
            &error_id,
            "--remote",
            "--format",
            "json",
        ],
    );
    assert_eq!(observed["condition"], "Observed", "{observed}");
    assert_eq!(
        objects(&observed)[0]["Outcome"]["intent"],
        error_result["receipt"]["intent"]
    );
    let consumed = issuer.call("request.acknowledge", json!({"operation_id": error_id}));
    assert_eq!(consumed["condition"], "Consumed");
    assert_eq!(
        issuer.call("request.pending", json!({}))["result"]["operation_ids"],
        json!([])
    );
    let lost_after = alice.call("request.inspect", json!({"operation_id": lost_id}));
    assert_eq!(lost_after["condition"], "Committed", "{lost_after}");
    assert_eq!(lost_after["result"], lost);
    drop(alice);
    drop(issuer);
    drop(server);
}
