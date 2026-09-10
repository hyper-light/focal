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
//! The remaining native verbs through the real binary (REMAINING §9 A3):
//! admission and increment evaluations selected by phase, sealed increment
//! targets, explicit whole-work entry, work receipt and rejection, failed
//! slot production, receipt adoption, scope release, the result testament
//! (audit) and durable monitors, with a killed and restarted node and exact
//! retries afterwards. Trusted deadline timers arrive with Batch F.
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
fn cli(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let output = run(root, context, &args);
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
/// Administrative and status commands print JSON without a format flag.
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
fn committed(value: &Value) -> (String, Value) {
    assert_eq!(value["condition"], "Committed", "{value}");
    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["result"]["kind"], "native");
    let id = value["operation_id"].as_str().unwrap().to_string();
    assert!(id.starts_with("n1:"));
    (id, value["result"].clone())
}
fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| {
            entry["id"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
                .collect::<String>()
        })
        .collect()
}
fn objects(page: &Value) -> &Vec<Value> {
    assert_eq!(page["result"]["kind"], "native_read", "{page}");
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

/// A refused command: the structured error printed on stdout for a service
/// refusal or on stderr for an input refusal, with the exit code.
fn refused(root: &Path, context: Option<&str>, args: &[&str]) -> (i32, Value) {
    let mut full = args.to_vec();
    full.extend(["--format", "json"]);
    let output = run(root, context, &full);
    assert!(!output.status.success(), "{args:?} succeeded");
    let text = if output.stdout.is_empty() {
        output.stderr.clone()
    } else {
        output.stdout.clone()
    };
    let value: Value = serde_json::from_slice(&text)
        .unwrap_or_else(|error| panic!("{args:?}: {error}: {}", String::from_utf8_lossy(&text)));
    assert_eq!(value["condition"], "Error", "{value}");
    (output.status.code().unwrap(), value)
}
fn list(root: &Path, context: Option<&str>, args: &[&str]) -> Vec<Value> {
    let mut full = vec!["list"];
    full.extend_from_slice(args);
    let page = cli(root, context, &full);
    assert_eq!(page["condition"], "Listed", "{page}");
    assert_eq!(page["result"]["kind"], "native_list", "{page}");
    page["result"]["page"]["objects"]
        .as_array()
        .unwrap()
        .clone()
}
fn claim_object(root: &Path, claim: &str) -> Value {
    let page = cli(root, None, &["get", "claim", claim]);
    objects(&page)[0]["Claim"].clone()
}
fn artifact_hash(root: &Path, context: Option<&str>, artifact: &str) -> String {
    let page = cli(root, context, &["get", "artifact", artifact]);
    hex_hash(&objects(&page)[0]["Artifact"]["content_hash"])
}
fn evaluations(root: &Path, validation: &str) -> Vec<Value> {
    let page = cli(root, None, &["get", "validation", validation]);
    objects(&page)
        .iter()
        .filter(|object| object.get("Evaluation").is_some())
        .map(|object| object["Evaluation"].clone())
        .collect()
}
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;
const ERROR: &str = r#"{"code":"malformed","message":"The report is not a test report."}"#;
const FAR: u64 = 4_102_444_800_000;
/// Posted, Received, Satisfied: codes of the frozen claim status vocabulary.
const POSTED: u64 = 2;
const RECEIVED: u64 = 3;
const SATISFIED: u64 = 8;

fn handler(id: u128) -> Value {
    json!({"id": format!("{id:032x}"), "version": format!("{id:064x}")})
}
fn check(kind: &str, description: &str, target: Value, phase: &str, handler_id: u128) -> Value {
    json!({
        "kind": kind, "description": description, "target": target, "phase": phase,
        "evaluator": "self", "handlers": [handler(handler_id)], "deadline": {"at": FAR}
    })
}

#[test]
fn the_remaining_native_verbs_run_through_the_binary_and_survive_a_kill() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-a3-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-a3-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let status = admin(root, None, &["status"]);
    let issuer = hex_hash(&objects(&status)[0]["Standing"]["principal"]);

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
    assert!(
        run(
            client.path(),
            None,
            &[
                "context",
                "enroll",
                "alice",
                "--invite-file",
                invitation.to_str().unwrap()
            ]
        )
        .status
        .success()
    );
    let alice_root = client.path();
    let alice_ctx = Some("alice");
    let alice_standing = admin(alice_root, alice_ctx, &["status"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);
    assert_ne!(alice, issuer);

    // ---- Claim A: admission, increment and whole-work checks by phase ----
    let document = json!({
        "description": "Run the suite with admission and increment checks.",
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            check("test", "The plan is admissible.", json!({"type": "admission"}), "admission", 71),
            check("test", "Each increment passes.", json!({"type": "increment"}), "increment", 72),
            check("test", "The suite passes.", json!({"type": "slot", "index": 0, "name": "report"}), "whole_work", 73)
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 3}]}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let a = created(&result, "Claim").remove(0);
    let validations = created(&result, "Validation");
    let (admission, increment, whole_work) = (&validations[1], &validations[2], &validations[3]);
    committed(&cli(root, None, &["claim", "post", &a]));

    // Posting registers the admission evaluation; the issuer is its
    // evaluator and selects it by phase. The default phase does not see it.
    let (code, error) = refused(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &a,
            "--validation",
            admission,
        ],
    );
    assert_eq!(code, 4, "{error}");
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &a,
            "--validation",
            admission,
            "--phase",
            "admission",
        ],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &a,
            "--validation",
            admission,
            "--phase",
            "admission",
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    assert!(
        evaluations(root, admission)
            .iter()
            .any(|evaluation| evaluation["state"] == "Validated"),
        "admission result not accepted"
    );
    assert_eq!(claim_object(root, &a)["status"], POSTED);

    // The respondent acquires the receipt and delivers slot zero; the
    // submission registers the increment evaluation of that exact artifact.
    committed(&cli(alice_root, alice_ctx, &["receipt", "acquire", &a]));
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact", "submit", "--claim", &a, "--slot", "0", "--text", PROOF,
        ],
    ));
    let output = created(&result, "Artifact").remove(0);
    let output_hash = artifact_hash(alice_root, alice_ctx, &output);
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &a,
            "--validation",
            increment,
            "--phase",
            "increment",
            "--target",
            &output,
        ],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &a,
            "--validation",
            increment,
            "--phase",
            "increment",
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    // The issuer receives the generated work and seals increment targets
    // while the response is still open; sealing again is the same outcome.
    committed(&cli(
        root,
        None,
        &["artifact", "receive", &output, "--claim", &a],
    ));
    let (seal_id, seal) = committed(&cli(
        root,
        None,
        &["validation", "seal-increments", "--claim", &a],
    ));
    let sealed_again = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &seal_id],
    );
    assert_eq!(sealed_again["result"]["receipt"], seal["receipt"]);

    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "testament",
            "submit",
            "--claim",
            &a,
            "--summary",
            "Suite passed.",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
            "--slot",
            &format!("0={output}:{output_hash}"),
        ],
    ));
    let testament = created(&result, "Response").remove(0);
    committed(&cli(
        alice_root,
        alice_ctx,
        &["testament", "post", &testament, "--claim", &a],
    ));
    committed(&cli(
        root,
        None,
        &["testament", "receive", &testament, "--claim", &a],
    ));
    // Explicit whole-work entry closes the increment cohort; the slot check
    // is then begun and reported without naming a phase.
    committed(&cli(
        root,
        None,
        &["validation", "enter-whole-work", &testament, "--claim", &a],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &a,
            "--validation",
            whole_work,
        ],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &a,
            "--validation",
            whole_work,
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    assert_eq!(claim_object(root, &a)["status"], SATISFIED);
    // The evaluator's context read composes the claim, definition, selected
    // evaluation, the target's manifest with custody, the accepted results
    // and the delivery result at one prefix.
    let page = cli(root, None, &["get", "validation", whole_work, "--context"]);
    let context = &objects(&page)[0]["Context"];
    assert_eq!(
        hex_hash(&context["claim"]["binding"]["object"]),
        a,
        "{page}"
    );
    assert_eq!(
        hex_hash(&context["definition"]["binding"]["object"]),
        *whole_work
    );
    assert_eq!(context["registration"]["generation"], 1, "{page}");
    assert_eq!(context["registration"]["eligible"], false, "{page}");
    assert_eq!(context["evaluation"]["state"], "Validated", "{page}");
    assert_eq!(context["manifest"][0]["slot"], 0, "{page}");
    assert_eq!(context["manifest"][0]["custody_verified"], true, "{page}");
    assert_eq!(
        hex_hash(&context["manifest"][0]["artifact"]["id"]),
        output,
        "{page}"
    );
    assert!(!context["results"].as_array().unwrap().is_empty(), "{page}");
    assert!(context["delivery"].is_object(), "{page}");
    let page = cli(
        root,
        None,
        &[
            "get",
            "validation",
            admission,
            "--context",
            "--phase",
            "admission",
        ],
    );
    let context = &objects(&page)[0]["Context"];
    assert_eq!(
        context["evaluation"]["key"]["target"], "Admission",
        "{page}"
    );
    assert!(context["manifest"].as_array().unwrap().is_empty(), "{page}");
    assert_eq!(context["results"].as_array().unwrap().len(), 1, "{page}");
    // A phase the definition never had selects no registration.
    let page = cli(
        root,
        None,
        &[
            "get",
            "validation",
            admission,
            "--context",
            "--phase",
            "increment",
        ],
    );
    let context = &objects(&page)[0]["Context"];
    assert_eq!(context["registration"]["state"], "Missing", "{page}");
    assert!(context["evaluation"].is_null(), "{page}");

    // The terminal claim releases its owned scope once; the audit is
    // generated from the accepted results and posted by the issuer.
    committed(&cli(root, None, &["claim", "release-scope", &a]));
    assert_eq!(claim_object(root, &a)["released"], true);
    let (code, _) = refused(root, None, &["claim", "release-scope", &a]);
    assert_eq!(code, 5);
    let (_, result) = committed(&cli(root, None, &["audit", "generate", "--claim", &a]));
    let audit = created(&result, "ResultTestament").remove(0);
    let (audit_post_id, audit_post) = committed(&cli(root, None, &["audit", "post", &audit]));
    let page = cli(root, None, &["get", "testament", &audit]);
    let posted = objects(&page)
        .iter()
        .find_map(|object| object.get("ResultTestament"))
        .unwrap_or_else(|| panic!("{page}"));
    assert_eq!(posted["state"], "Posted", "{page}");
    assert_eq!(hex_hash(&posted["claim"]), a, "{page}");

    // ---- Claim B: rejected work, failed production and adoption ----
    let document = json!({
        "description": "Produce two reports.",
        "target": alice,
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}],
        "slots": [{"slot": 0}, {"slot": 1}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let b = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &b]));
    committed(&cli(alice_root, alice_ctx, &["receipt", "acquire", &b]));
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact",
            "submit",
            "--claim",
            &b,
            "--slot",
            "0",
            "--text",
            PROOF,
            "--visibility",
            "team",
        ],
    ));
    let bad = created(&result, "Artifact").remove(0);
    // Only structure or metadata failures are rejections; the diagnostic
    // inherits the rejected product's visibility.
    let (code, _) = refused(
        root,
        None,
        &[
            "artifact", "reject", &bad, "--claim", &b, "--reason", "work", "--text", ERROR,
        ],
    );
    assert_eq!(code, 2);
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "artifact",
            "reject",
            &bad,
            "--claim",
            &b,
            "--reason",
            "structure",
            "--text",
            ERROR,
        ],
    ));
    let rejection = created(&result, "Artifact").remove(0);
    let page = cli(root, None, &["get", "artifact", &rejection]);
    let descriptor = &objects(&page)[0]["Artifact"];
    assert_eq!(descriptor["kind"], "error", "{page}");
    assert_eq!(descriptor["visibility"], json!(["team"]), "{page}");
    let page = cli(root, None, &["get", "artifact", &bad]);
    let work = objects(&page)
        .iter()
        .find_map(|object| object.get("Work"))
        .unwrap_or_else(|| panic!("{page}"));
    assert_eq!(work["state"], "ReceiptFailed", "{page}");
    assert_eq!(hex_hash(&work["diagnostic"]["artifact"]["id"]), rejection);
    // The holder records slot one as unproducible with its own production
    // diagnostic; a work diagnostic is not accepted for that.
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact",
            "diagnostic",
            "--claim",
            &b,
            "--reason",
            "work",
            "--text",
            ERROR,
        ],
    ));
    let work_diagnostic = created(&result, "Artifact").remove(0);
    let (code, _) = refused(
        alice_root,
        alice_ctx,
        &[
            "artifact",
            "fail",
            "--claim",
            &b,
            "--slot",
            "1",
            "--diagnostic",
            &work_diagnostic,
        ],
    );
    assert_eq!(code, 5);
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact",
            "diagnostic",
            "--claim",
            &b,
            "--reason",
            "production",
            "--text",
            ERROR,
        ],
    ));
    let production = created(&result, "Artifact").remove(0);
    let production_hash = artifact_hash(alice_root, alice_ctx, &production);
    committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact",
            "fail",
            "--claim",
            &b,
            "--slot",
            "1",
            "--diagnostic",
            &format!("{production}:{production_hash}"),
        ],
    ));
    // The failed slot's work object is the diagnostic itself.
    let page = cli(root, None, &["get", "artifact", &production]);
    let failed = objects(&page)
        .iter()
        .find_map(|object| object.get("Work"))
        .unwrap_or_else(|| panic!("{page}"));
    assert_eq!(failed["state"], "GenerationFailed", "{page}");
    assert_eq!(failed["slot"], 1, "{page}");
    // The issuer adopts responsibility; the old receipt is fenced and the
    // respondent's later testimony is refused as stale.
    let before = claim_object(root, &b);
    assert_eq!(before["status"], RECEIVED);
    assert_eq!(before["receipt"]["fence"]["epoch"], 1);
    let (_, result) = committed(&cli(
        root,
        None,
        &["receipt", "adopt", &b, "--holder", "self"],
    ));
    assert_eq!(created(&result, "Receipt").len(), 1);
    let after = claim_object(root, &b);
    assert_eq!(after["receipt"]["fence"]["epoch"], 2, "{after}");
    assert_eq!(hex_hash(&after["receipt"]["holder"]), issuer, "{after}");
    let receipts = list(root, None, &["receipts", "--claim", &b]);
    assert_eq!(receipts.len(), 2, "{receipts:?}");
    let (code, _) = refused(
        alice_root,
        alice_ctx,
        &[
            "testament",
            "submit",
            "--claim",
            &b,
            "--summary",
            "Late.",
            "--confidence",
            "committed",
            "--outcome",
            "failed",
        ],
    );
    assert_ne!(code, 0);

    // ---- Claim C: a durable monitor over committed claims ----
    let document = json!({
        "description": "Wait for the reports.",
        "target": alice,
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let c = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &c]));
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "monitor",
            "register",
            "--owner",
            &c,
            "--root",
            &format!("satisfied:{b}"),
            "--at",
            &FAR.to_string(),
        ],
    ));
    let monitor = created(&result, "Monitor").remove(0);
    let monitors = list(root, None, &["monitors", "--claim", &c]);
    assert_eq!(monitors.len(), 1, "{monitors:?}");
    assert_eq!(hex_hash(&monitors[0]["Monitor"]["id"]), monitor);
    assert!(
        monitors[0]["Monitor"]["disposition"].is_null(),
        "{monitors:?}"
    );
    // Rebinding follows only a committed supersession of the root; the
    // supersession terminalizes the predecessor without settling the wait.
    let document = json!({
        "description": "Produce two reports (successor).",
        "target": alice,
        "relations": [{"kind": "supersedes", "target": format!("claim:{b}")}],
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}],
        "slots": [{"slot": 0}, {"slot": 1}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let d = created(&result, "Claim").remove(0);
    let (code, _) = refused(
        root,
        None,
        &[
            "monitor",
            "rebind",
            &monitor,
            "--owner",
            &c,
            "--predecessor",
            &b,
            "--successor",
            &c,
        ],
    );
    assert_ne!(code, 0);
    committed(&cli(
        root,
        None,
        &[
            "monitor",
            "rebind",
            &monitor,
            "--owner",
            &c,
            "--predecessor",
            &b,
            "--successor",
            &d,
        ],
    ));
    let monitors = list(root, None, &["monitors", "--claim", &c]);
    assert_eq!(
        hex_hash(&monitors[0]["Monitor"]["last_rebinding"]["successor"]),
        d,
        "{monitors:?}"
    );
    // Only a terminal claim's remaining wait can be disposed of explicitly;
    // cancelling the claim keeps the monitor until the issuer cancels it.
    let (code, _) = refused(root, None, &["monitor", "cancel", &monitor, "--owner", &c]);
    assert_eq!(code, 5);
    committed(&cli(root, None, &["claim", "cancel", &c]));
    committed(&cli(
        root,
        None,
        &["monitor", "cancel", &monitor, "--owner", &c],
    ));
    let monitors_before = list(root, None, &["monitors", "--claim", &c]);
    assert!(
        !monitors_before[0]["Monitor"]["disposition"].is_null(),
        "{monitors_before:?}"
    );
    let (code, _) = refused(root, None, &["monitor", "cancel", &monitor, "--owner", &c]);
    assert_ne!(code, 0);

    // ---- Claim E: the trusted deadline timers fire from the node's clock ----
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let soon = now + 2_500;
    let document = json!({
        "description": "Expires soon.",
        "target": alice,
        "deadline": {"at": soon},
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let e = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &e]));
    // A monitor on the live claim with a deadline of its own.
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "monitor",
            "register",
            "--owner",
            &e,
            "--root",
            &format!("satisfied:{d}"),
            "--at",
            &(soon + 500).to_string(),
        ],
    ));
    let e_monitor = created(&result, "Monitor").remove(0);
    assert_eq!(claim_object(root, &e)["status"], POSTED);
    // Expired is code 16 of the frozen claim status vocabulary; the owner's
    // sweep runs once a second on the embedded host.
    let started = std::time::Instant::now();
    let expired = loop {
        let claim = claim_object(root, &e);
        if claim["status"] == 16 {
            break claim;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "claim did not expire: {claim}"
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(expired["deadline"]["at"], soon, "{expired}");
    // The expired claim's monitor timer fires on the terminal owner and is
    // consumed without inventing a release; the registration stays readable.
    let started = std::time::Instant::now();
    loop {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        if now > soon + 2_000 {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
        assert!(started.elapsed() < Duration::from_secs(20));
    }
    let monitors = list(root, None, &["monitors", "--claim", &e]);
    assert_eq!(
        hex_hash(&monitors[0]["Monitor"]["id"]),
        e_monitor,
        "{monitors:?}"
    );
    // The expiry is a terminal fact: a later post is refused as stale and a
    // cancellation, whatever it records, never rewrites the status.
    let (code, _) = refused(root, None, &["claim", "post", &e]);
    assert_eq!(code, 5);
    let _ = run(root, None, &["claim", "cancel", &e, "--format", "json"]);
    assert_eq!(claim_object(root, &e)["status"], 16);

    // ---- Kill and restart: everything above is durable and retries hold ----
    let e_before = claim_object(root, &e);
    let a_before = claim_object(root, &a);
    let b_before = claim_object(root, &b);
    drop(server);
    let server = start(root, &advertise);
    assert_eq!(claim_object(root, &a), a_before);
    assert_eq!(claim_object(root, &b), b_before);
    assert_eq!(claim_object(root, &e), e_before);
    assert_eq!(
        list(root, None, &["monitors", "--claim", &c]),
        monitors_before
    );
    let retried = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &audit_post_id],
    );
    assert_eq!(retried["result"]["receipt"], audit_post["receipt"]);
    let (code, _) = refused(root, None, &["audit", "post", &audit]);
    assert_eq!(code, 5);
    drop(server);
}
