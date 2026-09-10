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
//! The second product gate on the native engine: failed respondent work and
//! an evaluator that fails to execute remain distinct, inspectable evidence
//! through the real binary, across a lost reply and a killed and restarted
//! node (REMAINING §9 A2).
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
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

const ERROR: &str = r#"{"code":"build","message":"The build failed before any test ran."}"#;
const FAR: u64 = 4_102_444_800_000;
/// Codes of the frozen claim status vocabulary.
const POSTED: u64 = 2;
const VALIDATING: u64 = 7;
const SATISFIED: u64 = 8;
const VALIDATION_INCOMPLETE: u64 = 12;
/// Codes of the frozen outcome and verdict vocabularies.
const OUTCOME_FAILED: u64 = 6;
const VERDICT_PASS: u64 = 1;
const VERDICT_ERROR: u64 = 4;

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
fn claim_object(root: &Path, claim: &str) -> Value {
    objects(&cli(root, None, &["get", "claim", claim]))[0]["Claim"].clone()
}
fn artifact_object(root: &Path, context: Option<&str>, artifact: &str) -> Value {
    objects(&cli(root, context, &["get", "artifact", artifact]))[0]["Artifact"].clone()
}
fn response_object(root: &Path, testament: &str) -> Value {
    let page = cli(root, None, &["get", "testament", testament]);
    objects(&page)
        .iter()
        .find_map(|object| object.get("Response"))
        .unwrap_or_else(|| panic!("{page}"))
        .clone()
}
fn evaluations(root: &Path, validation: &str) -> Vec<Value> {
    let page = cli(root, None, &["get", "validation", validation]);
    objects(&page)
        .iter()
        .filter(|object| object.get("Evaluation").is_some())
        .map(|object| object["Evaluation"].clone())
        .collect()
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
/// A claim with the mandatory delivery check and one programmatic test check
/// on slot zero whose handler may be attempted twice.
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

#[test]
fn failed_work_and_evaluator_errors_stay_distinct_inspectable_evidence_through_the_binary() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-a2-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-a2-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let alice_root = client.path();
    let alice_ctx = Some("alice");

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
    let enrolled = run(
        alice_root,
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
    let alice =
        hex_hash(&objects(&admin(alice_root, alice_ctx, &["status"]))[0]["Standing"]["principal"]);

    // ---- Claim A: the respondent's work fails ----
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "Run the suite and deliver the report.").to_string(),
        ],
    ));
    let a = created(&result, "Claim").remove(0);
    let a_check = created(&result, "Validation").remove(1);
    committed(&cli(root, None, &["claim", "post", &a]));
    committed(&cli(alice_root, alice_ctx, &["receipt", "acquire", &a]));
    // Failed testimony without its diagnostic is refused before anything is
    // sent: a failure is evidence, never an omission.
    let (code, error) = refused(
        alice_root,
        alice_ctx,
        &[
            "testament",
            "submit",
            "--claim",
            &a,
            "--summary",
            "Could not build.",
            "--confidence",
            "committed",
            "--outcome",
            "failed",
        ],
    );
    assert_eq!(code, 2, "{error}");
    assert_eq!(error["error"]["code"], "invalid_input", "{error}");
    // The respondent records the actual diagnostic of the failed work.
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact",
            "diagnostic",
            "--claim",
            &a,
            "--reason",
            "work",
            "--text",
            ERROR,
        ],
    ));
    let diagnostic = created(&result, "Artifact").remove(0);
    let diagnostic_object = artifact_object(alice_root, alice_ctx, &diagnostic);
    let diagnostic_hash = hex_hash(&diagnostic_object["content_hash"]);
    assert_eq!(diagnostic_object["kind"], "error", "{diagnostic_object}");
    assert_eq!(inline_text(&diagnostic_object), ERROR);

    // The failed testament's reply is lost on a closed stdout: the exact frame
    // stays journaled, the recovery command finds it committed, and no second
    // testament is authored.
    let lost_id = {
        use std::os::{fd::OwnedFd, unix::net::UnixStream};
        let (closed, output) = UnixStream::pair().unwrap();
        drop(closed);
        let failed = Command::new(env!("CARGO_BIN_EXE_focal"))
            .args([
                "--data-dir",
                alice_root.to_str().unwrap(),
                "--client-context",
                "alice",
                "testament",
                "submit",
                "--claim",
                &a,
                "--summary",
                "Could not build.",
                "--confidence",
                "committed",
                "--outcome",
                "failed",
                "--diagnostic",
                &format!("{diagnostic}:{diagnostic_hash}"),
                "--format",
                "json",
            ])
            .stdout(Stdio::from(OwnedFd::from(output)))
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!failed.status.success());
        let pending = cli(alice_root, alice_ctx, &["request", "pending"]);
        let rows: Vec<&Value> = pending["operations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["client"] == "CLI native")
            .collect();
        assert_eq!(rows.len(), 1, "{pending}");
        assert_eq!(rows[0]["condition"], "Committed", "{pending}");
        let id = rows[0]["operation_id"].as_str().unwrap().to_string();
        let diagnostic_text = String::from_utf8(failed.stderr).unwrap();
        assert!(
            diagnostic_text.contains(&format!("request retry --operation-id {id}")),
            "{diagnostic_text}"
        );
        id
    };
    let replayed = cli(
        alice_root,
        alice_ctx,
        &["request", "retry", "--operation-id", &lost_id],
    );
    let (_, result) = committed(&replayed);
    let testament_a = created(&result, "Response").remove(0);
    committed(&cli(
        alice_root,
        alice_ctx,
        &["testament", "post", &testament_a, "--claim", &a],
    ));
    committed(&cli(
        root,
        None,
        &["testament", "receive", &testament_a, "--claim", &a],
    ));
    let responses = objects(&cli(root, None, &["get", "claim", &a]))
        .iter()
        .filter(|object| object.get("Response").is_some())
        .count();
    assert_eq!(responses, 1);

    // The testimony is the respondent's own: Failed, with the exact diagnostic
    // reference and an empty manifest; the claimant reads the diagnostic bytes.
    let response = response_object(root, &testament_a);
    assert_eq!(response["outcome"], OUTCOME_FAILED, "{response}");
    assert_eq!(response["state"], "Received", "{response}");
    assert_eq!(hex_hash(&response["respondent"]), alice, "{response}");
    assert!(
        response["manifest"].as_array().unwrap().is_empty(),
        "{response}"
    );
    let cited = &response["diagnostics"][0];
    assert_eq!(hex_hash(&cited["producer"]), alice, "{response}");
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
    let read_by_claimant = artifact_object(root, None, &diagnostic);
    assert_eq!(inline_text(&read_by_claimant), ERROR);
    assert_eq!(hex_hash(&read_by_claimant["producer"]), alice);

    // Recording failed work satisfies nothing: the slot check's target is the
    // missing slot, the evaluator records Incomplete against it, and the claim
    // ends ValidationIncomplete, never Satisfied.
    let page = cli(root, None, &["get", "validation", &a_check, "--context"]);
    let context = &objects(&page)[0]["Context"];
    assert_eq!(
        hex_hash(&context["claim"]["binding"]["object"]),
        a,
        "{page}"
    );
    // The manifest names the declared slot without an artifact, the
    // evaluation's exact target is that missing slot, the registration is
    // ineligible, and the delivery check itself passed: the failure is the
    // work's, not the delivery's.
    assert_eq!(context["manifest"].as_array().unwrap().len(), 1, "{page}");
    assert_eq!(context["manifest"][0]["slot"], 0, "{page}");
    assert!(context["manifest"][0]["artifact"].is_null(), "{page}");
    assert_eq!(context["manifest"][0]["custody_verified"], false, "{page}");
    assert_eq!(
        context["evaluation"]["key"]["target"]["MissingSlot"]["slot"], 0,
        "{page}"
    );
    assert_eq!(context["evaluation"]["has_begun"], false, "{page}");
    assert_eq!(context["registration"]["eligible"], false, "{page}");
    assert_eq!(context["delivery"]["verdict"], VERDICT_PASS, "{page}");
    assert_eq!(
        context["delivery"]["resulting_state"], "Validated",
        "{page}"
    );
    // The missing slot cannot be begun or reported: no evaluator may
    // manufacture a verdict for absent work. Explicit whole-work entry
    // assesses it: the required check ends ValidationIncomplete from a
    // missing-target result with no attempt and no evidence, and the claim
    // ends ValidationIncomplete, never Satisfied.
    let (code, error) = refused(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &a,
            "--validation",
            &a_check,
        ],
    );
    assert_eq!(code, 4, "{error}");
    let (code, error) = refused(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &a,
            "--validation",
            &a_check,
            "--verdict",
            "incomplete",
            "--text",
            ERROR,
        ],
    );
    assert_eq!(code, 4, "{error}");
    let (_, entered) = committed(&cli(
        root,
        None,
        &[
            "validation",
            "enter-whole-work",
            &testament_a,
            "--claim",
            &a,
        ],
    ));
    assert!(created(&entered, "Artifact").is_empty(), "{entered}");
    let a_evaluations = evaluations(root, &a_check);
    let missing = a_evaluations
        .iter()
        .find(|evaluation| evaluation["key"]["target"].get("MissingSlot").is_some())
        .unwrap_or_else(|| panic!("{a_evaluations:?}"));
    assert_eq!(missing["state"], "ValidationIncomplete", "{missing}");
    assert!(missing["attempt_index"].is_null(), "{missing}");
    assert!(missing["current_attempt"].is_null(), "{missing}");
    assert_eq!(missing["has_begun"], false, "{missing}");
    // The terminal result is the missing-target result of this very key.
    assert_eq!(
        missing["last_result"]["evaluation"]["target"]["MissingSlot"]["slot"], 0,
        "{missing}"
    );
    assert_eq!(
        missing["last_result"]["revision"], missing["binding"]["revision"],
        "{missing}"
    );
    assert!(
        !a_evaluations
            .iter()
            .any(|evaluation| evaluation["state"] == "Validated"),
        "{a_evaluations:?}"
    );
    let a_before = claim_object(root, &a);
    assert_eq!(a_before["status"], VALIDATION_INCOMPLETE, "{a_before}");
    assert_ne!(a_before["status"], SATISFIED);
    let response_after_entry = response_object(root, &testament_a);
    assert_eq!(
        response_after_entry["state"], "ValidationIncomplete",
        "{response_after_entry}"
    );
    assert_eq!(response_after_entry["outcome"], OUTCOME_FAILED);

    // ---- Claim B: the work succeeds and the evaluator fails to execute ----
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "Run the suite again and deliver the report.").to_string(),
        ],
    ));
    let b = created(&result, "Claim").remove(0);
    let b_check = created(&result, "Validation").remove(1);
    committed(&cli(root, None, &["claim", "post", &b]));
    assert_eq!(claim_object(root, &b)["status"], POSTED);
    committed(&cli(alice_root, alice_ctx, &["receipt", "acquire", &b]));
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "artifact", "submit", "--claim", &b, "--slot", "0", "--text", PROOF,
        ],
    ));
    let output = created(&result, "Artifact").remove(0);
    let output_hash = hex_hash(&artifact_object(alice_root, alice_ctx, &output)["content_hash"]);
    let (_, result) = committed(&cli(
        alice_root,
        alice_ctx,
        &[
            "testament",
            "submit",
            "--claim",
            &b,
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
    let testament_b = created(&result, "Response").remove(0);
    committed(&cli(
        alice_root,
        alice_ctx,
        &["testament", "post", &testament_b, "--claim", &b],
    ));
    committed(&cli(
        root,
        None,
        &["testament", "receive", &testament_b, "--claim", &b],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &b,
            "--validation",
            &b_check,
        ],
    ));
    // The evaluator could not run its handler: an Error result names the
    // exact target and attempt one. The evaluation stays open on attempt two
    // and nothing is satisfied; the work itself is untouched.
    let (error_id, result) = committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &b,
            "--validation",
            &b_check,
            "--verdict",
            "error",
            "--text",
            ERROR,
        ],
    ));
    let error_report = created(&result, "Artifact").remove(0);
    let errored = artifact_object(root, None, &error_report);
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
    let b_evaluations = evaluations(root, &b_check);
    let open = b_evaluations
        .iter()
        .find(|evaluation| evaluation["state"] == "Validating")
        .unwrap_or_else(|| panic!("{b_evaluations:?}"));
    assert_eq!(open["attempt_index"], 1, "{open}");
    assert_eq!(open["attempt_bound"], 2, "{open}");
    assert_eq!(claim_object(root, &b)["status"], VALIDATING);
    let work = artifact_object(root, None, &output);
    assert_eq!(inline_text(&work), PROOF);
    let response_b = response_object(root, &testament_b);
    assert_eq!(response_b["state"], "Validating", "{response_b}");
    assert_eq!(response_b["outcome"], 1, "{response_b}");

    // The retry on attempt two passes: the claim is satisfied and the error
    // report remains as evidence beside the passing one.
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &b,
            "--validation",
            &b_check,
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    let pass_report = created(&result, "Artifact").remove(0);
    let passed = artifact_object(root, None, &pass_report);
    assert_eq!(
        passed["result_provenance"]["value"], VERDICT_PASS,
        "{passed}"
    );
    assert_eq!(
        passed["result_provenance"]["attempt"]["index"], 1,
        "{passed}"
    );
    assert_eq!(claim_object(root, &b)["status"], SATISFIED);
    assert!(
        evaluations(root, &b_check)
            .iter()
            .any(|evaluation| evaluation["state"] == "Validated")
    );
    assert_eq!(artifact_object(root, None, &error_report), errored);
    // The distinction is in the record: A's respondent reported Failed with a
    // work diagnostic; B's respondent reported Complete and only the
    // evaluator's first attempt errored.
    assert_eq!(
        response_object(root, &testament_a)["outcome"],
        OUTCOME_FAILED
    );
    assert_eq!(response_object(root, &testament_b)["outcome"], 1);
    assert_eq!(claim_object(root, &a)["status"], VALIDATION_INCOMPLETE);
    let a_reports: Vec<Value> = evaluations(root, &a_check);
    assert!(
        a_reports
            .iter()
            .all(|evaluation| evaluation["state"] != "Errored"),
        "{a_reports:?}"
    );

    // Kill the node without warning: both histories, the diagnostic bytes,
    // the report artifacts and every journaled receipt survive unchanged.
    let a_check_before = cli(root, None, &["get", "validation", &a_check]);
    let b_check_before = cli(root, None, &["get", "validation", &b_check]);
    let b_before = claim_object(root, &b);
    drop(server);
    let server = start(root, &advertise);
    assert_eq!(claim_object(root, &a), a_before);
    assert_eq!(claim_object(root, &b), b_before);
    assert_eq!(response_object(root, &testament_a), response_after_entry);
    assert_eq!(artifact_object(root, None, &diagnostic), read_by_claimant);
    assert_eq!(artifact_object(root, None, &error_report), errored);
    assert_eq!(artifact_object(root, None, &pass_report), passed);
    assert_eq!(
        objects(&cli(root, None, &["get", "validation", &a_check])),
        objects(&a_check_before)
    );
    assert_eq!(
        objects(&cli(root, None, &["get", "validation", &b_check])),
        objects(&b_check_before)
    );
    let lost_after = cli(
        alice_root,
        alice_ctx,
        &["request", "inspect", "--operation-id", &lost_id],
    );
    assert_eq!(lost_after["condition"], "Committed", "{lost_after}");
    assert_eq!(
        lost_after["result"]["receipt"],
        replayed["result"]["receipt"]
    );
    let error_after = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &error_id],
    );
    assert_eq!(error_after["condition"], "Committed", "{error_after}");
    assert_eq!(
        created(&error_after["result"], "Artifact"),
        vec![error_report.clone()]
    );
    // Nothing elapsed-time-driven changed either claim: no corrective claim
    // or follow-up exists unless a participant authors one (R5).
    let claims = objects(&cli(root, None, &["get", "claim", &b]))
        .iter()
        .filter(|object| object.get("Claim").is_some())
        .count();
    assert_eq!(claims, 1);
    let _ = issuer;
    drop(server);
}
