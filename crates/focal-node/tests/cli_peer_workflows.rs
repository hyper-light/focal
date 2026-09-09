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
//! The peer workflows through the real binary on one native node: a
//! consultation answered and followed up within its policy, a challenge
//! disputing the exact answer that fails its evaluator's verdict, the
//! correction that rests on that verdict under the challenge's policy, the
//! typed refusals around them, lineage reads, the `testament` wait, a lost
//! reply resolved by identity, and a kill-and-restart that changes none of it.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    net::UdpSocket,
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
fn address() -> String {
    for port in 26_000..30_000u16 {
        let candidate = format!("127.0.0.1:{port}");
        if UdpSocket::bind(&candidate).is_ok() && std::net::TcpListener::bind(&candidate).is_ok() {
            return candidate;
        }
    }
    panic!("no free port")
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
fn parse_output(args: &[&str], output: &Output) -> Value {
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
fn cli(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    parse_output(&args, &run(root, context, &args))
}
fn admin(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    parse_output(args, &run(root, context, args))
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
        .map(|entry| hex_hash(&entry["id"]))
        .collect()
}
fn objects(page: &Value) -> &Vec<Value> {
    assert_eq!(page["result"]["kind"], "native_read", "{page}");
    page["result"]["page"]["objects"].as_array().unwrap()
}
fn hex_hash(value: &Value) -> String {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{value}"))
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
/// A refused command: the structured error and the exit code.
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
    (output.status.code().unwrap(), value)
}
fn code_of(value: &Value) -> &str {
    value["result"]["code"]
        .as_str()
        .or_else(|| value["error"]["code"].as_str())
        .unwrap_or_else(|| panic!("no refusal code: {value}"))
}
const FAR: u64 = 4_102_444_800_000;
const ANSWER: &str = r#"{"passed":2,"failed":0,"skipped":1}"#;
const PROOF: &str = r#"{"passed":1,"failed":0,"skipped":0}"#;
const FAILED: &str = r#"{"passed":0,"failed":1,"skipped":0}"#;

fn enroll(root: &Path, client: &Path, name: &str) -> String {
    let invitation = client.join(format!("{name}.invite"));
    admin(
        root,
        None,
        &[
            "cluster",
            "client",
            "invite",
            "--name",
            name,
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    let enrolled = run(
        client,
        None,
        &[
            "context",
            "enroll",
            name,
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(
        enrolled.status.success(),
        "enroll {name}: {}",
        String::from_utf8_lossy(&enrolled.stderr)
    );
    let standing = admin(client, Some(name), &["status"]);
    hex_hash(&objects(&standing)[0]["Standing"]["principal"])
}
fn receipt_check() -> String {
    json!({"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}})
        .to_string()
}
fn test_check(evaluator: &str) -> String {
    json!({"kind": "test", "description": "The proof holds.",
        "target": {"type": "slot", "index": 0, "name": "proof"},
        "evaluator": evaluator,
        "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77), "attempts": 1}],
        "deadline": {"at": FAR}})
    .to_string()
}
fn claim_object(root: &Path, claim: &str) -> Value {
    let page = cli(root, None, &["get", "claim", claim]);
    objects(&page)[0]["Claim"].clone()
}
fn artifact_hash(root: &Path, context: Option<&str>, artifact: &str) -> String {
    let page = cli(root, context, &["get", "artifact", artifact]);
    hex_hash(&objects(&page)[0]["Artifact"]["binding"]["content"])
}
/// The respondent's full cycle on one claim: receipt, one work artifact in
/// slot 0, a complete testament, posted; the issuer then receives it.
fn respond(root: &Path, client: &Path, holder: &str, claim: &str, payload: &str) -> String {
    let context = Some(holder);
    committed(&cli(client, context, &["receipt", "acquire", claim]));
    let (_, result) = committed(&cli(
        client,
        context,
        &[
            "artifact", "submit", "--claim", claim, "--slot", "0", "--text", payload,
        ],
    ));
    let artifact = created(&result, "Artifact").remove(0);
    let hash = artifact_hash(client, context, &artifact);
    let (_, result) = committed(&cli(
        client,
        context,
        &[
            "testament",
            "submit",
            "--claim",
            claim,
            "--summary",
            "Done.",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
            "--slot",
            &format!("0={artifact}:{hash}"),
        ],
    ));
    let testament = created(&result, "Response").remove(0);
    committed(&cli(
        client,
        context,
        &["testament", "post", &testament, "--claim", claim],
    ));
    committed(&cli(
        root,
        None,
        &["testament", "receive", &testament, "--claim", claim],
    ));
    artifact
}
fn lineage_ids(root: &Path, claim: &str) -> Vec<String> {
    let page = cli(root, None, &["claim", "lineage", claim]);
    assert_eq!(page["condition"], "Lineage", "{page}");
    objects(&page)
        .iter()
        .map(|object| hex_hash(&object["Claim"]["binding"]["object"]))
        .collect()
}

#[test]
fn peer_workflows_run_through_the_cli_with_typed_refusals_identity_and_restart() {
    let founder = tempfile::Builder::new()
        .prefix("focal-peer-cli-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-peer-cli-client-")
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
    let alice = enroll(root, client.path(), "alice");
    let eve = enroll(root, client.path(), "eve");
    assert!(alice != issuer && eve != issuer && eve != alice);

    // ---- A consultation, answered, observed and followed up ----
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "claim",
            "consult",
            "--target",
            &alice,
            "--description",
            "Which cases does the parser leave undefined?",
            "--validation-json",
            &receipt_check(),
            "--slot-json",
            r#"{"slot":0,"checks":[]}"#,
            "--policy-json",
            r#"{"max_follow_ups":1,"escalation":"none"}"#,
        ],
    ));
    let consult = created(&result, "Claim").remove(0);
    let consult_object = claim_object(root, &consult);
    // Frozen vocabularies read back as codes: consultation is action 2.
    assert_eq!(consult_object["content"]["action"], 2, "{consult_object}");
    assert_eq!(
        consult_object["content"]["policy"]["max_follow_ups"], 1,
        "{consult_object}"
    );
    committed(&cli(root, None, &["claim", "post", &consult]));
    committed(&cli(
        client.path(),
        Some("alice"),
        &["receipt", "acquire", &consult],
    ));
    let (_, result) = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "artifact", "submit", "--claim", &consult, "--slot", "0", "--text", ANSWER,
        ],
    ));
    let answer = created(&result, "Artifact").remove(0);
    let answer_hash = artifact_hash(client.path(), Some("alice"), &answer);
    let (_, result) = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "testament",
            "submit",
            "--claim",
            &consult,
            "--summary",
            "Answered.",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
            "--slot",
            &format!("0={answer}:{answer_hash}"),
        ],
    ));
    let answer_testament = created(&result, "Response").remove(0);
    committed(&cli(
        client.path(),
        Some("alice"),
        &["testament", "post", &answer_testament, "--claim", &consult],
    ));
    // Before the issuer receives it, a short testament wait ends Pending.
    let (code, pending) = refused(
        root,
        None,
        &[
            "claim",
            "wait",
            &consult,
            "--until",
            "testament",
            "--timeout-ms",
            "1500",
        ],
    );
    assert_ne!(code, 0);
    assert_eq!(pending["condition"], "Pending", "{pending}");
    assert_eq!(pending["result"]["kind"], "native_wait", "{pending}");
    committed(&cli(
        root,
        None,
        &[
            "testament",
            "receive",
            &answer_testament,
            "--claim",
            &consult,
        ],
    ));
    let met = cli(
        root,
        None,
        &[
            "claim",
            "wait",
            &consult,
            "--until",
            "testament",
            "--timeout-ms",
            "5000",
        ],
    );
    assert_eq!(met["condition"], "Met", "{met}");
    assert_eq!(met["result"]["result"]["probes"], 1, "{met}");

    // One follow-up is admitted; its identity derives from the facts, so a
    // repeated command is the same claim; a second distinct follow-up is
    // refused by the policy, and the subject may not file one.
    fn follow_args<'a>(consult: &'a str, description: &'a str, receipt: &'a str) -> Vec<&'a str> {
        vec![
            "claim",
            "follow-up",
            "--refines",
            consult,
            "--description",
            description,
            "--validation-json",
            receipt,
        ]
    }
    let receipt = receipt_check();
    let args = follow_args(&consult, "And the unicode cases?", &receipt);
    let (_, result) = committed(&cli(root, None, &args));
    let follow_up = created(&result, "Claim").remove(0);
    let (_, again) = committed(&cli(root, None, &args));
    assert_eq!(created(&again, "Claim").remove(0), follow_up);
    let follow_object = claim_object(root, &follow_up);
    assert_eq!(
        hex_hash(&follow_object["subject"]),
        alice,
        "{follow_object}"
    );
    let second = follow_args(&consult, "And the surrogate pairs?", &receipt);
    let (code, refusal) = refused(root, None, &second);
    assert_eq!(code, 2, "{refusal}");
    assert_eq!(code_of(&refusal), "invalid_policy", "{refusal}");
    let (code, refusal) = refused(
        client.path(),
        Some("alice"),
        &[
            "claim",
            "follow-up",
            "--refines",
            &consult,
            "--target",
            &issuer,
            "--description",
            "May I ask back?",
            "--validation-json",
            &receipt,
        ],
    );
    assert_eq!(code, 3, "{refusal}");
    assert_eq!(code_of(&refusal), "unauthorized", "{refusal}");
    assert_eq!(
        lineage_ids(root, &consult),
        vec![consult.clone(), follow_up.clone()]
    );

    // ---- A challenge disputing the exact answer, failed by its evaluator ----
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "claim",
            "challenge",
            "--target",
            &alice,
            "--artifact",
            &answer,
            "--description",
            "Prove the undefined cases are covered.",
            "--validation-json",
            &receipt,
            "--validation-json",
            &test_check(&eve),
            "--slot-json",
            r#"{"slot":0,"checks":[{"declaration":1}]}"#,
            "--policy-json",
            r#"{"corrective_allowed":true,"max_follow_ups":0,"single_issuer":true,"escalation":"evaluator"}"#,
        ],
    ));
    let challenge = created(&result, "Claim").remove(0);
    let check = created(&result, "Validation").remove(1);
    let challenge_object = claim_object(root, &challenge);
    let disputes = challenge_object["content"]["relations"]
        .as_array()
        .unwrap()
        .iter()
        .any(|relation| {
            relation["kind"] == 12
                && hex_hash(&relation["target"]["Evidence"]["id"]) == answer
                && hex_hash(&relation["target"]["Evidence"]["hash"]) == answer_hash
        });
    assert!(disputes, "{challenge_object}");
    committed(&cli(root, None, &["claim", "post", &challenge]));
    let proof = respond(root, client.path(), "alice", &challenge, PROOF);
    committed(&cli(
        client.path(),
        Some("eve"),
        &[
            "validation",
            "begin",
            "--claim",
            &challenge,
            "--validation",
            &check,
        ],
    ));
    let (_, result) = committed(&cli(
        client.path(),
        Some("eve"),
        &[
            "validation",
            "report",
            "--claim",
            &challenge,
            "--validation",
            &check,
            "--verdict",
            "fail",
            "--text",
            FAILED,
        ],
    ));
    let report = created(&result, "Artifact").remove(0);
    let judged = claim_object(root, &challenge);
    let (code, unmet) = refused(
        root,
        None,
        &[
            "claim",
            "wait",
            &challenge,
            "--until",
            "satisfied",
            "--timeout-ms",
            "1000",
        ],
    );
    assert_ne!(code, 0);
    assert_eq!(unmet["condition"], "Unmet", "{unmet}");

    // ---- Corrections under the challenge's policy ----
    let correct =
        |context: &Path, name: Option<&str>, verdict: &str, target: Option<&str>| -> Output {
            let mut args = vec![
                "claim",
                "correct",
                "--challenge",
                &challenge,
                "--verdict",
                verdict,
                "--description",
                "Redo the inspection with the undefined cases.",
                "--validation-json",
                &receipt,
            ];
            if let Some(target) = target {
                args.extend(["--target", target]);
            }
            args.extend(["--format", "json"]);
            run(context, name, &args)
        };
    // The work is not the verdict.
    let output = correct(root, None, &proof, None);
    assert!(!output.status.success());
    let refusal: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(2), "{refusal}");
    assert_eq!(code_of(&refusal), "missing_evidence", "{refusal}");
    // The reporting evaluator corrects; the same command again is the same
    // claim; the exact retry of the first reference is its committed receipt.
    let output = correct(client.path(), Some("eve"), &report, None);
    let first: Value = serde_json::from_slice(&output.stdout).unwrap();
    let (operation, result) = committed(&first);
    let correction = created(&result, "Claim").remove(0);
    let output = correct(client.path(), Some("eve"), &report, None);
    let (_, again) = committed(&serde_json::from_slice(&output.stdout).unwrap());
    assert_eq!(created(&again, "Claim").remove(0), correction);
    let retried = cli(
        client.path(),
        Some("eve"),
        &["request", "retry", "--operation-id", &operation],
    );
    assert_eq!(retried["condition"], "Committed", "{retried}");
    let correction_object = claim_object(root, &correction);
    assert_eq!(
        correction_object["content"]["action"], 9,
        "{correction_object}"
    );
    assert_eq!(hex_hash(&correction_object["issuer"]), eve);
    assert_eq!(hex_hash(&correction_object["subject"]), alice);
    // Under single_issuer the holder's and the issuer's corrections conflict.
    let output = correct(client.path(), Some("alice"), &report, Some(&issuer));
    let refusal: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(5), "{refusal}");
    assert_eq!(code_of(&refusal), "conflicting_cause", "{refusal}");
    let output = correct(root, None, &report, None);
    let refusal: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(code_of(&refusal), "conflicting_cause", "{refusal}");
    // The challenge is terminal and untouched; its lineage names the correction.
    let challenge_after = claim_object(root, &challenge);
    assert_eq!(
        challenge_after["binding"]["revision"], judged["binding"]["revision"],
        "{challenge_after}"
    );
    assert_eq!(
        challenge_after["status"], judged["status"],
        "{challenge_after}"
    );
    assert_eq!(
        lineage_ids(root, &challenge),
        vec![challenge.clone(), correction.clone()]
    );

    // ---- Restart: everything reads the same ----
    drop(server);
    let _server = start(root, &advertise);
    assert_eq!(
        lineage_ids(root, &challenge),
        vec![challenge.clone(), correction.clone()]
    );
    assert_eq!(
        lineage_ids(root, &consult),
        vec![consult.clone(), follow_up.clone()]
    );
    let terminal = cli(
        root,
        None,
        &[
            "claim",
            "wait",
            &challenge,
            "--until",
            "terminal",
            "--timeout-ms",
            "5000",
        ],
    );
    assert_eq!(terminal["condition"], "Met", "{terminal}");
    let output = correct(root, None, &report, None);
    let refusal: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(code_of(&refusal), "conflicting_cause", "{refusal}");
}
