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
//! The peer workflows through the MCP adapter on one native node, with three
//! adapters (issuer, subject, evaluator): a consultation answered, observed
//! with the `testament` wait and followed up within its policy; a challenge
//! disputing the exact answer, failed by its evaluator; the correction that
//! rests on that verdict; typed refusals; a lost reply resolved by identity;
//! cancellation of a follow-up; lineage reads; and a kill-and-restart.
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
fn enroll(root: &Path, client: &Path, name: &str) {
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
}

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
        let result = self.output.recv_timeout(Duration::from_secs(40)).unwrap();
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
    fn committed(&mut self, name: &str, args: Value) -> (String, Value) {
        let value = self.call(name, args);
        assert_eq!(value["condition"], "Committed", "{name}: {value}");
        assert_eq!(value["schema_version"], 2);
        assert_eq!(value["result"]["kind"], "native");
        let id = value["operation_id"].as_str().unwrap().to_string();
        assert!(id.starts_with("n1:"));
        let acknowledged = self.call("request.acknowledge", json!({"operation_id": id}));
        assert_eq!(acknowledged["condition"], "Consumed");
        (id, value["result"].clone())
    }
    fn read(&mut self, name: &str, args: Value) -> Value {
        let value = self.call(name, args);
        assert_eq!(value["schema_version"], 2);
        assert_eq!(value["result"]["kind"], "native_read", "{value}");
        value
    }
    fn claim(&mut self, id: &str) -> Value {
        objects(&self.read("claim.get", json!({"id": id})))[0]["Claim"].clone()
    }
    fn artifact_hash(&mut self, id: &str) -> String {
        hex_hash(
            &objects(&self.read("artifact.get", json!({"id": id})))[0]["Artifact"]["binding"]["content"],
        )
    }
    fn lineage(&mut self, id: &str) -> Vec<String> {
        let page = self.read("claim.lineage", json!({"id": id}));
        assert_eq!(page["condition"], "Read", "{page}");
        objects(&page)
            .iter()
            .map(|object| hex_hash(&object["Claim"]["binding"]["object"]))
            .collect()
    }
    /// The wait observer's structured result, whatever its condition.
    fn wait(&mut self, claim: &str, until: &str, timeout_ms: u32) -> Value {
        let value = self.call(
            "claim.wait",
            json!({"claim": claim, "until": until, "timeout_ms": timeout_ms}),
        );
        assert_eq!(value["result"]["kind"], "native_wait", "{value}");
        value
    }
    fn refused(&mut self, name: &str, args: Value) -> (String, Value) {
        let result = self.raw(name, args);
        assert_eq!(result["isError"], true, "tool {name}: {result}");
        let content = result["structuredContent"].clone();
        assert_eq!(content["condition"], "Error", "{content}");
        // Client-side and ingress refusals carry `result.code`; the owner's
        // typed refusals name their code under `result.refusal.kind.Refused`.
        let code = content["result"]["code"]
            .as_str()
            .or_else(|| content["result"]["refusal"]["kind"]["Refused"].as_str())
            .unwrap_or_else(|| panic!("{content}"))
            .to_string();
        (code, content)
    }
    /// The respondent's full cycle: receipt, one work artifact in slot 0, a
    /// complete testament, posted. Returns the artifact and the testament.
    fn respond(&mut self, claim: &str, payload: &str) -> (String, String) {
        self.committed("receipt.acquire", json!({"claim": claim}));
        let (_, result) = self.committed(
            "artifact.submit",
            json!({"claim": claim, "slot": 0, "payload": {"type": "text", "text": payload}}),
        );
        let artifact = created(&result, "Artifact").remove(0);
        let hash = self.artifact_hash(&artifact);
        let (_, result) = self.committed(
            "testament.submit",
            json!({"claim": claim, "summary": "Done.", "confidence": "committed", "outcome": "complete",
                   "manifest": [{"slot": 0, "artifact": {"id": artifact, "hash": hash}}]}),
        );
        let testament = created(&result, "Response").remove(0);
        self.committed(
            "testament.post",
            json!({"claim": claim, "testament": testament}),
        );
        (artifact, testament)
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
        .unwrap_or_else(|| panic!("{value}"))
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
const FAR: u64 = 4_102_444_800_000;
const ANSWER: &str = r#"{"passed":2,"failed":0,"skipped":1}"#;
const PROOF: &str = r#"{"passed":1,"failed":0,"skipped":0}"#;
const FAILED: &str = r#"{"passed":0,"failed":1,"skipped":0}"#;
fn receipt_check() -> Value {
    json!({"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}})
}
fn test_check(evaluator: &str) -> Value {
    json!({"kind": "test", "description": "The proof holds.",
        "target": {"type": "slot", "index": 0, "name": "proof"},
        "evaluator": evaluator,
        "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77), "attempts": 1}],
        "deadline": {"at": FAR}})
}
fn principal(mcp: &mut Mcp) -> String {
    let standing = mcp.read("ledger.standing", json!({}));
    hex_hash(&objects(&standing)[0]["Standing"]["principal"])
}

#[test]
fn peer_workflows_run_through_mcp_with_typed_refusals_identity_cancellation_and_restart() {
    let founder = tempfile::Builder::new()
        .prefix("focal-peer-mcp-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-peer-mcp-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    enroll(root, client.path(), "alice");
    enroll(root, client.path(), "eve");
    let mut issuer = Mcp::open(root, None);
    let mut alice = Mcp::open(client.path(), Some("alice"));
    let mut eve = Mcp::open(client.path(), Some("eve"));
    let issuer_id = principal(&mut issuer);
    let alice_id = principal(&mut alice);
    let eve_id = principal(&mut eve);
    assert!(alice_id != issuer_id && eve_id != alice_id);

    // ---- Consultation ----
    let (_, result) = issuer.committed(
        "claim.consult",
        json!({"description": "Which cases does the parser leave undefined?", "target": alice_id,
               "validations": [receipt_check()], "slots": [{"slot": 0, "checks": []}],
               "policy": {"max_follow_ups": 1, "escalation": "none"}}),
    );
    let consult = created(&result, "Claim").remove(0);
    let consult_object = issuer.claim(&consult);
    // Frozen vocabularies read back as codes: consultation is action 2.
    assert_eq!(consult_object["content"]["action"], 2, "{consult_object}");
    assert_eq!(
        consult_object["content"]["policy"]["escalation"], "None",
        "{consult_object}"
    );
    issuer.committed("claim.post", json!({"claim": consult}));
    let (answer, answer_testament) = alice.respond(&consult, ANSWER);
    let pending = issuer.wait(&consult, "testament", 1500);
    assert_eq!(pending["condition"], "Pending", "{pending}");
    issuer.committed(
        "testament.receive",
        json!({"claim": consult, "testament": answer_testament}),
    );
    let met = issuer.wait(&consult, "testament", 5000);
    assert_eq!(met["condition"], "Met", "{met}");
    assert_eq!(met["result"]["result"]["probes"], 1, "{met}");

    // One follow-up, the same one twice, a second refused, the subject refused.
    let follow_up = |description: &str| json!({"refines": consult, "description": description, "validations": [receipt_check()]});
    let (_, result) = issuer.committed("claim.follow_up", follow_up("And the unicode cases?"));
    let follow = created(&result, "Claim").remove(0);
    let (_, again) = issuer.committed("claim.follow_up", follow_up("And the unicode cases?"));
    assert_eq!(created(&again, "Claim").remove(0), follow);
    assert_eq!(hex_hash(&issuer.claim(&follow)["subject"]), alice_id);
    let (code, refusal) = issuer.refused("claim.follow_up", follow_up("And the surrogate pairs?"));
    assert_eq!(code, "InvalidPolicy", "{refusal}");
    let (code, refusal) = alice.refused(
        "claim.follow_up",
        json!({"refines": consult, "target": issuer_id, "description": "May I ask back?", "validations": [receipt_check()]}),
    );
    assert_eq!(code, "WrongActor", "{refusal}");
    assert_eq!(
        issuer.lineage(&consult),
        vec![consult.clone(), follow.clone()]
    );
    // Cancelling the generated follow-up is an explicit business decision;
    // the wait sees it terminal at once.
    issuer.committed("claim.cancel", json!({"claim": follow}));
    let cancelled = issuer.wait(&follow, "terminal", 5000);
    assert_eq!(cancelled["condition"], "Met", "{cancelled}");
    let unmet = issuer.wait(&follow, "testament", 5000);
    assert_eq!(unmet["condition"], "Unmet", "{unmet}");

    // ---- Challenge disputing the exact answer ----
    let (_, result) = issuer.committed(
        "claim.challenge",
        json!({"description": "Prove the undefined cases are covered.", "target": alice_id, "artifact": answer,
               "validations": [receipt_check(), test_check(&eve_id)],
               "slots": [{"slot": 0, "checks": [{"declaration": 1}]}],
               "policy": {"corrective_allowed": true, "max_follow_ups": 0, "single_issuer": true, "escalation": "evaluator"}}),
    );
    let challenge = created(&result, "Claim").remove(0);
    let check = created(&result, "Validation").remove(1);
    let answer_hash = alice.artifact_hash(&answer);
    let challenge_object = issuer.claim(&challenge);
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
    issuer.committed("claim.post", json!({"claim": challenge}));
    let (proof, proof_testament) = alice.respond(&challenge, PROOF);
    issuer.committed(
        "testament.receive",
        json!({"claim": challenge, "testament": proof_testament}),
    );
    eve.committed(
        "validation.begin",
        json!({"claim": challenge, "validation": check}),
    );
    let (_, result) = eve.committed(
        "validation.report",
        json!({"claim": challenge, "validation": check, "verdict": "fail", "payload": {"type": "text", "text": FAILED}}),
    );
    let report = created(&result, "Artifact").remove(0);
    let unmet = issuer.wait(&challenge, "satisfied", 1000);
    assert_eq!(unmet["condition"], "Unmet", "{unmet}");
    let judged = issuer.claim(&challenge);

    // ---- Corrections ----
    let correction = |verdict: &str, target: Option<&str>| {
        let mut document = json!({"challenge": challenge, "verdict": verdict,
            "description": "Redo the inspection with the undefined cases.",
            "validations": [receipt_check()]});
        if let Some(target) = target {
            document["target"] = json!(target);
        }
        document
    };
    let (code, refusal) = issuer.refused("claim.correct", correction(&proof, None));
    assert_eq!(code, "MissingEvidence", "{refusal}");
    let (operation, result) = eve.committed("claim.correct", correction(&report, Some(&issuer_id)));
    let corrected = created(&result, "Claim").remove(0);
    let (_, again) = eve.committed("claim.correct", correction(&report, Some(&issuer_id)));
    assert_eq!(created(&again, "Claim").remove(0), corrected);
    // The owner's committed outcome for the consumed reference is readable
    // by identity: one creation at its native position.
    let observed = eve.call(
        "request.inspect",
        json!({"operation_id": operation, "remote": true}),
    );
    assert_eq!(observed["condition"], "Observed", "{observed}");
    let outcome = &objects(&observed)[0]["Outcome"];
    assert_eq!(outcome["operation"], "Create", "{observed}");
    assert_eq!(outcome["counts"]["created"], 1, "{observed}");
    let (code, refusal) = alice.refused("claim.correct", correction(&report, Some(&issuer_id)));
    assert_eq!(code, "ConflictingCause", "{refusal}");
    let (code, refusal) = issuer.refused("claim.correct", correction(&report, None));
    assert_eq!(code, "ConflictingCause", "{refusal}");
    let corrected_object = eve.claim(&corrected);
    assert_eq!(
        corrected_object["content"]["action"], 9,
        "{corrected_object}"
    );
    assert_eq!(hex_hash(&corrected_object["issuer"]), eve_id);
    let challenge_after = issuer.claim(&challenge);
    assert_eq!(
        challenge_after["binding"]["revision"],
        judged["binding"]["revision"]
    );
    assert_eq!(challenge_after["status"], judged["status"]);
    assert_eq!(
        issuer.lineage(&challenge),
        vec![challenge.clone(), corrected.clone()]
    );
    assert_eq!(alice.lineage(&corrected), vec![corrected.clone()]);

    // ---- Restart ----
    drop(server);
    let _server = start(root, &advertise);
    let mut issuer = Mcp::open(root, None);
    assert_eq!(
        issuer.lineage(&challenge),
        vec![challenge.clone(), corrected.clone()]
    );
    assert_eq!(
        issuer.lineage(&consult),
        vec![consult.clone(), follow.clone()]
    );
    let terminal = issuer.wait(&challenge, "terminal", 5000);
    assert_eq!(terminal["condition"], "Met", "{terminal}");
    let (code, _) = issuer.refused("claim.correct", correction(&report, None));
    assert_eq!(code, "ConflictingCause");
}
