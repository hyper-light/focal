#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! The first product gate on the native engine through the MCP adapter: a
//! real binary, two real participants each behind their own `mcp serve`,
//! offline activation, the complete claim cycle through native tools, a
//! killed and restarted node, identical reads afterwards, explicit
//! acknowledgment and exact retries of journaled frames (REMAINING §9 A1).
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
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
    // A fresh directory is already owner-only on Windows (its DACL is inherited
    // from the owner-owned temp root); on Unix, tighten it to 0700.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    #[cfg(not(unix))]
    let _ = path;
}
fn scratch(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    // Unix keeps the path short for the Unix-socket path limit (/tmp); Windows
    // names its pipe by a hash of the data directory, so the default temp root
    // is fine there.
    #[cfg(unix)]
    {
        builder.tempdir_in("/tmp").unwrap()
    }
    #[cfg(not(unix))]
    {
        builder.tempdir().unwrap()
    }
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
    fn names(&mut self) -> Vec<String> {
        let mut names = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let params = match &cursor {
                Some(cursor) => json!({"cursor": cursor}),
                None => json!({}),
            };
            let page = self.rpc("tools/list", params);
            for tool in page["tools"].as_array().unwrap() {
                names.push(tool["name"].as_str().unwrap().to_string());
            }
            match page["nextCursor"].as_str() {
                Some(next) => cursor = Some(next.to_string()),
                None => return names,
            }
        }
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

#[test]
fn two_participants_complete_a_native_claim_cycle_through_mcp_and_survive_a_kill() {
    let founder = scratch("focal-native-mcp-a1-");
    let client = scratch("focal-native-mcp-a1-client-");
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

    // Each participant's adapter probes the engine once and serves the
    // native catalogue: version 2 tools, exact reads and the n1: recovery
    // tools; no V1 application, managed, transfer or watch tool.
    let mut issuer = Mcp::open(root, None);
    let mut alice = Mcp::open(client.path(), Some("alice"));
    let names = issuer.names();
    for name in [
        "claim.submit",
        "claim.get",
        "ledger.standing",
        "validation.report",
        "request.pending",
        "request.acknowledge",
        "claim.release_scope",
        "receipt.adopt",
        "artifact.fail",
        "artifact.receive",
        "artifact.reject",
        "validation.seal_increments",
        "validation.enter_whole_work",
        "audit.generate",
        "audit.post",
        "monitor.register",
        "monitor.rebind",
        "monitor.cancel",
        "validation.context",
    ] {
        assert!(names.iter().any(|n| n == name), "{name} missing: {names:?}");
    }
    for name in [
        "ledger.traverse",
        "request.reserve",
        "request.seal",
        "upload.begin",
    ] {
        assert!(
            !names.iter().any(|n| n == name),
            "{name} present: {names:?}"
        );
    }
    // Durable watches are offered on the native engine.
    for name in [
        "watch.open",
        "watch.next",
        "watch.acknowledge",
        "watch.inspect",
    ] {
        assert!(names.iter().any(|n| n == name), "{name} missing: {names:?}");
    }
    let standing = issuer.read("ledger.standing", json!({}));
    let standing = &objects(&standing)[0]["Standing"];
    assert_eq!(standing["profile"], "AuthoredV1", "{standing}");
    let issuer_id = hex_hash(&standing["principal"]);
    let alice_standing = alice.read("ledger.standing", json!({}));
    let alice_id = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);
    assert_ne!(alice_id, issuer_id);

    // Issuer authors and posts the claim.
    let claim_document = json!({
        "description": "Run the suite and deliver the report.",
        "target": alice_id,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": 4_102_444_800_000u64}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    let (_, result) = issuer.committed("claim.submit", claim_document.clone(), false);
    let claim = created(&result, "Claim").remove(0);
    let validation = created(&result, "Validation").remove(1);
    let (post_id, _) = issuer.committed("claim.post", json!({"claim": claim}), true);
    let page = issuer.read("claim.get", json!({"id": claim}));
    assert_eq!(objects(&page)[0]["Claim"]["status"], 2, "{page}");

    // A child cause through the adapter: the issuer may cite the posted claim
    // as the cause of a follow-up; the respondent holds no receipt yet and is
    // refused with a typed outcome, never with an unknown one.
    let mut follow_up = claim_document.clone();
    follow_up["parent"] = json!(claim);
    follow_up["description"] = json!("Follow-up: collect the coverage report.");
    let (_, child_result) = issuer.committed("claim.submit", follow_up.clone(), false);
    let child = created(&child_result, "Claim").remove(0);
    let child_page = issuer.read("claim.get", json!({"id": child}));
    assert_eq!(
        hex_hash(&objects(&child_page)[0]["Claim"]["cause"]["Claim"]),
        claim,
        "{child_page}"
    );
    let mut stranger_follow_up = follow_up;
    stranger_follow_up["target"] = json!(issuer_id);
    let stranger = alice.raw("claim.submit", stranger_follow_up);
    assert_eq!(stranger["isError"], true, "{stranger}");
    assert_eq!(
        stranger["structuredContent"]["result"]["kind"], "native_refused",
        "{stranger}"
    );
    assert_eq!(
        stranger["structuredContent"]["condition"], "Error",
        "{stranger}"
    );

    // Respondent acquires the receipt and delivers the work.
    alice.committed("receipt.acquire", json!({"claim": claim}), false);
    let (_, result) = alice.committed(
        "artifact.submit",
        json!({"claim": claim, "slot": 0, "payload": {"type": "text", "text": PROOF}}),
        false,
    );
    let artifact = created(&result, "Artifact").remove(0);
    let page = alice.read("artifact.get", json!({"id": artifact}));
    let hash = hex_hash(&objects(&page)[0]["Artifact"]["content_hash"]);
    let (_, result) = alice.committed(
        "testament.submit",
        json!({"claim": claim, "summary": "Suite passed.", "confidence": "committed", "outcome": "complete",
               "manifest": [{"slot": 0, "artifact": {"id": artifact, "hash": hash}}]}),
        false,
    );
    let testament = created(&result, "Response").remove(0);
    alice.committed(
        "testament.post",
        json!({"claim": claim, "testament": testament}),
        false,
    );
    let page = alice.read("testament.get", json!({"id": testament}));
    assert!(
        page["result"]["page"]["objects"][0]
            .get("Response")
            .is_some(),
        "{page}"
    );

    // Issuer receives, evaluates and reports; acceptance is derived.
    issuer.committed(
        "testament.receive",
        json!({"claim": claim, "testament": testament}),
        false,
    );
    issuer.committed(
        "validation.begin",
        json!({"claim": claim, "validation": validation}),
        false,
    );
    let (report_id, report) = issuer.committed(
        "validation.report",
        json!({"claim": claim, "validation": validation, "verdict": "pass", "payload": {"type": "text", "text": PROOF}}),
        true,
    );
    let before = issuer.read("claim.get", json!({"id": claim}));
    assert_eq!(objects(&before)[0]["Claim"]["status"], 8, "{before}");
    let evaluations = issuer.read("validation.get", json!({"id": validation}));
    assert!(
        objects(&evaluations)
            .iter()
            .any(|object| object["Evaluation"]["state"] == "Validated"),
        "{evaluations}"
    );

    // The adapter's durable watch on the native engine: an unseeded watch of
    // the claim replays every committed native record as a schema-2 delta,
    // the delivery is acknowledged by its identity, and the next page polls
    // the tail.
    let opened = issuer.call(
        "watch.open",
        json!({"name": "cycle", "claims": [claim], "seed": false, "max_items": 64}),
    );
    assert_eq!(opened["condition"], "Delivery", "{opened}");
    assert_eq!(opened["result"]["kind"], "watch");
    assert_eq!(opened["result"]["status"]["options"]["engine"], "Native");
    let delivery = &opened["result"]["delivery"];
    let events = delivery["page"]["Events"]["page"]["events"]
        .as_array()
        .unwrap_or_else(|| panic!("{opened}"));
    let native: Vec<&Value> = events
        .iter()
        .filter_map(|event| event.get("Delta"))
        .map(|delta| &delta["delta"])
        .collect();
    assert!(!native.is_empty(), "{opened}");
    assert!(native.iter().all(|delta| delta["schema"] == 2), "{opened}");
    assert!(
        native
            .iter()
            .all(|delta| hex_hash(&delta["claim"]) == claim),
        "{opened}"
    );
    assert!(
        native
            .iter()
            .any(|delta| delta["fact"]["Native"]["fact"]["Claim"]["kind"] == "Created"),
        "{opened}"
    );
    assert!(
        native
            .iter()
            .any(|delta| delta["fact"]["Native"]["fact"]["Claim"]["kind"] == "Satisfied"),
        "{opened}"
    );
    let delivery_id = hex_hash(&delivery["id"]);
    let consumed = issuer.call(
        "watch.acknowledge",
        json!({"name": "cycle", "delivery_id": delivery_id}),
    );
    assert_eq!(consumed["condition"], "Consumed", "{consumed}");
    let next = issuer.call("watch.next", json!({"name": "cycle"}));
    assert_eq!(next["condition"], "Delivery", "{next}");
    assert_eq!(next["result"]["delivery"]["number"], 2, "{next}");

    // Unacknowledged results stay listed; retry and inspect reprint them.
    // Outstanding references are listed in identity order.
    let pending = issuer.call("request.pending", json!({}));
    let mut listed: Vec<String> = pending["result"]["operation_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();
    listed.sort();
    let mut expected = vec![post_id.clone(), report_id.clone()];
    expected.sort();
    assert_eq!(listed, expected, "{pending}");
    let retried = issuer.call("request.retry", json!({"operation_id": report_id}));
    assert_eq!(retried["result"], report);

    // Kill the node without warning; both adapters reconnect to the restarted
    // node and every read and journaled result is unchanged.
    drop(server);
    let server = start(root, &advertise);
    let after = issuer.read("claim.get", json!({"id": claim}));
    assert_eq!(objects(&after)[0]["Claim"], objects(&before)[0]["Claim"]);
    let inspected = issuer.call("request.inspect", json!({"operation_id": report_id}));
    assert_eq!(inspected["condition"], "Committed");
    assert_eq!(inspected["result"], report);
    let remote = issuer.call(
        "request.inspect",
        json!({"operation_id": report_id, "remote": true}),
    );
    assert_eq!(remote["condition"], "Observed", "{remote}");
    assert_eq!(
        objects(&remote)[0]["Outcome"]["intent"],
        report["receipt"]["intent"],
        "{remote}"
    );
    // The human CLI observes the adapter's operation through the owner too.
    let observed = admin(
        root,
        None,
        &[
            "request",
            "inspect",
            "--operation-id",
            &report_id,
            "--remote",
            "--format",
            "json",
        ],
    );
    assert_eq!(observed["condition"], "Observed", "{observed}");
    assert_eq!(
        objects(&observed)[0]["Outcome"]["intent"],
        report["receipt"]["intent"]
    );

    // A stale binding after restart is a typed refusal, not an unknown
    // outcome, and it does not stay pending.
    let stale = issuer.raw("claim.post", json!({"claim": claim}));
    assert_eq!(stale["isError"], true, "{stale}");
    let refusal = &stale["structuredContent"];
    assert_eq!(refusal["schema_version"], 2);
    assert_eq!(refusal["result"]["kind"], "native_refused", "{refusal}");
    let stale_id = refusal["operation_id"].as_str().unwrap().to_string();
    let inspected = issuer.raw("request.inspect", json!({"operation_id": stale_id}));
    assert_eq!(
        inspected["structuredContent"]["result"]["kind"],
        "native_refused"
    );

    // Acknowledgment retires the listed results; a second acknowledgment
    // of the same result is idempotent.
    for id in [&post_id, &report_id] {
        let consumed = issuer.call("request.acknowledge", json!({"operation_id": id}));
        assert_eq!(consumed["condition"], "Consumed");
    }
    let consumed = issuer.call("request.acknowledge", json!({"operation_id": report_id}));
    assert_eq!(consumed["condition"], "Consumed");
    let pending = issuer.call("request.pending", json!({}));
    assert_eq!(pending["result"]["operation_ids"], json!([]));

    // A reply lost between adapter and agent: the adapter is killed after
    // the owner commits; a fresh adapter on the same journal lists the
    // operation and its exact retry finds the same receipt.
    let mut second = claim_document_for(&alice_id);
    second["operation_id"] = json!("n1:000000000000000000000000000000ab");
    let (lost_id, lost) = issuer.committed("claim.submit", second, true);
    drop(issuer);
    let mut issuer = Mcp::open(root, None);
    let pending = issuer.call("request.pending", json!({}));
    assert_eq!(pending["result"]["operation_ids"], json!([lost_id]));
    let replayed = issuer.call("request.retry", json!({"operation_id": lost_id}));
    assert_eq!(replayed["result"], lost);
    // The same reference with the same input resumes; different input under
    // the same reference is refused before any identity is minted.
    let mut same = claim_document_for(&alice_id);
    same["operation_id"] = json!("n1:000000000000000000000000000000ab");
    let resumed = issuer.call("claim.submit", same);
    assert_eq!(resumed["result"], lost);
    let mut changed = claim_document_for(&alice_id);
    changed["operation_id"] = json!("n1:000000000000000000000000000000ab");
    changed["description"] = json!("A different request under the same reference.");
    let conflict = issuer.raw("claim.submit", changed);
    assert_eq!(conflict["isError"], true, "{conflict}");
    assert_eq!(
        conflict["structuredContent"]["result"]["code"], "operation_conflict",
        "{conflict}"
    );
    drop(alice);
    drop(issuer);
    drop(server);
}
fn claim_document_for(target: &str) -> Value {
    json!({
        "description": "A second request.",
        "target": target,
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}]
    })
}
