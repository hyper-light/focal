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
//! The fourth product gate on the native engine through the MCP adapter:
//! adapters and CLI processes of both participants submitting at once on
//! their own durable journals, a tool call cancelled by the agent, replies
//! lost before and after commitment by crash cuts at the node's durable
//! boundaries, node restarts, exact reconciliation, and exhausted admission
//! capacity that still answers exact retries of committed work
//! (REMAINING §9 A4).
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    net::UdpSocket,
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
        let began = std::time::Instant::now();
        let result = self
            .output
            .recv_timeout(Duration::from_secs(120))
            .unwrap_or_else(|error| panic!("{method} {id}: {error} after {:?}", began.elapsed()));
        eprintln!("rpc {method} {id} answered after {:?}", began.elapsed());
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
const FAR: u64 = 4_102_444_800_000;

fn start_with(root: &Path, advertise: &str, env: &[(&str, &str)]) -> Server {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command
        .args([
            "--data-dir",
            root.to_str().unwrap(),
            "start",
            "--advertise",
            advertise,
        ])
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().unwrap();
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
fn claim_document(target: &str, description: &str) -> Value {
    json!({
        "description": description,
        "target": target,
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]
    })
}
fn sequence(root: &Path) -> u64 {
    admin(root, None, &["status"])["result"]["page"]["native_sequence"]
        .as_u64()
        .unwrap()
}
fn claims_of(root: &Path, issuer: &str) -> usize {
    let page = admin(
        root,
        None,
        &["list", "claims", "--source", issuer, "--format", "json"],
    );
    assert_eq!(page["result"]["kind"], "native_list", "{page}");
    page["result"]["page"]["objects"].as_array().unwrap().len()
}
fn pending_ids(value: &Value) -> Vec<String> {
    value["result"]["operation_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect()
}
impl Mcp {
    fn send(&mut self, value: &Value) {
        serde_json::to_writer(&mut self.input, value).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
    }
    /// A tool call that failed with a structured failure and its code.
    fn failed(&mut self, name: &str, args: Value) -> (String, Value) {
        let result = self.raw(name, args);
        assert_eq!(result["isError"], true, "tool {name}: {result}");
        let content = result["structuredContent"].clone();
        (content["condition"].as_str().unwrap().to_string(), content)
    }
}

#[test]
fn concurrent_adapters_cancelled_calls_lost_replies_and_exhausted_capacity_reconcile_exactly_once()
{
    let founder = tempfile::Builder::new()
        .prefix("focal-native-mcp-a4-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-mcp-a4-client-")
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
    let issuer_id =
        hex_hash(&objects(&issuer.read("ledger.standing", json!({})))[0]["Standing"]["principal"]);
    let alice_id =
        hex_hash(&objects(&alice.read("ledger.standing", json!({})))[0]["Standing"]["principal"]);

    // ---- Concurrency: both adapters and three CLI processes at once, each
    // on its own durable journal ----
    let before = sequence(root);
    let handles: Vec<_> = (0..3)
        .map(|index| {
            let (dir, context, target) = if index == 1 {
                (
                    client.path().to_path_buf(),
                    Some("alice"),
                    issuer_id.clone(),
                )
            } else {
                (root.to_path_buf(), None, alice_id.clone())
            };
            std::thread::spawn(move || {
                let document = claim_document(&target, &format!("CLI claim {index}.")).to_string();
                let value = admin(
                    &dir,
                    context,
                    &["submit", "claim", "--json", &document, "--format", "json"],
                );
                assert_eq!(value["condition"], "Committed", "{value}");
                value["operation_id"].as_str().unwrap().to_string()
            })
        })
        .collect();
    let (issuer_op, _) = issuer.committed(
        "claim.submit",
        claim_document(&alice_id, "Issuer adapter claim."),
        false,
    );
    let (alice_op, _) = alice.committed(
        "claim.submit",
        claim_document(&issuer_id, "Respondent adapter claim."),
        false,
    );
    let mut ids: Vec<String> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    ids.push(issuer_op);
    ids.push(alice_op);
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 5, "{ids:?}");
    assert_eq!(sequence(root), before + 5);
    assert_eq!(
        pending_ids(&issuer.call("request.pending", json!({}))),
        Vec::<String>::new()
    );
    assert_eq!(
        pending_ids(&alice.call("request.pending", json!({}))),
        Vec::<String>::new()
    );
    assert_eq!(claims_of(root, &issuer_id), 3);
    assert_eq!(claims_of(root, &alice_id), 2);

    // ---- A cancelled transport request: the agent cancels its call; the
    // adapter drops only the wait, and the exact operation is reconciled
    // from the journal afterwards ----
    let before = sequence(root);
    let cancelled = issuer.next;
    issuer.next += 1;
    issuer.send(&json!({"jsonrpc":"2.0","id":cancelled,"method":"tools/call","params":{"name":"claim.submit","arguments":claim_document(&alice_id, "Cancelled by the agent.")}}));
    issuer.send(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":cancelled,"reason":"agent moved on"}}));
    // The adapter stays responsive; whatever reply the cancelled call got is
    // drained, and the journal decides what happened.
    let probe = issuer.next;
    issuer.next += 1;
    issuer.send(&json!({"jsonrpc":"2.0","id":probe,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}));
    loop {
        let reply = issuer.output.recv_timeout(Duration::from_secs(30)).unwrap();
        if reply["id"] == probe {
            assert!(reply["result"]["tools"].is_array(), "{reply}");
            break;
        }
        assert_eq!(reply["id"], cancelled, "{reply}");
    }
    let listed = pending_ids(&issuer.call("request.pending", json!({})));
    let outcome = match listed.as_slice() {
        [id] => {
            // Reserved and possibly committed: the exact retry settles it.
            let retried = issuer.call("request.retry", json!({"operation_id": id}));
            assert_eq!(retried["condition"], "Committed", "{retried}");
            let consumed = issuer.call("request.acknowledge", json!({"operation_id": id}));
            assert_eq!(consumed["condition"], "Consumed");
            1
        }
        [] => 0,
        other => panic!("{other:?}"),
    };
    assert_eq!(sequence(root), before + outcome);
    assert!(pending_ids(&issuer.call("request.pending", json!({}))).is_empty());

    // ---- Reply lost before commitment: the node aborts after receiving the
    // frame and before proposing it ----
    let before = sequence(root);
    drop(server);
    let server = start_with(root, &advertise, &[("FOCAL_FAULT", "before-propose:1")]);
    let (condition, unknown) = issuer.failed(
        "claim.submit",
        claim_document(&alice_id, "Cut before the proposal."),
    );
    assert_eq!(condition, "OutcomeUnknown", "{unknown}");
    let lost_before = unknown["operation_id"].as_str().unwrap().to_string();
    drop(server);
    let server = start(root, &advertise);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(sequence(root), before, "the cut node committed nothing");
    assert_eq!(
        pending_ids(&issuer.call("request.pending", json!({}))),
        vec![lost_before.clone()]
    );
    let inspected = issuer.call("request.inspect", json!({"operation_id": lost_before}));
    assert_eq!(inspected["condition"], "Pending", "{inspected}");
    let replayed = issuer.call("request.retry", json!({"operation_id": lost_before}));
    assert_eq!(replayed["condition"], "Committed", "{replayed}");
    assert_eq!(sequence(root), before + 1);
    let again = issuer.call("request.retry", json!({"operation_id": lost_before}));
    assert_eq!(again["result"], replayed["result"]);
    assert_eq!(sequence(root), before + 1);
    let consumed = issuer.call("request.acknowledge", json!({"operation_id": lost_before}));
    assert_eq!(consumed["condition"], "Consumed");

    // ---- Reply lost after commitment: the node aborts after the owner
    // committed the frame and before any reply was written ----
    let before = sequence(root);
    drop(server);
    let server = start_with(
        root,
        &advertise,
        &[("FOCAL_FAULT", "after-commit-before-reply:1")],
    );
    let (condition, unknown) = alice.failed(
        "claim.submit",
        claim_document(&issuer_id, "Cut after the commit."),
    );
    assert_eq!(condition, "OutcomeUnknown", "{unknown}");
    let lost_after = unknown["operation_id"].as_str().unwrap().to_string();
    drop(server);
    let server = start(root, &advertise);
    // The restarted node re-commits its durable tail in its new term; the
    // acknowledged commit is there, never lost.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while sequence(root) < before + 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the commit did not survive the cut"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(sequence(root), before + 1, "the commit survived the cut");
    assert_eq!(
        pending_ids(&alice.call("request.pending", json!({}))),
        vec![lost_after.clone()]
    );
    let reconciled = alice.call("request.retry", json!({"operation_id": lost_after}));
    assert_eq!(reconciled["condition"], "Committed", "{reconciled}");
    assert_eq!(sequence(root), before + 1);
    let remote = alice.call(
        "request.inspect",
        json!({"operation_id": lost_after, "remote": true}),
    );
    assert_eq!(remote["condition"], "Observed", "{remote}");
    assert_eq!(
        objects(&remote)[0]["Outcome"]["intent"],
        reconciled["result"]["receipt"]["intent"]
    );
    let consumed = alice.call("request.acknowledge", json!({"operation_id": lost_after}));
    assert_eq!(consumed["condition"], "Consumed");
    assert_eq!(claims_of(root, &alice_id), 3);

    // ---- Exhausted admission capacity: fresh work is refused while the
    // exact retry of committed work is answered unchanged; the refused
    // reference stays journaled and commits once the pressure lifts ----
    let (post_id, post_result) = {
        let (_, result) = issuer.committed(
            "claim.submit",
            claim_document(&alice_id, "Posted before the pressure."),
            false,
        );
        let id = created(&result, "Claim").remove(0);
        issuer.committed("claim.post", json!({"claim": id}), true)
    };
    let before = sequence(root);
    drop(server);
    let server = start_with(
        root,
        &advertise,
        &[("FOCAL_DISK_HEADROOM_BYTES", "18446744073709551615")],
    );
    let (condition, refused) = issuer.failed(
        "claim.submit",
        claim_document(&alice_id, "Refused under pressure."),
    );
    assert_eq!(condition, "Error", "{refused}");
    assert_eq!(refused["result"]["code"], "capacity", "{refused}");
    let refused_id = refused["operation_id"].as_str().unwrap().to_string();
    assert_eq!(sequence(root), before);
    let retried = issuer.call("request.retry", json!({"operation_id": post_id}));
    assert_eq!(retried["condition"], "Committed", "{retried}");
    assert_eq!(retried["result"], post_result);
    let observed = issuer.call(
        "request.inspect",
        json!({"operation_id": post_id, "remote": true}),
    );
    assert_eq!(observed["condition"], "Observed", "{observed}");
    assert_eq!(
        objects(&observed)[0]["Outcome"]["intent"],
        post_result["receipt"]["intent"]
    );
    let inspected = issuer.call("request.inspect", json!({"operation_id": refused_id}));
    assert_eq!(inspected["condition"], "Pending", "{inspected}");
    assert_eq!(sequence(root), before);
    drop(server);
    let server = start(root, &advertise);
    let admitted = issuer.call("request.retry", json!({"operation_id": refused_id}));
    assert_eq!(admitted["condition"], "Committed", "{admitted}");
    assert_eq!(sequence(root), before + 1);
    for id in [&post_id, &refused_id] {
        let consumed = issuer.call("request.acknowledge", json!({"operation_id": id}));
        assert_eq!(consumed["condition"], "Consumed");
    }
    assert!(pending_ids(&issuer.call("request.pending", json!({}))).is_empty());
    drop(alice);
    drop(issuer);
    drop(server);
}
