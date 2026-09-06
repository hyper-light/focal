#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Real stdio client against the executable and authenticated ledger socket.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(root: &Path) -> Process {
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root)
            .arg("start")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let output = process.0.stdout.take().unwrap();
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
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap()["condition"],
        "Ready"
    );
    process
}
struct Mcp {
    process: Process,
    input: Option<ChildStdin>,
    output: Receiver<Value>,
    next: u64,
    modern: bool,
}
impl Mcp {
    fn start(root: &Path, modern: bool) -> Self {
        let mut process = Process(
            Command::new(env!("CARGO_BIN_EXE_focal"))
                .arg("--data-dir")
                .arg(root)
                .args(["mcp", "serve"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let input = process.0.stdin.take();
        let output = process.0.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let Ok(line) = line else { break };
                let value = serde_json::from_str(&line)
                    .expect("stdout contains one JSON-RPC message per line only");
                if send.send(value).is_err() {
                    break;
                }
            }
        });
        let mut client = Self {
            process,
            input,
            output: receive,
            next: 0,
            modern,
        };
        if !modern {
            let init=client.request("initialize",json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"focal-test","version":"1"}}));
            assert_eq!(init["result"]["protocolVersion"], "2025-11-25");
            client.send(json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}));
        }
        client
    }
    fn send(&mut self, value: Value) {
        let input = self.input.as_mut().unwrap();
        serde_json::to_writer(&mut *input, &value).unwrap();
        input.write_all(b"\n").unwrap();
        input.flush().unwrap();
    }
    fn request(&mut self, method: &str, mut params: Value) -> Value {
        self.next += 1;
        let id = self.next;
        if self.modern {
            params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}});
        }
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        let result = self
            .output
            .recv_timeout(Duration::from_secs(20))
            .expect("MCP response");
        assert_eq!(result["id"], id, "{result}");
        assert!(result.get("error").is_none(), "{result}");
        result
    }
    fn call(&mut self, name: &str, args: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":args}))
    }
    fn success(&mut self, name: &str, args: Value) -> Value {
        let result = self.call(name, args);
        assert_eq!(result["result"]["isError"], false, "{result}");
        let content = &result["result"]["structuredContent"];
        assert_eq!(
            serde_json::from_str::<Value>(result["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap(),
            *content
        );
        content.clone()
    }
    fn mutation(&mut self, name: &str, op: u128, mut args: Value) -> Value {
        args["operation_id"] = json!(id(op));
        let result = self.success(name, args);
        assert_eq!(result["condition"], "Committed", "{result}");
        result
    }
    fn finish(mut self) {
        self.input.take();
        let start = Instant::now();
        loop {
            if let Some(status) = self.process.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "bounded EOF shutdown"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
fn id(value: u128) -> String {
    format!("{value:032x}")
}
fn claim() -> Value {
    json!({"id":id(100),"description":"MCP proof workflow","target":"self","action":"handoff","scopes":[{"kind":"file","key":"report.json"}],"validations":[{"id":id(102),"kind":"receipt","phase":"whole_work","mode":"required","description":"Review report","evaluator":"self"}]})
}

#[test]
fn modern_and_legacy_stdio_share_durable_ids_builders_and_real_evidence_workflow() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    assert_eq!(
        mcp.request("server/discover", json!({}))["result"]["resultType"],
        "complete"
    );
    let first = mcp.request("tools/list", json!({}));
    let cursor = first["result"]["nextCursor"].as_str().unwrap();
    let second = mcp.request("tools/list", json!({"cursor":cursor}));
    let names: std::collections::BTreeSet<_> = first["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["result"]["tools"].as_array().unwrap())
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 20);
    assert!(names.contains("request.retry"));
    assert!(names.contains("validation.list"));
    let submitted = mcp.mutation("claim.submit", 1, claim());
    let reconciled = mcp.success(
        "request.inspect",
        json!({"operation_id":id(1),"remote":true}),
    );
    assert_eq!(reconciled["condition"], "Reconciled");
    assert_eq!(
        reconciled["result"]["reply"]["page"]["result"]["Receipt"]["resolution"]["Committed"],
        submitted["result"]["reply"]["Committed"]
    );
    let epoch = mcp.success("request.epoch", json!({"epoch":1}));
    assert_eq!(
        epoch["result"]["reply"]["page"]["result"]["Epoch"]["admitted"],
        true
    );
    let unknown = mcp.success("request.status", json!({"epoch":1,"request_id":id(999)}));
    assert_eq!(
        unknown["result"]["reply"]["page"]["result"]["Receipt"]["resolution"],
        "Unknown"
    );
    assert_eq!(mcp.mutation("claim.submit", 1, claim()), submitted);
    let store = focal_client::operation_store::OperationStore::open(
        root.path().join("MCP.operations"),
        Default::default(),
    )
    .unwrap();
    let journal = store.operation_path(&id(1)).unwrap();
    let recovered = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args(["request", "retry"])
        .arg(journal)
        .args(["--format", "json"])
        .output()
        .unwrap();
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    let recovered: Value = serde_json::from_slice(&recovered.stdout).unwrap();
    assert_eq!(
        recovered["receipt"],
        submitted["result"]["reply"]["Committed"]
    );
    let mut changed = claim();
    changed["description"] = json!("changed");
    changed["operation_id"] = json!(id(1));
    let conflict = mcp.call("claim.submit", changed);
    assert_eq!(conflict["result"]["isError"], true);
    assert_eq!(
        conflict["result"]["structuredContent"]["result"]["code"],
        "operation_conflict"
    );
    let mut revision = claim();
    revision["operation_id"] = json!(id(1));
    revision["expected_revision"] = json!(0);
    assert_eq!(
        mcp.call("claim.submit", revision)["result"]["isError"],
        true
    );
    mcp.finish();
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(
        mcp.success("request.inspect", json!({"operation_id":id(1)})),
        submitted
    );
    assert_eq!(
        mcp.success("request.retry", json!({"operation_id":id(1)})),
        submitted
    );
    assert_eq!(mcp.mutation("claim.submit", 1, claim()), submitted);
    mcp.mutation("claim.post", 2, json!({"claim":id(100)}));
    mcp.mutation(
        "receipt.acquire",
        3,
        json!({"claim":id(100),"id":id(104),"epoch":1}),
    );
    mcp.mutation(
        "evidence.begin",
        4,
        json!({"claim":id(100),"id":id(105),"receipt":{"id":id(104),"epoch":1}}),
    );
    mcp.mutation(
        "claim.progress",
        5,
        json!({"claim":id(100),"receipt":{"id":id(104),"epoch":1},"message":"Collected report"}),
    );
    mcp.mutation("artifact.submit",6,json!({"id":id(106),"claim":id(100),"receipt":{"id":id(104),"epoch":1},"evidence_set":id(105),"kind":"test_report","schema_hash":focal_evidence::test_report_schema().to_string(),"payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}));
    // Read through the independent manual adapter, then use the exact manifest
    // hash it exposes to bind the testament to the uploaded proof.
    let cli = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args(["get", "artifact", &id(106), "--format", "json"])
        .output()
        .unwrap();
    assert!(cli.status.success());
    let artifact: Value = serde_json::from_slice(&cli.stdout).unwrap();
    let focal_wire::ReadObject::Artifact { value, .. } =
        serde_json::from_value(artifact["result"]["object"].clone()).unwrap()
    else {
        panic!("artifact")
    };
    let artifact_hash = value.content_hash().to_string();
    mcp.mutation("testament.submit",7,json!({"id":id(107),"claim":id(100),"receipt":{"id":id(104),"epoch":1},"evidence_set":id(105),"manifest":[{"id":id(106),"hash":artifact_hash}],"summary":"One passing test","confidence":"committed","outcome":"complete"}));
    for (family, object) in [
        ("claim", 100),
        ("testament", 107),
        ("artifact", 106),
        ("validation", 102),
    ] {
        let read = mcp.success(&format!("{family}.get"), json!({"id":id(object)}));
        assert_eq!(read["result"]["kind"], "read");
        let listed = mcp.success(&format!("{family}.list"), json!({}));
        assert_eq!(listed["result"]["kind"], "list");
    }
    let missing = mcp.call("claim.get", json!({"id":id(999)}));
    assert_eq!(missing["result"]["isError"], true);
    assert_eq!(
        missing["result"]["structuredContent"]["result"]["code"],
        "not_found"
    );
    mcp.finish();
}

#[test]
fn lost_first_response_and_both_process_restarts_preserve_generated_ids() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    let mut authored = claim();
    authored["operation_id"] = json!(id(1));
    mcp.send(json!({"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"claim.submit","arguments":authored}}));
    // Deliberately never consume the mutation's MCP response. Observe its
    // committed object independently and then lose both processes.
    let deadline = Instant::now() + Duration::from_secs(5);
    let observed = loop {
        let output = Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root.path())
            .args(["get", "claim", &id(100), "--format", "json"])
            .output()
            .unwrap();
        if output.status.success() {
            break serde_json::from_slice::<Value>(&output.stdout).unwrap();
        }
        assert!(Instant::now() < deadline, "mutation never reached ledger");
        std::thread::sleep(Duration::from_millis(10));
    };
    drop(mcp);
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    let retried = mcp.success("request.retry", json!({"operation_id":id(1)}));
    assert_eq!(retried["condition"], "Committed");
    assert_eq!(mcp.mutation("claim.submit", 1, claim()), retried);
    let page = mcp.success("claim.get", json!({"id":id(100)}));
    assert_eq!(
        page["result"]["page"]["objects"][0],
        observed["result"]["object"]
    );
    let listed = mcp.success("claim.list", json!({}));
    assert_eq!(
        listed["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    mcp.finish();
}
