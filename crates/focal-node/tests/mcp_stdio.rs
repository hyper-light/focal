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
#[path = "support/mcp_claim_get.rs"]
mod claim_selection;
#[path = "support/mcp_claim_wait.rs"]
mod claim_wait;
#[path = "support/mcp_monitor.rs"]
mod monitor;
#[path = "support/mcp_peer.rs"]
mod peer;
#[path = "support/mcp_summary.rs"]
mod summary;
#[path = "support/mcp_watch.rs"]
mod watch;
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
        assert_eq!(result["result"]["isError"], false, "tool {name}: {result}");
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
    fn reserve(&mut self) -> String {
        let result = self.success("request.reserve", json!({}));
        assert_eq!(result["condition"], "Reserved");
        let id = result["operation_id"].as_str().unwrap().to_string();
        assert!(
            id.parse::<focal_client::managed_store::ManagedOperationId>()
                .is_ok()
        );
        id
    }
    fn managed_mutation(&mut self, name: &str, id: &str, mut args: Value) -> Value {
        args["operation_id"] = json!(id);
        let result = self.success(name, args);
        assert_eq!(result["condition"], "Committed", "{result}");
        assert_eq!(result["result"]["kind"], "managed");
        result
    }
    fn consumed_mutation(&mut self, name: &str, args: Value) -> Value {
        let id = self.reserve();
        let result = self.managed_mutation(name, &id, args);
        assert_eq!(
            self.success("request.acknowledge", json!({"operation_id":id}))["condition"],
            "Retired"
        );
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
    let mut names = std::collections::BTreeSet::new();
    let mut cursor = None;
    for _ in 0..16 {
        let params = cursor
            .take()
            .map(|cursor: String| json!({"cursor":cursor}))
            .unwrap_or_else(|| json!({}));
        let page = mcp.request("tools/list", params);
        for tool in page["result"]["tools"].as_array().unwrap() {
            assert!(
                names.insert(tool["name"].as_str().unwrap().to_owned()),
                "duplicate advertised tool"
            );
        }
        cursor = page["result"]["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        cursor.is_none(),
        "bounded catalogue pagination did not finish"
    );
    for descriptor in focal_client::operations::descriptors() {
        assert!(
            names.contains(descriptor.name),
            "missing shared tool {}",
            descriptor.name
        );
    }
    for transfer in [
        "upload.begin",
        "upload.append",
        "upload.seal",
        "upload.cancel",
        "artifact.download",
    ] {
        assert!(names.contains(transfer), "missing transfer tool {transfer}");
    }
    assert!(names.contains("validation.context"));
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
    let before_close = mcp.success("validation.context", json!({"id":id(102)}));
    assert!(before_close["operation_id"].is_null());
    assert!(before_close["result"]["context"]["testament"].is_null());
    mcp.mutation("testament.submit",7,json!({"id":id(107),"claim":id(100),"receipt":{"id":id(104),"epoch":1},"evidence_set":id(105),"manifest":[{"id":id(106),"hash":artifact_hash}],"summary":"One passing test","confidence":"committed","outcome":"complete"}));
    let after_close = mcp.success("validation.context", json!({"id":id(102)}));
    let context = &after_close["result"]["context"];
    let view: focal_client::validation_context::ValidationContext =
        serde_json::from_value(context.clone()).unwrap();
    assert_eq!(
        view.claim.lifecycle().status,
        focal_model::ClaimStatus::TestamentGenerated
    );
    assert!(view.records.is_empty());
    let testament = view.testament.unwrap();
    assert_eq!(testament.id, focal_model::TestamentId::from_u128(107));
    assert!(testament.value.lifecycle().acknowledged.is_none());
    assert_eq!(
        testament.value.content().artifacts[0].hash.to_string(),
        artifact_hash
    );
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
fn validation_context_pages_match_human_cli_without_reserving_mutations() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let demo = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .arg("demo")
        .output()
        .unwrap();
    assert!(
        demo.status.success(),
        "{}",
        String::from_utf8_lossy(&demo.stderr)
    );
    let _server = start(root.path());
    for modern in [true, false] {
        let mut mcp = Mcp::start(root.path(), modern);
        let definitions = mcp.success("validation.list", json!({"kind":"test"}));
        let focal_wire::ListPage { objects, .. } =
            serde_json::from_value(definitions["result"]["page"].clone()).unwrap();
        let focal_wire::ReadObject::Validation {
            id: validation_id, ..
        } = &objects[0]
        else {
            panic!("validation")
        };
        let first = mcp.success(
            "validation.context",
            json!({"id":validation_id.to_string(),"limit":1}),
        );
        assert!(first["operation_id"].is_null());
        let view: focal_client::validation_context::ValidationContext =
            serde_json::from_value(first["result"]["context"].clone()).unwrap();
        let position = view.next.unwrap();
        let phase = match position.run.phase {
            focal_model::ValidationPhase::Admission => "admission",
            focal_model::ValidationPhase::Increment => "increment",
            focal_model::ValidationPhase::WholeWork => "whole_work",
        };
        let second = mcp.success("validation.context", json!({"id":validation_id.to_string(),"limit":1,"prefix":{"sequence":view.token.sequence.0,"route_epoch":view.token.route_epoch.0},"after":{"target_hash":position.run.target_hash.to_string(),"phase":phase,"epoch":position.run.epoch,"attempt":position.attempt}}));
        assert_eq!(
            second["result"]["context"]["token"],
            first["result"]["context"]["token"]
        );
        assert!(second["result"]["context"]["next"].is_null());
        let cli = Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root.path())
            .args([
                "get",
                "validation",
                &validation_id.to_string(),
                "--context",
                "--limit",
                "1",
                "--format",
                "json",
            ])
            .output()
            .unwrap();
        assert!(
            cli.status.success(),
            "{}",
            String::from_utf8_lossy(&cli.stderr)
        );
        let human: Value = serde_json::from_slice(&cli.stdout).unwrap();
        assert_eq!(human["context"], first["result"]["context"]);
        assert!(
            mcp.success("request.pending", json!({}))["result"]["operation_ids"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let bad = mcp.call(
            "validation.context",
            json!({"id":validation_id.to_string(),"operation_id":id(500)}),
        );
        assert_eq!(bad["result"]["isError"], true);
        let missing = mcp.call("validation.context", json!({"id":id(999)}));
        assert_eq!(
            missing["result"]["structuredContent"]["result"]["code"],
            "not_found"
        );
        let observed = mcp.success("validation.get", json!({"id":validation_id.to_string()}));
        assert_eq!(
            observed["result"]["page"]["token"],
            first["result"]["context"]["token"]
        );
        mcp.finish();
    }
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

#[test]
fn managed_stdio_requires_consumption_and_preserves_unknown_gaps_across_restart() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    // Neither listing nor inspecting creates business work. Losing a reserve
    // response remains discoverable without reserving another ordinal.
    assert_eq!(
        mcp.success("request.pending", json!({}))["result"]["operation_ids"],
        json!([])
    );
    let gap = mcp.reserve();
    let operation = mcp.reserve();
    assert_eq!(
        mcp.success("request.pending", json!({}))["result"]["operation_ids"],
        json!([gap, operation])
    );
    assert_eq!(
        mcp.success("request.inspect", json!({"operation_id":gap}))["condition"],
        "Reserved"
    );
    assert_eq!(
        mcp.call("request.retry", json!({"operation_id":gap}))["result"]["isError"],
        true
    );
    let submitted = mcp.managed_mutation("claim.submit", &operation, claim());
    assert_eq!(
        mcp.managed_mutation("claim.submit", &operation, claim()),
        submitted
    );
    let mut changed = claim();
    changed["description"] = json!("different intent");
    changed["operation_id"] = json!(operation);
    assert_eq!(
        mcp.call("claim.submit", changed)["result"]["structuredContent"]["result"]["code"],
        "operation_conflict"
    );
    let mut changed_revision = claim();
    changed_revision["expected_revision"] = json!(0);
    changed_revision["operation_id"] = json!(operation);
    assert_eq!(
        mcp.call("claim.submit", changed_revision)["result"]["structuredContent"]["result"]["code"],
        "operation_conflict"
    );
    mcp.finish();
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(
        mcp.success("request.retry", json!({"operation_id":operation})),
        submitted
    );
    let remote = mcp.success(
        "request.inspect",
        json!({"operation_id":operation,"remote":true}),
    );
    assert_eq!(
        remote["result"]["reply"]["page"]["result"]["Receipt"]["resolution"]["Retained"],
        submitted["result"]["receipt"]
    );
    // Explicitly consuming a later result cannot retire the unknown earlier one.
    assert_eq!(
        mcp.success("request.acknowledge", json!({"operation_id":operation}))["condition"],
        "Consumed"
    );
    assert_eq!(
        mcp.success("request.inspect", json!({"operation_id":operation})),
        submitted
    );
    let sealed = mcp.success("request.seal", json!({"operation_id":gap}));
    assert_eq!(sealed["condition"], "Sealed");
    assert_eq!(
        mcp.success("request.seal", json!({"operation_id":gap})),
        sealed
    );
    assert_eq!(
        mcp.success("request.acknowledge", json!({"operation_id":gap}))["condition"],
        "Retired"
    );
    assert_eq!(
        mcp.success("request.acknowledge", json!({"operation_id":operation}))["condition"],
        "Retired"
    );
    assert_eq!(
        mcp.success("request.pending", json!({}))["result"]["operation_ids"],
        json!([])
    );
    assert_eq!(
        mcp.success("request.inspect", json!({"operation_id":operation}))["condition"],
        "Retired"
    );
    let retired = mcp.success(
        "request.inspect",
        json!({"operation_id":operation,"remote":true}),
    );
    assert!(
        retired["result"]["reply"]["page"]["result"]["Receipt"]["resolution"]
            .get("Retired")
            .is_some()
    );
    assert_eq!(
        mcp.call("request.retry", json!({"operation_id":operation}))["result"]["structuredContent"]
            ["result"]["code"],
        "managed_retired"
    );
    let mut replay = claim();
    replay["operation_id"] = json!(operation);
    assert_eq!(mcp.call("claim.submit", replay)["result"]["isError"], true);
    // Retirement is bounded history reuse; repeating it never creates a claim.
    assert_eq!(
        mcp.success("claim.list", json!({}))["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for _ in 0..40 {
        let operation = mcp.reserve();
        mcp.success("request.seal", json!({"operation_id":operation}));
        assert_eq!(
            mcp.success("request.acknowledge", json!({"operation_id":operation}))["condition"],
            "Retired"
        );
    }
    assert_eq!(
        mcp.success("request.pending", json!({}))["result"]["operation_ids"],
        json!([])
    );
    mcp.finish();
}

#[test]
fn lost_managed_mutation_response_recovers_through_human_cli_without_reexecution() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    let operation = mcp.reserve();
    let mut authored = claim();
    authored["operation_id"] = json!(operation);
    mcp.send(json!({"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"claim.submit","arguments":authored}}));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let output = Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root.path())
            .args(["get", "claim", &id(100), "--format", "json"])
            .output()
            .unwrap();
        if output.status.success() {
            break;
        }
        assert!(Instant::now() < deadline, "mutation never reached ledger");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Drop the unread response and both processes after independently observing
    // commit. MCP output alone never acknowledges receipt consumption.
    drop(mcp);
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    let retried = mcp.success("request.retry", json!({"operation_id":operation}));
    assert_eq!(retried["condition"], "Committed");
    let recovered = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args([
            "request",
            "retry",
            "--operation-id",
            &operation,
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    let recovered: Value = serde_json::from_slice(&recovered.stdout).unwrap();
    assert_eq!(recovered["receipt"], retried["result"]["receipt"]);
    assert_eq!(
        mcp.success("request.inspect", json!({"operation_id":operation}))["condition"],
        "Retired"
    );
    assert_eq!(
        mcp.success("claim.list", json!({}))["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    mcp.finish();
}

#[test]
fn managed_evidence_lifecycle_preserves_fences_and_testament_is_not_satisfaction() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    fn status(mcp: &mut Mcp) -> focal_model::ClaimStatus {
        let reply = mcp.success("claim.get", json!({"id":id(100)}));
        let object: focal_wire::ReadObject =
            serde_json::from_value(reply["result"]["page"]["objects"][0].clone()).unwrap();
        let focal_wire::ReadObject::Claim { value, .. } = object else {
            panic!("claim")
        };
        value.lifecycle().status
    }
    mcp.consumed_mutation("claim.submit", claim());
    assert_eq!(status(&mut mcp), focal_model::ClaimStatus::Generated);
    mcp.consumed_mutation("claim.post", json!({"claim":id(100)}));
    assert_eq!(status(&mut mcp), focal_model::ClaimStatus::Posted);
    mcp.consumed_mutation(
        "receipt.acquire",
        json!({"claim":id(100),"id":id(104),"epoch":1}),
    );
    assert_eq!(status(&mut mcp), focal_model::ClaimStatus::Received);
    mcp.consumed_mutation(
        "evidence.begin",
        json!({"claim":id(100),"id":id(105),"receipt":{"id":id(104),"epoch":1}}),
    );
    mcp.consumed_mutation(
        "claim.progress",
        json!({"claim":id(100),"receipt":{"id":id(104),"epoch":1},"message":"Report ready"}),
    );
    assert_eq!(status(&mut mcp), focal_model::ClaimStatus::Progressed);
    let mut artifact = json!({"id":id(106),"claim":id(100),"receipt":{"id":id(104),"epoch":1},"evidence_set":id(105),"kind":"test_report","schema_hash":focal_evidence::test_report_schema().to_string(),"payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}});
    let rejected = mcp.reserve();
    let mut stale = artifact.clone();
    stale["receipt"]["epoch"] = json!(2);
    stale["operation_id"] = json!(rejected);
    let refused = mcp.call("artifact.submit", stale);
    assert_eq!(refused["result"]["isError"], true);
    assert_eq!(
        refused["result"]["structuredContent"]["condition"],
        "DomainOutcome"
    );
    assert_eq!(
        mcp.success("request.inspect", json!({"operation_id":rejected}))["condition"],
        "Pending"
    );
    assert_eq!(
        mcp.success("artifact.list", json!({}))["result"]["page"]["objects"],
        json!([])
    );
    mcp.success("request.seal", json!({"operation_id":rejected}));
    mcp.success("request.acknowledge", json!({"operation_id":rejected}));
    mcp.consumed_mutation("artifact.submit", artifact.clone());
    let read = mcp.success("artifact.get", json!({"id":id(106)}));
    let object: focal_wire::ReadObject =
        serde_json::from_value(read["result"]["page"]["objects"][0].clone()).unwrap();
    let focal_wire::ReadObject::Artifact { value, .. } = object else {
        panic!("artifact")
    };
    mcp.consumed_mutation("testament.submit", json!({"id":id(107),"claim":id(100),"receipt":{"id":id(104),"epoch":1},"evidence_set":id(105),"manifest":[{"id":id(106),"hash":value.content_hash().to_string()}],"summary":"One passing test","confidence":"committed","outcome":"complete"}));
    // Transport result acknowledgment does not acknowledge the testament or
    // manufacture a validator verdict. The runtime owns those transitions.
    assert_eq!(
        status(&mut mcp),
        focal_model::ClaimStatus::TestamentGenerated
    );
    let late = mcp.reserve();
    artifact["id"] = json!(id(108));
    artifact["operation_id"] = json!(late);
    let after_close = mcp.call("artifact.submit", artifact);
    assert_eq!(
        after_close["result"]["structuredContent"]["condition"],
        "DomainOutcome"
    );
    assert_eq!(
        mcp.success("artifact.list", json!({"testament":id(107)}))["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        status(&mut mcp),
        focal_model::ClaimStatus::TestamentGenerated
    );
    mcp.success("request.seal", json!({"operation_id":late}));
    mcp.success("request.acknowledge", json!({"operation_id":late}));
    assert_eq!(
        mcp.success("request.pending", json!({}))["result"]["operation_ids"],
        json!([])
    );
    mcp.finish();
}

#[path = "support/mcp_selection.rs"]
mod selection;
