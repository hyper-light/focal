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
//! Bounded native lists through the real binary and the MCP adapter (doc 22
//! §9): every family and filter, the residual-filtered empty page that still
//! continues, cursor tamper, a cursor reused under another filter, and the
//! node restart that retires every cursor it issued.
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
fn admin(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let output = run(root, context, args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn committed(value: &Value) -> Value {
    assert_eq!(value["condition"], "Committed", "{value}");
    assert_eq!(value["result"]["kind"], "native");
    value["result"].clone()
}
fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| hex(&entry["id"]))
        .collect()
}
fn hex(value: &Value) -> String {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
fn read_objects(page: &Value) -> &Vec<Value> {
    assert_eq!(page["result"]["kind"], "native_read", "{page}");
    page["result"]["page"]["objects"].as_array().unwrap()
}
/// One list page: its objects and its continuation in hexadecimal.
fn list(root: &Path, context: Option<&str>, args: &[&str]) -> (Vec<Value>, Option<String>, u64) {
    let mut full = vec!["list"];
    full.extend_from_slice(args);
    let page = cli(root, context, &full);
    assert_eq!(page["condition"], "Listed", "{page}");
    assert_eq!(page["schema_version"], 2, "{page}");
    assert_eq!(page["result"]["kind"], "native_list", "{page}");
    let body = &page["result"]["page"];
    let next = body["next"].as_array().map(|_| hex(&body["next"]));
    (
        body["objects"].as_array().unwrap().clone(),
        next,
        body["visited"].as_u64().unwrap(),
    )
}
fn ids(objects: &[Value], kind: &str) -> Vec<String> {
    objects
        .iter()
        .map(|object| hex(&object[kind]["binding"]["object"]))
        .collect()
}
/// A refused list: the structured error the CLI prints, on stdout for a
/// service refusal or on stderr for an input refusal, always with
/// `condition = "Error"` and a nonzero exit.
fn refused(root: &Path, args: &[&str]) -> Value {
    let mut full = vec!["list"];
    full.extend_from_slice(args);
    full.extend(["--format", "json"]);
    let output = run(root, None, &full);
    assert!(!output.status.success(), "{args:?} succeeded");
    let text = if output.stdout.is_empty() {
        output.stderr.clone()
    } else {
        output.stdout.clone()
    };
    let value: Value = serde_json::from_slice(&text).unwrap_or_else(|_| {
        panic!(
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(value["condition"], "Error", "{args:?}: {value}");
    value
}

struct Mcp {
    _process: Server,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    next: u64,
}
impl Mcp {
    fn open(root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
            .args(["--data-dir", root.to_str().unwrap(), "mcp", "serve"])
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
    fn call(&mut self, name: &str, args: Value) -> Value {
        let result = self.rpc("tools/call", json!({"name":name,"arguments":args}));
        assert_eq!(result["isError"], false, "tool {name}: {result}");
        result["structuredContent"].clone()
    }
}
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

#[test]
fn native_lists_select_every_family_continue_through_empty_pages_and_refuse_foreign_cursors() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-lists-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-lists-client-")
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
    let issuer = hex(&read_objects(&status)[0]["Standing"]["principal"]);
    assert_ne!(issuer.len(), 0);
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
    let alice_standing = admin(client.path(), Some("alice"), &["status"]);
    let alice = hex(&read_objects(&alice_standing)[0]["Standing"]["principal"]);

    // Two authored claims: the first runs the complete cycle to Satisfied,
    // the second reviews it and stays Posted.
    let first_document = json!({
        "description": "Run the suite and deliver the report.",
        "target": alice,
        "scopes": [{"kind": "file", "key": "src/lib.rs"}],
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": 4_102_444_800_000u64}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    let result = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &first_document.to_string()],
    ));
    let first = created(&result, "Claim").remove(0);
    let validation = created(&result, "Validation").remove(1);
    committed(&cli(root, None, &["claim", "post", &first]));
    let second_document = json!({
        "description": "Review the delivered report.",
        "target": alice,
        "action": "consultation",
        "scopes": [{"kind": "symbol", "key": "lib::run"}],
        "relations": [{"kind": "reviews", "target": format!("claim:{first}")}],
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}
        ]
    });
    let result = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &second_document.to_string()],
    ));
    let second = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &second]));
    committed(&cli(
        client.path(),
        Some("alice"),
        &["receipt", "acquire", &first],
    ));
    let result = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "artifact", "submit", "--claim", &first, "--slot", "0", "--text", PROOF,
        ],
    ));
    let artifact = created(&result, "Artifact").remove(0);
    let page = cli(
        client.path(),
        Some("alice"),
        &["get", "artifact", &artifact],
    );
    let hash = hex(&read_objects(&page)[0]["Artifact"]["content_hash"]);
    let schema = hex(&read_objects(&page)[0]["Artifact"]["schema_hash"]);
    let result = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "testament",
            "submit",
            "--claim",
            &first,
            "--summary",
            "Suite passed.",
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
        client.path(),
        Some("alice"),
        &["testament", "post", &testament, "--claim", &first],
    ));
    committed(&cli(
        root,
        None,
        &["testament", "receive", &testament, "--claim", &first],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &first,
            "--validation",
            &validation,
        ],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &first,
            "--validation",
            &validation,
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    let first_page = cli(root, None, &["get", "claim", &first]);
    assert_eq!(read_objects(&first_page)[0]["Claim"]["status"], 8);
    let first_created = read_objects(&first_page)[0]["Claim"]["created"]
        .as_u64()
        .unwrap();

    // Claims: unfiltered, every indexed predicate and residual combinations.
    let sorted = |mut values: Vec<String>| {
        values.sort();
        values
    };
    let both = sorted(vec![first.clone(), second.clone()]);
    let (objects, next, visited) = list(root, None, &["claims"]);
    assert_eq!(sorted(ids(&objects, "Claim")), both);
    assert!(next.is_none());
    assert_eq!(visited, 2);
    let (objects, ..) = list(root, None, &["claims", "--source", "self"]);
    assert_eq!(sorted(ids(&objects, "Claim")), both);
    let (objects, ..) = list(root, None, &["claims", "--source", &alice]);
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["claims", "--target", &alice]);
    assert_eq!(sorted(ids(&objects, "Claim")), both);
    let (objects, ..) = list(root, None, &["claims", "--status", "posted"]);
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&second));
    let (objects, ..) = list(root, None, &["claims", "--status", "satisfied"]);
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&first));
    let (objects, ..) = list(root, None, &["claims", "--action", "consultation"]);
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&second));
    let (objects, ..) = list(root, None, &["claims", "--scope", "file:src/lib.rs"]);
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&first));
    let (objects, ..) = list(root, None, &["claims", "--scope", "symbol:lib::run"]);
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&second));
    let (objects, ..) = list(
        root,
        None,
        &["claims", "--relation", &format!("reviews=claim:{first}")],
    );
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&second));
    let (objects, ..) = list(
        root,
        None,
        &["claims", "--relation", &format!("depends_on=claim:{first}")],
    );
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["claims", "--created-after", "0"]);
    assert_eq!(sorted(ids(&objects, "Claim")), both);
    let (objects, ..) = list(
        root,
        None,
        &["claims", "--created-after", &first_created.to_string()],
    );
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&second));
    // Residual predicates narrow the indexed scan.
    let (objects, ..) = list(
        root,
        None,
        &["claims", "--target", &alice, "--status", "satisfied"],
    );
    assert_eq!(ids(&objects, "Claim"), std::slice::from_ref(&first));
    let (objects, ..) = list(
        root,
        None,
        &["claims", "--scope", "file:src/lib.rs", "--status", "posted"],
    );
    assert!(objects.is_empty());

    // Paging: one item per page, then an exact continuation.
    let (objects, next, visited) = list(root, None, &["claims", "--limit", "1"]);
    assert_eq!(objects.len(), 1);
    assert_eq!(visited, 1);
    let cursor = next.expect("a continuation after the first of two");
    let (rest, next, _) = list(root, None, &["claims", "--limit", "1", "--cursor", &cursor]);
    assert_eq!(rest.len(), 1);
    assert_ne!(ids(&rest, "Claim"), ids(&objects, "Claim"));
    // The last row was the last visited: the continuation ends only on the
    // page that finds nothing more.
    let (tail, next, visited) = match next {
        Some(cursor) => list(root, None, &["claims", "--limit", "1", "--cursor", &cursor]),
        None => (Vec::new(), None, 0),
    };
    assert!(tail.is_empty() && next.is_none(), "{visited}");
    // Residual filtering can exhaust the visit allowance before any match:
    // the page is empty and still continues.
    let (objects, next, visited) = list(
        root,
        None,
        &[
            "claims",
            "--target",
            &alice,
            "--status",
            "generated",
            "--limit",
            "1",
            "--max-visits",
            "1",
        ],
    );
    assert!(objects.is_empty());
    assert_eq!(visited, 1);
    let cursor = next.expect("an empty page still continues");
    let (objects, next, visited) = list(
        root,
        None,
        &[
            "claims",
            "--target",
            &alice,
            "--status",
            "generated",
            "--limit",
            "1",
            "--max-visits",
            "1",
            "--cursor",
            &cursor,
        ],
    );
    assert!(objects.is_empty());
    assert_eq!(visited, 1);
    assert!(next.is_none());
    // --all streams every page at one prefix.
    let streamed = run(
        root,
        None,
        &[
            "list", "claims", "--all", "--limit", "1", "--format", "table",
        ],
    );
    assert!(streamed.status.success());
    let text = String::from_utf8(streamed.stdout).unwrap();
    assert_eq!(text.matches("OBJECT\tClaim").count(), 2, "{text}");
    assert!(text.matches("PREFIX\t").count() >= 2, "{text}");

    // A tampered cursor, and a cursor under another filter, are refused.
    let (_, next, _) = list(root, None, &["claims", "--limit", "1"]);
    let cursor = next.unwrap();
    let mut tampered = cursor.clone();
    let last = tampered.pop().unwrap();
    tampered.push(if last == '0' { '1' } else { '0' });
    refused(root, &["claims", "--limit", "1", "--cursor", &tampered]);
    refused(root, &["claims", "--status", "posted", "--cursor", &cursor]);
    refused(root, &["artifacts", "--cursor", &cursor]);
    // V1-only predicates are refused, never ignored.
    refused(root, &["claims", "--caused-by", &format!("claim:{first}")]);
    refused(root, &["claims", "--scope", "file:a", "--scope", "file:b"]);

    // Artifacts: the respondent's work artifact and the issuer's result.
    let (objects, ..) = list(root, None, &["artifacts"]);
    assert_eq!(objects.len(), 2);
    let (objects, ..) = list(root, None, &["artifacts", "--producer", &alice]);
    assert_eq!(ids(&objects, "Artifact"), std::slice::from_ref(&artifact));
    let (objects, ..) = list(root, None, &["artifacts", "--producer", "self"]);
    assert_eq!(objects.len(), 1);
    assert_ne!(ids(&objects, "Artifact"), std::slice::from_ref(&artifact));
    let (objects, ..) = list(root, None, &["artifacts", "--schema-hash", &schema]);
    assert!(ids(&objects, "Artifact").contains(&artifact));
    let kind = read_objects(&page)[0]["Artifact"]["kind"]
        .as_str()
        .unwrap()
        .to_string();
    let (objects, ..) = list(root, None, &["artifacts", "--kind", &kind]);
    assert!(ids(&objects, "Artifact").contains(&artifact));
    let (objects, ..) = list(root, None, &["artifacts", "--kind", "no-such-kind"]);
    assert!(objects.is_empty());
    let (objects, ..) = list(
        root,
        None,
        &["artifacts", "--input", &format!("claim:{first}")],
    );
    assert!(objects.is_empty());

    // Definitions, evaluations, testaments, receipts, monitors and events.
    let (objects, ..) = list(root, None, &["validations", "--claim", &first]);
    assert_eq!(objects.len(), 2);
    assert!(ids(&objects, "Definition").contains(&validation));
    let (objects, ..) = list(root, None, &["validations", "--claim", &second]);
    assert_eq!(objects.len(), 1);
    let (objects, ..) = list(root, None, &["validations", "--evaluator", "self"]);
    assert_eq!(objects.len(), 3);
    let (objects, ..) = list(root, None, &["validations", "--evaluator", &alice]);
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["validations"]);
    assert_eq!(objects.len(), 3);
    let (objects, ..) = list(root, None, &["evaluations", "--claim", &first]);
    assert_eq!(objects.len(), 2);
    let (objects, ..) = list(root, None, &["evaluations", "--validation", &validation]);
    assert_eq!(objects.len(), 1);
    assert_eq!(
        objects[0]["Evaluation"]["state"], "Validated",
        "{objects:?}"
    );
    let (objects, ..) = list(root, None, &["evaluations", "--verdict", "pass"]);
    assert_eq!(objects.len(), 1);
    let (objects, ..) = list(root, None, &["evaluations", "--verdict", "fail"]);
    assert!(objects.is_empty());
    // Only the first claim's evaluations exist: the second claim's delivery
    // evaluation is registered when its receipt is acquired.
    let (objects, ..) = list(root, None, &["evaluations", "--evaluator", "self"]);
    assert_eq!(objects.len(), 2);
    let (objects, ..) = list(root, None, &["evaluations"]);
    assert_eq!(objects.len(), 2);
    let (objects, ..) = list(root, None, &["evaluations", "--evaluator", &alice]);
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["testaments", "--claim", &first]);
    assert_eq!(objects.len(), 1);
    assert_eq!(hex(&objects[0]["Response"]["binding"]["object"]), testament);
    let (objects, ..) = list(root, None, &["testaments", "--claim", &second]);
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["receipts", "--holder", &alice]);
    assert_eq!(objects.len(), 1);
    assert_eq!(hex(&objects[0]["Receipt"]["claim"]), first);
    let (objects, ..) = list(root, None, &["receipts", "--claim", &second]);
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["receipts", "--holder", "self"]);
    assert!(objects.is_empty());
    let (objects, next, _) = list(root, None, &["monitors", "--claim", &first]);
    assert!(objects.is_empty() && next.is_none());
    let mut total = 0usize;
    let mut cursor: Option<String> = None;
    let mut last_position = None;
    loop {
        let mut args = vec!["events", "--limit", "5"];
        if let Some(cursor) = &cursor {
            args.extend(["--cursor", cursor]);
        }
        let (objects, next, _) = list(root, None, &args);
        assert!(objects.len() <= 5);
        for object in &objects {
            let position = (
                object["Event"]["sequence"].as_u64().unwrap(),
                object["Event"]["ordinal"].as_u64().unwrap(),
            );
            assert!(last_position < Some(position), "{object}");
            last_position = Some(position);
        }
        total += objects.len();
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert!(total >= 10, "{total}");
    let (sequence, ordinal) = last_position.unwrap();
    let (objects, ..) = list(
        root,
        None,
        &["events", "--after", &format!("{sequence}:{ordinal}")],
    );
    assert!(objects.is_empty());
    let (objects, ..) = list(root, None, &["events", "--after", "1:0"]);
    assert_eq!(objects.len(), total - 1);

    // The MCP adapter serves the same lists as tools.
    {
        let mut mcp = Mcp::open(root);
        mcp.rpc(
            "initialize",
            json!({"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"lists","version":"1"}}),
        );
        let names = mcp.names();
        for tool in [
            "claim.list",
            "artifact.list",
            "validation.list",
            "evaluation.list",
            "testament.list",
            "receipt.list",
            "monitor.list",
            "event.list",
        ] {
            assert!(names.iter().any(|name| name == tool), "{tool}: {names:?}");
        }
        let page = mcp.call("claim.list", json!({"status": "posted"}));
        assert_eq!(page["condition"], "Listed", "{page}");
        assert_eq!(page["result"]["kind"], "native_list", "{page}");
        let objects = page["result"]["page"]["objects"].as_array().unwrap();
        assert_eq!(ids(objects, "Claim"), std::slice::from_ref(&second));
        let page = mcp.call("claim.list", json!({"limit": 1}));
        let body = &page["result"]["page"];
        assert_eq!(body["objects"].as_array().unwrap().len(), 1);
        let cursor = hex(&body["next"]);
        let page = mcp.call("claim.list", json!({"limit": 1, "cursor": cursor}));
        assert_eq!(
            page["result"]["page"]["objects"].as_array().unwrap().len(),
            1
        );
        let page = mcp.call("evaluation.list", json!({"verdict": "pass"}));
        assert_eq!(
            page["result"]["page"]["objects"].as_array().unwrap().len(),
            1
        );
        let page = mcp.call("receipt.list", json!({"holder": alice}));
        assert_eq!(
            page["result"]["page"]["objects"].as_array().unwrap().len(),
            1
        );
    }

    // A restart retires every cursor the previous incarnation issued.
    let (_, next, _) = list(root, None, &["claims", "--limit", "1"]);
    let stale = next.unwrap();
    drop(server);
    let server = start(root, &advertise);
    refused(root, &["claims", "--limit", "1", "--cursor", &stale]);
    let (objects, next, _) = list(root, None, &["claims"]);
    assert_eq!(sorted(ids(&objects, "Claim")), both);
    assert!(next.is_none());
    drop(server);
}
