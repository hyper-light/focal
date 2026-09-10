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
//! Retirement to the archive through the real binary (26 §4): a terminal,
//! released claim leaves the core behind its continuation once the archive
//! agent has sealed its bundle under custody and the retirement record has
//! applied; `get claim` answers with the continuation, the operator sees
//! the retention floor's counts and the verified bundle, the exact retry
//! of the retired claim's creation still finds its outcome, and a killed
//! and restarted node holds all of it.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
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
        .env("FOCAL_RETIRE_INTERVAL_MS", "200")
        .env("FOCAL_RETIRE_AFTER_MS", "0")
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
fn admin(root: &Path, args: &[&str]) -> Value {
    let output = run(root, None, args);
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
fn refused(root: &Path, args: &[&str]) -> (i32, Value) {
    let mut full = args.to_vec();
    full.extend(["--format", "json"]);
    let output = run(root, None, &full);
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
fn committed(value: &Value) -> (String, Value) {
    assert_eq!(value["condition"], "Committed", "{value}");
    assert_eq!(value["result"]["kind"], "native");
    let id = value["operation_id"].as_str().unwrap().to_string();
    (id, value["result"].clone())
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
fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| hex_hash(&entry["id"]))
        .collect()
}
fn first_object(root: &Path, claim: &str) -> Value {
    let page = cli(root, None, &["get", "claim", claim]);
    objects(&page)[0].clone()
}
/// Poll `get claim` until the continuation replaces the claim. While the
/// record applies, the authority rebuilds its owner at a fresh readiness
/// barrier and a read may be refused for a tick; that is waited out too.
fn retired(root: &Path, claim: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let output = run(root, None, &["get", "claim", claim, "--format", "json"]);
        let last = if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
        {
            let object = objects(&page)[0].clone();
            if object.get("Retired").is_some() {
                return object["Retired"].clone();
            }
            object
        } else {
            json!({"stderr": String::from_utf8_lossy(&output.stderr), "stdout": String::from_utf8_lossy(&output.stdout)})
        };
        assert!(
            Instant::now() < deadline,
            "claim {claim} never retired: {last}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}
fn retention(root: &Path) -> Value {
    let shown = admin(root, &["cluster", "retention", "show"]);
    assert_eq!(shown["result"]["kind"], "retention", "{shown}");
    shown["result"]["retention"].clone()
}
const FAR: u64 = 4_102_444_800_000;

#[test]
fn a_terminal_released_claim_retires_to_the_archive_and_the_node_survives_a_kill() {
    let founder = tempfile::Builder::new()
        .prefix("focal-retention-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-retention-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let invitation = client.path().join("alice.invite");
    admin(
        root,
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
    let alice_standing = admin(client.path(), &["--client-context", "alice", "status"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);

    // Two claims: one runs its course and is released, one stays live.
    let document = json!({
        "description": "Deliver the report.",
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}
        ]
    });
    let (operation, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let a = created(&result, "Claim").remove(0);
    let created_outcome = result.clone();
    let live = json!({
        "description": "Keep this one.",
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}
        ]
    });
    let (_, live_result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &live.to_string()],
    ));
    let b = created(&live_result, "Claim").remove(0);
    // Nothing retires while the claim is live, cancelled but held, or
    // released only just now and still under the agent's cadence.
    let before = retention(root);
    assert_eq!(before["retired"], 0, "{before}");
    committed(&cli(root, None, &["claim", "cancel", &a]));
    // The final status is read before the release: once released, the
    // agent may retire the claim before another read lands.
    let status = first_object(root, &a)["Claim"]["status"].clone();
    assert!(!status.is_null());
    committed(&cli(root, None, &["claim", "release-scope", &a]));
    // The agent seals the bundle under custody and commits the record.
    let continuation = retired(root, &a);
    assert_eq!(hex_hash(&continuation["claim"]), a);
    assert_eq!(continuation["status"], status);
    assert!(continuation["bytes"].as_u64().unwrap() > 0);
    let through = continuation["through"].as_u64().unwrap();
    assert!(continuation["retired_at"].as_u64().unwrap() > through);
    assert!(continuation["events"].as_u64().unwrap() > 0);
    let bundle = hex_hash(&continuation["bundle"]);
    // The live claim is untouched, and the retired one refuses commands.
    assert!(first_object(root, &b).get("Claim").is_some());
    let (code, _) = refused(root, &["claim", "cancel", &a]);
    assert_ne!(code, 0);
    // The operator sees the count and the verified bundle with its receipt.
    let after = retention(root);
    assert_eq!(after["retired"], 1, "{after}");
    assert_eq!(after["retiring"], false, "{after}");
    let archive = admin(root, &["cluster", "archive", "show", "--claim", &a]);
    assert_eq!(archive["result"]["kind"], "archive", "{archive}");
    let shown = &archive["result"]["archive"];
    assert_eq!(shown["claim"], json!(a));
    assert_eq!(shown["bundle"], json!(bundle));
    assert_eq!(shown["verified"], true, "{archive}");
    assert_eq!(shown["root"], json!(a));
    assert_eq!(shown["members"], json!([a]));
    assert!(shown["rows"].as_u64().unwrap() >= 4, "{archive}");
    assert!(!shown["families"].as_array().unwrap().is_empty());
    let identity = admin(root, &["cluster", "node", "identity"])["result"]["identity"].clone();
    let node = identity["node"].clone();
    let tenant_hex = identity["tenant"].as_str().unwrap().to_owned();
    assert_eq!(shown["receipts"], json!([node]), "{archive}");
    // A live claim has no archive.
    let none = admin(root, &["cluster", "archive", "show", "--claim", &b]);
    assert_eq!(none["result"]["archive"], Value::Null, "{none}");
    // The outcome of the retired claim's creation stays: the exact retry
    // is answered from it.
    let retried = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &operation],
    );
    assert_eq!(retried["condition"], "Committed", "{retried}");
    assert_eq!(retried["result"]["created"], created_outcome["created"]);
    // A corrupted bundle is detected: its digest no longer verifies, the
    // continuation still names it, and the operator sees it unverified.
    let chunk = std::fs::read_dir(root.join("content").join("objects").join(&tenant_hex))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "chunk"))
        .unwrap();
    let original = std::fs::read(&chunk).unwrap();
    let mut tampered = original.clone();
    tampered[0] ^= 0xff;
    std::fs::write(&chunk, &tampered).unwrap();
    let archive = admin(root, &["cluster", "archive", "show", "--claim", &a]);
    assert_eq!(archive["result"]["archive"]["verified"], false, "{archive}");
    assert_eq!(archive["result"]["archive"]["bundle"], json!(bundle));
    std::fs::write(&chunk, &original).unwrap();
    let archive = admin(root, &["cluster", "archive", "show", "--claim", &a]);
    assert_eq!(archive["result"]["archive"]["verified"], true, "{archive}");
    // A kill and restart keep the continuation, the count and the bundle.
    drop(server);
    let _server = start(root, &advertise);
    let again = first_object(root, &a);
    assert_eq!(again["Retired"], continuation);
    assert!(first_object(root, &b).get("Claim").is_some());
    let restarted = retention(root);
    assert_eq!(restarted["retired"], 1, "{restarted}");
    let archive = admin(root, &["cluster", "archive", "show", "--claim", &a]);
    assert_eq!(archive["result"]["archive"]["verified"], true, "{archive}");
    assert_eq!(
        archive["result"]["archive"]["receipts"],
        json!([node]),
        "{archive}"
    );
}
