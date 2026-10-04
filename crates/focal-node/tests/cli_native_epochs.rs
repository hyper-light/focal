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
//! The audit's F12 through the real binary: a principal's requests are
//! issued in generations; the node's resident outcome window is bounded, and
//! under pressure the archive agent closes the least recently used
//! generations and seals their outcomes into bundles under custody. A client
//! whose generation the owner closed learns it by name, resumes in the
//! generation the owner admits, and reads the outcome of a sealed operation
//! from its seal — through a restart of the node too.
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
#[path = "support/deadline.rs"]
mod deadline;
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
/// The resident outcome window of this node (F12): pressure on it forces
/// the generation floors once the resident outcomes and the candidates
/// that may still be admitted (the standard thirty-two) would pass it.
const OUTCOMES: &str = "48";
/// A node on the network when `advertise` names its endpoint, else the
/// embedded node of the data directory alone.
fn start(root: &Path, advertise: Option<&str>) -> Server {
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap(), "start"]);
    if let Some(advertise) = advertise {
        command.args(["--advertise", advertise]);
    }
    let mut child = command
        .env("FOCAL_NATIVE_OUTCOMES", OUTCOMES)
        // The archive agent looks every fifth of a second, and retires a
        // released family at once.
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
/// A command's structured output with its exit code, success or not.
fn attempt(root: &Path, context: Option<&str>, args: &[&str]) -> (i32, Value) {
    let mut full = args.to_vec();
    full.extend(["--format", "json"]);
    let output = run(root, context, &full);
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
    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["result"]["kind"], "native");
    let id = value["operation_id"].as_str().unwrap().to_string();
    assert!(id.starts_with("n1:"));
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
const FAR: u64 = 4_102_444_800_000;
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
fn seals_proposed(root: &Path) -> u64 {
    let shown = admin(root, None, &["diagnose", "node", "--storage"]);
    assert_eq!(shown["result"]["kind"], "storage", "{shown}");
    shown["result"]["storage"]["retire"]["seals_proposed"]
        .as_u64()
        .unwrap()
}
/// The founder's committed outcome for `operation`, read from the owner —
/// from the live window or from the seal that holds it.
fn observed(root: &Path, operation: &str) -> Value {
    let page = cli(
        root,
        None,
        &[
            "request",
            "inspect",
            "--operation-id",
            operation,
            "--remote",
        ],
    );
    assert_eq!(page["condition"], "Observed", "{page}");
    let outcome = objects(&page)[0]["Outcome"].clone();
    assert!(!outcome.is_null(), "{page}");
    outcome
}

#[test]
fn a_closed_generation_is_learned_by_name_and_its_sealed_outcomes_are_still_read() {
    let founder = tempfile::Builder::new()
        .prefix("focal-epochs-")
        .tempdir()
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-epochs-client-")
        .tempdir()
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, Some(&advertise));
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
    assert!(enrolled.status.success());
    let alice_standing = admin(client.path(), Some("alice"), &["status"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);

    // The founder issues claims until the owner closes its generation: the
    // resident window fills, the archive agent forces the founder's floor
    // (the only open generation is the least recent one) and seals the
    // closed generation's outcomes. The refused command is refused by name;
    // no other refusal, and no unknown outcome, is acceptable.
    let mut committed_ops: Vec<(String, Value)> = Vec::new();
    let mut expired = None;
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    for round in 0..96u32 {
        assert!(
            deadline.open(),
            "the generation never closed: {committed_ops:?}"
        );
        let document = claim_document(&alice, &format!("Claim {round} of the window."));
        let (code, value) = attempt(
            root,
            None,
            &["submit", "claim", "--json", &document.to_string()],
        );
        if code == 0 {
            committed_ops.push(committed(&value));
            continue;
        }
        assert_eq!(code, 5, "{value}");
        assert_eq!(value["condition"], "Error", "{value}");
        assert_eq!(
            value["result"]["code"], "request_history_expired",
            "{value}"
        );
        expired = value["operation_id"].as_str().map(str::to_string);
        break;
    }
    let expired = expired.expect("the founder's generation was closed under pressure");
    assert!(
        committed_ops.len() >= 8,
        "closed before any pressure: {}",
        committed_ops.len()
    );
    assert!(seals_proposed(root) >= 1);
    // The refused operation never executed and never will: its journal
    // holds the refusal, and the owner has no outcome for it.
    let inspected = cli(
        root,
        None,
        &["request", "inspect", "--operation-id", &expired],
    );
    assert_eq!(inspected["condition"], "Error", "{inspected}");
    assert_eq!(inspected["result"]["code"], "request_history_expired");
    let missing = cli(
        root,
        None,
        &["request", "inspect", "--operation-id", &expired, "--remote"],
    );
    assert!(objects(&missing)[0].get("Missing").is_some(), "{missing}");
    // The journal learned the owner's window: the next command is issued in
    // the generation the owner admits and commits.
    let before = sequence(root);
    let (_, resumed) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "The first claim after the floor.").to_string(),
        ],
    ));
    assert_eq!(sequence(root), before + 1);
    // Every committed operation of the closed generation is still answered:
    // by its journal without a send, and by the owner from the seal that
    // holds its outcome, the receipt unchanged.
    let (first_id, first_result) = &committed_ops[0];
    let retried = cli(
        root,
        None,
        &["request", "retry", "--operation-id", first_id],
    );
    assert_eq!(retried["condition"], "Committed", "{retried}");
    assert_eq!(retried["result"]["receipt"], first_result["receipt"]);
    let sealed = observed(root, first_id);
    assert_eq!(sealed["intent"], first_result["receipt"]["intent"]);
    assert_eq!(sealed["sequence"], first_result["receipt"]["sequence"]);
    let (last_id, last_result) = committed_ops.last().unwrap();
    assert_eq!(
        observed(root, last_id)["intent"],
        last_result["receipt"]["intent"]
    );
    // The claims themselves are live: the seal took outcomes, not work.
    let claim = &resumed["created"][0]["id"];
    let page = cli(root, None, &["get", "claim", &hex_hash(claim)]);
    assert!(objects(&page)[0].get("Claim").is_some(), "{page}");

    // A restart replays the seals and the windows from disk: the sealed
    // outcomes are read as before, the closed generation stays closed, and
    // work goes on in the open one.
    drop(server);
    let server = start(root, Some(&advertise));
    let after_restart = observed(root, first_id);
    assert_eq!(after_restart, sealed);
    let (code, again) = attempt(
        root,
        None,
        &["request", "retry", "--operation-id", &expired],
    );
    assert_eq!(code, 5, "{again}");
    assert_eq!(again["condition"], "Error", "{again}");
    assert_eq!(again["result"]["code"], "request_history_expired");
    committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "The claim after the restart.").to_string(),
        ],
    ));
    drop(server);
}

/// Poll `get claim` until the continuation replaces the claim (26 §4).
fn retired(root: &Path, claim: &str) -> Value {
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    loop {
        let page = cli(root, None, &["get", "claim", claim]);
        let object = objects(&page)[0].clone();
        if object.get("Retired").is_some() {
            return object["Retired"].clone();
        }
        assert!(deadline.open(), "claim {claim} never retired: {object}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The embedded node — `focal start` without a network — runs the archive
/// agent's walk on its owner thread against its own store (26 §4a): under
/// the same pressure its founder's generation closes and is learned by
/// name, its sealed outcomes are read from the seal, a released family
/// retires behind its continuation, and a restart keeps it all.
#[test]
fn an_embedded_node_seals_its_closed_generations_and_retires_released_families() {
    let founder = tempfile::Builder::new()
        .prefix("focal-epochs-embedded-")
        .tempdir()
        .unwrap();
    private(founder.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let server = start(root, None);
    // The founder issues to the worker identity its data directory names.
    let identity = focal_node::embedded::decode_identity(&root.join("IDENTITY")).unwrap();
    let worker: String = identity
        .worker
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    // A family released before the pressure: it retires by itself.
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&worker, "A claim to release.").to_string(),
        ],
    ));
    let released = hex_hash(&result["created"][0]["id"]);
    committed(&cli(root, None, &["claim", "cancel", &released]));
    committed(&cli(root, None, &["claim", "release-scope", &released]));
    let continuation = retired(root, &released);
    assert!(continuation["bundle"].is_array(), "{continuation}");

    // The founder issues claims until the owner closes its generation.
    let mut committed_ops: Vec<(String, Value)> = Vec::new();
    let mut expired = None;
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    for round in 0..96u32 {
        assert!(
            deadline.open(),
            "the generation never closed: {committed_ops:?}"
        );
        let document = claim_document(&worker, &format!("Claim {round} of the window."));
        let (code, value) = attempt(
            root,
            None,
            &["submit", "claim", "--json", &document.to_string()],
        );
        if code == 0 {
            committed_ops.push(committed(&value));
            continue;
        }
        assert_eq!(code, 5, "{value}");
        assert_eq!(
            value["result"]["code"], "request_history_expired",
            "{value}"
        );
        expired = value["operation_id"].as_str().map(str::to_string);
        break;
    }
    let expired = expired.expect("the founder's generation was closed under pressure");
    assert!(committed_ops.len() >= 8, "{}", committed_ops.len());
    // The next command commits in the admitted generation; the sealed
    // operations are still answered from the seal, the claims are live.
    let (_, resumed) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&worker, "The first claim after the floor.").to_string(),
        ],
    ));
    let (first_id, first_result) = &committed_ops[0];
    let sealed = observed(root, first_id);
    assert_eq!(sealed["intent"], first_result["receipt"]["intent"]);
    let missing = cli(
        root,
        None,
        &["request", "inspect", "--operation-id", &expired, "--remote"],
    );
    assert!(objects(&missing)[0].get("Missing").is_some(), "{missing}");
    let claim = hex_hash(&resumed["created"][0]["id"]);
    assert!(
        objects(&cli(root, None, &["get", "claim", &claim]))[0]
            .get("Claim")
            .is_some()
    );
    // The retired family stays retired; a restart keeps the seals and the
    // continuation and admits work in the open generation.
    assert!(
        objects(&cli(root, None, &["get", "claim", &released]))[0]
            .get("Retired")
            .is_some()
    );
    drop(server);
    let server = start(root, None);
    assert_eq!(observed(root, first_id), sealed);
    assert!(
        objects(&cli(root, None, &["get", "claim", &released]))[0]
            .get("Retired")
            .is_some()
    );
    committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&worker, "The claim after the restart.").to_string(),
        ],
    ));
    drop(server);
}
