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
//! Backups through the real binary (26 §6): a hosted session with a sealed
//! work artifact is backed up at its committed prefix, the backup verifies
//! file by file and against its own envelope, a tampered chunk is named, a
//! second write into the same directory is refused, verification runs with
//! the node killed, and after the claim retires a fresh backup carries the
//! bundle and the proof its header names.
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
        .env("FOCAL_GC_INTERVAL_MS", "300")
        .env("FOCAL_GC_GRACE_MS", "1500")
        .env("FOCAL_GC_QUARANTINE_MS", "600000")
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

const FAR: u64 = 4_102_444_800_000;
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

fn backup(root: &Path, output: &Path) -> Value {
    let created = admin(
        root,
        &[
            "cluster",
            "backup",
            "create",
            "--output",
            output.to_str().unwrap(),
        ],
    );
    assert_eq!(created["result"]["kind"], "backup_created", "{created}");
    created["result"]["backup"].clone()
}
fn verify(root: &Path, input: &Path) -> Value {
    let verified = admin(
        root,
        &[
            "cluster",
            "backup",
            "verify",
            "--input",
            input.to_str().unwrap(),
        ],
    );
    assert_eq!(verified["result"]["kind"], "backup_verified", "{verified}");
    verified["result"]["verification"].clone()
}

#[test]
fn a_backup_holds_the_prefix_and_its_proof_and_verifies_without_the_node() {
    let founder = tempfile::Builder::new()
        .prefix("focal-backup-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-backup-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let identity = admin(root, &["cluster", "node", "identity"])["result"]["identity"].clone();
    let session = identity["session"].as_str().unwrap().to_owned();
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
    let document = json!({
        "description": "Run the suite and deliver the report.",
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let claim = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &claim]));
    committed(&cli(
        client.path(),
        Some("alice"),
        &["receipt", "acquire", &claim],
    ));
    committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "artifact", "submit", "--claim", &claim, "--slot", "0", "--text", PROOF,
        ],
    ));
    // The export waits for the session's registration with the directory:
    // a backup is taken under a committed placement.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let placement = admin(root, &["cluster", "placement"]);
        let registered = placement["result"]["placement"]["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|partition| partition["sessions"].as_array().unwrap().iter())
            .any(|entry| entry["session"] == session);
        if registered {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session never registered: {placement}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    // The first backup: the envelope and the sealed payload.
    let first = founder.path().join("backups").join("first");
    let backup_one = backup(root, &first);
    assert_eq!(backup_one["prefix"]["session"], session, "{backup_one}");
    assert_eq!(backup_one["prefix"]["node"], identity["node"]);
    assert_eq!(backup_one["objects"], 1, "{backup_one}");
    assert_eq!(backup_one["bundles"], 0);
    assert!(backup_one["prefix"]["native_sequence"].as_u64().unwrap() >= 4);
    assert!(backup_one["prefix"]["index"].as_u64().unwrap() > 0);
    assert!(backup_one["checkpoint_bytes"].as_u64().unwrap() > 0);
    assert!(first.join("MANIFEST").is_file());
    assert!(first.join("checkpoint").is_file());
    let verified = verify(root, &first);
    assert_eq!(verified["complete"], true, "{verified}");
    assert_eq!(verified["inventory_matches"], true);
    // The storage view names the volume's pressure, the agents and the
    // session's floor.
    let storage = admin(root, &["cluster", "storage", "show"]);
    assert_eq!(storage["result"]["kind"], "storage", "{storage}");
    let storage = &storage["result"]["storage"];
    assert_eq!(storage["node"], identity["node"]);
    assert!(storage["disk"]["free"].as_u64().is_some(), "{storage}");
    assert!(storage["disk"]["headroom"].as_u64().is_some());
    assert_eq!(storage["retire"]["interval_ms"], 200);
    assert_eq!(storage["retire"]["grace_ms"], 0);
    assert_eq!(storage["gc"]["interval_ms"], 300);
    let listed = storage["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["session"] == session)
        .expect("the hosted session is listed");
    assert_eq!(listed["authoritative"], true, "{listed}");
    assert!(listed["retention"]["floor"].as_u64().is_some(), "{listed}");
    assert_eq!(storage["truncated"], false);
    assert_eq!(verified["decoder_supported"], true);
    assert_eq!(verified["objects_verified"], 1);
    assert_eq!(verified["problems"].as_array().unwrap().len(), 0);
    assert_eq!(verified["prefix"], backup_one["prefix"]);
    // A second write into the same directory is refused; the backup stands.
    let refused = run(
        root,
        None,
        &[
            "cluster",
            "backup",
            "create",
            "--output",
            first.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert!(!refused.status.success());
    assert_eq!(verify(root, &first)["complete"], true);
    // A tampered chunk is named; the envelope still verifies on its own.
    let chunk = std::fs::read_dir(first.join("content"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "chunk"))
        .unwrap();
    let mut bytes = std::fs::read(&chunk).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&chunk, &bytes).unwrap();
    let tampered = verify(root, &first);
    assert_eq!(tampered["complete"], false, "{tampered}");
    assert_eq!(tampered["checkpoint_verified"], true);
    assert!(
        tampered["problems"]
            .as_array()
            .unwrap()
            .iter()
            .any(|problem| problem.as_str().unwrap().contains("chunk")),
        "{tampered}"
    );
    // The claim retires: the next backup carries the bundle and the proof
    // its header names.
    committed(&cli(root, None, &["claim", "cancel", &claim]));
    committed(&cli(root, None, &["claim", "release-scope", &claim]));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let output = run(root, None, &["get", "claim", &claim, "--format", "json"]);
        if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
            && objects(&page)[0].get("Retired").is_some()
        {
            break;
        }
        assert!(Instant::now() < deadline, "the claim never retired");
        std::thread::sleep(Duration::from_millis(200));
    }
    let second = founder.path().join("backups").join("second");
    let backup_two = backup(root, &second);
    assert_eq!(backup_two["bundles"], 1, "{backup_two}");
    assert_eq!(backup_two["objects"], 2, "payload and bundle: {backup_two}");
    assert!(
        backup_two["prefix"]["native_sequence"].as_u64().unwrap()
            > backup_one["prefix"]["native_sequence"].as_u64().unwrap()
    );
    let verified = verify(root, &second);
    assert_eq!(verified["complete"], true, "{verified}");
    assert_eq!(verified["objects_verified"], 2);
    // Verification needs no node: the killed node's backups still verify,
    // and a directory without a manifest is not a backup.
    drop(server);
    let verified = verify(root, &second);
    assert_eq!(verified["complete"], true, "{verified}");
    let empty = founder.path().join("backups").join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let missing = run(
        root,
        None,
        &[
            "cluster",
            "backup",
            "verify",
            "--input",
            empty.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert!(!missing.status.success());
    let _ = start(root, &advertise);
}
