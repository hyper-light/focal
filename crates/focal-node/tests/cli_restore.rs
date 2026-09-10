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
//! Restore through the real binary (26 §6): a session backed up on one
//! cluster is restored onto the founder of a fresh cluster as a recovery
//! incarnation, registers there, answers reads of its history through an
//! enrolled client addressing the restored session, backs up again, refuses
//! a second restore and an unacknowledged recovery, and survives a restart.
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

fn temp(prefix: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in("/tmp")
        .unwrap();
    private(dir.path());
    dir
}
fn enroll(root: &Path, client: &Path, name: &str) -> String {
    let invitation = client.join(format!("{name}.invite"));
    admin(
        root,
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
    assert!(
        run(
            client,
            None,
            &[
                "context",
                "enroll",
                name,
                "--invite-file",
                invitation.to_str().unwrap()
            ]
        )
        .status
        .success()
    );
    let standing = admin(client, &["--client-context", name, "status"]);
    hex_hash(&objects(&standing)[0]["Standing"]["principal"])
}
fn wait_registered(root: &Path, session: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let placement = admin(root, &["cluster", "placement"]);
        let found = placement["result"]["placement"]["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|partition| partition["sessions"].as_array().unwrap().iter())
            .find(|entry| entry["session"] == session)
            .cloned();
        if let Some(entry) = found {
            return entry;
        }
        assert!(
            Instant::now() < deadline,
            "the session never registered: {placement}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn a_backup_restores_onto_a_fresh_cluster_as_a_recovery_incarnation() {
    let founder = temp("focal-restore-a-");
    let client = temp("focal-restore-client-a-");
    let root = founder.path();
    let activation = admin(root, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let identity = admin(root, &["cluster", "node", "identity"])["result"]["identity"].clone();
    let session = identity["session"].as_str().unwrap().to_owned();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    let alice = enroll(root, client.path(), "alice");
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
    let (_, result) = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "artifact", "submit", "--claim", &claim, "--slot", "0", "--text", PROOF,
        ],
    ));
    let artifact = created(&result, "Artifact").remove(0);
    let claim_before = objects(&cli(root, None, &["get", "claim", &claim]))[0].clone();
    let artifact_before = objects(&cli(root, None, &["get", "artifact", &artifact]))[0].clone();
    assert!(claim_before.get("Claim").is_some(), "{claim_before}");
    wait_registered(root, &session);
    let backup = founder.path().join("backup");
    let created_backup = admin(
        root,
        &[
            "cluster",
            "backup",
            "create",
            "--output",
            backup.to_str().unwrap(),
        ],
    );
    assert_eq!(created_backup["result"]["kind"], "backup_created");
    let backed = created_backup["result"]["backup"].clone();
    assert_eq!(backed["objects"], 1);
    // The old cluster is gone.
    drop(server);

    // A fresh cluster on a fresh node serves the old tenant and restores.
    let other = temp("focal-restore-b-");
    let client_b = temp("focal-restore-client-b-");
    let root_b = other.path();
    let activation = admin(root_b, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise_b = address();
    let server_b = start(root_b, &advertise_b);
    let identity_b = admin(root_b, &["cluster", "node", "identity"])["result"]["identity"].clone();
    assert_ne!(identity_b["cluster"], identity["cluster"]);
    let admitted = admin(
        root_b,
        &["cluster", "tenants", "admit", "--tenant", &tenant],
    );
    assert_eq!(admitted["result"]["kind"], "tenants", "{admitted}");
    assert!(
        admitted["result"]["admitted"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry == &Value::String(tenant.clone())),
        "{admitted}"
    );
    // A tampered backup restores nothing: verification comes first.
    let chunk = std::fs::read_dir(backup.join("content"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "chunk"))
        .unwrap();
    let original = std::fs::read(&chunk).unwrap();
    let mut tampered = original.clone();
    tampered[0] ^= 0xff;
    std::fs::write(&chunk, &tampered).unwrap();
    let corrupt = run(
        root_b,
        None,
        &[
            "cluster",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
            "--format",
            "json",
        ],
    );
    assert!(
        !corrupt.status.success(),
        "{}",
        String::from_utf8_lossy(&corrupt.stdout)
    );
    std::fs::write(&chunk, &original).unwrap();
    // The source is another cluster: its incarnation cannot continue here.
    let refused = run(
        root_b,
        None,
        &[
            "cluster",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert!(
        !refused.status.success(),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    let restored = admin(
        root_b,
        &[
            "cluster",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
        ],
    );
    assert_eq!(restored["result"]["kind"], "restored", "{restored}");
    let restored = restored["result"]["restored"].clone();
    assert_eq!(restored["decision"], "recovery_incarnation");
    assert_eq!(restored["session"], session);
    assert_eq!(restored["tenant"], tenant);
    assert_eq!(restored["node"], identity_b["node"]);
    assert_eq!(restored["objects_imported"], 1);
    assert_eq!(
        restored["prefix"]["native_sequence"],
        backed["prefix"]["native_sequence"]
    );
    assert_ne!(restored["group"], backed["prefix"]["group"]);
    // The restored session registers with the new cluster's directory as a
    // session founded here.
    let registered = wait_registered(root_b, &session);
    assert_eq!(registered["founder"], identity_b["node"], "{registered}");
    assert_eq!(registered["tenant"], tenant);
    // The operator's local connection addresses the restored session, a
    // session of a served tenant, and reads the history the old cluster's
    // participants wrote.
    assert!(
        run(
            client_b.path(),
            None,
            &[
                "context",
                "add",
                "restored",
                "--node-data-dir",
                root_b.to_str().unwrap(),
                "--tenant",
                &tenant,
                "--session",
                &session
            ]
        )
        .status
        .success()
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let claim_after = loop {
        let output = run(
            client_b.path(),
            Some("restored"),
            &["get", "claim", &claim, "--format", "json"],
        );
        if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
            && page["result"]["kind"] == "native_read"
        {
            break objects(&page)[0].clone();
        }
        assert!(
            Instant::now() < deadline,
            "the restored session never answered: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(
        claim_after["Claim"]["status"],
        claim_before["Claim"]["status"]
    );
    assert_eq!(claim_after["Claim"]["id"], claim_before["Claim"]["id"]);
    let artifact_after = objects(&cli(
        client_b.path(),
        Some("restored"),
        &["get", "artifact", &artifact],
    ))[0]
        .clone();
    assert_eq!(
        artifact_after["Artifact"]["payload"],
        artifact_before["Artifact"]["payload"]
    );
    // The restored session backs up again from the new incarnation.
    let again = founder.path().join("backup-again");
    let created_again = admin(
        root_b,
        &[
            "cluster",
            "backup",
            "create",
            "--tenant",
            &tenant,
            "--session",
            &session,
            "--output",
            again.to_str().unwrap(),
        ],
    );
    let again_backup = created_again["result"]["backup"].clone();
    assert_eq!(again_backup["objects"], 1, "{again_backup}");
    assert_eq!(again_backup["prefix"]["group"], restored["group"]);
    assert_eq!(again_backup["prefix"]["session"], session);
    let verified = admin(
        root_b,
        &[
            "cluster",
            "backup",
            "verify",
            "--input",
            again.to_str().unwrap(),
        ],
    );
    assert_eq!(
        verified["result"]["verification"]["complete"], true,
        "{verified}"
    );
    // A session this node already hosts is not restored again.
    let twice = run(
        root_b,
        None,
        &[
            "cluster",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
            "--format",
            "json",
        ],
    );
    assert!(!twice.status.success());
    // A restart reopens the restored session from the install record.
    drop(server_b);
    let _server_b = start(root_b, &advertise_b);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let output = run(
            client_b.path(),
            Some("restored"),
            &["get", "claim", &claim, "--format", "json"],
        );
        if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
            && page["result"]["kind"] == "native_read"
        {
            assert_eq!(
                objects(&page)[0]["Claim"]["status"],
                claim_before["Claim"]["status"]
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the restored session never reopened"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}
