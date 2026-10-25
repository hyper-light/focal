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
//! From the at-rest level the backup is sealed under an operator's key file
//! (29 §12.8): nothing of the session lies in it in the clear, and it is
//! verified and restored with that key file alone; a backup from below the
//! level, in the clear, restores as it always did.
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
use deadline::progress;
fn address() -> String {
    ports::address()
}
fn start(root: &Path, advertise: &str) -> Server {
    start_announcing(root, advertise, None)
}
/// The node on `root` started announcing the capability level `announced` (a staged rollout, 24
/// §21), or the binary's own.
fn start_announcing(root: &Path, advertise: &str, announced: Option<&str>) -> Server {
    progress::admit();
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    if let Some(level) = announced {
        command.env("FOCAL_CAPABILITY_LEVEL", level);
    }
    let mut child = command
        .args([
            "--data-dir",
            root.to_str().unwrap(),
            "start",
            "node",
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
    let mut server = Server(child);
    let status = progress::reported(&receive, &mut server.0, root, Duration::from_secs(25))
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
const DESCRIPTION: &str = "Run the suite and deliver the report.";

/// A key file the real binary makes (`focal create root-key`), as an operator makes one.
fn key_file(dir: &Path, name: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let made = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["create", "root-key", "--output", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(made.status.success(), "{made:?}");
    path
}
/// The code of the error a refused admin command printed (`focal: [code] message`): a refusal
/// the command made, not one its arguments' parsing made.
fn refusal(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "{}\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    stderr
        .split_once("focal: [")
        .and_then(|(_, rest)| rest.split_once(']'))
        .map(|(code, _)| code.to_owned())
        .unwrap_or_else(|| panic!("no refusal code: {stderr}"))
}
/// Every file under `dir`, with its bytes.
fn files_under(dir: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let Ok(listing) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in listing {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                found.push((path, bytes));
            }
        }
    }
    found
}
fn holds(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
        .collect()
}
/// Nothing of the restored session's on the node at `root`: no file in its tenant's content
/// domain, no seed or cold directory of its session.
fn untouched(root: &Path, tenant: &str, session: &str) -> bool {
    files_under(&root.join("content").join("objects").join(tenant)).is_empty()
        && !root.join("seeds").join(session).exists()
        && !root.join("cold").join(session).exists()
}

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
            "invite",
            "client",
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
                "enroll",
                "context",
                name,
                "--invite-file",
                invitation.to_str().unwrap()
            ]
        )
        .status
        .success()
    );
    let standing = admin(client, &["--client-context", name, "inspect", "prefix"]);
    hex_hash(&objects(&standing)[0]["Standing"]["principal"])
}
fn wait_registered(root: &Path, session: &str) -> Value {
    let mut deadline = deadline::Deadline::after(Duration::from_secs(90));
    loop {
        let placement = admin(root, &["inspect", "placement"]);
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
        assert!(deadline.open(), "the session never registered: {placement}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn a_backup_restores_onto_a_fresh_cluster_as_a_recovery_incarnation() {
    let founder = temp("focal-restore-a-");
    let client = temp("focal-restore-client-a-");
    let root = founder.path();
    let activation = admin(root, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let identity = admin(root, &["inspect", "node", "--identity"])["result"]["identity"].clone();
    let session = identity["session"].as_str().unwrap().to_owned();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    let alice = enroll(root, client.path(), "alice");
    let document = json!({
        "description": DESCRIPTION,
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
    committed(&cli(root, None, &["post", "claim", &claim]));
    committed(&cli(
        client.path(),
        Some("alice"),
        &["acquire", "receipt", &claim],
    ));
    let (_, result) = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "submit", "artifact", "--claim", &claim, "--slot", "0", "--text", PROOF,
        ],
    ));
    let artifact = created(&result, "Artifact").remove(0);
    let claim_before = objects(&cli(root, None, &["get", "claim", &claim]))[0].clone();
    let artifact_before = objects(&cli(root, None, &["get", "artifact", &artifact]))[0].clone();
    assert!(claim_before.get("Claim").is_some(), "{claim_before}");
    wait_registered(root, &session);
    let backup = founder.path().join("backup");
    // At the binary's level a backup is sealed under an operator's key file (29 §12.8): one asked
    // for without a key file is refused by name before the node is asked.
    let keys = temp("focal-restore-keys-");
    let ops = key_file(keys.path(), "ops.key");
    let stranger = key_file(keys.path(), "other.key");
    let unkeyed = run(
        root,
        None,
        &["create", "backup", "--output", backup.to_str().unwrap()],
    );
    assert_eq!(refusal(&unkeyed), "backup_key_required");
    assert!(!backup.exists(), "a refused backup writes nothing");
    let created_backup = admin(
        root,
        &[
            "create",
            "backup",
            "--output",
            backup.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    assert_eq!(created_backup["result"]["kind"], "backup_created");
    let backed = created_backup["result"]["backup"].clone();
    assert_eq!(backed["objects"], 1);
    assert_eq!(backed["sealed"], true, "{backed}");
    let key = backed["key"].as_str().unwrap().to_owned();
    assert_eq!(key.len(), 32, "the operator key's ID: {backed}");
    // Nothing of the session lies in its backup in the clear: no row's text, no payload, no
    // identity, no hash, no manifest's marker, in any file's bytes or name.
    let checkpoint_hash = backed["checkpoint_hash"].as_str().unwrap().to_owned();
    let mut markers: Vec<Vec<u8>> = vec![
        PROOF.as_bytes().to_vec(),
        DESCRIPTION.as_bytes().to_vec(),
        b"FCLBKUP1".to_vec(),
    ];
    for id in [&claim, &artifact, &session, &tenant, &checkpoint_hash] {
        markers.push(id.as_bytes().to_vec());
        markers.push(unhex(id));
    }
    let sealed_files = files_under(&backup);
    assert!(sealed_files.len() > 3, "{sealed_files:?}");
    for (path, bytes) in &sealed_files {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        for id in [&claim, &artifact, &session, &tenant, &checkpoint_hash] {
            assert!(!name.contains(id.as_str()), "{name} is named by {id}");
        }
        for marker in &markers {
            assert!(
                !holds(bytes, marker),
                "{path:?} holds {marker:?} in the clear"
            );
        }
    }
    // The old cluster is gone; the backup verifies without it, with its key file alone.
    drop(server);
    let verified = admin(
        root,
        &[
            "verify",
            "backup",
            "--input",
            backup.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    let verification = &verified["result"]["verification"];
    assert_eq!(verification["complete"], true, "{verified}");
    assert_eq!(verification["sealed"], true);
    assert_eq!(verification["key"], backed["key"]);
    assert_eq!(verification["objects_verified"], 1);
    let verify = |key: Option<&Path>| {
        let mut args = vec!["verify", "backup", "--input", backup.to_str().unwrap()];
        if let Some(key) = key {
            args.extend(["--key-file", key.to_str().unwrap()]);
        }
        run(root, None, &args)
    };
    // Refused without its key file, or with another, the backup naming the key it needs.
    let unkeyed = verify(None);
    assert_eq!(refusal(&unkeyed), "backup_key_required");
    assert!(String::from_utf8_lossy(&unkeyed.stderr).contains(&key));
    let mismatched = verify(Some(&stranger));
    assert_eq!(refusal(&mismatched), "backup_key_mismatch");
    assert!(String::from_utf8_lossy(&mismatched.stderr).contains(&key));

    // A fresh cluster on a fresh node serves the old tenant and restores.
    let other = temp("focal-restore-b-");
    let client_b = temp("focal-restore-client-b-");
    let root_b = other.path();
    let activation = admin(root_b, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise_b = address();
    let server_b = start(root_b, &advertise_b);
    let identity_b =
        admin(root_b, &["inspect", "node", "--identity"])["result"]["identity"].clone();
    assert_ne!(identity_b["cluster"], identity["cluster"]);
    let admitted = admin(root_b, &["admit", "tenant", "--tenant", &tenant]);
    assert_eq!(admitted["result"]["kind"], "tenants", "{admitted}");
    assert!(
        admitted["result"]["admitted"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry == &Value::String(tenant.clone())),
        "{admitted}"
    );
    // A restore that is refused writes nothing here: without its key file, with another, with a
    // file of it flipped or two of its files swapped, it is refused before anything of the session
    // reaches this node.
    assert!(untouched(root_b, &tenant, &session));
    let restore = |key: Option<&Path>| {
        let mut args = vec![
            "restore",
            "session",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
        ];
        if let Some(key) = key {
            args.extend(["--key-file", key.to_str().unwrap()]);
        }
        run(root_b, None, &args)
    };
    assert_eq!(refusal(&restore(None)), "backup_key_required");
    assert!(untouched(root_b, &tenant, &session));
    assert_eq!(refusal(&restore(Some(&stranger))), "backup_key_mismatch");
    assert!(untouched(root_b, &tenant, &session));
    let chunk = std::fs::read_dir(backup.join("content"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "chunk"))
        .unwrap();
    let original = std::fs::read(&chunk).unwrap();
    let mut tampered = original.clone();
    tampered[original.len() / 2] ^= 0xff;
    std::fs::write(&chunk, &tampered).unwrap();
    refusal(&restore(Some(&ops)));
    assert!(untouched(root_b, &tenant, &session));
    std::fs::write(&chunk, &original).unwrap();
    let checkpoint_file = backup.join("checkpoint");
    let checkpoint = std::fs::read(&checkpoint_file).unwrap();
    std::fs::write(&checkpoint_file, &original).unwrap();
    std::fs::write(&chunk, &checkpoint).unwrap();
    refusal(&restore(Some(&ops)));
    assert!(untouched(root_b, &tenant, &session));
    std::fs::write(&checkpoint_file, &checkpoint).unwrap();
    std::fs::write(&chunk, &original).unwrap();
    // The source is another cluster: its incarnation cannot continue here.
    let refused = run(
        root_b,
        None,
        &[
            "restore",
            "session",
            "--input",
            backup.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    refusal(&refused);
    let restored = admin(
        root_b,
        &[
            "restore",
            "session",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
            "--key-file",
            ops.to_str().unwrap(),
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
                "add",
                "context",
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
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
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
            deadline.open(),
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
            "create",
            "backup",
            "--tenant",
            &tenant,
            "--session",
            &session,
            "--output",
            again.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    let again_backup = created_again["result"]["backup"].clone();
    assert_eq!(again_backup["objects"], 1, "{again_backup}");
    assert_eq!(again_backup["sealed"], true);
    assert_eq!(again_backup["prefix"]["group"], restored["group"]);
    assert_eq!(again_backup["prefix"]["session"], session);
    let verified = admin(
        root_b,
        &[
            "verify",
            "backup",
            "--input",
            again.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    assert_eq!(
        verified["result"]["verification"]["complete"], true,
        "{verified}"
    );
    // The same restore asked again answers as the restore it repeats (an operator reissuing a
    // command whose reply was lost): the copy already installed under its recovery group, nothing
    // imported again.
    let twice = admin(
        root_b,
        &[
            "restore",
            "session",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    let twice = &twice["result"]["restored"];
    assert_eq!(twice["group"], restored["group"], "{twice}");
    assert_eq!(twice["objects_imported"], 0, "{twice}");
    // A restart reopens the restored session from the install record.
    drop(server_b);
    let _server_b = start(root_b, &advertise_b);
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
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
        if !deadline.open() {
            // What the client was told last, and what the node says of its replicas: which
            // says whether the session is unopened, reopening, or open and not answering.
            let replicas = run(root_b, None, &["inspect", "replicas", "--replicas"]);
            panic!(
                "the restored session never reopened: {}\n{}\nthe node's replicas: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
                String::from_utf8_lossy(&replicas.stdout),
                String::from_utf8_lossy(&replicas.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Do: found a node announcing the level below the at-rest level, write a claim, and back its
/// session up; ask for one under a key file; verify the backup with and without a key file; restore
/// it on a fresh node at the binary's level and read the claim back (29 §12.8). Expect: below the
/// level the backup is written in the clear as every earlier binary writes it (its manifest's
/// marker in its first bytes, no `ENCRYPTION-INFO`) and a key file is refused by name; it verifies
/// whole whatever key file is named; it restores onto the fresh node with no key file, its claim as
/// it was.
#[test]
fn a_clear_backup_from_below_the_at_rest_level_still_verifies_and_restores() {
    let founder = temp("focal-restore-clear-a-");
    let root = founder.path();
    let below = (focal_node::upgrade::AT_REST_LEVEL - 1).to_string();
    let activation = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "activate", "native"])
        .env("FOCAL_CAPABILITY_LEVEL", &below)
        .output()
        .unwrap();
    assert!(activation.status.success(), "{activation:?}");
    let advertise = address();
    let server = start_announcing(root, &advertise, Some(&below));
    let identity = admin(root, &["inspect", "node", "--identity"])["result"]["identity"].clone();
    let session = identity["session"].as_str().unwrap().to_owned();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    let document = json!({
        "description": DESCRIPTION,
        "target": format!("{:032x}", 5),
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}
        ]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let claim = created(&result, "Claim").remove(0);
    let claim_before = objects(&cli(root, None, &["get", "claim", &claim]))[0].clone();
    wait_registered(root, &session);
    let keys = temp("focal-restore-clear-keys-");
    let ops = key_file(keys.path(), "ops.key");
    let backup = founder.path().join("backup");
    // Below the level a backup is written in the clear: a key file asked for is refused by name.
    let keyed = run(
        root,
        None,
        &[
            "create",
            "backup",
            "--output",
            backup.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    assert_eq!(refusal(&keyed), "invalid_input");
    let created_backup = admin(
        root,
        &["create", "backup", "--output", backup.to_str().unwrap()],
    );
    let backed = created_backup["result"]["backup"].clone();
    assert_eq!(backed["sealed"], false, "{backed}");
    assert!(
        std::fs::read(backup.join("MANIFEST"))
            .unwrap()
            .starts_with(b"FCLBKUP1")
    );
    assert!(!backup.join("ENCRYPTION-INFO").exists());
    drop(server);
    // It verifies whole, with no key file and with one, which it does not need.
    for key in [None, Some(&ops)] {
        let mut args = vec!["verify", "backup", "--input", backup.to_str().unwrap()];
        if let Some(key) = key {
            args.extend(["--key-file", key.to_str().unwrap()]);
        }
        let verified = admin(root, &args);
        let verification = &verified["result"]["verification"];
        assert_eq!(verification["complete"], true, "{verified}");
        assert_eq!(verification["sealed"], false);
    }
    // A fresh node at the binary's level restores it as it always did.
    let other = temp("focal-restore-clear-b-");
    let client_b = temp("focal-restore-clear-client-b-");
    let root_b = other.path();
    let activation = admin(root_b, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise_b = address();
    let _server_b = start(root_b, &advertise_b);
    admin(root_b, &["admit", "tenant", "--tenant", &tenant]);
    let restored = admin(
        root_b,
        &[
            "restore",
            "session",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
        ],
    );
    assert_eq!(restored["result"]["kind"], "restored", "{restored}");
    assert_eq!(restored["result"]["restored"]["session"], session);
    wait_registered(root_b, &session);
    assert!(
        run(
            client_b.path(),
            None,
            &[
                "add",
                "context",
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
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
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
            deadline.open(),
            "the restored session never answered: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(claim_after["Claim"], claim_before["Claim"]);
}
