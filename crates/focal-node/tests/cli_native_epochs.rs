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
//! under pressure the owner closes the least recently used generations
//! itself (a closing seal) before a client meets the bound, and the archive
//! agent seals their outcomes into bundles under custody. A client whose
//! generation the owner closed learns it by name, resumes in the generation
//! the owner admits, and reads the outcome of a sealed operation from its
//! seal — through a restart of the node too.
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
/// The resident outcome window of this node (F12): pressure on it forces
/// the generation floors once the resident outcomes come within a quarter
/// of it (26 §4a).
const OUTCOMES: &str = "48";
/// A node on the network when `advertise` names its endpoint, else the
/// embedded node of the data directory alone.
fn start(root: &Path, advertise: Option<&str>) -> Server {
    start_with(root, advertise, "0")
}
/// [`start`] with a released family retiring once `grace_ms` of the node's logical time has
/// passed since its last event settled.
fn start_with(root: &Path, advertise: Option<&str>, grace_ms: &str) -> Server {
    progress::admit();
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap(), "start", "node"]);
    if let Some(advertise) = advertise {
        command.args(["--advertise", advertise]);
    }
    let mut child = command
        .env("FOCAL_NATIVE_OUTCOMES", OUTCOMES)
        // The archive agent looks every fifth of a second, and retires a
        // released family once its grace has passed.
        .env("FOCAL_RETIRE_INTERVAL_MS", "200")
        .env("FOCAL_RETIRE_AFTER_MS", grace_ms)
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
    admin(root, None, &["inspect", "prefix"])["result"]["page"]["native_sequence"]
        .as_u64()
        .unwrap()
}
fn seals_proposed(root: &Path) -> u64 {
    retire_status(root)["seals_proposed"].as_u64().unwrap()
}
/// The archive agent's account on this node (`inspect node --storage`).
fn retire_status(root: &Path) -> Value {
    let shown = admin(root, None, &["inspect", "node", "--storage"]);
    assert_eq!(shown["result"]["kind"], "storage", "{shown}");
    shown["result"]["storage"]["retire"].clone()
}
/// The founder's replica's live window and its seals (F12, 26 §4a).
fn sealing(root: &Path) -> Value {
    let shown = admin(root, None, &["inspect", "replicas", "--replicas"]);
    shown["result"]["diagnostics"]["sealing"].clone()
}
/// The founder's committed outcome for `operation`, read from the owner —
/// from the live window or from the seal that holds it.
fn observed(root: &Path, operation: &str) -> Value {
    let page = cli(
        root,
        None,
        &[
            "inspect",
            "request",
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
    let activation = admin(root, None, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, Some(&advertise));
    let invitation = client.path().join("alice.invite");
    admin(
        root,
        None,
        &[
            "invite",
            "client",
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
            "enroll",
            "context",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(enrolled.status.success());
    let alice_standing = admin(client.path(), Some("alice"), &["inspect", "prefix"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);

    // The founder issues claims until the owner closes its generation: the
    // resident window comes within a quarter of its bound, the owner forces
    // the founder's floor itself (the only open generation is the least
    // recent one) through a closing seal, and the archive agent seals the
    // closed generation's outcomes. The refused command is refused by name;
    // no other refusal — the bound's least of all — and no unknown outcome
    // is acceptable.
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
    // Closed in-band before the bound, by a closing seal; the archive agent
    // seals the closed generation after it.
    let closed = sealing(root);
    assert_eq!(closed["rule"], "anchored", "{closed}");
    assert_eq!(closed["closes_applied"], 1, "{closed}");
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    loop {
        let view = sealing(root);
        if view["seals_applied"].as_u64().unwrap() >= 1 && view["busy"] == false {
            assert!(
                view["resident_outcomes"].as_u64().unwrap()
                    < view["outcome_bound"].as_u64().unwrap() / 2,
                "{view}"
            );
            break;
        }
        assert!(
            deadline.open(),
            "the closed generation was never sealed: {view}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(seals_proposed(root) >= 1);
    // The refused operation never executed and never will: its journal
    // holds the refusal, and the owner has no outcome for it.
    let inspected = cli(
        root,
        None,
        &["inspect", "request", "--operation-id", &expired],
    );
    assert_eq!(inspected["condition"], "Error", "{inspected}");
    assert_eq!(inspected["result"]["code"], "request_history_expired");
    let missing = cli(
        root,
        None,
        &["inspect", "request", "--operation-id", &expired, "--remote"],
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
        &["retry", "request", "--operation-id", first_id],
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
        &["retry", "request", "--operation-id", &expired],
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

/// The embedded node — `focal start node` without a network — runs the archive
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
    let activation = admin(root, None, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let server = start(root, None);
    // The founder issues to the worker identity its data directory names.
    let identity = {
        // Its identity, one of the node's small files, sealed from the at-rest level (29 §12.10),
        // read as a command line beside the node reads it.
        let mut settings = focal_node::config::Settings::default();
        settings.node.data_dir = Some(root.to_path_buf());
        focal_node::node_files::read_identity(&settings, root).unwrap()
    };
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
    committed(&cli(root, None, &["cancel", "claim", &released]));
    committed(&cli(root, None, &["release", "scope", &released]));
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
        &["inspect", "request", "--operation-id", &expired, "--remote"],
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

/// A family released before its generation closed has its outcomes sealed while its grace runs,
/// and still retires once the grace ends (26 §4): a walk dates it by when the session's records
/// settled, never by its outcome, which the seal took out of the core. Dated by its outcome, a walk
/// that met it failed (`Corrupt`), went back to the first family and rested the interval, and no
/// family retired again: a deliver load filled its session's memory and stopped. The grace is a day
/// while the generation closes and is sealed; restarted with none, the node retires the family at
/// its first walks, and no walk fails.
#[test]
fn a_family_whose_outcomes_a_seal_took_retires_once_its_grace_ends() {
    let founder = tempfile::Builder::new()
        .prefix("focal-epochs-graced-")
        .tempdir()
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-epochs-graced-client-")
        .tempdir()
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start_with(root, Some(&advertise), "86400000");
    let invitation = client.path().join("alice.invite");
    admin(
        root,
        None,
        &[
            "invite",
            "client",
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
            "enroll",
            "context",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(enrolled.status.success());
    let alice_standing = admin(client.path(), Some("alice"), &["inspect", "prefix"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);

    // A family released at once, in the founder's first generation.
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "A claim to release.").to_string(),
        ],
    ));
    let released = hex_hash(&result["created"][0]["id"]);
    committed(&cli(root, None, &["cancel", "claim", &released]));
    let (release, _) = committed(&cli(root, None, &["release", "scope", &released]));

    // Claims until the owner closes that generation, and the archive agent seals its outcomes,
    // the release's among them.
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    let mut closed = false;
    for round in 0..96u32 {
        assert!(deadline.open(), "the generation never closed");
        let document = claim_document(&alice, &format!("Claim {round} of the window."));
        let (code, value) = attempt(
            root,
            None,
            &["submit", "claim", "--json", &document.to_string()],
        );
        if code == 0 {
            committed(&value);
            continue;
        }
        assert_eq!(
            value["result"]["code"], "request_history_expired",
            "{value}"
        );
        closed = true;
        break;
    }
    assert!(closed, "the founder's generation was closed under pressure");
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    loop {
        let view = sealing(root);
        if view["seals_applied"].as_u64().unwrap() >= 1 && view["busy"] == false {
            break;
        }
        assert!(
            deadline.open(),
            "the closed generation was never sealed: {view}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // The release's outcome is read from the seal; the family is live, within its grace, and
    // the walks that met it did not fail.
    assert!(!observed(root, &release).is_null());
    assert!(
        objects(&cli(root, None, &["get", "claim", &released]))[0]
            .get("Claim")
            .is_some()
    );
    assert_eq!(retire_status(root)["walks_failed"], 0);

    // Its grace ended: the family retires behind its continuation, and no walk failed.
    drop(server);
    let server = start_with(root, Some(&advertise), "0");
    let continuation = retired(root, &released);
    assert!(continuation["bundle"].is_array(), "{continuation}");
    assert_eq!(retire_status(root)["walks_failed"], 0);
    drop(server);
}
