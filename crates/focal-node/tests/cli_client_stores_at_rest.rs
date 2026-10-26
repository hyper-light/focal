#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! A client's stores written in the clear and converted by the real binary once the fence opens the
//! at-rest level (29 §12.11). A node founded below the level; through its own directory, a client
//! with requests answered, one pending (its reply never came: the node was down) and an upload half
//! done (its bytes staged in the client's store, nothing sent); a client enrolled from a directory of
//! its own, which records the fence it learns from its cluster. Below the fence every store is
//! written as every earlier binary wrote it. The fence rises and the node records it; each client's
//! stores convert at their next open (the enrolled client's once it has learned the fence beside a
//! request), the pending request is retried under its identity and answered once, the upload
//! completes from its sealed part, and no client store holds a record in the clear. Unix-only, as
//! the other real-binary at-rest tests: the data directories are kept private by their mode bits.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::Duration,
};

#[path = "support/deadline.rs"]
mod deadline;
use deadline::Deadline;
use deadline::progress;
#[path = "support/ports.rs"]
mod ports;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// What a sealed client record begins with (`focal_client::at_rest::SEALED_MAGIC`), and a part.
const SEALED: &[u8] = b"FCLCSEAL";
const PART: &[u8] = b"FCLPART1";
/// What a pending journal's record begins with in the clear.
const JOURNAL: &[u8] = b"FCLOP001";
/// The report the upload carries: what no client store may hold in the clear once sealed.
const REPORT: &[u8] = br#"{"passed":7,"failed":0,"skipped":0}"#;
/// What the local client's claim says: no client store may hold it in the clear once sealed.
const DESCRIPTION: &str = "Durable evidence of the client at rest";
/// What the enrolled client's claim says.
const ENROLLED: &str = "A claim the enrolled client keeps at rest";

/// Start the node on `root` announcing `announced` (a staged rollout, 24 §21), or the binary's own
/// level, and wait for its readiness.
fn start(root: &Path, advertise: &str, announced: Option<&str>) -> Server {
    progress::admit();
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args([
        "--data-dir",
        root.to_str().unwrap(),
        "start",
        "node",
        "--advertise",
        advertise,
    ]);
    if let Some(level) = announced {
        command.env("FOCAL_CAPABILITY_LEVEL", level);
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        let mut reported = false;
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            if reported {
                continue;
            }
            text.push_str(&line);
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                reported = true;
            }
        }
    });
    let mut server = Server(child);
    let status = progress::reported(&receive, &mut server.0, root, Duration::from_secs(30))
        .expect("the node did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    server
}

/// The binary on the data directory `root`, through `context` where one is named.
fn run(root: &Path, context: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap()]);
    if let Some(context) = context {
        command.args(["--client-context", context]);
    }
    command.args(args).output().unwrap()
}

/// A command that succeeds, its JSON.
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
    serde_json::from_slice(&output.stdout).unwrap()
}

/// An administrative command, which prints its JSON as it is: its JSON.
fn admin(root: &Path, args: &[&str]) -> Value {
    let output = run(root, None, args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// A command whose outcome stays unknown (exit 7): its JSON.
fn pending(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let output = run(root, context, &args);
    assert_eq!(
        output.status.code(),
        Some(7),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    serde_json::from_slice(&output.stdout).unwrap()
}

fn id(value: u128) -> String {
    format!("{value:032x}")
}

fn claim(id: &str, description: &str) -> String {
    json!({"id":id,"target":"self","action":"handoff","description":description,"validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive result","evaluator":"self"}]}).to_string()
}

/// Whether a file of this name is one a client store keeps in the clear by design: a lock, a
/// marker of its making (a fixed sentence), a mark, or the fence a context recorded.
fn by_design(name: &str) -> bool {
    name == "LOCK"
        || name == "INITIALIZED"
        || name == "SEALED"
        || name == "STORAGE.level"
        || name.ends_with("-sealed")
        || name.ends_with("-lock")
        || name.ends_with(".lock")
}

/// The client stores' files at and under each of `paths` that exist.
fn files(paths: &[PathBuf]) -> Vec<(PathBuf, Vec<u8>)> {
    let mut found = Vec::new();
    let mut pending: Vec<PathBuf> = paths.iter().filter(|path| path.exists()).cloned().collect();
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                pending.push(entry.unwrap().path());
            }
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if by_design(&name) {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        found.push((path, bytes));
    }
    found
}

/// Every record of the client stores at `paths` sealed (whole, or a part), none holding any of
/// `needles`; and at least one record there.
fn assert_sealed(paths: &[PathBuf], needles: &[&[u8]]) {
    let found = files(paths);
    assert!(!found.is_empty(), "no client record under {paths:?}");
    for (path, bytes) in found {
        assert!(
            bytes.starts_with(SEALED) || bytes.starts_with(PART),
            "{} lies in the clear",
            path.display()
        );
        for needle in needles {
            assert!(
                !bytes.windows(needle.len()).any(|window| window == *needle),
                "{} holds {:?} in the clear",
                path.display(),
                String::from_utf8_lossy(needle)
            );
        }
    }
}

/// Do: found a node below the at-rest level; through its own directory submit and answer a claim,
/// submit a claim with the node down (its outcome unknown) and an artifact whose upload stays
/// pending (its bytes staged, nothing sent); enroll a client from a directory of its own and
/// submit a claim through it; raise the fence to the at-rest level and restart the node; then
/// retry the pending claim and the pending artifact twice each, and have the enrolled client send a
/// request and open its journal again. Expect: below the fence every client record in the clear, as
/// every earlier binary wrote it, and the enrolled client's history recording the cluster's fence
/// below the level from its enrollment; after it, the pending claim and the artifact committed once
/// each (a second retry answered the same), the artifact's bytes read back exactly, the enrolled
/// client's history recording the risen fence after its first request and its journal sealed at its
/// next open, and no client store of either holding a record, the report or a claim's words in the
/// clear.
#[test]
fn a_clients_stores_convert_at_their_next_open_and_every_outcome_is_known_once() {
    let directory = tempfile::Builder::new()
        .prefix("focal-client-rest-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = &directory.path().join("node");
    std::fs::create_dir(root).unwrap();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let client = &directory.path().join("client");
    std::fs::create_dir(client).unwrap();
    std::fs::set_permissions(client, std::fs::Permissions::from_mode(0o700)).unwrap();
    let advertise = ports::address();
    let at_rest = focal_node::upgrade::AT_REST_LEVEL;
    let below = (at_rest - 1).to_string();
    let level = at_rest.to_string();

    // Founded below the at-rest level: answered requests, through the node's own directory.
    let server = start(root, &advertise, Some(&below));
    let answered = id(100);
    cli(
        root,
        None,
        &["submit", "claim", "--json", &claim(&answered, DESCRIPTION)],
    );
    cli(root, None, &["post", "claim", &answered]);
    let receipt = cli(root, None, &["acquire", "receipt", &answered])["result"]["receipt"]
        .as_str()
        .unwrap()
        .to_owned();
    let set = cli(
        root,
        None,
        &[
            "begin",
            "evidence",
            "--claim",
            &answered,
            "--receipt",
            &receipt,
            "--receipt-epoch",
            "1",
        ],
    )["result"]["evidence_set"]
        .as_str()
        .unwrap()
        .to_owned();
    let operation = cli(root, None, &["reserve", "request"])["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // A client enrolled from a directory of its own records its cluster's fence as it enrolls.
    let invitation = client.join("alice.invite");
    admin(
        root,
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
        client,
        None,
        &[
            "enroll",
            "context",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(enrolled.status.success(), "{enrolled:?}");
    // The context's history, where its stores lie (`CLIENT.contexts/history-<name>`).
    let history = client.join("CLIENT.contexts").join("history-alice");
    for recorded in [&history, client] {
        assert_eq!(
            focal_node::storage_level::known(recorded).unwrap(),
            Some(at_rest - 1),
            "the cluster's fence, learned at enrollment, in {}",
            recorded.display()
        );
    }
    let alice = cli(
        client,
        Some("alice"),
        &["submit", "claim", "--json", &claim(&id(200), ENROLLED)],
    );
    assert_eq!(alice["condition"], "Committed", "{alice}");
    let theirs = alice["operation_id"].as_str().unwrap().to_owned();
    drop(server);

    // The node down: a claim whose outcome stays unknown, journaled where the command was told,
    // and an artifact whose upload stays pending, its bytes owned by the client's store.
    let journal = root.join("pending-claim");
    let unknown = pending(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim(&id(101), DESCRIPTION),
            "--operation",
            journal.to_str().unwrap(),
        ],
    );
    assert_eq!(unknown["condition"], "OutcomeUnknown", "{unknown}");
    let mut bytes = vec![b' '; 200_000];
    bytes.extend_from_slice(REPORT);
    let source = directory.path().join("report.json");
    std::fs::write(&source, &bytes).unwrap();
    let artifact = id(201);
    let schema = focal_evidence::test_report_schema().to_string();
    let upload = pending(
        root,
        None,
        &[
            "submit",
            "artifact",
            "--claim",
            &answered,
            "--receipt",
            &receipt,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            &set,
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--id",
            &artifact,
            "--payload-file",
            source.to_str().unwrap(),
            "--operation-id",
            &operation,
        ],
    );
    assert_eq!(upload["condition"], "UploadPending", "{upload}");
    std::fs::remove_file(&source).unwrap();

    // Below the fence every client record lies as every earlier binary wrote it: the local
    // client's journal, managed ownership and uploads, and the enrolled client's history.
    let local = vec![
        journal.clone(),
        root.join("client"),
        root.join("CLI.requests"),
        root.join("CLI.requests.managed-owner"),
        root.join("CLI.uploads"),
        root.join("CLI.uploads.initialized"),
    ];
    let theirs_stores = vec![history.clone()];
    let below_the_level: Vec<_> = files(&local)
        .into_iter()
        .chain(files(&theirs_stores))
        .collect();
    assert!(below_the_level.len() > 8, "{below_the_level:?}");
    for (path, bytes) in below_the_level {
        assert!(
            !bytes.starts_with(SEALED) && !bytes.starts_with(PART),
            "{} sealed below the level",
            path.display()
        );
    }
    assert!(
        std::fs::read(journal.join("state.bin"))
            .unwrap()
            .starts_with(JOURNAL)
    );

    // At the binary's level the fence rises, once the node reports the level; the node records it.
    let server = start(root, &advertise, None);
    let mut deadline = Deadline::after(Duration::from_secs(90));
    loop {
        let output = run(root, None, &["inspect", "upgrade"]);
        if output.status.success()
            && let Ok(value) = serde_json::from_slice::<Value>(&output.stdout)
            && let Some(nodes) = value["result"]["upgrade"]["nodes"].as_array()
            && !nodes.is_empty()
            && nodes
                .iter()
                .all(|node| node["capability"].as_u64() >= Some(u64::from(at_rest)))
        {
            break;
        }
        assert!(
            deadline.open(),
            "the node never reported the at-rest level: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let activated = admin(root, &["activate", "upgrade", "--fence", &level]);
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    let mut deadline = Deadline::after(Duration::from_secs(60));
    while focal_node::storage_level::known(root).unwrap() < Some(at_rest) {
        assert!(deadline.open(), "the node never recorded the fence");
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(server);
    let _server = start(root, &advertise, None);

    // Through the node's directory: the pending claim retried under its identity and answered
    // once, the second retry answered the same; the artifact's upload completes from its sealed
    // part, once, and its bytes read back exactly.
    let retried = cli(root, None, &["retry", "request", journal.to_str().unwrap()]);
    assert_eq!(retried["stage"], "Completed", "{retried}");
    assert_eq!(
        retried["result"]["claims"][0],
        id(101).as_str(),
        "{retried}"
    );
    let again = cli(root, None, &["retry", "request", journal.to_str().unwrap()]);
    assert_eq!(again["receipt"], retried["receipt"], "answered once");
    let committed = cli(
        root,
        None,
        &["retry", "request", "--operation-id", &operation],
    );
    assert_eq!(committed["condition"], "Committed", "{committed}");
    assert_eq!(committed["result"]["artifact"], artifact.as_str());
    // Delivered, the operation retires: a second retry is refused, never executed again.
    let twice = run(
        root,
        None,
        &[
            "retry",
            "request",
            "--operation-id",
            &operation,
            "--format",
            "json",
        ],
    );
    assert_eq!(twice.status.code(), Some(5), "{twice:?}");
    // A refusal's structured error is written where errors go.
    let twice: Value = serde_json::from_slice(&twice.stderr).unwrap();
    assert_eq!(twice["condition"], "Retired", "{twice}");
    let output_path = directory.path().join("downloaded.json");
    cli(
        root,
        None,
        &[
            "get",
            "artifact",
            &artifact,
            "--output",
            output_path.to_str().unwrap(),
        ],
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), bytes);
    assert_sealed(&local, &[REPORT, DESCRIPTION.as_bytes(), JOURNAL]);

    // The enrolled client learns the risen fence beside its first request, and its stores take the
    // form it opens at their next open.
    cli(client, Some("alice"), &["inspect", "epoch"]);
    for recorded in [&history, client] {
        assert_eq!(
            focal_node::storage_level::known(recorded).unwrap(),
            Some(at_rest),
            "the fence, learned beside a request, in {}",
            recorded.display()
        );
    }
    assert!(
        !client.join("FILES.SEALED").exists(),
        "the directory's files convert at its next open"
    );
    // Its delivered claim is refused a second execution from its converted store, and a new one
    // is written sealed beside it.
    let kept = run(
        client,
        Some("alice"),
        &[
            "retry",
            "request",
            "--operation-id",
            &theirs,
            "--format",
            "json",
        ],
    );
    assert_eq!(kept.status.code(), Some(5), "{kept:?}");
    // A refusal's structured error is written where errors go.
    let kept: Value = serde_json::from_slice(&kept.stderr).unwrap();
    assert_eq!(kept["condition"], "Retired", "{kept}");
    let next = cli(
        client,
        Some("alice"),
        &["submit", "claim", "--json", &claim(&id(202), ENROLLED)],
    );
    assert_eq!(next["condition"], "Committed", "{next}");
    assert_sealed(&theirs_stores, &[ENROLLED.as_bytes()]);
    assert!(
        client.join("FILES.SEALED").exists(),
        "the client directory's own files converted at its open"
    );
}
