#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
#![cfg(unix)]
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
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
fn start(root: &Path) -> Server {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "start"])
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
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap()["condition"],
        "Ready"
    );
    server
}
fn command(root: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_focal"));
    cmd.args(["--data-dir", root.to_str().unwrap()]).args(args);
    cmd
}
fn run(root: &Path, args: &[&str]) -> Output {
    command(root, args).output().unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn quiet_cli(root: &Path, args: &[&str]) -> Value {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "successful managed command printed diagnostics: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn closed_stderr(root: &Path, args: &[&str]) -> Output {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    let (closed, error) = UnixStream::pair().unwrap();
    drop(closed);
    command(root, args)
        .stdout(Stdio::piped())
        .stderr(Stdio::from(OwnedFd::from(error)))
        .output()
        .unwrap()
}
fn claim() -> Value {
    json!({"target":"self","action":"handoff","description":"Checked human command","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive evidence","evaluator":"self"}]})
}
fn pending(root: &Path) -> Value {
    cli(root, &["request", "pending", "--format", "json"])["operations"].clone()
}
fn sequence(root: &Path) -> Value {
    cli(root, &["status"])["result"]["Read"]["token"]["sequence"].clone()
}

#[test]
fn default_human_commands_retire_beyond_window_without_manual_cleanup_and_keep_legacy_paths() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let _server = start(root.path());
    let initial = sequence(root.path()).as_u64().unwrap();
    let document = claim().to_string();
    let mut first = None;
    for _ in 0..40 {
        let reply = quiet_cli(
            root.path(),
            &["submit", "claim", "--json", &document, "--format", "json"],
        );
        assert_eq!(reply["condition"], "Committed");
        assert!(reply["operation_id"].as_str().unwrap().starts_with("m1:"));
        assert_eq!(reply["result"]["claims"].as_array().unwrap().len(), 1);
        if first.is_none() {
            first = Some(reply["operation_id"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(sequence(root.path()).as_u64().unwrap(), initial + 40);
    assert_eq!(pending(root.path()), json!([]));
    let first = first.unwrap();
    let retired = cli(
        root.path(),
        &[
            "request",
            "inspect",
            "--operation-id",
            &first,
            "--format",
            "json",
        ],
    );
    assert_eq!(retired["condition"], "Retired");
    assert_eq!(
        run(root.path(), &["request", "retry", "--operation-id", &first])
            .status
            .code(),
        Some(5)
    );
    assert_eq!(sequence(root.path()).as_u64().unwrap(), initial + 40);
    let legacy = root.path().join("legacy journal");
    let reply = cli(
        root.path(),
        &[
            "submit",
            "claim",
            "--json",
            &document,
            "--operation",
            legacy.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(reply["stage"], "Completed");
    let before = sequence(root.path());
    let retried = cli(
        root.path(),
        &[
            "request",
            "retry",
            legacy.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert_eq!(retried["receipt"], reply["receipt"]);
    assert_eq!(sequence(root.path()), before);
    let before = sequence(root.path()).as_u64().unwrap();
    let success = closed_stderr(
        root.path(),
        &["submit", "claim", "--json", &document, "--format", "json"],
    );
    assert!(success.status.success());
    let success: Value = serde_json::from_slice(&success.stdout).unwrap();
    assert_eq!(success["condition"], "Committed");
    assert_eq!(sequence(root.path()).as_u64().unwrap(), before + 1);
    assert_eq!(pending(root.path()), json!([]));
}

#[test]
fn broken_stdout_keeps_exact_committed_request_for_retry_after_restart() {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let server = start(root.path());
    let document = claim().to_string();
    let (closed, output) = UnixStream::pair().unwrap();
    drop(closed);
    let failed = command(
        root.path(),
        &["submit", "claim", "--json", &document, "--format", "json"],
    )
    .stdout(Stdio::from(OwnedFd::from(output)))
    .stderr(Stdio::piped())
    .output()
    .unwrap();
    assert!(!failed.status.success());
    let rows = pending(root.path());
    assert_eq!(rows.as_array().unwrap().len(), 1);
    let id = rows[0]["operation_id"].as_str().unwrap().to_string();
    assert_eq!(rows[0]["condition"], "Committed");
    let diagnostic = String::from_utf8(failed.stderr).unwrap();
    assert!(diagnostic.contains(&format!(
        "Recovery: focal --data-dir '{}' --client-context 'local' request retry --operation-id {id}",
        root.path().display()
    )));
    let inspected = cli(
        root.path(),
        &[
            "request",
            "inspect",
            "--operation-id",
            &id,
            "--format",
            "json",
        ],
    );
    let receipt = inspected["receipt"].clone();
    // A later successfully delivered result cannot retire this earlier receipt
    // merely because both are present locally.
    let later = cli(
        root.path(),
        &["submit", "claim", "--json", &document, "--format", "json"],
    );
    assert_eq!(later["condition"], "Committed");
    assert_eq!(pending(root.path()).as_array().unwrap().len(), 2);
    let prefix = sequence(root.path());
    drop(server);
    let _server = start(root.path());
    let remote = cli(
        root.path(),
        &[
            "request",
            "inspect",
            "--operation-id",
            &id,
            "--remote",
            "--format",
            "json",
        ],
    );
    assert!(remote["reply"]["page"]["result"]["Receipt"]["resolution"]["Retained"].is_object());
    assert_eq!(pending(root.path()).as_array().unwrap().len(), 2);
    let retried = quiet_cli(
        root.path(),
        &[
            "request",
            "retry",
            "--operation-id",
            &id,
            "--format",
            "json",
        ],
    );
    assert_eq!(retried["receipt"], receipt);
    assert_eq!(sequence(root.path()), prefix);
    assert_eq!(pending(root.path()), json!([]));
}

#[test]
fn reserved_ids_are_not_business_commands_and_refusal_stays_pending_until_explicit_seal() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let _server = start(root.path());
    let initial = sequence(root.path());
    let bad = run(
        root.path(),
        &["submit", "claim", "--json", "{\"unknown\":true}"],
    );
    assert_eq!(bad.status.code(), Some(2));
    assert_eq!(pending(root.path()), json!([]));
    assert!(!root.path().join("CLI.requests").exists());
    let reserved = cli(root.path(), &["request", "reserve", "--format", "json"]);
    let id = reserved["operation_id"].as_str().unwrap();
    assert_eq!(reserved["condition"], "Reserved");
    assert_eq!(sequence(root.path()), initial);
    assert_eq!(
        cli(
            root.path(),
            &[
                "request",
                "inspect",
                "--operation-id",
                id,
                "--format",
                "json"
            ]
        )["condition"],
        "Reserved"
    );
    let unprepared = run(root.path(), &["request", "retry", "--operation-id", id]);
    assert!(!unprepared.status.success());
    let diagnostic = String::from_utf8(unprepared.stderr).unwrap();
    assert!(diagnostic.contains(&format!("Saved reservation: {id}")));
    assert!(diagnostic.contains("request pending"));
    assert!(diagnostic.contains(&format!("request seal --operation-id {id}")));
    assert!(!diagnostic.contains("request retry --operation-id"));
    assert_eq!(sequence(root.path()), initial);
    let refused = run(
        root.path(),
        &[
            "claim",
            "post",
            "00000000000000000000000000000999",
            "--operation-id",
            id,
            "--format",
            "json",
        ],
    );
    assert_eq!(
        refused.status.code(),
        Some(5),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    let outcome: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(outcome["condition"], "DomainOutcome");
    assert!(
        String::from_utf8_lossy(&refused.stderr)
            .contains(&format!("request retry --operation-id {id}"))
    );
    assert_eq!(pending(root.path())[0]["operation_id"], id);
    let refused_without_stderr = closed_stderr(
        root.path(),
        &["request", "retry", "--operation-id", id, "--format", "json"],
    );
    assert_eq!(refused_without_stderr.status.code(), Some(5));
    assert_eq!(
        serde_json::from_slice::<Value>(&refused_without_stderr.stdout).unwrap()["condition"],
        "DomainOutcome"
    );
    assert_eq!(
        run(root.path(), &["request", "retry", "--operation-id", id])
            .status
            .code(),
        Some(5)
    );
    assert_eq!(sequence(root.path()), initial);
    let sealed = quiet_cli(
        root.path(),
        &["request", "seal", "--operation-id", id, "--format", "json"],
    );
    assert_eq!(sealed["condition"], "Sealed");
    assert_eq!(sequence(root.path()), initial);
    assert_eq!(pending(root.path()), json!([]));
    assert_eq!(
        run(root.path(), &["request", "retry", "--operation-id", id])
            .status
            .code(),
        Some(5)
    );
}

#[test]
fn managed_flags_json_yaml_keep_shared_authored_identity_and_explicit_path_syntax() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let _server = start(root.path());
    let mut document = claim();
    document["id"] = json!(format!("{:032x}", 501));
    document["occurrence"] = json!(format!("{:032x}", 502));
    document["validations"][0]["id"] = json!(format!("{:032x}", 503));
    let a = cli(
        root.path(),
        &[
            "submit",
            "claim",
            "--json",
            &document.to_string(),
            "--format",
            "json",
        ],
    );
    let b = cli(
        root.path(),
        &[
            "submit",
            "claim",
            "--id",
            document["id"].as_str().unwrap(),
            "--occurrence",
            document["occurrence"].as_str().unwrap(),
            "--target",
            "self",
            "--action",
            "handoff",
            "--description",
            "Checked human command",
            "--validation-json",
            &document["validations"][0].to_string(),
            "--format",
            "json",
        ],
    );
    let yaml = format!(
        "id: '{:032x}'\noccurrence: '{:032x}'\ntarget: self\naction: handoff\ndescription: Checked human command\nvalidations:\n  - id: '{:032x}'\n    kind: receipt\n    phase: whole_work\n    mode: required\n    description: Receive evidence\n    evaluator: self",
        501, 502, 503
    );
    let c = cli(
        root.path(),
        &["submit", "claim", "--yaml", &yaml, "--format", "json"],
    );
    assert_eq!(a["receipt"]["intent_hash"], b["receipt"]["intent_hash"]);
    assert_eq!(a["receipt"]["intent_hash"], c["receipt"]["intent_hash"]);
    assert_ne!(a["operation_id"], b["operation_id"]);
    assert_eq!(a["result"], b["result"]);
    assert_eq!(a["result"], c["result"]);
    assert_eq!(
        run(
            root.path(),
            &[
                "request",
                "retry",
                "some-path",
                "--operation-id",
                a["operation_id"].as_str().unwrap()
            ]
        )
        .status
        .code(),
        Some(2)
    );
}

/// One CLI invocation whose managed generations rotate after `bound` ordinals.
fn rotating(root: &Path, bound: &str, args: &[&str]) -> Value {
    let output = command(root, args)
        .env("FOCAL_MANAGED_ROTATION", bound)
        .output()
        .unwrap();
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

#[test]
fn a_bounded_generation_rotates_automatically_and_retires_its_references_across_processes() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let server = start(root.path());
    let initial = sequence(root.path()).as_u64().unwrap();
    let document = claim().to_string();
    let mut ids = Vec::new();
    // Each command is its own process; the shared bound is saved with the
    // owner record on first use and every later process must match it.
    for _ in 0..7 {
        let reply = rotating(
            root.path(),
            "3",
            &["submit", "claim", "--json", &document, "--format", "json"],
        );
        assert_eq!(reply["condition"], "Committed", "{reply}");
        ids.push(reply["operation_id"].as_str().unwrap().to_string());
    }
    assert_eq!(sequence(root.path()).as_u64().unwrap(), initial + 7);
    // Generations: ordinals 1..3 of generation one, then a rotation, and so
    // on; the managed reference carries the generation and ordinal.
    let generation = |id: &str| u64::from_str_radix(id.split(':').nth(2).unwrap(), 16).unwrap();
    let ordinal = |id: &str| u64::from_str_radix(id.split(':').nth(3).unwrap(), 16).unwrap();
    let generations: Vec<u64> = ids.iter().map(|id| generation(id)).collect();
    let ordinals: Vec<u64> = ids.iter().map(|id| ordinal(id)).collect();
    assert_eq!(ordinals, [1, 2, 3, 1, 2, 3, 1], "{ids:?}");
    assert!(generations[0] == generations[1] && generations[1] == generations[2]);
    assert!(generations[3] > generations[2], "{generations:?}");
    assert!(generations[6] > generations[5], "{generations:?}");
    assert!(
        std::fs::read_dir(root.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("CLI.requests.g")),
        "the rotated generation lives in its own store"
    );
    assert!(!root.path().join("CLI.requests").exists());
    // A different bound cannot open the saved owner record.
    let mismatch = command(
        root.path(),
        &["submit", "claim", "--json", &document, "--format", "json"],
    )
    .env("FOCAL_MANAGED_ROTATION", "4")
    .output()
    .unwrap();
    assert!(!mismatch.status.success());
    // Retired references stay retired across processes and a restart, and
    // never execute again; nothing is pending.
    for id in &ids[..6] {
        let inspected = rotating(
            root.path(),
            "3",
            &[
                "request",
                "inspect",
                "--operation-id",
                id,
                "--format",
                "json",
            ],
        );
        assert_eq!(inspected["condition"], "Retired", "{inspected}");
    }
    assert_eq!(
        rotating(
            root.path(),
            "3",
            &["request", "pending", "--format", "json"]
        )["operations"],
        json!([])
    );
    drop(server);
    let _server = start(root.path());
    let retry = command(
        root.path(),
        &["request", "retry", "--operation-id", &ids[0]],
    )
    .env("FOCAL_MANAGED_ROTATION", "3")
    .output()
    .unwrap();
    assert_eq!(retry.status.code(), Some(5));
    assert_eq!(sequence(root.path()).as_u64().unwrap(), initial + 7);
    let reply = rotating(
        root.path(),
        "3",
        &["submit", "claim", "--json", &document, "--format", "json"],
    );
    assert_eq!(reply["condition"], "Committed", "{reply}");
    assert_eq!(ordinal(reply["operation_id"].as_str().unwrap()), 2);
    assert_eq!(sequence(root.path()).as_u64().unwrap(), initial + 8);
}
