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
//! The fourth product gate on the native engine through the real binary:
//! concurrent clients on their durable request journals, replies lost before
//! and after commitment by crash cuts at the durable boundaries, client and
//! server restarts, exact reconciliation, and exhausted admission capacity
//! that still answers exact retries of committed work (REMAINING §9 A4).
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    net::UdpSocket,
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
fn address() -> String {
    // Probe outside the ephemeral range so a concurrent test cannot take the
    // port between the probe and the bind.
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
/// Administrative and status commands print JSON without a format flag.
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
fn committed(value: &Value) -> (String, Value) {
    assert_eq!(value["condition"], "Committed", "{value}");
    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["result"]["kind"], "native");
    let id = value["operation_id"].as_str().unwrap().to_string();
    assert!(id.starts_with("n1:"));
    (id, value["result"].clone())
}
fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| {
            entry["id"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
                .collect::<String>()
        })
        .collect()
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
/// Codes of the frozen claim status vocabulary.
const POSTED: u64 = 2;

fn start_with(root: &Path, advertise: &str, env: &[(&str, &str)]) -> Server {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command
        .args([
            "--data-dir",
            root.to_str().unwrap(),
            "start",
            "--advertise",
            advertise,
        ])
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().unwrap();
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
fn sequence(root: &Path) -> u64 {
    admin(root, None, &["status"])["result"]["page"]["native_sequence"]
        .as_u64()
        .unwrap()
}
fn claim_document(target: &str, description: &str) -> Value {
    json!({
        "description": description,
        "target": target,
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]
    })
}
/// A failed command's structured output (stdout or stderr) with its exit code.
fn failed(root: &Path, context: Option<&str>, args: &[&str]) -> (i32, Value) {
    let mut full = args.to_vec();
    full.extend(["--format", "json"]);
    let output = run(root, context, &full);
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
fn native_rows(pending: &Value) -> Vec<Value> {
    pending["operations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["client"] == "CLI native")
        .cloned()
        .collect()
}
fn claims_of(root: &Path, issuer: &str) -> Vec<String> {
    let page = cli(root, None, &["list", "claims", "--source", issuer]);
    assert_eq!(page["result"]["kind"], "native_list", "{page}");
    page["result"]["page"]["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| hex_hash(&object["Claim"]["binding"]["object"]))
        .collect()
}

#[test]
fn concurrent_clients_lost_replies_restarts_and_exhausted_capacity_reconcile_exactly_once() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-a4-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-a4-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let alice_root = client.path();
    let alice_ctx = Some("alice");
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let issuer = hex_hash(&objects(&admin(root, None, &["status"]))[0]["Standing"]["principal"]);
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
        alice_root,
        None,
        &[
            "context",
            "enroll",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(
        enrolled.status.success(),
        "enroll: {}
{}",
        String::from_utf8_lossy(&enrolled.stderr),
        String::from_utf8_lossy(&enrolled.stdout)
    );
    let alice =
        hex_hash(&objects(&admin(alice_root, alice_ctx, &["status"]))[0]["Standing"]["principal"]);

    // ---- Concurrency: six processes on two durable journals at once ----
    let before = sequence(root);
    let handles: Vec<_> = (0..6)
        .map(|index| {
            let (dir, context, target, description) = if index % 2 == 0 {
                (
                    root.to_path_buf(),
                    None,
                    alice.clone(),
                    format!("Issuer claim {index}."),
                )
            } else {
                (
                    alice_root.to_path_buf(),
                    Some("alice"),
                    issuer.clone(),
                    format!("Respondent claim {index}."),
                )
            };
            std::thread::spawn(move || {
                let document = claim_document(&target, &description).to_string();
                let value = cli(&dir, context, &["submit", "claim", "--json", &document]);
                committed(&value)
            })
        })
        .collect();
    let mut ids: Vec<String> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().0)
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 6, "{ids:?}");
    assert_eq!(sequence(root), before + 6);
    assert!(native_rows(&cli(root, None, &["request", "pending"])).is_empty());
    assert!(native_rows(&cli(alice_root, alice_ctx, &["request", "pending"])).is_empty());
    assert_eq!(claims_of(root, &issuer).len(), 3);
    assert_eq!(claims_of(root, &alice).len(), 3);

    // ---- Reply lost before commitment: the node aborts after receiving
    // the frame and before proposing it, so nothing is committed ----
    let before = sequence(root);
    drop(server);
    let server = start_with(root, &advertise, &[("FOCAL_FAULT", "before-propose:1")]);
    let cut_document = claim_document(&alice, "Cut before the proposal.").to_string();
    let (code, unknown) = failed(root, None, &["submit", "claim", "--json", &cut_document]);
    assert_eq!(code, 7, "{unknown}");
    assert_eq!(unknown["condition"], "OutcomeUnknown", "{unknown}");
    let lost_before = unknown["operation_id"].as_str().unwrap().to_string();
    drop(server);
    let server = start(root, &advertise);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(sequence(root), before, "the cut node committed nothing");
    let rows = native_rows(&cli(root, None, &["request", "pending"]));
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["condition"], "Pending", "{rows:?}");
    assert_eq!(rows[0]["operation_id"], lost_before, "{rows:?}");
    // The journaled frame is sent again unchanged and commits exactly once.
    let replayed = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &lost_before],
    );
    let (replayed_id, replayed_result) = committed(&replayed);
    assert_eq!(replayed_id, lost_before);
    assert_eq!(sequence(root), before + 1);
    let again = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &lost_before],
    );
    assert_eq!(again["result"]["receipt"], replayed_result["receipt"]);
    assert_eq!(sequence(root), before + 1);
    assert!(native_rows(&cli(root, None, &["request", "pending"])).is_empty());

    // ---- Reply lost after commitment: the node aborts after the owner
    // committed the frame and before any reply was written ----
    let before = sequence(root);
    drop(server);
    let server = start_with(
        root,
        &advertise,
        &[("FOCAL_FAULT", "after-commit-before-reply:1")],
    );
    let cut_document = claim_document(&alice, "Cut after the commit.").to_string();
    let (code, unknown) = failed(root, None, &["submit", "claim", "--json", &cut_document]);
    assert_eq!(code, 7, "{unknown}");
    let lost_after = unknown["operation_id"].as_str().unwrap().to_string();
    drop(server);
    let server = start(root, &advertise);
    // The restarted node re-commits its durable tail in its new term; the
    // acknowledged commit is there, never lost.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while sequence(root) < before + 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the commit did not survive the cut"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(sequence(root), before + 1, "the commit survived the cut");
    let rows = native_rows(&cli(root, None, &["request", "pending"]));
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["operation_id"], lost_after, "{rows:?}");
    // The exact retry is answered by the owner's committed outcome for the
    // same identity: no second claim, the sequence does not move.
    let reconciled = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &lost_after],
    );
    let (reconciled_id, reconciled_result) = committed(&reconciled);
    assert_eq!(reconciled_id, lost_after);
    assert_eq!(sequence(root), before + 1);
    let remote = cli(
        root,
        None,
        &[
            "request",
            "inspect",
            "--operation-id",
            &lost_after,
            "--remote",
        ],
    );
    assert_eq!(remote["condition"], "Observed", "{remote}");
    assert_eq!(
        objects(&remote)[0]["Outcome"]["intent"],
        reconciled_result["receipt"]["intent"]
    );
    assert!(native_rows(&cli(root, None, &["request", "pending"])).is_empty());
    let issuer_claims = claims_of(root, &issuer);
    assert_eq!(issuer_claims.len(), 5, "{issuer_claims:?}");

    // ---- Exhausted admission capacity: the WAL volume must keep more free
    // space than it can have, so every fresh candidate is refused while the
    // exact retry of committed work is still answered and its bytes and
    // receipt are unchanged ----
    let posted_document = claim_document(&alice, "Posted before the pressure.").to_string();
    let (post_id, post_result) = {
        let (_, result) = committed(&cli(
            root,
            None,
            &["submit", "claim", "--json", &posted_document],
        ));
        let id = created(&result, "Claim").remove(0);
        let (post_id, post_result) = committed(&cli(root, None, &["claim", "post", &id]));
        assert_eq!(
            objects(&cli(root, None, &["get", "claim", &id]))[0]["Claim"]["status"],
            POSTED
        );
        (post_id, post_result)
    };
    let before = sequence(root);
    drop(server);
    let server = start_with(
        root,
        &advertise,
        &[("FOCAL_DISK_HEADROOM_BYTES", "18446744073709551615")],
    );
    let (code, refused) = failed(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "Refused under pressure.").to_string(),
        ],
    );
    assert_eq!(code, 6, "{refused}");
    assert_eq!(refused["result"]["code"], "capacity", "{refused}");
    let refused_id = refused["operation_id"].as_str().unwrap().to_string();
    assert_eq!(sequence(root), before);
    let retried = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &post_id],
    );
    assert_eq!(retried["condition"], "Committed", "{retried}");
    assert_eq!(retried["result"]["receipt"], post_result["receipt"]);
    let observed = cli(
        root,
        None,
        &["request", "inspect", "--operation-id", &post_id, "--remote"],
    );
    assert_eq!(observed["condition"], "Observed", "{observed}");
    assert_eq!(
        objects(&observed)[0]["Outcome"]["intent"],
        post_result["receipt"]["intent"]
    );
    assert_eq!(sequence(root), before);
    // The refusal had no effect: the reference stays journaled as pending
    // with its exact frame, and retrying it under the same pressure is the
    // same refusal.
    let inspected = cli(
        root,
        None,
        &["request", "inspect", "--operation-id", &refused_id],
    );
    assert_eq!(inspected["condition"], "Pending", "{inspected}");
    let (code, refused_again) = failed(
        root,
        None,
        &["request", "retry", "--operation-id", &refused_id],
    );
    assert_eq!(code, 6, "{refused_again}");
    assert_eq!(sequence(root), before);
    // Pressure lifted: the retained reference commits exactly once, fresh
    // work is admitted again and nothing was lost.
    drop(server);
    let server = start(root, &advertise);
    let (admitted_id, admitted) = committed(&cli(
        root,
        None,
        &["request", "retry", "--operation-id", &refused_id],
    ));
    assert_eq!(admitted_id, refused_id);
    assert_eq!(created(&admitted, "Claim").len(), 1);
    assert_eq!(sequence(root), before + 1);
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&alice, "Admitted after the pressure.").to_string(),
        ],
    ));
    assert_eq!(created(&result, "Claim").len(), 1);
    assert_eq!(sequence(root), before + 2);
    assert_eq!(claims_of(root, &issuer).len(), 8);
    assert!(native_rows(&cli(root, None, &["request", "pending"])).is_empty());
    drop(server);
}
