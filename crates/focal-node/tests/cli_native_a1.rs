#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! The first product gate on the native engine: a real binary, two real
//! participants, offline activation, the complete claim cycle, a killed and
//! restarted node, identical reads afterwards and exact retries of journaled
//! frames (REMAINING §9 A1).
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
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
    // A fresh directory is already owner-only on Windows (its DACL is inherited
    // from the owner-owned temp root); on Unix, tighten it to 0700.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    #[cfg(not(unix))]
    let _ = path;
}
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn start(root: &Path, advertise: &str) -> Server {
    // Four rows per member: the balancer splits the group while the
    // workflow runs (doc 25 §8), so every step below runs across a reshape.
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args([
            "--data-dir",
            root.to_str().unwrap(),
            "start",
            "--advertise",
            advertise,
        ])
        .env("FOCAL_RANGE_TARGET_ENTRIES", "4")
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
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

#[test]
fn two_participants_complete_a_native_claim_cycle_through_the_binary_and_survive_a_kill() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-a1-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-a1-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();

    // Offline activation on the laptop before the node ever listens.
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(
        activation["result"]["kind"], "replica_native_activation_proposed",
        "{activation}"
    );
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);

    // The engine is reported by standing; the legacy read is not used.
    let status = admin(root, None, &["status"]);
    let standing = &objects(&status)[0]["Standing"];
    assert_eq!(standing["profile"], "AuthoredV1", "{status}");
    let issuer = hex_hash(&standing["principal"]);

    // Enroll the respondent as a second authenticated participant over QUIC.
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
    assert!(
        enrolled.status.success(),
        "enroll: {}
{}",
        String::from_utf8_lossy(&enrolled.stderr),
        String::from_utf8_lossy(&enrolled.stdout)
    );
    let alice_standing = admin(client.path(), Some("alice"), &["status"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);
    assert_ne!(alice, issuer);

    // Issuer authors the claim: one required delivery check plus one
    // programmatic test check on manifest slot zero, evaluated by the issuer.
    let claim_document = json!({
        "description": "Run the suite and deliver the report.",
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": 4_102_444_800_000u64}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &claim_document.to_string()],
    ));
    let claim = created(&result, "Claim").remove(0);
    let validation = created(&result, "Validation").remove(1);
    let (post_id, _) = committed(&cli(root, None, &["claim", "post", &claim]));
    let page = cli(root, None, &["get", "claim", &claim]);
    // Frozen vocabularies serialize as their registered codes: Posted is 2.
    assert_eq!(objects(&page)[0]["Claim"]["status"], 2, "{page}");

    // Respondent acquires the receipt and delivers work.
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
    let page = cli(
        client.path(),
        Some("alice"),
        &["get", "artifact", &artifact],
    );
    let hash = hex_hash(&objects(&page)[0]["Artifact"]["content_hash"]);
    let (_, result) = committed(&cli(
        client.path(),
        Some("alice"),
        &[
            "testament",
            "submit",
            "--claim",
            &claim,
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
        &["testament", "post", &testament, "--claim", &claim],
    ));

    // Issuer receives, evaluates and reports; acceptance is derived.
    committed(&cli(
        root,
        None,
        &["testament", "receive", &testament, "--claim", &claim],
    ));
    committed(&cli(
        root,
        None,
        &[
            "validation",
            "begin",
            "--claim",
            &claim,
            "--validation",
            &validation,
        ],
    ));
    let (report_id, _) = committed(&cli(
        root,
        None,
        &[
            "validation",
            "report",
            "--claim",
            &claim,
            "--validation",
            &validation,
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    let before = cli(root, None, &["get", "claim", &claim]);
    // Satisfied is code 8 of the frozen claim status vocabulary.
    assert_eq!(objects(&before)[0]["Claim"]["status"], 8, "{before}");
    let evaluations = cli(root, None, &["get", "validation", &validation]);
    assert!(
        objects(&evaluations)
            .iter()
            .any(|object| object["Evaluation"]["state"] == "Validated"),
        "{evaluations}"
    );

    // A retry of a committed operation prints the same receipt without a send.
    let retried = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &report_id],
    );
    assert_eq!(retried["condition"], "Committed");
    assert_eq!(retried["operation_id"], report_id);

    // Kill the node without warning; everything above must be durable.
    drop(server);
    let server = start(root, &advertise);
    let after = cli(root, None, &["get", "claim", &claim]);
    assert_eq!(objects(&after)[0]["Claim"], objects(&before)[0]["Claim"]);
    let retried_after = cli(
        root,
        None,
        &["request", "retry", "--operation-id", &report_id],
    );
    assert_eq!(
        retried_after["result"]["receipt"],
        retried["result"]["receipt"]
    );
    let posted = cli(
        root,
        None,
        &["request", "inspect", "--operation-id", &post_id],
    );
    assert_eq!(posted["condition"], "Committed");
    // A stale binding after restart is a closed refusal, not an unknown outcome.
    let stale = run(root, None, &["claim", "post", &claim, "--format", "json"]);
    assert_eq!(
        stale.status.code(),
        Some(5),
        "{}",
        String::from_utf8_lossy(&stale.stdout)
    );
    let refusal: Value = serde_json::from_slice(&stale.stdout).unwrap();
    assert_eq!(refusal["result"]["kind"], "error");

    // A reply lost on a closed stdout leaves the exact frame journaled; the
    // recovery command replays it and finds the committed receipt.
    {
        use std::os::{fd::OwnedFd, unix::net::UnixStream};
        let (closed, output) = UnixStream::pair().unwrap();
        drop(closed);
        let second = json!({
            "description": "A second request.",
            "target": alice,
            "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}]
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
        command
            .args([
                "--data-dir",
                root.to_str().unwrap(),
                "submit",
                "claim",
                "--json",
                &second.to_string(),
                "--format",
                "json",
            ])
            .stdout(Stdio::from(OwnedFd::from(output)))
            .stderr(Stdio::piped());
        let failed = command.output().unwrap();
        assert!(!failed.status.success());
        let pending = cli(root, None, &["request", "pending"]);
        let rows: Vec<&Value> = pending["operations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["client"] == "CLI native")
            .collect();
        assert_eq!(rows.len(), 1, "{pending}");
        assert_eq!(
            rows[0]["condition"],
            "Committed",
            "{}",
            String::from_utf8_lossy(&failed.stderr)
        );
        let id = rows[0]["operation_id"].as_str().unwrap().to_string();
        let diagnostic = String::from_utf8(failed.stderr).unwrap();
        assert!(
            diagnostic.contains(&format!("request retry --operation-id {id}")),
            "{diagnostic}"
        );
        let replayed = cli(root, None, &["request", "retry", "--operation-id", &id]);
        let (_, result) = committed(&replayed);
        assert_eq!(created(&result, "Claim").len(), 1);
    }
    // The balancer reshaped the group under the workflow: the map holds
    // several members, every one with rows, at a later range epoch.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let output = run(root, None, &["cluster", "replicas", "ranges", "list"]);
        let view: Option<Value> = output
            .status
            .success()
            .then(|| serde_json::from_slice::<Value>(&output.stdout).ok())
            .flatten()
            .map(|value| value["result"]["ranges"].clone());
        if let Some(view) = &view
            && view["members"].as_array().is_some_and(|members| {
                members.len() >= 2 && members.iter().all(|member| member["entries"] != 0)
            })
            && view["epoch"].as_u64().is_some_and(|epoch| epoch >= 2)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the balancer never split the group: {view:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    drop(server);
}
