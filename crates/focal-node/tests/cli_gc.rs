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
//! The collector through the real binary (26 §5): an object sealed by an
//! upload that never bound to a row is quarantined once past the grace, a
//! work artifact's sealed payload stays while its claim is live and after
//! its family retired (the bundle's header names it), the bundle itself
//! stays and still verifies, the operator sees the pass and brings the
//! orphan back from quarantine, and a killed and restarted node keeps the
//! quarantine.
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
fn gc_status(root: &Path) -> Value {
    let shown = admin(root, &["cluster", "gc", "show"]);
    assert_eq!(shown["result"]["kind"], "gc", "{shown}");
    shown["result"]["gc"].clone()
}
/// Poll the collector until a completed pass satisfies `condition`.
fn pass_where(root: &Path, condition: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = gc_status(root);
        if status["passes"].as_u64().unwrap() > 0
            && status["last"]["content"]["complete"] == true
            && condition(&status["last"])
        {
            return status;
        }
        assert!(Instant::now() < deadline, "no pass satisfied: {status}");
        std::thread::sleep(Duration::from_millis(250));
    }
}
fn manifests(root: &Path, tenant: &str) -> Vec<String> {
    let directory = root.join("content").join("objects").join(tenant);
    let mut names: Vec<String> = std::fs::read_dir(&directory)
        .map(|entries| {
            entries
                .filter_map(|entry| {
                    let name = entry.unwrap().file_name().to_str().unwrap().to_owned();
                    name.strip_suffix(".manifest").map(str::to_owned)
                })
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}
const FAR: u64 = 4_102_444_800_000;
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

#[test]
fn unreferenced_objects_leave_through_quarantine_while_proof_stays() {
    let founder = tempfile::Builder::new()
        .prefix("focal-gc-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-gc-client-")
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
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
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

    // A claim whose work artifact's inline payload is sealed as an object.
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
    let bound = manifests(root, &tenant);
    assert_eq!(
        bound.len(),
        1,
        "the sealed payload is one object: {bound:?}"
    );
    // A second delivery to the filled slot is refused by the owner after
    // the node sealed its payload under custody: an object no row names.
    let refused = run(
        client.path(),
        Some("alice"),
        &[
            "artifact",
            "submit",
            "--claim",
            &claim,
            "--slot",
            "0",
            "--text",
            r#"{"passed":0,"failed":9,"skipped":0}"#,
            "--format",
            "json",
        ],
    );
    assert!(
        !refused.status.success(),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    let mut all = manifests(root, &tenant);
    assert_eq!(all.len(), 2, "{all:?}");
    let orphan = all
        .iter()
        .find(|name| !bound.contains(name))
        .cloned()
        .unwrap();
    // Past the grace the orphan leaves through quarantine; the bound
    // payload stays, protected by the claim's row.
    let status = pass_where(root, |last| {
        last["content"]["objects_quarantined"].as_u64().unwrap() >= 1
    });
    let last = &status["last"];
    assert_eq!(last["opaque_domains"], 0, "{status}");
    assert!(last["protected_objects"].as_u64().unwrap() >= 1, "{status}");
    assert_eq!(last["replicas"], 1, "{status}");
    assert_eq!(manifests(root, &tenant), bound);
    let config = &status["config"];
    assert_eq!(config["grace_ms"], 1500);
    assert_eq!(config["quarantine_ms"], 600_000);
    assert_eq!(config["interval_ms"], 300);
    // The orphan comes back on request, exactly once.
    let restored = admin(
        root,
        &[
            "cluster", "gc", "restore", "--domain", &tenant, "--root", &orphan,
        ],
    );
    assert_eq!(restored["result"]["kind"], "gc_restored", "{restored}");
    assert_eq!(restored["result"]["restored"], true);
    all = manifests(root, &tenant);
    assert!(all.contains(&orphan), "{all:?}");
    let again = admin(
        root,
        &[
            "cluster", "gc", "restore", "--domain", &tenant, "--root", &orphan,
        ],
    );
    assert_eq!(again["result"]["restored"], false);
    // A later pass takes it again; its bytes are still old.
    let before = status["passes"].as_u64().unwrap();
    pass_where(root, |_| {
        gc_status(root)["passes"].as_u64().unwrap() > before + 1
    });
    assert_eq!(manifests(root, &tenant), bound);
    // The claim retires: its family's bundle is a new object, the payload's
    // object stays because the bundle's header names it, and the bundle
    // still verifies after the collector ran over it.
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
    let after = gc_status(root)["passes"].as_u64().unwrap();
    let status = pass_where(root, |_| {
        gc_status(root)["passes"].as_u64().unwrap() > after + 1
    });
    assert_eq!(status["last"]["bundles_unreadable"], 0, "{status}");
    assert!(
        status["last"]["protected_objects"].as_u64().unwrap() >= 2,
        "{status}"
    );
    let retired = manifests(root, &tenant);
    assert_eq!(retired.len(), 2, "payload and bundle: {retired:?}");
    assert!(retired.contains(&bound[0]));
    let archive = admin(root, &["cluster", "archive", "show", "--claim", &claim]);
    assert_eq!(archive["result"]["archive"]["verified"], true, "{archive}");
    let bundle = archive["result"]["archive"]["bundle"].as_str().unwrap();
    assert!(retired.contains(&bundle.to_owned()));
    let _ = artifact;
    // A kill and restart keep the quarantine: the orphan still comes back.
    drop(server);
    let _server = start(root, &advertise);
    let restored = admin(
        root,
        &[
            "cluster", "gc", "restore", "--domain", &tenant, "--root", &orphan,
        ],
    );
    assert_eq!(restored["result"]["restored"], true, "{restored}");
    assert!(manifests(root, &tenant).contains(&orphan));
    assert_eq!(gc_status(root)["node"], identity["node"]);
}
