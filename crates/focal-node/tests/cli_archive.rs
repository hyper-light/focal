#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! The audit's F11: a completed two-party workflow retires to the archive,
//! and every exact identity a participant kept — the claim, its artifact, its
//! validation with the evaluation and the accepted result, its testament —
//! is followed through `get archived` into the bundle, read as the live core
//! read it, across a kill and restart; custody this node lacks, an object the
//! bundle never held and a live family are told apart.
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
fn scratch(prefix: &str) -> tempfile::TempDir {
    // Unix keeps the path short for the Unix-socket path limit (/tmp); Windows
    // names its pipe by a hash of the data directory, so the default temp root
    // is fine there.
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    #[cfg(unix)]
    {
        builder.tempdir_in("/tmp").unwrap()
    }
    #[cfg(not(unix))]
    {
        builder.tempdir().unwrap()
    }
}
#[path = "support/deadline.rs"]
mod deadline;
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn start(root: &Path, advertise: &str) -> Server {
    deadline::observe(root);
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
        // The archive agent retires a released family at once instead of
        // after its day of grace, and looks every fifth of a second.
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

/// Poll `get claim` until the continuation replaces the claim (26 §4).
fn retired(root: &Path, claim: &str) -> Value {
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    loop {
        let output = run(root, None, &["get", "claim", claim, "--format", "json"]);
        let last = if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
        {
            let object = objects(&page)[0].clone();
            if object.get("Retired").is_some() {
                return object["Retired"].clone();
            }
            object
        } else {
            json!({"stderr": String::from_utf8_lossy(&output.stderr), "stdout": String::from_utf8_lossy(&output.stdout)})
        };
        if !deadline.open() {
            let retention = admin(root, None, &["cluster", "retention", "show"]);
            panic!("claim {claim} never retired: {retention}\n{last}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
/// The archived objects of one `get archived` page, each with the bundle it
/// came from checked against the continuation.
fn archived(page: &Value, continuation: &Value) -> Vec<Value> {
    objects(page)
        .iter()
        .map(|object| {
            let archived = &object["Archived"];
            assert!(!archived.is_null(), "{object}");
            assert_eq!(archived["bundle"], continuation["bundle"], "{object}");
            assert_eq!(archived["bytes"], continuation["bytes"], "{object}");
            assert_eq!(archived["through"], continuation["through"], "{object}");
            archived["object"].clone()
        })
        .collect()
}

#[test]
fn a_retired_family_is_read_from_its_bundle_by_every_identity_a_participant_kept() {
    let founder = scratch("focal-archive-");
    let client = scratch("focal-archive-client-");
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
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

    // ---- The complete two-party cycle (the A1 gate) ----
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
    let live_artifact = objects(&cli(
        client.path(),
        Some("alice"),
        &["get", "artifact", &artifact],
    ))[0]["Artifact"]
        .clone();
    let hash = hex_hash(&live_artifact["content_hash"]);
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
    committed(&cli(
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
    // What the live ledger says, before the family leaves it.
    let live_claim = objects(&cli(root, None, &["get", "claim", &claim]))[0]["Claim"].clone();
    assert_eq!(live_claim["status"], 8, "{live_claim}");
    let live_validation = cli(root, None, &["get", "validation", &validation]);
    let live_definition = objects(&live_validation)[0]["Definition"].clone();
    let live_evaluation = objects(&live_validation)
        .iter()
        .find_map(|object| object.get("Evaluation"))
        .unwrap()
        .clone();
    assert_eq!(live_evaluation["state"], "Validated", "{live_validation}");
    let live_testament =
        objects(&cli(root, None, &["get", "testament", &testament]))[0]["Response"].clone();
    // A live family answers `get archived` from the ledger, unwrapped: the
    // read says which it was.
    let online = cli(
        root,
        None,
        &["get", "archived", &claim, "--artifact", &artifact],
    );
    assert_eq!(objects(&online)[0]["Artifact"], live_artifact, "{online}");

    // ---- Retirement: satisfied and released, the family leaves ----
    committed(&cli(root, None, &["claim", "release-scope", &claim]));
    let continuation = retired(root, &claim);
    assert_eq!(hex_hash(&continuation["claim"]), claim);
    // The plain reads say the rows left: the artifact and the testament are
    // missing, the validation's key names the retired claim.
    let gone = cli(root, None, &["get", "artifact", &artifact]);
    assert!(
        objects(&gone)
            .iter()
            .all(|object| object.get("Missing").is_some()),
        "{gone}"
    );

    // ---- Every identity kept is followed into the bundle ----
    let page = cli(root, None, &["get", "archived", &claim]);
    let found = archived(&page, &continuation);
    assert_eq!(found.len(), 1);
    // The claim as it was at its last event: the release, one revision
    // past the satisfied claim read above, and what the continuation says.
    let binding = &found[0]["Claim"]["binding"];
    assert_eq!(binding["object"], live_claim["binding"]["object"], "{page}");
    assert_eq!(
        binding["content"], live_claim["binding"]["content"],
        "{page}"
    );
    assert_eq!(
        binding["revision"].as_u64().unwrap(),
        live_claim["binding"]["revision"].as_u64().unwrap() + 1,
        "{page}"
    );
    assert_eq!(binding, &continuation["binding"], "{page}");
    assert_eq!(found[0]["Claim"]["status"], live_claim["status"]);
    assert_eq!(found[0]["Claim"]["issuer"], live_claim["issuer"]);
    let page = cli(
        root,
        None,
        &["get", "archived", &claim, "--artifact", &artifact],
    );
    let found = archived(&page, &continuation);
    assert_eq!(
        found[0]["Artifact"]["binding"], live_artifact["binding"],
        "{page}"
    );
    assert_eq!(
        found[0]["Artifact"]["content_hash"],
        live_artifact["content_hash"]
    );
    let page = cli(
        root,
        None,
        &["get", "archived", &claim, "--validation", &validation],
    );
    let found = archived(&page, &continuation);
    assert_eq!(
        found[0]["Definition"]["binding"], live_definition["binding"],
        "{page}"
    );
    let evaluation = found
        .iter()
        .find_map(|object| object.get("Evaluation"))
        .unwrap_or_else(|| panic!("no evaluation in {page}"));
    assert_eq!(evaluation["key"], live_evaluation["key"]);
    assert_eq!(evaluation["state"], "Validated");
    assert!(
        found.iter().any(|object| object.get("Result").is_some()),
        "no accepted result in {page}"
    );
    let page = cli(
        root,
        None,
        &["get", "archived", &claim, "--testament", &testament],
    );
    let found = archived(&page, &continuation);
    assert_eq!(
        found[0]["Response"]["binding"], live_testament["binding"],
        "{page}"
    );
    // The respondent reads the same through its own context.
    let page = cli(
        client.path(),
        Some("alice"),
        &["get", "archived", &claim, "--artifact", &artifact],
    );
    assert_eq!(
        archived(&page, &continuation)[0]["Artifact"]["content_hash"],
        live_artifact["content_hash"]
    );
    // An object the bundle never held is missing — not an error, and not
    // an archived object.
    let page = cli(
        root,
        None,
        &["get", "archived", &claim, "--artifact", &"f".repeat(32)],
    );
    assert!(objects(&page)[0].get("Missing").is_some(), "{page}");
    // Custody this node does not hold, or holds corrupt, is unavailable —
    // a typed refusal, never a missing object.
    let identity =
        admin(root, None, &["cluster", "node", "identity"])["result"]["identity"].clone();
    let tenant_hex = identity["tenant"].as_str().unwrap().to_owned();
    let bundle_hex = hex_hash(&continuation["bundle"]);
    let objects_dir = root.join("content").join("objects").join(&tenant_hex);
    let chunk = std::fs::read_dir(&objects_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension().is_some_and(|ext| ext == "chunk")
                && std::fs::read(path).is_ok_and(|bytes| {
                    format!("{}", blake3::hash(&bytes).to_hex()) == bundle_hex
                        || bytes.starts_with(b"FCNARCHV")
                })
        })
        .unwrap_or_else(|| panic!("no bundle chunk under {}", objects_dir.display()));
    let original = std::fs::read(&chunk).unwrap();
    let mut tampered = original.clone();
    tampered[0] ^= 0xff;
    std::fs::write(&chunk, &tampered).unwrap();
    let refused = run(
        root,
        None,
        &[
            "get",
            "archived",
            &claim,
            "--artifact",
            &artifact,
            "--format",
            "json",
        ],
    );
    assert!(!refused.status.success());
    // A refusal is printed to stderr, as every CLI error is.
    let refusal: Value = serde_json::from_slice(&refused.stderr).unwrap_or_else(|_| {
        panic!(
            "{}\n{}",
            String::from_utf8_lossy(&refused.stdout),
            String::from_utf8_lossy(&refused.stderr)
        )
    });
    assert_eq!(refusal["condition"], "Error", "{refusal}");
    assert_eq!(refusal["error"]["code"], "unavailable", "{refusal}");
    std::fs::write(&chunk, &original).unwrap();

    // ---- A kill and restart change none of it ----
    drop(server);
    let _server = start(root, &advertise);
    let page = cli(
        root,
        None,
        &["get", "archived", &claim, "--artifact", &artifact],
    );
    assert_eq!(
        archived(&page, &continuation)[0]["Artifact"]["content_hash"],
        live_artifact["content_hash"]
    );
    let page = cli(
        root,
        None,
        &["get", "archived", &claim, "--validation", &validation],
    );
    assert!(
        archived(&page, &continuation)
            .iter()
            .any(|object| object.get("Result").is_some()),
        "{page}"
    );
}
