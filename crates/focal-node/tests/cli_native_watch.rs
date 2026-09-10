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
//! Durable watches over the native engine through the real binary: the
//! client-driven seed of a claim filter and of a family list, the schema-2
//! deltas derived from committed native records on the continuous stream
//! line, an unseeded replay from the origin, table output, and resumption
//! after the node is killed and restarted.
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
/// Received: a code of the frozen claim status vocabulary.
const RECEIVED: u64 = 3;

fn handler(id: u128) -> Value {
    json!({"id": format!("{id:032x}"), "version": format!("{id:064x}")})
}
/// One `focal watch` invocation printing JSON deliveries, one per line.
fn watch(root: &Path, context: Option<&str>, args: &[&str]) -> Vec<Value> {
    let mut full = vec!["watch"];
    full.extend_from_slice(args);
    full.extend(["--format", "json"]);
    let output = run(root, context, &full);
    assert!(
        output.status.success(),
        "{full:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_else(|error| panic!("{error}: {line}")))
        .collect()
}
/// The native deltas of one Events delivery.
fn deltas(delivery: &Value) -> Vec<Value> {
    delivery["page"]["Events"]["page"]["events"]
        .as_array()
        .map(|events| {
            events
                .iter()
                .filter_map(|event| event.get("Delta"))
                .map(|delta| delta["delta"].clone())
                .collect()
        })
        .unwrap_or_default()
}
fn native_claim_kind(delta: &Value) -> Option<&str> {
    delta["fact"]["Native"]["fact"]["Claim"]["kind"].as_str()
}

#[test]
fn native_watches_seed_through_native_reads_and_stream_schema_two_deltas() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-watch-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-watch-client-")
        .tempdir_in("/tmp")
        .unwrap();
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
    let status = admin(root, None, &["status"]);
    let issuer = hex_hash(&objects(&status)[0]["Standing"]["principal"]);
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
    let alice_root = client.path();
    let alice_ctx = Some("alice");
    let alice_standing = admin(alice_root, alice_ctx, &["status"]);
    let alice = hex_hash(&objects(&alice_standing)[0]["Standing"]["principal"]);
    assert_ne!(alice, issuer);

    // Before any record, an unseeded watch of everything replays nothing and
    // the seeded claim watch of a claim that does not exist yet is refused
    // only by the reader, never by the stream: the seed page reports the
    // address as missing.
    let document = json!({
        "description": "Run the suite and deliver the report.",
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.",
             "target": {"type": "slot", "index": 0, "name": "report"}, "phase": "whole_work",
             "evaluator": "self", "handlers": [handler(71)], "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    let (_, result) = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let a = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &a]));
    committed(&cli(alice_root, alice_ctx, &["receipt", "acquire", &a]));
    let page = cli(root, None, &["get", "claim", &a]);
    assert_eq!(objects(&page)[0]["Claim"]["status"], RECEIVED, "{page}");

    // A seeded claim watch reads the claim with its responses and
    // evaluations at a prefix no older than the pinned snapshot; the single
    // claim completes the seed.
    let seed = watch(
        root,
        None,
        &["claims", "--claim", &a, "--name", "seeded", "--pages", "1"],
    );
    assert_eq!(seed.len(), 1, "{seed:?}");
    let page = &seed[0]["page"]["NativeSeed"];
    assert!(!page.is_null(), "{}", seed[0]);
    assert_eq!(page["next"], "Complete", "{page}");
    assert!(page["token"]["sequence"].as_u64().unwrap() >= 3, "{page}");
    let claims: Vec<String> = page["objects"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|object| object.get("Claim"))
        .map(|claim| hex_hash(&claim["binding"]["object"]))
        .collect();
    assert_eq!(claims, vec![a.clone()], "{page}");
    // The claim is read with its content and scopes at the received status;
    // its whole-work evaluation does not exist until work is submitted.
    let claim = &page["objects"][0]["Claim"];
    assert_eq!(claim["status"], RECEIVED, "{claim}");
    assert!(claim["content"].is_object(), "{claim}");
    assert!(claim["scopes"].is_object(), "{claim}");
    let inspected = cli(root, None, &["watch", "inspect", "seeded"]);
    assert_eq!(inspected["status"]["acknowledged"], 1, "{inspected}");
    assert_eq!(inspected["status"]["seeding"], true, "{inspected}");
    assert_eq!(inspected["status"]["options"]["engine"], "Native");

    // A record committed after the snapshot arrives on the tail as a
    // schema-2 delta carrying the exact native fact.
    committed(&cli(root, None, &["claim", "cancel", &a]));
    let tail = watch(root, None, &["resume", "seeded", "--pages", "3"]);
    let cancelled: Vec<Value> = tail
        .iter()
        .flat_map(deltas)
        .filter(|delta| native_claim_kind(delta) == Some("Cancelled"))
        .collect();
    assert_eq!(cancelled.len(), 1, "{tail:?}");
    assert_eq!(cancelled[0]["schema"], 2);
    assert_eq!(hex_hash(&cancelled[0]["claim"]), a);
    assert_eq!(hex_hash(&cancelled[0]["actor"]), issuer);
    assert_eq!(
        cancelled[0]["fact"]["Native"]["sequence"], cancelled[0]["id"]["sequence"],
        "a genesis ledger's stream line is its native sequence"
    );
    // The seed's own facts (creation, post, receipt) are not replayed after
    // the snapshot: they were read as objects.
    assert!(
        tail.iter()
            .flat_map(deltas)
            .all(|delta| native_claim_kind(&delta) != Some("Created")),
        "{tail:?}"
    );
    let inspected = cli(root, None, &["watch", "inspect", "seeded"]);
    assert_eq!(inspected["status"]["seeding"], false, "{inspected}");

    // An unseeded watch of everything replays the whole native history from
    // the origin: creation, post, receipt and cancellation in order, each a
    // schema-2 delta whose action is the nearest legacy lifecycle action.
    let everything = watch(
        root,
        None,
        &["all", "--name", "everything", "--no-seed", "--pages", "1"],
    );
    assert_eq!(everything.len(), 1);
    let history = deltas(&everything[0]);
    let kinds: Vec<&str> = history.iter().filter_map(native_claim_kind).collect();
    assert_eq!(
        kinds,
        ["Created", "Posted", "Received", "Cancelled"],
        "{history:?}"
    );
    assert!(
        history.iter().all(|delta| delta["schema"] == 2),
        "{history:?}"
    );
    let sequences: Vec<u64> = history
        .iter()
        .map(|delta| delta["id"]["sequence"].as_u64().unwrap())
        .collect();
    assert!(
        sequences.windows(2).all(|pair| pair[0] <= pair[1]),
        "{sequences:?}"
    );
    assert_eq!(sequences.first(), Some(&1));
    assert!(
        history
            .iter()
            .any(|delta| delta["fact"]["Native"]["fact"].get("Receipt").is_some()),
        "{history:?}"
    );
    assert!(
        history
            .iter()
            .any(|delta| delta["fact"]["Native"]["fact"].get("Definition").is_some()),
        "{history:?}"
    );

    // Family watches seed through the family's list: definitions exist,
    // artifacts do not, and both complete their seed in one page.
    let definitions = watch(
        root,
        None,
        &["validations", "--name", "defs", "--pages", "1"],
    );
    let page = &definitions[0]["page"]["NativeSeed"];
    assert_eq!(page["next"], "Complete", "{page}");
    assert!(
        page["objects"]
            .as_array()
            .unwrap()
            .iter()
            .all(|object| object.get("Definition").is_some()),
        "{page}"
    );
    assert_eq!(page["objects"].as_array().unwrap().len(), 2, "{page}");
    let artifacts = watch(root, None, &["artifacts", "--name", "arts", "--pages", "1"]);
    let page = &artifacts[0]["page"]["NativeSeed"];
    assert_eq!(page["next"], "Complete", "{page}");
    assert_eq!(page["objects"], json!([]), "{page}");

    // Alice's own context watches the same claim from her side.
    let hers = watch(
        alice_root,
        alice_ctx,
        &[
            "claims",
            "--claim",
            &a,
            "--name",
            "mine",
            "--no-seed",
            "--pages",
            "1",
        ],
    );
    let her_history = deltas(&hers[0]);
    let kinds: Vec<&str> = her_history.iter().filter_map(native_claim_kind).collect();
    assert_eq!(
        kinds,
        ["Created", "Posted", "Received", "Cancelled"],
        "{hers:?}"
    );

    // Table output labels native facts compactly.
    let table = run(
        root,
        None,
        &["watch", "resume", "everything", "--pages", "1"],
    );
    assert!(
        table.status.success(),
        "{}",
        String::from_utf8_lossy(&table.stderr)
    );
    let text = String::from_utf8(table.stdout).unwrap();
    assert!(text.starts_with("DELIVERY\t"), "{text}");

    // The node is killed and restarted: every watch resumes from its saved
    // cursor over the retained events, and the retained deltas are unchanged.
    drop(server);
    let _server = start(root, &advertise);
    let resumed = watch(root, None, &["resume", "seeded", "--pages", "1"]);
    assert_eq!(resumed.len(), 1);
    assert!(deltas(&resumed[0]).is_empty(), "{resumed:?}");
    let again = watch(
        root,
        None,
        &[
            "all",
            "--name",
            "everything-again",
            "--no-seed",
            "--pages",
            "1",
        ],
    );
    assert_eq!(deltas(&again[0]), history);
    let inspected = cli(root, None, &["watch", "inspect"]);
    let mut names: Vec<&str> = inspected["names"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["arts", "defs", "everything", "everything-again", "seeded"]
    );
}
