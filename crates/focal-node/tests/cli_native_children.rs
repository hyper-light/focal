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
//! The narrow child-cause authority (P17.12) through the real binary: a
//! participant may cite a committed claim as the cause of a new claim only
//! as that claim's issuer or its current receipt holder, only while the
//! parent is live, and only at the parent's exact committed binding; the
//! owner registers the child on the parent, cancels it with the parent, and
//! refuses forged, foreign and late parentage with typed outcomes.
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

/// A refused command: the structured error printed on stdout for a service
/// refusal or on stderr for an input refusal, with the exit code.
fn refused(root: &Path, context: Option<&str>, args: &[&str]) -> (i32, Value) {
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
    assert_eq!(value["condition"], "Error", "{value}");
    (output.status.code().unwrap(), value)
}

const FAR: u64 = 4_102_444_800_000;
/// Received and Cancelled: codes of the frozen claim status vocabulary.
const RECEIVED: u64 = 3;
const CANCELLED: u64 = 15;

fn enroll(root: &Path, client: &Path, name: &str) -> String {
    let invitation = client.join(format!("{name}.invite"));
    admin(
        root,
        None,
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
    let standing = admin(client, Some(name), &["status"]);
    hex_hash(&objects(&standing)[0]["Standing"]["principal"])
}
fn document(target: &str, description: &str, parent: Option<&str>) -> String {
    let mut document = json!({
        "description": description,
        "target": target,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.",
             "target": {"type": "slot", "index": 0, "name": "report"}, "phase": "whole_work",
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 71), "version": format!("{:064x}", 71)}],
             "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    if let Some(parent) = parent {
        document["parent"] = Value::String(parent.into());
    }
    document.to_string()
}
/// The refusal code: owner refusals print the version-2 result shape, client
/// refusals before sending print the version-1 error shape.
fn code_of(value: &Value) -> &str {
    value["result"]["code"]
        .as_str()
        .or_else(|| value["error"]["code"].as_str())
        .unwrap_or_else(|| panic!("no refusal code: {value}"))
}
fn claim_object(root: &Path, context: Option<&str>, claim: &str) -> Value {
    let page = cli(root, context, &["get", "claim", claim]);
    objects(&page)[0]["Claim"].clone()
}
fn children(claim: &Value) -> Vec<String> {
    claim["scopes"]["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|child| hex_hash(&child["binding"]["object"]))
        .collect()
}

#[test]
fn child_causes_are_bound_to_the_parent_its_issuer_or_receipt_holder_and_its_live_binding() {
    let founder = tempfile::Builder::new()
        .prefix("focal-native-children-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-native-children-client-")
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
    let alice = enroll(root, client.path(), "alice");
    let bob = enroll(root, client.path(), "bob");
    assert_ne!(alice, issuer);
    assert_ne!(bob, alice);
    let alice_ctx = Some("alice");
    let bob_ctx = Some("bob");

    // The parent: issued to alice, posted, and received by her.
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &document(&alice, "Run the suite and deliver the report.", None),
        ],
    ));
    let parent = created(&result, "Claim").remove(0);
    committed(&cli(root, None, &["claim", "post", &parent]));
    committed(&cli(
        client.path(),
        alice_ctx,
        &["receipt", "acquire", &parent],
    ));
    let before = claim_object(root, None, &parent);
    assert_eq!(before["status"], RECEIVED, "{before}");
    assert_eq!(children(&before), Vec::<String>::new());

    // The issuer registers a child under it; the child's cause is the parent
    // and the parent's registry names the child at its committed binding.
    let (_, result) = committed(&cli(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &document(
                &alice,
                "Follow-up: collect the coverage report.",
                Some(&parent),
            ),
        ],
    ));
    let first = created(&result, "Claim").remove(0);
    let child = claim_object(root, None, &first);
    assert_eq!(hex_hash(&child["cause"]["Claim"]), parent, "{child}");
    assert_eq!(hex_hash(&child["binding"]["object"]), first);
    let after_first = claim_object(root, None, &parent);
    assert_eq!(children(&after_first), vec![first.clone()], "{after_first}");
    assert!(
        after_first["binding"]["revision"].as_u64().unwrap()
            > before["binding"]["revision"].as_u64().unwrap(),
        "registering a child is a parent revision"
    );

    // The current receipt holder may cite the parent too; the request pins
    // the parent's receipt so a later adoption would refuse it as stale.
    let (_, result) = committed(&cli(
        client.path(),
        alice_ctx,
        &[
            "submit",
            "claim",
            "--json",
            &document(
                &issuer,
                "Consult: which fixtures are authoritative?",
                Some(&parent),
            ),
        ],
    ));
    let second = created(&result, "Claim").remove(0);
    let after_second = claim_object(root, None, &parent);
    let mut registered = children(&after_second);
    registered.sort();
    let mut expected = vec![first.clone(), second.clone()];
    expected.sort();
    assert_eq!(registered, expected, "{after_second}");
    let second_claim = claim_object(client.path(), alice_ctx, &second);
    assert_eq!(hex_hash(&second_claim["cause"]["Claim"]), parent);
    assert_eq!(hex_hash(&second_claim["issuer"]), alice);

    // A third participant holds neither role: refused as unauthorized by the
    // owner, and the parent's registry is unchanged.
    let (code, refusal) = refused(
        client.path(),
        bob_ctx,
        &[
            "submit",
            "claim",
            "--json",
            &document(&issuer, "Bob cites a claim he does not own.", Some(&parent)),
        ],
    );
    assert_eq!(code, 3, "{refusal}");
    assert_eq!(code_of(&refusal), "unauthorized", "{refusal}");
    assert_eq!(children(&claim_object(root, None, &parent)), registered);

    // A parent that does not exist is refused before any frame is sent.
    let forged = format!("{:032x}", 0x5eedu128);
    let (code, refusal) = refused(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &document(&alice, "Forged parentage.", Some(&forged)),
        ],
    );
    assert_eq!(code, 4, "{refusal}");
    assert_eq!(code_of(&refusal), "not_found", "{refusal}");

    // Cancelling the parent cancels its pending children with it; a terminal
    // parent then accepts no new child.
    committed(&cli(root, None, &["claim", "cancel", &parent]));
    for id in [&first, &second] {
        assert_eq!(claim_object(root, None, id)["status"], CANCELLED, "{id}");
    }
    let (code, refusal) = refused(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &document(&alice, "Late child of a cancelled parent.", Some(&parent)),
        ],
    );
    assert_eq!(code, 5, "{refusal}");
    assert_eq!(code_of(&refusal), "invalid_transition", "{refusal}");

    // The lineage survives a kill and restart unchanged.
    drop(server);
    let _server = start(root, &advertise);
    let child = claim_object(root, None, &first);
    assert_eq!(hex_hash(&child["cause"]["Claim"]), parent);
    assert_eq!(child["status"], CANCELLED);
    let mut registered = children(&claim_object(root, None, &parent));
    registered.sort();
    assert_eq!(registered, expected);
}
