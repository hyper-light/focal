#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Code mode from the real binary against a real node (19 §Code mode): one
//! program submits, posts and reads a native claim; the node is killed and
//! restarted; the same run replays to the same claim and the ledger holds
//! one; the same run with other input is refused.
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
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    // Unix keeps the path short for the Unix-socket path limit (/tmp); Windows
    // names its pipe by a hash of the data directory, so the default temp root
    // is fine there.
    #[cfg(unix)]
    {
        builder.tempdir_in("/tmp").unwrap()
    }
    #[cfg(not(unix))]
    {
        builder.tempdir().unwrap()
    }
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
            "node",
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

/// `focal run code|search`, its parsed result and whether it exited 0.
fn code(root: &Path, args: &[&str]) -> (bool, Value) {
    let output = run(root, None, args);
    let value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{args:?}: {error}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.success(), value)
}

const PROGRAM: &str = r#"
const hex = bytes => bytes.map(b => b.toString(16).padStart(2, "0")).join("");
const submitted = await focal.claim.submit(input.claim);
const claim = hex(submitted.result.created.find(c => c.kind === "Claim").id);
await focal.claim.post({ claim });
const page = await focal.claim.get({ id: claim });
return { claim, status: page.result.page.objects[0].Claim.status };
"#;

const COUNT: &str = r#"
const standing = await focal.ledger.standing({});
const me = standing.result.page.objects[0].Standing.principal
  .map(b => b.toString(16).padStart(2, "0")).join("");
const page = await focal.claim.list({ issuer: me, limit: 256 });
return page.result.page.objects.length;
"#;

#[test]
fn a_program_run_twice_across_a_killed_node_makes_one_claim() {
    let founder = scratch("focal-code-mode-");
    let client = scratch("focal-code-mode-client-");
    private(founder.path());
    private(client.path());
    let root = founder.path();
    let activation = admin(root, None, &["activate", "native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = address();
    let server = start(root, &advertise);
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
    assert!(
        enrolled.status.success(),
        "{}",
        String::from_utf8_lossy(&enrolled.stderr)
    );
    let whoami = client.path().join("whoami.js");
    std::fs::write(
        &whoami,
        "const s = await focal.ledger.standing({}); return s.result.page.objects[0].Standing.principal.map(b => b.toString(16).padStart(2, \"0\")).join(\"\");",
    )
    .unwrap();
    let output = run(
        client.path(),
        Some("alice"),
        &[
            "run",
            "code",
            "--run",
            "whoami",
            "--file",
            whoami.to_str().unwrap(),
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let whoami: Value = serde_json::from_slice(&output.stdout).unwrap();
    let alice_id = whoami["result"]["outcome"]["value"]
        .as_str()
        .map(str::to_owned);

    // What the program may call, found by a search program.
    let files = founder.path().join("programs");
    std::fs::create_dir(&files).unwrap();
    let search = files.join("search.js");
    std::fs::write(
        &search,
        "return registry.filter(t => t.name.startsWith(\"claim.\")).map(t => t.name).sort();",
    )
    .unwrap();
    let (ok, found) = code(
        root,
        &["search", "tools", "--file", search.to_str().unwrap()],
    );
    assert!(ok, "{found}");
    let names = &found["result"]["outcome"]["value"];
    for name in ["claim.get", "claim.list", "claim.post", "claim.submit"] {
        assert!(
            names.as_array().unwrap().iter().any(|n| n == name),
            "{name}: {found}"
        );
    }

    let program = files.join("cycle.js");
    std::fs::write(&program, PROGRAM).unwrap();
    let target = alice_id.expect("alice's principal");
    let claim = json!({
        "description": "Run the suite and deliver the report.",
        "target": target,
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}]
    });
    let input = files.join("input.json");
    std::fs::write(&input, json!({"claim": claim}).to_string()).unwrap();
    let cycle = [
        "run",
        "code",
        "--run",
        "cycle-1",
        "--file",
        program.to_str().unwrap(),
        "--input",
        input.to_str().unwrap(),
    ];
    let (ok, first) = code(root, &cycle);
    assert!(ok, "{first}");
    let value = first["result"]["outcome"]["value"].clone();
    assert_eq!(value["status"], 2, "posted: {first}");
    let calls = first["result"]["calls"].as_array().unwrap();
    assert_eq!(calls.len(), 3, "{first}");
    assert!(
        calls[0]["operation_id"]
            .as_str()
            .unwrap()
            .starts_with("n1:"),
        "{first}"
    );

    // The node dies without warning and comes back; the same run replays to
    // the same claim, every mutation resumed from the journal.
    drop(server);
    let server = start(root, &advertise);
    let (ok, again) = code(root, &cycle);
    assert!(ok, "{again}");
    assert_eq!(again["result"]["outcome"]["value"], value, "{again}");
    assert_eq!(
        again["result"]["calls"][0]["operation_id"],
        calls[0]["operation_id"]
    );

    // The same run with other input reaches the same reference: refused.
    let mut other = claim.clone();
    other["description"] = json!("A different occurrence.");
    std::fs::write(&input, json!({"claim": other}).to_string()).unwrap();
    let output = run(root, None, &cycle);
    assert_eq!(
        output.status.code(),
        Some(10),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let changed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(changed["result"]["outcome"]["end"], "failed", "{changed}");

    let count = files.join("count.js");
    std::fs::write(&count, COUNT).unwrap();
    let (ok, counted) = code(
        root,
        &[
            "run",
            "code",
            "--run",
            "count-1",
            "--file",
            count.to_str().unwrap(),
        ],
    );
    assert!(ok, "{counted}");
    assert_eq!(
        counted["result"]["outcome"]["value"], 1,
        "one claim: {counted}"
    );
    drop(server);
}
