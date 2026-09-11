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
//! Custody repair (24 §20): a session with one artifact is expanded onto
//! three hosts; a copy that loses the object's chunk, or holds it corrupt,
//! recopies it from another required copy under the same identity; a
//! holder whose peers lost it gives it back to them; an object no copy can
//! supply is reported as unrecoverable, and the walk is idempotent.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

const FAR: u64 = 4_102_444_800_000;
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn private_dir(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("focal-repair-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
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
fn start(root: &Path, address: Option<&str>) -> (Server, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap(), "start"]);
    if let Some(address) = address {
        command.args(["--advertise", address]);
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
        .recv_timeout(Duration::from_secs(30))
        .expect("the node did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    (server, status)
}
fn join(founder: &Path, host: &Path, name: &str, advertise: &str) -> u64 {
    let invitation = founder.join(format!("{name}.invite"));
    let written = admin(
        founder,
        &[
            "cluster",
            "invite",
            "--node",
            name,
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    assert_eq!(written["condition"], "InvitationWritten");
    let joined = admin(
        host,
        &[
            "join",
            "--invite-file",
            invitation.to_str().unwrap(),
            "--advertise",
            advertise,
        ],
    );
    joined["node"].as_u64().unwrap()
}
fn placement(root: &Path) -> Option<Value> {
    let output = run(root, None, &["cluster", "placement"]);
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .map(|value| value["result"]["placement"].clone())
}
fn session<'a>(view: &'a Value, ledger: &str) -> Option<&'a Value> {
    view["partitions"]
        .as_array()?
        .iter()
        .flat_map(|partition| partition["sessions"].as_array().into_iter().flatten())
        .find(|session| session["session"] == ledger)
}
fn node(view: &Value, id: u64) -> Option<&Value> {
    view["partitions"]
        .as_array()?
        .iter()
        .flat_map(|partition| partition["nodes"].as_array().into_iter().flatten())
        .find(|node| node["node"] == id)
}
fn wait_for(
    root: &Path,
    what: &str,
    timeout: Duration,
    condition: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(view) = placement(root) {
            if condition(&view) {
                return view;
            }
            last = Some(view);
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    panic!("{what} did not happen within {timeout:?}; last view: {last:#?}");
}
fn settled(view: &Value, ledger: &str, nodes: &[u64], max_failures: u64) -> bool {
    session(view, ledger).is_some_and(|session| {
        session["pending"].is_null()
            && session["achieved_max_failures"] == max_failures
            && session["retiring"].as_array().is_some_and(Vec::is_empty)
    }) && nodes.iter().all(|id| {
        node(view, *id)
            .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
    })
}
fn committed(value: &Value) -> Value {
    assert_eq!(value["condition"], "Committed", "{value}");
    assert_eq!(value["result"]["kind"], "native", "{value}");
    value["result"].clone()
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
fn hex_hash(value: &Value) -> String {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
fn objects(page: &Value) -> &Vec<Value> {
    assert_eq!(page["result"]["kind"], "native_read", "{page}");
    page["result"]["page"]["objects"].as_array().unwrap()
}
fn repair(root: &Path, tenant: &str, ledger: &str) -> Value {
    let report = admin(
        root,
        &["cluster", "repair", "--tenant", tenant, "--session", ledger],
    );
    assert_eq!(report["result"]["kind"], "repaired", "{report}");
    report["result"]["repair"].clone()
}
fn chunk_files(root: &Path, tenant: &str) -> Vec<PathBuf> {
    let directory = root.join("content").join("objects").join(tenant);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&directory)
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "chunk"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

#[test]
fn repair_recopies_lost_or_corrupt_objects_completes_peers_and_reports_the_unrecoverable() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b", "client"]
        .iter()
        .map(|name| private_dir(name))
        .collect();
    let founder = dirs[0].path();
    let host_a = dirs[1].path();
    let host_b = dirs[2].path();
    let client = dirs[3].path();
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    let activation = admin(founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let (_founder_server, status) = start(founder, Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let identity = admin(founder, &["cluster", "node", "identity"])["result"]["identity"].clone();
    let founder_node = identity["node"].as_u64().unwrap();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    let ledger = identity["session"].as_str().unwrap().to_owned();
    // A participant delivers one artifact into the founder's session.
    let invitation = client.join("alice.invite");
    admin(
        founder,
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
            client,
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
    let standing = admin(client, &["--client-context", "alice", "status"]);
    let alice = hex_hash(&objects(&standing)[0]["Standing"]["principal"]);
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
    let result = committed(&cli(
        founder,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let claim = created(&result, "Claim").remove(0);
    committed(&cli(founder, None, &["claim", "post", &claim]));
    committed(&cli(client, Some("alice"), &["receipt", "acquire", &claim]));
    committed(&cli(
        client,
        Some("alice"),
        &[
            "artifact", "submit", "--claim", &claim, "--slot", "0", "--text", PROOF,
        ],
    ));
    let founder_chunks = chunk_files(founder, &tenant);
    assert_eq!(founder_chunks.len(), 1, "{founder_chunks:?}");
    let original = std::fs::read(&founder_chunks[0]).unwrap();
    // Alone, the founder verifies its one object; nothing to push.
    let report = wait_for_report(founder, &tenant, &ledger, |report| report["objects"] == 1);
    assert_eq!(report["verified"], 1, "{report}");
    assert_eq!(report["repaired"], 0);
    assert_eq!(report["pushed"], 0);
    assert_eq!(report["unrecoverable_count"], 0);
    assert_eq!(report["restore_required"], false);
    assert_eq!(report["complete"], true);
    assert_eq!(report["next_after"], Value::Null);
    assert_eq!(report["node"], founder_node);
    assert!(report["index"].as_u64().unwrap() > 0, "{report}");
    // Two hosts join; the session is planned for one tolerated node loss,
    // so every host becomes a required copy and receives the object.
    let node_a = join(founder, host_a, "host-a", &addresses[1]);
    let node_b = join(founder, host_b, "host-b", &addresses[2]);
    let _server_a = start(host_a, None).0;
    let _server_b = start(host_b, None).0;
    let all = [founder_node, node_a, node_b];
    wait_for(founder, "three hosts", Duration::from_secs(90), |view| {
        settled(view, &ledger, &all, 0)
    });
    let planned = admin(
        founder,
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--max-failures",
            "1",
        ],
    )["result"]
        .clone();
    assert_eq!(planned["state"], "planned", "{planned}");
    let view = wait_for(founder, "activation", Duration::from_secs(180), |view| {
        settled(view, &ledger, &all, 1)
    });
    // One tolerated loss: three voters, two content copies (the founder
    // and one host); the other host votes and replays the record, so it
    // holds the object too, but owes no custody for it.
    let copies: Vec<u64> = session(&view, &ledger).unwrap()["content_copies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_u64().unwrap())
        .collect();
    assert_eq!(copies.len(), 2, "{view}");
    assert!(copies.contains(&founder_node));
    let other_copies = copies.iter().filter(|id| **id != founder_node).count();
    // The founder completes the copies that lack the object, then every
    // host verifies it and the walk is idempotent.
    let report = repair(founder, &tenant, &ledger);
    assert_eq!(report["verified"], 1, "{report}");
    assert_eq!(report["unrecoverable_count"], 0, "{report}");
    for host in [host_a, host_b] {
        let report = wait_for_report(host, &tenant, &ledger, |report| {
            report["objects"] == 1 && report["unrecoverable_count"] == 0
        });
        assert_eq!(
            report["verified"].as_u64().unwrap() + report["repaired"].as_u64().unwrap(),
            1,
            "{report}"
        );
        let again = repair(host, &tenant, &ledger);
        assert_eq!(again["verified"], 1, "{again}");
        assert_eq!(again["repaired"], 0, "{again}");
        assert_eq!(chunk_files(host, &tenant).len(), 1);
    }
    let report = repair(founder, &tenant, &ledger);
    assert_eq!(report["verified"], 1, "{report}");
    assert_eq!(report["pushed"], 0, "{report}");
    // A copy that lost the chunk recopies it from another required copy.
    let chunk_b = chunk_files(host_b, &tenant).remove(0);
    std::fs::remove_file(&chunk_b).unwrap();
    let report = repair(host_b, &tenant, &ledger);
    assert_eq!(report["repaired"], 1, "{report}");
    assert_eq!(report["verified"], 0);
    assert_eq!(report["unrecoverable_count"], 0);
    assert_eq!(std::fs::read(&chunk_b).unwrap(), original);
    // A copy that holds the chunk corrupt receives verified bytes over it.
    let chunk_a = chunk_files(host_a, &tenant).remove(0);
    let mut tampered = original.clone();
    tampered[0] ^= 0xff;
    std::fs::write(&chunk_a, &tampered).unwrap();
    let report = repair(host_a, &tenant, &ledger);
    assert_eq!(report["repaired"], 1, "{report}");
    assert_eq!(std::fs::read(&chunk_a).unwrap(), original);
    // No copy holds the object: it is reported unrecoverable, restore
    // required; nothing is manufactured.
    for root in [founder, host_a, host_b] {
        std::fs::remove_file(chunk_files(root, &tenant).remove(0)).unwrap();
    }
    let report = repair(founder, &tenant, &ledger);
    assert_eq!(report["unrecoverable_count"], 1, "{report}");
    assert_eq!(report["restore_required"], true);
    assert_eq!(report["verified"], 0);
    assert_eq!(report["repaired"], 0);
    assert_eq!(report["complete"], true);
    let listed = report["unrecoverable"].as_array().unwrap();
    assert_eq!(listed.len(), 1, "{report}");
    assert_eq!(listed[0]["length"], PROOF.len() as u64);
    // Both other nodes were asked: the other content copy, then the voter.
    assert_eq!(listed[0]["asked"], 2);
    assert!(chunk_files(founder, &tenant).is_empty());
    // One copy recovers its bytes (an operator's restore of the file): the
    // holder gives the object back to every required copy that lost it;
    // the voter that owes no custody recopies it through its own repair.
    std::fs::write(&founder_chunks[0], &original).unwrap();
    let report = repair(founder, &tenant, &ledger);
    assert_eq!(report["verified"], 1, "{report}");
    assert_eq!(report["pushed"], other_copies as u64, "{report}");
    assert_eq!(report["unrecoverable_count"], 0);
    for (host, id) in [(host_a, node_a), (host_b, node_b)] {
        if copies.contains(&id) {
            let files = chunk_files(host, &tenant);
            assert_eq!(files.len(), 1, "{files:?}");
            assert_eq!(std::fs::read(&files[0]).unwrap(), original);
            let report = repair(host, &tenant, &ledger);
            assert_eq!(report["verified"], 1, "{report}");
        } else {
            assert!(chunk_files(host, &tenant).is_empty());
            let report = repair(host, &tenant, &ledger);
            assert_eq!(report["repaired"], 1, "{report}");
            let files = chunk_files(host, &tenant);
            assert_eq!(files.len(), 1, "{files:?}");
            assert_eq!(std::fs::read(&files[0]).unwrap(), original);
        }
    }
    // A bounded walk resumes where it stopped.
    let bounded = admin(
        founder,
        &[
            "cluster",
            "repair",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--limit",
            "1",
        ],
    )["result"]["repair"]
        .clone();
    assert_eq!(bounded["objects"], 1, "{bounded}");
    assert_eq!(bounded["complete"], false, "{bounded}");
    let after = bounded["next_after"].as_str().unwrap().to_owned();
    let resumed = admin(
        founder,
        &[
            "cluster",
            "repair",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--after",
            &after,
        ],
    )["result"]["repair"]
        .clone();
    assert_eq!(resumed["complete"], true, "{resumed}");
    assert_eq!(resumed["objects"], 0, "{resumed}");
    // A session this node does not host is refused.
    let output = run(
        founder,
        None,
        &[
            "cluster",
            "repair",
            "--tenant",
            &tenant,
            "--session",
            &format!("{:032x}", 0xabcd),
        ],
    );
    assert!(!output.status.success());
}
/// The session registers with the directory shortly after it starts; a
/// repair before that finds no placement and is refused, so the first
/// report is waited for.
fn wait_for_report(
    root: &Path,
    tenant: &str,
    ledger: &str,
    condition: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = None;
    while Instant::now() < deadline {
        let output = run(
            root,
            None,
            &["cluster", "repair", "--tenant", tenant, "--session", ledger],
        );
        if output.status.success()
            && let Ok(value) = serde_json::from_slice::<Value>(&output.stdout)
        {
            let report = value["result"]["repair"].clone();
            if condition(&report) {
                return report;
            }
            last = Some(report);
        } else {
            last = Some(Value::String(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("no repair report satisfied the condition; last: {last:#?}");
}
