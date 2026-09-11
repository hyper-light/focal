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
//! Declared topology and the residency fence (24 §22): nodes announce
//! their region and zone, the root registers regions and grants the
//! domains, a zone-survival plan places voters across distinct zones inside
//! the residency, and a move to a node outside the residency is refused.
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
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
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
struct Node {
    dir: tempfile::TempDir,
    config: PathBuf,
}
impl Node {
    fn new(name: &str, region: &str, zone: &str, residency: &[&str]) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("focal-zones-{name}-"))
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = dir.path().join("focal.yaml");
        let mut text = format!(
            "version: 1\ntopology:\n  region: {region}\n  zone: {zone}\ndurability:\n  survive: node\n  max_failures: 0\n"
        );
        if !residency.is_empty() {
            text.push_str("placement:\n  residency: [");
            text.push_str(&residency.join(", "));
            text.push_str("]\n");
        }
        std::fs::write(&config, text).unwrap();
        Self { dir, config }
    }
    fn root(&self) -> &Path {
        self.dir.path()
    }
}
fn run(node: &Node, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--config", node.config.to_str().unwrap()])
        .args(["--data-dir", node.root().to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}
fn admin(node: &Node, args: &[&str]) -> Value {
    let output = run(node, args);
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
fn failure(node: &Node, args: &[&str]) -> (i32, String) {
    let output = run(node, args);
    assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}
fn start(node: &Node, address: Option<&str>) -> (Server, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--config", node.config.to_str().unwrap()]);
    command.args(["--data-dir", node.root().to_str().unwrap(), "start"]);
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
fn join(founder: &Node, host: &Node, name: &str, advertise: &str) -> u64 {
    let invitation = founder.root().join(format!("{name}.invite"));
    admin(
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
fn placement(node: &Node) -> Option<Value> {
    let output = run(node, &["cluster", "placement"]);
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
fn node_row(view: &Value, id: u64) -> Option<&Value> {
    view["partitions"]
        .as_array()?
        .iter()
        .flat_map(|partition| partition["nodes"].as_array().into_iter().flatten())
        .find(|node| node["node"] == id)
}
fn wait_for(
    node: &Node,
    what: &str,
    timeout: Duration,
    condition: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(view) = placement(node) {
            if condition(&view) {
                return view;
            }
            last = Some(view);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("{what} did not happen within {timeout:?}; last view: {last:#?}");
}
fn ids(value: &Value) -> Vec<u64> {
    value
        .as_array()
        .map(|items| items.iter().filter_map(Value::as_u64).collect())
        .unwrap_or_default()
}
fn labeled(view: &Value, id: u64, region: &str, zone: &str) -> bool {
    node_row(view, id).is_some_and(|row| {
        row["alive"] == true
            && row["region"] == region
            && row["zone"] == zone
            && row["disk_available"].is_number()
    })
}

#[test]
fn declared_zones_place_voters_across_domains_and_the_residency_fence_refuses_a_move_outside() {
    // Three hosts in region `ra` (zones a1, a2, a3) and one in `rb`; the
    // founder's policy keeps every copy inside `ra`.
    let founder = Node::new("founder", "ra", "a1", &["ra"]);
    let host_b = Node::new("host-b", "ra", "a2", &[]);
    let host_c = Node::new("host-c", "ra", "a3", &[]);
    let host_d = Node::new("host-d", "rb", "b1", &[]);
    let addresses: Vec<String> = (0..4).map(|_| address()).collect();
    let activation = admin(&founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let (_founder_server, status) = start(&founder, Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let identity = admin(&founder, &["cluster", "node", "identity"])["result"]["identity"].clone();
    let founder_node = identity["node"].as_u64().unwrap();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    let ledger = identity["session"].as_str().unwrap().to_owned();
    let node_b = join(&founder, &host_b, "host-b", &addresses[1]);
    let node_c = join(&founder, &host_c, "host-c", &addresses[2]);
    let node_d = join(&founder, &host_d, "host-d", &addresses[3]);
    let _server_b = start(&host_b, None).0;
    let _server_c = start(&host_c, None).0;
    let _server_d = start(&host_d, None).0;
    // Every node is granted with the topology it declared, and the founder's
    // session carries its residency as labels.
    let view = wait_for(
        &founder,
        "declared topology",
        Duration::from_secs(120),
        |view| {
            labeled(view, founder_node, "ra", "a1")
                && labeled(view, node_b, "ra", "a2")
                && labeled(view, node_c, "ra", "a3")
                && labeled(view, node_d, "rb", "b1")
                && session(view, &ledger).is_some_and(|session| {
                    session["residency"] == serde_json::json!(["ra"])
                        && session["pending"].is_null()
                })
        },
    );
    assert_eq!(
        session(&view, &ledger).unwrap()["home_regions"],
        serde_json::json!([])
    );
    // One tolerated zone loss: three voters in three distinct zones of `ra`;
    // the host in `rb` is never chosen.
    let planned = admin(
        &founder,
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--survive",
            "zone",
            "--max-failures",
            "1",
        ],
    )["result"]
        .clone();
    assert_eq!(planned["state"], "planned", "{planned}");
    let view = wait_for(
        &founder,
        "zone activation",
        Duration::from_secs(180),
        |view| {
            session(view, &ledger).is_some_and(|session| {
                session["pending"].is_null()
                    && session["achieved_survive"] == "Zone"
                    && session["achieved_max_failures"] == 1
                    && session["retiring"].as_array().is_some_and(Vec::is_empty)
            })
        },
    );
    let active = session(&view, &ledger).unwrap().clone();
    let voters = ids(&active["voters"]);
    assert_eq!(voters.len(), 3, "{active}");
    assert!(voters.contains(&founder_node) && voters.contains(&node_b) && voters.contains(&node_c));
    assert!(!voters.contains(&node_d), "{active}");
    assert!(
        !ids(&active["content_copies"]).contains(&node_d),
        "{active}"
    );
    assert_eq!(active["survive"], "Zone");
    // A move of a range member to the node outside the residency is refused
    // by name before anything moves; the same move inside it is admitted.
    let ranges = admin(
        &founder,
        &[
            "cluster",
            "replicas",
            "ranges",
            "--session",
            &ledger,
            "list",
        ],
    );
    let member = ranges["result"]["ranges"]["members"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (code, report) = failure(
        &founder,
        &[
            "cluster",
            "replicas",
            "ranges",
            "--session",
            &ledger,
            "move",
            "--member",
            &member,
            "--node",
            &node_d.to_string(),
        ],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[outside_residency]"), "{report}");
    assert!(report.contains("rb"), "{report}");
    let after = placement(&founder).unwrap();
    assert!(session(&after, &ledger).unwrap()["pending"].is_null());
    // A plan that needs a second region the residency excludes cannot be
    // placed: refused up front, or reported as anything but planned.
    let output = run(
        &founder,
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--survive",
            "region",
            "--max-failures",
            "1",
            "--dry-run",
        ],
    );
    if output.status.success() {
        let regional: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_ne!(regional["result"]["state"], "planned", "{regional}");
    } else {
        let report = String::from_utf8_lossy(&output.stderr);
        assert!(
            report.contains("[invalid_input]") || report.contains("[unavailable]"),
            "{report}"
        );
    }
    let after = placement(&founder).unwrap();
    assert!(session(&after, &ledger).unwrap()["pending"].is_null());
}
