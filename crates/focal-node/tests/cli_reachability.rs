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
//! Reachability outlives addresses (24 §24): a host started with
//! `--invite-file` enrolls and starts in one command, a node that advertises
//! a name announces it, the founder's placement view shows every node's
//! contact, and a node restarted at another address keeps its identity and
//! is found again.
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
struct Node {
    dir: tempfile::TempDir,
}
impl Node {
    fn new(name: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("focal-reach-{name}-"))
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { dir }
    }
    fn root(&self) -> &Path {
        self.dir.path()
    }
}
fn run(node: &Node, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
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
    serde_json::from_slice(&output.stdout).unwrap()
}
/// Start and wait for the readiness record; a `--invite-file` start prints
/// the joined identity first.
fn start(node: &Node, args: &[&str]) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", node.root().to_str().unwrap(), "start"])
        .args(args)
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
                text.clear();
                if value.get("condition").is_some() && send.send(value).is_err() {
                    break;
                }
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
    eprintln!(
        "started {} listen {} advertise {} args {args:?}",
        status["node"], status["listen"], status["advertise"]
    );
    server
}
fn node_row(founder: &Node, id: u64) -> Option<Value> {
    let output = run(founder, &["cluster", "placement"]);
    if !output.status.success() {
        return None;
    }
    let view: Value = serde_json::from_slice(&output.stdout).ok()?;
    view["result"]["placement"]["partitions"]
        .as_array()?
        .iter()
        .flat_map(|partition| partition["nodes"].as_array().cloned().unwrap_or_default())
        .find(|row| row["node"] == id)
}
/// The announced address is whichever loopback family `localhost` resolved
/// to, at the expected port; the name is exactly what was given.
fn wait_for_contact(founder: &Node, id: u64, port: &str, endpoint: Option<&str>) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(row) = node_row(founder, id) {
            let expected = endpoint.map_or(Value::Null, |name| Value::String(name.into()));
            let announced = row["advertise"].as_str().unwrap_or_default();
            let at_port =
                announced == format!("127.0.0.1:{port}") || announced == format!("[::1]:{port}");
            if at_port && row["endpoint"] == expected && row["alive"] == true {
                return row;
            }
            last = Some(row);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let view = run(founder, &["cluster", "placement"]);
    panic!(
        "{id} never announced port {port} / {endpoint:?}; last {last:#?}; view {}",
        String::from_utf8_lossy(&view.stdout)
    );
}

#[test]
fn a_host_enrolls_at_its_first_start_announces_its_name_and_is_found_after_moving() {
    let founder = Node::new("founder");
    let host = Node::new("host");
    let founder_address = ports::address();
    let founder_port = founder_address.rsplit(':').next().unwrap().to_owned();
    // The founder advertises a name: invitations carry it.
    let _founder_server = start(
        &founder,
        &["--advertise", &format!("localhost:{founder_port}")],
    );
    let founder_id =
        admin(&founder, &["cluster", "node", "identity"])["result"]["identity"]["node"]
            .as_u64()
            .unwrap();
    let founder_row = wait_for_contact(
        &founder,
        founder_id,
        &founder_port,
        Some(&format!("localhost:{founder_port}")),
    );
    assert_eq!(founder_row["eligible"], true);
    // `cluster invite --output -` hands the invitation to a pipe.
    let invitation = run(
        &founder,
        &["cluster", "invite", "--node", "host", "--output", "-"],
    );
    assert!(
        invitation.status.success(),
        "{}",
        String::from_utf8_lossy(&invitation.stderr)
    );
    assert!(invitation.stdout.len() > 64);
    let invite_file: PathBuf = host.root().join("host.invite");
    std::fs::write(&invite_file, &invitation.stdout).unwrap();
    // A delivered invitation is private: the file a secret mount provides
    // is group-readable at most; a pipe's copy is the operator's to protect.
    std::fs::set_permissions(&invite_file, std::fs::Permissions::from_mode(0o440)).unwrap();
    // One command enrolls and starts the host, advertising a name.
    let host_address = ports::address();
    let host_port = host_address.rsplit(':').next().unwrap().to_owned();
    let host_server = start(
        &host,
        &[
            "--advertise",
            &format!("localhost:{host_port}"),
            "--invite-file",
            invite_file.to_str().unwrap(),
        ],
    );
    let host_id = admin(&host, &["cluster", "node", "identity"])["result"]["identity"]["node"]
        .as_u64()
        .unwrap();
    assert_ne!(host_id, founder_id);
    wait_for_contact(
        &founder,
        host_id,
        &host_port,
        Some(&format!("localhost:{host_port}")),
    );
    // A second start with the invitation is a plain start: the identity holds.
    drop(host_server);
    let moved_address = ports::address();
    let moved_port = moved_address.rsplit(':').next().unwrap().to_owned();
    // Restarted at another address, without a name: the same node, found
    // again at its new contact. The literal keeps the family `localhost`
    // resolved to, since a fleet speaks one address family (24 §24).
    let loopback = std::net::ToSocketAddrs::to_socket_addrs(&("localhost", 0))
        .unwrap()
        .next()
        .unwrap()
        .ip();
    let moved_literal =
        std::net::SocketAddr::new(loopback, moved_port.parse().unwrap()).to_string();
    let host_server = start(
        &host,
        &[
            "--advertise",
            &moved_literal,
            "--invite-file",
            invite_file.to_str().unwrap(),
        ],
    );
    assert_eq!(
        admin(&host, &["cluster", "node", "identity"])["result"]["identity"]["node"],
        host_id
    );
    wait_for_contact(&founder, host_id, &moved_port, None);
    let probe = run(&host, &["cluster", "node", "probe", "--check", "alive"]);
    assert!(probe.status.success());
    // And back to a name at a third address.
    drop(host_server);
    let third_address = ports::address();
    let third_port = third_address.rsplit(':').next().unwrap().to_owned();
    let _host_server = start(&host, &["--advertise", &format!("localhost:{third_port}")]);
    wait_for_contact(
        &founder,
        host_id,
        &third_port,
        Some(&format!("localhost:{third_port}")),
    );
    // Restated identical reachability changes nothing: the row stays.
    let row = node_row(&founder, host_id).unwrap();
    assert_eq!(row["generation"], 1, "{row}");
}
