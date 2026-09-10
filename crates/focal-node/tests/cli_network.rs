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
//! Contracted network CLI, real child processes and durable private join state.
#[path = "support/cli_client_context.rs"]
mod client_context;
#[path = "support/cli_cluster.rs"]
mod cluster;
#[path = "support/cli_replicas.rs"]
mod replicas;
use focal_node::network_join::NodeInvitation;
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
fn command(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}
fn success(root: &Path, args: &[&str]) -> (Value, Output) {
    let output = command(root, args);
    assert!(
        output.status.success(),
        "arguments {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (serde_json::from_slice(&output.stdout).unwrap(), output)
}
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
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
        let mut first = String::new();
        let mut reported = false;
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            if reported {
                continue;
            }
            first.push_str(&line);
            first.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&first) {
                let _ = send.send(value);
                reported = true;
            }
            if first.len() > 65536 {
                break;
            }
        }
    });
    let mut server = Server(child);
    let status = receive
        .recv_timeout(Duration::from_secs(25))
        .unwrap_or_else(|error| {
            let exit = server.0.try_wait();
            let holders = Command::new("lsof")
                .args(["-nP", "-iUDP", "-iTCP"])
                .output()
                .map(|output| {
                    String::from_utf8_lossy(&output.stdout)
                        .lines()
                        .filter(|line| {
                            line.rsplit(':')
                                .next()
                                .and_then(|port| port.split(' ').next()?.parse::<u32>().ok())
                                .is_some_and(|port| (24_000..32_000).contains(&port))
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            panic!(
                "network process did not report startup ({error}; data dir {}, advertise {address:?}, exit {exit:?}); sockets in the test range:\n{holders}",
                root.display()
            )
        });
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    (server, status)
}
fn assert_redacted(output: &Output, token: &str) {
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
}

#[test]
fn founder_invite_join_and_network_restart_preserve_identity_without_exposing_secrets() {
    let founder = tempfile::Builder::new()
        .prefix("focal-network-cli-")
        .tempdir_in("/tmp")
        .unwrap();
    let peer = tempfile::Builder::new()
        .prefix("focal-network-peer-")
        .tempdir_in("/tmp")
        .unwrap();
    let founder_address = address();
    let peer_address = address();
    let (server, status) = start(founder.path(), Some(&founder_address));
    assert_eq!(status["condition"], "Ready");
    let (identity, _) = success(founder.path(), &["identity"]);
    let invitation = founder.path().join("worker-2.invite");
    let args = [
        "cluster",
        "invite",
        "--node",
        "worker-2",
        "--output",
        invitation.to_str().unwrap(),
    ];
    let (written, first_output) = success(founder.path(), &args);
    assert_eq!(written["condition"], "InvitationWritten");
    let original = std::fs::read(&invitation).unwrap();
    assert_eq!(
        std::fs::metadata(&invitation).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let bundle = NodeInvitation::load(&invitation).unwrap();
    let token = bundle.invitation().expose_token().unwrap();
    assert_redacted(&first_output, &token);
    let (_, retry_output) = success(founder.path(), &args);
    assert_redacted(&retry_output, &token);
    assert_eq!(std::fs::read(&invitation).unwrap(), original);
    let occupied = founder.path().join("occupied.invite");
    std::fs::write(&occupied, b"existing private file").unwrap();
    std::fs::set_permissions(&occupied, std::fs::Permissions::from_mode(0o600)).unwrap();
    let refused = command(
        founder.path(),
        &[
            "cluster",
            "invite",
            "--node",
            "worker-2",
            "--output",
            occupied.to_str().unwrap(),
        ],
    );
    assert!(!refused.status.success());
    assert_redacted(&refused, &token);
    assert_eq!(std::fs::read(&occupied).unwrap(), b"existing private file");
    let join_args = [
        "join",
        "--invite-file",
        invitation.to_str().unwrap(),
        "--advertise",
        peer_address.as_str(),
    ];
    let (joined, join_output) = success(peer.path(), &join_args);
    assert_redacted(&join_output, &token);
    assert_eq!(joined["cluster"], identity["cluster"]);
    assert_eq!(joined["ledger"], identity["ledger"]);
    assert_ne!(joined["node"], identity["node"]);
    assert!(joined.get("condition").is_none()); // join reports identity only
    assert!(!peer.path().join("wal").exists());
    let key = std::fs::read(peer.path().join("JOIN/node-key/join-key.bin")).unwrap();
    let (retried, retry_output) = success(peer.path(), &join_args);
    assert_eq!(retried, joined);
    assert_redacted(&retry_output, &token);
    assert_eq!(
        std::fs::read(peer.path().join("JOIN/node-key/join-key.bin")).unwrap(),
        key
    );
    let changed_address = address();
    let conflict = command(
        peer.path(),
        &[
            "join",
            "--invite-file",
            invitation.to_str().unwrap(),
            "--advertise",
            &changed_address,
        ],
    );
    assert!(!conflict.status.success());
    assert_redacted(&conflict, &token);
    drop(server); // crash instead of invoking a shutdown callback
    let (_server, status) = start(founder.path(), None);
    assert_eq!(status["condition"], "Ready");
    assert_eq!(success(founder.path(), &["identity"]).0, identity);
    let (_, retry_output) = success(founder.path(), &args);
    assert_redacted(&retry_output, &token);
    assert_eq!(std::fs::read(&invitation).unwrap(), original);
    let (peer_server, _) = start(peer.path(), None);
    assert_eq!(success(peer.path(), &["identity"]).0, joined);
    // Every physical owner has local diagnostics. Sharing domain IDs must never
    // grant the founder's Runtime identity or invitation-signing authority.
    let forged = json!({"protocol":1,"ledger":identity["ledger"],"route_epoch":1,"request_epoch":1,"request_id":vec![41;16],"operation":{"Submit":{"expected_revision":null,"command":{"NegotiateEpoch":{"epoch":1}}}}});
    let file = peer.path().join("forged-runtime.json");
    std::fs::write(&file, serde_json::to_vec(&forged).unwrap()).unwrap();
    assert!(
        !command(peer.path(), &["request", file.to_str().unwrap()])
            .status
            .success()
    );
    assert!(peer.path().join("focal-admin.sock").exists());
    assert_eq!(
        success(peer.path(), &["cluster", "node", "identity"]).0["result"]["identity"]["node"],
        joined["node"]
    );
    let refused = command(
        peer.path(),
        &[
            "cluster",
            "invite",
            "--node",
            "forged",
            "--output",
            peer.path().join("forged.invite").to_str().unwrap(),
        ],
    );
    assert!(!refused.status.success());
    assert_redacted(&refused, &token);
    assert!(!peer.path().join("forged.invite").exists());
    drop(peer_server);
    let (_peer_server, _) = start(peer.path(), None);
    assert_eq!(success(peer.path(), &["identity"]).0, joined);
}

#[test]
fn network_cli_refuses_unsafe_or_incomplete_input_without_initializing_a_local_replacement() {
    let root = tempfile::Builder::new()
        .prefix("focal-network-refuse-")
        .tempdir_in("/tmp")
        .unwrap();
    assert!(
        !command(root.path(), &["start", "--listen", "127.0.0.1:7443"])
            .status
            .success()
    );
    assert!(!root.path().join("wal").exists());
    assert!(
        !command(
            root.path(),
            &[
                "join",
                "--invite-file",
                "missing",
                "--advertise",
                "127.0.0.1:7444"
            ]
        )
        .status
        .success()
    );
    assert!(!root.path().join("JOIN").exists());
    assert!(
        !command(
            root.path(),
            &[
                "cluster", "invite", "--node", "bad name", "--output", "unused"
            ]
        )
        .status
        .success()
    );
}
