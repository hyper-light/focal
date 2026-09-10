#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::disallowed_macros
)]
//! External MCP client qualification (REMAINING §9.5): third-party clients
//! drive a real `focal mcp serve` adapter. The scripts live under
//! `crates/focal-mcp/tests/external` and run only with `FOCAL_EXTERNAL_MCP=1`,
//! because they need tools from outside this repository; otherwise this test
//! records the skip and passes.
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
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
fn scripts() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../focal-mcp/tests/external")
}

#[test]
fn external_mcp_clients_complete_discovery_and_one_read_against_the_adapter() {
    if std::env::var_os("FOCAL_EXTERNAL_MCP").is_none_or(|value| value != "1") {
        eprintln!("FOCAL_EXTERNAL_MCP is not set: external MCP client qualification skipped");
        return;
    }
    let root = tempfile::Builder::new()
        .prefix("focal-mcp-external-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let activation = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args([
            "--data-dir",
            root.path().to_str().unwrap(),
            "cluster",
            "replicas",
            "activate-native",
        ])
        .output()
        .unwrap();
    assert!(activation.status.success());
    let _server = start(root.path(), &address());
    let mut failures = Vec::new();
    for script in ["inspector.sh", "claude-code.sh", "python-sdk.py"] {
        let output = Command::new(scripts().join(script))
            .env("FOCAL_BIN", env!("CARGO_BIN_EXE_focal"))
            .env("FOCAL_DATA_DIR", root.path())
            .output()
            .unwrap();
        let report = format!(
            "{script}: exit {:?}\n{}\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!("{report}");
        match output.status.code() {
            Some(0) => {}
            Some(3) => failures.push(format!("{script}: client unavailable")),
            _ => failures.push(report),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
