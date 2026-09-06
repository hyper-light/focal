#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use serde_json::Value;
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
fn start(root: &Path) -> Server {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = command(root, &["start"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        for line in BufReader::new(stdout).lines() {
            text.push_str(&line.unwrap());
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                break;
            }
        }
    });
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap()["condition"],
        "Ready"
    );
    Server(child)
}
fn command(root: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.arg("--data-dir").arg(root).args(args);
    command
}
fn run(root: &Path, args: &[&str]) -> Output {
    command(root, args).output().unwrap()
}
fn json(root: &Path, args: &[&str]) -> Value {
    let out = run(root, args);
    assert!(
        out.status.success(),
        "{:?}: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}
#[test]
fn broken_output_retains_exact_page_and_resume_flushes_before_ack() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path());
    let mut child = command(
        root.path(),
        &[
            "watch",
            "claims",
            "--name",
            "broken",
            "--no-seed",
            "--pages",
            "1",
            "--format",
            "json",
        ],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("watch resume 'broken'"), "{error}");
    assert!(error.contains("--data-dir"));
    let before = json(
        root.path(),
        &["watch", "inspect", "broken", "--format", "json"],
    );
    assert_eq!(before["status"]["acknowledged"], 0);
    let retained = before["delivery"].clone();
    assert_eq!(retained["number"], 1);
    drop(server);
    let _server = start(root.path());
    let reply = run(
        root.path(),
        &[
            "watch", "resume", "broken", "--pages", "1", "--format", "json",
        ],
    );
    assert!(
        reply.status.success(),
        "{}",
        String::from_utf8_lossy(&reply.stderr)
    );
    assert!(reply.stderr.is_empty());
    let delivered: Value = serde_json::from_slice(&reply.stdout).unwrap();
    assert_eq!(delivered, retained);
    let after = json(
        root.path(),
        &["watch", "inspect", "broken", "--format", "json"],
    );
    assert_eq!(after["status"]["acknowledged"], 1);
    assert!(after["delivery"].is_null());
    let out = run(
        root.path(),
        &[
            "watch", "resume", "broken", "--pages", "6", "--format", "json",
        ],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stderr.is_empty());
    assert_eq!(String::from_utf8(out.stdout).unwrap().lines().count(), 6);
    assert_eq!(
        json(root.path(), &["watch", "inspect", "--format", "json"])["names"],
        serde_json::json!(["broken"])
    );
    let yaml = run(
        root.path(),
        &[
            "watch", "resume", "broken", "--pages", "2", "--format", "yaml",
        ],
    );
    assert!(
        yaml.status.success(),
        "{}",
        String::from_utf8_lossy(&yaml.stderr)
    );
    let text = String::from_utf8(yaml.stdout).unwrap();
    assert_eq!(text.lines().filter(|line| *line == "---").count(), 2);
}
#[test]
fn ctrl_c_preserves_retained_page_and_prints_context_bound_recovery() {
    let root = tempfile::tempdir().unwrap();
    let _server = start(root.path());
    let mut child = command(
        root.path(),
        &[
            "watch",
            "all",
            "--name",
            "interrupt",
            "--no-seed",
            "--format",
            "json",
        ],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut lines = BufReader::new(output).lines();
        let first = lines.next().unwrap().unwrap();
        send.send(first).unwrap();
        for line in lines {
            if line.is_err() {
                break;
            }
        }
    });
    let first: Value =
        serde_json::from_str(&receive.recv_timeout(Duration::from_secs(20)).unwrap()).unwrap();
    assert_eq!(first["number"], 1);
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("watch resume 'interrupt'")
    );
    let status = json(
        root.path(),
        &["watch", "inspect", "interrupt", "--format", "json"],
    );
    assert!(status["status"]["acknowledged"].as_u64().unwrap() >= 1);
}
