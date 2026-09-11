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
//! Metrics (24 §23): the node's sampled snapshot as Prometheus text over
//! the admin socket and, when configured, over a loopback HTTP/1.0
//! endpoint that serves nothing else.
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    os::unix::fs::PermissionsExt,
    path::Path,
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
fn run(root: &Path, config: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--config", config.to_str().unwrap()])
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}
fn start(root: &Path, config: &Path, advertise: &str) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--config", config.to_str().unwrap()])
        .args(["--data-dir", root.to_str().unwrap(), "start"])
        .args(["--advertise", advertise])
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
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                let _ = send.send(value);
                break;
            }
        }
    });
    let server = Server(child);
    let status = receive
        .recv_timeout(Duration::from_secs(30))
        .expect("the node did not publish readiness");
    assert_eq!(status["condition"], "Ready", "{status}");
    server
}
fn http(address: &str, request: &str) -> (String, String) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP response");
    (head.to_owned(), body.to_owned())
}

#[test]
fn metrics_render_the_sampled_snapshot_over_the_admin_socket_and_the_loopback_endpoint() {
    let dir = tempfile::Builder::new()
        .prefix("focal-metrics-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let root = dir.path();
    let listen = ports::address();
    let advertise = ports::address();
    let config = root.join("focal.yaml");
    std::fs::write(
        &config,
        format!(
            "version: 1\nnode:\n  metrics_listen: {listen}\ntopology:\n  region: eu-a\n  zone: eu-a-1\n"
        ),
    )
    .unwrap();
    // The operator surface is the network node's admin socket; a laptop
    // node without an address has none.
    let _server = start(root, &config, &advertise);
    let identity = run(root, &config, &["cluster", "node", "identity"]);
    assert!(identity.status.success());
    let identity: serde_json::Value = serde_json::from_slice(&identity.stdout).unwrap();
    let node = identity["result"]["identity"]["node"].as_u64().unwrap();
    let session = identity["result"]["identity"]["session"]
        .as_str()
        .unwrap()
        .to_owned();
    // The admin socket renders the latest snapshot as text, with the fixed
    // labels and the founder's session, once the sampler has run.
    let deadline = Instant::now() + Duration::from_secs(30);
    let text = loop {
        let output = run(root, &config, &["cluster", "node", "metrics"]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        if text.contains("focal_session_applied_index{") && text.contains("focal_node_info{") {
            break text;
        }
        assert!(Instant::now() < deadline, "no snapshot within 30 s: {text}");
        std::thread::sleep(Duration::from_millis(250));
    };
    assert!(
        !text.trim_start().starts_with('{'),
        "text, not JSON: {text}"
    );
    let base = format!("node=\"{node}\",cluster=\"");
    assert!(text.contains(&format!("focal_node_info{{{base}")), "{text}");
    assert!(
        text.contains("role=\"founder\",region=\"eu-a\",zone=\"eu-a-1\",capability=\"1\"} 1"),
        "{text}"
    );
    for series in [
        "focal_memory_used_bytes{",
        "focal_memory_limit_bytes{",
        "focal_disk_outstanding_bytes{",
        "focal_wal_appended_records_total{",
        "focal_fleet_installed{",
        "focal_root_applied_index{",
        "focal_peer_messages_delivered_total{",
        "focal_liveness_members{",
        "focal_upgrade_fence_level{",
        "focal_upgrade_announced_level{",
        "focal_placement_installed{",
        "focal_admission_tenants{",
        "# TYPE focal_session_apply_lag gauge",
    ] {
        assert!(text.contains(series), "missing {series}: {text}");
    }
    assert!(text.contains(&format!("session=\"{session}\"")), "{text}");
    assert!(text.contains("focal_upgrade_fence_level{") && text.contains("} 0\n"));
    assert!(text.ends_with('\n'));
    // The loopback endpoint serves the same exposition to GET /metrics, and
    // nothing else.
    let (head, body) = http(&listen, "GET /metrics HTTP/1.0\r\nHost: focal\r\n\r\n");
    assert!(head.starts_with("HTTP/1.0 200 OK"), "{head}");
    assert!(
        head.contains("Content-Type: text/plain; version=0.0.4"),
        "{head}"
    );
    assert!(head.contains("Connection: close"), "{head}");
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(length, body.len());
    assert!(body.contains("focal_node_info{"), "{body}");
    assert!(body.contains("focal_session_applied_index{"), "{body}");
    let (head, body) = http(&listen, "GET /other HTTP/1.0\r\n\r\n");
    assert!(head.starts_with("HTTP/1.0 404"), "{head}");
    assert!(!body.contains("focal_"));
    let (head, _) = http(&listen, "POST /metrics HTTP/1.0\r\n\r\n");
    assert!(head.starts_with("HTTP/1.0 405"), "{head}");
    // A second scrape still answers after the refused ones.
    let (head, body) = http(&listen, "GET /metrics HTTP/1.0\r\n\r\n");
    assert!(head.starts_with("HTTP/1.0 200 OK"), "{head}");
    assert!(body.contains("focal_metrics_sampled_milliseconds{"));
}
