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
//! A session the partition leader does not vote in (24 §9): a host creates
//! a session for an admitted tenant, the founder — which leads the
//! directory but holds no copy of that session — plans it for one tolerated
//! node loss and drives the expansion through the session's own leader,
//! then the session's leader is drained: the placement heals onto the other
//! hosts through the leader until its removal commits and the log elects
//! another, after which the drained host is removed.
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
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
fn command(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}
fn success(root: &Path, args: &[&str]) -> Value {
    let output = command(root, args);
    assert!(
        output.status.success(),
        "arguments {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stdout)))
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
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|error| {
            panic!(
                "process did not report startup ({error}; data dir {}, exit {:?})",
                root.display(),
                server.0.try_wait()
            )
        });
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    (server, status)
}
fn placement(root: &Path) -> Option<Value> {
    let output = command(root, &["cluster", "placement"]);
    if !output.status.success() {
        return None;
    }
    let value: Value = serde_json::from_slice(&output.stdout).ok()?;
    Some(value["result"]["placement"].clone())
}
/// The session with this identity in the view.
fn session<'a>(view: &'a Value, id: &str) -> Option<&'a Value> {
    view["partitions"].as_array()?.iter().find_map(|partition| {
        partition["sessions"]
            .as_array()?
            .iter()
            .find(|session| session["session"].as_str() == Some(id))
    })
}
fn node(view: &Value, id: u64) -> Option<&Value> {
    view["partitions"].as_array()?.iter().find_map(|partition| {
        partition["nodes"]
            .as_array()?
            .iter()
            .find(|node| node["node"].as_u64() == Some(id))
    })
}
fn ids(value: &Value) -> Vec<u64> {
    let mut ids: Vec<u64> = value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}
fn names(session: &Value, id: u64) -> bool {
    ["voters", "materializers", "content_copies", "retiring"]
        .iter()
        .any(|list| ids(&session[list]).contains(&id))
        || session["pending"]["voters"]
            .as_array()
            .is_some_and(|voters| voters.iter().any(|v| v.as_u64() == Some(id)))
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
    let health = command(root, &["cluster", "node", "health"]);
    panic!(
        "{what} did not happen within {timeout:?}; health: {}; last view: {last:#?}",
        String::from_utf8_lossy(&health.stdout)
    );
}
fn join(founder: &Path, host: &Path, name: &str, advertise: &str) -> u64 {
    let invitation = founder.join(format!("{name}.invite"));
    let written = success(
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
    let joined = success(
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
fn private_dir(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("focal-remote-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
/// Every listed node alive and reporting, and the session settled at
/// `max_failures` with no plan pending and nothing retiring.
fn settled(view: &Value, id: &str, nodes: &[u64], max_failures: u64) -> bool {
    session(view, id).is_some_and(|session| {
        session["pending"].is_null()
            && session["achieved_max_failures"] == max_failures
            && session["retiring"].as_array().is_some_and(Vec::is_empty)
    }) && nodes.iter().all(|id| {
        node(view, *id)
            .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
    })
}

#[test]
fn a_session_the_founder_does_not_vote_in_expands_and_heals_through_its_own_leader() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b", "host-c"]
        .iter()
        .map(|name| private_dir(name))
        .collect();
    let founder = dirs[0].path();
    let host_a = dirs[1].path();
    let addresses: Vec<String> = (0..4).map(|_| address()).collect();
    let (_founder_server, status) = start(founder, Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let founder_node = success(founder, &["identity"])["node"].as_u64().unwrap();
    let node_a = join(founder, host_a, "host-a", &addresses[1]);
    let node_b = join(founder, dirs[2].path(), "host-b", &addresses[2]);
    let node_c = join(founder, dirs[3].path(), "host-c", &addresses[3]);
    let mut servers = vec![
        (node_a, Some(start(host_a, None).0)),
        (node_b, Some(start(dirs[2].path(), None).0)),
        (node_c, Some(start(dirs[3].path(), None).0)),
    ];
    let all = [founder_node, node_a, node_b, node_c];
    wait_for(founder, "four hosts", Duration::from_secs(90), |view| {
        all.iter().all(|id| {
            node(view, *id)
                .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
        })
    });
    // A tenant the cluster serves, and a session for it created on host A:
    // the founder holds no copy of it.
    let tenant = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b";
    let admitted = success(
        founder,
        &["cluster", "tenants", "admit", "--tenant", tenant],
    )["result"]
        .clone();
    assert_eq!(admitted["kind"], "tenants", "{admitted}");
    let created = wait_created(host_a, tenant);
    let session_id = created["session"].as_str().unwrap().to_owned();
    assert_eq!(created["node"], node_a, "{created}");
    let registered = {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(view) = placement(founder)
                && session(&view, &session_id).is_some_and(|session| {
                    session["pending"].is_null()
                        && session["voters"].as_array().is_some_and(|v| v.len() == 1)
                })
            {
                break view;
            }
            assert!(
                Instant::now() < deadline,
                "registration did not happen; host agent: {}",
                String::from_utf8_lossy(&command(host_a, &["cluster", "node", "health"]).stdout)
            );
            std::thread::sleep(Duration::from_millis(150));
        }
    };
    let registered = session(&registered, &session_id).unwrap().clone();
    assert_eq!(registered["voters"], serde_json::json!([node_a]));
    assert_eq!(registered["founder"], node_a);
    // The founder plans one tolerated loss and drives it through host A.
    let planned = success(
        founder,
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            tenant,
            "--session",
            &session_id,
            "--max-failures",
            "1",
        ],
    )["result"]
        .clone();
    assert_eq!(planned["state"], "planned", "{planned}");
    let view = wait_for(founder, "activation", Duration::from_secs(240), |view| {
        settled(view, &session_id, &all, 1)
    });
    let active = session(&view, &session_id).unwrap().clone();
    assert_eq!(active["route_epoch"], 2);
    let voters = ids(&active["voters"]);
    assert_eq!(voters.len(), 3, "{active}");
    assert!(
        voters.contains(&node_a),
        "the incumbent keeps its copy: {active}"
    );
    // Readiness probes: the founder leads its own session and the root, so
    // it is authoritative and not catching up; the policy holds once the
    // expansion activated; host A leads the new session.
    let readiness =
        success(founder, &["cluster", "node", "readiness"])["result"]["readiness"].clone();
    assert_eq!(readiness["alive"], true, "{readiness}");
    assert_eq!(readiness["authoritative"], true);
    assert_eq!(readiness["catching_up"], false);
    assert_eq!(readiness["policy_satisfied"], true, "{readiness}");
    assert!(
        command(founder, &["cluster", "node", "probe", "--check", "alive"])
            .status
            .success()
    );
    assert!(
        command(
            founder,
            &["cluster", "node", "probe", "--check", "authoritative"]
        )
        .status
        .success()
    );
    let output = command(
        founder,
        &["cluster", "node", "probe", "--check", "catching-up"],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("[probe_failed]"));
    let host_readiness =
        success(host_a, &["cluster", "node", "readiness"])["result"]["readiness"].clone();
    assert_eq!(host_readiness["authoritative"], true, "{host_readiness}");
    assert!(
        host_readiness["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["session"] == session_id.as_str()
                && session["leader"] == node_a
                && session["achieved_max_failures"] == 1),
        "{host_readiness}"
    );
    // The founder still holds no copy: its replica list does not name the session.
    let health = success(founder, &["cluster", "node", "health"])["result"]["health"].clone();
    assert!(
        !health["placement"]["installed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry.as_str().unwrap().ends_with(&session_id)),
        "{health}"
    );
    // Drain the session's own leader: the heal runs through it until its
    // removal commits and the log elects another leader.
    let drained = success(
        founder,
        &["cluster", "nodes", "drain", "--node", &node_a.to_string()],
    )["result"]
        .clone();
    assert_eq!(drained["changed"], true, "{drained}");
    let remaining: Vec<u64> = all.iter().copied().filter(|id| *id != node_a).collect();
    let view = wait_for(
        founder,
        "heal around the leader",
        Duration::from_secs(300),
        |view| {
            settled(view, &session_id, &remaining, 1)
                && session(view, &session_id).is_some_and(|session| !names(session, node_a))
        },
    );
    let healed = session(&view, &session_id).unwrap().clone();
    let healed_voters = ids(&healed["voters"]);
    assert_eq!(healed_voters.len(), 3, "{healed}");
    assert!(!healed_voters.contains(&node_a));
    // The drained host leaves the cluster.
    let deadline = Instant::now() + Duration::from_secs(120);
    let removed = loop {
        let output = command(
            founder,
            &["cluster", "nodes", "remove", "--node", &node_a.to_string()],
        );
        if output.status.success() {
            break serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("[node_holding]")
                || stderr.contains("[not_leader]")
                || stderr.contains("[unavailable]"),
            "{stderr}"
        );
        assert!(
            Instant::now() < deadline,
            "removal stayed refused: {stderr}"
        );
        std::thread::sleep(Duration::from_millis(500));
    };
    assert_eq!(removed["kind"], "node_removed", "{removed}");
    assert_eq!(removed["revoked"], true);
    servers[0].1.take();
    drop(servers);
}
/// Session creation on a host waits for the host's directory view.
fn wait_created(host: &Path, tenant: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let output = command(
            host,
            &[
                "cluster", "sessions", "create", "--tenant", tenant, "--name", "orders",
            ],
        );
        if output.status.success() {
            return serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "session creation stayed refused: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}
