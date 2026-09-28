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
//! Where many logs are led, across three real `focal` processes over QUIC
//! ([27](../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §5):
//! a fleet that ran with every session led by the node that founded it is
//! restarted, one process at a time, with leader balancing on. The
//! controller moves preferred leaders one session at a time until no node
//! is preferred by two sessions more than another, every copy of every
//! session follows the move, and each session's log is then led by the
//! leader its placement prefers: handed over by the replica that led in its
//! place, without an election timing out.
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::Duration,
};

#[path = "support/deadline.rs"]
mod deadline;
#[path = "support/ports.rs"]
mod ports;

const OFF: &[(&str, &str)] = &[("FOCAL_LEADER_BALANCE", "off")];
const ON: &[(&str, &str)] = &[("FOCAL_LEADER_BALANCE_HOLD_SECS", "2")];

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
fn start(root: &Path, address: Option<&str>, envs: &[(&str, &str)]) -> Server {
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap(), "start"]);
    command.envs(envs.iter().copied());
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
    server
}
fn placement(root: &Path) -> Option<Value> {
    let output = command(root, &["cluster", "placement"]);
    if !output.status.success() {
        return None;
    }
    let value: Value = serde_json::from_slice(&output.stdout).ok()?;
    Some(value["result"]["placement"].clone())
}
fn sessions(view: &Value) -> Vec<&Value> {
    view["partitions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|partition| partition["sessions"].as_array())
        .flatten()
        .collect()
}
fn node(view: &Value, id: u64) -> Option<&Value> {
    view["partitions"].as_array()?.iter().find_map(|partition| {
        partition["nodes"]
            .as_array()?
            .iter()
            .find(|node| node["node"].as_u64() == Some(id))
    })
}
/// Every node alive and reporting, and `count` sessions settled with three
/// voters and nothing pending or retiring.
fn settled(view: &Value, nodes: &[u64], count: usize) -> bool {
    let sessions = sessions(view);
    sessions.len() == count
        && sessions.iter().all(|session| {
            session["pending"].is_null()
                && session["achieved_max_failures"] == 1
                && session["voters"].as_array().is_some_and(|v| v.len() == 3)
                && session["retiring"].as_array().is_some_and(Vec::is_empty)
        })
        && nodes.iter().all(|id| {
            node(view, *id)
                .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
        })
}
/// Sessions that prefer each node as their leader.
fn preferred(view: &Value) -> BTreeMap<u64, u64> {
    let mut counts = BTreeMap::new();
    for session in sessions(view) {
        *counts
            .entry(session["preferred_leader"].as_u64().unwrap())
            .or_default() += 1;
    }
    counts
}
fn wait_for(
    root: &Path,
    what: &str,
    timeout: Duration,
    condition: impl Fn(&Value) -> bool,
) -> Value {
    let mut deadline = deadline::Deadline::after(timeout);
    let mut last = None;
    while deadline.open() {
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
        .prefix(&format!("focal-leading-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
/// A command the node refuses while its view is behind, asked until it is
/// taken.
fn eventually(root: &Path, args: &[&str]) -> Value {
    let mut deadline = deadline::Deadline::after(Duration::from_secs(90));
    loop {
        let output = command(root, args);
        if output.status.success() {
            return serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        assert!(
            deadline.open(),
            "{args:?} stayed refused: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}
/// One series of the node's metrics by session: what the replica of each
/// session this node hosts says.
fn series(root: &Path, name: &str) -> Option<BTreeMap<String, u64>> {
    let output = command(root, &["cluster", "node", "metrics"]);
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        let Some(rest) = rest.strip_prefix('{') else {
            continue;
        };
        let (labels, value) = rest.split_once("} ")?;
        let session = labels
            .split(',')
            .find_map(|label| label.strip_prefix("session=\""))?
            .trim_end_matches('"');
        values.insert(session.to_owned(), value.trim().parse().ok()?);
    }
    Some(values)
}

#[test]
fn a_fleet_led_from_its_founder_spreads_its_leaders_and_leadership_follows() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b"]
        .iter()
        .map(|name| private_dir(name))
        .collect();
    let roots: Vec<&Path> = dirs.iter().map(|dir| dir.path()).collect();
    let founder = roots[0];
    let addresses: Vec<String> = (0..3).map(|_| ports::address()).collect();
    let mut servers = vec![Some(start(founder, Some(&addresses[0]), OFF))];
    let founder_node = success(founder, &["identity"])["node"].as_u64().unwrap();
    let node_a = join(founder, roots[1], "host-a", &addresses[1]);
    let node_b = join(founder, roots[2], "host-b", &addresses[2]);
    servers.push(Some(start(roots[1], None, OFF)));
    servers.push(Some(start(roots[2], None, OFF)));
    let all = [founder_node, node_a, node_b];
    let view = wait_for(founder, "three hosts", Duration::from_secs(90), |view| {
        sessions(view).len() == 1
            && sessions(view)[0]["pending"].is_null()
            && all.iter().all(|id| {
                node(view, *id)
                    .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
            })
    });
    let tenant = sessions(&view)[0]["tenant"].as_str().unwrap().to_owned();

    // Three sessions, founded on one node and led there: each tolerates one
    // node loss on the same three hosts.
    for name in ["orders", "returns"] {
        let created = eventually(
            founder,
            &[
                "cluster", "sessions", "create", "--tenant", &tenant, "--name", name,
            ],
        );
        assert_eq!(created["node"], founder_node, "{created}");
    }
    let view = wait_for(founder, "three sessions", Duration::from_secs(90), |view| {
        sessions(view).len() == 3
            && sessions(view)
                .iter()
                .all(|session| session["pending"].is_null())
    });
    let ids: Vec<String> = sessions(&view)
        .iter()
        .map(|session| session["session"].as_str().unwrap().to_owned())
        .collect();
    for id in &ids {
        let planned = eventually(
            founder,
            &[
                "cluster",
                "sessions",
                "plan",
                "--tenant",
                &tenant,
                "--session",
                id,
                "--max-failures",
                "1",
            ],
        );
        assert!(
            matches!(planned["state"].as_str(), Some("planned" | "pending")),
            "{planned}"
        );
    }
    let view = wait_for(
        founder,
        "three sessions on three hosts",
        Duration::from_secs(480),
        |view| settled(view, &all, 3),
    );
    // With balancing off every leader stayed where its session was founded.
    assert_eq!(
        preferred(&view),
        BTreeMap::from([(founder_node, 3)]),
        "{view:#?}"
    );
    let epochs: BTreeMap<String, u64> = sessions(&view)
        .iter()
        .map(|session| {
            (
                session["session"].as_str().unwrap().to_owned(),
                session["placement_epoch"].as_u64().unwrap(),
            )
        })
        .collect();

    // The fleet is restarted with balancing on, one process at a time: a
    // quorum of every log stays up throughout.
    for (index, root) in roots.iter().enumerate() {
        servers[index] = None;
        servers[index] = Some(start(root, None, ON));
        wait_for(
            founder,
            "the restarted host",
            Duration::from_secs(180),
            |view| settled(view, &all, 3),
        );
    }

    // Leaders spread: one session each, by two moves, each a placement of
    // its own that every copy followed.
    let view = wait_for(
        founder,
        "preferred leaders spread",
        Duration::from_secs(600),
        |view| {
            settled(view, &all, 3) && preferred(view) == all.iter().map(|node| (*node, 1)).collect()
        },
    );
    let mut moved = 0;
    for session in sessions(&view) {
        let id = session["session"].as_str().unwrap();
        let epoch = session["placement_epoch"].as_u64().unwrap();
        if session["preferred_leader"] == founder_node {
            assert_eq!(
                epoch, epochs[id],
                "a session that stayed was moved: {session}"
            );
        } else {
            assert_eq!(epoch, epochs[id] + 1, "one move for one session: {session}");
            moved += 1;
        }
        assert_eq!(session["voters"].as_array().unwrap().len(), 3);
    }
    assert_eq!(moved, 2);
    let leaders: BTreeMap<String, u64> = sessions(&view)
        .iter()
        .map(|session| {
            (
                session["session"].as_str().unwrap().to_owned(),
                session["preferred_leader"].as_u64().unwrap(),
            )
        })
        .collect();

    // Leadership follows: every copy of every session is led by the leader
    // its placement prefers, and says that it prefers it.
    let mut deadline = deadline::Deadline::after(Duration::from_secs(180));
    loop {
        let said: Vec<_> = roots
            .iter()
            .map(|root| {
                (
                    series(root, "focal_session_leader"),
                    series(root, "focal_session_preferred_leader"),
                )
            })
            .collect();
        if said.iter().all(|(led, preferred)| {
            led.as_ref() == Some(&leaders) && preferred.as_ref() == Some(&leaders)
        }) {
            break;
        }
        assert!(
            deadline.open(),
            "leadership did not follow the placement {leaders:?}: {said:#?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    // Where it was handed over and not taken by election, the hand-overs
    // are counted, and none is counted as failed that was not asked for.
    let asked: u64 = roots
        .iter()
        .filter_map(|root| series(root, "focal_session_leader_returns_total"))
        .flat_map(BTreeMap::into_values)
        .sum();
    let failed: u64 = roots
        .iter()
        .filter_map(|root| series(root, "focal_session_leader_returns_failed_total"))
        .flat_map(BTreeMap::into_values)
        .sum();
    assert!(failed <= asked, "{failed} of {asked}");
    println!("{asked} hand-overs asked for, {failed} that did not hold");

    // At rest: nothing is planned, and nothing moves while it is watched.
    let plan = success(founder, &["cluster", "plan"])["result"]["actions"].clone();
    assert_eq!(plan, serde_json::json!([]));
    // Four holds of the controller's own time, by the periods it ran.
    let began = deadline::periods(founder).unwrap();
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    while deadline::periods(founder).is_none_or(|now| now < began + 80) {
        assert!(deadline.open(), "the founder stopped");
        std::thread::sleep(Duration::from_millis(250));
    }
    let rested = placement(founder).unwrap();
    assert!(settled(&rested, &all, 3), "{rested:#?}");
    assert_eq!(preferred(&rested), preferred(&view));
    for session in sessions(&rested) {
        let id = session["session"].as_str().unwrap();
        assert_eq!(session["preferred_leader"], leaders[id], "{session}");
    }
    drop(servers);
}
