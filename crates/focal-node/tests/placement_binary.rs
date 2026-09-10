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
//! Placement across three real `focal` processes over QUIC
//! ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §17):
//! a laptop session expands to three hosts under an operator's durability
//! request, the session leader is killed with SIGKILL while the plan is
//! being executed and converges after its restart, and the activated
//! placement survives the loss and return of one host.
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
    start_with(root, address, &[])
}
/// Start a server with extra environment for the process.
fn start_with(root: &Path, address: Option<&str>, envs: &[(&str, &str)]) -> (Server, Value) {
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
    (server, status)
}
/// The checkpoint seed files a node holds, across every session directory.
fn seed_files(root: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let Ok(sessions) = std::fs::read_dir(root.join("seeds")) else {
        return names;
    };
    for session in sessions.flatten() {
        let Ok(files) = std::fs::read_dir(session.path()) else {
            continue;
        };
        names.extend(
            files
                .flatten()
                .filter_map(|file| file.file_name().to_str().map(str::to_owned))
                .filter(|name| name.ends_with(".seed")),
        );
    }
    names.sort();
    names
}
/// The operator's placement view, or `None` while the node's admin socket
/// is not answering (during a restart).
fn placement(root: &Path) -> Option<Value> {
    let output = command(root, &["cluster", "placement"]);
    if !output.status.success() {
        return None;
    }
    let value: Value = serde_json::from_slice(&output.stdout).ok()?;
    Some(value["result"]["placement"].clone())
}
fn session(view: &Value) -> Option<&Value> {
    view["partitions"]
        .as_array()?
        .iter()
        .find_map(|partition| partition["sessions"].as_array()?.first())
}
fn node(view: &Value, id: u64) -> Option<&Value> {
    view["partitions"].as_array()?.iter().find_map(|partition| {
        partition["nodes"]
            .as_array()?
            .iter()
            .find(|node| node["node"].as_u64() == Some(id))
    })
}
/// Poll the founder's view until `condition` holds, with the last view in
/// the failure message.
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
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{what} did not happen within {timeout:?}; last view: {last:#?}");
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

#[test]
fn a_laptop_session_expands_to_three_processes_and_converges_after_its_leader_is_killed_mid_plan() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b"]
        .iter()
        .map(|name| {
            tempfile::Builder::new()
                .prefix(&format!("focal-placement-{name}-"))
                .tempdir_in("/tmp")
                .unwrap()
        })
        .collect();
    let founder = dirs[0].path();
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    let (mut founder_server, status) = start(founder, Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let identity = success(founder, &["identity"]);
    let founder_node = identity["node"].as_u64().unwrap();
    let node_a = join(founder, dirs[1].path(), "host-a", &addresses[1]);
    let node_b = join(founder, dirs[2].path(), "host-b", &addresses[2]);
    let (_server_a, _) = start(dirs[1].path(), None);
    let (server_b, _) = start(dirs[2].path(), None);
    // Every host is enrolled, alive and reporting load; the founder's session
    // is registered at its single-node guarantee.
    let view = wait_for(
        founder,
        "three hosts enrolled",
        Duration::from_secs(90),
        |view| {
            session(view).is_some_and(|session| session["pending"].is_null())
                && [founder_node, node_a, node_b].iter().all(|id| {
                    node(view, *id).is_some_and(|node| {
                        node["alive"] == true && node["disk_available"].is_number()
                    })
                })
        },
    );
    let registered = session(&view).unwrap().clone();
    assert_eq!(registered["route_epoch"], 1);
    assert_eq!(registered["max_failures"], 0);
    assert_eq!(registered["founder"], founder_node);
    assert_eq!(registered["voters"], serde_json::json!([founder_node]));
    let tenant = registered["tenant"].as_str().unwrap().to_owned();
    let ledger = registered["session"].as_str().unwrap().to_owned();
    // The operator asks for one tolerated node loss: the planner picks the
    // three hosts and the request is exact on retry.
    let plan_args = [
        "cluster",
        "sessions",
        "plan",
        "--tenant",
        tenant.as_str(),
        "--session",
        ledger.as_str(),
        "--survive",
        "node",
        "--max-failures",
        "1",
    ];
    let planned = success(founder, &plan_args)["result"].clone();
    assert_eq!(planned["kind"], "session_planned");
    assert_eq!(planned["state"], "planned");
    assert_eq!(planned["max_failures"], 1);
    let mut voters: Vec<u64> = planned["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    voters.sort_unstable();
    assert_eq!(voters, {
        let mut all = vec![founder_node, node_a, node_b];
        all.sort_unstable();
        all
    });
    let operation = planned["operation"].as_str().unwrap().to_owned();
    let retried = success(founder, &plan_args)["result"].clone();
    assert_eq!(retried["operation"], operation);
    assert!(
        matches!(retried["state"].as_str(), Some("planned" | "pending")),
        "{retried}"
    );
    // Kill the session leader (the founder, which also runs the controller)
    // with SIGKILL while the plan is being executed, then bring it back.
    let seen = wait_for(
        founder,
        "the plan under way",
        Duration::from_secs(90),
        |view| {
            session(view).is_some_and(|session| {
                session["pending"]["phase"]
                    .as_str()
                    .is_some_and(|phase| phase != "Planned")
                    || session["route_epoch"] == 2
            })
        },
    );
    let phase = session(&seen).unwrap()["pending"]["phase"].clone();
    founder_server.0.kill().unwrap();
    founder_server.0.wait().unwrap();
    let (founder_server, status) = start(founder, None);
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    // The controller reconstructs the plan from the committed directory and
    // drives it to activation: three voters at route epoch 2, the promised
    // failure achieved, nothing blocking.
    let activated = wait_for(founder, "activation", Duration::from_secs(180), |view| {
        session(view).is_some_and(|session| {
            session["pending"].is_null()
                && session["route_epoch"] == 2
                && session["achieved_max_failures"] == 1
        })
    });
    let active = session(&activated).unwrap();
    assert_eq!(active["operation"], Value::Null);
    assert_eq!(active["max_failures"], 1);
    assert_eq!(active["membership_epoch"], 3, "two promotions");
    assert_eq!(active["placement_epoch"], 2);
    assert_eq!(active["founder"], founder_node);
    assert!(active["blocked_by"].as_array().unwrap().is_empty());
    assert_eq!(active["retiring"], serde_json::json!([]));
    let mut active_voters: Vec<u64> = active["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    active_voters.sort_unstable();
    assert_eq!(active_voters, voters);
    // The same request now reads as satisfied.
    let satisfied = success(founder, &plan_args)["result"].clone();
    assert_eq!(
        satisfied["state"], "satisfied",
        "{satisfied} (killed at {phase})"
    );
    let plan = success(founder, &["cluster", "plan"])["result"]["actions"].clone();
    assert_eq!(plan, serde_json::json!([]));
    // The founder's local socket follows the route epoch the expansion moved.
    let output = command(founder, &["status"]);
    assert!(
        output.status.success(),
        "status after activation: {}; founder replicas {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&command(founder, &["cluster", "replicas", "diagnostics"]).stdout),
    );
    // Losing one host keeps a quorum: the founder still answers a quorum read
    // of its session, and the directory measures the weaker guarantee.
    drop(server_b);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let output = command(founder, &["status"]);
        if output.status.success() {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert!(
                value["result"]["Read"]["token"]["sequence"].is_number(),
                "{value}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session lost its quorum after one host loss: {}; founder replicas {}; host-a replicas {}; founder view {:#?}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(
                &command(founder, &["cluster", "replicas", "diagnostics"]).stdout
            ),
            String::from_utf8_lossy(
                &command(dirs[1].path(), &["cluster", "replicas", "diagnostics"]).stdout
            ),
            placement(founder).and_then(|view| session(&view).cloned())
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    wait_for(
        founder,
        "the lost host suspected",
        Duration::from_secs(120),
        |view| {
            node(view, node_b).is_some_and(|node| node["alive"] == false)
                && session(view).is_some_and(|session| session["achieved_max_failures"] == 0)
        },
    );
    // The host returns, reopens its copy and the guarantee is whole again.
    let (_server_b, _) = start(dirs[2].path(), None);
    wait_for(
        founder,
        "the returned host alive",
        Duration::from_secs(120),
        |view| {
            node(view, node_b).is_some_and(|node| node["alive"] == true)
                && session(view).is_some_and(|session| {
                    session["achieved_max_failures"] == 1 && session["pending"].is_null()
                })
        },
    );
    drop(founder_server);
}

/// A native founder session whose checkpoint exceeds the inline bound
/// (forced down to 64 bytes here) is carried to new hosts as chunked seeds
/// (25 §5): the founder seals its Core root as seeds when it checkpoints,
/// each fresh copy retains the Raft snapshot until it has pulled every chunk
/// from the founder under the pending placement's announcement, and the
/// expansion activates exactly as an inline one would.
#[test]
fn a_seeded_native_checkpoint_carries_the_founder_session_to_new_hosts() {
    const SEEDED: &[(&str, &str)] = &[("FOCAL_SEED_INLINE_BYTES", "64")];
    let dirs: Vec<_> = ["founder", "host-a", "host-b"]
        .iter()
        .map(|name| {
            tempfile::Builder::new()
                .prefix(&format!("focal-seeded-{name}-"))
                .tempdir_in("/tmp")
                .unwrap()
        })
        .collect();
    let founder = dirs[0].path();
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    // Offline native activation on the laptop before the node ever listens.
    let activation = success(founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let (founder_server, status) = start_with(founder, Some(&addresses[0]), SEEDED);
    assert_eq!(status["condition"], "Ready");
    let identity = success(founder, &["identity"]);
    let founder_node = identity["node"].as_u64().unwrap();
    // An explicit checkpoint (retried while the node's first proposals are
    // still in flight) seals the Core root as seeds beside the founder's
    // data, since it exceeds 64 inline bytes; the log is compacted behind
    // it, so a later copy can only catch up through the seeded snapshot.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let output = command(founder, &["cluster", "replicas", "checkpoint"]);
        if output.status.success() {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(value["result"]["kind"], "replica_checkpointed", "{value}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the founder never checkpointed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let founder_seeds = seed_files(founder);
    assert!(
        !founder_seeds.is_empty(),
        "the founder's checkpoint was not seeded"
    );
    // The founder itself restores from its own seeded checkpoint after a kill.
    let mut founder_server = founder_server;
    founder_server.0.kill().unwrap();
    founder_server.0.wait().unwrap();
    let (founder_server, status) = start_with(founder, None, SEEDED);
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    let node_a = join(founder, dirs[1].path(), "host-a", &addresses[1]);
    let node_b = join(founder, dirs[2].path(), "host-b", &addresses[2]);
    let (_server_a, _) = start_with(dirs[1].path(), None, SEEDED);
    let (_server_b, _) = start_with(dirs[2].path(), None, SEEDED);
    let view = wait_for(
        founder,
        "three hosts enrolled",
        Duration::from_secs(90),
        |view| {
            session(view).is_some_and(|session| session["pending"].is_null())
                && [founder_node, node_a, node_b].iter().all(|id| {
                    node(view, *id).is_some_and(|node| {
                        node["alive"] == true && node["disk_available"].is_number()
                    })
                })
        },
    );
    let registered = session(&view).unwrap().clone();
    let tenant = registered["tenant"].as_str().unwrap().to_owned();
    let ledger = registered["session"].as_str().unwrap().to_owned();
    let plan_args = [
        "cluster",
        "sessions",
        "plan",
        "--tenant",
        tenant.as_str(),
        "--session",
        ledger.as_str(),
        "--survive",
        "node",
        "--max-failures",
        "1",
    ];
    let planned = success(founder, &plan_args)["result"].clone();
    assert_eq!(planned["kind"], "session_planned");
    // The copies can only catch up through the seeded snapshot: activation
    // proves every chunk was pulled and the Core root assembled on each.
    let deadline = Instant::now() + Duration::from_secs(240);
    let activated = loop {
        let view = placement(founder);
        if let Some(view) = &view
            && session(view).is_some_and(|session| {
                session["pending"].is_null()
                    && session["route_epoch"] == 2
                    && session["achieved_max_failures"] == 1
            })
        {
            break view.clone();
        }
        if Instant::now() >= deadline {
            let diagnostics: Vec<String> = dirs
                .iter()
                .map(|dir| {
                    let output = command(dir.path(), &["cluster", "replicas", "diagnostics"]);
                    let membership = command(dir.path(), &["cluster", "replicas", "show"]);
                    let health = command(dir.path(), &["cluster", "node", "health"]);
                    format!(
                        "{}: seeds {:?}; {}{}{}{}{}{}",
                        dir.path().display(),
                        seed_files(dir.path()),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr),
                        String::from_utf8_lossy(&membership.stdout),
                        String::from_utf8_lossy(&membership.stderr),
                        String::from_utf8_lossy(&health.stdout),
                        String::from_utf8_lossy(&health.stderr),
                    )
                })
                .collect();
            panic!(
                "activation over a seeded checkpoint did not happen; last view: {view:#?}; {}",
                diagnostics.join("\n")
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let active = session(&activated).unwrap();
    let mut voters: Vec<u64> = active["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    voters.sort_unstable();
    assert_eq!(voters, {
        let mut all = vec![founder_node, node_a, node_b];
        all.sort_unstable();
        all
    });
    // The authority checkpointed again as each learner joined (a member
    // added behind a compacted log can only be seeded by a snapshot that
    // names it), so every host holds a chunk the founder sealed, beside the
    // seeds of its own readiness checkpoints.
    let sealed = seed_files(founder);
    assert!(sealed.len() > founder_seeds.len(), "{sealed:?}");
    for host in [dirs[1].path(), dirs[2].path()] {
        let seeds = seed_files(host);
        assert!(
            seeds.iter().any(|seed| sealed.contains(seed)),
            "{} holds {seeds:?}, the founder sealed {sealed:?}",
            host.display()
        );
    }
    let output = command(founder, &["status"]);
    assert!(
        output.status.success(),
        "status after activation: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Movement across processes (25 §6): the operator moves the one member
    // to host-a; the founder's controller carries the transfer through the
    // seed, the barrier, host-a's readiness stated over its own connection,
    // activation and cleanup, and every process reports the same map.
    let ranges = |root: &Path| -> Option<Value> {
        let output = command(root, &["cluster", "replicas", "ranges", "list"]);
        if !output.status.success() {
            return None;
        }
        serde_json::from_slice::<Value>(&output.stdout)
            .ok()
            .map(|value| value["result"]["ranges"].clone())
    };
    let initial = ranges(founder).expect("ranges list");
    assert_eq!(initial["epoch"], 1, "{initial}");
    assert_eq!(initial["members"].as_array().unwrap().len(), 1, "{initial}");
    assert!(initial["members"][0]["holder"].is_null(), "{initial}");
    let member = initial["members"][0]["id"].as_str().unwrap().to_owned();
    let moved = success(
        founder,
        &[
            "cluster",
            "replicas",
            "ranges",
            "move",
            "--member",
            member.as_str(),
            "--node",
            &node_a.to_string(),
        ],
    );
    assert_eq!(moved["result"]["kind"], "range_move_proposed", "{moved}");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let views: Vec<Option<Value>> = dirs.iter().map(|dir| ranges(dir.path())).collect();
        let done = views.iter().all(|view| {
            view.as_ref().is_some_and(|view| {
                view["epoch"] == 2
                    && view["pending"].is_null()
                    && view["history"].as_array().is_some_and(Vec::is_empty)
                    && view["members"][0]["holder"] == node_a
            })
        });
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the member did not move within 120s: {views:#?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    // The founder restarts on its checkpoint and log and reports the same.
    let mut founder_server = founder_server;
    founder_server.0.kill().unwrap();
    founder_server.0.wait().unwrap();
    let (founder_server, _) = start_with(founder, None, SEEDED);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(view) = ranges(founder)
            && view["epoch"] == 2
            && view["members"][0]["holder"] == node_a
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the restarted founder lost the map"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    drop(founder_server);
}

fn ranges(root: &Path) -> Option<Value> {
    let output = command(root, &["cluster", "replicas", "ranges", "list"]);
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .map(|value| value["result"]["ranges"].clone())
}
/// Every process reports the same settled map at `epoch` with its one
/// member held by `holder`.
fn converged(dirs: &[&Path], epoch: u64, holder: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(150);
    loop {
        let views: Vec<Option<Value>> = dirs.iter().map(|dir| ranges(dir)).collect();
        let done = views.iter().all(|view| {
            view.as_ref().is_some_and(|view| {
                view["epoch"] == epoch
                    && view["pending"].is_null()
                    && view["history"].as_array().is_some_and(Vec::is_empty)
                    && view["members"]
                        .as_array()
                        .is_some_and(|members| members.len() == 1)
                    && view["members"][0]["holder"] == holder
            })
        });
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the map did not settle at epoch {epoch} under node {holder}: {views:#?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}
fn kill(server: &mut Server) {
    let _ = server.0.kill();
    let _ = server.0.wait();
}
/// The process reached its configured cut and aborted.
fn wait_cut(server: &mut Server, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            assert!(!status.success(), "{what}: exited normally");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the cut was never reached"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
fn hex_of(value: &Value) -> String {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
/// One page of the operator's claims: object identities and the
/// continuation, in hexadecimal.
fn claims_page(root: &Path, cursor: Option<&str>) -> (Vec<String>, Option<String>) {
    let mut args = vec!["list", "claims", "--limit", "1"];
    if let Some(cursor) = cursor {
        args.extend(["--cursor", cursor]);
    }
    args.extend(["--format", "json"]);
    let page = success(root, &args);
    assert_eq!(page["condition"], "Listed", "{page}");
    let body = &page["result"]["page"];
    let ids = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| hex_of(&object["Claim"]["binding"]["object"]))
        .collect();
    let next = body["next"].as_array().map(|_| hex_of(&body["next"]));
    (ids, next)
}

/// Movement under faults across three real processes (25 §9): the founder
/// carries a crash cut at each step of the transfer it controls — before it
/// begins the move, records the seed, proposes the barrier, proposes a
/// holder's verified readiness or seal, activates, and cleans up — and the
/// transfer completes under the next leader from the committed record; a
/// duplicate move names the same transfer; a move to a dead destination
/// holds at the barrier with the member fenced until the destination
/// returns; a listing's continuation taken before a move stays valid after
/// it; and the directory publishes the holders at every settled epoch.
#[test]
fn movement_survives_a_cut_at_every_step_a_dead_destination_and_duplicate_requests() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b"]
        .iter()
        .map(|name| {
            tempfile::Builder::new()
                .prefix(&format!("focal-cuts-{name}-"))
                .tempdir_in("/tmp")
                .unwrap()
        })
        .collect();
    let roots: Vec<&Path> = dirs.iter().map(|dir| dir.path()).collect();
    let founder = roots[0];
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    let activation = success(founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let (mut founder_server, status) = start(founder, Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let founder_node = success(founder, &["identity"])["node"].as_u64().unwrap();
    let node_a = join(founder, roots[1], "host-a", &addresses[1]);
    let node_b = join(founder, roots[2], "host-b", &addresses[2]);
    let (mut server_a, _) = start(roots[1], None);
    let (server_b, _) = start(roots[2], None);
    let nodes = [founder_node, node_a, node_b];
    let view = wait_for(
        founder,
        "three hosts enrolled",
        Duration::from_secs(90),
        |view| {
            session(view).is_some_and(|session| session["pending"].is_null())
                && nodes.iter().all(|id| {
                    node(view, *id).is_some_and(|node| {
                        node["alive"] == true && node["disk_available"].is_number()
                    })
                })
        },
    );
    let registered = session(&view).unwrap().clone();
    let tenant = registered["tenant"].as_str().unwrap().to_owned();
    let ledger = registered["session"].as_str().unwrap().to_owned();
    let planned = success(
        founder,
        &[
            "cluster",
            "sessions",
            "plan",
            "--tenant",
            tenant.as_str(),
            "--session",
            ledger.as_str(),
            "--survive",
            "node",
            "--max-failures",
            "1",
        ],
    )["result"]
        .clone();
    assert_eq!(planned["kind"], "session_planned");
    wait_for(
        founder,
        "the three-voter placement activated",
        Duration::from_secs(240),
        |view| {
            session(view).is_some_and(|session| {
                session["pending"].is_null()
                    && session["route_epoch"] == 2
                    && session["achieved_max_failures"] == 1
            })
        },
    );
    // Three claims by the operator to an enrolled participant, so the
    // session has rows a listing pages.
    let client = tempfile::Builder::new()
        .prefix("focal-cuts-client-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(client.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let invitation = client.path().join("alice.invite");
    success(
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
    let enrolled = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", client.path().to_str().unwrap()])
        .args(["context", "enroll", "alice", "--invite-file"])
        .arg(&invitation)
        .output()
        .unwrap();
    assert!(
        enrolled.status.success(),
        "{}",
        String::from_utf8_lossy(&enrolled.stderr)
    );
    let enrolled: Value = serde_json::from_slice(&enrolled.stdout).unwrap();
    assert_eq!(enrolled["condition"], "Enrolled", "{enrolled}");
    let principal = hex_of(&enrolled["principal"]);
    let mut claims = Vec::new();
    for text in ["First.", "Second.", "Third."] {
        let document = serde_json::json!({
            "description": text,
            "target": principal,
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}
            ]
        });
        let submitted = success(
            founder,
            &[
                "submit",
                "claim",
                "--json",
                &document.to_string(),
                "--format",
                "json",
            ],
        );
        assert_eq!(submitted["condition"], "Committed", "{submitted}");
        let created = submitted["result"]["created"]
            .as_array()
            .unwrap()
            .iter()
            .find(|object| object["kind"] == "Claim")
            .unwrap_or_else(|| panic!("{submitted}"));
        claims.push(hex_of(&created["id"]));
    }
    claims.sort_unstable();
    let initial = ranges(founder).expect("ranges list");
    assert_eq!(initial["epoch"], 1, "{initial}");
    assert!(initial["members"][0]["holder"].is_null(), "{initial}");
    let member_of = |root: &Path| -> String {
        ranges(root).unwrap()["members"][0]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let move_args = |member: &str, target: u64| -> Vec<String> {
        [
            "cluster",
            "replicas",
            "ranges",
            "move",
            "--member",
            member,
            "--node",
            &target.to_string(),
        ]
        .iter()
        .map(|arg| (*arg).to_owned())
        .collect()
    };
    let run_move = |root: &Path, member: &str, target: u64| -> Output {
        let args = move_args(member, target);
        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        command(root, &borrowed)
    };
    // A cut at every step the founder's controller takes; the seal and the
    // cleanup need a replica-held source, which every move after the first
    // has.
    const SITES: [&str; 7] = [
        "movement-begin",
        "movement-seed",
        "movement-barrier",
        "movement-ready",
        "movement-seal",
        "movement-activate",
        "movement-cleanup",
    ];
    let mut epoch = 1;
    for (index, site) in SITES.iter().enumerate() {
        let target = if index % 2 == 0 { node_a } else { node_b };
        kill(&mut founder_server);
        let fault = format!("{site}:1");
        founder_server = start_with(founder, None, &[("FOCAL_FAULT", fault.as_str())]).0;
        let member = member_of(founder);
        let attempted = run_move(founder, &member, target);
        wait_cut(&mut founder_server, site);
        founder_server = start(founder, None).0;
        if *site == "movement-begin" {
            // The founder died before it began: the operator's command
            // failed, nothing moved, and the same request begins the move
            // once the restarted controller claims the session again.
            assert!(!attempted.status.success(), "{site}: the move was begun");
            let deadline = Instant::now() + Duration::from_secs(90);
            loop {
                if run_move(founder, &member, target).status.success() {
                    break;
                }
                assert!(Instant::now() < deadline, "{site}: the move never began");
                std::thread::sleep(Duration::from_millis(500));
            }
        }
        epoch += 1;
        converged(&roots, epoch, target, site);
    }
    // A duplicate request names the same transfer.
    let member = member_of(founder);
    let first = run_move(founder, &member, node_b);
    let second = run_move(founder, &member, node_b);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(first["result"]["kind"], "range_move_proposed", "{first}");
    assert_eq!(
        first["result"]["operation"], second["result"]["operation"],
        "{first} {second}"
    );
    epoch += 1;
    converged(&roots, epoch, node_b, "duplicate move");
    // A listing's continuation taken before a move stays valid after it: a
    // move changes the range epoch, not the route or the key order.
    let (first_page, cursor) = claims_page(founder, None);
    let cursor = cursor.expect("a continuation after one of three claims");
    // A move to a dead destination holds at the barrier: the seed and the
    // barrier commit, readiness never arrives, and a mutation on the fenced
    // member is refused until the destination returns and the transfer
    // completes.
    kill(&mut server_a);
    let member = member_of(founder);
    let begun = run_move(founder, &member, node_a);
    assert!(
        begun.status.success(),
        "{}",
        String::from_utf8_lossy(&begun.stderr)
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(view) = ranges(founder)
            && view["pending"]["barrier"].is_number()
        {
            break;
        }
        assert!(Instant::now() < deadline, "the barrier was never proposed");
        std::thread::sleep(Duration::from_millis(250));
    }
    std::thread::sleep(Duration::from_secs(6));
    let held = ranges(founder).unwrap();
    assert!(held["pending"]["barrier"].is_number(), "{held}");
    assert!(
        held["pending"]["ready"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "{held}"
    );
    let fenced = serde_json::json!({
        "description": "Fenced.",
        "target": principal,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}
        ]
    });
    let refused = command(
        founder,
        &[
            "submit",
            "claim",
            "--json",
            &fenced.to_string(),
            "--format",
            "json",
        ],
    );
    assert!(
        !refused.status.success(),
        "a mutation on the fenced member committed: {}",
        String::from_utf8_lossy(&refused.stdout)
    );
    server_a = start(roots[1], None).0;
    epoch += 1;
    converged(&roots, epoch, node_a, "dead destination returned");
    let mut listed = first_page;
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let (page, following) = claims_page(founder, Some(&cursor));
        listed.extend(page);
        next = following;
    }
    listed.sort_unstable();
    assert_eq!(
        listed, claims,
        "the continuation skipped or repeated a claim"
    );
    // After the fence lifts, the same mutation commits.
    let committed = success(
        founder,
        &[
            "submit",
            "claim",
            "--json",
            &fenced.to_string(),
            "--format",
            "json",
        ],
    );
    assert_eq!(committed["condition"], "Committed", "{committed}");
    // The directory publishes the holders at the settled epoch.
    let view = wait_for(
        founder,
        "the directory published the holders",
        Duration::from_secs(60),
        |view| {
            session(view).is_some_and(|session| {
                session["range_epoch"] == epoch
                    && session["holders"]
                        .as_array()
                        .is_some_and(|holders| holders.len() == 1 && holders[0]["node"] == node_a)
            })
        },
    );
    let published = session(&view).unwrap();
    assert_eq!(
        published["holders"][0]["member"],
        member_of(founder),
        "{published}"
    );
    drop(server_b);
    drop(server_a);
    drop(founder_server);
}
