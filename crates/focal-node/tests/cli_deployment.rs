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
//! Deployment plans through the real binary (doc 08 §9, DC05/DC06/DC16): a
//! laptop session with two enrolled hosts is planned for one tolerated node
//! loss. A dry run prints the plan and journals nothing; a plan that needs
//! more domains than exist names the blocked session and is refused by
//! `apply`; the written plan applies once (policy revision 2, the session
//! plan under way, then activated), resumes as complete, a plan built on an
//! older observation is refused as stale before any side effect, a
//! tampered plan and a plan from another deployment are refused, and the
//! founder restarts under its stronger committed policy with or without
//! the configuration file.
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
fn command(root: &Path, config: Option<&Path>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap()]);
    if let Some(config) = config {
        command.args(["--config", config.to_str().unwrap()]);
    }
    command.args(args).output().unwrap()
}
fn success(root: &Path, config: Option<&Path>, args: &[&str]) -> Value {
    let output = command(root, config, args);
    assert!(
        output.status.success(),
        "arguments {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stdout)))
}
/// The exit code and the diagnostic (`focal: [code] message`) of a refused
/// invocation.
fn failure(root: &Path, config: Option<&Path>, args: &[&str]) -> (i32, String) {
    let output = command(root, config, args);
    assert!(!output.status.success(), "arguments {args:?} succeeded");
    (
        output.status.code().unwrap(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn start(root: &Path, config: Option<&Path>, address: Option<&str>) -> (Server, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap()]);
    if let Some(config) = config {
        command.args(["--config", config.to_str().unwrap()]);
    }
    command.arg("start");
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
    let output = command(root, None, &["cluster", "placement"]);
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
        None,
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
        None,
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
/// The committed policy revision `deployment explain` reports; the report
/// carries it whether or not the local inventory satisfies the policy.
fn explain(root: &Path, config: Option<&Path>) -> Value {
    let output = command(root, config, &["deployment", "explain"]);
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{error}: {} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}
fn committed_revision(root: &Path, config: Option<&Path>) -> u64 {
    explain(root, config)["committed_revision"]
        .as_u64()
        .unwrap()
}
fn private_dir(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("focal-deploy-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[test]
fn a_deployment_plan_is_dry_run_written_applied_resumed_and_refused_when_stale_or_foreign() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b", "laptop", "files"]
        .iter()
        .map(|name| private_dir(name))
        .collect();
    let founder = dirs[0].path();
    let laptop = dirs[3].path();
    let files = dirs[4].path();
    let write = |name: &str, text: &str| {
        let path = files.join(name);
        std::fs::write(&path, text).unwrap();
        path
    };
    let topology = "version: 1\ntopology:\n  region: r1\n  zone: z1\n";
    let founder_config = write("founder.yaml", topology);
    let node_1 = write(
        "node-1.yaml",
        &format!("{topology}durability:\n  survive: node\n  max_failures: 1\n"),
    );
    let node_2 = write(
        "node-2.yaml",
        &format!("{topology}durability:\n  survive: node\n  max_failures: 2\n"),
    );
    let node_1_home = write(
        "node-1-home.yaml",
        &format!(
            "{topology}durability:\n  survive: node\n  max_failures: 1\nplacement:\n  home_regions: [r1]\n"
        ),
    );
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    let (founder_server, status) = start(founder, Some(&founder_config), Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let identity = success(founder, None, &["identity"]);
    let founder_node = identity["node"].as_u64().unwrap();
    let node_a = join(founder, dirs[1].path(), "host-a", &addresses[1]);
    let node_b = join(founder, dirs[2].path(), "host-b", &addresses[2]);
    let (_server_a, _) = start(dirs[1].path(), None, None);
    let (_server_b, _) = start(dirs[2].path(), None, None);
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
    assert_eq!(registered["route_epoch"], 1);
    assert_eq!(committed_revision(founder, None), 1);

    // Planning needs the requested configuration.
    let (code, report) = failure(founder, None, &["deployment", "plan", "--dry-run"]);
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[invalid_input]"), "{report}");

    // A dry run prints the plan: the policy commit first, then the session's
    // placement request, the guarantee before and after; nothing is created
    // and the directory journals nothing.
    let dry =
        success(founder, Some(&node_1), &["deployment", "plan", "--dry-run"])["result"].clone();
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["output"], Value::Null);
    assert_eq!(dry["empty"], false);
    let plan = &dry["plan"];
    assert_eq!(plan["kind"], "deployment_plan");
    assert_eq!(plan["deployment"]["node"], founder_node);
    assert_eq!(plan["observed"]["policy_revision"], 1);
    assert_eq!(plan["requested"]["durability"]["max_failures"], 1);
    assert_eq!(plan["blocked"], serde_json::json!([]));
    let changes = plan["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 2, "{plan}");
    assert_eq!(changes[0]["change"], "commit_policy");
    assert_eq!(changes[0]["from_revision"], 1);
    assert_eq!(changes[0]["to_revision"], 2);
    assert_eq!(changes[1]["change"], "plan_session");
    assert_eq!(changes[1]["tenant"], tenant);
    assert_eq!(changes[1]["session"], ledger);
    assert_eq!(changes[1]["pending"], false);
    assert_eq!(changes[1]["expected_route_epoch"], 1);
    let mut voters: Vec<u64> = changes[1]["voters"]
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
    let operation = changes[1]["operation"].as_str().unwrap().to_owned();
    assert_eq!(plan["guarantee"]["before"]["max_failures"], 0);
    assert_eq!(plan["guarantee"]["during"]["max_failures"], 0);
    assert_eq!(plan["guarantee"]["after"]["max_failures"], 1);
    assert_eq!(plan["guarantee"]["after"]["survive"], "node");
    let plan_id = plan["plan_id"].as_str().unwrap().to_owned();
    assert!(!founder.join("cluster/apply").exists());
    assert_eq!(committed_revision(founder, None), 1);
    std::thread::sleep(Duration::from_secs(2));
    let after_dry = session(&placement(founder).unwrap()).unwrap().clone();
    assert_eq!(after_dry["pending"], Value::Null, "{after_dry}");
    assert_eq!(after_dry["route_epoch"], 1);
    assert_eq!(after_dry["max_failures"], 0);
    let status = success(founder, None, &["deployment", "status"])["result"].clone();
    assert_eq!(status["plans"], serde_json::json!([]));

    // Too few independent domains: the session is blocked, the guarantee
    // after the plan is the guarantee before it, and apply refuses it.
    let blocked_file = files.join("blocked.plan");
    let blocked = success(
        founder,
        Some(&node_2),
        &[
            "deployment",
            "plan",
            "--output",
            blocked_file.to_str().unwrap(),
        ],
    )["result"]
        .clone();
    assert_eq!(
        blocked["plan"]["blocked"].as_array().unwrap().len(),
        1,
        "{blocked}"
    );
    assert_eq!(blocked["plan"]["blocked"][0]["session"], ledger);
    assert_eq!(blocked["plan"]["guarantee"]["after"]["max_failures"], 0);
    assert!(blocked_file.is_file());
    let (code, report) = failure(
        founder,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            blocked_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 6, "{report}");
    assert!(report.contains("[guarantee_unsatisfied]"), "{report}");
    assert_eq!(committed_revision(founder, None), 1);
    assert!(!founder.join("cluster/apply").exists());

    // The written plan is the dry run's plan: identity from facts alone.
    let plan_file = files.join("node-1.plan");
    let written = success(
        founder,
        Some(&node_1),
        &[
            "deployment",
            "plan",
            "--output",
            plan_file.to_str().unwrap(),
        ],
    )["result"]
        .clone();
    assert_eq!(written["plan"]["plan_id"], plan_id, "{written}");
    assert_eq!(written["output"], plan_file.to_str().unwrap());
    assert_eq!(written["dry_run"], false);
    let bytes = std::fs::read(&plan_file).unwrap();
    assert_eq!(&bytes[..8], b"FCLPLAN1");
    // Plans are immutable: the same path is never overwritten.
    let (code, _) = failure(
        founder,
        Some(&node_1),
        &[
            "deployment",
            "plan",
            "--output",
            plan_file.to_str().unwrap(),
        ],
    );
    assert_ne!(code, 0);
    assert_eq!(std::fs::read(&plan_file).unwrap(), bytes);
    // A second plan from the same observation with another request.
    let later_file = files.join("node-1-home.plan");
    let later = success(
        founder,
        Some(&node_1_home),
        &[
            "deployment",
            "plan",
            "--output",
            later_file.to_str().unwrap(),
        ],
    )["result"]
        .clone();
    assert_ne!(later["plan"]["plan_id"], plan_id);
    assert_eq!(later["plan"]["observed"]["policy_revision"], 1);
    let later_id = later["plan"]["plan_id"].as_str().unwrap().to_owned();

    // A tampered plan is refused before anything is read from the node.
    let tampered_file = files.join("tampered.plan");
    let mut tampered = bytes.clone();
    tampered[40] ^= 0x01;
    std::fs::write(&tampered_file, &tampered).unwrap();
    let (code, report) = failure(
        founder,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            tampered_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[plan_corrupt]"), "{report}");

    // A plan from another deployment: a laptop node commits its own policy.
    let laptop_config = write("laptop.yaml", topology);
    let laptop_home = write(
        "laptop-home.yaml",
        &format!("{topology}placement:\n  home_regions: [r1]\n"),
    );
    let (laptop_server, _) = start(laptop, Some(&laptop_config), None);
    let laptop_plan_file = files.join("laptop.plan");
    let laptop_plan = success(
        laptop,
        Some(&laptop_home),
        &[
            "deployment",
            "plan",
            "--output",
            laptop_plan_file.to_str().unwrap(),
        ],
    )["result"]
        .clone();
    assert_eq!(
        laptop_plan["plan"]["changes"].as_array().unwrap().len(),
        1,
        "{laptop_plan}"
    );
    assert_eq!(laptop_plan["plan"]["changes"][0]["change"], "commit_policy");
    let (code, report) = failure(
        founder,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            laptop_plan_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[wrong_deployment]"), "{report}");
    let applied = success(
        laptop,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            laptop_plan_file.to_str().unwrap(),
        ],
    )["result"]
        .clone();
    assert_eq!(applied["outcome"], "Complete", "{applied}");
    assert_eq!(committed_revision(laptop, Some(&laptop_config)), 2);
    drop(laptop_server);
    // The laptop restarts under its committed policy with the file that
    // states it; without the file the node lacks the topology fact its
    // committed home region needs, and the old file is refused by name.
    let (laptop_server, _) = start(laptop, Some(&laptop_home), None);
    drop(laptop_server);
    let (code, report) = failure(laptop, None, &["demo"]);
    assert_eq!(code, 1, "{report}");
    assert!(report.contains("no ordering home"), "{report}");
    let old_laptop = write(
        "laptop-old.yaml",
        &format!("{topology}placement:\n  home_regions: []\n"),
    );
    let (code, report) = failure(laptop, Some(&old_laptop), &["identity"]);
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[committed_policy]"), "{report}");

    // Apply the fleet plan: the policy commits, the session's placement
    // request is journaled, and waiting sees it activated.
    let applied = success(
        founder,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
            "--wait",
            "180",
        ],
    )["result"]
        .clone();
    assert_eq!(applied["kind"], "deployment_applied");
    assert_eq!(applied["plan_id"], plan_id);
    assert_eq!(applied["outcome"], "Complete", "{applied}");
    let steps = applied["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["phase"], "Complete");
    assert_eq!(steps[1]["phase"], "Complete");
    assert_eq!(steps[1]["operation"], operation);
    assert_eq!(committed_revision(founder, None), 2);
    let journal = founder.join("cluster/apply").join(&plan_id);
    assert!(journal.join("JOURNAL").is_file());
    assert_eq!(std::fs::read(journal.join("PLAN")).unwrap(), bytes);
    let active = session(&placement(founder).unwrap()).unwrap().clone();
    assert_eq!(active["pending"], Value::Null, "{active}");
    assert_eq!(active["route_epoch"], 2);
    assert_eq!(active["max_failures"], 1);
    assert_eq!(active["achieved_max_failures"], 1);
    // A repeated apply resumes as complete without sending anything.
    let again = success(
        founder,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
        ],
    )["result"]
        .clone();
    assert_eq!(again["outcome"], "Complete");
    assert_eq!(committed_revision(founder, None), 2);
    let status = success(founder, None, &["deployment", "status"])["result"].clone();
    assert_eq!(status["plans"].as_array().unwrap().len(), 1);
    assert_eq!(status["plans"][0]["kind"], "deployment_status");
    assert_eq!(status["plans"][0]["plan_id"], plan_id);
    assert_eq!(status["plans"][0]["outcome"], "Complete");
    let one = success(
        founder,
        None,
        &["deployment", "status", "--plan", plan_id.as_str()],
    )["result"]
        .clone();
    assert_eq!(one["plans"][0]["plan_id"], plan_id);
    // The plan built on the earlier observation is stale: refused before
    // any side effect, and nothing of it is journaled.
    let (code, report) = failure(
        founder,
        None,
        &[
            "deployment",
            "apply",
            "--plan-file",
            later_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[stale_plan]"), "{report}");
    assert!(!founder.join("cluster/apply").join(&later_id).exists());
    assert_eq!(committed_revision(founder, None), 2);
    // The same request now plans nothing: satisfied at the committed policy.
    let settled =
        success(founder, Some(&node_1), &["deployment", "plan", "--dry-run"])["result"].clone();
    assert_eq!(settled["empty"], true, "{settled}");
    assert_eq!(settled["plan"]["observed"]["policy_revision"], 2);
    assert_eq!(settled["plan"]["guarantee"]["before"]["max_failures"], 1);

    // The founder restarts under the stronger committed policy: with the
    // file that requested it, and with none (omitted fields are committed
    // values); the old file is refused by name.
    drop(founder_server);
    let (founder_server, status) = start(founder, Some(&node_1), Some(&addresses[0]));
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    drop(founder_server);
    let (_founder_server, status) = start(founder, None, Some(&addresses[0]));
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    // The original file omitted durability, so it still starts the node
    // (omitted fields are the committed values); one that states the old
    // value is refused by name.
    let identity_again = success(founder, Some(&founder_config), &["identity"]);
    assert_eq!(identity_again["node"], founder_node);
    let old_founder = write(
        "founder-old.yaml",
        &format!("{topology}durability:\n  survive: node\n  max_failures: 0\n"),
    );
    let (code, report) = failure(founder, Some(&old_founder), &["identity"]);
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[committed_policy]"), "{report}");
    assert!(report.contains("durability.max_failures"), "{report}");
    let explained = explain(founder, None);
    assert_eq!(
        explained["condition"], "GuaranteeUnsatisfied",
        "one local node"
    );
    assert_eq!(explained["committed_revision"], 2);
    assert_eq!(explained["effective"]["durability"]["max_failures"], 1);
    assert_eq!(explained["requested"]["durability"]["max_failures"], 1);
}
