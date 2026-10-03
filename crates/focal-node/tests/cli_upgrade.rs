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
//! The upgrade fence (24 §21): every node reports its binary's capability
//! level; the founder raises the fence only once every node reports the
//! level, never lowers it, and a binary announcing less than the committed
//! fence refuses to serve.
use serde_json::Value;
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
#[path = "support/deadline.rs"]
mod deadline;
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn private_dir(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("focal-upgrade-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}
fn admin(root: &Path, args: &[&str]) -> Value {
    let output = run(root, args);
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
fn failure(root: &Path, args: &[&str]) -> (i32, String) {
    let output = run(root, args);
    assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}
fn spawn(
    root: &Path,
    address: Option<&str>,
    announced: Option<&str>,
) -> (Child, mpsc::Receiver<Value>) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap(), "start"]);
    if let Some(address) = address {
        command.args(["--advertise", address]);
    }
    if let Some(level) = announced {
        command.env("FOCAL_CAPABILITY_LEVEL", level);
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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
    (child, receive)
}
fn start(root: &Path, address: Option<&str>) -> (Server, Value) {
    start_announcing(root, address, None)
}
/// Start a node announcing `announced` (a staged rollout, 24 §21), or the
/// binary's own level.
fn start_announcing(
    root: &Path,
    address: Option<&str>,
    announced: Option<&str>,
) -> (Server, Value) {
    deadline::observe(root);
    let (child, receive) = spawn(root, address, announced);
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
fn join(founder: &Path, host: &Path, name: &str, advertise: &str) -> u64 {
    let invitation = founder.join(format!("{name}.invite"));
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
fn upgrade(root: &Path) -> Value {
    let status = admin(root, &["cluster", "upgrade", "status"]);
    assert_eq!(status["result"]["kind"], "upgrade", "{status}");
    status["result"]["upgrade"].clone()
}
fn wait_for_upgrade(root: &Path, what: &str, condition: impl Fn(&Value) -> bool) -> Value {
    let mut deadline = deadline::Deadline::after(Duration::from_secs(90));
    let mut last = None;
    while deadline.open() {
        let output = run(root, &["cluster", "upgrade", "status"]);
        if output.status.success()
            && let Ok(value) = serde_json::from_slice::<Value>(&output.stdout)
        {
            let view = value["result"]["upgrade"].clone();
            if condition(&view) {
                return view;
            }
            last = Some(view);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    // What the node knows of itself and the fleet when the level it waited
    // for never came: a level rides the placement agent's load report (an
    // ubuntu run showed the host at the level before for 90 s, 2026-10-03).
    let health = run(root, &["cluster", "node", "health"]);
    let placement = run(root, &["cluster", "placement"]);
    panic!(
        "{what} did not happen; last: {last:#?}\nhealth: {}\nplacement: {}",
        String::from_utf8_lossy(&health.stdout),
        String::from_utf8_lossy(&placement.stdout)
    );
}
fn capability(view: &Value, node: u64) -> u64 {
    view["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["node"] == node)
        .map(|entry| entry["capability"].as_u64().unwrap())
        .unwrap_or(u64::MAX)
}

#[test]
fn the_fence_rises_only_once_every_node_reports_the_level_and_a_lower_binary_refuses_to_serve() {
    let dirs: Vec<_> = ["founder", "host"]
        .iter()
        .map(|name| private_dir(name))
        .collect();
    let founder = dirs[0].path();
    let host = dirs[1].path();
    let addresses: Vec<String> = (0..2).map(|_| address()).collect();
    // A rollout rehearsed from the level before this binary's: the cluster
    // is founded by a founder announcing it, and holds the fence at that
    // level from genesis (24 §21) — never at a level its founder would
    // refuse to serve under.
    let compiled = u64::from(focal_node::upgrade::CAPABILITY_LEVEL);
    assert!(
        compiled >= 2,
        "this journey rehearses a rollout to the binary's level"
    );
    let before = compiled - 1;
    let before_text = before.to_string();
    let compiled_text = compiled.to_string();
    let above_text = (compiled + 1).to_string();
    let (_founder_server, status) =
        start_announcing(founder, Some(&addresses[0]), Some(&before_text));
    assert_eq!(status["condition"], "Ready");
    let founder_node = admin(founder, &["identity"])["node"].as_u64().unwrap();
    let node = join(founder, host, "host", &addresses[1]);
    let host_server = start_announcing(host, None, Some(&before_text)).0;
    // Both nodes report the level they announce; the fence stands at the
    // founder's level from genesis.
    let view = wait_for_upgrade(founder, "both nodes reporting", |view| {
        capability(view, founder_node) == before && capability(view, node) == before
    });
    assert_eq!(view["fence_level"], before, "{view}");
    assert!(view["fence_activated_at"].as_i64().unwrap() > 0, "{view}");
    assert_eq!(view["fence_revision"], 1, "{view}");
    assert_eq!(view["binary_level"], compiled);
    assert_eq!(view["announced_level"], before);
    assert_eq!(view["activatable"], before);
    assert_eq!(view["nodes"].as_array().unwrap().len(), 2);
    assert!(view["registry_revision"].as_u64().unwrap() > 0);
    // A host reads the same fence; only the founder raises it.
    let host_view = wait_for_upgrade(host, "host view", |view| {
        capability(view, founder_node) == before && capability(view, node) == before
    });
    assert_eq!(host_view["fence_level"], before, "{host_view}");
    // A level no node supports is refused by name, naming every node —
    // read from any node, before the founder's authority is even asked.
    for root in [host, founder] {
        let (code, report) = failure(
            root,
            &["cluster", "upgrade", "activate", "--fence", &compiled_text],
        );
        assert_eq!(code, 5, "{report}");
        assert!(report.contains("[members_behind]"), "{report}");
        assert!(
            report.contains(&founder_node.to_string()) && report.contains(&node.to_string()),
            "{report}"
        );
    }
    assert_eq!(upgrade(founder)["fence_level"], before);
    // Zero is invalid input; the fence's own level reads as done.
    let (code, _) = failure(founder, &["cluster", "upgrade", "activate", "--fence", "0"]);
    assert_eq!(code, 2);
    let standing = admin(
        founder,
        &["cluster", "upgrade", "activate", "--fence", &before_text],
    );
    assert_eq!(standing["result"]["changed"], false, "{standing}");
    // The rollout: both binaries come back announcing their own level.
    drop(host_server);
    drop(_founder_server);
    let (_founder_server, _) = start(founder, Some(&addresses[0]));
    let host_server = start(host, None).0;
    wait_for_upgrade(founder, "both nodes at the binary's level", |view| {
        capability(view, founder_node) == compiled && capability(view, node) == compiled
    });
    // A level above the binary's is still refused by name.
    let (code, report) = failure(
        founder,
        &["cluster", "upgrade", "activate", "--fence", &above_text],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[members_behind]"), "{report}");
    // Only the founder raises the fence: a host asking for the level every
    // node now supports is refused at the founder's authority. The host
    // reads the levels from its own view of the root, which must hold the
    // fact first.
    wait_for_upgrade(host, "the host sees both at the binary's level", |view| {
        capability(view, founder_node) == compiled && capability(view, node) == compiled
    });
    let (code, report) = failure(
        host,
        &["cluster", "upgrade", "activate", "--fence", &compiled_text],
    );
    assert_eq!(code, 3, "{report}");
    // The fence rises to the level every node supports, exactly once.
    let activated = admin(
        founder,
        &["cluster", "upgrade", "activate", "--fence", &compiled_text],
    );
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    assert_eq!(activated["result"]["changed"], true);
    let fence = activated["result"]["upgrade"].clone();
    assert_eq!(fence["fence_level"], compiled, "{fence}");
    assert!(fence["fence_activated_at"].as_i64().unwrap() > 0);
    assert!(fence["fence_revision"].as_u64().unwrap() > 1);
    let again = admin(
        founder,
        &["cluster", "upgrade", "activate", "--fence", &compiled_text],
    );
    assert_eq!(again["result"]["changed"], false, "{again}");
    assert_eq!(
        again["result"]["upgrade"]["fence_revision"],
        fence["fence_revision"]
    );
    // A fence never lowers: refused as invalid input.
    let (code, report) = failure(
        founder,
        &["cluster", "upgrade", "activate", "--fence", &before_text],
    );
    assert_eq!(code, 2, "{report}");
    // The host observes the committed fence.
    wait_for_upgrade(host, "host sees the fence", |view| {
        view["fence_level"] == compiled
    });
    // A binary announcing less than the fence refuses to start: the host
    // is restarted announcing the level before and exits with
    // `upgrade_fenced`.
    drop(host_server);
    let (mut child, receive) = spawn(host, None, Some(&before_text));
    let exit = child.wait_timeout_or_kill(Duration::from_secs(60));
    let stderr = {
        let mut text = String::new();
        if let Some(mut err) = child.stderr.take() {
            let _ = std::io::Read::read_to_string(&mut err, &mut text);
        }
        text
    };
    assert!(!exit.success(), "a fenced binary started: {stderr}");
    assert_eq!(exit.code(), Some(5), "{stderr}");
    assert!(stderr.contains("[upgrade_fenced]"), "{stderr}");
    assert!(
        receive.try_recv().is_err(),
        "a fenced binary published readiness"
    );
    // A binary at the fence's level serves again and announces itself.
    let (server, status) = start(host, None);
    assert!(matches!(
        status["condition"].as_str(),
        Some("Ready" | "CatchingUp")
    ));
    let view = wait_for_upgrade(host, "host serving under the fence", |view| {
        view["fence_level"] == compiled && view["announced_level"] == compiled
    });
    assert_eq!(view["binary_level"], compiled, "{view}");
    // The announced level can be lowered to the fence, never raised past
    // the binary: a rehearsal at the fence's own level still serves.
    drop(server);
    let (child, receive) = spawn(host, None, Some(&compiled_text));
    let server = Server(child);
    let status = receive
        .recv_timeout(Duration::from_secs(30))
        .expect("a binary at the fence's level did not start");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    let view = wait_for_upgrade(host, "announced at the fence", |view| {
        view["announced_level"] == compiled
    });
    assert_eq!(view["binary_level"], compiled);
    drop(server);
}
trait WaitTimeout {
    fn wait_timeout_or_kill(&mut self, timeout: Duration) -> std::process::ExitStatus;
}
impl WaitTimeout for Child {
    fn wait_timeout_or_kill(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let mut deadline = deadline::Deadline::after(timeout);
        loop {
            if let Some(status) = self.try_wait().unwrap() {
                return status;
            }
            if !deadline.open() {
                let _ = self.kill();
                return self.wait().unwrap();
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
