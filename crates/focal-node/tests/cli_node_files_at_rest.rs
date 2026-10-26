#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! A node's small files written in the clear and converted by the real binary at its start
//! (29 §12.10). A node founded below the at-rest level writes its identity, policy, network state
//! and founded allowance as every earlier binary wrote them, byte for byte, and keeps
//! the plan it applies and its journal so too; the fence rises to the at-rest level and the node
//! records it; started again, the node converts every one of them before it serves, marks its data
//! directory, reads them back (its identity, its committed policy, its deployment's plan and
//! journal), and serves. A file planted in the clear after the mark is refused by name, and the
//! sealed file put back is read again. Unix-only, as the other real-binary at-rest tests: the data
//! directory is kept private by its mode bits.
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::Duration,
};

#[path = "support/deadline.rs"]
mod deadline;
use deadline::Deadline;
use deadline::progress;
#[path = "support/ports.rs"]
mod ports;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The node files a networked founder writes, each with what an earlier binary began it with in the
/// clear. (The root's genesis, `cluster/BOOTSTRAP`, is an embedded root's enrollment's, which the
/// binary does not found with: its conversion is `node_files`'s test's.)
const FILES: [(&str, &[u8]); 4] = [
    ("IDENTITY", b"FOCALND1"),
    ("POLICY", b"FCLPOL2\0"),
    ("NETWORK", b"FCLNET01"),
    ("SESSION.memory", b""),
];

/// What a sealed node file begins with, in the clear (`focal_node::node_files::SEALED_MAGIC`).
const SEALED: &[u8] = b"FCLNSEAL";

/// The node's topology, which a home region needs, and the policy the node's deployment commits:
/// its home in that region.
const TOPOLOGY: &str = "version: 1\ntopology:\n  region: r1\n  zone: z1\n";
const HOME: &str = "placement:\n  home_regions: [r1]\n";

/// The binary with the data directory `root` and the configuration file `config`.
fn focal(root: &Path, config: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args([
        "--data-dir",
        root.to_str().unwrap(),
        "--config",
        config.to_str().unwrap(),
    ]);
    command
}

/// Start the node on `root` under `config`, announcing `announced` (a staged rollout, 24 §21) or
/// the binary's own level, and wait for its readiness. What it prints after is read and let go.
fn start(root: &Path, config: &Path, advertise: &str, announced: Option<&str>) -> Server {
    progress::admit();
    deadline::observe(root);
    let mut command = focal(root, config);
    command.args(["start", "node", "--advertise", advertise]);
    if let Some(level) = announced {
        command.env("FOCAL_CAPABILITY_LEVEL", level);
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        let mut reported = false;
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            if reported {
                continue;
            }
            text.push_str(&line);
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                reported = true;
            }
        }
    });
    let mut server = Server(child);
    let status = progress::reported(&receive, &mut server.0, root, Duration::from_secs(30))
        .expect("the node did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    server
}

fn run(root: &Path, config: &Path, args: &[&str]) -> Output {
    focal(root, config).args(args).output().unwrap()
}

fn admin(root: &Path, config: &Path, args: &[&str]) -> Value {
    let output = run(root, config, args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The node's identity as `focal inspect identity` prints it.
fn identity(root: &Path, config: &Path) -> Value {
    admin(root, config, &["inspect", "identity"])
}

/// The committed policy as `explain deployment` reads it from the node's `POLICY`: its revision and
/// the guarantee in effect. The report is printed whether or not this machine's inventory satisfies
/// the policy, so its exit status is not the test's.
fn policy(root: &Path, config: &Path) -> (Value, Value) {
    let output = run(root, config, &["explain", "deployment"]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{error}: {} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (
        report["committed_revision"].clone(),
        report["effective"].clone(),
    )
}

/// The deployment's plans as `inspect deployment` reads them from the node's plans and journals.
fn deployments(root: &Path, config: &Path) -> Value {
    admin(root, config, &["inspect", "deployment"])["result"]["plans"].clone()
}

/// Do: found a node below the at-rest level and start it; plan and apply a deployment that commits
/// its home region; stop it and read its node files; raise the fence to the at-rest level once the
/// node, restarted at the binary's level, reports it; once the node has recorded the fence, start
/// it again; then plant a file in the clear. Expect: below the fence every node file, the applied
/// plan and its journal among them, begins as every earlier binary began it and no mark is made;
/// after the restart at the level every one is sealed, nothing of its clear form left, the
/// directory marked, the node's identity, committed policy and deployment what they were and the
/// node serving; the planted file refused by name, and read again once its sealed form is back.
#[test]
fn a_nodes_small_files_in_the_clear_convert_at_its_start_once_the_fence_opens_the_level() {
    let directory = tempfile::Builder::new()
        .prefix("focal-node-files-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = directory.path();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let configs = tempfile::Builder::new()
        .prefix("focal-node-files-config-")
        .tempdir_in("/tmp")
        .unwrap();
    let topology = configs.path().join("topology.yaml");
    std::fs::write(&topology, TOPOLOGY).unwrap();
    let home = configs.path().join("home.yaml");
    std::fs::write(&home, format!("{TOPOLOGY}{HOME}")).unwrap();
    let advertise = ports::address();
    let at_rest = focal_node::upgrade::AT_REST_LEVEL;
    assert!(
        (2..=focal_node::upgrade::CAPABILITY_LEVEL).contains(&at_rest),
        "this journey rehearses the rollout that opens the at-rest level"
    );
    let below = (at_rest - 1).to_string();
    let level = at_rest.to_string();

    // Founded below the at-rest level: its node files are written in the clear.
    let activation = focal(root, &topology)
        .args(["activate", "native"])
        .env("FOCAL_CAPABILITY_LEVEL", &below)
        .output()
        .unwrap();
    assert!(activation.status.success(), "{activation:?}");
    let server = start(root, &topology, &advertise, Some(&below));
    let founded = identity(root, &topology);
    assert_eq!(
        policy(root, &topology).0,
        1,
        "the founded policy's revision"
    );

    // Its deployment commits its home region: the plan and its journal are kept in the clear too.
    let plan_file = configs.path().join("home.plan");
    let mut deadline = Deadline::after(Duration::from_secs(60));
    let planned = loop {
        let output = run(
            root,
            &home,
            &[
                "plan",
                "deployment",
                "--output",
                plan_file.to_str().unwrap(),
            ],
        );
        if output.status.success() {
            break serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        // The directory reports a node it has heard from: until then a plan is refused as not
        // observed, and asked again.
        let said = String::from_utf8_lossy(&output.stderr).to_string();
        assert!(
            said.contains("not_observed") && deadline.open(),
            "the plan was refused: {said}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(
        planned["plan"]["changes"][0]["change"], "commit_policy",
        "{planned}"
    );
    let plan_id = planned["plan"]["plan_id"].as_str().unwrap().to_owned();
    let applied = admin(
        root,
        &topology,
        &[
            "apply",
            "deployment",
            "--plan-file",
            plan_file.to_str().unwrap(),
        ],
    );
    assert_eq!(applied["result"]["outcome"], "Complete", "{applied}");
    let committed = policy(root, &home);
    assert_eq!(
        committed.0, 2,
        "the deployment's policy revision: {committed:?}"
    );
    let deployed = deployments(root, &home);
    assert_eq!(deployed[0]["plan_id"], plan_id.as_str(), "{deployed}");
    assert_eq!(deployed[0]["outcome"], "Complete", "{deployed}");
    drop(server);

    let plan_record = format!("cluster/apply/{plan_id}/PLAN");
    let journal_record = format!("cluster/apply/{plan_id}/JOURNAL");
    let kept: Vec<(String, &[u8])> = FILES
        .iter()
        .map(|(name, magic)| ((*name).to_owned(), *magic))
        .chain([
            (plan_record, b"FCLPLAN1".as_slice()),
            (journal_record, b"FCLAPLY1".as_slice()),
        ])
        .collect();
    let mut clear = Vec::new();
    for (name, magic) in &kept {
        let bytes = std::fs::read(root.join(name))
            .unwrap_or_else(|error| panic!("{name} was not written: {error}"));
        assert!(bytes.starts_with(magic), "{name} is not in its clear form");
        assert!(!bytes.starts_with(SEALED), "{name} sealed below the level");
        clear.push((name.clone(), bytes));
    }
    assert!(
        !root.join("FILES.SEALED").exists(),
        "marked below the level"
    );

    // At the binary's level the fence rises; the files stay as they are until the node starts
    // again having recorded the fence.
    let server = start(root, &home, &advertise, None);
    let mut deadline = Deadline::after(Duration::from_secs(90));
    loop {
        let output = run(root, &home, &["inspect", "upgrade"]);
        if output.status.success()
            && let Ok(value) = serde_json::from_slice::<Value>(&output.stdout)
            && let Some(nodes) = value["result"]["upgrade"]["nodes"].as_array()
            && !nodes.is_empty()
            && nodes
                .iter()
                .all(|node| node["capability"].as_u64() >= Some(u64::from(at_rest)))
        {
            break;
        }
        assert!(deadline.open(), "the node never reported the at-rest level");
        std::thread::sleep(Duration::from_millis(200));
    }
    let activated = admin(root, &home, &["activate", "upgrade", "--fence", &level]);
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    let mut deadline = Deadline::after(Duration::from_secs(60));
    while focal_node::storage_level::known(root).unwrap() < Some(at_rest) {
        assert!(deadline.open(), "the node never recorded the fence");
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(server);

    // Started at the level: every node file is converted before the node serves.
    let server = start(root, &home, &advertise, None);
    assert!(
        root.join("FILES.SEALED").is_file(),
        "the directory was not marked"
    );
    for (name, before) in &clear {
        let bytes = std::fs::read(root.join(name)).unwrap();
        assert!(bytes.starts_with(SEALED), "{name} was not sealed");
        if before.len() >= 8 {
            assert!(
                !bytes
                    .windows(before.len().min(32))
                    .any(|window| window == &before[..window.len()]),
                "{name} holds its clear form"
            );
        }
    }
    assert_eq!(
        identity(root, &home),
        founded,
        "the identity read back is not the one founded"
    );
    assert_eq!(
        policy(root, &home),
        committed,
        "the policy read back is not the one committed"
    );
    assert_eq!(
        deployments(root, &home),
        deployed,
        "the deployment read back is not the one applied"
    );
    let health = admin(root, &home, &["inspect", "node", "--health"]);
    assert!(health.is_object(), "{health}");
    drop(server);

    // A file planted in the clear after the mark is refused by name; its sealed form put back is
    // read again.
    let (name, clear_bytes) = clear.iter().find(|(name, _)| name == "IDENTITY").unwrap();
    let path = root.join(name);
    let sealed = std::fs::read(&path).unwrap();
    std::fs::write(&path, clear_bytes).unwrap();
    let refused = run(root, &home, &["inspect", "identity"]);
    assert!(
        !refused.status.success(),
        "a clear identity was read after the mark"
    );
    let said = String::from_utf8_lossy(&refused.stderr).to_string()
        + &String::from_utf8_lossy(&refused.stdout);
    assert!(
        said.contains("in the clear") && said.contains("IDENTITY"),
        "the refusal does not name the file: {said}"
    );
    std::fs::write(&path, &sealed).unwrap();
    assert_eq!(identity(root, &home), founded);
}
