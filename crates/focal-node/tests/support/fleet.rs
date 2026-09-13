//! A fleet of real `focal` processes for runbook and journey tests: nodes
//! with their own directories and configurations, starts that wait for the
//! readiness record, invitations through a pipe, one-command joins, pauses
//! and kills, the founder's placement view, and one participant workflow
//! that writes a claim with an artifact.
#![allow(dead_code)]
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

#[path = "ports.rs"]
mod ports;
pub fn address() -> String {
    ports::address()
}
pub const FAR: u64 = 4_102_444_800_000;
pub const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;

pub struct Node {
    pub dir: tempfile::TempDir,
    pub config: Option<PathBuf>,
}
impl Node {
    pub fn new(name: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("focal-{name}-"))
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { dir, config: None }
    }
    /// A node with a configuration file (topology, durability, placement).
    pub fn with_config(name: &str, yaml: &str) -> Self {
        let mut node = Self::new(name);
        let path = node.dir.path().join("focal.yaml");
        std::fs::write(&path, yaml).unwrap();
        node.config = Some(path);
        node
    }
    pub fn root(&self) -> &Path {
        self.dir.path()
    }
}
pub struct Server(pub Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Server {
    pub fn pid(&self) -> u32 {
        self.0.id()
    }
    /// Pause the process (`SIGSTOP`): alive to the OS, silent to its peers.
    pub fn pause(&self) {
        signal(self.pid(), "STOP");
    }
    pub fn resume(&self) {
        signal(self.pid(), "CONT");
    }
}
fn signal(pid: u32, name: &str) {
    let status = Command::new("kill")
        .args([&format!("-{name}"), &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -{name} {pid}");
}
fn base(node: &Node, context: Option<&str>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    if let Some(config) = &node.config {
        command.args(["--config", config.to_str().unwrap()]);
    }
    command.args(["--data-dir", node.root().to_str().unwrap()]);
    if let Some(context) = context {
        command.args(["--client-context", context]);
    }
    command
}
pub fn run(node: &Node, context: Option<&str>, args: &[&str]) -> Output {
    base(node, context).args(args).output().unwrap()
}
/// Run without injecting the node's own `--config`: for `deployment
/// plan|explain --config POLICY`, which carries its own configuration and
/// would clash with the node's.
pub fn run_bare(node: &Node, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", node.root().to_str().unwrap()]);
    command.args(args).output().unwrap()
}
/// Run a command, riding out the retryable exit-6 (`unavailable` or `capacity`)
/// that a cluster reconfiguration (a range move, a voter change, a leader
/// election) briefly returns while the authoritative service settles. The CLI's
/// own retry rides out most of it; this harness resends a few more times so no
/// test is flaked by a moment of reconfiguration. A definite error (bad input,
/// not found) is returned at once. Both operator (`admin`) and client (`cli`)
/// commands share this: an operator command issued mid-reconfiguration (e.g. a
/// `move` during a prior transfer) is as subject to the transient as a workload.
fn run_riding_out(node: &Node, context: Option<&str>, args: &[&str]) -> Output {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = run(node, context, args);
        if output.status.success()
            || output.status.code() != Some(6)
            || std::time::Instant::now() >= deadline
        {
            break output;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}
pub fn admin(node: &Node, args: &[&str]) -> Value {
    let output = run_riding_out(node, None, args);
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
pub fn cli(node: &Node, context: Option<&str>, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let output = run_riding_out(node, context, &args);
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
pub fn failure(node: &Node, context: Option<&str>, args: &[&str]) -> (i32, String) {
    let output = run(node, context, args);
    assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}
/// Spawn `start` with extra arguments and environment; the receiver yields
/// every JSON record the process prints that carries a `condition`.
pub fn spawn(node: &Node, args: &[&str], envs: &[(&str, &str)]) -> (Child, mpsc::Receiver<Value>) {
    let mut command = base(node, None);
    command.arg("start").args(args);
    command.envs(envs.iter().copied());
    let mut child = command
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
                text.clear();
                if value.get("condition").is_some() && send.send(value).is_err() {
                    break;
                }
            }
        }
    });
    (child, receive)
}
pub fn start_with(node: &Node, args: &[&str], envs: &[(&str, &str)]) -> Server {
    let (child, receive) = spawn(node, args, envs);
    let server = Server(child);
    let status = receive
        .recv_timeout(Duration::from_secs(45))
        .unwrap_or_else(|_| panic!("{} did not publish readiness", node.root().display()));
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    server
}
pub fn start(node: &Node, args: &[&str]) -> Server {
    start_with(node, args, &[])
}
/// The founder's invitation for `name`, through a pipe, as a private file
/// in the host's directory.
pub fn invitation(founder: &Node, host: &Node, name: &str) -> PathBuf {
    let output = run(
        founder,
        None,
        &["cluster", "invite", "--node", name, "--output", "-"],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = host.root().join(format!("{name}.invite"));
    std::fs::write(&path, &output.stdout).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}
/// Enroll and start a host in one command; its node id.
pub fn join_start(founder: &Node, host: &Node, name: &str, advertise: &str) -> (Server, u64) {
    let invite = invitation(founder, host, name);
    let server = start(
        host,
        &[
            "--advertise",
            advertise,
            "--invite-file",
            invite.to_str().unwrap(),
        ],
    );
    let id = identity(host).0;
    (server, id)
}
/// (node, tenant, session) of a running node.
pub fn identity(node: &Node) -> (u64, String, String) {
    let identity = admin(node, &["cluster", "node", "identity"])["result"]["identity"].clone();
    (
        identity["node"].as_u64().unwrap(),
        identity["tenant"].as_str().unwrap().to_owned(),
        identity["session"].as_str().unwrap().to_owned(),
    )
}
pub fn placement(node: &Node) -> Option<Value> {
    let output = run(node, None, &["cluster", "placement"]);
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .map(|value| value["result"]["placement"].clone())
}
pub fn session_row<'a>(view: &'a Value, ledger: &str) -> Option<&'a Value> {
    view["partitions"].as_array()?.iter().find_map(|partition| {
        partition["sessions"]
            .as_array()?
            .iter()
            .find(|session| session["session"] == ledger)
    })
}
pub fn node_row(view: &Value, id: u64) -> Option<&Value> {
    view["partitions"].as_array()?.iter().find_map(|partition| {
        partition["nodes"]
            .as_array()?
            .iter()
            .find(|node| node["node"] == id)
    })
}
pub fn ids(value: &Value) -> Vec<u64> {
    value
        .as_array()
        .map(|ids| ids.iter().filter_map(Value::as_u64).collect())
        .unwrap_or_default()
}
/// Every node listed, alive and reporting load; the session settled at
/// `max_failures` with no plan pending.
pub fn settled(view: &Value, ledger: &str, nodes: &[u64], max_failures: u64) -> bool {
    nodes.iter().all(|id| {
        node_row(view, *id)
            .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
    }) && session_row(view, ledger).is_some_and(|session| {
        session["pending"].is_null()
            && session["achieved_max_failures"] == max_failures
            && session["retiring"].as_array().is_some_and(Vec::is_empty)
    })
}
pub fn wait_for(
    node: &Node,
    what: &str,
    timeout: Duration,
    condition: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(view) = placement(node) {
            if condition(&view) {
                return view;
            }
            last = Some(view);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let health = run(node, None, &["cluster", "node", "health"]);
    panic!(
        "{what} did not happen within {timeout:?}; health: {}; last view: {last:#?}",
        String::from_utf8_lossy(&health.stdout)
    );
}
/// Plan a session's durability and wait for its activation.
pub fn plan_and_settle(
    founder: &Node,
    tenant: &str,
    ledger: &str,
    survive: Option<&str>,
    max_failures: u64,
    nodes: &[u64],
) -> Value {
    let failures = max_failures.to_string();
    let mut args = vec![
        "cluster",
        "sessions",
        "plan",
        "--tenant",
        tenant,
        "--session",
        ledger,
        "--max-failures",
        &failures,
    ];
    if let Some(survive) = survive {
        args.extend(["--survive", survive]);
    }
    let planned = admin(founder, &args)["result"].clone();
    assert!(
        matches!(planned["state"].as_str(), Some("planned" | "satisfied")),
        "{planned}"
    );
    wait_for(founder, "activation", Duration::from_secs(240), |view| {
        settled(view, ledger, nodes, max_failures)
    })
}
/// A participant enrolled from the founder; its principal.
pub fn enroll_client(founder: &Node, client: &Node, name: &str) -> String {
    let invitation = client.root().join(format!("{name}.invite"));
    admin(
        founder,
        &[
            "cluster",
            "client",
            "invite",
            "--name",
            name,
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    let enrolled = run(
        client,
        None,
        &[
            "context",
            "enroll",
            name,
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(
        enrolled.status.success(),
        "{}",
        String::from_utf8_lossy(&enrolled.stderr)
    );
    let standing = admin(client, &["--client-context", name, "status"]);
    hex_hash(&objects(&standing)[0]["Standing"]["principal"])
}
/// An identity as the CLI prints it: hex, or the byte array of a raw result.
pub fn hex_hash(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_owned();
    }
    value
        .as_array()
        .unwrap_or_else(|| panic!("not an identity: {value}"))
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}
pub fn objects(page: &Value) -> &Vec<Value> {
    assert_eq!(page["result"]["kind"], "native_read", "{page}");
    page["result"]["page"]["objects"].as_array().unwrap()
}
pub fn committed(value: &Value) -> Value {
    assert_eq!(value["condition"], "Committed", "{value}");
    assert_eq!(value["result"]["kind"], "native", "{value}");
    value["result"].clone()
}
/// A founder whose session is native before its first start (23 §5): the
/// participant workflow the runbooks write needs the native engine.
pub fn activate_native(node: &Node) {
    let activation = admin(node, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
}
pub fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap_or_else(|| panic!("no created list: {result}"))
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| {
            assert!(!entry["id"].is_null(), "created entry without id: {result}");
            hex_hash(&entry["id"])
        })
        .collect()
}
/// A claim for `principal` with one artifact slot, written through `node`.
pub fn claim_document(principal: &str, description: &str) -> Value {
    json!({
        "description": description,
        "target": principal,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    })
}
/// Submit and post a claim through `node`; its id.
pub fn write_claim(node: &Node, principal: &str, description: &str) -> String {
    let document = claim_document(principal, description);
    let result = committed(&cli(
        node,
        None,
        &["submit", "claim", "--json", &document.to_string()],
    ));
    let claim = created(&result, "Claim").remove(0);
    committed(&cli(node, None, &["claim", "post", &claim]));
    claim
}
/// A claim written by `write_claim` with its artifact delivered by the
/// participant; the artifact id.
pub fn deliver_artifact(client: &Node, context: &str, claim: &str) -> String {
    committed(&cli(client, Some(context), &["receipt", "acquire", claim]));
    let result = committed(&cli(
        client,
        Some(context),
        &[
            "artifact", "submit", "--claim", claim, "--slot", "0", "--text", PROOF,
        ],
    ));
    created(&result, "Artifact").remove(0)
}
pub fn read_claim(node: &Node, claim: &str) -> Value {
    objects(&cli(node, None, &["get", "claim", claim]))[0].clone()
}
/// The identity a read claim object carries (its binding's object id).
pub fn claim_id(object: &Value) -> String {
    hex_hash(&object["Claim"]["binding"]["object"])
}
pub fn chunk_files(node: &Node, tenant: &str) -> Vec<PathBuf> {
    let directory = node.root().join("content").join("objects").join(tenant);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&directory)
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "chunk"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}
pub fn repair(node: &Node, tenant: &str, ledger: &str) -> Value {
    let report = admin(
        node,
        &["cluster", "repair", "--tenant", tenant, "--session", ledger],
    );
    assert_eq!(report["result"]["kind"], "repaired", "{report}");
    report["result"]["repair"].clone()
}
pub fn readiness(node: &Node) -> Value {
    admin(node, &["cluster", "node", "readiness"])["result"]["readiness"].clone()
}
/// Start under a file-size limit (`ulimit -f`, in 512-byte blocks) with
/// `SIGXFSZ` ignored, so an oversized write returns `EFBIG` instead of
/// killing the process: a full volume modelled as a write failure.
pub fn start_limited(node: &Node, args: &[&str], blocks: u64) -> Server {
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        "trap '' XFSZ; ulimit -f \"$1\"; shift; exec \"$@\"",
        "sh",
        &blocks.to_string(),
        env!("CARGO_BIN_EXE_focal"),
    ]);
    if let Some(config) = &node.config {
        command.args(["--config", config.to_str().unwrap()]);
    }
    command.args(["--data-dir", node.root().to_str().unwrap(), "start"]);
    command.args(args);
    let mut child = command
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
                text.clear();
                if value.get("condition").is_some() && send.send(value).is_err() {
                    break;
                }
            }
        }
    });
    let server = Server(child);
    let status = receive
        .recv_timeout(Duration::from_secs(45))
        .expect("the limited node did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    server
}
/// The largest file under the data directory, in bytes.
pub fn largest_file(root: &Path) -> u64 {
    fn walk(path: &Path, largest: &mut u64) {
        for entry in std::fs::read_dir(path).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, largest);
            } else if let Ok(metadata) = path.metadata() {
                *largest = (*largest).max(metadata.len());
            }
        }
    }
    let mut largest = 0;
    walk(root, &mut largest);
    largest
}
pub fn ranges(node: &Node, ledger: &str) -> Option<Value> {
    let output = run(
        node,
        None,
        &["cluster", "replicas", "ranges", "--session", ledger, "list"],
    );
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .map(|value| value["result"]["ranges"].clone())
}
