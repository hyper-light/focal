#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]
//! Every store focal writes, sealed at rest behind the fence and converted by the real binary (doc
//! 29 §12, the plan's step 8). A cluster founded below the at-rest level holds every store in the
//! clear: its session's checkpoint seeds, a family retired to its cold stream, its content store's
//! uploads being staged, a chunk being received and its custody records, a backup, a client's stores
//! in the node's directory and an enrolled client's in its own, and the node's small files; beside it
//! a bus group's part. The fence rises to the at-rest level and the node records it; started again,
//! every store converts (the node's files before it serves, the seeds, the cold streams and the files
//! beside the objects as it serves, each client's stores at their next open) while its claims, its
//! artifact and its retired family keep reading back, a request pending across the fence is answered
//! once, and a backup taken at the level is sealed and restores on a fresh node with its key file.
//! Then every data directory is scanned: no file holds a marker the journey wrote in the clear or the
//! magic any store's clear form begins with, and no name holds a hash the journey made that a store
//! files under a keyed name; the files doc 29 §12 lists clear by design hold no more than a marker. Unix-only, as the other real-binary at-rest tests: the data directories are kept private
//! by their mode bits.
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::task::Poll;
use std::time::{Duration, Instant};

use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::budget::BusBudget;
use focal_bus::client::{Client, Delivery};
use focal_bus::directory::BusDirectory;
use focal_bus::limits::IO;
use focal_bus::wire::{Ack, Bus, Durability};
use focal_memory::MemoryBudget;
use focal_timing::ProgressDeadline;
use hyper_rt::runtime::LocalRuntime;
use hyper_rt_transport::Driver;
use hyper_transport::{Event, Refusal};

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

/// Every start seeds its checkpoints (an inline bound of 64 bytes, as `cli_seeds_at_rest.rs` forces
/// it), runs the collector every 200 ms, and retires a released family at once and looks every
/// fifth of a second (as `cli_archive.rs` has it), so each store fills and converts while the
/// journey looks.
const NODE_ENV: &[(&str, &str)] = &[
    ("FOCAL_SEED_INLINE_BYTES", "64"),
    ("FOCAL_GC_INTERVAL_MS", "200"),
    ("FOCAL_RETIRE_INTERVAL_MS", "200"),
    ("FOCAL_RETIRE_AFTER_MS", "0"),
];

/// What the journey wrote that no file may hold in the clear once its store is sealed: each a
/// marker no store writes of its own.
const CLAIM_WORDS: &str = "Deliver the at-rest journey's report: zircon-heron-7177";
const ENROLLED_WORDS: &str = "The enrolled client's own claim: basalt-wren-7177";
const PENDING_WORDS: &str = "Cut after the commit, answered once: cobalt-lynx-7177";
const PROOF: &str = r#"{"passed":7177,"failed":0,"skipped":0}"#;
const STAGED: &[u8] = b"the staged upload's own bytes: quartz-ibis-7177";
const RECEIVED: &[u8] = b"the received chunk's own bytes: amber-tern-7177";
const RECORD: &[u8] = b"a custody record's own bytes: onyx-kite-7177";
const PUBLISHED: &[u8] = b"a bus record's own bytes: jade-moth-7177";
const FAR: u64 = 4_102_444_800_000;

/// The magic each store's clear form begins with, as its writer declares it (the eight-byte
/// `MAGIC`s of focal-node, focal-client, focal-enrollment, focal-bus, focal-evidence and
/// focal-ledger), but the clear headers of the sealed forms (a node file's `FCLNSEAL`, a client
/// record's `FCLCSEAL`, a record file's `FCLPART1` and `FCLCOLD1`, a sealed backup's `FCLBKEY1`)
/// and the fence's `FOCALSL1`, clear by design: no file holds one once its store is sealed, so a
/// store left in the clear is found even where the journey wrote no marker into it.
const CLEAR_FORMS: &[&str] = &[
    "FCCRCPT1",
    "FCLADMIN1",
    "FCLAPLY1",
    "FCLBKUP1",
    "FCLBUSK1",
    "FCLCLI01",
    "FCLID001",
    "FCLINV01",
    "FCLKEY01",
    "FCLMCA01",
    "FCLMCO02",
    "FCLMCP01",
    "FCLMPR01",
    "FCLMRE01",
    "FCLNCT01",
    "FCLNET01",
    "FCLNJR01",
    "FCLNPR01",
    "FCLOP001",
    "FCLPLAN1",
    "FCLPRE01",
    "FCLROOT1",
    "FCLTRST1",
    "FCLUPT02",
    "FCLWAT02",
    "FCNGENES",
    "FCNSESS1",
    "FOCALAC1",
    "FOCALCM1",
    "FOCALCU1",
    "FOCALCU2",
    "FOCALMC1",
    "FOCALMD1",
    "FOCALMS1",
    "FOCALMU1",
    "FOCALND1",
    "FOCALOP1",
    "FOCALPL1",
    "FOCALRG1",
    "FOCALRM1",
    "FOCALRT1",
    "FOCALSO1",
    "FOCALSO2",
    "FOCALSS1",
    "FOCALSS2",
    "FOCALSS3",
    "FOCALSS4",
    "FOCALSS5",
    "FOCALSS6",
    "FOCALSS7",
];

/// Start the node on `root` announcing `announced` (a staged rollout, 24 §21), or the binary's own
/// level, with `fault` where one is named, and wait for its readiness.
fn start(root: &Path, advertise: &str, announced: Option<&str>, fault: Option<&str>) -> Server {
    progress::admit();
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args([
        "--data-dir",
        root.to_str().unwrap(),
        "start",
        "node",
        "--advertise",
        advertise,
    ]);
    command.envs(NODE_ENV.iter().copied());
    if let Some(level) = announced {
        command.env("FOCAL_CAPABILITY_LEVEL", level);
    }
    if let Some(fault) = fault {
        command.env("FOCAL_FAULT", fault);
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

/// The binary on the data directory `root`, through `context` where one is named.
fn run(root: &Path, context: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap()]);
    if let Some(context) = context {
        command.args(["--client-context", context]);
    }
    command.args(args).output().unwrap()
}

/// A command that succeeds, its JSON (`--format json`).
fn cli(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let output = run(root, context, &args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// An administrative command, which prints its JSON as it is.
fn admin(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let output = run(root, context, args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// A command that fails: its exit code and the JSON it printed, where errors go or where results
/// go.
fn failed(root: &Path, context: Option<&str>, args: &[&str]) -> (i32, Value) {
    let mut full = args.to_vec();
    full.extend(["--format", "json"]);
    let output = run(root, context, &full);
    assert!(!output.status.success(), "{args:?} succeeded");
    let text = if output.stdout.is_empty() {
        output.stderr.clone()
    } else {
        output.stdout.clone()
    };
    let value: Value = serde_json::from_slice(&text)
        .unwrap_or_else(|error| panic!("{args:?}: {error}: {}", String::from_utf8_lossy(&text)));
    (output.status.code().unwrap(), value)
}

/// A mutation's committed result.
fn committed(value: &Value) -> Value {
    assert_eq!(value["condition"], "Committed", "{value}");
    value["result"].clone()
}

fn hex_hash(value: &Value) -> String {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
        .collect()
}

/// The objects a result created of `kind`, by their identities.
fn created(result: &Value, kind: &str) -> Vec<String> {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["kind"] == kind)
        .map(|entry| hex_hash(&entry["id"]))
        .collect()
}

fn objects(page: &Value) -> &Vec<Value> {
    page["result"]["page"]["objects"].as_array().unwrap()
}

/// The node's session's committed sequence.
fn sequence(root: &Path) -> u64 {
    admin(root, None, &["inspect", "prefix"])["result"]["page"]["native_sequence"]
        .as_u64()
        .unwrap()
}

/// A checkpoint of the node's session, retried while its first proposals are in flight.
fn checkpoint(root: &Path) {
    let mut deadline = Deadline::after(Duration::from_secs(60));
    loop {
        let output = run(root, None, &["checkpoint", "replica"]);
        if output.status.success() {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(value["result"]["kind"], "replica_checkpointed", "{value}");
            return;
        }
        assert!(
            deadline.open(),
            "the node never checkpointed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The claim's continuation once its family retired (26 §4).
fn retired(root: &Path, claim: &str) -> Value {
    let mut deadline = Deadline::after(Duration::from_secs(60));
    loop {
        let output = run(root, None, &["get", "claim", claim, "--format", "json"]);
        if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
            && let Some(object) = objects(&page).first()
            && object.get("Retired").is_some()
        {
            return object["Retired"].clone();
        }
        assert!(deadline.open(), "claim {claim} never retired");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The collector's last pass as `inspect node --gc` shows it; none while the node does not answer.
fn gc(root: &Path) -> Option<Value> {
    let output = run(root, None, &["inspect", "node", "--gc"]);
    if !output.status.success() {
        return None;
    }
    let shown: Value = serde_json::from_slice(&output.stdout).ok()?;
    (shown["result"]["kind"] == "gc").then(|| shown["result"]["gc"].clone())
}

/// Raise the fence of the node on `root` to the at-rest level: started at the binary's level, it
/// reports the level, the fence is activated, and the node records it; then it is stopped.
fn raise_fence(root: &Path, advertise: &str) {
    let at_rest = focal_node::upgrade::AT_REST_LEVEL;
    let server = start(root, advertise, None, None);
    let mut deadline = Deadline::after(Duration::from_secs(90));
    loop {
        let output = run(root, None, &["inspect", "upgrade"]);
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
        assert!(
            deadline.open(),
            "the node never reported the at-rest level: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let activated = admin(
        root,
        None,
        &["activate", "upgrade", "--fence", &at_rest.to_string()],
    );
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
}

/// A key file the real binary makes, as an operator makes one.
fn key_file(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let made = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["create", "root-key", "--output", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(made.status.success(), "{made:?}");
    path
}

/// The content store's limits, as the node's own are for a store of this journey's size.
fn limits() -> focal_evidence::StoreLimits {
    focal_evidence::StoreLimits {
        max_content_bytes: 64 << 20,
        max_staging_bytes: 64 << 20,
        max_uploads: 16,
        chunk_bytes: 64 << 10,
        max_manifest_bytes: 64 << 10,
    }
}

/// `length` bytes holding `marker` in each of their chunks.
fn marked(marker: &[u8], length: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..length).map(|index| (index % 251) as u8).collect();
    let mut at = 0;
    while at + marker.len() <= length {
        bytes[at..at + marker.len()].copy_from_slice(marker);
        at += 16 << 10;
    }
    bytes
}

/// Whether a file of this name is one doc 29 §12 lists in the clear by design: keys' records,
/// locks, marks and markers, the fence, an upload's or a stream's identity directories.
fn clear_by_design(name: &str) -> bool {
    matches!(
        name,
        "SEAL.node"
            | "SEAL.root"
            | "STORAGE.level"
            | "LOCK"
            | "INITIALIZED"
            | "SEALED"
            | "SEALING"
            | "REST_SEALED"
            | "REST_SEALING"
            | "FILES.SEALED"
            | "FILES.lock"
            | "DOMAIN.key"
    ) || name.ends_with(".lock")
        || name.ends_with("-lock")
        || name.ends_with("-sealed")
        || name.ends_with(".initialized")
        || name.ends_with(".client-initialized")
}

/// Markers searched for together: each file is read once and searched once for all of them, each
/// byte looked up among the markers' first bytes and only a byte that begins one compared further
/// (a pass a marker would read every byte of every file once a marker, fifty-eight times).
struct Markers<'a> {
    markers: Vec<&'a [u8]>,
    /// The markers each byte begins, by their places in `markers`.
    starting: Vec<Vec<usize>>,
}
impl<'a> Markers<'a> {
    fn new(markers: &[&'a [u8]]) -> Self {
        let mut starting = vec![Vec::new(); 256];
        for (index, marker) in markers.iter().enumerate() {
            starting[usize::from(marker[0])].push(index);
        }
        Self {
            markers: markers.to_vec(),
            starting,
        }
    }
    /// The markers `bytes` holds.
    fn held(&self, bytes: &[u8]) -> Vec<&'a [u8]> {
        let mut found = vec![false; self.markers.len()];
        for (at, byte) in bytes.iter().enumerate() {
            for &index in &self.starting[usize::from(*byte)] {
                if !found[index] && bytes[at..].starts_with(self.markers[index]) {
                    found[index] = true;
                }
            }
        }
        self.markers
            .iter()
            .zip(found)
            .filter_map(|(marker, found)| found.then_some(*marker))
            .collect()
    }
}

/// A file under the scanned roots: one doc 29 §12 lists clear by design by its name, of this
/// length, which holds no data (a fixed sentence at most, or a key wrapped under another), or
/// another, with the markers it holds.
enum Seen<'a> {
    ByDesign(u64),
    Data(Vec<&'a [u8]>),
}

/// Every file under each of `roots`, read one at a time and searched for `markers`, then handed to
/// `each` with what it held; a socket the node listened on is no file of a store.
fn each_file<'a>(roots: &[&Path], markers: &Markers<'a>, mut each: impl FnMut(&Path, Seen<'a>)) {
    let mut pending: Vec<PathBuf> = roots.iter().map(|root| root.to_path_buf()).collect();
    while let Some(path) = pending.pop() {
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        if metadata.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                pending.push(entry.unwrap().path());
            }
            continue;
        }
        if metadata.file_type().is_socket() {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if clear_by_design(&name) {
            each(&path, Seen::ByDesign(metadata.len()));
        } else {
            let bytes = std::fs::read(&path).unwrap();
            each(&path, Seen::Data(markers.held(&bytes)));
        }
    }
}

use std::os::unix::fs::FileTypeExt;

/// The scan: no file under `roots` holds any of `markers`, no path names any of `names` (each a
/// hash the journey made, which a store names its files by only under a keyed name), and each file
/// clear by design holds no more than a marker.
fn scan(roots: &[&Path], markers: &[&[u8]], names: &[String]) {
    let markers = Markers::new(markers);
    let mut data = 0usize;
    each_file(roots, &markers, |path, seen| {
        match seen {
            // A lock or a mark is empty or a fixed sentence; a key record is a key wrapped under
            // another (61 bytes), the fence a level and its CRC.
            Seen::ByDesign(length) => assert!(
                length <= 4096,
                "{} holds more than a marker",
                path.display()
            ),
            Seen::Data(held) => {
                data += 1;
                assert!(
                    held.is_empty(),
                    "{} holds {:?} in the clear",
                    path.display(),
                    held.iter()
                        .map(|marker| String::from_utf8_lossy(marker))
                        .collect::<Vec<_>>()
                );
            }
        }
        let shown = path.to_string_lossy();
        for name in names {
            assert!(
                !shown.contains(name.as_str()),
                "{} is named by {name} in the clear",
                path.display()
            );
        }
    });
    assert!(data > 0, "nothing under {roots:?}");
}

/// The files under `roots` that hold any of `markers`, each with those it holds.
fn holding<'a>(roots: &[&Path], markers: &[&'a [u8]]) -> Vec<(PathBuf, Vec<&'a [u8]>)> {
    let markers = Markers::new(markers);
    let mut holding = Vec::new();
    each_file(roots, &markers, |path, seen| {
        if let Seen::Data(held) = seen
            && !held.is_empty()
        {
            holding.push((path.to_path_buf(), held));
        }
    });
    holding
}

/// The group of bus nodes this journey runs (doc 34), killed when the journey ends.
struct Units(Vec<Child>);
impl Drop for Units {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A bus node waits this long for its first line at most: an OS's assessment of a first launch is
/// not the node's start (`bus_failover.rs`).
const FROZEN: Duration = Duration::from_secs(30);

/// Starts node `n` of the bus group listening at `addresses`, its directories under `dir`, and waits
/// for the line saying it listens.
fn start_bus(n: u64, addresses: &[SocketAddr], dir: &Path) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command
        .args(["start", "bus", "--listen"])
        .arg(addresses[(n - 1) as usize].to_string())
        .arg("--pki")
        .arg(dir.join("pki"))
        .args(["--node", &n.to_string()])
        .arg("--data")
        .arg(dir.join(format!("node-{n}")))
        .arg("--key-file")
        .arg(dir.join("keys").join(format!("node-{n}.key")));
    for (at, address) in addresses.iter().enumerate() {
        let peer = at as u64 + 1;
        if peer != n {
            command.arg("--peer").arg(format!("{peer}={address}"));
        }
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && send.send(value).is_err()
            {
                break;
            }
        }
    });
    let began = Instant::now();
    loop {
        match lines.recv_timeout(Duration::from_millis(100)) {
            Ok(line) if line["condition"] == "Listening" => break,
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!(
                    "bus node {n} exited before it listened: {:?}",
                    child.try_wait()
                )
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                assert!(
                    began.elapsed() < FROZEN,
                    "bus node {n} printed nothing for {FROZEN:?}"
                );
            }
        }
    }
    child
}

type Drv = Driver<Bus, BusBudget, BusDirectory>;

/// Publishes `count` batches to the bus group at `nodes`, each one record holding [`PUBLISHED`], and
/// returns once every one is acknowledged durable (`bus_failover.rs`'s client, without a failover).
fn publish(nodes: &[(u64, SocketAddr)], pki: &Path, count: u32) {
    const STREAM: u64 = 1;
    const SESSION: u64 = 1;
    let local = focal_bus::pki::load_or_make(pki, None, nodes.len(), 2).unwrap();
    let budget = MemoryBudget::new(64 << 20, 0).unwrap();
    let endpoint =
        focal_bus::client::endpoint(&local, 0, nodes.len(), budget, Instant::now()).unwrap();
    // The runtime runs a future of its own: the group's addresses go with it.
    let nodes = nodes.to_vec();
    let mut runtime = LocalRuntime::new(&focal_rt::local_config()).unwrap();
    runtime
        .block_on(async move {
            let mut driver: Drv =
                Driver::bind(endpoint, "127.0.0.1:0".parse().unwrap(), IO).unwrap();
            let mut bus = Client::group(&nodes).unwrap();
            bus.dial(driver.endpoint(), Instant::now()).unwrap();
            let mut builder = BatchBuilder::new();
            let mut unacked: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
            let mut pending: BTreeSet<u32> = BTreeSet::new();
            for sequence in 0..count {
                builder.begin(&Producing {
                    stream: STREAM,
                    producer: SESSION,
                    sequence,
                    flags: 0,
                    timestamp: 0,
                });
                builder.push(0, PUBLISHED).unwrap();
                unacked.insert(sequence, builder.finish().unwrap().to_vec());
                pending.insert(sequence);
            }
            let moves = |unacked: &BTreeMap<u32, Vec<u8>>| [unacked.len() as u64];
            let mut wait = ProgressDeadline::begin(&moves(&unacked), u64::MAX, FROZEN);
            while !unacked.is_empty() {
                let now = Instant::now();
                if bus.ready(now) {
                    let mut sends = if bus.asking() { 1 } else { pending.len() };
                    while sends > 0
                        && let Some(&first) = pending.first()
                    {
                        sends -= 1;
                        let Some(bytes) = unacked.get(&first) else {
                            pending.remove(&first);
                            continue;
                        };
                        match bus.publish(driver.endpoint(), bytes) {
                            Ok(()) => {
                                pending.remove(&first);
                            }
                            Err(Refusal::NotConnected) => {
                                let _ = bus.dial(driver.endpoint(), now);
                                break;
                            }
                            Err(Refusal::LaneFull) => break,
                            Err(other) => panic!("publish refused: {other:?}"),
                        }
                    }
                }
                let mut tick = pin!(hyper_rt::futures::sleep(1_000_000));
                let first = poll_fn(|context| {
                    if let Poll::Ready(event) = driver.poll_event(context) {
                        return Poll::Ready(Some(event.unwrap()));
                    }
                    if tick.as_mut().poll(context).is_ready() {
                        return Poll::Ready(None);
                    }
                    Poll::Pending
                })
                .await;
                let mut events: Vec<Event<Bus>> = first.into_iter().collect();
                while let Some(event) = driver.endpoint().poll_event() {
                    events.push(event);
                }
                for event in events {
                    let mut acks: Vec<Ack> = Vec::new();
                    let mut again: Vec<u32> = Vec::new();
                    let mut rerouted = false;
                    bus.on_event(
                        driver.endpoint(),
                        event,
                        Instant::now(),
                        |delivery| match delivery {
                            Delivery::Acked(ack) => acks.push(ack),
                            Delivery::Redirected(redirect) => again.push(redirect.sequence),
                            Delivery::Rerouted { .. } => rerouted = true,
                            Delivery::Lost { .. } => rerouted = true,
                            other => panic!("{other:?}"),
                        },
                    );
                    for ack in acks {
                        assert_eq!((ack.stream, ack.producer), (STREAM, SESSION), "{ack:?}");
                        if ack.class == Ack::OUT_OF_ORDER {
                            let from = u32::try_from(ack.base).unwrap();
                            pending.extend(unacked.range(from..).map(|(sequence, _)| *sequence));
                            continue;
                        }
                        assert_eq!(
                            Durability::of(ack.class),
                            Some(Durability::Persisted),
                            "{ack:?}"
                        );
                        unacked.remove(&ack.sequence);
                        pending.remove(&ack.sequence);
                    }
                    for sequence in again {
                        if unacked.contains_key(&sequence) {
                            pending.insert(sequence);
                        }
                    }
                    if rerouted {
                        pending.extend(unacked.keys().copied());
                    }
                }
                if let Err(frozen) = wait.check(&moves(&unacked)) {
                    panic!(
                        "the bus never acknowledged: {frozen:?}; {} left",
                        unacked.len()
                    );
                }
            }
        })
        .unwrap();
}

/// Do: found a node below the at-rest level; enroll a client from its own directory; run a
/// two-party cycle to its retirement, take claims and checkpoints, and back the session up; leave a
/// request committed with its reply lost; lay out, as such a node keeps them, an upload half sent, a
/// chunk half received and a custody record of each kind; run a bus group and publish to it; raise
/// the fence to the at-rest level and start the node again; read the stores back while they
/// convert; retry the lost request; back the session up at the level and restore it on a fresh node
/// with its key file; open each client's stores again; restart the bus group. Expect: below the
/// fence every store holds what the journey wrote in the clear; after it every store is sealed (the
/// node's files marked before it serves, the seeds, the cold streams and the files beside the
/// objects converted as it serves, each client's stores marked at their next open), the claims, the
/// artifact, the retired family, the upload, the chunk and the records read back, the lost request
/// answered once, the sealed backup refused without its key and restored with it, and no file of any
/// data directory holding a marker the journey wrote or any store's clear form, nor any path a hash
/// it made, but the clear backup taken below the fence, which is clear by design.
#[test]
fn every_store_converts_once_the_fence_opens_the_at_rest_level() {
    let directory = tempfile::Builder::new()
        .prefix("focal-at-rest-")
        .tempdir_in("/tmp")
        .unwrap();
    let private = |name: &str| {
        let path = directory.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path
    };
    let root = &private("node");
    let client = &private("client");
    let fresh = &private("fresh");
    let buses = &private("bus");
    let backups = &private("backups");
    let advertise = ports::address();
    let at_rest = focal_node::upgrade::AT_REST_LEVEL;
    assert!(
        (focal_node::upgrade::CONTENT_LEVEL + 1..=focal_node::upgrade::CAPABILITY_LEVEL)
            .contains(&at_rest),
        "this journey rehearses the rollout that opens the at-rest level past the content level"
    );
    let below = (at_rest - 1).to_string();

    // Founded below the at-rest level: every store but the content store's objects is written in
    // the clear.
    let activation = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "activate", "native"])
        .env("FOCAL_CAPABILITY_LEVEL", &below)
        .output()
        .unwrap();
    assert!(activation.status.success(), "{activation:?}");
    let server = start(root, &advertise, Some(&below), None);
    // The bundle is the operator's, kept outside every data directory (29 §12).
    let invitation = directory.path().join("alice.invite");
    admin(
        root,
        None,
        &[
            "invite",
            "client",
            "--name",
            "alice",
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    let enrolled = run(
        client,
        None,
        &[
            "enroll",
            "context",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(enrolled.status.success(), "{enrolled:?}");
    let history = client.join("CLIENT.contexts").join("history-alice");
    assert_eq!(
        focal_node::storage_level::known(&history).unwrap(),
        Some(at_rest - 1),
        "the enrolled client learned its cluster's fence"
    );
    let standing = admin(client, Some("alice"), &["inspect", "prefix"]);
    let alice = hex_hash(&objects(&standing)[0]["Standing"]["principal"]);
    // The node's own principal, which the enrolled client's claims are for.
    let standing = admin(root, None, &["inspect", "prefix"]);
    let issuer = hex_hash(&objects(&standing)[0]["Standing"]["principal"]);

    // A two-party cycle to its retirement: the family leaves the live ledger for the node's cold
    // stream, in the clear below the fence.
    let claim_document = json!({
        "description": CLAIM_WORDS,
        "target": alice,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    let result = committed(&cli(
        root,
        None,
        &["submit", "claim", "--json", &claim_document.to_string()],
    ));
    let claim = created(&result, "Claim").remove(0);
    let validation = created(&result, "Validation").remove(1);
    committed(&cli(root, None, &["post", "claim", &claim]));
    committed(&cli(client, Some("alice"), &["acquire", "receipt", &claim]));
    let result = committed(&cli(
        client,
        Some("alice"),
        &[
            "submit", "artifact", "--claim", &claim, "--slot", "0", "--text", PROOF,
        ],
    ));
    let artifact = created(&result, "Artifact").remove(0);
    let live_artifact =
        objects(&cli(client, Some("alice"), &["get", "artifact", &artifact]))[0]["Artifact"]
            .clone();
    let artifact_hash = hex_hash(&live_artifact["content_hash"]);
    let result = committed(&cli(
        client,
        Some("alice"),
        &[
            "submit",
            "testament",
            "--claim",
            &claim,
            "--summary",
            "Suite passed.",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
            "--slot",
            &format!("0={artifact}:{artifact_hash}"),
        ],
    ));
    let testament = created(&result, "Response").remove(0);
    committed(&cli(
        client,
        Some("alice"),
        &["post", "testament", &testament, "--claim", &claim],
    ));
    committed(&cli(
        root,
        None,
        &["receive", "testament", &testament, "--claim", &claim],
    ));
    committed(&cli(
        root,
        None,
        &[
            "begin",
            "validation",
            "--claim",
            &claim,
            "--validation",
            &validation,
        ],
    ));
    committed(&cli(
        root,
        None,
        &[
            "report",
            "verdict",
            "--claim",
            &claim,
            "--validation",
            &validation,
            "--verdict",
            "pass",
            "--text",
            PROOF,
        ],
    ));
    committed(&cli(root, None, &["release", "scope", &claim]));
    let continuation = retired(root, &claim);

    // Claims of the enrolled client's own, kept in its stores, and of the node's, seeded into
    // checkpoints: the session's seeds, in the clear below the fence.
    let theirs = cli(
        client,
        Some("alice"),
        &[
            "submit",
            "claim",
            "--json",
            &json!({"description": ENROLLED_WORDS, "target": issuer,
                    "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]})
            .to_string(),
        ],
    );
    let their_claim = created(&committed(&theirs), "Claim").remove(0);
    let mut kept = vec![(claim.clone(), CLAIM_WORDS)];
    for index in 0..6 {
        let words = format!("{CLAIM_WORDS}, seeded {index}");
        let result = committed(&cli(
            root,
            None,
            &[
                "submit",
                "claim",
                "--json",
                &json!({"description": words, "target": alice,
                        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]})
                .to_string(),
            ],
        ));
        kept.push((created(&result, "Claim").remove(0), CLAIM_WORDS));
        if index % 3 == 2 {
            checkpoint(root);
        }
    }

    // A backup below the fence: in the clear, as every earlier binary wrote one, and refused a key
    // file (29 §12.8).
    let clear_backup = backups.join("clear");
    let made = admin(
        root,
        None,
        &[
            "create",
            "backup",
            "--output",
            clear_backup.to_str().unwrap(),
        ],
    );
    assert_eq!(made["result"]["kind"], "backup_created", "{made}");
    assert_eq!(made["result"]["backup"]["sealed"], false, "{made}");
    let before_cut = sequence(root);
    drop(server);

    // A request committed with its reply lost: the node aborts after its owner commits and before
    // any reply is written, the client's native journal keeping the request pending.
    let cut = start(
        root,
        &advertise,
        Some(&below),
        Some("after-commit-before-reply:1"),
    );
    let (code, unknown) = failed(
        root,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &json!({"description": PENDING_WORDS, "target": alice,
                    "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]})
            .to_string(),
        ],
    );
    assert_eq!(code, 7, "{unknown}");
    assert_eq!(unknown["condition"], "OutcomeUnknown", "{unknown}");
    let lost = unknown["operation_id"].as_str().unwrap().to_owned();
    drop(cut);

    // What such a node keeps beside its objects, laid out by the store's own writer under the
    // node's keys while the node is down: an upload half sent, a chunk half received from a peer,
    // and a record of each kind.
    let store = root.join("content");
    let keys = focal_seal::test_keys(root).unwrap();
    let domain = focal_model::ContentDomainId([7; 16]);
    let open = |root: &Path, keys: &focal_seal::DataKeys, rest| {
        focal_evidence::ContentStore::open_forms(
            root,
            limits(),
            focal_memory::DiskBudget::new(focal_memory::DiskBudgetConfig::unbounded()).unwrap(),
            keys,
            focal_evidence::ContentForm::Sealed,
            rest,
        )
        .unwrap()
    };
    let evidence = focal_model::ContentClass::Evidence;
    let half = focal_evidence::UploadId([0x5e; 16]);
    let half_bytes = marked(STAGED, 96 << 10);
    let sent = half_bytes.len() / 2;
    let source = tempfile::tempdir().unwrap();
    let source_keys = focal_seal::test_keys(source.path()).unwrap();
    let mut peer = open(
        source.path(),
        &source_keys,
        focal_evidence::ContentForm::Sealed,
    );
    let shipped = marked(RECEIVED, 50_000);
    let shipped_upload = focal_evidence::UploadId([0x5f; 16]);
    peer.begin(shipped_upload, domain, evidence, shipped.len() as u64, None)
        .unwrap();
    peer.append(shipped_upload, 0, &shipped).unwrap();
    let shipped_ref = peer.seal(shipped_upload).unwrap();
    let exported = peer.export_manifest(&shipped_ref).unwrap();
    let chunk = peer.read_transfer_chunk(&exported, 0).unwrap();
    drop(peer);
    let received = 20_000;
    let record = marked(RECORD, 600);
    // A receipt is a small record (its bound is a receipt's), the marker within it.
    let receipt = marked(RECORD, 80);
    let receipt_name = focal_model::ContentHash([0x93; 32]);
    let (transfer, record_hash) = {
        let mut writer = open(&store, &keys, focal_evidence::ContentForm::Clear);
        writer
            .begin(half, domain, evidence, half_bytes.len() as u64, None)
            .unwrap();
        writer.append(half, 0, &half_bytes[..sent]).unwrap();
        let transfer = writer
            .prepare_import(shipped_ref.clone(), exported.encoded().to_vec())
            .unwrap();
        writer
            .import_chunk_part(&transfer, 0, 0, &chunk[..received])
            .unwrap();
        let record_hash = writer
            .install_custody_record(focal_evidence::CustodyRecordKind::Checkpoint, &record)
            .unwrap();
        writer
            .replace_named_custody_record(
                focal_evidence::CustodyRecordKind::Receipt,
                receipt_name,
                &receipt,
            )
            .unwrap();
        (transfer, record_hash)
    };

    // A bus group's part: three nodes, a stream published to.
    std::fs::create_dir_all(buses.join("keys")).unwrap();
    let addresses: Vec<SocketAddr> = (0..3).map(|_| ports::address().parse().unwrap()).collect();
    let nodes: Vec<(u64, SocketAddr)> = (1..=3).zip(addresses.iter().copied()).collect();
    {
        let mut units = Units(vec![start_bus(1, &addresses, buses)]);
        for n in 2..=3 {
            units.0.push(start_bus(n, &addresses, buses));
        }
        publish(&nodes, &buses.join("pki"), 16);
    }

    // Below the fence every store holds what the journey wrote in the clear: the node's identity,
    // the session's seeds named by their hashes, its cold stream, the files beside the objects, a
    // client's journal in the node's directory and the enrolled client's in its own.
    assert!(
        std::fs::read(root.join("IDENTITY"))
            .unwrap()
            .starts_with(b"FOCALND1")
    );
    assert!(!root.join("FILES.SEALED").exists());
    assert!(!root.join("content").join("REST_SEALED").exists());
    let native_journal = root.join("client").join("native");
    assert!(native_journal.is_dir(), "the client's native journal");
    assert!(!native_journal.join("SEALED").exists());
    let clear_seeds: Vec<String> = std::fs::read_dir(root.join("seeds"))
        .unwrap()
        .flat_map(|session| std::fs::read_dir(session.unwrap().path()).unwrap())
        .map(|file| file.unwrap().file_name().to_string_lossy().into_owned())
        .filter_map(|name| name.strip_suffix(".seed").map(str::to_owned))
        .collect();
    assert!(!clear_seeds.is_empty(), "the checkpoints seeded nothing");
    assert!(
        clear_seeds.iter().all(|stem| stem.len() == 64),
        "{clear_seeds:?}"
    );
    // What the journey wrote lies in the clear in those stores, so the scan after the fence proves
    // the conversion took it: the claims' words in the session's seeds and cold stream, the
    // enrolled client's in its journal, the lost request's in the node's client's journal, the
    // upload's, the chunk's and the record's bytes beside the objects. (The artifact's report is an
    // object, sealed from the content level; the bus seals its log.) And each store's clear form
    // begins with its magic, which the scan after the fence finds nowhere: the node's identity and
    // network state, the client's native journal (its catalogue, its operations' journals and their
    // prepared requests) and a content store's receipt are held in their clear forms now. One pass
    // over the directories finds them all.
    let mut sought: Vec<&[u8]> = vec![
        CLAIM_WORDS.as_bytes(),
        ENROLLED_WORDS.as_bytes(),
        PENDING_WORDS.as_bytes(),
        STAGED,
        RECEIVED,
        RECORD,
    ];
    sought.extend(CLEAR_FORMS.iter().map(|magic| magic.as_bytes()));
    let below = holding(&[root, client, buses], &sought);
    let held_below = |marker: &[u8]| below.iter().any(|(_, held)| held.contains(&marker));
    for (marker, store) in [
        (CLAIM_WORDS.as_bytes(), "the seeds and the cold stream"),
        (ENROLLED_WORDS.as_bytes(), "the enrolled client's journal"),
        (PENDING_WORDS.as_bytes(), "the client's journal"),
        (STAGED, "the staged upload"),
        (RECEIVED, "the received chunk"),
        (RECORD, "the custody records"),
        (b"FOCALND1".as_slice(), "the node's identity"),
        (b"FCLNET01".as_slice(), "the node's network state"),
        (b"FCLNCT01".as_slice(), "the client's native catalogue"),
        (b"FCLNJR01".as_slice(), "the client's operations' journals"),
        (b"FCLNPR01".as_slice(), "the client's prepared requests"),
        (b"FCCRCPT1".as_slice(), "a content store's receipt"),
    ] {
        assert!(
            held_below(marker),
            "{store} below the fence holds no {:?} in the clear",
            String::from_utf8_lossy(marker)
        );
    }
    let cold = root.join("cold");
    assert!(
        below
            .iter()
            .any(|(path, held)| path.starts_with(&cold) && held.contains(&CLAIM_WORDS.as_bytes())),
        "the retired family is not in the clear cold stream"
    );

    // At the binary's level the fence rises and the node records it.
    raise_fence(root, &advertise);

    // Started again, the node's files are converted before it serves, and its seeds, cold streams
    // and the files beside its objects as it serves, while its claims, its artifact and its retired
    // family read back.
    let server = start(root, &advertise, None, None);
    assert!(root.join("FILES.SEALED").exists(), "the node's files");
    let mut deadline = Deadline::after(Duration::from_secs(180));
    loop {
        for (claim, words) in &kept {
            let page = cli(root, None, &["get", "claim", claim]);
            let object = &objects(&page)[0];
            assert!(
                object.get("Retired").is_some() || object.to_string().contains(words),
                "{claim}: {object}"
            );
        }
        let archived = cli(
            root,
            None,
            &["get", "archived", &claim, "--artifact", &artifact],
        );
        assert_eq!(
            objects(&archived)[0]["Archived"]["bundle"],
            continuation["bundle"],
            "{archived}"
        );
        if let Some(view) = gc(root)
            && view["last"]["seeds"]["conversion"]["sealed"] == 1
            && ["counting", "converting", "clear", "stopped"]
                .iter()
                .all(|form| view["last"]["seeds"]["conversion"][form] == 0)
            && view["last"]["seeds"]["cold"]["sealed"].as_u64() > Some(0)
            && ["counting", "converting", "clear", "stopped"]
                .iter()
                .all(|form| view["last"]["seeds"]["cold"][form] == 0)
            && view["content"]["form"] == "sealed"
            && store.join("REST_SEALED").exists()
        {
            break;
        }
        assert!(
            deadline.open(),
            "the stores never finished converting: {:?}",
            gc(root)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // The request lost before the fence, retried from the client's journal converted at its open,
    // is answered by its committed outcome: once, the sequence unmoved.
    let committed_by_then = sequence(root);
    assert!(committed_by_then > before_cut, "the cut's commit survived");
    let retried = cli(root, None, &["retry", "request", "--operation-id", &lost]);
    assert_eq!(retried["operation_id"], lost.as_str(), "{retried}");
    committed(&retried);
    let again = cli(root, None, &["retry", "request", "--operation-id", &lost]);
    assert_eq!(again["result"]["receipt"], retried["result"]["receipt"]);
    assert_eq!(sequence(root), committed_by_then, "answered once");
    assert!(native_journal.join("SEALED").exists(), "the native journal");

    // The enrolled client learns the risen fence beside its first request, and its stores convert
    // at their next open: its claim reads back through them.
    cli(client, Some("alice"), &["inspect", "epoch"]);
    assert_eq!(
        focal_node::storage_level::known(&history).unwrap(),
        Some(at_rest),
        "the fence, learned beside a request"
    );
    let theirs_again = cli(client, Some("alice"), &["get", "claim", &their_claim]);
    assert!(
        objects(&theirs_again)[0]
            .to_string()
            .contains(ENROLLED_WORDS),
        "{theirs_again}"
    );
    let next = cli(
        client,
        Some("alice"),
        &[
            "submit",
            "claim",
            "--json",
            &json!({"description": ENROLLED_WORDS, "target": issuer,
                    "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}}]})
            .to_string(),
        ],
    );
    committed(&next);
    assert!(
        client.join("FILES.SEALED").exists(),
        "the client directory's files"
    );

    // A backup at the level is sealed under the operator's key file, verifies, and restores on a
    // fresh node with it; without it, refused before anything reaches the node.
    let ops = key_file(directory.path(), "ops.key");
    let sealed_backup = backups.join("sealed");
    let made = admin(
        root,
        None,
        &[
            "create",
            "backup",
            "--output",
            sealed_backup.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    assert_eq!(made["result"]["backup"]["sealed"], true, "{made}");
    let backed = made["result"]["backup"].clone();
    let verified = admin(
        root,
        None,
        &[
            "verify",
            "backup",
            "--input",
            sealed_backup.to_str().unwrap(),
            "--key-file",
            ops.to_str().unwrap(),
        ],
    );
    assert_eq!(verified["result"]["kind"], "backup_verified", "{verified}");
    let identity =
        admin(root, None, &["inspect", "node", "--identity"])["result"]["identity"].clone();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    drop(server);
    let fresh_advertise = ports::address();
    let fresh_server = start(fresh, &fresh_advertise, None, None);
    let admitted = admin(fresh, None, &["admit", "tenant", "--tenant", &tenant]);
    assert_eq!(admitted["result"]["kind"], "tenants", "{admitted}");
    let restore = |key: Option<&Path>| {
        let mut args = vec![
            "restore",
            "session",
            "--input",
            sealed_backup.to_str().unwrap(),
            "--new-incarnation",
        ];
        if let Some(key) = key {
            args.extend(["--key-file", key.to_str().unwrap()]);
        }
        run(fresh, None, &args)
    };
    let refused = restore(None);
    assert!(!refused.status.success(), "{refused:?}");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("backup_key_required"),
        "{refused:?}"
    );
    let restored = restore(Some(&ops));
    assert!(restored.status.success(), "{restored:?}");
    let restored: Value = serde_json::from_slice(&restored.stdout).unwrap();
    assert_eq!(restored["result"]["kind"], "restored", "{restored}");
    assert_eq!(
        restored["result"]["restored"]["prefix"]["native_sequence"],
        backed["prefix"]["native_sequence"],
        "{restored}"
    );
    drop(fresh_server);

    // The bus group opens its sealed files again.
    {
        let mut units = Units(vec![start_bus(1, &addresses, buses)]);
        for n in 2..=3 {
            units.0.push(start_bus(n, &addresses, buses));
        }
    }

    // The upload, the chunk and the records go on from their sealed forms, through the store's own
    // writer with the node down.
    let mut store_writer = open(&store, &keys, focal_evidence::ContentForm::Sealed);
    assert_eq!(store_writer.offset(half).unwrap(), sent as u64);
    store_writer
        .append(half, sent as u64, &half_bytes[sent..])
        .unwrap();
    let reference = store_writer.seal(half).unwrap();
    assert_eq!(
        store_writer
            .read_bytes(&reference, half_bytes.len())
            .unwrap(),
        half_bytes
    );
    assert_eq!(
        store_writer.staged_chunk_bytes(&transfer, 0).unwrap(),
        received as u64
    );
    assert_eq!(
        store_writer
            .read_custody_record(
                focal_evidence::CustodyRecordKind::Checkpoint,
                record_hash,
                1 << 20
            )
            .unwrap(),
        record
    );
    assert_eq!(
        store_writer
            .read_named_custody_record(
                focal_evidence::CustodyRecordKind::Receipt,
                receipt_name,
                1 << 20
            )
            .unwrap(),
        Some(receipt)
    );
    drop(store_writer);

    // The clear backup taken below the fence still verifies: a binary below the level may restore
    // it, and it is clear by design (29 §12.8).
    let verified = admin(
        root,
        None,
        &[
            "verify",
            "backup",
            "--input",
            clear_backup.to_str().unwrap(),
        ],
    );
    assert_eq!(verified["result"]["kind"], "backup_verified", "{verified}");

    // The scan: no file of any data directory holds a marker the journey wrote or a store's clear
    // form, nor any path a hash it made that a store files under a keyed name, but the files listed
    // clear by design.
    let mut names: Vec<String> = clear_seeds.clone();
    names.push(artifact_hash.clone());
    names.push(hex_hash(&json!(half.0)));
    names.push(hex_hash(&json!(shipped_ref.root.0)));
    names.push(hex_hash(&json!(record_hash.0)));
    let mut markers: Vec<&[u8]> = vec![
        CLAIM_WORDS.as_bytes(),
        ENROLLED_WORDS.as_bytes(),
        PENDING_WORDS.as_bytes(),
        PROOF.as_bytes(),
        STAGED,
        RECEIVED,
        RECORD,
        PUBLISHED,
    ];
    markers.extend(CLEAR_FORMS.iter().map(|magic| magic.as_bytes()));
    scan(
        &[root, client, fresh, buses, &sealed_backup],
        &markers,
        &names,
    );
}
