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
//! A session's checkpoint seeds written in the clear and converted by the real binary as the
//! session serves (29 §12.5). A node founded below the at-rest level seeds its checkpoints in the
//! clear (its inline bound forced down to 64 bytes, as the placement journey forces it), each chunk
//! named by its hash and holding its bytes as every earlier binary wrote them; the fence rises to
//! the at-rest level and the node records it; restarted, the node's collector converts the session's
//! seed directory a job at a time, off the session's owner, while the session takes writes, and
//! `inspect node --gc` shows the directory counting or converting and then sealed. Every chunk the
//! clear directory held is held sealed after, nothing is named in the clear, and a checkpoint taken
//! after seals its seeds too. The session's cold streams (29 §12.6) go the same way: reported in
//! the clear below the fence, then sealed, each open chunk a record file. Unix-only, as the other
//! real-binary content tests (`cli_gc.rs`): the data directory is kept private by its mode bits.
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

/// Every start seeds its checkpoints (an inline bound of 64 bytes, as `placement_binary.rs` forces
/// it) and runs the collector every 200 ms, so a conversion goes a pass at a time while the test
/// looks; the grace keeps every seed past the test's run.
const NODE_ENV: &[(&str, &str)] = &[
    ("FOCAL_SEED_INLINE_BYTES", "64"),
    ("FOCAL_GC_INTERVAL_MS", "200"),
];

/// Start the node on `root` announcing `announced` (a staged rollout, 24 §21), or the binary's own
/// level, and wait for its readiness.
fn start(root: &Path, advertise: &str, announced: Option<&str>) -> Server {
    let (mut server, receive) = spawn(root, advertise, announced);
    ready(&receive, &mut server, root);
    server
}
/// The readiness the node on `root` published.
fn ready(receive: &mpsc::Receiver<Value>, server: &mut Server, root: &Path) {
    let status = progress::reported(receive, &mut server.0, root, Duration::from_secs(30))
        .expect("the node did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
}
/// The node on `root` started, its readiness to come on the receiver.
fn spawn(root: &Path, advertise: &str, announced: Option<&str>) -> (Server, mpsc::Receiver<Value>) {
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
                let _ = send.send(value);
                break;
            }
        }
    });
    (Server(child), receive)
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
    serde_json::from_slice(&output.stdout).unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .args(["--format", "json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
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
const FAR: u64 = 4_102_444_800_000;

/// A claim for `target`, the `index`th of the journey.
fn claim(root: &Path, target: &str, index: u64) {
    let document = serde_json::json!({
        "description": format!("Run suite {index} and deliver its report."),
        "target": target,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
             "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
             "deadline": {"at": FAR}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    });
    committed(&cli(
        root,
        &["submit", "claim", "--json", &document.to_string()],
    ));
}

/// A checkpoint of the node's session, retried while the node's first proposals are still in
/// flight (as `placement_binary.rs` takes one).
fn checkpoint(root: &Path) {
    let mut deadline = Deadline::after(Duration::from_secs(60));
    loop {
        let output = run(root, &["checkpoint", "replica"]);
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

/// The seed files of every session directory the node keeps, by name.
fn seed_files(root: &Path) -> Vec<(std::path::PathBuf, String)> {
    let mut files = Vec::new();
    let Ok(sessions) = std::fs::read_dir(root.join("seeds")) else {
        return files;
    };
    for session in sessions.flatten() {
        for file in std::fs::read_dir(session.path()).unwrap().flatten() {
            let name = file.file_name().to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".seed") {
                files.push((file.path(), stem.to_owned()));
            }
        }
    }
    files
}
/// Whether `stem` names a chunk in the clear: 64 lowercase hex digits, its hash.
fn clear_name(stem: &str) -> bool {
    stem.len() == 64
        && stem
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
/// The chunks the node's seed directories hold, by their hashes, read through each with the node's
/// keys.
fn seed_chunks(root: &Path) -> Vec<focal_model::ContentHash> {
    let mut chunks = Vec::new();
    let keys = focal_seal::test_keys(root).unwrap();
    let Ok(sessions) = std::fs::read_dir(root.join("seeds")) else {
        return chunks;
    };
    for session in sessions.flatten() {
        let reader = focal_evidence::SeedReader::open(session.path(), keys.clone()).unwrap();
        chunks.extend(reader.chunks().unwrap());
    }
    chunks.sort_unstable();
    chunks.dedup();
    chunks
}
/// The seed directories' conversion, as the collector's last pass left it in `inspect node --gc`;
/// none while the node does not answer or has made no pass.
fn conversion(root: &Path) -> Option<Value> {
    let output = run(root, &["inspect", "node", "--gc"]);
    if !output.status.success() {
        return None;
    }
    let shown: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(shown["result"]["kind"], "gc", "{shown}");
    let conversion = shown["result"]["gc"]["last"]["seeds"]["conversion"].clone();
    (!conversion.is_null()).then_some(conversion)
}
/// The cold streams' directories at rest (29 §12.6) as the same pass left them, and how many
/// replicas it visited: each keeps a data stream and a marks stream.
fn cold(root: &Path) -> Option<(Value, u64)> {
    let output = run(root, &["inspect", "node", "--gc"]);
    if !output.status.success() {
        return None;
    }
    let shown: Value = serde_json::from_slice(&output.stdout).unwrap();
    let seeds = &shown["result"]["gc"]["last"]["seeds"];
    let replicas = seeds["replicas"].as_u64()?;
    (!seeds["cold"].is_null() && replicas > 0).then(|| (seeds["cold"].clone(), replicas))
}
/// Every open chunk of the cold streams under `root`: each stream's directory under `cold/`.
fn cold_open_chunks(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let Ok(sessions) = std::fs::read_dir(root.join("cold")) else {
        return found;
    };
    for session in sessions {
        for stream in ["data", "marks"] {
            let Ok(entries) = std::fs::read_dir(session.as_ref().unwrap().path().join(stream))
            else {
                continue;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("open."))
                {
                    found.push(path);
                }
            }
        }
    }
    found
}
fn field(view: &Value, name: &str) -> u64 {
    view[name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name} in {view}"))
}
/// A look that agrees with itself: what is left is what was counted less what was converted and
/// found gone.
fn consistent(view: &Value) {
    for unit in ["files", "bytes"] {
        let counted = field(view, &format!("{unit}_counted"));
        let converted = field(view, &format!("{unit}_converted"));
        let gone = field(view, &format!("{unit}_gone"));
        let left = field(view, &format!("{unit}_left"));
        assert_eq!(converted + gone + left, counted, "{view}");
    }
}

/// Do: found a node announcing the level below the at-rest level and seed three checkpoints of its
/// session; raise the fence to the at-rest level once the node, restarted at the binary's level,
/// reports it; once the node has recorded the fence, restart it, take writes, and read `inspect node
/// --gc` until the session's seed directory is sealed; then checkpoint again. Expect: below the
/// fence every seed is named by its hash and holds the bytes it hashes to; every look while it
/// converts agrees with itself and its converted chunks never fall; it ends sealed with every chunk
/// counted in the clear converted, none gone or left, nothing named in the clear, every chunk the
/// clear directory held held sealed, and the checkpoint after sealing seeds of its own.
#[test]
fn a_seed_directory_in_the_clear_converts_while_the_session_serves() {
    let directory = tempfile::Builder::new()
        .prefix("focal-seeds-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = directory.path();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let advertise = ports::address();
    let at_rest = focal_node::upgrade::AT_REST_LEVEL;
    assert!(
        (2..=focal_node::upgrade::CAPABILITY_LEVEL).contains(&at_rest),
        "this journey rehearses the rollout that opens the at-rest level"
    );
    let below = (at_rest - 1).to_string();
    let level = at_rest.to_string();
    let client = tempfile::Builder::new()
        .prefix("focal-seeds-client-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(client.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

    // Founded below the at-rest level: the session's seeds are written in the clear. The rollout's
    // binary announces its level to every command, the activation founding the cluster's fence as
    // the start does.
    let activation = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "activate", "native"])
        .env("FOCAL_CAPABILITY_LEVEL", &below)
        .output()
        .unwrap();
    assert!(activation.status.success(), "{activation:?}");
    let server = start(root, &advertise, Some(&below));
    let invitation = client.path().join("alice.invite");
    admin(
        root,
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
        client.path(),
        &[
            "enroll",
            "context",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    assert!(enrolled.status.success(), "{enrolled:?}");
    let standing = admin(
        client.path(),
        &["--client-context", "alice", "inspect", "prefix"],
    );
    let alice = hex_hash(
        &standing["result"]["page"]["objects"].as_array().unwrap()[0]["Standing"]["principal"],
    );
    let mut claims = 0;
    for _ in 0..3 {
        for _ in 0..4 {
            claim(root, &alice, claims);
            claims += 1;
        }
        checkpoint(root);
    }
    drop(server);
    // In the clear, as every earlier binary wrote them: each chunk named by its hash, holding the
    // bytes that hash to its name.
    let clear = seed_files(root);
    assert!(clear.len() >= 3, "three seeded checkpoints left {clear:?}");
    for (path, stem) in &clear {
        assert!(clear_name(stem), "{stem} is not a hash in the clear");
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(&blake3::hash(&bytes).to_hex().to_string(), stem);
    }
    let held = seed_chunks(root);
    assert_eq!(held.len(), clear.len());

    // At the binary's level the fence rises; the seeds stay as they opened, in the clear, until
    // the node starts again having recorded the fence.
    let server = start(root, &advertise, None);
    let mut deadline = Deadline::after(Duration::from_secs(90));
    loop {
        let output = run(root, &["inspect", "upgrade"]);
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
    let activated = admin(root, &["activate", "upgrade", "--fence", &level]);
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    let mut deadline = Deadline::after(Duration::from_secs(60));
    while focal_node::storage_level::known(root).unwrap() < Some(at_rest) {
        assert!(deadline.open(), "the node never recorded the fence");
        std::thread::sleep(Duration::from_millis(100));
    }
    // The session's cold streams opened in the clear too, and the collector says so: each
    // replica's two directories.
    let mut deadline = Deadline::after(Duration::from_secs(60));
    loop {
        if let Some((view, replicas)) = cold(root)
            && field(&view, "clear") == 2 * replicas
        {
            assert_eq!(field(&view, "sealed"), 0, "{view}");
            break;
        }
        assert!(
            deadline.open(),
            "the collector never reported the cold streams in the clear"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        cold_open_chunks(root)
            .iter()
            .all(|path| !std::fs::read(path).unwrap().starts_with(b"FCLCOLD1")),
        "a cold stream sealed below the fence"
    );
    drop(server);
    let clear = seed_files(root);
    assert!(clear.iter().all(|(_, stem)| clear_name(stem)));
    let clear_bytes: u64 = clear
        .iter()
        .map(|(path, _)| std::fs::metadata(path).unwrap().len())
        .sum();

    // Restarted, the session opens its seeds to seal them, and the collector converts them while
    // the session takes writes.
    let (mut server, receive) = spawn(root, &advertise, None);
    ready(&receive, &mut server, root);
    let mut looks = 0u64;
    let mut converted = 0u64;
    let mut deadline = Deadline::after(Duration::from_secs(120));
    let last = loop {
        // The session serves throughout.
        claim(root, &alice, claims);
        claims += 1;
        let Some(view) = conversion(root) else {
            assert!(deadline.open(), "the collector never reported the seeds");
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        consistent(&view);
        looks += 1;
        let now = field(&view, "files_converted");
        assert!(now >= converted, "converted fell from {converted}: {view}");
        converted = now;
        assert_eq!(field(&view, "stopped"), 0, "{view}");
        assert_eq!(field(&view, "clear"), 0, "{view}");
        if field(&view, "sealed") == 1
            && field(&view, "counting") == 0
            && field(&view, "converting") == 0
        {
            break view;
        }
        assert!(
            deadline.open(),
            "the seed directory never finished converting: {view}"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    println!(
        "{looks} looks at the seeds; {} chunks of {clear_bytes} bytes converted",
        clear.len()
    );
    assert_eq!(field(&last, "files_counted"), clear.len() as u64, "{last}");
    assert_eq!(field(&last, "bytes_counted"), clear_bytes, "{last}");
    assert_eq!(
        field(&last, "files_converted"),
        clear.len() as u64,
        "{last}"
    );
    assert_eq!(field(&last, "bytes_converted"), clear_bytes, "{last}");
    assert_eq!(field(&last, "files_gone"), 0, "{last}");
    assert_eq!(field(&last, "files_left"), 0, "{last}");
    // Nothing named in the clear; every chunk the clear directory held is held sealed.
    let sealed = seed_files(root);
    assert!(
        sealed.iter().all(|(_, stem)| !clear_name(stem)),
        "{sealed:?}"
    );
    let now = seed_chunks(root);
    for chunk in &held {
        assert!(now.contains(chunk), "{chunk} not held sealed");
    }

    // The cold streams opened sealed: every directory reported sealed, every open chunk a record
    // file under a key of its own.
    let (view, replicas) = cold(root).expect("the collector reported the cold streams");
    consistent(&view);
    assert_eq!(field(&view, "sealed"), 2 * replicas, "{view}");
    for form in ["clear", "counting", "converting", "stopped"] {
        assert_eq!(field(&view, form), 0, "{view}");
    }
    let open = cold_open_chunks(root);
    assert!(!open.is_empty(), "no cold stream open");
    for path in &open {
        assert!(
            std::fs::read(path).unwrap().starts_with(b"FCLCOLD1"),
            "{} is not a record file",
            path.display()
        );
    }

    // A checkpoint taken now seals its seeds too.
    checkpoint(root);
    let after = seed_chunks(root);
    assert!(after.len() > now.len(), "the checkpoint sealed no seed");
    assert!(
        seed_files(root).iter().all(|(_, stem)| !clear_name(stem)),
        "a seed written in the clear after the conversion"
    );
    drop(server);
}
