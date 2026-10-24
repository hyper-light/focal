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
//! A content store written in the clear and converted by the real binary while an operator
//! watches (29 §11, 26 §5). A node founded below the content level keeps its store in the clear;
//! its objects are laid out there in the layout every binary below the level writes; the fence
//! rises to the content level, and the node records it; restarted, the node counts what it must
//! convert and converts it as it serves. `inspect node --gc` and the metrics page are read at every
//! step: the form and the tally agree with themselves at every look, and the store ends sealed,
//! every file counted converted, every object whole. Unix-only, as the other real-binary content
//! tests (`cli_gc.rs`): the data directory is kept private by its mode bits.
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
fn cli(root: &Path, context: Option<&str>, args: &[&str]) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap()]);
    if let Some(context) = context {
        command.args(["--client-context", context]);
    }
    let output = command
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
fn created(result: &Value, kind: &str) -> String {
    result["created"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["kind"] == kind)
        .map(|entry| hex_hash(&entry["id"]))
        .unwrap_or_else(|| panic!("no {kind} created: {result}"))
}
fn domain_of(tenant: &str) -> focal_model::ContentDomainId {
    let mut domain = [0u8; 16];
    for (index, byte) in domain.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&tenant[index * 2..index * 2 + 2], 16).unwrap();
    }
    focal_model::ContentDomainId(domain)
}
const FAR: u64 = 4_102_444_800_000;
/// Work artifacts delivered through the node below the content level, their payloads sealed as
/// objects in the clear.
const ARTIFACTS: u64 = 4;

/// The content store as `inspect node --gc` shows it.
fn content(root: &Path) -> Value {
    let shown = admin(root, &["inspect", "node", "--gc"]);
    assert_eq!(shown["result"]["kind"], "gc", "{shown}");
    shown["result"]["gc"]["content"].clone()
}
fn metrics(root: &Path) -> String {
    let output = run(root, &["inspect", "node", "--metrics"]);
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).unwrap()
}
fn field(view: &Value, name: &str) -> u64 {
    view[name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}: {view}"))
}
/// A look at the store agrees with itself: a form, left the counted less converted and gone,
/// nothing stopped.
fn consistent(view: &Value) {
    assert!(
        matches!(
            view["form"].as_str(),
            Some("counting" | "converting" | "sealed")
        ),
        "{view}"
    );
    for unit in ["files", "bytes"] {
        assert_eq!(
            field(view, &format!("{unit}_left")),
            field(view, &format!("{unit}_counted"))
                - field(view, &format!("{unit}_converted"))
                - field(view, &format!("{unit}_gone")),
            "{view}"
        );
    }
    assert!(view["stopped"].is_null(), "{view}");
}
/// The metrics page agrees with itself: while it shows the conversion, every form's series with
/// exactly one 1 (and that one counting or converting), and left the counted less converted and
/// gone; at rest, none of the conversion's families.
fn page_consistent(text: &str) -> Option<&'static str> {
    let value = |line: &str| -> u64 { line.rsplit_once(' ').unwrap().1.parse().unwrap() };
    let forms: Vec<&str> = text
        .lines()
        .filter(|line| line.starts_with("focal_content_form{"))
        .collect();
    if forms.is_empty() {
        assert!(!text.contains("focal_content_conversion_"), "{text}");
        return None;
    }
    assert_eq!(forms.len(), 4, "{forms:?}");
    let on: Vec<&&str> = forms.iter().filter(|line| value(line) == 1).collect();
    assert_eq!(on.len(), 1, "{forms:?}");
    let form = if on[0].contains("\"counting\"") {
        "counting"
    } else if on[0].contains("\"converting\"") {
        "converting"
    } else {
        panic!("a form at rest shown: {forms:?}")
    };
    for unit in ["files", "bytes"] {
        let state = |state: &str| -> u64 {
            let prefix = format!("focal_content_conversion_{unit}{{state=\"{state}\"}} ");
            value(
                text.lines()
                    .find(|line| line.starts_with(&prefix))
                    .unwrap_or_else(|| panic!("no {prefix}")),
            )
        };
        assert_eq!(
            state("left"),
            state("counted") - state("converted") - state("gone")
        );
    }
    Some(form)
}
fn limits() -> focal_evidence::StoreLimits {
    focal_evidence::StoreLimits {
        max_content_bytes: 64 * 1024 * 1024,
        max_staging_bytes: 128 * 1024 * 1024,
        max_uploads: 16,
        chunk_bytes: 1024 * 1024,
        max_manifest_bytes: 1024 * 1024,
    }
}
/// An object's bytes, its own for each `seed`: SplitMix64's stream from the seed (Steele, Lea and
/// Flood, OOPSLA 2014), so no two objects share their content and none is stored once for two.
fn payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        out.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    out.truncate(len);
    out
}
/// The files in the clear under a store's objects and its quarantine, as the conversion counts
/// them, and their bytes.
fn clear_files(store: &Path) -> (u64, u64) {
    let (mut files, mut bytes) = (0u64, 0u64);
    let mut stack = vec![store.join("objects"), store.join("quarantine")];
    while let Some(directory) = stack.pop() {
        let Ok(listing) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in listing {
            let entry = entry.unwrap();
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                stack.push(path);
                continue;
            }
            let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if kind.is_file()
                && (extension == "chunk" || extension == "manifest")
                && stem.len() == 64
            {
                files += 1;
                bytes += entry.metadata().unwrap().len();
            }
        }
    }
    (files, bytes)
}

/// Objects enough that converting them (a flush a file) outlasts the node's start and the first
/// looks by seconds: the operator reads the store as it converts, not only once it has.
const OBJECTS: u64 = 640;
const OBJECT_BYTES: usize = 16 * 1024;

/// Do: found a node announcing the level below the content level; lay out objects in its store
/// in the clear; restart it at the binary's level and raise the fence to the content level; once
/// the node has recorded the fence, restart it, and read `inspect node --gc` and the metrics page
/// until the store is sealed. Expect: the store reads clear below the fence; every look while it
/// converts agrees with itself and its converted files never fall; it ends sealed with every file
/// counted in the clear converted, none gone, left or stopped, nothing named in the clear on disk,
/// every object whole through the store's reader, and the page at rest showing none of the
/// conversion's families.
#[test]
fn a_store_in_the_clear_converts_while_an_operator_watches() {
    let directory = tempfile::Builder::new()
        .prefix("focal-form-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = directory.path();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let advertise = ports::address();
    let content_level = focal_node::upgrade::CONTENT_LEVEL;
    assert!(
        (2..=focal_node::upgrade::CAPABILITY_LEVEL).contains(&content_level),
        "this journey rehearses the rollout that opens the content level"
    );
    let below = (content_level - 1).to_string();
    let level = content_level.to_string();
    let client = tempfile::Builder::new()
        .prefix("focal-form-client-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(client.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

    // Founded below the content level: the store writes in the clear. Work artifacts delivered
    // through the node there have their payloads sealed as objects, in the clear.
    // Every command of that rollout's binary announces its level: the activation founds the
    // cluster's fence as the start does.
    let activation = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "activate", "native"])
        .env("FOCAL_CAPABILITY_LEVEL", &below)
        .output()
        .unwrap();
    assert!(activation.status.success(), "{activation:?}");
    let activation: Value = serde_json::from_slice(&activation.stdout).unwrap();
    assert_eq!(activation["activated"], true, "{activation}");
    let server = start(root, &advertise, Some(&below));
    assert_eq!(content(root)["form"], "clear");
    let identity = admin(root, &["inspect", "node", "--identity"])["result"]["identity"].clone();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
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
    let mut payloads = Vec::new();
    for index in 0..ARTIFACTS {
        let document = serde_json::json!({
            "description": format!("Run suite {index} and deliver its report."),
            "target": alice,
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
                {"kind": "test", "description": "The suite passes.", "target": {"type": "slot", "index": 0, "name": "report"},
                 "evaluator": "self", "handlers": [{"id": format!("{:032x}", 77), "version": format!("{:064x}", 77)}],
                 "deadline": {"at": FAR}}
            ],
            "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
        });
        let claim = created(
            &committed(&cli(
                root,
                None,
                &["submit", "claim", "--json", &document.to_string()],
            )),
            "Claim",
        );
        committed(&cli(root, None, &["post", "claim", &claim]));
        committed(&cli(
            client.path(),
            Some("alice"),
            &["acquire", "receipt", &claim],
        ));
        let payload = format!(r#"{{"passed":{},"failed":0,"skipped":0}}"#, index + 3);
        committed(&cli(
            client.path(),
            Some("alice"),
            &[
                "submit", "artifact", "--claim", &claim, "--slot", "0", "--text", &payload,
            ],
        ));
        payloads.push(payload.into_bytes());
    }
    drop(server);

    // The objects such a node holds, laid out in the clear by the store's own writer under the
    // node's keys, while the node is down (the store has one writer).
    let store = root.join("content");
    let keys = focal_seal::test_keys(root).unwrap();
    let mut objects = Vec::new();
    {
        let mut writer = focal_evidence::ContentStore::open_with_disk(
            &store,
            limits(),
            focal_memory::DiskBudget::new(focal_memory::DiskBudgetConfig::unbounded()).unwrap(),
            &keys,
            focal_evidence::ContentForm::Clear,
        )
        .unwrap();
        let domain = focal_model::ContentDomainId([7; 16]);
        for index in 0..OBJECTS {
            let bytes = payload(index, OBJECT_BYTES);
            let mut upload = [0u8; 16];
            upload[..8].copy_from_slice(&index.to_le_bytes());
            let upload = focal_evidence::UploadId(upload);
            writer
                .begin(
                    upload,
                    domain,
                    focal_model::ContentClass::Evidence,
                    bytes.len() as u64,
                    None,
                )
                .unwrap();
            writer.append(upload, 0, &bytes).unwrap();
            let reference = writer.seal(upload).unwrap();
            writer.finish(upload).unwrap();
            objects.push((reference, bytes));
        }
    }
    assert!(
        clear_files(&store).0 >= 2 * (OBJECTS + ARTIFACTS),
        "a chunk and a manifest an object"
    );

    // At the binary's level the fence rises; the store stays as it opened, in the clear, until the
    // node starts again having recorded the fence.
    let server = start(root, &advertise, None);
    assert_eq!(content(root)["form"], "clear");
    // The fence rises only to a level every node reports (24 §21): the node's report first.
    let mut deadline = Deadline::after(Duration::from_secs(90));
    loop {
        let output = run(root, &["inspect", "upgrade"]);
        if output.status.success()
            && let Ok(value) = serde_json::from_slice::<Value>(&output.stdout)
            && let Some(nodes) = value["result"]["upgrade"]["nodes"].as_array()
            && !nodes.is_empty()
            && nodes
                .iter()
                .all(|node| node["capability"].as_u64() >= Some(u64::from(content_level)))
        {
            break;
        }
        assert!(deadline.open(), "the node never reported the content level");
        std::thread::sleep(Duration::from_millis(200));
    }
    let activated = admin(root, &["activate", "upgrade", "--fence", &level]);
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    let mut deadline = Deadline::after(Duration::from_secs(60));
    while focal_node::storage_level::known(root).unwrap() < Some(content_level) {
        assert!(deadline.open(), "the node never recorded the fence");
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(server);
    // What the start that converts will find, the node down: every file in the clear its store
    // holds, the artifacts' payloads and the objects laid out beside them.
    let (files, bytes) = clear_files(&store);

    // Restarted, the node counts what it must convert and converts it as it serves.
    let began = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // The operator looks from the moment the node runs, as its store converts from its open on:
    // a look the node cannot answer yet (its admin or its store not up) is no look.
    let (mut server, receive) = spawn(root, &advertise, None);
    let mut looks = 0u64;
    let mut converting_looks = 0u64;
    let mut pages_showing = 0u64;
    let mut converted = 0u64;
    let mut deadline = Deadline::after(Duration::from_secs(120));
    let last = loop {
        let output = run(root, &["inspect", "node", "--gc"]);
        let view = if output.status.success() {
            let shown: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(shown["result"]["kind"], "gc", "{shown}");
            shown["result"]["gc"]["content"].clone()
        } else {
            Value::Null
        };
        if view.is_null() {
            assert!(deadline.open(), "the node never answered for its store");
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        consistent(&view);
        looks += 1;
        let now = field(&view, "files_converted");
        assert!(now >= converted, "converted fell from {converted}: {view}");
        converted = now;
        if view["form"] == "sealed" {
            break view;
        }
        converting_looks += 1;
        let page = run(root, &["inspect", "node", "--metrics"]);
        if page.status.success()
            && page_consistent(&String::from_utf8(page.stdout).unwrap()).is_some()
        {
            pages_showing += 1;
        }
        assert!(
            deadline.open(),
            "the store never finished converting: {view}"
        );
    };
    ready(&receive, &mut server, root);
    println!(
        "{looks} looks at the store, {converting_looks} while it counted or converted, \
         {pages_showing} pages showing the conversion; {files} files of {bytes} bytes; \
         converted from {} to {} ms after the start began",
        field(&last, "started_ms").saturating_sub(began),
        field(&last, "finished_ms").saturating_sub(began),
    );
    assert_eq!(field(&last, "files_counted"), files, "{last}");
    assert_eq!(field(&last, "bytes_counted"), bytes, "{last}");
    assert_eq!(field(&last, "files_converted"), files, "{last}");
    assert_eq!(field(&last, "bytes_converted"), bytes, "{last}");
    assert_eq!(field(&last, "files_gone"), 0, "{last}");
    assert_eq!(field(&last, "files_left"), 0, "{last}");
    assert_eq!(last["resumed"], false, "{last}");
    assert!(
        field(&last, "finished_ms") >= field(&last, "started_ms"),
        "{last}"
    );
    assert!(field(&last, "started_ms") > 0, "{last}");
    assert_eq!(clear_files(&store), (0, 0));

    // Every object whole through the store's reader, from its sealed form: the laid-out objects
    // by their references, and every artifact's payload among the objects of the tenant's
    // domain, each read back verified.
    let reader = focal_evidence::ContentReader::open(&store, keys).unwrap();
    for (reference, bytes) in &objects {
        assert_eq!(&reader.read_bytes(reference, 1 << 20).unwrap(), bytes);
    }
    let domain = domain_of(&tenant);
    let mut stored = Vec::new();
    for root in reader.manifest_roots(domain).unwrap() {
        let described = reader.describe_object(domain, root).unwrap();
        stored.push(reader.read_bytes(described.reference(), 1 << 20).unwrap());
    }
    for payload in &payloads {
        assert!(
            stored.contains(payload),
            "{} not read back among {} objects",
            String::from_utf8_lossy(payload),
            stored.len()
        );
    }

    // The page at rest shows none of the conversion's families, once it is sampled after the
    // seal.
    let mut deadline = Deadline::after(Duration::from_secs(60));
    while page_consistent(&metrics(root)).is_some() {
        assert!(deadline.open(), "the page still shows the conversion");
        std::thread::sleep(Duration::from_millis(100));
    }
}
