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
//! Stage 1, the laptop (08 §2; DC01, DC02, DC04, DC13, DC15, DC16, DC17):
//! a directory, a loopback address and one command; the claims demo
//! between the node's principal and a participant enrolled on the same
//! machine; a crash and a restart that read back the same records; a full
//! volume, an unwritable directory and a second writer refused without a
//! volatile acknowledgement; a backup restored on a fresh laptop as a
//! recovery incarnation, and only when the operator says so; explain
//! naming requested, effective and observed values; a dry run that creates
//! nothing, a plan of another deployment and a tampered plan refused; and
//! malformed input refused without echoing it, with every output searched
//! for the participant's invitation token.
//!
//! The loopback address is the laptop's one input beyond its directory: the
//! native engine admits no self-issued work (a claim needs a second
//! principal), and a second principal on the same machine is a client
//! context, which connects to the node's listener.
use serde_json::Value;
use std::{collections::BTreeSet, os::unix::fs::PermissionsExt, path::Path};

#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/journey.rs"]
mod journey;
use fleet::*;
use journey::*;

/// Every file under a directory, relative, sorted.
fn files(root: &Path) -> BTreeSet<String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out);
    out
}

#[test]
fn the_laptop_stage_needs_a_directory_an_address_and_one_command() {
    let mut journey = Journey::new("laptop", "deployment_laptop");
    let laptop = Node::new("laptop");
    let loopback = address();
    // ---- DC01: an empty directory, the native engine, one start on a
    // loopback address; the demo with a participant enrolled on the same
    // machine; a crash; the same records after the restart.
    journey.admin(
        &laptop,
        "native engine",
        &["data directory"],
        &["cluster", "replicas", "activate-native"],
    );
    let server = journey.start(
        &laptop,
        "start",
        &["data directory", "loopback address"],
        &["--advertise", &loopback],
    );
    let demo = journey.enroll(&laptop, "alice");
    let token = Journey::client_token(&demo);
    let first = journey.demo(&laptop, &demo, "on the laptop");
    let claim = first.claim.clone();
    assert!(first.artifact.is_some());
    assert!(
        !first.claim_object["Claim"]["receipt"].is_null(),
        "{}",
        first.claim_object
    );
    journey.manual("crash", &[], &["kill", "-KILL", "<laptop process>"]);
    drop(server);
    let server = journey.start(&laptop, "start", &["data directory"], &[]);
    journey.same(&laptop, &first);
    // ---- DC02: a second writer of the same directory is refused while the
    // first lives; an unwritable directory is refused; a full volume
    // refuses the write that does not fit and acknowledges nothing.
    let (code, report) = journey.failure(&laptop, None, "one writer", &[], &["start"]);
    assert_eq!(code, 6, "{report}");
    assert!(report.contains("[directory_owned]"), "{report}");
    let unwritable = Node::new("unwritable");
    std::fs::set_permissions(unwritable.root(), std::fs::Permissions::from_mode(0o500)).unwrap();
    journey.manual(
        "unwritable directory",
        &[],
        &["chmod", "0500", "<unwritable>"],
    );
    let (code, report) =
        journey.failure(&unwritable, None, "unwritable directory", &[], &["start"]);
    std::fs::set_permissions(unwritable.root(), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[permission_denied]"), "{report}");
    assert!(files(unwritable.root()).is_empty());
    drop(server);
    let limit = largest_file(laptop.root()) + 8 * 1024;
    journey.manual(
        "full volume",
        &["file size limit"],
        &[
            "ulimit",
            "-f",
            "<blocks>",
            "focal",
            "--data-dir",
            "<laptop>",
            "start",
        ],
    );
    let server = start_limited(&laptop, &[], limit.div_ceil(512));
    let big = "x".repeat(64 * 1024);
    let refused = journey.run(
        &laptop,
        None,
        "claim",
        &["claim document"],
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&demo.principal, &big).to_string(),
            "--format",
            "json",
        ],
    );
    assert!(
        !refused.status.success(),
        "an oversized write was acknowledged: {}",
        String::from_utf8_lossy(&refused.stdout)
    );
    drop(server);
    let server = journey.start(&laptop, "start", &["data directory"], &[]);
    journey.same(&laptop, &first);
    let later = journey.write(&laptop, &demo, "after the volume was freed");
    assert_ne!(later, claim);
    // Planning and explaining read the directory, which names the node
    // once it has installed.
    let identity = journey.admin(&laptop, "identity", &[], &["cluster", "node", "identity"])
        ["result"]["identity"]
        .clone();
    let laptop_node = identity["node"].as_u64().unwrap();
    let tenant = identity["tenant"].as_str().unwrap().to_owned();
    wait_for(
        &laptop,
        "the directory naming the laptop",
        std::time::Duration::from_secs(60),
        |view| {
            node_row(view, laptop_node).is_some()
                && ids(&view["partitions"][0]["sessions"][0]["voters"]).contains(&laptop_node)
        },
    );
    let view = journey.admin(&laptop, "placement view", &[], &["cluster", "placement"]);
    assert!(
        node_row(&view["result"]["placement"], laptop_node).is_some(),
        "{view}"
    );
    // ---- DC15: explain names what was requested, what is effective and
    // where every value came from; the golden holds the shape.
    let explained = journey.admin(&laptop, "explain", &[], &["deployment", "explain"]);
    let golden: Value = serde_json::from_str(
        &std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/deployment/explain-laptop.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let mut shape = explained.clone();
    shape["observed"]["observed_at"] = Value::from(0);
    for node in shape["observed"]["nodes"].as_array_mut().unwrap() {
        node["node"] = Value::from(0);
    }
    for session in shape["observed"]["sessions"].as_array_mut().unwrap() {
        session["tenant"] = Value::from("<id>");
        session["session"] = Value::from("<id>");
    }
    for member in ["voters", "content_copies"] {
        shape["plan"][member] = Value::from(vec![0]);
    }
    shape["plan"]["preferred_leader"] = Value::from(0);
    assert_eq!(
        shape,
        golden,
        "{}",
        serde_json::to_string_pretty(&shape).unwrap()
    );
    // ---- DC16: a dry run creates nothing under the directory; a written
    // plan is immutable; a plan that cannot hold its guarantee is refused
    // before any side effect, as is a tampered one.
    let policy = laptop.root().join("policy.yaml");
    std::fs::write(
        &policy,
        "version: 1\ndurability:\n  survive: node\n  max_failures: 1\n",
    )
    .unwrap();
    let before = files(laptop.root());
    let dry = journey.admin(
        &laptop,
        "plan",
        &["policy file"],
        &[
            "--config",
            policy.to_str().unwrap(),
            "deployment",
            "plan",
            "--dry-run",
        ],
    );
    assert_eq!(dry["result"]["dry_run"], true, "{dry}");
    assert!(
        !dry["result"]["plan"]["blocked"]
            .as_array()
            .unwrap()
            .is_empty(),
        "one laptop cannot survive a node loss: {dry}"
    );
    assert_eq!(
        files(laptop.root()),
        before,
        "a dry run wrote under the directory"
    );
    let plan_file = laptop.root().join("laptop.plan");
    journey.admin(
        &laptop,
        "plan",
        &["policy file", "plan file"],
        &[
            "--config",
            policy.to_str().unwrap(),
            "deployment",
            "plan",
            "--output",
            plan_file.to_str().unwrap(),
        ],
    );
    let bytes = std::fs::read(&plan_file).unwrap();
    assert_eq!(&bytes[..8], b"FCLPLAN1");
    let (code, report) = journey.failure(
        &laptop,
        None,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 6, "{report}");
    assert!(report.contains("[guarantee_unsatisfied]"), "{report}");
    assert!(!laptop.root().join("cluster/apply").exists());
    let mut tampered = bytes.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    let tampered_file = laptop.root().join("tampered.plan");
    std::fs::write(&tampered_file, &tampered).unwrap();
    let (code, report) = journey.failure(
        &laptop,
        None,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            tampered_file.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 2, "{report}");
    assert!(report.contains("[plan_corrupt]"), "{report}");
    // ---- DC04: the laptop's records restored on a fresh laptop. The
    // source cannot be fenced from there, so the restore is a recovery
    // incarnation, and only when the operator says so; the other laptop
    // refuses this one's plan by name.
    let backup = laptop.root().join("backup");
    let created = journey.admin(
        &laptop,
        "backup",
        &["backup directory"],
        &[
            "cluster",
            "backup",
            "create",
            "--output",
            backup.to_str().unwrap(),
        ],
    );
    assert_eq!(created["result"]["kind"], "backup_created", "{created}");
    drop(server);
    journey.manual("crash", &[], &["kill", "-KILL", "<laptop process>"]);
    let other = Node::new("laptop-b");
    let other_loopback = address();
    journey.admin(
        &other,
        "native engine",
        &["data directory"],
        &["cluster", "replicas", "activate-native"],
    );
    let _other_server = journey.start(
        &other,
        "start",
        &["data directory", "loopback address"],
        &["--advertise", &other_loopback],
    );
    let foreign = journey.failure(
        &other,
        None,
        "apply",
        &["plan file"],
        &[
            "deployment",
            "apply",
            "--plan-file",
            plan_file.to_str().unwrap(),
        ],
    );
    assert_eq!(foreign.0, 2, "{}", foreign.1);
    assert!(foreign.1.contains("[wrong_deployment]"), "{}", foreign.1);
    journey.admin(
        &other,
        "tenant",
        &["tenant"],
        &["cluster", "tenants", "admit", "--tenant", &tenant],
    );
    let (code, report) = journey.failure(
        &other,
        None,
        "restore",
        &["backup directory"],
        &["cluster", "restore", "--input", backup.to_str().unwrap()],
    );
    assert_ne!(code, 0, "{report}");
    let restored = journey.admin(
        &other,
        "restore",
        &["backup directory"],
        &[
            "cluster",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--new-incarnation",
        ],
    );
    assert_eq!(restored["result"]["kind"], "restored", "{restored}");
    assert_eq!(
        restored["result"]["restored"]["decision"], "recovery_incarnation",
        "{restored}"
    );
    assert_eq!(restored["result"]["restored"]["objects_imported"], 1);
    // The restored session is a session of a served tenant on the other
    // laptop: a saved connection addresses it and reads the old records.
    let session = restored["result"]["restored"]["session"]
        .as_str()
        .unwrap()
        .to_owned();
    let reader = Node::new("reader");
    let added = journey.run(
        &reader,
        None,
        "connection",
        &["tenant", "session"],
        &[
            "context",
            "add",
            "restored",
            "--node-data-dir",
            other.root().to_str().unwrap(),
            "--tenant",
            &tenant,
            "--session",
            &session,
        ],
    );
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let restored_claim = loop {
        let output = fleet::run(
            &reader,
            Some("restored"),
            &["get", "claim", &claim, "--format", "json"],
        );
        if output.status.success()
            && let Ok(page) = serde_json::from_slice::<Value>(&output.stdout)
            && page["result"]["kind"] == "native_read"
        {
            break objects(&page)[0].clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the restored session never answered: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(std::time::Duration::from_millis(250));
    };
    journey.cli(
        &reader,
        Some("restored"),
        "read",
        &[],
        &["get", "claim", &claim],
    );
    assert_eq!(claim_id(&restored_claim), claim);
    assert_eq!(
        restored_claim["Claim"]["status"],
        first.claim_object["Claim"]["status"]
    );
    // ---- DC17: malformed input is refused without echoing it; no output
    // of this journey carried the participant's token.
    let garbage = other.root().join("garbage.invite");
    std::fs::write(&garbage, b"not an invitation").unwrap();
    std::fs::set_permissions(&garbage, std::fs::Permissions::from_mode(0o600)).unwrap();
    let fresh = Node::new("fresh");
    let (code, report) = journey.failure(
        &fresh,
        None,
        "start",
        &["invitation file"],
        &["start", "--invite-file", garbage.to_str().unwrap()],
    );
    assert_ne!(code, 0);
    assert!(!report.contains("not an invitation"), "{report}");
    let (code, _) = journey.failure(
        &other,
        None,
        "claim",
        &["claim document"],
        &["submit", "claim", "--json", "{\"description\": "],
    );
    assert_eq!(code, 2);
    journey.assert_redacted(&[&token, "not an invitation"]);
    journey.not_executed(
        "power loss: the crash is a kill; storage cuts at every durable boundary are the crash matrix (R11)",
    );
    journey.not_executed(
        "the demo through MCP: mcp_native_a1 runs the same claims through the MCP server",
    );
    journey.finish();
}
