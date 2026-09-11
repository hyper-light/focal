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
//! The runbooks (`docs/runbooks`), executed against the real binary: each
//! test walks one runbook's commands through the failure it names and
//! verifies what the runbook says holds and what it says recovers.
use serde_json::Value;
use std::{
    path::Path,
    time::{Duration, Instant},
};

#[path = "support/fleet.rs"]
mod fleet;
use fleet::*;

/// A founder with a client that writes claims: the workload every runbook
/// checks before and after its failure.
struct Workload {
    client: Node,
    alice: String,
}
impl Workload {
    fn new(founder: &Node) -> Self {
        let client = Node::new("client");
        let alice = enroll_client(founder, &client, "alice");
        Self { client, alice }
    }
    fn write(&self, node: &Node, what: &str) -> String {
        write_claim(node, &self.alice, what)
    }
    fn deliver(&self, claim: &str) -> String {
        deliver_artifact(&self.client, "alice", claim)
    }
}
fn founder_with(yaml: &str) -> Node {
    Node::with_config("founder", yaml)
}
fn wait_until(what: &str, timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("{what} did not happen within {timeout:?}");
}
fn alive(view: &Value, id: u64) -> bool {
    node_row(view, id).is_some_and(|row| row["alive"] == true)
}
fn guarantee(view: &Value, ledger: &str) -> (Option<u64>, usize) {
    let session = session_row(view, ledger).unwrap();
    (
        session["achieved_max_failures"].as_u64(),
        session["blocked_by"].as_array().map_or(0, Vec::len),
    )
}

#[test]
fn runbook_disk_exhaustion() {
    let founder = Node::new("founder");
    let address = address();
    activate_native(&founder);
    let server = start(&founder, &["--advertise", &address]);
    let workload = Workload::new(&founder);
    let before = workload.write(&founder, "before the volume fills");
    drop(server);
    // The volume "fills": nothing under the data directory may grow past
    // its present size plus a little, so the next sizeable write fails.
    let limit_bytes = largest_file(founder.root()) + 8 * 1024;
    let server = start_limited(
        &founder,
        &["--advertise", &address],
        limit_bytes.div_ceil(512),
    );
    let storage = admin(&founder, &["cluster", "storage", "show"]);
    assert_eq!(storage["result"]["kind"], "storage", "{storage}");
    // A write that needs more than the volume gives is refused, never
    // acknowledged: the claim carries a description larger than the room.
    let big = "x".repeat(64 * 1024);
    let output = run(
        &founder,
        None,
        &[
            "submit",
            "claim",
            "--json",
            &claim_document(&workload.alice, &big).to_string(),
            "--format",
            "json",
        ],
    );
    assert!(
        !output.status.success(),
        "an oversized write was acknowledged: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    // Reads of the durable prefix keep answering, or the node stopped its
    // owner and exits; either way nothing is lost.
    drop(server);
    let _server = start(&founder, &["--advertise", &address]);
    let ready = readiness(&founder);
    assert_eq!(ready["alive"], true, "{ready}");
    assert_eq!(ready["authoritative"], true, "{ready}");
    let claim = read_claim(&founder, &before);
    assert_eq!(claim_id(&claim), before);
    let after = workload.write(&founder, "after space was freed");
    assert_ne!(after, before);
    let listed = cli(&founder, None, &["get", "claim", &before]);
    assert_eq!(listed["result"]["kind"], "native_read");
}

#[test]
fn runbook_corrupt_or_missing_content() {
    let founder = Node::new("founder");
    let host_a = Node::new("host-a");
    let host_b = Node::new("host-b");
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    let activation = admin(&founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let workload = Workload::new(&founder);
    let claim = workload.write(&founder, "deliver the report");
    workload.deliver(&claim);
    let original = std::fs::read(&chunk_files(&founder, &tenant)[0]).unwrap();
    let (_server_a, node_a) = join_start(&founder, &host_a, "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &host_b, "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    let view = plan_and_settle(&founder, &tenant, &ledger, None, 1, &all);
    let copies = ids(&session_row(&view, &ledger).unwrap()["content_copies"]);
    assert!(copies.len() >= 2, "{view}");
    // Every copy holds the object verified before the failures.
    for (host, id) in [(&host_a, node_a), (&host_b, node_b)] {
        if copies.contains(&id) {
            wait_until("copy holds the object", Duration::from_secs(90), || {
                chunk_files(host, &tenant).len() == 1
            });
        }
    }
    let victims: Vec<(&Node, u64)> = [(&host_a, node_a), (&host_b, node_b)]
        .into_iter()
        .filter(|(_, id)| copies.contains(id))
        .collect();
    let (lost, _) = victims[0];
    // One copy loses the chunk: repair recopies it from another copy.
    let chunk = chunk_files(lost, &tenant).remove(0);
    std::fs::remove_file(&chunk).unwrap();
    let report = repair(lost, &tenant, &ledger);
    assert_eq!(report["repaired"], 1, "{report}");
    assert_eq!(report["unrecoverable_count"], 0, "{report}");
    assert_eq!(std::fs::read(&chunk).unwrap(), original);
    // The founder's copy holds it corrupt: repair replaces the bytes.
    let founder_chunk = chunk_files(&founder, &tenant).remove(0);
    let mut tampered = original.clone();
    tampered[0] ^= 0xff;
    std::fs::write(&founder_chunk, &tampered).unwrap();
    let report = repair(&founder, &tenant, &ledger);
    assert_eq!(report["repaired"], 1, "{report}");
    assert_eq!(std::fs::read(&founder_chunk).unwrap(), original);
    // Verification: a second run finds everything verified and nothing to
    // repair, and the artifact reads back.
    for node in [&founder, lost] {
        let again = repair(node, &tenant, &ledger);
        assert_eq!(again["verified"], 1, "{again}");
        assert_eq!(again["repaired"], 0, "{again}");
        assert_eq!(again["complete"], true, "{again}");
    }
    let read = read_claim(&founder, &claim);
    assert_eq!(claim_id(&read), claim);
}

/// Three voters for one tolerated loss; the founder, two hosts and their
/// ids, plus a workload.
fn three_voters(
    founder_yaml: Option<&str>,
    host_yaml: [Option<&str>; 2],
    survive: Option<&str>,
) -> (
    Node,
    [Node; 2],
    [Server; 3],
    [u64; 3],
    Workload,
    String,
    String,
) {
    let founder = match founder_yaml {
        Some(yaml) => founder_with(yaml),
        None => Node::new("founder"),
    };
    let hosts = [
        host_yaml[0].map_or_else(
            || Node::new("host-a"),
            |yaml| Node::with_config("host-a", yaml),
        ),
        host_yaml[1].map_or_else(
            || Node::new("host-b"),
            |yaml| Node::with_config("host-b", yaml),
        ),
    ];
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let workload = Workload::new(&founder);
    let (server_a, node_a) = join_start(&founder, &hosts[0], "host-a", &addresses[1]);
    let (server_b, node_b) = join_start(&founder, &hosts[1], "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    plan_and_settle(&founder, &tenant, &ledger, survive, 1, &all);
    (
        founder,
        hosts,
        [founder_server, server_a, server_b],
        all,
        workload,
        tenant,
        ledger,
    )
}

#[test]
fn runbook_stalled_replication() {
    let (founder, _hosts, servers, members, workload, _tenant, ledger) =
        three_voters(None, [None, None], None);
    let stalled = members[2];
    // The host is paused: alive to its OS, silent to its peers.
    servers[2].pause();
    let claim = workload.write(&founder, "while one voter is stalled");
    let view = wait_for(
        &founder,
        "the stalled voter is confirmed",
        Duration::from_secs(120),
        |view| !alive(view, stalled) && guarantee(view, &ledger).1 > 0,
    );
    let (achieved, blocked) = guarantee(&view, &ledger);
    assert!(blocked > 0, "{view}");
    assert!(achieved.is_none_or(|level| level < 1), "{view}");
    let ready = readiness(&founder);
    assert_eq!(ready["policy_satisfied"], false, "{ready}");
    assert_eq!(ready["authoritative"], true, "{ready}");
    // Resumed, the replica catches up and the guarantee is back.
    servers[2].resume();
    let view = wait_for(
        &founder,
        "the voter catches up",
        Duration::from_secs(120),
        |view| alive(view, stalled) && guarantee(view, &ledger) == (Some(1), 0),
    );
    assert_eq!(guarantee(&view, &ledger), (Some(1), 0));
    assert_eq!(claim_id(&read_claim(&founder, &claim)), claim);
    wait_until("policy satisfied again", Duration::from_secs(60), || {
        readiness(&founder)["policy_satisfied"] == true
    });
}

#[test]
fn runbook_node_loss() {
    let (founder, _hosts, mut servers, members, workload, tenant, ledger) =
        three_voters(None, [None, None], None);
    let spare = Node::new("spare");
    let spare_address = address();
    let (_spare_server, spare_id) = join_start(&founder, &spare, "spare", &spare_address);
    wait_for(
        &founder,
        "the spare reports",
        Duration::from_secs(90),
        |view| {
            node_row(view, spare_id)
                .is_some_and(|row| row["alive"] == true && row["disk_available"].is_number())
        },
    );
    // A voter is lost for good.
    let lost = members[1];
    drop(std::mem::replace(&mut servers[1], start_placeholder()));
    let claim = workload.write(&founder, "while one voter is lost");
    wait_for(
        &founder,
        "the loss is confirmed",
        Duration::from_secs(120),
        |view| !alive(view, lost),
    );
    let replaced = admin(
        &founder,
        &[
            "cluster",
            "nodes",
            "replace",
            "--node",
            &lost.to_string(),
            "--with",
            &spare_id.to_string(),
        ],
    )["result"]
        .clone();
    assert_eq!(replaced["kind"], "node_eligibility", "{replaced}");
    let view = wait_for(
        &founder,
        "healed onto the spare",
        Duration::from_secs(240),
        |view| {
            session_row(view, &ledger).is_some_and(|session| {
                session["pending"].is_null()
                    && session["achieved_max_failures"] == 1
                    && ids(&session["voters"]).contains(&spare_id)
                    && !ids(&session["voters"]).contains(&lost)
                    && session["retiring"].as_array().is_some_and(Vec::is_empty)
            })
        },
    );
    assert_eq!(guarantee(&view, &ledger), (Some(1), 0));
    let removed = wait_removed(&founder, lost);
    assert_eq!(removed["membership_removed"], true, "{removed}");
    assert_eq!(claim_id(&read_claim(&founder, &claim)), claim);
    let _ = tenant;
}
/// A `Server` that holds no process, for replacing a killed one.
fn start_placeholder() -> Server {
    Server(std::process::Command::new("true").spawn().unwrap())
}
fn wait_removed(founder: &Node, node: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let output = run(
            founder,
            None,
            &["cluster", "nodes", "remove", "--node", &node.to_string()],
        );
        if output.status.success() {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            return value["result"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "removal never succeeded: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn domain_loss(survive: &str, founder_yaml: &str, host_yaml: [&str; 2]) {
    let (founder, hosts, mut servers, members, workload, _tenant, ledger) = three_voters(
        Some(founder_yaml),
        [Some(host_yaml[0]), Some(host_yaml[1])],
        Some(survive),
    );
    let lost = members[2];
    drop(std::mem::replace(&mut servers[2], start_placeholder()));
    let claim = workload.write(&founder, "while one domain is lost");
    let view = wait_for(
        &founder,
        "the domain's loss",
        Duration::from_secs(120),
        |view| !alive(view, lost),
    );
    assert!(guarantee(&view, &ledger).1 > 0, "{view}");
    // The domain returns with its disk and the guarantee is restored.
    servers[2] = start(&hosts[1], &[]);
    let view = wait_for(
        &founder,
        "the domain rejoins",
        Duration::from_secs(180),
        |view| alive(view, lost) && guarantee(view, &ledger) == (Some(1), 0),
    );
    let session = session_row(&view, &ledger).unwrap();
    assert_eq!(
        session["achieved_survive"]
            .as_str()
            .map(str::to_ascii_lowercase),
        Some(survive.to_owned()),
        "{session}"
    );
    assert_eq!(claim_id(&read_claim(&founder, &claim)), claim);
}

#[test]
fn runbook_zone_loss() {
    domain_loss(
        "zone",
        "version: 1\ntopology:\n  region: ra\n  zone: a1\n",
        [
            "version: 1\ntopology:\n  region: ra\n  zone: a2\n",
            "version: 1\ntopology:\n  region: ra\n  zone: a3\n",
        ],
    );
}

#[test]
fn runbook_region_loss() {
    domain_loss(
        "region",
        "version: 1\ntopology:\n  region: r1\n",
        [
            "version: 1\ntopology:\n  region: r2\n",
            "version: 1\ntopology:\n  region: r3\n",
        ],
    );
}

#[test]
fn runbook_stale_clone() {
    let founder = Node::new("founder");
    let host = Node::new("host");
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    activate_native(&founder);
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (server, host_id) = join_start(&founder, &host, "host", &addresses[1]);
    wait_for(
        &founder,
        "the host reports",
        Duration::from_secs(90),
        |view| {
            node_row(view, host_id)
                .is_some_and(|row| row["alive"] == true && row["advertise"] == addresses[1])
        },
    );
    // The host's disk is copied while it is stopped, then the host restarts.
    drop(server);
    let clone = Node::new("clone");
    let copied = std::process::Command::new("cp")
        .args([
            "-Rp",
            &format!("{}/.", host.root().display()),
            clone.root().to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(copied.success());
    let _server = start(&host, &[]);
    wait_for(
        &founder,
        "the host is back",
        Duration::from_secs(90),
        |view| alive(view, host_id),
    );
    // The clone starts at another address: its contact is refused while the
    // original is not confirmed dead, and it never leaves catching up.
    let _clone_server = start(&clone, &["--advertise", &addresses[2]]);
    assert_eq!(identity(&clone).0, host_id);
    std::thread::sleep(Duration::from_secs(20));
    let view = placement(&founder).unwrap();
    let row = node_row(&view, host_id).unwrap();
    assert_eq!(row["advertise"], addresses[1], "{row}");
    assert_eq!(row["alive"], true, "{row}");
    let clone_readiness = readiness(&clone);
    assert_eq!(clone_readiness["catching_up"], false, "{clone_readiness}");
    assert_eq!(clone_readiness["authoritative"], false, "{clone_readiness}");
    // The original is gone: once the detector confirms it, the clone is
    // admitted at its address.
    drop(_server);
    wait_for(
        &founder,
        "the clone is admitted",
        Duration::from_secs(180),
        |view| {
            node_row(view, host_id)
                .is_some_and(|row| row["advertise"] == addresses[2] && row["alive"] == true)
        },
    );
    wait_until("the clone catches up", Duration::from_secs(90), || {
        readiness(&clone)["catching_up"] == true
    });
}

#[test]
fn runbook_failed_movement() {
    let founder = Node::new("founder");
    let host_a = Node::new("host-a");
    let host_b = Node::new("host-b");
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    let activation = admin(&founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (founder_node, tenant, ledger) = identity(&founder);
    let workload = Workload::new(&founder);
    let before = workload.write(&founder, "before the move");
    let (mut server_a, node_a) = join_start(&founder, &host_a, "host-a", &addresses[1]);
    let (_server_b, node_b) = join_start(&founder, &host_b, "host-b", &addresses[2]);
    let all = [founder_node, node_a, node_b];
    wait_for(&founder, "three hosts", Duration::from_secs(120), |view| {
        settled(view, &ledger, &all, 0)
    });
    plan_and_settle(&founder, &tenant, &ledger, None, 1, &all);
    let initial = ranges(&founder, &ledger).expect("ranges list");
    assert_eq!(initial["epoch"], 1, "{initial}");
    let member = initial["members"][0]["id"].as_str().unwrap().to_owned();
    let moved = admin(
        &founder,
        &[
            "cluster",
            "replicas",
            "ranges",
            "--session",
            &ledger,
            "move",
            "--member",
            &member,
            "--node",
            &node_a.to_string(),
        ],
    );
    assert_eq!(moved["result"]["kind"], "range_move_proposed", "{moved}");
    // The destination dies before the transfer completes.
    drop(std::mem::replace(&mut server_a, start_placeholder()));
    let during = workload.write(&founder, "while the move is stalled");
    let listing = ranges(&founder, &ledger).unwrap();
    assert!(
        listing["pending"].is_object() || listing["members"][0]["holder"] == node_a,
        "{listing}"
    );
    // The destination returns with its disk: the transfer finishes.
    server_a = start(&host_a, &[]);
    let deadline = Instant::now() + Duration::from_secs(240);
    loop {
        let views: Vec<Option<Value>> = [&founder, &host_a, &host_b]
            .iter()
            .map(|node| ranges(node, &ledger))
            .collect();
        let done = views.iter().all(|view| {
            view.as_ref().is_some_and(|view| {
                view["epoch"] == 2
                    && view["pending"].is_null()
                    && view["members"][0]["holder"] == node_a
            })
        });
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the move never finished: {views:#?}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    for claim in [&before, &during] {
        assert_eq!(claim_id(&read_claim(&founder, claim)), *claim);
    }
    let after = workload.write(&founder, "after the move");
    assert_ne!(after, before);
    drop(server_a);
}

#[test]
fn runbook_expired_credentials() {
    let (founder, hosts, mut servers, members, workload, _tenant, ledger) =
        three_voters(None, [None, None], None);
    let expired = members[2];
    // Expiry beyond the grace is modelled by revoking the host's invitation.
    let invitations = admin(&founder, &["cluster", "invitations", "list"]);
    let invitation = invitations["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["credential"]["node"] == expired)
        .unwrap_or_else(|| panic!("no invitation enrolled node {expired}: {invitations}"))["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let revoked = admin(&founder, &["cluster", "invitations", "revoke", &invitation]);
    assert!(revoked["result"]["operation_id"].is_string(), "{revoked}");
    let claim = workload.write(&founder, "while a credential is revoked");
    // The registry retires the credential at once; the node's own view
    // shows what it can no longer do.
    let view = wait_for(
        &founder,
        "the credential is retired",
        Duration::from_secs(90),
        |view| node_row(view, expired).is_some_and(|row| row["credential"] == "retired"),
    );
    assert!(node_row(&view, members[0]).is_some_and(|row| row["credential"] == "active"));
    // The host is cut off: its peers close its connections, so it learns
    // nothing more. Restarted, it either refuses to start (its own registry
    // applied the revocation before it was cut off) or starts and never
    // catches up; either way it takes no further part.
    drop(std::mem::replace(&mut servers[2], start_placeholder()));
    let (mut child, receive) = spawn(&hosts[1], &[], &[]);
    let restarted = receive.recv_timeout(Duration::from_secs(45)).ok();
    let mut cut_off = false;
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(5), "{status:?}");
            cut_off = true;
            break;
        }
        if restarted.is_some()
            && run(&hosts[1], None, &["cluster", "node", "readiness"])
                .status
                .success()
        {
            let ready = readiness(&hosts[1]);
            if ready["catching_up"] == false && ready["authoritative"] == false {
                std::thread::sleep(Duration::from_secs(10));
                let again = readiness(&hosts[1]);
                if again["catching_up"] == false && again["authoritative"] == false {
                    cut_off = true;
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        cut_off,
        "a retired credential kept taking part after a restart"
    );
    // The host is drained and removed; the machine enrolls again fresh.
    let drained = admin(
        &founder,
        &["cluster", "nodes", "drain", "--node", &expired.to_string()],
    );
    assert_eq!(drained["result"]["eligible"], false, "{drained}");
    let fresh = Node::new("host-c");
    let fresh_address = address();
    let (_fresh_server, fresh_id) = join_start(&founder, &fresh, "host-c", &fresh_address);
    let view = wait_for(
        &founder,
        "healed onto the fresh host",
        Duration::from_secs(240),
        |view| {
            session_row(view, &ledger).is_some_and(|session| {
                session["pending"].is_null()
                    && session["achieved_max_failures"] == 1
                    && ids(&session["voters"]).contains(&fresh_id)
                    && !ids(&session["voters"]).contains(&expired)
                    && session["retiring"].as_array().is_some_and(Vec::is_empty)
            })
        },
    );
    assert_eq!(guarantee(&view, &ledger), (Some(1), 0));
    let removed = wait_removed(&founder, expired);
    assert_eq!(removed["membership_removed"], true, "{removed}");
    assert_eq!(claim_id(&read_claim(&founder, &claim)), claim);
    drop(hosts);
}

#[test]
fn runbook_interrupted_upgrade() {
    let founder = Node::new("founder");
    let host = Node::new("host");
    let addresses: Vec<String> = (0..2).map(|_| address()).collect();
    let _founder_server = start(&founder, &["--advertise", &addresses[0]]);
    let (server, host_id) = join_start(&founder, &host, "host", &addresses[1]);
    let founder_id = identity(&founder).0;
    let level = |view: &Value, node: u64| {
        view["nodes"]
            .as_array()
            .and_then(|nodes| nodes.iter().find(|entry| entry["node"] == node))
            .and_then(|entry| entry["capability"].as_u64())
    };
    wait_until("both nodes report level 1", Duration::from_secs(90), || {
        let view = admin(&founder, &["cluster", "upgrade", "status"])["result"]["upgrade"].clone();
        level(&view, founder_id) == Some(1) && level(&view, host_id) == Some(1)
    });
    let activated = admin(
        &founder,
        &["cluster", "upgrade", "activate", "--fence", "1"],
    );
    assert_eq!(
        activated["result"]["kind"], "fence_activated",
        "{activated}"
    );
    wait_until("the host sees the fence", Duration::from_secs(60), || {
        admin(&host, &["cluster", "upgrade", "status"])["result"]["upgrade"]["fence_level"] == 1
    });
    // A binary below the fence refuses to serve; the fence never lowers.
    drop(server);
    let (mut child, receive) = spawn(&host, &[], &[("FOCAL_CAPABILITY_LEVEL", "0")]);
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "a fenced binary kept running");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(status.code(), Some(5), "{status:?}");
    assert!(
        receive.try_recv().is_err(),
        "a fenced binary published readiness"
    );
    let (code, report) = failure(
        &founder,
        None,
        &["cluster", "upgrade", "activate", "--fence", "0"],
    );
    assert_eq!(code, 2, "{report}");
    // Upgraded (the binary announces the fence's level), the host serves.
    let _server = start(&host, &[]);
    wait_until(
        "the host serves under the fence",
        Duration::from_secs(60),
        || {
            let view = admin(&host, &["cluster", "upgrade", "status"])["result"]["upgrade"].clone();
            view["fence_level"] == 1 && view["announced_level"] == 1
        },
    );
}

#[test]
fn runbook_interrupted_restore() {
    let founder = Node::new("founder");
    let address_a = address();
    let activation = admin(&founder, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let server = start(&founder, &["--advertise", &address_a]);
    let (_, tenant, session) = identity(&founder);
    let workload = Workload::new(&founder);
    let claim = workload.write(&founder, "to be restored");
    workload.deliver(&claim);
    let before = read_claim(&founder, &claim);
    wait_for(
        &founder,
        "the session is registered",
        Duration::from_secs(90),
        |view| session_row(view, &session).is_some(),
    );
    let backup = founder.root().join("backup");
    let created = admin(
        &founder,
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
    let verified = admin(
        &founder,
        &[
            "cluster",
            "backup",
            "verify",
            "--input",
            backup.to_str().unwrap(),
        ],
    );
    assert_eq!(verified["result"]["kind"], "backup_verified", "{verified}");
    // A fresh founder restores; it is killed as soon as the restore is
    // issued, restarted, and the restore issued again.
    let other = Node::new("restore-target");
    let activation = admin(&other, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let address_b = address();
    let server_b = start(&other, &["--advertise", &address_b]);
    let admitted = admin(
        &other,
        &["cluster", "tenants", "admit", "--tenant", &tenant],
    );
    assert_eq!(admitted["result"]["kind"], "tenants", "{admitted}");
    let restore_args = [
        "cluster",
        "restore",
        "--input",
        backup.to_str().unwrap(),
        "--new-incarnation",
    ];
    let first = {
        let other_root = other.root().to_path_buf();
        let args: Vec<String> = restore_args.iter().map(|arg| (*arg).to_owned()).collect();
        std::thread::spawn(move || {
            let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
            std::process::Command::new(env!("CARGO_BIN_EXE_focal"))
                .args(["--data-dir", other_root.to_str().unwrap()])
                .args(&borrowed)
                .output()
                .unwrap()
        })
    };
    std::thread::sleep(Duration::from_millis(40));
    drop(server_b);
    let interrupted = first.join().unwrap();
    let _server_b = start(&other, &["--advertise", &address_b]);
    wait_until(
        "the restarted node answers its admin socket",
        Duration::from_secs(60),
        || {
            run(&other, None, &["cluster", "node", "readiness"])
                .status
                .success()
        },
    );
    let again = run(&other, None, &restore_args);
    let text = String::from_utf8_lossy(&again.stdout).into_owned()
        + &String::from_utf8_lossy(&again.stderr);
    if again.status.success() {
        let value: Value = serde_json::from_slice(&again.stdout).unwrap();
        assert_eq!(value["result"]["kind"], "restored", "{value}");
    } else {
        // Already restored by the interrupted attempt: refused by name.
        assert!(
            interrupted.status.success(),
            "neither attempt restored: {text}\n{}",
            String::from_utf8_lossy(&interrupted.stderr)
        );
    }
    // The claim reads back from the restored session over the operator's
    // local connection, a session of a served tenant.
    let client = Node::new("client-b");
    let added = run(
        &client,
        None,
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
    let deadline = Instant::now() + Duration::from_secs(90);
    let after = loop {
        let output = run(
            &client,
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
            Instant::now() < deadline,
            "the restored session never answered: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(claim_id(&after), claim_id(&before));
    assert_eq!(claim_id(&after), claim);
    assert_eq!(after["Claim"]["status"], before["Claim"]["status"]);
    let _ = Path::new("");
}
