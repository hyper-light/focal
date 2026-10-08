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
//! Draining, removing and replacing nodes through the real binary (24 §19,
//! DC19): a session placed on three of four hosts loses one host to a
//! drain — the directory heals the placement onto the remaining hosts and
//! retires the drained copies — after which the host is removed (its seat
//! in the directory's partition group, which it led, then root membership,
//! then its credential) and a repeat resumes; a drain the
//! remaining hosts cannot absorb keeps the copies where they are and the
//! removal is refused; undraining heals again; the founder is never
//! drained; a replacement must be enrolled and reporting before it drains
//! its predecessor.
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
/// The exit code and diagnostic (`focal: [code] message`) of a refusal.
fn failure(root: &Path, args: &[&str]) -> (i32, String) {
    let output = command(root, args);
    assert!(!output.status.success(), "arguments {args:?} succeeded");
    (
        output.status.code().unwrap(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}
#[path = "support/deadline.rs"]
mod deadline;
#[path = "support/ports.rs"]
mod ports;
fn address() -> String {
    ports::address()
}
fn start(root: &Path, address: Option<&str>) -> (Server, Value) {
    deadline::observe(root);
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.args(["--data-dir", root.to_str().unwrap(), "start", "node"]);
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
    let output = command(root, &["inspect", "placement"]);
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
fn ids(value: &Value) -> Vec<u64> {
    let mut ids: Vec<u64> = value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}
/// Whether a session names the node anywhere: voters, materializers,
/// content copies, retiring copies or a pending assignment.
fn names(session: &Value, id: u64) -> bool {
    ["voters", "materializers", "content_copies", "retiring"]
        .iter()
        .any(|list| ids(&session[list]).contains(&id))
        || session["pending"]["voters"]
            .as_array()
            .is_some_and(|voters| voters.iter().any(|v| v.as_u64() == Some(id)))
}
fn wait_for(
    root: &Path,
    what: &str,
    timeout: Duration,
    condition: impl Fn(&Value) -> bool,
) -> Value {
    let mut deadline = deadline::Deadline::after(timeout);
    let mut last = None;
    while deadline.open() {
        if let Some(view) = placement(root) {
            if condition(&view) {
                return view;
            }
            last = Some(view);
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    // Every node this test started: its health (placement refusals,
    // authority refusals) and its metrics (consensus progress, budget and
    // queue refusals), so a stall names the member and the refusal.
    let mut report = String::new();
    for observed in deadline::observed() {
        for read in [
            ["inspect", "node", "--health"],
            ["inspect", "node", "--metrics"],
        ] {
            let output = command(&observed, &read);
            report.push_str(&format!(
                "\n== {} {}\n{}{}",
                observed.display(),
                read[2],
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    panic!("{what} did not happen within {timeout:?}; nodes:{report}\nlast view: {last:#?}");
}
fn join(founder: &Path, host: &Path, name: &str, advertise: &str) -> u64 {
    let invitation = founder.join(format!("{name}.invite"));
    let written = success(
        founder,
        &[
            "invite",
            "node",
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
            "cluster",
            "--invite-file",
            invitation.to_str().unwrap(),
            "--advertise",
            advertise,
        ],
    );
    joined["node"].as_u64().unwrap()
}
fn private_dir(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("focal-nodes-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
/// One membership change of the directory's partition group by hand
/// (`cluster partitions`, 24 §13), asked again while the group is not
/// ready for it — a learner still catching up before its promotion, a
/// change still committing, an outcome lost — within a bounded wait.
fn partition_change(founder: &Path, verb: &str, partition: &str, node: u64) -> Value {
    // The derived `cluster partitions VERB`, said as its action and thing.
    let (action, thing) = match verb {
        "add-learner" => ("add", "partition-learner"),
        "promote" => ("promote", "partition-learner"),
        "remove" => ("remove", "partition-member"),
        "transfer" => ("transfer", "partition-leader"),
        other => panic!("no partition change {other}"),
    };
    let text = node.to_string();
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    loop {
        let output = command(
            founder,
            &[action, thing, "--partition", partition, "--node", &text],
        );
        if output.status.success() {
            return serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("[not_ready]")
                || stderr.contains("[unavailable]")
                || stderr.contains("[compare_failed]")
                || stderr.contains("[outcome_unknown]"),
            "{verb} of {node}: {stderr}"
        );
        if !deadline.open() {
            let health = command(founder, &["inspect", "node", "--health"]);
            let shown = command(founder, &["inspect", "partition", "--partition", partition]);
            let view = command(founder, &["inspect", "placement"]);
            panic!(
                "{verb} of {node} stayed refused: {stderr}\nhealth: {}\nshown: {}\nplacement: {}",
                String::from_utf8_lossy(&health.stdout),
                String::from_utf8_lossy(&shown.stdout),
                String::from_utf8_lossy(&view.stdout),
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}
/// The directory partition group's configuration as the founder's replica
/// applied it.
fn partition_shown(founder: &Path, partition: &str) -> Value {
    success(founder, &["inspect", "partition", "--partition", partition])["result"]["configuration"]
        .clone()
}
/// Every listed node alive and reporting, and the session settled at
/// `max_failures` with no plan pending.
fn settled(view: &Value, nodes: &[u64], max_failures: u64) -> bool {
    session(view).is_some_and(|session| {
        session["pending"].is_null()
            && session["achieved_max_failures"] == max_failures
            && session["retiring"].as_array().is_some_and(Vec::is_empty)
    }) && nodes.iter().all(|id| {
        node(view, *id)
            .is_some_and(|node| node["alive"] == true && node["disk_available"].is_number())
    })
}

#[test]
fn a_drained_host_is_healed_around_removed_once_empty_and_a_drain_without_capacity_is_refused() {
    let dirs: Vec<_> = ["founder", "host-a", "host-b", "host-c", "host-d"]
        .iter()
        .map(|name| private_dir(name))
        .collect();
    let founder = dirs[0].path();
    let addresses: Vec<String> = (0..5).map(|_| address()).collect();
    let (_founder_server, status) = start(founder, Some(&addresses[0]));
    assert_eq!(status["condition"], "Ready");
    let founder_node = success(founder, &["inspect", "identity"])["node"]
        .as_u64()
        .unwrap();
    let node_a = join(founder, dirs[1].path(), "host-a", &addresses[1]);
    let node_b = join(founder, dirs[2].path(), "host-b", &addresses[2]);
    let node_c = join(founder, dirs[3].path(), "host-c", &addresses[3]);
    let mut servers = vec![
        (node_a, Some(start(dirs[1].path(), None).0)),
        (node_b, Some(start(dirs[2].path(), None).0)),
        (node_c, Some(start(dirs[3].path(), None).0)),
    ];
    let all = [founder_node, node_a, node_b, node_c];
    let view = wait_for(founder, "four hosts", Duration::from_secs(90), |view| {
        settled(view, &all, 0)
    });
    // The directory's partition group is seated on every host by hand
    // (24 §13; the audit's F24): each is admitted as a learner — the root's
    // grant seats it and it opens a replica — and promoted once caught up,
    // so that whichever host leaves takes a seat with it.
    let partition = view["control"]["partitions"][0]["partition"]
        .as_str()
        .unwrap()
        .to_owned();
    for node in [node_a, node_b, node_c] {
        let added = partition_change(founder, "add-learner", &partition, node);
        assert_eq!(added["kind"], "committed", "{added}");
        assert!(
            added["operation_id"].as_str().unwrap().starts_with("p1:"),
            "{added}"
        );
        let promoted = partition_change(founder, "promote", &partition, node);
        assert_eq!(promoted["kind"], "committed", "{promoted}");
    }
    let seated = partition_shown(founder, &partition);
    let mut seated_voters = ids(&seated["voters"]);
    seated_voters.sort_unstable();
    let mut everyone = all.to_vec();
    everyone.sort_unstable();
    assert_eq!(seated_voters, everyone, "{seated}");
    assert!(ids(&seated["learners"]).is_empty(), "{seated}");
    let registered = session(&view).unwrap().clone();
    let tenant = registered["tenant"].as_str().unwrap().to_owned();
    let ledger = registered["session"].as_str().unwrap().to_owned();
    // One tolerated node loss: three of the four hosts become voters.
    // A plan is answered once it committed; one the partition refused —
    // planned on an observation that went stale — is answered
    // `compare_failed`, and the operator plans again (24 §16).
    let planned = plan_until_planned(
        founder,
        &[
            "plan",
            "session",
            "--tenant",
            &tenant,
            "--session",
            &ledger,
            "--max-failures",
            "1",
        ],
    );
    assert_eq!(planned["state"], "planned");
    let view = wait_for(founder, "activation", Duration::from_secs(180), |view| {
        settled(view, &all, 1)
    });
    let active = session(&view).unwrap().clone();
    assert_eq!(active["route_epoch"], 2);
    let voters = ids(&active["voters"]);
    assert_eq!(voters.len(), 3);
    let drained = *voters.iter().find(|id| **id != founder_node).unwrap();
    let spare = *all
        .iter()
        .find(|id| !voters.contains(id))
        .expect("one host is not a voter");
    let founder_text = founder_node.to_string();
    let drained_text = drained.to_string();
    // The host about to leave leads the partition group: its removal must
    // hand that leadership on before it can take the seat away.
    let handed = success(
        founder,
        &[
            "transfer",
            "partition-leader",
            "--partition",
            &partition,
            "--node",
            &drained_text,
        ],
    )["result"]
        .clone();
    assert_eq!(handed["kind"], "transfer_initiated", "{handed}");
    assert_eq!(handed["target"], drained);
    wait_for(
        founder,
        "the partition led by the leaving host",
        Duration::from_secs(60),
        |view| view["control"]["partitions"][0]["leader"] == drained,
    );

    // The founder is never drained; a host that is still eligible is not
    // removed; unknown nodes are named.
    let (code, report) = failure(founder, &["drain", "node", "--node", &founder_text]);
    assert_eq!(code, 2, "{report}");
    let (code, report) = failure(founder, &["remove", "node", "--node", &drained_text]);
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[not_drained]"), "{report}");
    let (code, report) = failure(founder, &["drain", "node", "--node", "424242"]);
    assert_eq!(code, 4, "{report}");
    assert!(report.contains("[unknown_node]"), "{report}");
    let (code, report) = failure(
        founder,
        &[
            "replace",
            "node",
            "--node",
            &drained_text,
            "--with",
            "424242",
        ],
    );
    assert_eq!(code, 5, "{report}");
    assert!(report.contains("[node_not_ready]"), "{report}");

    // Drain one voter: its grant is re-issued ineligible at generation 2,
    // exactly once.
    let result = success(founder, &["drain", "node", "--node", &drained_text])["result"].clone();
    assert_eq!(result["kind"], "node_eligibility", "{result}");
    assert_eq!(result["node"], drained);
    assert_eq!(result["eligible"], false);
    assert_eq!(result["changed"], true);
    assert_eq!(result["generation"], 2);
    assert!(result["operation_id"].as_str().unwrap().starts_with("a1:"));
    // A remove that follows the drain at once is refused for the copies the
    // host still holds — never as "not drained": the committed grant is
    // ineligible even while the partition is still observing it.
    let (code, report) = failure(founder, &["remove", "node", "--node", &drained_text]);
    if code != 5 || !report.contains("[node_holding]") {
        // What each side saw of the drain: the founder's view, and the
        // drained host's own, which hosts a replica of the partition.
        let drained_dir = dirs[all.iter().position(|id| *id == drained).unwrap()].path();
        let founder_view = command(founder, &["inspect", "placement"]);
        let drained_view = command(drained_dir, &["inspect", "placement"]);
        let drained_health = command(drained_dir, &["inspect", "node", "--health"]);
        panic!(
            "{report}\nfounder placement: {}\ndrained placement: {}\ndrained health: {}",
            String::from_utf8_lossy(&founder_view.stdout),
            String::from_utf8_lossy(&drained_view.stdout),
            String::from_utf8_lossy(&drained_health.stdout),
        );
    }
    let again = success(founder, &["drain", "node", "--node", &drained_text])["result"].clone();
    assert_eq!(again["changed"], false, "{again}");
    assert_eq!(again["generation"], 2);
    assert_eq!(again["operation_id"], Value::Null);
    // The partition learns the drained grant, the placement heals onto the
    // remaining hosts and the drained copies retire.
    let view = wait_for(
        founder,
        "heal after the drain",
        Duration::from_secs(240),
        |view| {
            node(view, drained)
                .is_some_and(|node| node["eligible"] == false && node["generation"] == 2)
                && session(view).is_some_and(|session| {
                    session["pending"].is_null()
                        && session["achieved_max_failures"] == 1
                        && !names(session, drained)
                })
        },
    );
    let healed = session(&view).unwrap().clone();
    let healed_voters = ids(&healed["voters"]);
    assert_eq!(healed_voters.len(), 3, "{healed}");
    assert!(healed_voters.contains(&spare), "{healed}");
    assert!(!healed_voters.contains(&drained));
    assert!(healed["route_epoch"].as_u64().unwrap() >= 3, "{healed}");

    // Remove the drained host: root membership (it was admitted as a
    // member), then its credential; a repeat resumes and changes nothing.
    let removed = wait_for_removal(founder, drained);
    assert_eq!(removed["kind"], "node_removed", "{removed}");
    assert_eq!(removed["node"], drained);
    assert_eq!(removed["membership_removed"], true, "{removed}");
    // Its seat in the partition group went first — leadership handed back
    // to a voter that stays — so the group counts no voter that is gone.
    assert_eq!(removed["partitions_vacated"], 1, "{removed}");
    let vacated = partition_shown(founder, &partition);
    assert!(!ids(&vacated["voters"]).contains(&drained), "{vacated}");
    assert!(!ids(&vacated["learners"]).contains(&drained), "{vacated}");
    assert_eq!(ids(&vacated["voters"]).len(), 3, "{vacated}");
    let view = placement(founder).unwrap();
    let partition_leader = view["control"]["partitions"][0]["leader"].as_u64().unwrap();
    assert!(
        partition_leader != drained && partition_leader != 0,
        "{view}"
    );
    assert_eq!(removed["revoked"], true, "{removed}");
    assert_eq!(removed["contact_retired"], true, "{removed}");
    let invitation = removed["invitation"].as_str().unwrap().to_owned();
    // The removed node's committed contact is gone: the bounded contact
    // table keeps no slot for a node that no longer exists.
    let listed = success(founder, &["list", "nodes"]);
    assert!(
        !listed["result"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|contact| contact["node"] == drained),
        "{listed}"
    );
    let repeated = success(founder, &["remove", "node", "--node", &drained_text])["result"].clone();
    assert_eq!(repeated["membership_removed"], false, "{repeated}");
    assert_eq!(repeated["partitions_vacated"], 0, "{repeated}");
    assert_eq!(repeated["revoked"], false);
    assert_eq!(repeated["contact_retired"], false, "{repeated}");
    assert_eq!(repeated["invitation"], invitation);
    let inspected = success(founder, &["get", "invitation", &invitation]);
    assert_eq!(
        inspected["result"]["entries"][0]["revoked"], true,
        "{inspected}"
    );
    let configuration = success(founder, &["inspect", "membership"])["result"].clone();
    assert!(
        !ids(&configuration["configuration"]["voters"]).contains(&drained)
            && !ids(&configuration["configuration"]["learners"]).contains(&drained),
        "{configuration}"
    );
    // The removed host's process is stopped; the cluster does not miss it.
    let position = servers.iter().position(|(id, _)| *id == drained).unwrap();
    servers[position].1.take();
    let remaining: Vec<u64> = all.iter().copied().filter(|id| *id != drained).collect();

    // A drain the remaining hosts cannot absorb: the placement records a
    // refusal, keeps its copies, and the host cannot be removed.
    let victim = *healed_voters
        .iter()
        .find(|id| **id != founder_node)
        .unwrap();
    let victim_text = victim.to_string();
    let result = success(founder, &["drain", "node", "--node", &victim_text])["result"].clone();
    assert_eq!(result["changed"], true, "{result}");
    assert_eq!(result["generation"], 2);
    wait_for(
        founder,
        "the drained grant",
        Duration::from_secs(90),
        |view| {
            node(view, victim)
                .is_some_and(|node| node["eligible"] == false && node["generation"] == 2)
        },
    );
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    let mut refused = None;
    while deadline.open() {
        let (code, report) = failure(founder, &["remove", "node", "--node", &victim_text]);
        assert_eq!(code, 5, "{report}");
        assert!(report.contains("[node_holding]"), "{report}");
        if let Some(view) = placement(founder)
            && session(&view).is_some_and(|session| {
                session["blocked_by"]
                    .as_array()
                    .is_some_and(|blocked| !blocked.is_empty())
                    || session["pending"].is_null() && names(session, victim)
            })
        {
            refused = Some(view);
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let refused = refused.expect("the placement kept its copies");
    assert!(names(session(&refused).unwrap(), victim), "{refused}");
    // Undrain: the host is considered again and the placement heals back
    // to one tolerated loss.
    let result = success(founder, &["undrain", "node", "--node", &victim_text])["result"].clone();
    assert_eq!(result["eligible"], true, "{result}");
    assert_eq!(result["changed"], true);
    assert_eq!(result["generation"], 3);
    let view = wait_for(
        founder,
        "heal after the undrain",
        Duration::from_secs(240),
        |view| {
            node(view, victim)
                .is_some_and(|node| node["eligible"] == true && node["generation"] == 3)
                && settled(view, &remaining, 1)
        },
    );
    assert_eq!(ids(&session(&view).unwrap()["voters"]).len(), 3);

    // Replace: a new host joins; once it reports, the replacement drains its
    // predecessor and the placement heals onto the newcomer.
    let node_d = join(founder, dirs[4].path(), "host-d", &addresses[4]);
    let (_server_d, _) = start(dirs[4].path(), None);
    wait_for(
        founder,
        "the newcomer reporting",
        Duration::from_secs(90),
        |view| {
            node(view, node_d).is_some_and(|node| {
                node["alive"] == true
                    && node["eligible"] == true
                    && node["disk_available"].is_number()
            })
        },
    );
    let result = success(
        founder,
        &[
            "replace",
            "node",
            "--node",
            &victim_text,
            "--with",
            &node_d.to_string(),
        ],
    )["result"]
        .clone();
    assert_eq!(result["kind"], "node_eligibility", "{result}");
    assert_eq!(result["eligible"], false);
    assert_eq!(result["generation"], 4);
    let with_d: Vec<u64> = remaining
        .iter()
        .copied()
        .filter(|id| *id != victim)
        .chain([node_d])
        .collect();
    let view = wait_for(
        founder,
        "heal onto the newcomer",
        Duration::from_secs(240),
        |view| {
            settled(view, &with_d, 1)
                && session(view).is_some_and(|session| !names(session, victim))
        },
    );
    let final_voters = ids(&session(&view).unwrap()["voters"]);
    assert!(final_voters.contains(&node_d), "{view}");
    assert!(!final_voters.contains(&victim));

    // An administrator's change is made where the partition group leads
    // (24 §13): from a host whose replica votes and follows — neither the
    // founder, which leads, nor the drained victim, whose seat is what the
    // operator vacates by hand — the node takes the group's leadership
    // first and the change commits there.
    let before = partition_shown(founder, &partition);
    let actor = *ids(&before["voters"])
        .iter()
        .find(|id| **id != founder_node && **id != victim)
        .unwrap();
    let actor_dir = dirs[all.iter().position(|id| *id == actor).unwrap()].path();
    let led = placement(founder).unwrap();
    assert_ne!(led["control"]["partitions"][0]["leader"], actor, "{led}");
    let removed = partition_change(actor_dir, "remove", &partition, victim);
    assert_eq!(removed["kind"], "committed", "{removed}");
    // The founder's replica follows the group now: its view is what it
    // applied, a moment behind the leader's commit (macOS CI read the
    // victim still seated right after the receipt, 2026-10-02), so the
    // fact is waited on, bounded.
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    let after = loop {
        let shown = partition_shown(founder, &partition);
        if !ids(&shown["voters"]).contains(&victim) {
            break shown;
        }
        assert!(
            deadline.open(),
            "the founder never applied the victim's removal: {shown}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(!ids(&after["learners"]).contains(&victim), "{after}");
    let view = placement(founder).unwrap();
    assert_eq!(view["control"]["partitions"][0]["leader"], actor, "{view}");
    // The issuers the cluster's credentials chain to (24 §11): the genesis
    // issuer alone, under a fence a cluster founded by this binary holds at
    // its level from genesis. The operator stages the successor: endorsed
    // by the genesis issuer, trusted everywhere from its staging, issuing
    // from the controller's next step; asked again, it is answered as it is.
    // A participant enrolled under the genesis issuer, before it succeeds
    // itself: its context holds that issuer alone.
    let client_dir = private_dir("client");
    let client = client_dir.path();
    let invitation = founder.join("alice.invite");
    success(
        founder,
        &[
            "invite",
            "client",
            "--name",
            "alice",
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    success(
        client,
        &[
            "enroll",
            "context",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    let standing = success(client, &["--client-context", "alice", "inspect", "prefix"]);
    assert!(
        standing["result"].is_object() || standing.is_object(),
        "{standing}"
    );
    let adopted_file = client
        .join("CLIENT.contexts")
        .join("enrollment-alice")
        .join("trust-adopted.bin");
    assert!(
        !adopted_file.exists(),
        "nothing endorsed beyond the invitation's issuers, nothing adopted"
    );
    let issuers = success(founder, &["list", "issuers"])["result"].clone();
    assert!(issuers["successor"].is_null(), "{issuers}");
    assert!(issuers["retiring"].is_null(), "{issuers}");
    assert_eq!(issuers["current"]["endorsed"], false, "{issuers}");
    assert_eq!(
        issuers["fence_level"], issuers["succession_level"],
        "{issuers}"
    );
    let genesis_issuer = issuers["current"]["fingerprint"].clone();
    let rotated = success(founder, &["rotate", "issuer"])["result"].clone();
    let staged = rotated["successor"].clone();
    assert_eq!(staged["endorsed"], true, "{rotated}");
    assert!(staged["staged_at"].is_number(), "{rotated}");
    assert_ne!(staged["fingerprint"], genesis_issuer, "{rotated}");
    assert_eq!(
        rotated["current"]["fingerprint"], genesis_issuer,
        "{rotated}"
    );
    let again = success(founder, &["rotate", "issuer"])["result"].clone();
    assert!(
        again["successor"]["fingerprint"] == staged["fingerprint"]
            || (again["current"]["fingerprint"] == staged["fingerprint"]
                && again["retiring"]["fingerprint"] == genesis_issuer),
        "{again}"
    );
    // Asked on, the succession is stepped to its activation: the successor
    // issues, the genesis issuer retires while what it issued lives.
    let mut deadline = deadline::Deadline::after(Duration::from_secs(60));
    let activated = loop {
        let view = success(founder, &["rotate", "issuer"])["result"].clone();
        if view["current"]["fingerprint"] == staged["fingerprint"] {
            break view;
        }
        assert!(deadline.open(), "the successor never issued: {view}");
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(
        activated["retiring"]["fingerprint"], genesis_issuer,
        "{activated}"
    );
    // The founder renews under the successor and presents the endorsed
    // chain; the participant, holding the genesis issuer alone, verifies it
    // through the endorsement and adopts the successor beside its journal
    // (24 §11) — and serves on the adopted root from then on.
    let renewed = success(founder, &["renew", "credential"])["result"].clone();
    assert!(renewed["issued_at"].is_number(), "{renewed}");
    let endorsed = success(client, &["--client-context", "alice", "inspect", "prefix"]);
    assert!(endorsed.is_object(), "{endorsed}");
    assert!(
        adopted_file.is_file(),
        "the successor the participant was shown endorsed was not adopted at {}",
        adopted_file.display()
    );
    let adopted_again = success(client, &["--client-context", "alice", "inspect", "prefix"]);
    assert!(adopted_again.is_object(), "{adopted_again}");
    drop(servers);
}
/// A plan refused as `compare_failed` was made on an observation that went
/// stale; the operator plans again, on the next.
fn plan_until_planned(founder: &Path, args: &[&str]) -> Value {
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    loop {
        let output = command(founder, args);
        if output.status.success() {
            return serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("[compare_failed]"), "{stderr}");
        assert!(deadline.open(), "the plan stayed refused: {stderr}");
        std::thread::sleep(Duration::from_millis(500));
    }
}
/// Removal waits for the retiring copies to leave the directory; until
/// then it is refused as holding.
fn wait_for_removal(founder: &Path, node: u64) -> Value {
    let text = node.to_string();
    let mut deadline = deadline::Deadline::after(Duration::from_secs(120));
    loop {
        let output = command(founder, &["remove", "node", "--node", &text]);
        if output.status.success() {
            return serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"].clone();
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("[node_holding]")
                || stderr.contains("[not_leader]")
                || stderr.contains("[unavailable]")
                || stderr.contains("[partition_pending]")
                || stderr.contains("[leader_leaving]"),
            "{stderr}"
        );
        assert!(deadline.open(), "removal stayed refused: {stderr}");
        std::thread::sleep(Duration::from_millis(500));
    }
}
