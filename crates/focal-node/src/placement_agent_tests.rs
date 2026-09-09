use super::*;
use crate::network_service::tests::{Running, settings};
use focal_control::{ControlRequest, ControlRequestId};
use focal_directory::{LogGroupId, OperationId, SessionFenceKind};
use focal_model::RouteEpoch;
use std::{collections::BTreeMap, path::Path};

const CONTROLLER: [u8; 16] = [41; 16];

pub(crate) async fn partition_host(running: &Running) -> ControlHost {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(host) = running.handles.directory.host()
                && host.progress().applied_index > 0
            {
                return host;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("partition owner never became available")
}
pub(crate) fn controller_peer(running: &Running) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(CONTROLLER),
        tenants: BTreeSet::from([running.handles.directory.namespace().tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap()
}
pub(crate) async fn observe(
    running: &Running,
    host: &ControlHost,
    id: u128,
) -> Result<
    (
        PartitionCheckpoint,
        ControlSnapshot,
        ControlAuthoritySnapshot,
    ),
    ControlFailure,
> {
    let ControlReadResult::StateAndAuthority {
        snapshot,
        authority: Some(authority),
    } = host
        .read(
            controller_peer(running),
            RequestId::from_u128(id),
            ControlRead::StateAndAuthority,
        )
        .await?
    else {
        panic!("partition authority not installed");
    };
    let ControlBootstrap::Partition { directory } = &snapshot.state else {
        panic!("partition state");
    };
    Ok((directory.clone(), *snapshot, authority))
}
/// None when the service ended; the caller reports its outcome.
pub(crate) async fn wait_for(
    running: &Running,
    host: &ControlHost,
    what: &str,
    condition: impl Fn(&PartitionCheckpoint) -> bool,
) -> Option<(
    PartitionCheckpoint,
    ControlSnapshot,
    ControlAuthoritySnapshot,
)> {
    let mut id = 1_000;
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            id += 1;
            match observe(running, host, id).await {
                Ok(observed) if condition(&observed.0) => return Some(observed),
                Ok(_) => {}
                Err(_) if host.progress().stopped => return None,
                Err(error) => panic!("partition read failed while waiting for {what}: {error:?}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("partition never reached: {what}"))
}
macro_rules! reached {
    ($running:expr, $wait:expr) => {
        match $wait.await {
            Some(observed) => observed,
            None => panic!("service ended: {:?}", $running.outcome().await),
        }
    };
}
async fn submit(
    running: &Running,
    host: &ControlHost,
    sequence: u64,
    directory: &PartitionCheckpoint,
    installed: &ControlAuthoritySnapshot,
    operation: PartitionOperation,
) {
    let now = unix_time().unwrap();
    let cluster = crate::embedded::decode_identity(&data_dir(running).join("IDENTITY"))
        .unwrap()
        .cluster;
    let evidence = control_evidence(
        installed,
        cluster,
        now,
        &MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024).unwrap(),
    )
    .unwrap();
    host.submit(
        controller_peer(running),
        ControlRequest {
            id: ControlRequestId {
                client: CONTROLLER,
                sequence,
            },
            acknowledged_through: sequence - 1,
            command: ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                command: PartitionCommand {
                    expected_revision: directory.revision,
                    delegation_epoch: directory.delegation.epoch,
                    operation,
                },
                evidence,
            }),
        },
    )
    .await
    .unwrap();
}
fn data_dir(running: &Running) -> std::path::PathBuf {
    running
        .status
        .socket
        .parent()
        .expect("socket lives in the data directory")
        .to_path_buf()
}
fn journal_files(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = ["placement-root", "placement-partition"]
        .into_iter()
        .filter(|name| {
            root.join("cluster")
                .join(name)
                .join("journal.bin")
                .is_file()
        })
        .map(str::to_owned)
        .collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn founder_agent_registers_its_session_reports_load_and_restarts_without_repeating() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let founder = Running::start(&settings).await;
    assert_eq!(founder.status.condition, "Ready");
    let ledger = founder.status.ledger;
    let node = founder.status.node;
    let host = partition_host(&founder).await;
    let (checkpoint, _, _) = reached!(
        founder,
        wait_for(&founder, &host, "session registered with load", |state| {
            state.sessions.contains_key(&ledger)
                && state
                    .nodes
                    .get(&node)
                    .is_some_and(|record| record.load.is_some())
        })
    );
    let descriptor = &checkpoint.sessions[&ledger];
    assert_eq!(descriptor.authority.kind, SessionFenceKind::Created);
    assert_eq!(descriptor.route_epoch, RouteEpoch(1));
    assert_eq!(descriptor.revision, 1);
    assert_eq!(
        descriptor.active.placement.voters,
        BTreeMap::from([(node, 1)])
    );
    assert_eq!(descriptor.active.policy.durability.max_failures, 0);
    assert!(descriptor.pending.is_none());
    let record = &checkpoint.nodes[&node];
    assert_eq!(record.enrollment.generation, 1);
    let load = record.load.unwrap();
    assert!(
        load.disk_available > 0,
        "disk headroom is sampled from the data directory"
    );
    assert!(load.available_memory > 0);
    assert_eq!(load.active_weight, 1);
    // The agent reports what the node hosts: its own tenant under the
    // standard bound, and the volume envelope the load report came from.
    let status = founder.handles.placement.status().await.unwrap();
    let admission = &status.admission;
    assert_eq!(
        admission.max_tenants,
        crate::admission::AdmissionPolicy::DEFAULT_MAX_TENANTS
    );
    assert_eq!(admission.tenants.len(), 1);
    assert_eq!(admission.tenants[0].tenant, ledger.tenant);
    assert_eq!(admission.tenants[0].weight, 1);
    assert!(admission.tenants[0].memory_limit > 0);
    // The envelope's sample moves with every committed write, so only its
    // presence is stable across the two observations.
    assert!(admission.disk_free.is_some_and(|free| free > 0));
    assert_eq!(admission.disk_headroom, 64 * 1024 * 1024);
    assert!(admission.memory_limit > admission.memory_used);
    assert_eq!(
        journal_files(directory.path()),
        vec![
            "placement-partition".to_owned(),
            "placement-root".to_owned()
        ]
    );
    assert!(
        directory
            .path()
            .join("PLACEMENT-PARTITION.initialized")
            .is_file()
    );
    // The registered session keeps serving.
    let membership = founder
        .handles
        .ledger
        .as_ref()
        .unwrap()
        .membership()
        .await
        .unwrap();
    assert_eq!(membership.view().configuration.voters, vec![node]);
    let revision = checkpoint.revision;
    founder.stop().await;

    let founder = Running::start(&settings).await;
    let host = partition_host(&founder).await;
    // A restart re-reads its journals and finds every registration fact already
    // committed; the only new command is a fresh load report.
    let (after, _, _) = reached!(
        founder,
        wait_for(&founder, &host, "fresh load report", |state| {
            state.nodes[&node]
                .load
                .is_some_and(|reported| reported.report > load.report)
        })
    );
    assert_eq!(after.revision, revision + 1);
    assert_eq!(after.sessions[&ledger], checkpoint.sessions[&ledger]);
    assert_eq!(
        after.nodes[&node].enrollment,
        checkpoint.nodes[&node].enrollment
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (settled, _, _) = observe(&founder, &host, 7).await.unwrap();
    assert_eq!(
        settled.revision,
        revision + 1,
        "no further commands within the load interval"
    );
    founder.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_completes_a_plan_on_one_host_with_signed_readiness_and_fences() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let founder = Running::start(&settings).await;
    let ledger = founder.status.ledger;
    let node = founder.status.node;
    let host = partition_host(&founder).await;
    let (checkpoint, _, installed) = reached!(
        founder,
        wait_for(&founder, &host, "session registered with load", |state| {
            state.sessions.contains_key(&ledger)
                && state
                    .nodes
                    .get(&node)
                    .is_some_and(|record| record.load.is_some())
        })
    );
    let descriptor = checkpoint.sessions[&ledger].clone();
    let desired = descriptor.active.clone();
    let operation = OperationId::from_u128(2);
    // The operator plans the (unchanged) placement; the controller does the
    // rest: preparation, the log's cutover record, the founder's own signed
    // readiness, promotion, the signed cutover fence and activation.
    submit(
        &founder,
        &host,
        1,
        &checkpoint,
        &installed,
        PartitionOperation::Session {
            ledger,
            expected_revision: descriptor.revision,
            change: SessionChange::Plan {
                operation,
                desired: desired.clone(),
                observations: BTreeMap::new(),
            },
        },
    )
    .await;
    let (checkpoint, _, _) = reached!(
        founder,
        wait_for(&founder, &host, "signed readiness", |state| {
            state.sessions[&ledger]
                .pending
                .as_ref()
                .is_some_and(|plan| plan.ready.contains_key(&node))
        })
    );
    let plan = checkpoint.sessions[&ledger].pending.clone().unwrap();
    let ready = &plan.ready[&node];
    assert_eq!(ready.route_epoch, RouteEpoch(2));
    assert_eq!(ready.operation, operation);
    assert_ne!(ready.custody, ContentHash([0; 32]));
    assert_ne!(ready.attestation, ContentHash([0; 32]));
    assert_eq!(plan.progress[&node].custody_epoch, 2);
    let (checkpoint, _, _) = reached!(
        founder,
        wait_for(&founder, &host, "placement activated", |state| {
            state.sessions[&ledger].pending.is_none()
                && state.sessions[&ledger].route_epoch == RouteEpoch(2)
        })
    );
    let active = &checkpoint.sessions[&ledger];
    assert_eq!(active.active, desired);
    assert_eq!(active.authority.kind, SessionFenceKind::Activated);
    assert_eq!(active.authority.operation, operation);
    assert_eq!(active.membership_epoch, 1, "no voter changed");
    assert_eq!(active.placement_epoch, 2);
    assert!(active.retiring.is_empty());
    // The founder's own log holds the activated record and serves it.
    let replica = founder.handles.ledger.as_ref().unwrap();
    let facts = replica.registration_facts().await.unwrap().value().clone();
    assert_eq!(
        facts.active.as_ref().map(|(fence, _)| fence.kind),
        Some(SessionFenceKind::Activated)
    );
    // A restart finds nothing pending: only a load report follows.
    let revision = checkpoint.revision;
    founder.stop().await;
    let founder = Running::start(&settings).await;
    let host = partition_host(&founder).await;
    let (after, _, _) = reached!(
        founder,
        wait_for(&founder, &host, "load after restart", |state| {
            state.revision > revision
        })
    );
    assert_eq!(after.revision, revision + 1);
    assert_eq!(after.sessions[&ledger].authority, active.authority);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let (settled, _, _) = observe(&founder, &host, 11).await.unwrap();
    assert_eq!(settled.revision, revision + 1);
    founder.stop().await;
}

/// Join a second host to a running founder and return its service.
pub(crate) async fn join_peer(
    founder: &Running,
    founder_dir: &Path,
    name: &str,
    peer_settings: &crate::network_service::tests::TestSettings,
) -> (Running, u64) {
    let identity = crate::embedded::decode_identity(&founder_dir.join("IDENTITY")).unwrap();
    let admin = focal_wire::UnixRemote::new(
        founder.status.admin_socket.as_ref().unwrap(),
        crate::network_admin::admin_wire_limits(),
    )
    .unwrap();
    let request = crate::network_admin::AdminCommand::invitation(name)
        .unwrap()
        .request(&identity)
        .unwrap();
    let focal_wire::Response::Control { response } = admin.request(&request).await.unwrap().result
    else {
        panic!("private invitation response required");
    };
    let invitation = crate::network_join::NodeInvitation::decode(&response).unwrap();
    let listen = peer_settings.node.listen.unwrap();
    let pending =
        crate::network_join::PendingJoin::open(peer_settings, invitation, listen, listen).unwrap();
    let client = focal_enrollment::EnrollmentClient::bind(
        "127.0.0.1:0".parse().unwrap(),
        focal_enrollment::TransportLimits::default(),
    )
    .unwrap();
    let receipt = pending.redeem(&client, unix_time().unwrap()).await.unwrap();
    let peer_node = receipt.identity.node_id.unwrap();
    drop(pending.install(receipt, unix_time().unwrap()).unwrap());
    (Running::start(peer_settings).await, peer_node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_expands_a_laptop_session_to_three_hosts_that_survive_one_loss() {
    let founder_dir = tempfile::tempdir().unwrap();
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let founder_settings = settings(founder_dir.path());
    let peer_settings = [settings(dirs[0].path()), settings(dirs[1].path())];
    let founder = Running::start(&founder_settings).await;
    let (peer_a, node_a) =
        join_peer(&founder, founder_dir.path(), "host-a", &peer_settings[0]).await;
    let (peer_b, node_b) =
        join_peer(&founder, founder_dir.path(), "host-b", &peer_settings[1]).await;
    let ledger = founder.status.ledger;
    let founder_node = founder.status.node;
    let host = partition_host(&founder).await;
    let enrolled = wait_for(&founder, &host, "three hosts enrolled with load", |state| {
        state.sessions.contains_key(&ledger)
            && [founder_node, node_a, node_b].iter().all(|node| {
                state
                    .nodes
                    .get(node)
                    .is_some_and(|record| record.load.is_some())
            })
    });
    let (checkpoint, _, installed) =
        match tokio::time::timeout(Duration::from_secs(40), enrolled).await {
            Ok(Some(observed)) => observed,
            _ => panic!(
                "enrollment: a {:?}; b {:?}; founder {:?}",
                peer_a.handles.placement.status().await,
                peer_b.handles.placement.status().await,
                founder.handles.placement.status().await
            ),
        };
    let descriptor = checkpoint.sessions[&ledger].clone();
    // The operator asks for one tolerated node loss; the planner picks the
    // three hosts, and the controller executes the plan unattended.
    let policy = focal_directory::PlacementPolicy {
        durability: focal_directory::DurabilityIntent {
            survive: focal_directory::FailureClass::Node,
            max_failures: 1,
        },
        ..descriptor.active.policy.clone()
    };
    let proposal = focal_directory::propose_placement(&checkpoint.nodes, &policy, 31, 1).unwrap();
    assert_eq!(proposal.spec.placement.voters.len(), 3);
    // The planner's leader hint follows measured load; the log's leader stays
    // where it is until a transfer, which this batch does not perform.
    assert!(proposal.spec.placement.voters.contains_key(&founder_node));
    let operation = OperationId::from_u128(2);
    submit(
        &founder,
        &host,
        1,
        &checkpoint,
        &installed,
        PartitionOperation::Session {
            ledger,
            expected_revision: descriptor.revision,
            change: SessionChange::Plan {
                operation,
                desired: proposal.spec.clone(),
                observations: proposal.observations.clone(),
            },
        },
    )
    .await;
    let activated = wait_for(&founder, &host, "placement activated", |state| {
        state.sessions[&ledger].pending.is_none()
            && state.sessions[&ledger].route_epoch == RouteEpoch(2)
    });
    let (checkpoint, _, _) = match tokio::time::timeout(Duration::from_secs(90), activated).await {
        Ok(Some(observed)) => observed,
        _ => panic!(
            "activation: founder {:?}; a {:?}; b {:?}; membership {:?}; session {:?}",
            founder.handles.placement.status().await,
            peer_a.handles.placement.status().await,
            peer_b.handles.placement.status().await,
            founder
                .handles
                .ledger
                .as_ref()
                .unwrap()
                .membership()
                .await
                .map(|view| view.view().clone()),
            observe(&founder, &host, 999)
                .await
                .map(|(state, _, _)| state.sessions[&ledger].clone())
        ),
    };
    let active = &checkpoint.sessions[&ledger];
    assert_eq!(active.active, proposal.spec);
    assert_eq!(active.authority.kind, SessionFenceKind::Activated);
    assert_eq!(
        active.membership_epoch, 3,
        "two promotions advanced the epoch twice"
    );
    assert_eq!(active.placement_epoch, 2);
    assert!(active.retiring.is_empty());
    let report = focal_directory::effective_guarantee(active, &checkpoint.nodes).unwrap();
    assert_eq!(report.achieved, Some(policy.durability));
    assert!(report.blocked_by.is_empty());
    for peer in [&peer_a, &peer_b] {
        assert!(peer.handles.fleet.hosts(ledger));
        assert_eq!(peer.handles.fleet.status().installed, 1);
    }
    // The root grant names three voters at the epoch the fence carries.
    let root = founder.handles.control.observe_root().await.unwrap();
    let grant = root.authority().unwrap().groups[&LogGroupId(ledger.session.0)].clone();
    assert_eq!(grant.membership_epoch, 3);
    assert_eq!(grant.voters.len(), 3);
    assert!(grant.learners.is_empty());
    // Losing one host keeps a quorum: the founder still answers a quorum read
    // of its membership and stays leader.
    peer_b.stop().await;
    let replica = founder.handles.ledger.as_ref().unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if replica.membership().await.is_ok() && replica.progress().leader == founder_node {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the session lost its quorum after one host loss");
    // The lost host returns, reopens its copy and rejoins as a voter.
    let peer_b = Running::start(&peer_settings[1]).await;
    if tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(copy) = peer_b.handles.fleet.current_host(ledger)
                && copy.progress().leader == founder_node
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_err()
    {
        panic!(
            "the returning host never reopened its copy: hosts={} status={:?} progress={:?}",
            peer_b.handles.fleet.hosts(ledger),
            peer_b.handles.placement.status().await,
            peer_b
                .handles
                .fleet
                .current_host(ledger)
                .map(|copy| copy.progress())
        );
    }
    assert_eq!(peer_b.handles.fleet.status().installed, 1);
    peer_a.stop().await;
    peer_b.stop().await;
    founder.stop().await;
}
