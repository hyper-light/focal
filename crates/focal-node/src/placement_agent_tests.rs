use super::*;
use crate::network_service::tests::{Running, settings};
use focal_control::{ControlRequest, ControlRequestId};
use focal_directory::{LogGroupId, OperationId, SessionFenceKind};
use focal_model::RouteEpoch;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

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
    // The operator reads the same facts through the admin socket: one
    // partition, this node alive and loaded, the session at its single-node
    // guarantee with nothing blocking it, and no controller action pending.
    {
        use focal_client::admin::AdminResult;
        let admin = crate::cluster_admin::ClusterAdmin::open(&settings).unwrap();
        let placement = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(AdminResult::Placement { placement }) = admin.placement().await
                    && placement.partitions.iter().any(|partition| {
                        !partition.sessions.is_empty()
                            && partition
                                .nodes
                                .iter()
                                .any(|n| n.node == node && n.disk_available.is_some())
                    })
                {
                    return placement;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        let placement = match placement {
            Ok(placement) => placement,
            Err(_) => panic!(
                "placement never reported: {:?}; status {:?}",
                admin.placement().await,
                founder.handles.placement.status().await
            ),
        };
        assert!(placement.observed_at > 0);
        assert_eq!(placement.partitions.len(), 1);
        let partition = &placement.partitions[0];
        assert_eq!(partition.epoch, 1);
        assert!(partition.sealed.is_none());
        assert!(!partition.truncated);
        assert_eq!(partition.namespace_start, "0".repeat(64));
        assert!(partition.namespace_end.is_none());
        let me = partition.nodes.iter().find(|n| n.node == node).unwrap();
        assert!(me.eligible && me.alive);
        assert_eq!(me.generation, 1);
        assert!(me.disk_available.is_some_and(|bytes| bytes > 0));
        assert_eq!(partition.sessions.len(), 1);
        let session = &partition.sessions[0];
        assert_eq!(session.tenant, ledger.tenant.to_string());
        assert_eq!(session.session, ledger.session.to_string());
        assert_eq!(session.route_epoch, 1);
        assert_eq!(session.voters, vec![node]);
        assert_eq!(session.preferred_leader, node);
        assert_eq!(session.survive, "Node");
        assert_eq!(session.max_failures, 0);
        assert_eq!(session.achieved_max_failures, Some(0));
        assert!(session.blocked_by.is_empty());
        assert!(session.pending.is_none());
        assert!(session.retiring.is_empty());
        let Ok(AdminResult::Plan { actions }) = admin.plan().await else {
            panic!("plan");
        };
        assert!(actions.is_empty(), "{actions:?}");
    }
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

#[test]
fn created_session_identity_is_exact_per_cluster_tenant_and_name() {
    use crate::placement_control::created_session_id;
    let tenant = TenantId::from_u128(9);
    let id = created_session_id([1; 16], tenant, "orders").unwrap();
    assert_eq!(id, created_session_id([1; 16], tenant, "orders").unwrap());
    assert_ne!(id, created_session_id([2; 16], tenant, "orders").unwrap());
    assert_ne!(
        id,
        created_session_id([1; 16], TenantId::from_u128(10), "orders").unwrap()
    );
    assert_ne!(id, created_session_id([1; 16], tenant, "orders2").unwrap());
    assert!(created_session_id([1; 16], tenant, "").is_none());
    assert!(created_session_id([1; 16], TenantId::from_u128(0), "orders").is_none());
    assert!(created_session_id([1; 16], tenant, &"x".repeat(129)).is_none());
}

#[test]
fn install_records_before_created_sessions_decode_as_assigned_copies() {
    #[derive(Serialize)]
    struct CopyV1 {
        group: [u8; 16],
        bootstrap_voters: Vec<u64>,
        route_epoch: RouteEpoch,
        policy_revision: u64,
        voters: BTreeSet<u64>,
        copies: BTreeSet<u64>,
    }
    #[derive(Serialize)]
    struct RecordV1 {
        schema: u16,
        node: u64,
        installed: BTreeMap<LedgerId, CopyV1>,
    }
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: focal_model::SessionId::from_u128(2),
    };
    let legacy = RecordV1 {
        schema: 1,
        node: 4,
        installed: BTreeMap::from([(
            ledger,
            CopyV1 {
                group: [3; 16],
                bootstrap_voters: vec![1],
                route_epoch: RouteEpoch(2),
                policy_revision: 2,
                voters: BTreeSet::from([1, 4]),
                copies: BTreeSet::from([4]),
            },
        )]),
    };
    let record = InstallRecord::decode(&postcard::to_stdvec(&legacy).unwrap()).unwrap();
    assert_eq!(record.schema, INSTALL_RECORD_SCHEMA);
    assert_eq!(record.node, 4);
    let copy = &record.installed[&ledger];
    assert!(!copy.created);
    assert_eq!(copy.group, [3; 16]);
    assert_eq!(copy.bootstrap_voters, vec![1]);
    assert_eq!(copy.route_epoch, RouteEpoch(2));
    assert_eq!(copy.voters, BTreeSet::from([1, 4]));
    let mut current = record;
    current.installed.get_mut(&ledger).unwrap().created = true;
    let again = InstallRecord::decode(&postcard::to_stdvec(&current).unwrap()).unwrap();
    assert!(again.installed[&ledger].created);
    assert_eq!(again.installed[&ledger], current.installed[&ledger]);
    let mut future = current;
    future.schema = INSTALL_RECORD_SCHEMA + 1;
    assert!(InstallRecord::decode(&postcard::to_stdvec(&future).unwrap()).is_err());
}

/// Doc 24 §16: an operator admits a tenant as an enrollment fact and
/// creates sessions by name; the agent hosts, registers and serves them,
/// the local socket's grant follows the registry, and a restart reopens
/// them with the same identities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operators_admit_tenants_and_create_sessions_that_register_serve_and_survive_restart() {
    use crate::{
        cluster_admin::ClusterAdmin,
        placement_control::created_session_id,
        route_cache_tests::{ask, envelope},
    };
    use focal_client::admin::AdminResult;
    use focal_wire::{AccessError, Response, UnixRemote, WireLimits};
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let founder = Running::start(&settings).await;
    let node = founder.status.node;
    let (_, cluster) = founder.handles.fleet.identity();
    let founder_ledger = founder.status.ledger;
    let host = partition_host(&founder).await;
    let admin = ClusterAdmin::open(&settings).unwrap();
    let AdminResult::Tenants {
        founder: founder_tenant,
        admitted,
        applied_index,
        ..
    } = admin.tenants().await.unwrap()
    else {
        panic!("tenants");
    };
    assert_eq!(founder_tenant, founder_ledger.tenant.to_string());
    assert!(admitted.is_empty());
    assert!(applied_index > 0);
    let tenant = TenantId([9; 16]);
    let name = "orders";
    let ledger = LedgerId {
        tenant,
        session: created_session_id(cluster, tenant, name).unwrap(),
    };
    // A tenant the cluster does not serve gets no session, and the local
    // socket's grant does not name it.
    assert!(admin.create_session(tenant.0, name).await.is_err());
    let local =
        UnixRemote::new(directory.path().join("focal.sock"), WireLimits::default()).unwrap();
    assert!(matches!(
        local
            .request(&envelope(ledger, 1, 601))
            .await
            .unwrap()
            .result,
        Response::Error(AccessError::Unauthorized)
    ));
    assert!(admin.admit_tenant([0; 16]).await.is_err());
    let AdminResult::Tenants {
        admitted, revision, ..
    } = admin.admit_tenant(tenant.0).await.unwrap()
    else {
        panic!("admit");
    };
    assert_eq!(admitted, vec![tenant.to_string()]);
    // A retry reads as done at the same committed revision.
    let AdminResult::Tenants {
        admitted: same,
        revision: again,
        ..
    } = admin.admit_tenant(tenant.0).await.unwrap()
    else {
        panic!("admit again");
    };
    assert_eq!((same, again), (admitted.clone(), revision));
    let AdminResult::Tenants {
        admitted: listed, ..
    } = admin.tenants().await.unwrap()
    else {
        panic!("list");
    };
    assert_eq!(listed, admitted);
    // The same name is the same session; another name or tenant is another.
    let AdminResult::SessionCreated {
        session: created,
        existing,
        node: at,
        group,
        ..
    } = admin.create_session(tenant.0, name).await.unwrap()
    else {
        panic!("create");
    };
    assert!(!existing);
    assert_eq!(at, node);
    assert_eq!(created, ledger.session.to_string());
    assert_eq!(group, crate::cluster_admin::hex(&ledger.session.0));
    let AdminResult::SessionCreated {
        session: retried,
        existing,
        ..
    } = admin.create_session(tenant.0, name).await.unwrap()
    else {
        panic!("retry");
    };
    assert!(existing);
    assert_eq!(retried, created);
    let other_ledger = LedgerId {
        tenant: founder_ledger.tenant,
        session: created_session_id(cluster, founder_ledger.tenant, "reports").unwrap(),
    };
    let AdminResult::SessionCreated {
        session: other,
        existing,
        ..
    } = admin
        .create_session(founder_ledger.tenant.0, "reports")
        .await
        .unwrap()
    else {
        panic!("create other");
    };
    assert!(!existing);
    assert_eq!(other, other_ledger.session.to_string());
    assert!(admin.create_session(tenant.0, "").await.is_err());
    assert!(founder.handles.fleet.current_host(ledger).is_ok());
    assert!(founder.handles.fleet.current_host(other_ledger).is_ok());
    // The agent registers both exactly as it did the founder's session,
    // recording the founding node.
    let (checkpoint, _, _) = reached!(
        founder,
        wait_for(&founder, &host, "created sessions registered", |state| {
            state.sessions.contains_key(&ledger) && state.sessions.contains_key(&other_ledger)
        })
    );
    for registered in [ledger, other_ledger, founder_ledger] {
        let descriptor = &checkpoint.sessions[&registered];
        assert_eq!(descriptor.founder, Some(node));
        assert_eq!(descriptor.authority.kind, SessionFenceKind::Created);
        assert_eq!(descriptor.route_epoch, RouteEpoch(1));
        assert_eq!(
            descriptor.active.placement.voters,
            BTreeMap::from([(node, 1)])
        );
        assert_eq!(descriptor.log_group, LogGroupId(registered.session.0));
        assert!(descriptor.pending.is_none());
    }
    // Served: through the node's data handler under a grant naming the
    // tenant, and through the local socket once its grant follows the
    // registry (no restart).
    let served = ask(&founder, envelope(ledger, 1, 602)).await;
    assert!(!matches!(served, Response::Error(_)), "{served:?}");
    let served = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let result = local
                .request(&envelope(ledger, 1, 603))
                .await
                .unwrap()
                .result;
            if !matches!(result, Response::Error(AccessError::Unauthorized)) {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("local grant never followed the registry");
    assert!(!matches!(served, Response::Error(_)), "{served:?}");
    let AdminResult::Placement { placement } = admin.placement().await.unwrap() else {
        panic!("placement");
    };
    let view = placement.partitions[0]
        .sessions
        .iter()
        .find(|session| session.session == created)
        .expect("created session in the operator view");
    assert_eq!(view.founder, Some(node));
    assert_eq!(view.tenant, tenant.to_string());
    founder.stop().await;

    // A restart reopens every created session from the install record; the
    // names still denote the same sessions and the tenant stays served.
    let founder = Running::start(&settings).await;
    // The founder's own session opens with the service; created sessions
    // are reopened by the agent's first tick from the install record.
    tokio::time::timeout(Duration::from_secs(30), async {
        while founder.handles.fleet.current_host(ledger).is_err()
            || founder.handles.fleet.current_host(other_ledger).is_err()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("created sessions reopened at restart");
    let admin = ClusterAdmin::open(&settings).unwrap();
    let AdminResult::SessionCreated {
        session: reopened,
        existing,
        ..
    } = admin.create_session(tenant.0, name).await.unwrap()
    else {
        panic!("retry after restart");
    };
    assert!(existing);
    assert_eq!(reopened, created);
    let AdminResult::Tenants { admitted, .. } = admin.tenants().await.unwrap() else {
        panic!("tenants after restart");
    };
    assert_eq!(admitted, vec![tenant.to_string()]);
    // A reopened single-voter log serves once it has elected itself again.
    let served = tokio::time::timeout(Duration::from_secs(20), async {
        let mut id = 604;
        loop {
            let result = ask(&founder, envelope(ledger, 1, id)).await;
            if !matches!(result, Response::Error(AccessError::Unavailable)) {
                return result;
            }
            id += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("reopened session never served");
    assert!(!matches!(served, Response::Error(_)), "{served:?}");
    let status = founder.handles.placement.status().await.unwrap();
    assert!(status.installed.contains(&ledger));
    assert!(status.installed.contains(&other_ledger));
    assert_eq!(status.admission.tenants.len(), 2);
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
    // The route epoch moved to 2: a client still at epoch 1 is answered with
    // the current epoch and the leader's endpoint by every host, and the
    // leader serves a current client without redirecting it.
    {
        use crate::route_cache_tests::{ask, envelope, redirect_from};
        let stale = redirect_from(&founder, envelope(ledger, 1, 9_001)).await;
        assert_eq!(stale.epoch, RouteEpoch(2));
        assert_eq!(stale.endpoint, founder.status.advertise.to_string());
        let stale_at_a = redirect_from(&peer_a, envelope(ledger, 1, 9_002)).await;
        assert_eq!(stale_at_a.epoch, RouteEpoch(2));
        assert_eq!(stale_at_a.endpoint, founder.status.advertise.to_string());
        // The founder re-fenced its replica to the activated route: a
        // current client is served there, not refused or redirected.
        let current = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let result = ask(&founder, envelope(ledger, 2, 9_003)).await;
                if !matches!(
                    result,
                    focal_wire::Response::Error(focal_wire::AccessError::Unavailable)
                ) {
                    return result;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the founder never served the activated route");
        assert!(
            matches!(current, focal_wire::Response::Summary(_)),
            "{current:?}; founder progress {:?}; agent {:?}",
            founder
                .handles
                .fleet
                .current_host(ledger)
                .map(|host| host.progress()),
            founder
                .handles
                .placement
                .status()
                .await
                .map(|s| s.last_error)
        );
    }
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
    if tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if replica.membership().await.is_ok() && replica.progress().leader == founder_node {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_err()
    {
        panic!(
            "the session lost its quorum after one host loss: handle progress {:?}; fleet progress {:?}; membership {:?}; a fleet {:?}; a status {:?}; founder status {:?}",
            replica.progress(),
            founder
                .handles
                .fleet
                .current_host(ledger)
                .map(|host| host.progress()),
            replica.membership().await.map(|view| view.view().clone()),
            peer_a
                .handles
                .fleet
                .current_host(ledger)
                .map(|host| host.progress()),
            peer_a
                .handles
                .placement
                .status()
                .await
                .map(|s| s.last_error),
            founder
                .handles
                .placement
                .status()
                .await
                .map(|s| s.last_error),
        );
    }
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
