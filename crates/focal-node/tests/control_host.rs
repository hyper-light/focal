#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::*;
use focal_enrollment::*;
use focal_memory::MemoryBudget;
use focal_model::{
    LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId,
};
use focal_node::control_host::*;
use focal_wire::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [61; 16];
const GROUP: [u8; 16] = [62; 16];
const OPERATOR: [u8; 16] = [63; 16];
// This verifier rejects all evidence-bearing directory operations. Tests never
// invent a positive attestation: region registration and partition sealing need
// authenticated operator authority, while enrollment uses genuine CSR/CA checks.
struct RejectUnverifiedEvidence;
impl AuthorityVerifier for RejectUnverifiedEvidence {
    fn verify_enrollment(&self, _: &NodeEnrollment) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_session_fence(&self, _: &SessionFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_replica_ready(&self, _: &ReplicaReady) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_custody(&self, _: &CustodyProof) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_delegation(&self, _: &DelegationFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
fn namespace() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(61),
        session: SessionId::from_u128(62),
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(OPERATOR),
        tenants: [namespace().tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
/// Holds the entire remaining allowance. The host's own tick may take or
/// release bytes between a statistics read and a reservation, so exhaustion is
/// reached by reserving until even one byte is refused rather than by trusting
/// a single snapshot.
fn exhaust(memory: &MemoryBudget) -> Vec<focal_memory::Allocation> {
    let mut held = Vec::new();
    for _ in 0..64 {
        let stats = memory.stats();
        let remaining = stats.limit.saturating_sub(stats.used).max(1);
        match memory.reserve(
            focal_memory::BudgetKind::Control,
            focal_memory::BudgetLane::Completion,
            remaining,
        ) {
            Ok(reservation) => held.push(reservation.commit()),
            Err(_) if remaining == 1 => return held,
            Err(_) => {}
        }
    }
    panic!("the control budget never reached exhaustion")
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn root_bootstrap(authority: &BootstrapAuthority) -> ControlBootstrap {
    let root = RootDirectory::new(
        focal_directory::ClusterId(CLUSTER),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    let enrollment = EnrollmentRegistry::new(
        CLUSTER,
        authority.ca_certificate().to_vec(),
        4,
        EnrollmentLimits::default(),
    )
    .unwrap();
    ControlBootstrap::root(&root, &enrollment).unwrap()
}
fn request(sequence: u64, command: ControlCommand) -> ControlRequest {
    ControlRequest {
        id: ControlRequestId {
            client: OPERATOR,
            sequence,
        },
        acknowledged_through: 0,
        command,
    }
}
fn region(revision: u64, id: u128) -> ControlCommand {
    ControlCommand::Root(RootCommand {
        expected_revision: revision,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId::from_u128(id),
                label: format!("region-{id}"),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    })
}
struct Rig {
    directories: Vec<tempfile::TempDir>,
    hosts: Vec<ControlHost>,
    owners: Vec<ControlOwner>,
    routers: Vec<tokio::task::JoinHandle<()>>,
    isolated: Arc<AtomicU64>,
    bootstrap: ControlBootstrap,
    group: [u8; 16],
}
impl Rig {
    fn new(bootstrap: ControlBootstrap, group: [u8; 16]) -> Self {
        let mut value = Self {
            directories: (0..3).map(|_| tempfile::tempdir().unwrap()).collect(),
            hosts: vec![],
            owners: vec![],
            routers: vec![],
            isolated: Arc::new(AtomicU64::new(0)),
            bootstrap,
            group,
        };
        value.start();
        value
    }
    fn start(&mut self) {
        let mut channels = Vec::new();
        for (offset, directory) in self.directories.iter().enumerate() {
            let id = offset as u64 + 1;
            let mut config = NodeConfig::single(id, CLUSTER, self.group);
            config.voters = vec![1, 2, 3];
            let allowance = budget();
            let replica = ControlReplica::open(
                ControlOptions::new(config),
                self.bootstrap.clone(),
                allowance.clone(),
                directory.path(),
            )
            .unwrap();
            let mut config = ControlHostConfig::new(namespace());
            config.tick = Duration::from_millis(25);
            config.request_timeout = Duration::from_millis(350);
            let (host, owner, channel) =
                ControlHost::spawn(replica, RejectUnverifiedEvidence, config, allowance).unwrap();
            self.hosts.push(host);
            self.owners.push(owner);
            channels.push((id, channel));
        }
        for (from, mut channel) in channels {
            let hosts = self.hosts.clone();
            let isolated = self.isolated.clone();
            self.routers.push(tokio::spawn(async move {
                while let Some(frame) = channel.recv().await {
                    let excluded = isolated.load(Ordering::SeqCst);
                    if excluded == from || excluded == frame.target {
                        continue;
                    }
                    let Some(target) = hosts.get(frame.target.saturating_sub(1) as usize) else {
                        continue;
                    };
                    let verified = verify_request(
                        peer(PeerRole::Node { node_id: from }),
                        frame.request.clone(),
                        &ControlHost::wire_limits(),
                    )
                    .unwrap();
                    let _ = target.handle(verified).await;
                    drop(frame);
                }
            }));
        }
    }
    async fn leader(&self, exclude: u64) -> usize {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for (index, host) in self.hosts.iter().enumerate() {
                    let status = host.progress();
                    if status.node != exclude
                        && status.leader == status.node
                        && host
                            .read(
                                peer(PeerRole::Runtime),
                                RequestId::from_u128(900),
                                ControlRead::State,
                            )
                            .await
                            .is_ok()
                    {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn state(&self, index: usize) -> ControlSnapshot {
        self.state_on_leader(index).await.1
    }
    async fn commit_on_leader(
        &self,
        mut index: usize,
        request: ControlRequest,
        excluded: u64,
    ) -> (usize, ControlReceipt) {
        // A successful setup read is not a lease on the leader. Preserve the
        // exact command and request ID across short host deadlines or elections;
        // the tests below still exercise minority/refusal boundaries directly.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self.hosts[index]
                    .submit(peer(PeerRole::Runtime), request.clone())
                    .await
                {
                    Ok(receipt) => {
                        assert_eq!(receipt.request, request.id);
                        return (index, receipt);
                    }
                    Err(
                        ControlFailure::OutcomeUnknown
                        | ControlFailure::Unavailable
                        | ControlFailure::NotLeader { .. }
                        | ControlFailure::NotReady,
                    ) => index = self.leader(excluded).await,
                    Err(error) => panic!("unexpected setup mutation failure: {error:?}"),
                }
            }
        })
        .await
        .expect("exact setup mutation did not commit within five seconds")
    }
    async fn state_on_leader(&self, mut index: usize) -> (usize, ControlSnapshot) {
        // A completed ReadIndex is not an owner lease. These eventual-state
        // assertions rediscover on transient leadership loss, while direct
        // minority reads elsewhere in the tests must still fail immediately.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self.hosts[index]
                    .read(
                        peer(PeerRole::Runtime),
                        RequestId::from_u128(901),
                        ControlRead::State,
                    )
                    .await
                {
                    Ok(ControlReadResult::State(snapshot)) => return (index, snapshot),
                    Ok(_) => panic!("state expected"),
                    Err(
                        ControlFailure::Unavailable
                        | ControlFailure::NotLeader { .. }
                        | ControlFailure::NotReady,
                    ) => index = self.leader(0).await,
                    Err(error) => panic!("unexpected state read failure: {error:?}"),
                }
            }
        })
        .await
        .expect("no quorum-ready state owner within five seconds")
    }
    async fn stop(&mut self) {
        for host in &self.hosts {
            host.stop().await.unwrap();
        }
        for owner in self.owners.drain(..) {
            owner.join().unwrap();
        }
        for router in self.routers.drain(..) {
            router.await.unwrap();
        }
        self.hosts.clear();
    }
}
fn registry(snapshot: &ControlSnapshot) -> EnrollmentRegistry {
    let ControlBootstrap::Root { enrollment, .. } = &snapshot.state else {
        panic!("root expected")
    };
    EnrollmentRegistry::restore(enrollment, CLUSTER, EnrollmentLimits::default()).unwrap()
}

#[tokio::test]
async fn directory_bootstrap_authorization_requires_a_fresh_root_quorum() {
    use focal_node::directory_bootstrap::{DirectoryBootstrapError, FirstDirectoryPlan};
    let directory = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        directory.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    let leader = rig.leader(0).await;
    let plan = FirstDirectoryPlan::derive(CLUSTER, 1).unwrap();
    // The quorum is live, but no real delegation/grant exists in this root.
    assert!(matches!(
        rig.hosts[leader].prepare_directory(plan).await,
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    let old_leader = rig.hosts[leader].progress().node;
    rig.isolated.store(old_leader, Ordering::SeqCst);
    // Even denial must be evaluated behind this request's new barrier. A
    // previously completed read cannot authorize a later startup request.
    assert!(matches!(
        rig.hosts[leader].prepare_directory(plan).await,
        Err(DirectoryBootstrapError::Unavailable)
    ));
    rig.isolated.store(0, Ordering::SeqCst);
    rig.stop().await;
}
fn join_request(
    authority: &BootstrapAuthority,
    invitation: &Invitation,
    key: &JoinKey,
) -> JoinRequest {
    let mut client = rustls::ClientConnection::new(
        Arc::new(invitation.client_config().unwrap()),
        rustls::pki_types::ServerName::try_from(invitation.trust().server_name.clone()).unwrap(),
    )
    .unwrap();
    let mut server = rustls::ServerConnection::new(Arc::new(
        authority.server_identity().server_config().unwrap(),
    ))
    .unwrap();
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            server.process_new_packets().unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return invitation.request_after_tls(&client, key, now()).unwrap();
        }
    }
    panic!("TLS handshake did not finish")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventual_state_rediscovers_majority_after_cached_owner_loses_quorum() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let stale = rig.leader(0).await;
    let (stale, _) = rig
        .commit_on_leader(stale, request(1, region(0, 1)), 0)
        .await;
    let excluded = rig.hosts[stale].progress().node;
    rig.isolated.store(excluded, Ordering::SeqCst);
    let majority = rig.leader(excluded).await;
    let (_, committed) = rig
        .commit_on_leader(majority, request(2, region(1, 2)), excluded)
        .await;
    assert_eq!(committed.revisions.root, 2);
    // A cached owner is not a lease: this read must still refuse minority state.
    assert!(
        rig.hosts[stale]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(903),
                ControlRead::State,
            )
            .await
            .is_err()
    );
    let (owner, snapshot) = rig.state_on_leader(stale).await;
    assert_ne!(owner, stale);
    assert!(snapshot.applied_index >= committed.committed_index);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_enrollment_majority_commit_exact_retry_and_disk_restart() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    let first = request(1, region(0, 1));
    let first_receipt = rig.hosts[leader]
        .submit(peer(PeerRole::Runtime), first.clone())
        .await
        .unwrap();
    assert_eq!(first_receipt.revisions.root, 1);
    let isolated_id = rig.hosts[leader].progress().node;
    rig.isolated.store(isolated_id, Ordering::SeqCst);
    let pending = request(2, region(1, 2));
    let lost = rig.hosts[leader]
        .submit(peer(PeerRole::Runtime), pending.clone())
        .await;
    assert!(matches!(
        lost,
        Err(ControlFailure::OutcomeUnknown
            | ControlFailure::NotLeader { .. }
            | ControlFailure::NotReady)
    ));
    assert!(
        rig.hosts[leader]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(902),
                ControlRead::State
            )
            .await
            .is_err()
    );
    let majority = rig.leader(isolated_id).await;
    let second_receipt = rig.hosts[majority]
        .submit(peer(PeerRole::Runtime), pending.clone())
        .await
        .unwrap();
    assert_eq!(second_receipt.revisions.root, 2);
    rig.isolated.store(0, Ordering::SeqCst);
    let (majority, snapshot) = rig.state_on_leader(majority).await;
    let draft = registry(&snapshot)
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    let invitation_request = request(3, ControlCommand::Enrollment(draft.command().clone()));
    let invitation_receipt = rig.hosts[majority]
        .submit(peer(PeerRole::Runtime), invitation_request.clone())
        .await
        .unwrap();
    let (majority, snapshot) = rig.state_on_leader(majority).await;
    let invitation = draft.release(&registry(&snapshot)).unwrap();
    let join_key = JoinKey::open_or_create(keys.path().join("join"), CLUSTER).unwrap();
    let join = join_request(&authority, &invitation, &join_key);
    let JoinPreparation::Commit(command) = registry(&snapshot)
        .prepare_join(&authority, &join, now())
        .unwrap()
    else {
        panic!("new enrollment expected")
    };
    let enrollment_request = request(4, ControlCommand::Enrollment(command));
    let enrollment_receipt = rig.hosts[majority]
        .submit(peer(PeerRole::Runtime), enrollment_request.clone())
        .await
        .unwrap();
    let enrolled = registry(&rig.state(majority).await)
        .release(&join, now())
        .unwrap();
    assert_eq!(enrolled.identity.node_id, Some(4));
    assert_eq!(enrollment_receipt.revisions.enrollment, 2);
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.stop().await;
    rig.start();
    rig.hosts[0].campaign().await.unwrap();
    let recovered = rig.leader(0).await;
    for (request, receipt) in [
        (first, first_receipt),
        (pending, second_receipt),
        (invitation_request, invitation_receipt),
        (enrollment_request, enrollment_receipt),
    ] {
        assert_eq!(
            rig.hosts[recovered]
                .submit(peer(PeerRole::Runtime), request)
                .await
                .unwrap(),
            receipt
        );
    }
    let snapshot = rig.state(recovered).await;
    let registry = registry(&snapshot);
    assert_eq!(registry.release(&join, now()).unwrap(), enrolled);
    assert_eq!(registry.enrollments().count(), 1);
    assert!(matches!(
        registry.prepare_join(&authority, &join, now()).unwrap(),
        JoinPreparation::Existing(_)
    ));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_owner_replication_and_authorization_are_independent_of_root() {
    let group = [72; 16];
    let directory = DirectoryPartition::new(
        focal_directory::ClusterId(CLUSTER),
        Delegation {
            namespace: NamespaceRange::all(),
            partition: PartitionId::from_u128(1),
            region: RegionId::from_u128(1),
            log_group: LogGroupId(group),
            epoch: 1,
            activation: None,
        },
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    let mut rig = Rig::new(ControlBootstrap::partition(&directory), group);
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    assert_eq!(
        rig.hosts[leader]
            .submit(peer(PeerRole::Actor), request(1, region(0, 1)))
            .await,
        Err(ControlFailure::Unauthorized)
    );
    let forged = ControlRequest {
        id: ControlRequestId {
            client: [99; 16],
            sequence: 1,
        },
        acknowledged_through: 0,
        command: region(0, 1),
    };
    assert_eq!(
        rig.hosts[leader]
            .submit(peer(PeerRole::Runtime), forged)
            .await,
        Err(ControlFailure::Unauthorized)
    );
    assert_eq!(
        rig.hosts[leader]
            .submit(peer(PeerRole::Runtime), request(1, region(0, 1)))
            .await,
        Err(ControlFailure::WrongOwner)
    );
    let request = request(
        1,
        ControlCommand::Partition(PartitionCommand {
            expected_revision: 0,
            delegation_epoch: 1,
            operation: PartitionOperation::SealForTransfer {
                operation: OperationId::from_u128(1),
                destination: PartitionId::from_u128(2),
                next_epoch: 2,
            },
        }),
    );
    let receipt = rig.hosts[leader]
        .submit(peer(PeerRole::Runtime), request.clone())
        .await
        .unwrap();
    assert_eq!(receipt.revisions.partition, 1);
    let rpc = ControlRpc::Read(ControlRead::State).encode(65536).unwrap();
    for (ledger, group) in [
        (
            LedgerId {
                session: SessionId::from_u128(999),
                ..namespace()
            },
            group,
        ),
        (namespace(), GROUP),
    ] {
        let wire = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(55),
            operation: Operation::Control {
                group,
                request: rpc.clone(),
            },
        };
        let verified =
            verify_request(peer(PeerRole::Runtime), wire, &ControlHost::wire_limits()).unwrap();
        let result = rig.hosts[leader].handle(verified).await;
        let Response::Control { response } = result.result else {
            panic!("control response expected")
        };
        assert!(matches!(
            ControlReply::decode(&response, 65536).unwrap(),
            ControlReply::Rejected(ControlFailure::Unauthorized | ControlFailure::WrongOwner)
        ));
    }
    let mut message = focal_consensus::Message {
        from: 2,
        to: rig.hosts[leader].progress().node,
        term: 1,
        ..Default::default()
    };
    message.set_msg_type(focal_consensus::MessageType::MsgHeartbeat);
    use focal_consensus::PbMessageExt;
    let wire = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(56),
        operation: Operation::Raft {
            group,
            message: message.write_to_bytes().unwrap(),
        },
    };
    let verified = verify_request(
        peer(PeerRole::Node { node_id: 999 }),
        wire,
        &ControlHost::wire_limits(),
    )
    .unwrap();
    assert_eq!(
        rig.hosts[leader].handle(verified).await.result,
        Response::Error(AccessError::Unauthorized)
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.stop().await;
    rig.start();
    rig.hosts[0].campaign().await.unwrap();
    let recovered = rig.leader(0).await;
    assert_eq!(
        rig.hosts[recovered]
            .submit(peer(PeerRole::Runtime), request)
            .await
            .unwrap(),
        receipt
    );
    let ControlBootstrap::Partition { directory } = rig.state(recovered).await.state else {
        panic!("partition expected")
    };
    assert_eq!(
        directory.sealed.unwrap().destination,
        PartitionId::from_u128(2)
    );
    rig.stop().await;
}

#[tokio::test]
async fn owned_control_response_retains_input_and_export_budgets_until_delivery_drop() {
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let memory = budget();
    let replica = ControlReplica::open(
        ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP)),
        root_bootstrap(&authority),
        memory.clone(),
        data.path().join("log"),
    )
    .unwrap();
    let (host, owner, _outgoing) = ControlHost::spawn(
        replica,
        RejectUnverifiedEvidence,
        ControlHostConfig::new(namespace()),
        memory.clone(),
    )
    .unwrap();
    host.campaign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if host
                .read(
                    peer(PeerRole::Runtime),
                    RequestId::from_u128(1),
                    ControlRead::State,
                )
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = memory.stats();
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(2),
        operation: Operation::Control {
            group: GROUP,
            request: ControlRpc::Read(ControlRead::State).encode(65536).unwrap(),
        },
    };
    let verified = verify_request(
        peer(PeerRole::Runtime),
        request,
        &ControlHost::wire_limits(),
    )
    .unwrap();
    let response = host.handle_accounted(verified).await;
    assert!(matches!(
        response.envelope().result,
        Response::Control { .. }
    ));
    let retained = memory.stats();
    use focal_memory::BudgetKind;
    assert!(
        retained.by_kind[BudgetKind::Pending as usize]
            > before.by_kind[BudgetKind::Pending as usize]
    );
    assert!(
        retained.by_kind[BudgetKind::Control as usize]
            > before.by_kind[BudgetKind::Control as usize]
    );
    // The transport may hold this value while a slow peer consumes its bytes.
    drop(response);
    assert_eq!(memory.stats(), before);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[tokio::test]
async fn follower_root_observation_exports_one_durable_prefix_and_retains_delivery_budget_after_stop()
 {
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let key = JoinKey::open_or_create(data.path().join("key"), CLUSTER).unwrap();
    let founder = FoundingEnrollmentDraft::open_or_create(
        data.path().join("founder"),
        &authority,
        &key,
        1,
        OPERATOR,
        EnrollmentLimits::default(),
        now(),
    )
    .unwrap();
    let root = RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget()).unwrap();
    let bootstrap = ControlBootstrap::root(&root, founder.registry()).unwrap();
    let options = ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP));
    let path = data.path().join("log");
    let mut replica =
        ControlReplica::open(options.clone(), bootstrap.clone(), budget(), &path).unwrap();
    replica.drain(&RejectUnverifiedEvidence).unwrap();
    replica.campaign().unwrap();
    replica.drain(&RejectUnverifiedEvidence).unwrap();
    let commit = |replica: &mut ControlReplica, request: ControlRequest| {
        let id = request.id;
        replica.submit(request, &RejectUnverifiedEvidence).unwrap();
        for _ in 0..8 {
            replica.drain(&RejectUnverifiedEvidence).unwrap();
            if let Some(receipt) = replica.receipt(id).unwrap() {
                return receipt;
            }
        }
        panic!("single-voter control proposal did not durably publish");
    };
    let region = commit(&mut replica, request(1, region(0, 1)));
    let contact = commit(
        &mut replica,
        request(
            2,
            ControlCommand::NodeContact(NodeContactCommand {
                node: 1,
                principal: OPERATOR,
                certificate_fingerprint: certificate_fingerprint(&founder.receipt().certificate),
                advertise: "127.0.0.1:7443".parse().unwrap(),
                expected_generation: 0,
                decided_at: now(),
                region: None,
                zone: None,
                endpoint: None,
            }),
        ),
    );
    let configuration = replica.configuration();
    let membership = commit(
        &mut replica,
        request(
            3,
            ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: configuration.configuration_index,
                expected: configuration.configuration,
                change: MembershipChange::AddLearner { node: 2 },
            }),
        ),
    );
    replica.checkpoint().unwrap();
    drop(replica);
    // Reopen without campaigning. All exported rows are recovered from disk,
    // while this owner has no current leader or quorum-read authority.
    let memory = budget();
    let replica = ControlReplica::open(options, bootstrap, memory.clone(), &path).unwrap();
    let mut config = ControlHostConfig::new(namespace());
    config.tick = Duration::from_secs(1);
    let (host, owner, outgoing) =
        ControlHost::spawn(replica, RejectUnverifiedEvidence, config, memory.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while host.progress().applied_index != membership.committed_index {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(host.progress().leader, 0);
    assert!(matches!(
        host.read(
            peer(PeerRole::Node { node_id: 1 }),
            RequestId::from_u128(90),
            ControlRead::State
        )
        .await,
        Err(ControlFailure::NotLeader { .. })
    ));
    let before = memory.stats();
    let exhausted = exhaust(&memory);
    assert!(matches!(
        host.observe_root().await,
        Err(ControlFailure::Capacity)
    ));
    drop(exhausted);
    assert_eq!(
        memory.stats().by_kind[focal_memory::BudgetKind::Control as usize],
        before.by_kind[focal_memory::BudgetKind::Control as usize]
    );
    let mut cancelled = Box::pin(host.observe_root());
    drop(std::future::Future::poll(
        cancelled.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    ));
    drop(cancelled);
    // The FIFO barrier also covers an observation whose receiver disappeared
    // before delivery; neither input nor exported state may leak its allowance.
    drop(host.observe_root().await.unwrap());
    assert_eq!(
        memory.stats().by_kind[focal_memory::BudgetKind::Control as usize],
        before.by_kind[focal_memory::BudgetKind::Control as usize]
    );
    let mut pending = Box::pin(host.observe_root());
    let immediate = match std::future::Future::poll(
        pending.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    ) {
        std::task::Poll::Ready(result) => Some(result.unwrap()),
        std::task::Poll::Pending => None,
    };
    // A second FIFO observation proves the first was delivered. Keep it in
    // its unpolled oneshot unless the owner already won the first poll race.
    let delivered = host.observe_root().await.unwrap();
    assert_eq!(
        delivered.snapshot().applied_index,
        membership.committed_index
    );
    assert_eq!(
        delivered.contacts().applied_index,
        delivered.snapshot().applied_index
    );
    assert_eq!(
        delivered.configuration().applied_index,
        delivered.snapshot().applied_index
    );
    assert_eq!(delivered.contacts().identity, delivered.snapshot().identity);
    assert_eq!(
        delivered.configuration().identity,
        delivered.snapshot().identity
    );
    assert_eq!(delivered.snapshot().revisions.root, region.revisions.root);
    assert_eq!(delivered.snapshot().revisions.enrollment, 1);
    assert_eq!(registry(delivered.snapshot()).enrollments().count(), 1);
    assert_eq!(delivered.contacts().contacts.records.len(), 1);
    assert_eq!(
        delivered.contacts().contacts.records[0].committed_index,
        contact.committed_index
    );
    assert_eq!(
        delivered.contacts().contacts.records[0].advertise,
        "127.0.0.1:7443".parse().unwrap()
    );
    assert_eq!(
        delivered.configuration().configuration_index,
        membership.committed_index
    );
    assert_eq!(delivered.configuration().configuration.voters, vec![1]);
    assert_eq!(delivered.configuration().configuration.learners, vec![2]);
    let two = memory.stats();
    assert!(
        two.by_kind[focal_memory::BudgetKind::Control as usize]
            > before.by_kind[focal_memory::BudgetKind::Control as usize]
    );
    drop(delivered);
    let queued = memory.stats();
    assert!(queued.used > before.used);
    assert!(queued.used < two.used);
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(outgoing);
    let stopped = memory.stats();
    assert!(
        stopped.used > 0,
        "unconsumed oneshot lost its exported-state reservation"
    );
    let observation = match immediate {
        Some(value) => value,
        None => pending.await.unwrap(),
    };
    assert_eq!(
        memory.stats(),
        stopped,
        "delivery itself must not release the reservation"
    );
    assert_eq!(
        observation.contacts().contacts.records[0].committed_index,
        contact.committed_index
    );
    assert_eq!(
        observation.configuration().configuration_index,
        membership.committed_index
    );
    drop(observation);
    assert_eq!(memory.stats().used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_requires_runtime_and_returns_only_committed_configuration_receipts() {
    let keys = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        keys.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut rig = Rig::new(root_bootstrap(&authority), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    let ControlReadResult::Configuration(before) = rig.hosts[leader]
        .read(
            peer(PeerRole::Runtime),
            RequestId::from_u128(910),
            ControlRead::Configuration,
        )
        .await
        .unwrap()
    else {
        panic!("configuration")
    };
    let add = request(
        1,
        ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: before.configuration_index,
            expected: before.configuration.clone(),
            change: MembershipChange::AddLearner { node: 4 },
        }),
    );
    for role in [
        PeerRole::Actor,
        PeerRole::Evaluator,
        PeerRole::Node { node_id: 2 },
    ] {
        assert!(matches!(
            rig.hosts[leader].submit(peer(role), add.clone()).await,
            Err(ControlFailure::Unauthorized)
        ));
    }
    let (leader, added) = rig.commit_on_leader(leader, add.clone(), 0).await;
    let ControlReadResult::Configuration(after) = rig.hosts[leader]
        .read(
            peer(PeerRole::Runtime),
            RequestId::from_u128(911),
            ControlRead::Configuration,
        )
        .await
        .unwrap()
    else {
        panic!("configuration")
    };
    assert_eq!(after.configuration_index, added.committed_index);
    assert_eq!(after.configuration.learners, vec![4]);
    assert_eq!(
        rig.hosts[(leader + 1) % 3]
            .submit(peer(PeerRole::Runtime), add)
            .await
            .unwrap(),
        added
    );
    // The absent fourth replica cannot be promoted merely because admission committed.
    let promote = request(
        2,
        ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: after.configuration_index,
            expected: after.configuration.clone(),
            change: MembershipChange::Promote { node: 4 },
        }),
    );
    assert!(
        rig.hosts[leader]
            .submit(peer(PeerRole::Runtime), promote)
            .await
            .is_err()
    );
    let remove = request(
        2,
        ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: after.configuration_index,
            expected: after.configuration,
            change: MembershipChange::Remove { node: 4 },
        }),
    );
    let removed = rig.hosts[leader]
        .submit(peer(PeerRole::Runtime), remove)
        .await
        .unwrap();
    let ControlReadResult::Configuration(current) = rig.hosts[leader]
        .read(
            peer(PeerRole::Runtime),
            RequestId::from_u128(912),
            ControlRead::Configuration,
        )
        .await
        .unwrap()
    else {
        panic!("configuration")
    };
    assert_eq!(current.configuration_index, removed.committed_index);
    assert!(current.configuration.learners.is_empty());
    let target = ((leader + 1) % 3) as u64 + 1;
    let transfer = ControlTransfer {
        expected_configuration_index: current.configuration_index,
        expected: current.configuration,
        target,
    };
    assert!(matches!(
        rig.hosts[leader]
            .transfer(
                peer(PeerRole::Node { node_id: target }),
                RequestId::from_u128(913),
                transfer.clone()
            )
            .await,
        Err(ControlFailure::Unauthorized)
    ));
    rig.hosts[leader]
        .transfer(peer(PeerRole::Runtime), RequestId::from_u128(914), transfer)
        .await
        .unwrap();
    let next = rig.leader(rig.hosts[leader].progress().node).await;
    assert_eq!(rig.hosts[next].progress().node, target);
    rig.stop().await;
}

#[tokio::test]
async fn recovered_control_events_are_forwarded_once_and_keep_frames_charged_after_owner_stop() {
    use focal_consensus::PbMessageExt;
    let data = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let memory = budget();
    let options = ControlOptions::new(NodeConfig::joining(
        1,
        CLUSTER,
        GROUP,
        vec![1, 2, 3],
        vec![],
    ));
    let mut replica = ControlReplica::open(
        options,
        root_bootstrap(&authority),
        memory.clone(),
        data.path().join("log"),
    )
    .unwrap();
    replica.drain(&RejectUnverifiedEvidence).unwrap();
    replica.campaign().unwrap();
    let initial = replica.drain(&RejectUnverifiedEvidence).unwrap();
    // Use real raft-rs campaign output, already drained by a trusted startup
    // owner. It must survive handoff even though RawNode no longer owns Ready.
    let mut expected: Vec<_> = initial
        .messages
        .iter()
        .map(|message| (message.to, message.write_to_bytes().unwrap()))
        .collect();
    expected.sort();
    assert!(!expected.is_empty());
    let mut config = ControlHostConfig::new(namespace());
    config.tick = Duration::from_secs(1);
    let (host, owner, mut outgoing) = ControlHost::spawn_recovered(
        replica,
        RejectUnverifiedEvidence,
        config,
        memory.clone(),
        initial,
    )
    .unwrap();
    let mut frames = Vec::new();
    let mut actual = Vec::new();
    for _ in 0..expected.len() {
        let frame = tokio::time::timeout(Duration::from_secs(3), outgoing.recv())
            .await
            .unwrap()
            .unwrap();
        let Operation::Raft { group, message } = &frame.request.operation else {
            panic!("replication frame");
        };
        assert_eq!(*group, GROUP);
        actual.push((frame.target, message.clone()));
        frames.push(frame);
    }
    actual.sort();
    assert_eq!(actual, expected);
    // An observation forces a later drain; it must not replay the initial batch.
    drop(host.observe_root().await.unwrap());
    assert!(matches!(
        outgoing.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(outgoing);
    assert!(memory.stats().by_kind[focal_memory::BudgetKind::Control as usize] > 0);
    drop(frames);
    assert_eq!(memory.stats().used, 0);
}
