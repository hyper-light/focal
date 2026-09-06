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
    rig.hosts[stale]
        .submit(peer(PeerRole::Runtime), request(1, region(0, 1)))
        .await
        .unwrap();
    let excluded = rig.hosts[stale].progress().node;
    rig.isolated.store(excluded, Ordering::SeqCst);
    let majority = rig.leader(excluded).await;
    let committed = rig.hosts[majority]
        .submit(peer(PeerRole::Runtime), request(2, region(1, 2)))
        .await
        .unwrap();
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
    let added = rig.hosts[leader]
        .submit(peer(PeerRole::Runtime), add.clone())
        .await
        .unwrap();
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
