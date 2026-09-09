#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::{FaultPoint, MessageType, NodeConfig, SharedWal, SnapshotStatus, StateRole};
use focal_control::*;
use focal_directory::*;
use focal_enrollment::{
    BootstrapAuthority, EnrollmentLimits, EnrollmentRegistry, EnrollmentRole, InviteOptions,
};
use focal_log::{WalIdentity, WalOptions};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::ContentHash;

const CLUSTER: [u8; 16] = [1; 16];
const ROOT: [u8; 16] = [2; 16];
const NOW: i64 = 1_800_000_000;
struct Evidence;
impl AuthorityVerifier for Evidence {
    fn verify_enrollment(&self, value: &NodeEnrollment) -> Result<(), DirectoryError> {
        if value.attestation == ContentHash([9; 32]) {
            Ok(())
        } else {
            Err(DirectoryError::UnverifiedAuthority)
        }
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
fn budget() -> MemoryBudget {
    MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap()
}
fn authority(dir: &tempfile::TempDir) -> BootstrapAuthority {
    BootstrapAuthority::open_or_create(
        dir.path().join("keys"),
        CLUSTER,
        vec!["localhost".into()],
        NOW,
    )
    .unwrap()
}
fn root_bootstrap(authority: &BootstrapAuthority) -> ControlBootstrap {
    let root = RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget()).unwrap();
    let registry = EnrollmentRegistry::new(
        CLUSTER,
        authority.ca_certificate().to_vec(),
        2,
        EnrollmentLimits::default(),
    )
    .unwrap();
    ControlBootstrap::root(&root, &registry).unwrap()
}
fn options(id: u64) -> ControlOptions {
    ControlOptions::new(NodeConfig::single(id, CLUSTER, ROOT))
}
fn request(
    client: u8,
    sequence: u64,
    acknowledged_through: u64,
    command: ControlCommand,
) -> ControlRequest {
    ControlRequest {
        id: ControlRequestId {
            client: [client; 16],
            sequence,
        },
        acknowledged_through,
        command,
    }
}
fn region(expected_revision: u64, id: u128) -> ControlCommand {
    ControlCommand::Root(RootCommand {
        expected_revision,
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
fn leader(replica: &mut ControlReplica) {
    replica.drain(&Evidence).unwrap();
    replica.campaign().unwrap();
    replica.drain(&Evidence).unwrap();
    assert_eq!(replica.status().role, StateRole::Leader);
}
fn commit(replica: &mut ControlReplica, request: ControlRequest) -> ControlReceipt {
    let id = request.id;
    assert_eq!(
        replica.submit(request, &Evidence).unwrap(),
        ControlSubmission::Pending(id)
    );
    let receipt = replica.drain(&Evidence).unwrap().completed.unwrap();
    assert_eq!(receipt.request, id);
    receipt
}

#[test]
fn single_voter_uses_reserved_persistence_under_ordinary_pressure_and_recovers_exact_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let keys = tempfile::tempdir().unwrap();
    let authority = authority(&keys);
    let bootstrap = root_bootstrap(&authority);
    let allowance = budget();
    let mut replica =
        ControlReplica::open(options(1), bootstrap.clone(), allowance.clone(), dir.path()).unwrap();
    leader(&mut replica);
    let first = request(1, 1, 0, region(0, 1));
    let baseline = allowance.stats();
    replica.submit(first.clone(), &Evidence).unwrap();
    assert_eq!(replica.root().unwrap().revision(), 0);
    assert!(replica.receipt(first.id).unwrap().is_none());
    assert_eq!(
        replica.submit(first.clone(), &Evidence).unwrap(),
        ControlSubmission::Pending(first.id)
    );
    let pressure = allowance
        .reserve(
            BudgetKind::Control,
            BudgetLane::Ordinary,
            allowance.stats().limit
                - allowance.stats().completion_reserve
                - allowance.stats().ordinary_used,
        )
        .unwrap()
        .commit();
    let receipt = replica.drain(&Evidence).unwrap().completed.unwrap();
    assert_eq!(receipt.revisions.root, 1);
    assert_eq!(
        replica.submit(first.clone(), &Evidence).unwrap(),
        ControlSubmission::Existing(receipt)
    );
    drop(pressure);
    assert!(allowance.stats().used > baseline.used);
    let baseline = allowance.stats();
    assert!(matches!(
        replica.submit(request(1, 2, 0, region(0, 2)), &Evidence),
        Err(ControlError::Directory(DirectoryError::CompareFailed))
    ));
    assert_eq!(
        allowance.stats(),
        baseline,
        "failed prepare releases retry and machine candidates"
    );
    assert!(!replica.has_pending());
    let second = request(1, 2, 0, region(1, 2));
    let second_receipt = commit(&mut replica, second.clone());
    replica.checkpoint().unwrap();
    let third = request(1, 3, 0, region(2, 3));
    let third_receipt = commit(&mut replica, third.clone());
    drop(replica);
    assert_eq!(allowance.stats().used, 0);
    let mut replica = ControlReplica::open(options(1), bootstrap, budget(), dir.path()).unwrap();
    assert!(matches!(
        replica.receipt(first.id),
        Err(ControlError::NotReady)
    ));
    replica.drain(&Evidence).unwrap();
    assert_eq!(replica.root().unwrap().revision(), 3);
    for (request, receipt) in [
        (first, receipt),
        (second, second_receipt),
        (third, third_receipt),
    ] {
        assert_eq!(
            replica.submit(request, &Evidence).unwrap(),
            ControlSubmission::Existing(receipt)
        );
    }
    leader(&mut replica);
    replica.read_index(b"directory-read".to_vec()).unwrap();
    let events = replica.drain(&Evidence).unwrap();
    assert_eq!(events.read_states.len(), 1);
    assert!(events.read_states[0].index <= events.applied_index);
}

#[test]
fn durable_retry_floors_bound_outcomes_and_never_allow_forgotten_ids_to_execute() {
    let dir = tempfile::tempdir().unwrap();
    let keys = tempfile::tempdir().unwrap();
    let authority = authority(&keys);
    let bootstrap = root_bootstrap(&authority);
    let mut cfg = options(1);
    cfg.limits.max_receipts_per_client = 2;
    cfg.limits.max_clients = 1;
    let mut replica =
        ControlReplica::open(cfg.clone(), bootstrap.clone(), budget(), dir.path()).unwrap();
    leader(&mut replica);
    let first = request(1, 1, 0, region(0, 1));
    commit(&mut replica, first.clone());
    commit(&mut replica, request(1, 2, 0, region(1, 2)));
    assert!(matches!(
        replica.submit(request(1, 3, 0, region(2, 3)), &Evidence),
        Err(ControlError::Capacity)
    ));
    assert!(matches!(
        replica.submit(request(1, 4, 0, region(2, 3)), &Evidence),
        Err(ControlError::RetryOrder)
    ));
    assert!(matches!(
        replica.submit(request(1, 2, 0, region(1, 20)), &Evidence),
        Err(ControlError::RetryConflict)
    ));
    assert!(matches!(
        replica.submit(request(2, 1, 0, region(2, 3)), &Evidence),
        Err(ControlError::Capacity)
    ));
    let third = request(1, 3, 2, region(2, 3));
    let receipt = commit(&mut replica, third.clone());
    assert!(matches!(
        replica.submit(first.clone(), &Evidence),
        Err(ControlError::RetryExpired)
    ));
    replica.checkpoint().unwrap();
    drop(replica);
    let mut replica = ControlReplica::open(cfg, bootstrap, budget(), dir.path()).unwrap();
    replica.drain(&Evidence).unwrap();
    assert!(matches!(
        replica.submit(first, &Evidence),
        Err(ControlError::RetryExpired)
    ));
    assert_eq!(
        replica.submit(third, &Evidence).unwrap(),
        ControlSubmission::Existing(receipt)
    );
    assert_eq!(replica.root().unwrap().revision(), 3);
}

#[test]
fn enrollment_drafts_release_only_after_shared_root_group_commit_and_survive_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let keys = tempfile::tempdir().unwrap();
    let authority = authority(&keys);
    let bootstrap = root_bootstrap(&authority);
    let allowance = budget();
    let mut replica =
        ControlReplica::open(options(1), bootstrap.clone(), allowance.clone(), dir.path()).unwrap();
    leader(&mut replica);
    commit(&mut replica, request(1, 1, 0, region(0, 1)));
    let draft = replica
        .enrollment()
        .unwrap()
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: NOW + 600,
            },
            NOW,
        )
        .unwrap();
    let request = request(1, 2, 0, ControlCommand::Enrollment(draft.command().clone()));
    replica.submit(request.clone(), &Evidence).unwrap();
    assert_eq!(replica.enrollment().unwrap().revision(), 0);
    let pressure = allowance
        .reserve(
            BudgetKind::Control,
            BudgetLane::Ordinary,
            allowance.stats().limit
                - allowance.stats().completion_reserve
                - allowance.stats().ordinary_used,
        )
        .unwrap()
        .commit();
    let receipt = replica.drain(&Evidence).unwrap().completed.unwrap();
    drop(pressure);
    assert_eq!(
        receipt.revisions,
        ControlRevisions {
            root: 1,
            enrollment: 1,
            partition: 0
        }
    );
    assert_eq!(
        replica.enrollment().unwrap().applied_index(),
        receipt.committed_index
    );
    let invitation = draft.release(replica.enrollment().unwrap()).unwrap();
    let id = invitation.id();
    replica.checkpoint().unwrap();
    drop(replica);
    let mut replica = ControlReplica::open(options(1), bootstrap, budget(), dir.path()).unwrap();
    replica.drain(&Evidence).unwrap();
    assert_eq!(
        replica.submit(request, &Evidence).unwrap(),
        ControlSubmission::Existing(receipt)
    );
    leader(&mut replica);
    let command = replica
        .enrollment()
        .unwrap()
        .prepare_revoke(id, NOW + 1)
        .unwrap();
    commit(
        &mut replica,
        self::request(1, 3, 0, ControlCommand::Enrollment(command)),
    );
    assert_eq!(replica.enrollment().unwrap().revision(), 2);
    assert_eq!(replica.root().unwrap().revision(), 1);
    assert!(
        replica.status().learners.is_empty(),
        "enrollment never promotes Raft membership"
    );
}

#[test]
fn io_failure_has_no_completion_and_fail_stops_until_disk_recovery() {
    for fault in [
        FaultPoint::AfterAppend,
        FaultPoint::AfterDataSync,
        FaultPoint::AfterFenceInstall,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let keys = tempfile::tempdir().unwrap();
        let authority = authority(&keys);
        let bootstrap = root_bootstrap(&authority);
        let mut replica =
            ControlReplica::open(options(1), bootstrap.clone(), budget(), dir.path()).unwrap();
        leader(&mut replica);
        let request = request(1, 1, 0, region(0, 1));
        replica.submit(request.clone(), &Evidence).unwrap();
        replica.inject_fault_once(fault);
        assert!(replica.drain(&Evidence).is_err());
        assert_eq!(replica.root().unwrap().revision(), 0);
        assert!(matches!(
            replica.receipt(request.id),
            Err(ControlError::Failed)
        ));
        assert!(matches!(
            replica.submit(request.clone(), &Evidence),
            Err(ControlError::Failed)
        ));
        drop(replica);
        let mut replica =
            ControlReplica::open(options(1), bootstrap, budget(), dir.path()).unwrap();
        replica.drain(&Evidence).unwrap();
        // A partially durable proposal is not a committed outcome. Elections
        // may later commit it; no reply is fabricated from its presence on disk.
        assert!(replica.receipt(request.id).unwrap().is_none());
    }
}

#[test]
fn genesis_and_group_scope_fence_recovery_and_cross_partition_commands() {
    let dir = tempfile::tempdir().unwrap();
    let keys = tempfile::tempdir().unwrap();
    let authority = authority(&keys);
    let bootstrap = root_bootstrap(&authority);
    let mut replica =
        ControlReplica::open(options(1), bootstrap.clone(), budget(), dir.path()).unwrap();
    leader(&mut replica);
    commit(&mut replica, request(1, 1, 0, region(0, 1)));
    drop(replica);
    let mut wrong = bootstrap;
    let ControlBootstrap::Root { directory, .. } = &mut wrong else {
        unreachable!()
    };
    directory.revision = 7;
    let mut replica = ControlReplica::open(options(1), wrong, budget(), dir.path()).unwrap();
    assert!(matches!(
        replica.drain(&Evidence),
        Err(ControlError::WrongOwner)
    ));
    assert!(matches!(
        replica.drain(&Evidence),
        Err(ControlError::Failed)
    ));
}

fn partition_bootstrap(id: u128) -> ControlBootstrap {
    let directory = DirectoryPartition::new(
        ClusterId(CLUSTER),
        Delegation {
            namespace: NamespaceRange::all(),
            partition: PartitionId::from_u128(id),
            region: RegionId::from_u128(1),
            log_group: LogGroupId::from_u128(id + 100),
            epoch: 1,
            activation: None,
        },
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    ControlBootstrap::partition(&directory)
}
fn partition_options(id: u128) -> ControlOptions {
    ControlOptions::new(NodeConfig::single(
        1,
        CLUSTER,
        LogGroupId::from_u128(id + 100).0,
    ))
}
fn enroll(node: u64) -> ControlCommand {
    ControlCommand::Partition(PartitionCommand {
        expected_revision: 0,
        delegation_epoch: 1,
        operation: PartitionOperation::Enroll {
            node: NodeEnrollment {
                node,
                generation: 1,
                region: RegionId::from_u128(1),
                zone: ZoneId::from_u128(1),
                endpoint: format!("node-{node}:443"),
                identity: ContentHash([1; 32]),
                authority_epoch: 1,
                attestation: ContentHash([9; 32]),
                eligible: true,
            },
            expected_generation: None,
        },
    })
}
#[test]
fn independent_partition_groups_share_physical_wal_without_sharing_control_authority() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SharedWal::open(
        dir.path(),
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 0,
        }),
    )
    .unwrap();
    let first_bootstrap = partition_bootstrap(1);
    let second_bootstrap = partition_bootstrap(2);
    let mut first = ControlReplica::open_on_wal(
        partition_options(1),
        first_bootstrap.clone(),
        budget(),
        wal.clone(),
    )
    .unwrap();
    let mut second = ControlReplica::open_on_wal(
        partition_options(2),
        second_bootstrap.clone(),
        budget(),
        wal.clone(),
    )
    .unwrap();
    leader(&mut first);
    leader(&mut second);
    assert!(matches!(
        first.submit(request(1, 1, 0, region(0, 1)), &Evidence),
        Err(ControlError::WrongOwner)
    ));
    let first_receipt = commit(&mut first, request(1, 1, 0, enroll(1)));
    assert_eq!(second.partition().unwrap().revision(), 0);
    let second_receipt = commit(&mut second, request(1, 1, 0, enroll(2)));
    first.checkpoint().unwrap();
    drop(first);
    drop(second);
    drop(wal);
    let wal = SharedWal::open(
        dir.path(),
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 0,
        }),
    )
    .unwrap();
    let mut first =
        ControlReplica::open_on_wal(partition_options(1), first_bootstrap, budget(), wal.clone())
            .unwrap();
    let mut second =
        ControlReplica::open_on_wal(partition_options(2), second_bootstrap, budget(), wal).unwrap();
    first.drain(&Evidence).unwrap();
    second.drain(&Evidence).unwrap();
    assert_eq!(
        first.receipt(first_receipt.request).unwrap(),
        Some(first_receipt)
    );
    assert_eq!(
        second.receipt(second_receipt.request).unwrap(),
        Some(second_receipt)
    );
    assert!(
        first
            .partition()
            .unwrap()
            .checkpoint()
            .nodes
            .contains_key(&1)
    );
    assert!(
        !first
            .partition()
            .unwrap()
            .checkpoint()
            .nodes
            .contains_key(&2)
    );
    assert!(
        second
            .partition()
            .unwrap()
            .checkpoint()
            .nodes
            .contains_key(&2)
    );
}

struct Cluster {
    _keys: tempfile::TempDir,
    dirs: Vec<tempfile::TempDir>,
    bootstrap: ControlBootstrap,
    nodes: Vec<ControlReplica>,
    completions: Vec<ControlReceipt>,
    uncertain: Vec<ControlRequestId>,
}
impl Cluster {
    fn new() -> Self {
        let keys = tempfile::tempdir().unwrap();
        let authority = authority(&keys);
        let bootstrap = root_bootstrap(&authority);
        drop(authority);
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let nodes = dirs
            .iter()
            .enumerate()
            .map(|(i, dir)| {
                let mut options = options(i as u64 + 1);
                options.consensus.voters = vec![1, 2, 3];
                ControlReplica::open(options, bootstrap.clone(), budget(), dir.path()).unwrap()
            })
            .collect();
        Self {
            _keys: keys,
            dirs,
            bootstrap,
            nodes,
            completions: vec![],
            uncertain: vec![],
        }
    }
    fn pump(&mut self, isolated: Option<u64>) {
        for _ in 0..100 {
            let mut messages = Vec::new();
            for node in &mut self.nodes {
                let events = node.drain(&Evidence).unwrap();
                messages.extend(events.messages);
                self.completions.extend(events.completed);
                self.uncertain.extend(events.uncertain);
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if isolated == Some(message.from) || isolated == Some(message.to) {
                    continue;
                }
                let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                let from = message.from;
                let to = message.to;
                self.nodes[(to - 1) as usize].step(message).unwrap();
                if snapshot {
                    self.nodes[(from - 1) as usize]
                        .report_snapshot(to, SnapshotStatus::Finish)
                        .unwrap();
                }
            }
        }
        panic!("bounded message pump did not quiesce");
    }
}

#[test]
fn three_voters_do_not_publish_minority_proposal_and_majority_leader_fences_old_preparation() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    assert_eq!(cluster.nodes[0].status().role, StateRole::Leader);
    let minority = request(1, 1, 0, region(0, 1));
    cluster.nodes[0]
        .submit(minority.clone(), &Evidence)
        .unwrap();
    cluster.pump(Some(1));
    assert!(cluster.completions.is_empty());
    assert!(
        cluster
            .nodes
            .iter()
            .all(|node| node.root().unwrap().revision() == 0)
    );
    for _ in 0..80 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(Some(1));
        if cluster.nodes[1..]
            .iter()
            .any(|node| node.status().role == StateRole::Leader)
        {
            break;
        }
    }
    let owner = (1..3)
        .find(|i| cluster.nodes[*i].status().role == StateRole::Leader)
        .unwrap();
    let majority = request(2, 1, 0, region(0, 2));
    cluster.nodes[owner]
        .submit(majority.clone(), &Evidence)
        .unwrap();
    cluster.pump(Some(1));
    assert_eq!(cluster.completions.len(), 1);
    assert_eq!(cluster.completions[0].request, majority.id);
    cluster.pump(None);
    for _ in 0..5 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(None);
    }
    assert!(cluster.uncertain.contains(&minority.id));
    for node in &cluster.nodes {
        assert_eq!(node.root().unwrap().revision(), 1);
        assert!(
            node.root()
                .unwrap()
                .checkpoint()
                .regions
                .contains_key(&RegionId::from_u128(2))
        );
        assert!(node.receipt(minority.id).unwrap().is_none());
    }
    let completed = cluster.completions[0];
    assert_eq!(
        cluster.nodes[0].submit(majority, &Evidence).unwrap(),
        ControlSubmission::Existing(completed)
    );
    let bootstrap = cluster.bootstrap.clone();
    let dirs = std::mem::take(&mut cluster.dirs);
    drop(cluster.nodes);
    for (i, dir) in dirs.iter().enumerate() {
        let mut options = options(i as u64 + 1);
        options.consensus.voters = vec![1, 2, 3];
        let mut node =
            ControlReplica::open(options, bootstrap.clone(), budget(), dir.path()).unwrap();
        node.drain(&Evidence).unwrap();
        assert_eq!(node.receipt(completed.request).unwrap(), Some(completed));
    }
}

#[test]
fn lagging_metadata_replica_installs_committed_checkpoint_and_retry_windows() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    let mut last = None;
    for id in 1..=6 {
        cluster.nodes[0]
            .submit(request(1, id, id - 1, region(id - 1, id.into())), &Evidence)
            .unwrap();
        cluster.pump(Some(3));
        last = cluster.completions.last().copied();
    }
    assert_eq!(cluster.nodes[2].root().unwrap().revision(), 0);
    cluster.nodes[0].checkpoint().unwrap();
    for _ in 0..10 {
        cluster.nodes[0].tick().unwrap();
        cluster.pump(None);
    }
    let last = last.unwrap();
    assert_eq!(cluster.nodes[2].root().unwrap().revision(), 6);
    assert_eq!(cluster.nodes[2].receipt(last.request).unwrap(), Some(last));
    assert!(matches!(
        cluster.nodes[2].receipt(ControlRequestId {
            client: [1; 16],
            sequence: 1
        }),
        Err(ControlError::RetryExpired)
    ));
}

#[test]
fn total_budget_exhaustion_never_publishes_and_reopen_allows_exact_retry() {
    let dir = tempfile::tempdir().unwrap();
    let keys = tempfile::tempdir().unwrap();
    let authority = authority(&keys);
    let bootstrap = root_bootstrap(&authority);
    let allowance = budget();
    let mut replica =
        ControlReplica::open(options(1), bootstrap.clone(), allowance.clone(), dir.path()).unwrap();
    leader(&mut replica);
    let pending = request(41, 1, 0, region(0, 1));
    replica.submit(pending.clone(), &Evidence).unwrap();
    let pressure = allowance
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            allowance.stats().limit - allowance.stats().used,
        )
        .unwrap()
        .commit();
    assert!(matches!(
        replica.drain(&Evidence),
        Err(ControlError::Consensus(
            focal_consensus::ConsensusError::Capacity
        ))
    ));
    assert_eq!(replica.root().unwrap().revision(), 0);
    assert!(matches!(
        replica.drain(&Evidence),
        Err(ControlError::Failed)
    ));
    drop(pressure);
    drop(replica);
    let mut replica = ControlReplica::open(options(1), bootstrap, allowance, dir.path()).unwrap();
    leader(&mut replica);
    assert!(replica.receipt(pending.id).unwrap().is_none());
    let receipt = commit(&mut replica, pending.clone());
    assert_eq!(receipt.revisions.root, 1);
    assert_eq!(
        replica.submit(pending, &Evidence).unwrap(),
        ControlSubmission::Existing(receipt)
    );
}
