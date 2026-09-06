#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::{MessageType, NodeConfig, PbMessageExt, SnapshotStatus, StateRole};
use focal_control::*;
use focal_directory::*;
use focal_enrollment::{BootstrapAuthority, EnrollmentLimits, EnrollmentRegistry};
use focal_memory::MemoryBudget;

const CLUSTER: [u8; 16] = [101; 16];
const GROUP: [u8; 16] = [102; 16];
struct NoEvidence;
impl AuthorityVerifier for NoEvidence {
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
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
struct Cluster {
    nodes: Vec<ControlReplica>,
    dirs: tempfile::TempDir,
    bootstrap: ControlBootstrap,
}
impl Cluster {
    fn options(node: u64) -> ControlOptions {
        ControlOptions::new(NodeConfig::joining(
            node,
            CLUSTER,
            GROUP,
            vec![1, 2, 3],
            vec![],
        ))
    }
    fn new() -> Self {
        let dirs = tempfile::tempdir().unwrap();
        let ca = BootstrapAuthority::open_or_create(
            dirs.path().join("ca"),
            CLUSTER,
            vec!["localhost".into()],
            1_800_000_000,
        )
        .unwrap();
        let registry = EnrollmentRegistry::new(
            CLUSTER,
            ca.ca_certificate().to_vec(),
            4,
            EnrollmentLimits::default(),
        )
        .unwrap();
        let directory =
            RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget()).unwrap();
        let bootstrap = ControlBootstrap::root(&directory, &registry).unwrap();
        let nodes = (1..=4)
            .map(|id| {
                ControlReplica::open(
                    Self::options(id),
                    bootstrap.clone(),
                    budget(),
                    dirs.path().join(id.to_string()),
                )
                .unwrap()
            })
            .collect();
        let mut value = Self {
            nodes,
            dirs,
            bootstrap,
        };
        value.pump(None);
        assert!(matches!(
            value.nodes[3].campaign(),
            Err(ControlError::Consensus(
                focal_consensus::ConsensusError::Configuration(_)
            ))
        ));
        value.pump(None);
        assert_eq!(value.nodes[3].status().role, StateRole::Follower);
        value.nodes[0].campaign().unwrap();
        value.pump(None);
        value
    }
    fn pump(&mut self, excluded: Option<u64>) {
        for _ in 0..200 {
            let messages = self
                .nodes
                .iter_mut()
                .flat_map(|node| node.drain(&NoEvidence).unwrap().messages)
                .collect::<Vec<_>>();
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if excluded == Some(message.from) || excluded == Some(message.to) {
                    continue;
                }
                let from = message.from;
                let to = message.to;
                // Production ControlHost drops packets from removed members.
                if !self.nodes[(to - 1) as usize].accepts_peer(from) {
                    continue;
                }
                let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                self.nodes[(to - 1) as usize]
                    .step_authenticated(from, &message.write_to_bytes().unwrap())
                    .unwrap();
                if snapshot {
                    self.nodes[(from - 1) as usize]
                        .report_snapshot(to, SnapshotStatus::Finish)
                        .unwrap();
                }
            }
        }
        panic!("message pump did not quiesce")
    }
    fn ticks(&mut self, excluded: Option<u64>, count: usize) {
        for _ in 0..count {
            for node in &mut self.nodes {
                node.tick().unwrap();
            }
            self.pump(excluded);
        }
    }
    fn request(&self, owner: usize, sequence: u64, change: MembershipChange) -> ControlRequest {
        let current = self.nodes[owner].configuration();
        ControlRequest {
            id: ControlRequestId {
                client: [103; 16],
                sequence,
            },
            acknowledged_through: 0,
            command: ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: current.configuration_index,
                expected: current.configuration,
                change,
            }),
        }
    }
    fn restart(&mut self) {
        for node in &mut self.nodes {
            node.checkpoint().unwrap();
        }
        self.nodes.clear();
        self.nodes = (1..=4)
            .map(|id| {
                ControlReplica::open(
                    Self::options(id),
                    self.bootstrap.clone(),
                    budget(),
                    self.dirs.path().join(id.to_string()),
                )
                .unwrap()
            })
            .collect();
        self.pump(None);
    }
}
#[test]
fn learner_admission_catchup_promotion_and_exact_receipt_survive_checkpoint_and_failover() {
    let mut cluster = Cluster::new();
    let original = cluster.nodes[0].identity();
    let add = cluster.request(0, 1, MembershipChange::AddLearner { node: 4 });
    assert!(matches!(
        cluster.nodes[0].submit(add.clone(), &NoEvidence).unwrap(),
        ControlSubmission::Pending(_)
    ));
    let local = cluster.nodes[0].drain(&NoEvidence).unwrap();
    assert!(local.completed.is_none());
    assert!(
        cluster.nodes[0]
            .configuration()
            .configuration
            .learners
            .is_empty()
    );
    // Deliver the persisted proposal's real messages to the original quorum.
    for message in local.messages {
        let from = message.from;
        let to = message.to;
        if to != 4 {
            cluster.nodes[(to - 1) as usize]
                .step_authenticated(from, &message.write_to_bytes().unwrap())
                .unwrap();
        }
    }
    cluster.pump(Some(4));
    let receipt = cluster.nodes[0].receipt(add.id).unwrap().unwrap();
    assert_eq!(
        cluster.nodes[0].configuration().configuration_index,
        receipt.committed_index
    );
    assert_eq!(
        cluster.nodes[0].configuration().configuration.learners,
        vec![4]
    );
    assert!(
        cluster.nodes[3]
            .configuration()
            .configuration
            .learners
            .is_empty()
    );
    let promote = cluster.request(0, 2, MembershipChange::Promote { node: 4 });
    assert!(matches!(
        cluster.nodes[0].submit(promote.clone(), &NoEvidence),
        Err(ControlError::Consensus(
            focal_consensus::ConsensusError::LearnerBehind
        ))
    ));
    // Force actual snapshot catch-up; the snapshot includes the membership fence
    // and exact admission receipt while preserving the original public genesis.
    cluster.nodes[0].checkpoint().unwrap();
    cluster.ticks(None, 4);
    assert_eq!(cluster.nodes[3].receipt(add.id).unwrap(), Some(receipt));
    assert_eq!(
        cluster.nodes[3].configuration().configuration_index,
        receipt.committed_index
    );
    cluster.nodes[0]
        .submit(promote.clone(), &NoEvidence)
        .unwrap();
    cluster.pump(None);
    let promoted = cluster.nodes[0].receipt(promote.id).unwrap().unwrap();
    assert_eq!(
        cluster.nodes[0].configuration().configuration.voters,
        vec![1, 2, 3, 4]
    );
    assert!(
        cluster.nodes[0]
            .configuration()
            .configuration
            .learners
            .is_empty()
    );
    // A retained exact retry bypasses changed preconditions but cannot alter intent.
    assert_eq!(
        cluster.nodes[2]
            .submit(promote.clone(), &NoEvidence)
            .unwrap(),
        ControlSubmission::Existing(promoted)
    );
    let mut conflict = promote.clone();
    let ControlCommand::Membership(change) = &mut conflict.command else {
        unreachable!()
    };
    change.change = MembershipChange::Remove { node: 4 };
    assert!(matches!(
        cluster.nodes[0].submit(conflict, &NoEvidence),
        Err(ControlError::RetryConflict)
    ));
    cluster.restart();
    assert_eq!(cluster.nodes[0].identity(), original);
    assert_eq!(
        cluster.nodes[3].receipt(promote.id).unwrap(),
        Some(promoted)
    );
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    let removed = cluster.request(0, 3, MembershipChange::Remove { node: 4 });
    cluster.nodes[0]
        .submit(removed.clone(), &NoEvidence)
        .unwrap();
    cluster.pump(Some(1));
    assert!(cluster.nodes[0].receipt(removed.id).unwrap().is_none());
    let mut leader = None;
    for _ in 0..100 {
        cluster.ticks(Some(1), 1);
        leader = (1..4).find(|index| cluster.nodes[*index].status().role == StateRole::Leader);
        if leader.is_some() {
            break;
        }
    }
    let leader = leader.expect("surviving three-voter quorum elects a leader");
    cluster.nodes[leader]
        .submit(removed.clone(), &NoEvidence)
        .unwrap();
    cluster.pump(Some(1));
    let removal = cluster.nodes[leader].receipt(removed.id).unwrap().unwrap();
    cluster.ticks(None, 4);
    assert_eq!(cluster.nodes[0].receipt(removed.id).unwrap(), Some(removal));
    assert_eq!(
        cluster.nodes[0].configuration().configuration.voters,
        vec![1, 2, 3]
    );
    let mut stale = cluster.request(leader, 4, MembershipChange::AddLearner { node: 4 });
    let ControlCommand::Membership(change) = &mut stale.command else {
        unreachable!()
    };
    change.expected_configuration_index = receipt.committed_index;
    assert!(matches!(
        cluster.nodes[leader].submit(stale, &NoEvidence),
        Err(ControlError::Directory(DirectoryError::CompareFailed))
    ));
}
