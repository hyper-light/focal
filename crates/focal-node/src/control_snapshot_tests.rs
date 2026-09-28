use super::*;
use crate::cluster::NoDirectoryAuthority;
use focal_consensus::{Message, MessageType, NodeConfig};
use focal_directory::{
    ClusterId, RegionId, RegionRecord, RootCommand, RootConfig, RootDirectory, RootOperation,
};
use focal_enrollment::{BootstrapAuthority, EnrollmentLimits, EnrollmentRegistry};
use focal_model::{SessionId, TenantId};

const CLUSTER: [u8; 16] = [211; 16];
const GROUP: [u8; 16] = [212; 16];

struct Fixture {
    owner: Owner<NoDirectoryAuthority>,
    outgoing: async_mpsc::Receiver<ControlReplicationFrame>,
    follower: ControlReplica,
    budget: MemoryBudget,
    _directory: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(256 * 1024 * 1024, 96 * 1024 * 1024).unwrap();
        let ca = BootstrapAuthority::open_or_create(
            directory.path().join("ca"),
            CLUSTER,
            vec!["localhost".into()],
            1_800_000_000,
        )
        .unwrap();
        let root =
            RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget.clone()).unwrap();
        let enrollment = EnrollmentRegistry::new(
            CLUSTER,
            ca.ca_certificate().to_vec(),
            3,
            EnrollmentLimits::default(),
        )
        .unwrap();
        let bootstrap = ControlBootstrap::root(&root, &enrollment).unwrap();
        let options =
            |node| ControlOptions::new(NodeConfig::joining(node, CLUSTER, GROUP, vec![1], vec![2]));
        let mut replica = ControlReplica::open(
            options(1),
            bootstrap.clone(),
            budget.clone(),
            directory.path().join("leader"),
        )
        .unwrap();
        replica.drain(&NoDirectoryAuthority).unwrap();
        replica.campaign().unwrap();
        replica.drain(&NoDirectoryAuthority).unwrap();
        replica
            .submit(
                ControlRequest {
                    id: ControlRequestId {
                        client: [213; 16],
                        sequence: 1,
                    },
                    acknowledged_through: 0,
                    command: ControlCommand::Root(RootCommand {
                        expected_revision: 0,
                        operation: RootOperation::RegisterRegion {
                            region: RegionRecord {
                                id: RegionId::from_u128(1),
                                label: "test-region".into(),
                                authority_epoch: 1,
                            },
                            expected_epoch: None,
                        },
                    }),
                },
                &NoDirectoryAuthority,
            )
            .unwrap();
        replica.drain(&NoDirectoryAuthority).unwrap();
        replica.checkpoint().unwrap();
        let follower_budget = MemoryBudget::new(256 * 1024 * 1024, 96 * 1024 * 1024).unwrap();
        let mut follower = ControlReplica::open(
            options(2),
            bootstrap,
            follower_budget,
            directory.path().join("follower"),
        )
        .unwrap();
        follower.drain(&NoDirectoryAuthority).unwrap();
        let (outbound, outgoing) = async_mpsc::channel(32);
        let status = replica.status();
        let (progress, _watch) = watch::channel(ControlProgressState {
            value: ControlProgress {
                identity: replica.identity(),
                node: 1,
                leader: 1,
                term: status.term,
                applied_index: replica.applied_index(),
                revisions: replica.revisions(),
                dropped_replication: 0,
                stopped: false,
                snapshot_index: 0,
                peers: Vec::new(),
                failure: None,
            },
            _allocation: None,
        });
        let owner = Owner {
            replica,
            initial: None,
            verifier: NoDirectoryAuthority,
            config: ControlHostConfig::new(LedgerId {
                tenant: TenantId(CLUSTER),
                session: SessionId(GROUP),
            }),
            limits: ControlHost::wire_limits(),
            budget: budget.clone(),
            pending: VecDeque::new(),
            directory: None,
            authority_refresh: None,
            snapshot_feedback: Default::default(),
            outbound,
            progress,
            nonce: 0,
            dropped: 0,
            failure: None,
            pace: Default::default(),
        };
        drop(root);
        Self {
            owner,
            outgoing,
            follower,
            budget,
            _directory: directory,
        }
    }
    fn deliver(&mut self, mut frame: ControlReplicationFrame) {
        let Operation::Raft { message, .. } = &frame.request.operation else {
            panic!("not Raft")
        };
        self.follower.step_authenticated(1, message).unwrap();
        frame.report_snapshot(true);
        let events = self.follower.drain(&NoDirectoryAuthority).unwrap();
        for message in events.messages {
            self.owner.replica.step(message).unwrap();
        }
    }
    fn snapshot(&mut self) -> ControlReplicationFrame {
        for _ in 0..32 {
            self.owner.replica.tick().unwrap();
            self.owner.drain().unwrap();
            while let Ok(frame) = self.outgoing.try_recv() {
                let Operation::Raft { message, .. } = &frame.request.operation else {
                    panic!("not Raft")
                };
                if focal_consensus::decode_message(message).unwrap().msg_type
                    == MessageType::MsgSnapshot as i32
                {
                    return frame;
                }
                self.deliver(frame);
                self.owner.drain().unwrap();
            }
        }
        panic!("snapshot did not retry in the same leader term")
    }
    fn finish(&mut self, frame: ControlReplicationFrame) {
        self.deliver(frame);
        for _ in 0..8 {
            self.owner.drain().unwrap();
            while let Ok(frame) = self.outgoing.try_recv() {
                self.deliver(frame);
            }
        }
        assert_eq!(self.follower.revisions(), self.owner.replica.revisions());
        assert_eq!(
            self.follower.applied_index(),
            self.owner.replica.applied_index()
        );
    }
}

#[test]
fn control_snapshot_cancellation_and_remote_rejection_retry_without_leader_change() {
    for rejected in [false, true] {
        let mut fixture = Fixture::new();
        let term = fixture.owner.replica.status().term;
        let mut first = fixture.snapshot();
        if rejected {
            first.report_snapshot(false);
        }
        drop(first);
        fixture.owner.drain().unwrap();
        let retry = fixture.snapshot();
        assert_eq!(fixture.owner.replica.status().term, term);
        fixture.finish(retry);
        let budget = fixture.budget.clone();
        drop(fixture);
        assert_eq!(budget.stats().used, 0);
    }
}

#[test]
fn control_snapshot_local_queue_and_frame_limit_rejection_release_flow_control() {
    for queue in [true, false] {
        let mut fixture = Fixture::new();
        let first = fixture.snapshot();
        drop(first);
        fixture.owner.drain().unwrap();
        let old_limit = fixture.owner.limits.max_frame_bytes;
        if queue {
            let (closed, receive) = async_mpsc::channel(1);
            drop(receive);
            fixture.owner.outbound = closed;
        } else {
            fixture.owner.limits.max_frame_bytes = 1;
        }
        let dropped = fixture.owner.dropped;
        for _ in 0..8 {
            // A heartbeat response makes the lagging learner recently active.
            let mut heartbeat = Message {
                from: 2,
                to: 1,
                term: fixture.owner.replica.status().term,
                ..Default::default()
            };
            heartbeat.set_msg_type(MessageType::MsgHeartbeatResponse);
            fixture.owner.replica.step(heartbeat).unwrap();
            fixture.owner.drain().unwrap();
        }
        assert!(fixture.owner.dropped > dropped);
        let (outbound, outgoing) = async_mpsc::channel(32);
        fixture.owner.outbound = outbound;
        fixture.outgoing = outgoing;
        fixture.owner.limits.max_frame_bytes = old_limit;
        let term = fixture.owner.replica.status().term;
        let retry = fixture.snapshot();
        assert_eq!(fixture.owner.replica.status().term, term);
        fixture.finish(retry);
    }
}

#[test]
fn control_snapshot_old_term_completion_cannot_release_current_flight_and_frame_keeps_charge() {
    let mut fixture = Fixture::new();
    let mut old = fixture.snapshot();
    let mut heartbeat = Message {
        from: 2,
        to: 1,
        term: fixture.owner.replica.status().term + 1,
        ..Default::default()
    };
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    fixture.owner.replica.step(heartbeat).unwrap();
    fixture.owner.drain().unwrap();
    fixture.owner.replica.campaign().unwrap();
    fixture.owner.drain().unwrap();
    let current = fixture.snapshot();
    old.report_snapshot(false);
    drop(old);
    fixture.owner.drain().unwrap();
    for _ in 0..4 {
        fixture.owner.replica.tick().unwrap();
        fixture.owner.drain().unwrap();
        while let Ok(frame) = fixture.outgoing.try_recv() {
            let Operation::Raft { message, .. } = &frame.request.operation else {
                panic!("not Raft")
            };
            assert_ne!(
                focal_consensus::decode_message(message).unwrap().msg_type,
                MessageType::MsgSnapshot as i32
            );
            fixture.deliver(frame);
        }
    }
    let budget = fixture.budget.clone();
    drop(fixture);
    assert!(budget.stats().used > 0);
    drop(current);
    assert_eq!(budget.stats().used, 0);
}

/// A stored snapshot carries the configuration at its index; a member admitted
/// after that snapshot cannot be caught up by it. When a membership change
/// commits above the snapshot floor the owner must refresh the snapshot to the
/// current configuration, otherwise a later-joined learner whose `next_index`
/// reaches the floor wedges (the leader can neither append below the floor nor
/// install a snapshot whose configuration omits the learner). This proves the
/// refresh fires on the stale-configuration trigger and lands at or above the
/// membership change, so the fresh snapshot includes the new member.
#[test]
fn maybe_checkpoint_refreshes_a_snapshot_whose_configuration_predates_a_member() {
    use focal_consensus::MembershipChange;
    let mut fixture = Fixture::new();
    // The fixture checkpointed at bootstrap, so a snapshot floor already exists
    // whose configuration is voters=[1], learners=[2].
    let floor_before = fixture.owner.replica.snapshot_index();
    assert!(floor_before > 0, "the bootstrap snapshot exists");
    // Admit a third node as a learner: the membership change commits above the
    // floor, so the stored snapshot's configuration can no longer contain every
    // member.
    fixture
        .owner
        .replica
        .submit(
            ControlRequest {
                id: ControlRequestId {
                    client: [214; 16],
                    sequence: 1,
                },
                acknowledged_through: 0,
                command: ControlCommand::Membership(ControlMembershipCommand {
                    expected_configuration_index: fixture.owner.replica.configuration_index(),
                    expected: fixture.owner.replica.membership_configuration(),
                    change: MembershipChange::AddLearner { node: 3 },
                }),
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    fixture.owner.replica.drain(&NoDirectoryAuthority).unwrap();
    let configuration_index = fixture.owner.replica.configuration_index();
    assert!(
        configuration_index > fixture.owner.replica.snapshot_index(),
        "the membership change is above the snapshot floor: the snapshot is now stale"
    );
    // The owner refreshes the snapshot to the current committed configuration.
    fixture.owner.maybe_checkpoint().unwrap();
    assert!(
        fixture.owner.replica.snapshot_index() >= configuration_index,
        "maybe_checkpoint refreshed the snapshot to include the membership change \
         (was {floor_before}, now {})",
        fixture.owner.replica.snapshot_index()
    );
}

/// The owner's tick period is never shorter than configured nor longer than
/// its ceiling, whatever is derived for it (27 §3.1 P2).
#[test]
fn the_tick_period_is_clamped_between_the_configured_period_and_its_ceiling() {
    let config = ControlHostConfig::new(LedgerId {
        tenant: focal_model::TenantId([1; 16]),
        session: focal_model::SessionId([2; 16]),
    });
    let pace = TickPeriod::default();
    assert_eq!(
        pace.get(config.tick, config.tick_ceiling),
        config.tick,
        "unset is as configured"
    );
    assert_eq!(pace.election_tick(), 0, "unknown until the owner opens");
    pace.announce(10);
    assert_eq!(pace.election_tick(), 10);
    pace.set(Duration::from_millis(1));
    assert_eq!(pace.get(config.tick, config.tick_ceiling), config.tick);
    pace.set(Duration::from_millis(750));
    assert_eq!(
        pace.get(config.tick, config.tick_ceiling),
        Duration::from_millis(750)
    );
    pace.set(Duration::from_secs(3600));
    assert_eq!(
        pace.get(config.tick, config.tick_ceiling),
        config.tick_ceiling
    );
    // A shared handle sees what the other wrote.
    let shared = pace.clone();
    shared.set(Duration::from_millis(300));
    assert_eq!(
        pace.get(config.tick, config.tick_ceiling),
        Duration::from_millis(300)
    );
    // A ceiling under the period, or past ten seconds, is not a configuration.
    let mut bad = config.clone();
    bad.tick_ceiling = Duration::from_millis(50);
    assert!(bad.validate().is_err());
    bad.tick_ceiling = Duration::from_secs(11);
    assert!(bad.validate().is_err());
}
