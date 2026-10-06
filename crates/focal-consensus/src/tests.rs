use super::*;

// These fixed bytes were verified against the original protobuf-codec encoder.
// They remain disk/wire fixtures: the envelope reads them, and writes them again
// byte for byte.
const PROTOBUF_ENTRY: &[u8] = &[0x10, 3, 0x18, 8, 0x22, 5, b'h', b'e', b'l', b'l', b'o'];
const PROTOBUF_HARD_STATE: &[u8] = &[0x08, 3, 0x10, 1, 0x18, 8];
const PROTOBUF_SNAPSHOT: &[u8] = &[
    0x0a, 5, b's', b't', b'a', b't', b'e', 0x12, 12, 0x0a, 6, 0x08, 1, 0x08, 2, 0x08, 3, 0x10, 8,
    0x18, 3,
];
const PROTOBUF_MESSAGE: &[u8] = &[
    0x08, 3, 0x10, 2, 0x18, 1, 0x20, 3, 0x28, 2, 0x30, 7, 0x3a, 11, 0x10, 3, 0x18, 8, 0x22, 5,
    b'h', b'e', b'l', b'l', b'o', 0x40, 7,
];

#[test]
fn original_protobuf_bytes_decode_and_reencode_exactly() {
    let entry = envelope::decode_entry(PROTOBUF_ENTRY).unwrap();
    assert_eq!(entry.term, 3);
    assert_eq!(entry.index, 8);
    assert_eq!(entry.data, b"hello");
    assert_eq!(envelope::encode_entry(&entry).unwrap(), PROTOBUF_ENTRY);
    let hard_state = envelope::decode_hard_state(PROTOBUF_HARD_STATE).unwrap();
    assert_eq!(hard_state.term, 3);
    assert_eq!(hard_state.vote, 1);
    assert_eq!(hard_state.commit, 8);
    assert_eq!(
        envelope::encode_hard_state(&hard_state).unwrap(),
        PROTOBUF_HARD_STATE
    );
    let snapshot = envelope::decode_snapshot(PROTOBUF_SNAPSHOT).unwrap();
    assert_eq!(snapshot.data, b"state");
    assert_eq!(metadata_of(&snapshot).index, 8);
    assert_eq!(conf_of(metadata_of(&snapshot)).voters, vec![1, 2, 3]);
    // Its voters were written unpacked; the envelope writes them packed, as
    // prost does, and reads either.
    let rewritten = envelope::encode_snapshot(&snapshot).unwrap();
    assert_eq!(envelope::decode_snapshot(&rewritten).unwrap(), snapshot);
    let message = decode_message(PROTOBUF_MESSAGE).unwrap();
    assert_eq!(message.to, 2);
    assert_eq!(message.from, 1);
    assert_eq!(message.msg_type, MessageType::MsgAppend);
    assert_eq!(message.entries, vec![entry]);
    assert_eq!(encode_message(&message).unwrap(), PROTOBUF_MESSAGE);
}

#[test]
fn restart_from_old_protobuf_snapshot_and_hard_state() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(1);
    let options = WalOptions::new(WalIdentity {
        cluster: cfg.cluster_id,
        node: cfg.node_id,
        stream: 0,
    });
    let mut wal = focal_log::Wal::open(dir.path(), options).unwrap();
    wal.append(&[
        identity_record(&cfg).unwrap(),
        Record {
            log: LogicalLogId(cfg.group_id),
            kind: RecordKind::Snapshot,
            index: 8,
            term: 3,
            payload: PROTOBUF_SNAPSHOT.to_vec(),
        },
        Record {
            log: LogicalLogId(cfg.group_id),
            kind: RecordKind::HardState,
            index: 8,
            term: 3,
            payload: PROTOBUF_HARD_STATE.to_vec(),
        },
    ])
    .unwrap();
    drop(wal);
    let mut node = DurableNode::open(cfg, dir.path()).unwrap();
    assert_eq!(node.status().voters, vec![1, 2, 3]);
    assert_eq!(node.drain().unwrap().snapshot.unwrap().data, b"state");
}

#[test]
fn deeply_nested_unknown_protobuf_groups_fail_without_stack_overflow() {
    // Field 99 is unknown in every Raft message. Using a known field would
    // reject its wire type immediately and would not test group recursion.
    let mut malicious = Vec::new();
    for _ in 0..100_000 {
        malicious.extend_from_slice(&[0x9b, 0x06]);
    }
    for _ in 0..100_000 {
        malicious.extend_from_slice(&[0x9c, 0x06]);
    }
    assert!(decode_message(&malicious).is_err());
    // Also exercise nested configuration data independently of the peer envelope.
    assert!(envelope::decode_conf_change_v2(&malicious).is_err());
}
pub(crate) fn config(id: u64) -> NodeConfig {
    NodeConfig::single(id, [1; 16], [2; 16])
}
/// The scalar and borrowed views say what the owned status says (the
/// audit's F53): the scalars copied, the members read in place.
#[test]
fn the_scalar_and_member_views_agree_with_the_owned_status() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    for _ in 0..2 {
        let status = node.status();
        let scalars = node.scalars();
        assert_eq!(
            (
                scalars.node_id,
                scalars.leader_id,
                scalars.term,
                scalars.committed_index,
                scalars.applied_index,
                scalars.role,
            ),
            (
                status.node_id,
                status.leader_id,
                status.term,
                status.committed_index,
                status.applied_index,
                status.role,
            )
        );
        let members = node.membership();
        assert_eq!(members.voters, status.voters.as_slice());
        assert_eq!(members.learners, status.learners.as_slice());
        assert_eq!(members.len(), status.voters.len() + status.learners.len());
        assert!(members.holds(1));
        assert!(!members.holds(2));
        node.campaign().unwrap();
        node.drain().unwrap();
    }
    assert_eq!(node.scalars().role, StateRole::Leader);
}
#[test]
fn single_voter_restart_and_quorum_read_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    assert_eq!(node.status().role, StateRole::Leader);
    node.propose(b"first".to_vec()).unwrap();
    let event = node.drain().unwrap();
    assert_eq!(event.committed.len(), 1);
    assert!(event.committed[0].index > 1);
    node.read_index(b"query-1".to_vec()).unwrap();
    assert_eq!(
        node.drain().unwrap().read_states[0].index,
        event.applied_index
    );
    drop(node);
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    assert_eq!(node.drain().unwrap().committed, event.committed);
}
#[test]
fn io_failure_never_releases_commit_and_node_fail_stops() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"uncertain".to_vec()).unwrap();
    node.inject_fault_once(FaultPoint::AfterDataSync).unwrap();
    assert!(node.drain().is_err());
    assert!(matches!(
        node.propose(b"later".to_vec()),
        Err(ConsensusError::Failed)
    ));
    drop(node);
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    assert!(node.drain().unwrap().committed.is_empty());
}
#[test]
fn checkpoint_restart_keeps_snapshot_and_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"first".to_vec()).unwrap();
    let index = node.drain().unwrap().applied_index;
    node.checkpoint(index, b"state-at-first".to_vec()).unwrap();
    node.propose(b"second".to_vec()).unwrap();
    node.drain().unwrap();
    drop(node);
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    let events = node.drain().unwrap();
    assert_eq!(events.snapshot.unwrap().data, b"state-at-first");
    assert_eq!(events.committed.len(), 1);
    assert_eq!(events.committed[0].data, b"second");
}

#[test]
fn delayed_snapshot_feedback_cannot_release_another_term_peer_or_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(1);
    cfg.learners = vec![2];
    let mut node = DurableNode::open(cfg, dir.path()).unwrap();
    node.campaign().unwrap();
    let index = node.drain().unwrap().applied_index;
    node.checkpoint(index, b"snapshot-prefix".to_vec()).unwrap();
    let term = node.status().term;
    // The learner answers a heartbeat a beat after its probe went
    // unanswered: the probe is sent again, as the checkpoint.
    for _ in 0..node.heartbeat_tick() {
        node.tick().unwrap();
        drop(node.drain().unwrap());
    }
    node.step(Message {
        from: 2,
        to: 1,
        term,
        msg_type: MessageType::MsgHeartbeatResponse,
        ..Message::default()
    })
    .unwrap();
    let events = node.drain().unwrap();
    let snapshot = events
        .messages
        .iter()
        .find(|m| m.msg_type == MessageType::MsgSnapshot)
        .unwrap();
    assert_eq!(
        metadata_of(snapshot.snapshot.as_deref().unwrap()).index,
        index
    );
    assert_eq!(
        node.log()
            .raw
            .raft
            .tracker()
            .get(2)
            .unwrap()
            .pending_snapshot,
        index
    );
    for (peer, expected_term, expected_index) in [
        (3, term, index),
        (2, term + 1, index),
        (2, term, index + 1),
        (2, term, 0),
    ] {
        node.report_snapshot_at(peer, expected_term, expected_index, SnapshotStatus::Failure)
            .unwrap();
        assert_eq!(
            node.log()
                .raw
                .raft
                .tracker()
                .get(2)
                .unwrap()
                .pending_snapshot,
            index
        );
    }
    node.report_snapshot_at(2, term, index, SnapshotStatus::Failure)
        .unwrap();
    assert_eq!(
        node.log()
            .raw
            .raft
            .tracker()
            .get(2)
            .unwrap()
            .pending_snapshot,
        0
    );
    assert_eq!(
        node.log().raw.raft.tracker().get(2).unwrap().state,
        hyper_raft::progress::ProgressState::Probe
    );
}
pub(crate) struct Cluster {
    dirs: Vec<tempfile::TempDir>,
    pub(crate) nodes: Vec<DurableNode>,
    pub(crate) applied: Vec<Vec<Vec<u8>>>,
    snapshots: Vec<Vec<AppliedSnapshot>>,
}
impl Cluster {
    pub(crate) fn new() -> Self {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let nodes = dirs
            .iter()
            .enumerate()
            .map(|(i, dir)| {
                let mut cfg = config(i as u64 + 1);
                cfg.voters = vec![1, 2, 3];
                DurableNode::open(cfg, dir.path()).unwrap()
            })
            .collect();
        Self {
            dirs,
            nodes,
            applied: vec![Vec::new(); 3],
            snapshots: vec![Vec::new(); 3],
        }
    }
    /// The flushes each member's log has made.
    fn flushes(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .map(|node| node.shared_wal().unwrap().stats().unwrap().group_commits)
            .collect()
    }
    pub(crate) fn pump(&mut self, isolated: Option<u64>) {
        for _ in 0..100 {
            let mut messages = Vec::new();
            for (i, node) in self.nodes.iter_mut().enumerate() {
                let events = node.drain().unwrap();
                self.applied[i].extend(events.committed.into_iter().map(|entry| entry.data));
                self.snapshots[i].extend(events.snapshot);
                messages.extend(events.messages);
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if isolated != Some(message.from) && isolated != Some(message.to) {
                    let snapshot = message.msg_type == MessageType::MsgSnapshot;
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
        }
        panic!("message delivery failed to quiesce");
    }
}
#[test]
fn three_voters_partition_leader_change_and_restart() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    assert_eq!(cluster.nodes[0].status().role, StateRole::Leader);
    cluster.nodes[0]
        .propose(b"committed-before-partition".to_vec())
        .unwrap();
    cluster.pump(None);
    cluster.nodes[0]
        .propose(b"isolated-uncommitted".to_vec())
        .unwrap();
    cluster.pump(Some(1));
    assert_eq!(cluster.applied[0].len(), 1);
    for _ in 0..30 {
        for (i, node) in cluster.nodes.iter_mut().enumerate() {
            node.log_mut()
                .raw
                .raft
                .set_randomized_election_timeout(10 + i * 3)
                .unwrap();
            node.tick().unwrap();
        }
        cluster.pump(Some(1));
    }
    let leader = (1..3)
        .find(|i| cluster.nodes[*i].status().role == StateRole::Leader)
        .unwrap();
    cluster.nodes[leader]
        .propose(b"committed-in-majority".to_vec())
        .unwrap();
    cluster.pump(Some(1));
    cluster.pump(None);
    for _ in 0..4 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(None);
    }
    let expected = vec![
        b"committed-before-partition".to_vec(),
        b"committed-in-majority".to_vec(),
    ];
    for actual in &cluster.applied {
        assert_eq!(actual, &expected);
    }
    // Every member stops and opens again alone: its log holds what it
    // applied, the commit written behind each release (the audit's F17).
    let Cluster { dirs, nodes, .. } = cluster;
    drop(nodes);
    for (i, dir) in dirs.iter().enumerate() {
        let mut cfg = config(i as u64 + 1);
        cfg.voters = vec![1, 2, 3];
        let mut node = DurableNode::open(cfg, dir.path()).unwrap();
        assert_eq!(
            node.drain()
                .unwrap()
                .committed
                .into_iter()
                .map(|entry| entry.data)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

/// The audit's F17: what a write waits for at each member. A commit is
/// waited for by no write of its own: the leader waits for one flush, its
/// entry's, and each follower for one; the entry is applied everywhere
/// while every disk is held; and the commit is written behind, so a member
/// that stops finds in its log what it applied, and one cut before that
/// write is told of the entry again.
#[test]
fn a_commit_is_waited_for_by_no_write_and_is_written_behind_what_it_released() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    cluster.nodes[0].propose(b"settle".to_vec()).unwrap();
    cluster.pump(None);
    for _ in 0..4 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(None);
    }
    let wals: Vec<SharedWal> = cluster
        .nodes
        .iter()
        .map(|node| node.shared_wal().unwrap())
        .collect();
    // Each member's entry is durable, by one flush each, before any disk is
    // held.
    cluster.nodes[0].propose(b"released".to_vec()).unwrap();
    let before = cluster.flushes();
    let appends = cluster.nodes[0].drain().unwrap().messages;
    let mut answers = Vec::new();
    for message in appends {
        let to = message.to as usize - 1;
        cluster.nodes[to].step(message).unwrap();
        answers.extend(cluster.nodes[to].drain().unwrap().messages);
    }
    for (before, after) in before.iter().zip(cluster.flushes()) {
        assert_eq!(after - before, 1);
    }
    // Every disk is held from here on: nothing more can become durable.
    let held: Vec<_> = wals
        .iter()
        .map(|wal| {
            let mut lease = wal.lease(LogicalLogId([250; 16])).unwrap();
            lease
                .append(&[Record {
                    log: LogicalLogId([250; 16]),
                    kind: RecordKind::Entry,
                    index: 1,
                    term: 1,
                    payload: vec![1],
                }])
                .unwrap();
            let (entered, entry) = std::sync::mpsc::sync_channel(1);
            let (resume, resumed) = std::sync::mpsc::sync_channel::<()>(1);
            let worker = std::thread::spawn(move || {
                let mut first = true;
                lease
                    .replay(|_| {
                        if first {
                            first = false;
                            entered.send(()).unwrap();
                            resumed.recv().unwrap();
                        }
                        Ok(())
                    })
                    .unwrap();
                lease
            });
            entry.recv().unwrap();
            (resume, worker)
        })
        .collect();
    // The answers commit the entry at the leader, which applies it and
    // tells its followers, which apply it: no member waits for its disk.
    let mut commits = Vec::new();
    for message in answers {
        cluster.nodes[0].step(message).unwrap();
        let events = cluster.nodes[0].drain().unwrap();
        cluster.applied[0].extend(events.committed.into_iter().map(|entry| entry.data));
        commits.extend(events.messages);
    }
    assert_eq!(cluster.applied[0].last().unwrap(), b"released");
    assert!(!cluster.nodes[0].persistence_pending());
    for message in commits {
        let to = message.to as usize - 1;
        cluster.nodes[to].step(message).unwrap();
        let events = cluster.nodes[to].drain().unwrap();
        cluster.applied[to].extend(events.committed.into_iter().map(|entry| entry.data));
    }
    for applied in &cluster.applied {
        assert_eq!(applied.last().unwrap(), b"released");
    }
    // A member cut here — its commit not yet written — is told of the
    // entry again; one that stops once its disk has caught up finds it in
    // its log. The disks are released and the members stop.
    for (resume, worker) in held {
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
    }
    let Cluster { dirs, nodes, .. } = cluster;
    drop(nodes);
    drop(wals);
    for (i, dir) in dirs.iter().enumerate() {
        let mut cfg = config(i as u64 + 1);
        cfg.voters = vec![1, 2, 3];
        let mut node = DurableNode::open(cfg, dir.path()).unwrap();
        assert_eq!(
            node.drain()
                .unwrap()
                .committed
                .into_iter()
                .map(|entry| entry.data)
                .collect::<Vec<_>>(),
            vec![b"settle".to_vec(), b"released".to_vec()]
        );
    }
}

#[test]
fn shared_stream_checkpoint_preserves_other_groups_and_group_exclusivity() {
    let dir = tempfile::tempdir().unwrap();
    let options = WalOptions::new(WalIdentity {
        cluster: [1; 16],
        node: 1,
        stream: 0,
    });
    let shared = SharedWal::open(dir.path(), options.clone()).unwrap();
    let a = config(1);
    let mut b = config(1);
    b.group_id = [3; 16];
    let mut first = DurableNode::open_on_wal(a.clone(), shared.clone()).unwrap();
    assert!(matches!(
        DurableNode::open_on_wal(a.clone(), shared.clone()),
        Err(ConsensusError::Log(LogError::LogicalLocked))
    ));
    let mut second = DurableNode::open_on_wal(b.clone(), shared.clone()).unwrap();
    first.campaign().unwrap();
    second.campaign().unwrap();
    first.drain().unwrap();
    second.drain().unwrap();
    first.propose(b"group-a".to_vec()).unwrap();
    second.propose(b"group-b".to_vec()).unwrap();
    let index = first.drain().unwrap().applied_index;
    second.drain().unwrap();
    first.checkpoint(index, b"checkpoint-a".to_vec()).unwrap();
    second
        .propose(b"group-b-after-compaction".to_vec())
        .unwrap();
    second.drain().unwrap();
    drop(first);
    drop(second);
    drop(shared);
    let shared = SharedWal::open(dir.path(), options).unwrap();
    let mut first = DurableNode::open_on_wal(a, shared.clone()).unwrap();
    let mut second = DurableNode::open_on_wal(b, shared).unwrap();
    let events = first.drain().unwrap();
    assert_eq!(events.snapshot.unwrap().data, b"checkpoint-a");
    assert!(events.committed.is_empty());
    assert_eq!(
        second
            .drain()
            .unwrap()
            .committed
            .into_iter()
            .map(|e| e.data)
            .collect::<Vec<_>>(),
        vec![b"group-b".to_vec(), b"group-b-after-compaction".to_vec()]
    );
}

#[test]
fn committed_membership_is_rebuilt_before_restart_can_campaign() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    let mut change = ConfChangeV2::default();
    let mut learner = ConfChangeSingle {
        node_id: 2,
        ..Default::default()
    };
    learner.change_type = ConfChangeType::AddLearnerNode;
    change.changes.push(learner);
    node.propose_conf_change(change).unwrap();
    node.drain().unwrap();
    assert_eq!(node.status().learners, vec![2]);
    drop(node);
    let node = DurableNode::open(config(1), dir.path()).unwrap();
    assert_eq!(node.status().learners, vec![2]);
}

fn member_change(id: u64, kind: ConfChangeType) -> ConfChangeV2 {
    let mut change = ConfChangeV2::default();
    let mut member = ConfChangeSingle {
        node_id: id,
        ..Default::default()
    };
    member.change_type = kind;
    change.changes.push(member);
    change
}

#[test]
fn recovered_snapshot_keeps_its_configuration_before_same_drain_membership_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    let prefix = node.status().applied_index;
    let before = node.membership_configuration();
    node.checkpoint(prefix, b"application-at-old-configuration".to_vec())
        .unwrap();
    node.propose_membership(
        &before,
        MembershipChange::AddLearner { node: 2 },
        b"membership-suffix".to_vec(),
    )
    .unwrap();
    let committed = node.drain().unwrap();
    assert_eq!(committed.membership.len(), 1);
    let after = committed.membership[0].after.clone();
    drop(committed);
    drop(node);
    let mut recovered = DurableNode::open(config(1), dir.path()).unwrap();
    assert_eq!(recovered.membership_configuration(), after);
    let events = recovered.drain().unwrap();
    let snapshot = events.snapshot.as_ref().unwrap();
    assert_eq!(snapshot.index, prefix);
    assert_eq!(snapshot.configuration, before);
    assert_eq!(events.membership.len(), 1);
    assert_eq!(events.membership[0].after, after);
    assert!(events.membership[0].index > snapshot.index);
    assert!(
        events.allocation.as_ref().unwrap().bytes()
            >= snapshot.configuration.charged_bytes().unwrap() + snapshot.data.capacity()
    );
}

#[test]
fn learner_requires_durable_catchup_and_installs_snapshot_before_promotion() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    cluster.nodes[0].propose(b"initial-state".to_vec()).unwrap();
    cluster.pump(None);
    cluster.nodes[0]
        .propose_conf_change(member_change(4, ConfChangeType::AddLearnerNode))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(4);
    cfg.voters = vec![1, 2, 3];
    cfg.learners = vec![4];
    cluster
        .nodes
        .push(DurableNode::open(cfg.clone(), dir.path()).unwrap());
    cluster.dirs.push(dir);
    cluster.applied.push(Vec::new());
    cluster.snapshots.push(Vec::new());
    cluster.pump(Some(4));
    assert!(matches!(
        cluster.nodes[0].propose_conf_change(member_change(4, ConfChangeType::AddNode)),
        Err(ConsensusError::LearnerBehind)
    ));
    let index = cluster.nodes[0].status().applied_index;
    cluster.nodes[0]
        .checkpoint(index, b"complete-initial-state".to_vec())
        .unwrap();
    for _ in 0..8 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(None);
    }
    assert_eq!(cluster.snapshots[3].len(), 1);
    assert_eq!(cluster.snapshots[3][0].data, b"complete-initial-state");
    cluster.nodes[0]
        .propose_conf_change(member_change(4, ConfChangeType::AddNode))
        .unwrap();
    cluster.pump(None);
    assert!(cluster.nodes[0].status().voters.contains(&4));
    assert!(!cluster.nodes[0].status().learners.contains(&4));
    cluster.nodes[0]
        .propose(b"after-promotion".to_vec())
        .unwrap();
    cluster.pump(None);
    assert_eq!(cluster.applied[3], vec![b"after-promotion".to_vec()]);
    let Cluster { nodes, dirs, .. } = cluster;
    drop(nodes);
    let mut restarted = DurableNode::open(cfg, dirs[3].path()).unwrap();
    assert!(restarted.status().voters.contains(&4));
    let events = restarted.drain().unwrap();
    assert_eq!(events.snapshot.unwrap().data, b"complete-initial-state");
    assert_eq!(events.committed[0].data, b"after-promotion");
}

/// A member behind the log's start is sent the snapshot, and its answer
/// that it holds it is sent the entries after it in the same transition.
/// The leader stages the larger of the two: priced as the snapshot alone, a
/// small snapshot with larger entries behind it outgrew the reservation the
/// answer was stepped under, and the leader stopped itself (`Capacity`, then
/// failed), a member seated after a split never caught up (focal PR #4's
/// split test on macOS and Windows CI).
#[test]
fn a_member_past_a_small_snapshot_is_sent_what_follows_it_within_its_staging() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    cluster.nodes[0]
        .propose_conf_change(member_change(4, ConfChangeType::AddLearnerNode))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(4);
    cfg.voters = vec![1, 2, 3];
    cfg.learners = vec![4];
    cluster
        .nodes
        .push(DurableNode::open(cfg, dir.path()).unwrap());
    cluster.dirs.push(dir);
    cluster.applied.push(Vec::new());
    cluster.snapshots.push(Vec::new());
    cluster.pump(Some(4));
    // A snapshot of one byte, and pages of entries after it far larger.
    let index = cluster.nodes[0].status().applied_index;
    cluster.nodes[0].checkpoint(index, b"s".to_vec()).unwrap();
    let entries: Vec<Vec<u8>> = (0..8u8).map(|byte| vec![byte; 16 * 1024]).collect();
    for entry in &entries {
        cluster.nodes[0].propose(entry.clone()).unwrap();
    }
    cluster.pump(Some(4));
    for _ in 0..8 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(None);
    }
    assert!(!cluster.nodes[0].failed(), "the leader stopped itself");
    assert_eq!(cluster.snapshots[3].len(), 1);
    assert_eq!(cluster.snapshots[3][0].data, b"s");
    assert_eq!(cluster.applied[3], entries);
}

#[test]
fn follower_io_failure_cannot_supply_a_quorum_acknowledgment() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    cluster.nodes[0]
        .propose(b"must-reach-durable-quorum".to_vec())
        .unwrap();
    let leader_events = cluster.nodes[0].drain().unwrap();
    assert!(leader_events.committed.is_empty());
    for message in leader_events
        .messages
        .into_iter()
        .filter(|message| message.to == 2)
    {
        cluster.nodes[1].step(message).unwrap();
    }
    cluster.nodes[1]
        .inject_fault_once(FaultPoint::AfterDataSync)
        .unwrap();
    assert!(cluster.nodes[1].drain().is_err());
    assert!(cluster.nodes[0].drain().unwrap().committed.is_empty());
    let failed = cluster.nodes.remove(1);
    drop(failed);
    let mut cfg = config(2);
    cfg.voters = vec![1, 2, 3];
    let mut recovered = DurableNode::open(cfg, cluster.dirs[1].path()).unwrap();
    assert!(recovered.drain().unwrap().committed.is_empty());
    cluster.nodes.insert(1, recovered);
    for _ in 0..4 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(Some(3));
    }
    assert_eq!(
        cluster.applied[0],
        vec![b"must-reach-durable-quorum".to_vec()]
    );
}

#[test]
fn isolated_leader_cannot_complete_a_quorum_read_barrier() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    cluster.nodes[0]
        .read_index(b"requires-majority".to_vec())
        .unwrap();
    let events = cluster.nodes[0].drain().unwrap();
    assert!(events.read_states.is_empty());
    assert!(!events.messages.is_empty());
    // Do not deliver the heartbeat probes. Merely ticking the former leader or
    // retaining its local disk state cannot produce a linearizable read receipt.
    for _ in 0..22 {
        cluster.nodes[0].tick().unwrap();
        assert!(cluster.nodes[0].drain().unwrap().read_states.is_empty());
    }
    assert!(matches!(
        cluster.nodes[0].read_index(b"stale-authority".to_vec()),
        Err(ConsensusError::NotLeader { .. })
    ));
}

/// Reads asked before the leader's owner drains leave in one round: two
/// heartbeats to two followers, whatever the number of reads, and the
/// answer of one follower gives every barrier. A write that commits while a
/// round is out is seen by the reads asked after it — they are asked for by
/// the next round — and not required of those asked before.
#[test]
fn reads_asked_together_are_confirmed_by_one_round_of_heartbeats() {
    let mut cluster = Cluster::new();
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    let heartbeat = MessageType::MsgHeartbeat;
    let answer = MessageType::MsgHeartbeatResponse;
    let mut asked = 0u32;
    for together in [1usize, 32, 128] {
        let contexts: Vec<Vec<u8>> = (0..together)
            .map(|_| {
                asked += 1;
                asked.to_be_bytes().to_vec()
            })
            .collect();
        for context in &contexts {
            cluster.nodes[0].read_index(context.clone()).unwrap();
        }
        assert!(cluster.nodes[0].reads_unasked());
        let events = cluster.nodes[0].drain().unwrap();
        assert!(events.read_states.is_empty());
        assert!(!cluster.nodes[0].reads_unasked());
        let round: Vec<Message> = events
            .messages
            .into_iter()
            .filter(|message| message.msg_type == heartbeat)
            .collect();
        assert_eq!(
            round.len(),
            2,
            "{together} reads left in {} heartbeats",
            round.len()
        );
        // One follower answers; the other's heartbeat is lost.
        let mut answers = Vec::new();
        for message in round.into_iter().filter(|message| message.to == 2) {
            cluster.nodes[1].step(message).unwrap();
            answers.extend(cluster.nodes[1].drain().unwrap().messages);
        }
        for message in answers
            .into_iter()
            .filter(|message| message.msg_type == answer)
        {
            cluster.nodes[0].step(message).unwrap();
        }
        let barriers = cluster.nodes[0].drain().unwrap().read_states;
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| barrier.context.clone())
                .collect::<Vec<_>>(),
            contexts
        );
    }
    cluster.pump(None);
    // A round is out for `before`; a write commits; `after` is asked.
    let commit = cluster.nodes[0].status().committed_index;
    cluster.nodes[0].read_index(b"before".to_vec()).unwrap();
    let first: Vec<Message> = cluster.nodes[0].drain().unwrap().messages;
    cluster.nodes[0].propose(b"write".to_vec()).unwrap();
    let appends = cluster.nodes[0].drain().unwrap().messages;
    let mut answers = Vec::new();
    for message in appends {
        let to = (message.to - 1) as usize;
        cluster.nodes[to].step(message).unwrap();
        answers.extend(cluster.nodes[to].drain().unwrap().messages);
    }
    let mut after_write = Vec::new();
    for message in answers {
        cluster.nodes[0].step(message).unwrap();
        after_write.extend(cluster.nodes[0].drain().unwrap().messages);
    }
    assert_eq!(cluster.nodes[0].status().committed_index, commit + 1);
    cluster.nodes[0].read_index(b"after".to_vec()).unwrap();
    let second: Vec<Message> = cluster.nodes[0].drain().unwrap().messages;
    // The answers to the first round confirm `before`, at the commit it was
    // asked at, and not `after`.
    let mut barriers = Vec::new();
    for round in [first, second] {
        let mut answers = Vec::new();
        for message in round
            .into_iter()
            .filter(|message| message.msg_type == heartbeat && message.to == 2)
        {
            cluster.nodes[1].step(message).unwrap();
            answers.extend(cluster.nodes[1].drain().unwrap().messages);
        }
        for message in answers
            .into_iter()
            .filter(|message| message.msg_type == answer)
        {
            cluster.nodes[0].step(message).unwrap();
        }
        barriers.push(cluster.nodes[0].drain().unwrap().read_states);
    }
    assert_eq!(
        barriers
            .iter()
            .map(|round| round
                .iter()
                .map(|barrier| (barrier.context.clone(), barrier.index))
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![(b"before".to_vec(), commit)],
            vec![(b"after".to_vec(), commit + 1)]
        ]
    );
    drop(after_write);
}

/// A member is sent a page ahead of its answers until its owner says what
/// the path to it carries; what the owner says is in force at once, is
/// never nothing, and is never more than the group's budget can stage for
/// one transition — a window the budget could not stage would refuse every
/// answer of the member it was made for.
#[test]
fn the_bytes_sent_ahead_of_a_members_answers_are_what_its_path_carries_and_the_budget_stages() {
    let dir = tempfile::tempdir().unwrap();
    let limit = 96 * 1024 * 1024;
    let budget = MemoryBudget::new(limit, 16 * 1024 * 1024).unwrap();
    let mut cfg = config(1);
    cfg.voters = vec![1, 2, 3];
    let mut node = DurableNode::open_in(cfg, dir.path(), &budget).unwrap();
    let page = node.page_bytes();
    assert_eq!(node.inflight_bytes(2), Some((0, page)));
    assert_eq!(node.inflight_bytes(9), None);
    assert!(node.set_inflight_bytes(2, 6_000).unwrap());
    assert_eq!(node.inflight_bytes(2), Some((0, 6_000)));
    assert_eq!(node.inflight_bytes(3), Some((0, page)));
    assert!(!node.set_inflight_bytes(9, 6_000).unwrap());
    assert!(node.set_inflight_bytes(2, 0).unwrap());
    assert_eq!(node.inflight_bytes(2), Some((0, 1)));
    // More than the budget holds: what it can stage beside a page for each
    // member, and no more.
    assert!(node.set_inflight_bytes(3, u64::MAX).unwrap());
    let (_, bound) = node.inflight_bytes(3).unwrap();
    assert!(bound > page && bound < limit as u64 - 4 * page, "{bound}");
    // The staging a transition reserves prices the window by its bound:
    // with every member at a page it reserves far less than with one
    // member at all the budget stages.
    let wide = node.staging_estimate().unwrap();
    assert!(node.set_inflight_bytes(3, page).unwrap());
    let narrow = node.staging_estimate().unwrap();
    assert!(narrow <= wide);
}

#[test]
fn a_failure_of_the_core_stops_only_this_replica_until_disk_recovery() {
    for unwinds in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut node = DurableNode::open(config(1), dir.path()).unwrap();
        node.campaign().unwrap();
        node.drain().unwrap();
        node.propose(b"durable-before-the-failure".to_vec())
            .unwrap();
        let committed = node.drain().unwrap().committed;
        let result = node.log_mut().guarded(|replica| {
            if unwinds {
                // Nothing the core does unwinds; what it depends on is
                // fenced all the same.
                std::panic::resume_unwind(Box::new("a dependency unwound"));
            }
            // The core says its state no longer adds up: what is applied
            // cannot pass what is committed.
            replica.raw.advance_apply_to(u64::MAX)?;
            Ok(())
        });
        if unwinds {
            assert!(matches!(result, Err(ConsensusError::DependencyFailure)));
        } else {
            assert!(matches!(
                result,
                Err(ConsensusError::Raft(hyper_raft::Error::Invariant(_)))
            ));
        }
        assert!(node.failed());
        assert!(matches!(
            node.propose(b"must-not-append".to_vec()),
            Err(ConsensusError::Failed)
        ));
        assert!(matches!(node.drain(), Err(ConsensusError::Failed)));
        drop(node);
        let mut recovered = DurableNode::open(config(1), dir.path()).unwrap();
        assert_eq!(recovered.drain().unwrap().committed, committed);
    }
}

#[test]
fn upstream_invariant_failure_stops_only_this_replica_until_disk_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"durable-before-dependency-failure".to_vec())
        .unwrap();
    let committed = node.drain().unwrap().committed;
    let result = node.log_mut().guarded(|_| -> Result<(), ConsensusError> {
        std::panic::resume_unwind(Box::new("a dependency unwound"))
    });
    assert!(matches!(result, Err(ConsensusError::DependencyFailure)));
    assert!(matches!(
        node.propose(b"must-not-append".to_vec()),
        Err(ConsensusError::Failed)
    ));
    assert!(matches!(node.drain(), Err(ConsensusError::Failed)));
    drop(node);
    let mut recovered = DurableNode::open(config(1), dir.path()).unwrap();
    assert_eq!(recovered.drain().unwrap().committed, committed);
}

#[test]
fn pathological_configuration_and_peer_shapes_return_errors_without_poisoning() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(1);
    cfg.election_tick = usize::MAX;
    assert!(matches!(
        DurableNode::open(cfg, dir.path()),
        Err(ConsensusError::Configuration(_))
    ));
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    // A kind no message has (the largest `int32`), to 1 from 2 in term 1: the
    // envelope refuses it before the core can be given it.
    let unknown_kind = [
        0x08, 0xff, 0xff, 0xff, 0xff, 0x07, 0x10, 1, 0x18, 2, 0x20, 1,
    ];
    assert!(matches!(
        node.step_authenticated(2, &unknown_kind),
        Err(ConsensusError::MalformedMessage(_))
    ));
    let mut message = Message {
        from: 2,
        to: 1,
        term: 1,
        index: 1,
        ..Default::default()
    };
    message.msg_type = MessageType::MsgAppend;
    message.entries.push(Entry {
        index: 3,
        term: 1,
        ..Default::default()
    });
    assert!(matches!(
        node.step(message),
        Err(ConsensusError::MalformedMessage(_))
    ));
    node.campaign().unwrap();
    node.drain().unwrap();
    assert_eq!(node.status().role, StateRole::Leader);
}

fn budgeted_node(budget: &MemoryBudget, dir: &std::path::Path) -> (DurableNode, SharedWal) {
    let cfg = config(1);
    let wal = SharedWal::open(
        dir,
        WalOptions::new(WalIdentity {
            cluster: cfg.cluster_id,
            node: 1,
            stream: 0,
        }),
    )
    .unwrap();
    (
        DurableNode::open_on_wal_in(cfg, wal.clone(), budget).unwrap(),
        wal,
    )
}

#[test]
fn hierarchy_admission_rolls_back_and_emitted_buffers_keep_their_permit() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(32 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let tenant = parent.child(24 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let (mut node, wal) = budgeted_node(&tenant, dir.path());
    assert!(node.is_budgeted_within(&parent));
    assert!(node.is_budgeted_within(&tenant));
    assert!(!node.is_budgeted_within(&MemoryBudget::new(1, 0).unwrap()));
    assert!(tenant.stats().used < 128 * 1024);
    node.campaign().unwrap();
    drop(node.drain().unwrap());
    let before = tenant.stats().used;
    let records = wal.stats().unwrap().appended_records;
    let pressure = tenant
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            tenant.stats().limit - tenant.stats().completion_reserve - tenant.stats().ordinary_used,
        )
        .unwrap()
        .commit();
    assert!(matches!(
        node.propose(vec![7; 256 * 1024]),
        Err(ConsensusError::Capacity)
    ));
    assert_eq!(wal.stats().unwrap().appended_records, records);
    node.propose_in(vec![9; 256 * 1024], BudgetLane::Completion)
        .unwrap();
    let mut events = node.drain().unwrap();
    assert_eq!(events.committed[0].data, vec![9; 256 * 1024]);
    let output_permit = events.take_allocation().unwrap();
    assert!(output_permit.bytes() >= events.committed[0].data.len());
    drop(pressure);
    assert!(tenant.stats().used > before);
    drop(node);
    assert_eq!(tenant.stats().used, output_permit.bytes());
    assert_eq!(parent.stats().used, output_permit.bytes());
    drop(events);
    drop(output_permit);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn retained_log_prepare_is_atomic_under_quota_and_compaction_releases_payloads() {
    let budget = MemoryBudget::new(256 * 1024, 0).unwrap();
    let mut log = RamLog::new(&config(1), budget.clone()).unwrap();
    let first = Entry {
        index: 1,
        term: 1,
        data: vec![1; 32 * 1024],
        ..Default::default()
    };
    log.append(std::slice::from_ref(&first)).unwrap();
    let baseline = budget.stats().used;
    let pressure = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap()
        .commit();
    let replacement = Entry {
        index: 1,
        term: 2,
        data: vec![2; 64 * 1024],
        ..Default::default()
    };
    assert!(matches!(
        log.prepare(&[replacement], None),
        Err(ConsensusError::Capacity)
    ));
    assert_eq!(log.entries.front(), Some(&first));
    drop(pressure);
    assert_eq!(budget.stats().used, baseline);
    let snapshot = Snapshot {
        data: vec![3; 1024],
        metadata: Some(SnapshotMetadata {
            conf_state: Some(log.conf_state.clone()),
            index: 1,
            term: 1,
        }),
    };
    let prepared = log.prepare_snapshot(&snapshot).unwrap();
    log.compact_prepared(prepared).unwrap();
    assert!(budget.stats().used < baseline);
    assert_eq!(log.first_index().unwrap(), 2);
    drop(log);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn maximum_entry_and_snapshot_recover_with_bounded_actual_decode_scratch() {
    let dir = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let mut cfg = config(1);
    cfg.max_entry_bytes = 8 * 1024 * 1024;
    let options = WalOptions::new(WalIdentity {
        cluster: cfg.cluster_id,
        node: 1,
        stream: 0,
    });
    let wal = SharedWal::open(dir.path(), options.clone()).unwrap();
    let mut node = DurableNode::open_on_wal_in(cfg.clone(), wal, &budget).unwrap();
    node.campaign().unwrap();
    drop(node.drain().unwrap());
    node.propose(vec![11; cfg.max_entry_bytes]).unwrap();
    let events = node.drain().unwrap();
    assert_eq!(events.committed[0].data.len(), cfg.max_entry_bytes);
    let index = events.applied_index;
    drop(events);
    drop(node);
    assert_eq!(budget.stats().used, 0);
    let wal = SharedWal::open(dir.path(), options.clone()).unwrap();
    let mut node = DurableNode::open_on_wal_in(cfg.clone(), wal, &budget).unwrap();
    let recovered = node.drain().unwrap();
    assert_eq!(recovered.committed[0].data, vec![11; cfg.max_entry_bytes]);
    drop(recovered);
    node.checkpoint(index, vec![13; 8 * 1024 * 1024]).unwrap();
    drop(node);
    assert_eq!(budget.stats().used, 0);
    let wal = SharedWal::open(dir.path(), options).unwrap();
    let mut node = DurableNode::open_on_wal_in(cfg, wal, &budget).unwrap();
    let recovered = node.drain().unwrap();
    assert_eq!(
        recovered.snapshot.as_ref().unwrap().data,
        vec![13; 8 * 1024 * 1024]
    );
    drop(node);
    assert!(budget.stats().used >= 8 * 1024 * 1024);
    drop(recovered);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn protobuf_preflight_accounts_repeated_structs_without_bulk_payload_multiplier() {
    let message = Message {
        entries: vec![Entry::default(); 4096],
        ..Default::default()
    };
    let encoded = crate::encode_message(&message).unwrap();
    let scratch = decode_message_charge(&encoded).unwrap();
    assert_eq!(scratch, memory::message_scratch(&encoded).unwrap());
    assert!(scratch > encoded.len() * 32);
    assert!(scratch >= memory::message_bytes(&message).unwrap());
    let mut snapshot = Snapshot {
        data: vec![5; 8 * 1024 * 1024],
        metadata: Some(SnapshotMetadata {
            conf_state: Some(ConfState {
                voters: vec![1],
                ..Default::default()
            }),
            ..Default::default()
        }),
    };
    let record = proto_record([2; 16], RecordKind::Snapshot, 1, 1, &snapshot).unwrap();
    assert!(memory::replay_scratch(&record).unwrap() < 17 * 1024 * 1024);
    snapshot
        .metadata
        .as_mut()
        .and_then(|metadata| metadata.conf_state.as_mut())
        .unwrap()
        .voters = vec![1; 2049];
    let oversized = proto_record([2; 16], RecordKind::Snapshot, 1, 1, &snapshot).unwrap();
    assert!(matches!(
        memory::replay_scratch(&oversized),
        Err(ConsensusError::Capacity)
    ));
    assert!(decode_message_charge(&[0x3a, 0xff, 0xff, 0xff]).is_err());
    assert!(matches!(
        decode_message_charge(&vec![0; 9 * 1024 * 1024 + 1]),
        Err(ConsensusError::Capacity)
    ));
}

/// A member's core bounds are what focal states of it (`core_state::limits`), read back through
/// `limits()`: a message of a page or one entry of the largest, inside its record; each queue one
/// ready's entries counted resident.
#[test]
fn a_members_core_bounds_are_derived_from_its_settings() {
    use hyper_raft::wire::{ENTRY_FIXED_BYTES, MESSAGE_RECORD_FIXED_BYTES};
    let dir = tempfile::tempdir().unwrap();
    let config = config(1);
    let node = DurableNode::open(config.clone(), dir.path()).unwrap();
    let limits = node.limits();
    let page = config.max_entry_bytes + 1024;
    let message = page.max(config.max_entry_bytes + ENTRY_FIXED_BYTES) + MESSAGE_RECORD_FIXED_BYTES;
    let payload = message - MESSAGE_RECORD_FIXED_BYTES;
    assert_eq!(limits.entries_per_message, payload / ENTRY_FIXED_BYTES);
    let ready = (config.max_inflight_messages * page).max(config.max_uncommitted_bytes as usize)
        + config.max_entry_bytes;
    let memory = ready / ENTRY_FIXED_BYTES * std::mem::size_of::<Entry>();
    assert_eq!(
        limits.unstable_entries,
        memory / std::mem::size_of::<Entry>()
    );
    assert_eq!(limits.proposals, payload / std::mem::size_of::<Entry>());
    assert_eq!(limits.pending_reads, config.max_inflight_messages + 1);
}
/// Three voters whose leader is node 1; `bounds` sets each one's largest
/// entry and the bytes of uncommitted entries it may lead.
fn led_cluster(bounds: Option<(usize, u64)>) -> Cluster {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let nodes = dirs
        .iter()
        .enumerate()
        .map(|(i, dir)| {
            let mut cfg = config(i as u64 + 1);
            cfg.voters = vec![1, 2, 3];
            if let Some((entry, uncommitted)) = bounds {
                cfg.max_entry_bytes = entry;
                cfg.max_uncommitted_bytes = uncommitted;
            }
            DurableNode::open(cfg, dir.path()).unwrap()
        })
        .collect();
    let mut cluster = Cluster {
        dirs,
        nodes,
        applied: vec![Vec::new(); 3],
        snapshots: vec![Vec::new(); 3],
    };
    cluster.nodes[0].campaign().unwrap();
    cluster.pump(None);
    assert_eq!(cluster.nodes[0].status().role, StateRole::Leader);
    cluster
}
/// hyper-raft answers one `ProposalDropped` for several causes; the owners
/// are told which, so a refusal is retried and never taken for a failure.
#[test]
fn a_dropped_proposal_is_refused_by_its_cause() {
    // A leader whose uncommitted entries fill its bound: capacity. Nothing
    // commits while the leader's messages go undelivered.
    // The smallest bound a node takes: one largest entry and 1 KiB more.
    let mut cluster = led_cluster(Some((512, 1536)));
    for fill in 0..3 {
        cluster.nodes[0].propose(vec![fill; 512]).unwrap();
    }
    assert!(matches!(
        cluster.nodes[0].propose(vec![3; 512]),
        Err(ConsensusError::Capacity)
    ));
    // Once those commit, the leader takes the next.
    cluster.pump(None);
    cluster.nodes[0].propose(vec![3; 512]).unwrap();

    // A leader handing over its leadership: leaving.
    let mut cluster = led_cluster(None);
    cluster.nodes[0].transfer_leader(2).unwrap();
    assert!(matches!(
        cluster.nodes[0].propose(b"during the handover".to_vec()),
        Err(ConsensusError::LeaderLeaving)
    ));

    // A member that does not lead names its leader, as it did before.
    assert!(matches!(
        cluster.nodes[2].propose(b"to a follower".to_vec()),
        Err(ConsensusError::NotLeader { leader: 1 })
    ));
}
