//! The fast track on durable nodes (27 §4): what a member approved by itself
//! is on disk before it says so, outlives a restart and a checkpoint, and a
//! group is opened with the track it was made with; a fast leader is ready only
//! once its term-start entry commits. And on the classic track, a member
//! restarted under loss catches up past its hole (hyper-raft S-4's two fixes).
use crate::{
    ConsensusError, DurableNode, Entry, EntryType, Message, MessageType, NodeConfig, StateRole,
    Wire, tests::config,
};
use focal_log::{LogicalLogId, RecordKind, SharedWal, WalIdentity, WalOptions};
use hyper_raft::fast::{FAST_PROPOSE, FAST_VOTE};

/// A member raised to the wire a fast group runs on: a follower releases what it approved by
/// itself only through the classic commit its leader sends (hyper-raft `Message::classic`), which
/// only the raised wire carries (`Wire`); below it a member holds what it approved until the
/// core's bound refuses more.
fn raised(mut node: DurableNode) -> DurableNode {
    node.set_raft_wire(Wire::Kept).unwrap();
    node
}

fn fast(id: u64) -> NodeConfig {
    let mut config = config(id);
    config.voters = vec![1, 2, 3];
    config.fast = true;
    config
}
struct Group {
    dirs: Vec<tempfile::TempDir>,
    nodes: Vec<DurableNode>,
    applied: Vec<Vec<Vec<u8>>>,
    displaced: Vec<Vec<Vec<u8>>>,
    /// What is on its way.
    net: Vec<Message>,
    /// The members' settings.
    make: fn(u64) -> NodeConfig,
}
impl Group {
    fn new() -> Self {
        Self::with(fast)
    }
    /// A group of three whose members open under `make`'s settings, the first elected.
    fn with(make: fn(u64) -> NodeConfig) -> Self {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let nodes = dirs
            .iter()
            .enumerate()
            .map(|(i, dir)| raised(DurableNode::open(make(i as u64 + 1), dir.path()).unwrap()))
            .collect();
        let mut group = Self {
            dirs,
            nodes,
            applied: vec![Vec::new(); 3],
            displaced: vec![Vec::new(); 3],
            net: Vec::new(),
            make,
        };
        group.nodes[0].campaign().unwrap();
        group.settle();
        assert_eq!(group.nodes[0].status().role, StateRole::Leader);
        assert!(group.nodes[0].has_committed_current_term());
        group
    }
    fn drain(&mut self) {
        for (i, node) in self.nodes.iter_mut().enumerate() {
            let events = node.drain().unwrap();
            self.applied[i].extend(events.committed.into_iter().map(|entry| entry.data));
            self.displaced[i].extend(events.displaced.into_iter().map(|entry| entry.data));
            self.net.extend(events.messages);
        }
    }
    /// Delivers what `carried` admits until nothing it admits is sent.
    fn carry(&mut self, carried: impl Fn(&Message) -> bool) {
        for _ in 0..100 {
            self.drain();
            let (sent, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.net)
                .into_iter()
                .partition(&carried);
            self.net = kept;
            if sent.is_empty() {
                return;
            }
            for message in sent {
                let to = message.to;
                // As a peer sends it: encoded under the wire, and its sender
                // the one the transport knows.
                let encoded = crate::encode_message_in(&message, Wire::Kept).unwrap();
                self.nodes[(to - 1) as usize]
                    .step_authenticated(message.from, &encoded)
                    .unwrap();
            }
        }
        panic!("message delivery failed to quiesce");
    }
    /// Ticks the leader through one heartbeat and delivers what follows.
    fn heartbeat(&mut self) {
        for _ in 0..(self.make)(1).heartbeat_tick {
            self.nodes[0].tick().unwrap();
        }
        self.settle();
    }
    fn settle(&mut self) {
        self.carry(|_| true);
    }
    /// Delivers one message, as `carry` does.
    fn deliver(&mut self, message: Message) {
        let encoded = crate::encode_message_in(&message, Wire::Kept).unwrap();
        self.nodes[(message.to - 1) as usize]
            .step_authenticated(message.from, &encoded)
            .unwrap();
    }
    fn reopen(&mut self, node: usize) {
        let config = (self.make)(node as u64 + 1);
        let dir = self.dirs[node].path().to_path_buf();
        // The node that was is gone before its log is opened again.
        let placeholder = tempfile::tempdir().unwrap();
        let mut other = config.clone();
        other.group_id = [0xee; 16];
        let old = std::mem::replace(
            &mut self.nodes[node],
            DurableNode::open(other, placeholder.path()).unwrap(),
        );
        drop(old);
        self.nodes[node] = raised(DurableNode::open(config, dir).unwrap());
    }
    fn held(&self, node: usize) -> Vec<(u64, Vec<u8>)> {
        self.nodes[node]
            .log()
            .raw
            .raft
            .proposals()
            .map(|held| (held.index, held.data.clone()))
            .collect()
    }
}

#[test]
fn a_followers_proposal_is_committed_by_the_fast_quorum_and_applied_by_all() {
    let mut group = Group::new();
    let before = group.nodes[0].status().committed_index;
    let index = group.nodes[1].propose_fast(b"fast".to_vec()).unwrap();
    assert_eq!(index, before + 1);
    // To every voter; and the votes, to the leader. What the leader sends
    // its members waits.
    group.carry(|message| message.msg_type == FAST_PROPOSE);
    assert_eq!(group.held(2), vec![(index, b"fast".to_vec())]);
    group.carry(|message| message.msg_type == FAST_VOTE);
    assert_eq!(group.nodes[0].status().committed_index, index);
    assert_eq!(group.nodes[0].fast_stats().committed, 1);
    assert!(
        !group
            .net
            .iter()
            .any(|message| message.msg_type == MessageType::MsgAppendResponse),
        "a member answered the leader before the index was committed"
    );
    assert_eq!(group.applied[0].last().unwrap(), b"fast");
    group.settle();
    for node in 0..3 {
        assert_eq!(group.applied[node].last().unwrap(), b"fast");
    }
    // What a member approved by itself it holds until it knows a classic quorum committed it
    // (hyper-raft `Ready::released`): the leader once its members answer the append, a follower
    // once its leader says so, on the next append or heartbeat.
    group.heartbeat();
    for node in 0..3 {
        assert!(
            group.held(node).is_empty(),
            "member {node} still holds what it approved"
        );
    }
    assert_eq!(group.nodes[1].fast_stats().proposed, 1);
    assert!(group.displaced.iter().all(Vec::is_empty));
    // A leader proposes as it always did.
    let index = group.nodes[0].propose_fast(b"led".to_vec()).unwrap();
    group.settle();
    assert_eq!(group.nodes[2].status().committed_index, index);
    assert_eq!(group.applied[2].last().unwrap(), b"led");
}

#[test]
fn what_a_member_approved_is_on_disk_before_it_says_so_and_after_it_stopped() {
    let mut group = Group::new();
    let index = group.nodes[1].propose_fast(b"held".to_vec()).unwrap();
    // The vote is of what is durable: none is sent before the drain that
    // persists what is held.
    assert!(group.nodes[1].has_ready());
    group.carry(|message| message.msg_type == FAST_PROPOSE && message.to == 3);
    assert!(
        group
            .net
            .iter()
            .any(|message| message.msg_type == FAST_VOTE && message.from == 3)
    );
    group.net.clear();
    for node in [1, 2] {
        group.reopen(node);
        assert_eq!(group.held(node), vec![(index, b"held".to_vec())]);
    }
    let lease = |dir: &std::path::Path, node: u64| {
        let wal = SharedWal::open(
            dir,
            WalOptions::new(WalIdentity {
                cluster: fast(node).cluster_id,
                node,
                stream: 0,
            }),
        )
        .unwrap();
        let lease = wal.lease(LogicalLogId(fast(node).group_id)).unwrap();
        let mut kinds = Vec::new();
        lease
            .replay(|record| {
                kinds.push((record.kind, record.index));
                Ok(())
            })
            .unwrap();
        kinds
    };
    // A checkpoint keeps what is held above it.
    let applied = group.nodes[2].status().applied_index;
    group.nodes[2]
        .checkpoint(applied, b"state".to_vec())
        .unwrap();
    group.reopen(2);
    assert_eq!(group.held(2), vec![(index, b"held".to_vec())]);
    let dir = group.dirs[2].path().to_path_buf();
    let placeholder = tempfile::tempdir().unwrap();
    let mut other = fast(3);
    other.group_id = [0xee; 16];
    let old = std::mem::replace(
        &mut group.nodes[2],
        DurableNode::open(other, placeholder.path()).unwrap(),
    );
    drop(old);
    let kinds = lease(&dir, 3);
    assert_eq!(
        kinds
            .iter()
            .filter(|(kind, _)| *kind == RecordKind::FastTrack)
            .count(),
        1
    );
    assert!(kinds.contains(&(RecordKind::Proposal, index)));
    group.nodes[2] = raised(DurableNode::open(fast(3), &dir).unwrap());
    // With the leader gone, whoever is elected takes what the two hold.
    for _ in 0..60 {
        for node in [1, 2] {
            group.nodes[node].tick().unwrap();
        }
        group.carry(|message| message.from != 1 && message.to != 1);
        if group.nodes[1..]
            .iter()
            .any(|node| node.status().role == StateRole::Leader)
        {
            break;
        }
    }
    let leader = (1..3)
        .find(|node| group.nodes[*node].status().role == StateRole::Leader)
        .expect("the two elect");
    assert!(group.nodes[leader].fast_stats().recovered >= 1);
    group.carry(|message| message.from != 1 && message.to != 1);
    for node in [1, 2] {
        assert!(group.applied[node].iter().any(|entry| entry == b"held"));
        assert!(group.held(node).is_empty());
    }
}

/// What a member approved by itself is held until it knows a classic quorum committed it, whatever
/// its log reached (hyper-raft d8578be: a member that let go once its log reached the index gave up
/// a fast vote a later leader could still count against). Here the follower's log holds the entry
/// before the follower learns the classic commit, and it holds the proposal across a restart; the
/// release is written, and a restart after it brings nothing back.
#[test]
fn a_proposal_the_log_reached_is_held_until_released_and_the_release_outlives_a_restart() {
    let mut group = Group::new();
    let index = group.nodes[1].propose_fast(b"kept".to_vec()).unwrap();
    group.settle();
    for node in 0..3 {
        assert!(group.nodes[node].status().committed_index >= index);
        assert_eq!(group.applied[node].last().unwrap(), b"kept");
    }
    // The follower's log holds the entry; no leader has said a classic quorum committed it.
    assert_eq!(group.held(1), vec![(index, b"kept".to_vec())]);
    group.reopen(1);
    assert_eq!(
        group.held(1),
        vec![(index, b"kept".to_vec())],
        "a proposal the log reached was let go at a restart before its release"
    );
    group.heartbeat();
    assert!(group.held(1).is_empty());
    // A release rides a write the member makes anyway (hyper-raft `Ready::released`): until one,
    // a restart gives the proposal back and the member holds it until it learns the commit again,
    // which costs room and never safety.
    group.reopen(1);
    assert_eq!(group.held(1), vec![(index, b"kept".to_vec())]);
    group.heartbeat();
    assert!(group.held(1).is_empty());
    // The next entry is such a write: the release is durable with it, and a restart after it
    // brings nothing back.
    group.nodes[0].propose(b"next".to_vec()).unwrap();
    group.settle();
    assert_eq!(group.applied[1].last().unwrap(), b"next");
    group.reopen(1);
    assert!(
        group.held(1).is_empty(),
        "a released proposal came back at a restart"
    );
    // And it survives the checkpoint that rewrites the member's stream.
    let applied = group.nodes[1].status().applied_index;
    group.nodes[1]
        .checkpoint(applied, b"state".to_vec())
        .unwrap();
    group.reopen(1);
    assert!(group.held(1).is_empty());
}

#[test]
fn of_two_proposals_for_one_index_the_one_not_taken_is_said_to_its_proposer() {
    let mut group = Group::new();
    let first = group.nodes[1].propose_fast(b"one".to_vec()).unwrap();
    let second = group.nodes[2].propose_fast(b"two".to_vec()).unwrap();
    assert_eq!(first, second);
    group.settle();
    let taken = group.applied[0].last().unwrap().clone();
    let (winner, loser) = if taken == b"one" { (1, 2) } else { (2, 1) };
    assert!(group.displaced[winner].is_empty());
    assert_eq!(group.displaced[loser].len(), 1);
    assert_ne!(group.displaced[loser][0], taken);
    for node in 0..3 {
        assert_eq!(group.applied[node].last().unwrap(), &taken);
    }
    // It proposes again, and is taken at the next index.
    let again = group.displaced[loser][0].clone();
    let index = group.nodes[loser].propose_fast(again.clone()).unwrap();
    assert_eq!(index, first + 1);
    group.settle();
    for node in 0..3 {
        assert_eq!(group.applied[node].last().unwrap(), &again);
    }
}

#[test]
fn a_group_is_opened_with_the_track_it_was_made_with() {
    let dir = tempfile::tempdir().unwrap();
    drop(DurableNode::open(fast(1), dir.path()).unwrap());
    assert!(matches!(
        DurableNode::open(
            {
                let mut classic = fast(1);
                classic.fast = false;
                classic
            },
            dir.path()
        ),
        Err(ConsensusError::Configuration(_))
    ));
    drop(DurableNode::open(fast(1), dir.path()).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let mut classic = fast(1);
    classic.fast = false;
    let mut node = DurableNode::open(classic.clone(), dir.path()).unwrap();
    assert!(matches!(
        DurableNode::open(fast(1), tempfile::tempdir().unwrap().path()).map(|node| node.fast()),
        Ok(true)
    ));
    // A group that has none neither proposes by it nor hears of it.
    assert!(matches!(
        node.propose_fast(b"x".to_vec()),
        Err(ConsensusError::Configuration(_))
    ));
    for kind in [FAST_PROPOSE, FAST_VOTE] {
        let message = Message {
            msg_type: kind,
            from: 2,
            to: 1,
            term: 1,
            entries: vec![Entry {
                index: 1,
                data: vec![1],
                ..Entry::default()
            }],
            ..Message::default()
        };
        assert!(matches!(
            node.step(message),
            Err(ConsensusError::MalformedMessage(_))
        ));
    }
    assert!(!node.failed());
    drop(node);
    assert!(matches!(
        DurableNode::open(fast(1), dir.path()),
        Err(ConsensusError::Configuration(_))
    ));
    // The identity a group states is as it was before there was a fast
    // track: a group that has none writes what it always wrote.
    let mut identity = classic.clone();
    identity.fast = true;
    assert_eq!(
        postcard::to_stdvec(&identity).unwrap(),
        postcard::to_stdvec(&classic).unwrap()
    );
}

#[test]
fn what_may_not_go_by_the_fast_track_is_refused_before_the_core() {
    let mut group = Group::new();
    let proposal = |entry: Entry| Message {
        msg_type: FAST_PROPOSE,
        from: 3,
        to: 2,
        entries: vec![entry],
        ..Message::default()
    };
    let refused = [
        proposal(Entry {
            entry_type: crate::EntryType::EntryConfChangeV2,
            index: 2,
            data: vec![1],
            ..Entry::default()
        }),
        proposal(Entry {
            index: 2,
            ..Entry::default()
        }),
        proposal(Entry {
            data: vec![1],
            ..Entry::default()
        }),
        Message {
            msg_type: FAST_PROPOSE,
            from: 3,
            to: 2,
            ..Message::default()
        },
    ];
    for message in refused {
        assert!(matches!(
            group.nodes[1].step(message),
            Err(ConsensusError::MalformedMessage(_))
        ));
    }
    assert!(matches!(
        group.nodes[1].step(proposal(Entry {
            index: 2,
            data: vec![0; 4 * 1024 * 1024 + 1],
            ..Entry::default()
        })),
        Err(ConsensusError::Capacity)
    ));
    assert!(!group.nodes[1].failed());
    assert!(group.held(1).is_empty());
    assert!(matches!(
        group.nodes[1].propose_fast(Vec::new()),
        Err(ConsensusError::Capacity)
    ));
}

/// The term-start entry of `node`'s log: the first entry of its current term with no data.
fn term_start(group: &Group, node: usize) -> Option<u64> {
    let raw = &group.nodes[node].log().raw;
    let term = raw.raft.term();
    raw.store()
        .entries
        .iter()
        .find(|entry| {
            entry.term == term
                && entry.entry_type == EntryType::EntryNormal
                && entry.data.is_empty()
        })
        .map(|entry| entry.index)
}

/// A fast leader is ready for reads and for a change of configuration only once it commits the
/// entry it began its term with (hyper-raft S-4; Ongaro's thesis §6.4). The leader of term 1
/// commits a proposal by the fast quorum and leaves before its members learn the commit; the member
/// elected after it takes what its voters approved into its log under its own term, below that
/// commit, and its term-start entry after it. Readiness is checked after every message delivered,
/// at the leader and at a follower: it never holds while the commit is below the term-start entry. (focal's members hold no more
/// approved bytes than one message carries, so today the recovered entries and the term-start
/// entry travel in one append and commit together; hyper-raft's judge reached the commit between
/// them at seed 15,761 of its fast schedules, and this holds focal's rule at that boundary.)
#[test]
fn a_fast_leader_is_ready_only_once_its_term_start_entry_commits() {
    let mut group = Group::new();
    let committed = group.nodes[1].propose_fast(b"approved".to_vec()).unwrap();
    group.carry(|message| message.msg_type == FAST_PROPOSE);
    group.carry(|message| message.msg_type == FAST_VOTE);
    assert_eq!(group.nodes[0].status().committed_index, committed);
    assert!(group.nodes[1].status().committed_index < committed);
    // The leader leaves: nothing more of it arrives, and its members' lease on it runs out.
    group.drain();
    group.net.clear();
    for _ in 0..2 * fast(2).election_tick {
        group.nodes[1].tick().unwrap();
        group.nodes[2].tick().unwrap();
    }
    // What the ticks began is dropped, so that member 2 alone asks, at a term no one has voted in.
    group.drain();
    group.net.clear();
    group.nodes[1].campaign().unwrap();
    let mut became = false;
    for _ in 0..1_000 {
        group.drain();
        let at = group
            .net
            .iter()
            .position(|message| message.from != 1 && message.to != 1);
        let Some(at) = at else { break };
        let message = group.net.remove(at);
        group.deliver(message);
        // Member 3, a follower, resolves a former leader's proposals only past the same entry
        // (focal-ledger's `settle`).
        let follower = &group.nodes[2];
        if follower.status().role == StateRole::Follower && follower.has_committed_current_term() {
            let start =
                term_start(&group, 2).expect("the follower's term-start entry is in its log");
            let commit = follower.status().committed_index;
            assert!(
                commit >= start,
                "follower ready at {commit}, its term began at {start}"
            );
        }
        let node = &group.nodes[1];
        if node.status().role != StateRole::Leader {
            continue;
        }
        became = true;
        let start = term_start(&group, 1);
        let commit = node.status().committed_index;
        if node.has_committed_current_term() {
            let start = start.expect("a ready leader's term-start entry is in its log");
            assert!(
                commit >= start,
                "ready at {commit}, its term began at {start}"
            );
        }
    }
    assert!(became);
    assert!(group.nodes[1].status().committed_index > committed);
    assert!(group.nodes[1].has_committed_current_term());
    assert!(group.nodes[2].has_committed_current_term());
    // What the term-1 fast quorum committed is in every log under the new term, before the entry
    // the term began with. An entry of the new term published there is no proof the term began:
    // focal-ledger's `settle`, which resolves a former leader's proposals by it, waits for the
    // term-start entry (hyper-raft S-4).
    let start = term_start(&group, 2).expect("the follower's term-start entry");
    assert!(committed < start);
    for node in [1, 2] {
        let term = group.nodes[node].status().term;
        assert_eq!(group.nodes[node].published_term(committed).unwrap(), term);
        assert!(!group.nodes[node].term_began_by(committed).unwrap());
        assert!(group.nodes[node].term_began_by(start).unwrap());
    }
}

/// A group of three on the classic track.
fn classic(id: u64) -> NodeConfig {
    let mut config = config(id);
    config.voters = vec![1, 2, 3];
    config
}

/// A member restarted under loss catches up past the hole the loss left. Member 3 misses the
/// appends of a first batch; the leader's appends of a second, ahead of that hole, are refused
/// (focal's members keep nothing ahead of a hole, `Ahead::Refused`); member 3 restarts, and what
/// was on its way to it is lost. The leader, told nothing of the restart, still reaches it: member 3
/// applies everything committed, in order, as the others did.
#[test]
fn a_member_restarted_under_loss_catches_up_past_its_hole() {
    let mut group = Group::with(classic);
    let to_three = |message: &Message| message.to == 3 || message.from == 3;
    for n in 0..4u8 {
        group.nodes[0].propose(vec![b'a', n]).unwrap();
    }
    // The first batch reaches member 2 alone.
    group.carry(|message| !to_three(message));
    group.net.retain(|message| !to_three(message));
    let first = group.nodes[0].status().committed_index;
    assert!(group.nodes[2].status().committed_index < first);
    // The second batch's appends reach member 3, ahead of its hole, and are refused.
    for n in 0..4u8 {
        group.nodes[0].propose(vec![b'b', n]).unwrap();
    }
    group.drain();
    let ahead: Vec<Message> = group
        .net
        .iter()
        .filter(|message| {
            message.to == 3
                && message.msg_type == MessageType::MsgAppend
                && message
                    .entries
                    .first()
                    .is_some_and(|entry| entry.index > first)
        })
        .cloned()
        .collect();
    assert!(
        !ahead.is_empty(),
        "an append ahead of member 3's hole was sent"
    );
    for message in ahead {
        group.deliver(message);
    }
    // Member 3 restarts; everything on its way to or from it is lost.
    group.drain();
    group.net.retain(|message| !to_three(message));
    group.reopen(2);
    group.applied[2].clear();
    // From here the network carries everything; the leader's heartbeats find member 3.
    for _ in 0..4 * classic(1).heartbeat_tick {
        group.nodes[0].tick().unwrap();
        group.settle();
    }
    let committed = group.nodes[0].status().committed_index;
    assert_eq!(group.nodes[2].status().committed_index, committed);
    assert_eq!(group.nodes[2].status().applied_index, committed);
    let tail = |applied: &Vec<Vec<u8>>| {
        applied
            .iter()
            .filter(|data| data.first().is_some_and(|b| *b == b'a' || *b == b'b'))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(tail(&group.applied[0]).len(), 8);
    assert_eq!(tail(&group.applied[2]), tail(&group.applied[0]));
}
