//! What the core refuses, and the bounds it keeps. What it does in a group
//! is tested against `raft-rs` and under schedules (`tests/`).
use crate::{
    Config, Error, Limits, RawNode, StateRole, StorageError,
    log::tests::{Memory, entry, snapshot},
    node::Ready,
    proto::{
        self, ConfChangeSingle, ConfChangeV2, Entry, EntryType, HardState, Message, MessageType,
        Snapshot,
    },
};

fn config(id: u64) -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 2,
        check_quorum: true,
        pre_vote: true,
        max_size_per_msg: 1 << 20,
        ..Config::new(id)
    }
}
/// Persists and applies what there is, and gives the messages.
fn drain(node: &mut RawNode<Memory>) -> Vec<Message> {
    let mut messages = Vec::new();
    while node.has_ready() {
        let mut ready: Ready = node.ready().unwrap();
        if let Some(snapshot) = ready.snapshot() {
            let snapshot = snapshot.clone();
            node.store_mut().install(snapshot);
        }
        let entries = ready.entries().to_vec();
        node.store_mut().append(&entries);
        if let Some(hard) = ready.hard_state() {
            let hard = hard.clone();
            node.store_mut().hard_state = hard;
        }
        messages.extend(ready.take_messages());
        messages.extend(ready.take_persisted_messages());
        let mut applied = ready.committed_entries().last().map(|entry| entry.index);
        let mut light = node.advance_append(ready).unwrap();
        messages.extend(light.take_messages());
        applied = light
            .committed_entries()
            .last()
            .map(|entry| entry.index)
            .or(applied);
        if let Some(applied) = applied {
            node.advance_apply_to(applied).unwrap();
        }
    }
    messages
}
fn answer(kind: MessageType, from: u64, to: u64, term: u64) -> Message {
    Message {
        msg_type: kind as i32,
        from,
        to,
        term,
        ..Message::default()
    }
}
/// Member 1 of three, elected with the vote of member 2.
fn leader_with(config: Config) -> RawNode<Memory> {
    let mut node = RawNode::new(&config, Memory::with_voters(&[1, 2, 3])).unwrap();
    node.campaign().unwrap();
    let asked = drain(&mut node);
    assert!(asked.iter().all(|message| {
        proto::message_type(message) == Some(MessageType::MsgRequestPreVote) && message.term == 1
    }));
    node.step(answer(MessageType::MsgRequestPreVoteResponse, 2, 1, 1))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Candidate);
    drain(&mut node);
    node.step(answer(MessageType::MsgRequestVoteResponse, 2, 1, 1))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Leader);
    drain(&mut node);
    node
}
fn leader() -> RawNode<Memory> {
    leader_with(config(1))
}
fn follower() -> RawNode<Memory> {
    let mut store = Memory::with_voters(&[1, 2, 3]);
    store.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]);
    store.hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 2,
    };
    RawNode::new(&config(2), store).unwrap()
}
fn unchanged(node: &RawNode<Memory>, hard: &HardState, last: u64) {
    assert_eq!(&node.raft.hard_state(), hard);
    assert_eq!(node.raft.log().last_index().unwrap(), last);
    assert!(node.raft.messages().is_empty());
}

#[test]
fn settings_that_cannot_hold_are_refused() {
    for (change, _why) in [
        (
            Box::new(|config: &mut Config| config.id = 0) as Box<dyn Fn(&mut Config)>,
            "identity",
        ),
        (Box::new(|config| config.heartbeat_tick = 0), "heartbeat"),
        (Box::new(|config| config.election_tick = 2), "election"),
        (Box::new(|config| config.max_inflight_msgs = 0), "window"),
        (
            Box::new(|config| config.max_uncommitted_size = 1),
            "uncommitted",
        ),
        (Box::new(|config| config.limits.pending_reads = 0), "reads"),
    ] {
        let mut config = config(1);
        change(&mut config);
        assert!(matches!(
            RawNode::new(&config, Memory::with_voters(&[1])),
            Err(Error::Settings(_))
        ));
    }
    // What storage holds must add up.
    let mut store = Memory::with_voters(&[1]);
    store.hard_state.commit = 5;
    assert!(matches!(
        RawNode::new(&config(1), store),
        Err(Error::Invariant(_))
    ));
    assert!(matches!(
        RawNode::new(&config(1), Memory::with_voters(&[])),
        Err(Error::Configuration(_))
    ));
}

#[test]
fn what_a_peer_may_not_say_is_refused_and_changes_nothing() {
    let mut node = follower();
    let hard = node.raft.hard_state();
    // A commit the log does not reach.
    let mut heartbeat = answer(MessageType::MsgHeartbeat, 1, 2, 1);
    heartbeat.commit = 9;
    assert_eq!(
        node.step(heartbeat),
        Err(Error::Violation("a commit beyond the log"))
    );
    unchanged(&node, &hard, 3);
    // What would replace a committed entry follows an index behind the
    // commit: it is out of date, and answered with the commit.
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 1;
    append.log_term = 1;
    append.entries = vec![entry(2, 9)];
    node.step(append).unwrap();
    let answered = drain(&mut node);
    assert_eq!(answered.len(), 1);
    assert_eq!((answered[0].index, answered[0].reject), (2, false));
    assert_eq!(crate::raft::held(&node.raft), vec![(1, 1), (2, 1), (3, 1)]);
    unchanged(&node, &hard, 3);
    // A snapshot that names no configuration, and one that names none
    // that can be.
    for conf in [None, Some(vec![])] {
        let mut sent = answer(MessageType::MsgSnapshot, 1, 2, 1);
        let mut stated: Snapshot = snapshot(7, 1, &[]);
        if let Some(metadata) = stated.metadata.as_mut() {
            metadata.conf_state = conf.map(|voters| proto::ConfState {
                voters,
                ..Default::default()
            });
        }
        sent.snapshot = Some(stated);
        assert!(matches!(node.step(sent), Err(Error::Violation(_))));
        unchanged(&node, &hard, 3);
    }
    // What cannot be counted.
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = u64::MAX;
    assert!(matches!(node.step(append), Err(Error::Violation(_))));
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 3;
    append.log_term = 1;
    append.entries = vec![entry(u64::MAX, 1)];
    assert!(matches!(node.step(append), Err(Error::Violation(_))));
    let unknown = Message {
        msg_type: 77,
        from: 1,
        to: 2,
        term: 1,
        ..Message::default()
    };
    assert!(matches!(node.step(unknown), Err(Error::Violation(_))));
    unchanged(&node, &hard, 3);
    // The member goes on.
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 3;
    append.log_term = 1;
    append.commit = 3;
    append.entries = vec![entry(4, 1)];
    node.step(append).unwrap();
    assert_eq!(node.raft.log().committed(), 3);
    assert_eq!(node.raft.log().last_index().unwrap(), 4);
}

#[test]
fn what_is_not_of_the_network_is_refused() {
    let mut node = leader();
    for kind in [
        MessageType::MsgHup,
        MessageType::MsgBeat,
        MessageType::MsgUnreachable,
        MessageType::MsgSnapStatus,
        MessageType::MsgCheckQuorum,
    ] {
        assert_eq!(
            node.step(answer(kind, 2, 1, 0)),
            Err(Error::StepLocalMessage)
        );
    }
    for kind in [
        MessageType::MsgAppendResponse,
        MessageType::MsgHeartbeatResponse,
        MessageType::MsgRequestVoteResponse,
        MessageType::MsgRequestPreVoteResponse,
    ] {
        assert_eq!(
            node.step(answer(kind, 9, 1, 1)),
            Err(Error::StepPeerNotFound)
        );
    }
    assert_eq!(node.raft.state(), StateRole::Leader);
}

#[test]
fn only_a_voter_campaigns() {
    let mut store = Memory::with_voters(&[1, 2]);
    store.configuration.learners = vec![3];
    let mut learner = RawNode::new(&config(3), store).unwrap();
    assert_eq!(learner.campaign(), Err(Error::NotPromotable));
    let mut stranger = RawNode::new(&config(4), Memory::with_voters(&[1, 2])).unwrap();
    assert_eq!(stranger.campaign(), Err(Error::NotPromotable));
    for node in [&mut learner, &mut stranger] {
        for _ in 0..100 {
            node.tick().unwrap();
        }
        // Told by a leader to campaign, it does not either.
        node.step(answer(MessageType::MsgTimeoutNow, 1, node.raft.id(), 1))
            .unwrap();
        assert_eq!(node.raft.state(), StateRole::Follower);
        assert!(drain(node).is_empty());
    }
}

#[test]
fn what_waits_to_be_taken_has_a_bound() {
    let mut node = leader_with(Config {
        limits: Limits {
            pending_messages: 6,
            ..Limits::default()
        },
        ..config(1)
    });
    // Nothing is taken: each heartbeat adds two messages.
    let mut pinged = 0;
    while node.ping().is_ok() {
        pinged += 1;
        assert!(pinged < 100, "messages wait without bound");
        if node.raft.messages().len() >= 6 {
            break;
        }
    }
    assert_eq!(
        node.propose(vec![], b"more".to_vec()),
        Err(Error::Capacity("messages that wait to be taken"))
    );
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    assert!(matches!(node.step(append), Err(Error::Capacity(_))));
    let held = node.raft.resident_bytes();
    assert!(!drain(&mut node).is_empty());
    assert!(node.raft.resident_bytes() < held);
    node.propose(vec![], b"more".to_vec()).unwrap();
}

#[test]
fn reads_that_wait_have_a_bound() {
    let mut node = leader_with(Config {
        limits: Limits {
            pending_reads: 3,
            ..Limits::default()
        },
        ..config(1)
    });
    // A leader answers reads once it committed in its term.
    node.read_index(b"early".to_vec()).unwrap();
    assert_eq!(node.raft.pending_read_count(), 0);
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    drain(&mut node);
    for read in 0..3u8 {
        node.read_index(vec![read]).unwrap();
    }
    assert!(matches!(node.read_index(vec![9]), Err(Error::Capacity(_))));
    assert_eq!(node.raft.pending_read_count(), 3);
    drain(&mut node);
    // One answer confirms every read asked before it.
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    heartbeat.context = vec![1];
    node.step(heartbeat).unwrap();
    assert_eq!(node.raft.pending_read_count(), 1);
    assert_eq!(node.raft.ready_read_count(), 2);
    // What is confirmed and not taken counts.
    node.read_index(vec![7]).unwrap_err();
    let ready = node.ready().unwrap();
    assert_eq!(
        ready
            .read_states()
            .iter()
            .map(|read| (read.index, read.request_ctx.clone()))
            .collect::<Vec<_>>(),
        vec![(1, vec![0]), (1, vec![1])]
    );
    node.advance(ready).unwrap();
    node.read_index(vec![7]).unwrap();
}

#[test]
fn what_is_not_durable_has_a_bound() {
    let mut store = Memory::with_voters(&[1, 2, 3]);
    store.append(&[entry(1, 1)]);
    store.hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 1,
    };
    let mut node = RawNode::new(
        &Config {
            limits: Limits {
                unstable_entries: 2,
                entries_per_message: 2,
                ..Limits::default()
            },
            ..config(2)
        },
        store,
    )
    .unwrap();
    // Of a message that holds more, what may be held is taken, and the
    // answer names the last entry taken: the leader sends the rest again.
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 1;
    append.log_term = 1;
    append.commit = 4;
    append.entries = vec![entry(2, 1), entry(3, 1), entry(4, 1)];
    node.step(append).unwrap();
    assert_eq!(node.raft.log().last_index().unwrap(), 3);
    assert_eq!(node.raft.log().committed(), 3);
    // Nothing more is held until that is durable.
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 3;
    append.log_term = 1;
    append.entries = vec![entry(4, 1)];
    assert_eq!(
        node.step(append),
        Err(Error::Capacity("entries not yet durable"))
    );
    assert_eq!(node.raft.log().last_index().unwrap(), 3);
    let answered = drain(&mut node);
    assert_eq!(answered.len(), 1);
    assert_eq!((answered[0].index, answered[0].commit), (3, 3));
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 3;
    append.log_term = 1;
    append.entries = vec![entry(4, 1), entry(5, 1)];
    node.step(append).unwrap();
    assert_eq!(node.raft.log().last_index().unwrap(), 5);
}

#[test]
fn a_message_carries_entries_to_a_bound_whatever_their_bytes() {
    let mut node = leader_with(Config {
        limits: Limits {
            entries_per_message: 3,
            unstable_entries: 64,
            ..Limits::default()
        },
        ..config(1)
    });
    assert!(matches!(
        RawNode::new(
            &Config {
                limits: Limits {
                    entries_per_message: 65,
                    unstable_entries: 64,
                    ..Limits::default()
                },
                ..config(1)
            },
            Memory::with_voters(&[1]),
        ),
        Err(Error::Settings(_))
    ));
    for _ in 0..8 {
        node.propose(vec![], b"x".to_vec()).unwrap();
    }
    drain(&mut node);
    // The member answers where it is, and is sent what follows.
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    let sent: Vec<Vec<u64>> = drain(&mut node)
        .iter()
        .filter(|message| message.to == 2 && !message.entries.is_empty())
        .map(|message| message.entries.iter().map(|entry| entry.index).collect())
        .collect();
    assert_eq!(sent, vec![vec![2, 3, 4], vec![5, 6, 7], vec![8, 9]]);
}

#[test]
fn a_leader_holds_uncommitted_what_it_may_and_one_proposal_at_least() {
    let mut node = leader_with(Config {
        max_uncommitted_size: 64,
        max_size_per_msg: 64,
        ..config(1)
    });
    node.propose(vec![], vec![1; 200]).unwrap();
    assert_eq!(node.raft.uncommitted_bytes(), 200);
    assert_eq!(
        node.propose(vec![], vec![2; 1]),
        Err(Error::ProposalDropped)
    );
    drain(&mut node);
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 2;
    node.step(append).unwrap();
    drain(&mut node);
    assert_eq!(node.raft.uncommitted_bytes(), 0);
    node.propose(vec![], vec![2; 40]).unwrap();
    node.propose(vec![], vec![3; 24]).unwrap();
    assert_eq!(
        node.propose(vec![], vec![4; 1]),
        Err(Error::ProposalDropped)
    );
}

#[test]
fn one_ready_is_out_at_a_time() {
    let mut node = leader();
    node.propose(vec![], b"x".to_vec()).unwrap();
    let ready = node.ready().unwrap();
    assert!(node.outstanding());
    assert!(matches!(node.ready(), Err(Error::Invariant(_))));
    assert!(matches!(
        node.propose(vec![], b"y".to_vec()),
        Err(Error::Invariant(_))
    ));
    assert!(matches!(node.tick(), Err(Error::Invariant(_))));
    let entries = ready.entries().to_vec();
    node.store_mut().append(&entries);
    // Another ready is not this one.
    assert!(matches!(
        node.advance_append(Ready::default()),
        Err(Error::Invariant(_))
    ));
    node.advance_append(ready).unwrap();
    assert!(!node.outstanding());
    node.propose(vec![], b"y".to_vec()).unwrap();
    // What is applied is within what is committed.
    assert!(matches!(
        node.advance_apply_to(99),
        Err(Error::Invariant(_))
    ));
}

#[test]
fn a_change_that_cannot_be_read_is_not_proposed() {
    let mut node = leader();
    for change in [
        ConfChangeV2 {
            transition: 9,
            ..Default::default()
        },
        ConfChangeV2 {
            changes: vec![ConfChangeSingle {
                change_type: 9,
                node_id: 4,
            }],
            ..Default::default()
        },
    ] {
        assert!(matches!(
            node.propose_conf_change(vec![], &change),
            Err(Error::Violation(_))
        ));
        assert!(matches!(
            node.apply_conf_change(&change),
            Err(Error::Violation(_))
        ));
    }
    // One that a peer's leader committed all the same is refused where it
    // is applied, and nothing unwinds.
    let garbled = Entry {
        entry_type: EntryType::EntryConfChangeV2 as i32,
        data: vec![0xff; 3],
        ..Entry::default()
    };
    let mut proposal = Message {
        msg_type: MessageType::MsgPropose as i32,
        from: 2,
        to: 1,
        ..Message::default()
    };
    proposal.entries = vec![garbled];
    assert_eq!(node.step(proposal), Err(Error::ProposalDropped));
    assert_eq!(node.raft.log().last_index().unwrap(), 1);
}

#[test]
fn election_timeouts_are_drawn_from_the_seed() {
    let drawn = |seed: u64| {
        let mut node = RawNode::new(
            &Config { seed, ..config(1) },
            Memory::with_voters(&[1, 2, 3]),
        )
        .unwrap();
        let mut timeouts = Vec::new();
        for _ in 0..64 {
            timeouts.push(node.raft.randomized_election_timeout());
            // An election that no one answers is held again.
            for _ in 0..20 {
                node.tick().unwrap();
            }
            drain(&mut node);
            node.step(answer(MessageType::MsgHeartbeat, 2, 1, 50))
                .unwrap();
            drain(&mut node);
        }
        timeouts
    };
    let first = drawn(1);
    assert_eq!(first, drawn(1));
    assert_ne!(first, drawn(2));
    assert!(first.iter().all(|ticks| (10..20).contains(ticks)));
    let distinct: std::collections::BTreeSet<_> = first.iter().collect();
    assert!(distinct.len() >= 8, "{first:?}");
    let mut node = leader();
    assert!(node.raft.set_randomized_election_timeout(9).is_err());
    assert!(node.raft.set_randomized_election_timeout(20).is_err());
    node.raft.set_randomized_election_timeout(19).unwrap();
}

#[test]
fn storage_that_fails_stops_no_one_and_is_said() {
    // A log compacted behind what a member needs, and no snapshot yet.
    let mut node = leader();
    for _ in 0..3 {
        node.propose(vec![], b"x".to_vec()).unwrap();
    }
    drain(&mut node);
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 4;
    node.step(append).unwrap();
    drain(&mut node);
    node.store_mut().compact(3, vec![1, 2, 3]);
    assert_eq!(
        node.raft.log().slice(2, 4, u64::MAX),
        Err(Error::Storage(StorageError::Compacted))
    );
    // The member behind is sent the snapshot.
    let mut refusal = answer(MessageType::MsgAppendResponse, 3, 1, 1);
    refusal.reject = true;
    refusal.index = 0;
    refusal.reject_hint = 0;
    node.step(refusal).unwrap();
    let sent = drain(&mut node);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        proto::message_type(&sent[0]),
        Some(MessageType::MsgSnapshot)
    );
    assert_eq!(
        sent[0].snapshot.as_ref().map(proto::snapshot_index),
        Some(3)
    );
    assert_eq!(crate::raft::held(&node.raft), vec![(4, 1)]);
}

#[test]
fn one_told_to_campaign_while_it_asks_whether_it_could_does() {
    let mut node = follower();
    let mut ticks = 0;
    while node.raft.state() != StateRole::PreCandidate {
        node.tick().unwrap();
        ticks += 1;
        assert!(ticks < 100, "it never asked");
    }
    assert_eq!(node.raft.term(), 1);
    let asked = drain(&mut node);
    assert!(asked.iter().all(
        |message| message.msg_type == MessageType::MsgRequestPreVote as i32
            && message.context.is_empty()
    ));
    // The others hear their leader and refuse; the leader hands over.
    node.step(answer(MessageType::MsgTimeoutNow, 1, 2, 1))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Candidate);
    assert_eq!(node.raft.term(), 2);
    let asked = drain(&mut node);
    assert_eq!(asked.len(), 2);
    for message in &asked {
        assert_eq!(message.msg_type, MessageType::MsgRequestVote as i32);
        assert_eq!(message.term, 2);
        assert_eq!(message.context, proto::CAMPAIGN_TRANSFER);
    }
    // What was answered to the asking before counts for nothing now.
    node.step(answer(MessageType::MsgRequestPreVoteResponse, 3, 2, 2))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Candidate);
    node.step(answer(MessageType::MsgRequestVoteResponse, 3, 2, 2))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Leader);
    // One that asks for votes is past the term of whoever tells it.
    let mut node = follower();
    node.step(answer(MessageType::MsgTimeoutNow, 1, 2, 1))
        .unwrap();
    assert_eq!(
        (node.raft.state(), node.raft.term()),
        (StateRole::Candidate, 2)
    );
    drain(&mut node);
    node.step(answer(MessageType::MsgTimeoutNow, 1, 2, 1))
        .unwrap();
    assert_eq!(
        (node.raft.state(), node.raft.term()),
        (StateRole::Candidate, 2)
    );
    assert!(drain(&mut node).is_empty());
}

#[test]
fn one_told_to_campaign_before_it_applied_a_change_campaigns_once_it_has() {
    use crate::proto::{ConfChangeType, protocompat::PbMessageExt};
    let change = ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::RemoveNode as i32,
            node_id: 1,
        }],
        ..Default::default()
    };
    let removal = Entry {
        entry_type: EntryType::EntryConfChangeV2 as i32,
        index: 4,
        term: 1,
        data: change.write_to_bytes().unwrap(),
        ..Entry::default()
    };
    let told = |heard: Option<MessageType>| {
        let mut node = follower();
        let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
        append.index = 3;
        append.log_term = 1;
        append.commit = 4;
        append.entries = vec![removal.clone()];
        node.step(append).unwrap();
        // Committed, and not applied: the owner has not taken it yet.
        node.step(answer(MessageType::MsgTimeoutNow, 1, 2, 1))
            .unwrap();
        assert_eq!(node.raft.state(), StateRole::Follower);
        if let Some(kind) = heard {
            node.step(answer(kind, 1, 2, 1)).unwrap();
        }
        let mut ready = node.ready().unwrap();
        let entries = ready.entries().to_vec();
        node.store_mut().append(&entries);
        let committed = ready.take_committed_entries();
        assert_eq!(committed.last().map(|entry| entry.index), Some(3));
        let mut light = node.advance_append(ready).unwrap();
        let committed = light.take_committed_entries();
        assert_eq!(committed.len(), 1);
        node.apply_conf_change(&change).unwrap();
        node.advance_apply_to(4).unwrap();
        node
    };
    let mut node = told(None);
    assert_eq!(node.raft.state(), StateRole::Candidate);
    assert_eq!(node.raft.term(), 2);
    let asked = drain(&mut node);
    // The one that left is not asked, and no lease refuses the others.
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].to, 3);
    assert_eq!(asked[0].context, proto::CAMPAIGN_TRANSFER);
    // One that heard of a leader since does not.
    for heard in [MessageType::MsgHeartbeat, MessageType::MsgAppend] {
        let node = told(Some(heard));
        assert_eq!(node.raft.state(), StateRole::Follower);
        assert_eq!(node.raft.term(), 1);
    }
}

/// A follower given patience campaigns only once its patience has passed
/// beyond its election timeout; given none, at its timeout.
#[test]
fn a_follower_waits_its_patience_before_it_campaigns() {
    let mut patient = follower();
    patient.raft.set_patience(7);
    assert_eq!(patient.raft.patience(), 7);
    let timeout = patient.raft.randomized_election_timeout();
    for _ in 0..timeout + 6 {
        patient.tick().unwrap();
    }
    assert_eq!(patient.raft.state(), StateRole::Follower);
    assert!(drain(&mut patient).is_empty());
    patient.tick().unwrap();
    assert_ne!(patient.raft.state(), StateRole::Follower);
    let mut prompt = follower();
    let timeout = prompt.raft.randomized_election_timeout();
    for _ in 0..timeout - 1 {
        prompt.tick().unwrap();
    }
    assert_eq!(prompt.raft.state(), StateRole::Follower);
    prompt.tick().unwrap();
    assert_ne!(prompt.raft.state(), StateRole::Follower);
}

#[test]
fn a_read_asked_by_two_members_under_one_context_answers_both() {
    let mut node = leader();
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    drain(&mut node);
    // Two followers forward a read under the same context: one read waits,
    // with two askers.
    for from in [2u64, 3] {
        let mut asked = answer(MessageType::MsgReadIndex, from, 1, 1);
        asked.entries = vec![Entry {
            data: b"same".to_vec(),
            ..Entry::default()
        }];
        node.step(asked).unwrap();
    }
    assert_eq!(node.raft.pending_read_count(), 1);
    drain(&mut node);
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    heartbeat.context = b"same".to_vec();
    node.step(heartbeat).unwrap();
    // The quorum confirms the read once, and each asker is answered.
    let answers: Vec<(u64, u64, Vec<u8>)> = drain(&mut node)
        .into_iter()
        .filter(|message| proto::message_type(message) == Some(MessageType::MsgReadIndexResp))
        .map(|message| {
            (
                message.to,
                message.index,
                message
                    .entries
                    .first()
                    .map(|entry| entry.data.clone())
                    .unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        answers,
        vec![(2, 1, b"same".to_vec()), (3, 1, b"same".to_vec())]
    );
    assert_eq!(node.raft.pending_read_count(), 0);
}
