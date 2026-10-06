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

/// What these tests state of each member (`Limits::derive`): a message of twice the bytes their
/// appends carry (`max_size_per_msg`, a MiB), groups of at most five members, as the largest here
/// has, queues of four such messages each, and one write out at a time.
fn limits() -> Limits {
    Limits::derive(crate::Stated {
        message: 2 << 20,
        members: 5,
        memory: 8 << 20,
        depth: 1,
    })
    .unwrap()
}
fn config(id: u64) -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 2,
        check_quorum: true,
        pre_vote: true,
        max_size_per_msg: 1 << 20,
        ..Config::new(id, limits())
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
            let hard = *hard;
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
        msg_type: kind,
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
        message.msg_type == MessageType::MsgRequestPreVote && message.term == 1
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
    follower_with(config(2))
}
fn follower_with(config: Config) -> RawNode<Memory> {
    let mut store = Memory::with_voters(&[1, 2, 3]);
    store.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]);
    store.hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 2,
    };
    RawNode::new(&config, store).unwrap()
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
        (Box::new(|config| config.max_inflight_bytes = 0), "bytes"),
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
        sent.snapshot = Some(Box::new(stated));
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
        // Told by a leader to campaign, it does not either, and says why.
        let id = node.raft.id();
        assert_eq!(
            node.step(answer(MessageType::MsgTimeoutNow, 1, id, 1)),
            Err(Error::NotPromotable)
        );
        assert_eq!(node.raft.state(), StateRole::Follower);
        assert!(drain(node).is_empty());
    }
}

/// Every bound is derived from what the member's owner states (`Limits::derive`, mantle note 32
/// §3.8): a message of the stated bytes holds as many entries as the bound says and not one more;
/// a member holds proposals that its vote carries in one such message; each queue holds what the
/// memory holds of its least element; and the members and the writes out are as stated. A
/// statement that admits nothing is refused, deriving or at open.
#[test]
fn every_bound_is_derived_from_what_the_owner_states() {
    use crate::wire::{ENTRY_FIXED_BYTES, MESSAGE_RECORD_FIXED_BYTES, Record};
    let stated = crate::Stated {
        message: 64 << 10,
        members: 5,
        memory: 1 << 20,
        depth: 3,
    };
    let limits = Limits::derive(stated).unwrap();
    // A message of as many entries as the bound, each its fixed bytes alone, is no more than the
    // stated bytes; one more entry passes them.
    let mut append = proto::message(2, MessageType::MsgAppend);
    append.entries = vec![Entry::default(); limits.entries_per_message];
    assert!(append.encoded_len() <= stated.message);
    append.entries.push(Entry::default());
    assert!(append.encoded_len() > stated.message);
    // What a member holds approved by itself, as much as it may, its vote carries in one message.
    let mut held = crate::fast::Proposals::new(limits.proposals, limits.proposal_bytes);
    let mut index = 0;
    // Held until it holds no more: refused for room, never for a place taken.
    while held
        .hold(
            Entry {
                index: index + 1,
                term: 1,
                data: vec![1],
                ..Entry::default()
            },
            true,
            true,
        )
        .is_ok_and(|took| took)
    {
        index += 1;
    }
    assert!(index > 0 && held.len() <= limits.proposals);
    let mut vote = proto::message(2, MessageType::MsgRequestVoteResponse);
    crate::log::copy_entries_of(held.iter(), &mut vote.entries).unwrap();
    assert!(vote.encoded_len() <= stated.message);
    assert_eq!(limits.fast_window, limits.proposals as u64);
    assert_eq!(limits.vote_bytes, stated.members * limits.proposal_bytes);
    assert_eq!(
        limits.proposal_bytes,
        stated.message - MESSAGE_RECORD_FIXED_BYTES
    );
    // Each queue: the memory over its least element.
    assert_eq!(
        limits.unstable_entries,
        stated.memory / std::mem::size_of::<Entry>()
    );
    assert_eq!(
        limits.pending_messages,
        stated.memory / proto::MESSAGE_ALLOWANCE
    );
    assert_eq!(
        limits.pending_reads,
        stated.memory / std::mem::size_of::<crate::read::PendingRead>()
    );
    assert_eq!((limits.readies_in_flight, limits.members), (3, 5));
    // A message that carries no entry derives nothing.
    let none = crate::Stated {
        message: MESSAGE_RECORD_FIXED_BYTES + ENTRY_FIXED_BYTES - 1,
        ..stated
    };
    assert!(matches!(Limits::derive(none), Err(Error::Settings(_))));
    // Memory that holds fewer entries than a message carries, and a group of no member, are
    // refused at open.
    for refused in [
        crate::Stated {
            memory: 64 << 10,
            ..stated
        },
        crate::Stated {
            members: 0,
            ..stated
        },
    ] {
        let config = Config::new(1, Limits::derive(refused).unwrap());
        assert!(matches!(
            RawNode::new(&config, Memory::with_voters(&[1])),
            Err(Error::Settings(_))
        ));
    }
}

/// A leader proposes no change past the members its group's configuration may name
/// (`Limits::members`): the entry keeps its place and states nothing, as a second change does
/// while one waits, and the group goes on as it was; within the bound the change is made.
#[test]
fn a_leader_proposes_no_change_past_the_members_a_configuration_names() {
    use crate::wire::Record;
    for (members, made) in [(3, false), (4, true)] {
        let mut node = leader_with(Config {
            limits: Limits {
                members,
                ..limits()
            },
            ..config(1)
        });
        node.propose_conf_change(
            vec![],
            &single(crate::proto::ConfChangeType::AddLearnerNode, 4),
        )
        .unwrap();
        drain(&mut node);
        let last = node.raft.log().last_index().unwrap();
        let entry = node
            .raft
            .log()
            .entries(last, u64::MAX, 1)
            .unwrap()
            .remove(0);
        let changes = entry.entry_type != EntryType::EntryNormal;
        assert_eq!(changes, made, "members {members}: {entry:?}");
        // The voters commit it; the configuration is as it was, or holds the learner.
        let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
        append.index = last;
        node.step(append).unwrap();
        drain(&mut node);
        assert_eq!(node.raft.log().committed(), last);
        if made {
            let change = ConfChangeV2::decode(&entry.data).unwrap();
            node.apply_conf_change(&change).unwrap();
        }
        assert_eq!(node.raft.tracker().len(), 3 + usize::from(made));
    }
}

#[test]
fn what_waits_to_be_taken_has_a_bound() {
    let mut node = leader_with(Config {
        limits: Limits {
            pending_messages: 6,
            ..limits()
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
            ..limits()
        },
        ..config(1)
    });
    // A read asked before the leader committed in its term waits for that commit (it was
    // dropped), and counts against the bound as it waits.
    node.read_index(b"early".to_vec()).unwrap();
    assert_eq!(node.raft.deferred_read_count(), 1);
    assert_eq!(node.raft.pending_read_count(), 0);
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    // Released at the commit, it waits for its quorum.
    assert_eq!(node.raft.deferred_read_count(), 0);
    assert_eq!(node.raft.pending_read_count(), 1);
    drain(&mut node);
    for read in 0..2u8 {
        node.read_index(vec![read]).unwrap();
    }
    assert!(matches!(node.read_index(vec![9]), Err(Error::Capacity(_))));
    assert_eq!(node.raft.pending_read_count(), 3);
    drain(&mut node);
    // One answer confirms every read asked before it.
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    heartbeat.context = round_answer(&[0], 2);
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
        vec![(1, b"early".to_vec()), (1, vec![0])]
    );
    node.advance(ready).unwrap();
    node.read_index(vec![7]).unwrap();
}

/// A read asked of a leader that has not committed an entry of its term waits for that commit
/// (the thesis's §6.4 step 1; etcd's `pendingReadIndexMessages`), where it was dropped and its
/// owner left to its deadline: once the term's first entry commits, the read leaves in a round
/// and is answered as any other.
#[test]
fn a_new_leaders_read_waits_for_its_terms_first_commit() {
    let mut node = leader();
    node.read_index(b"early".to_vec()).unwrap();
    assert_eq!(node.raft.deferred_read_count(), 1);
    assert_eq!(node.raft.pending_read_count(), 0);
    assert!(rounds(&drain(&mut node)).is_empty());
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    assert_eq!(node.raft.deferred_read_count(), 0);
    assert_eq!(
        rounds(&drain(&mut node)),
        vec![(2, b"early".to_vec()), (3, b"early".to_vec())]
    );
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    heartbeat.context = round_answer(b"early", 1);
    node.step(heartbeat).unwrap();
    assert_eq!(confirmed(&mut node), vec![b"early".to_vec()]);
}

/// A read a follower forwards to a leader that has not committed in its term waits as a local
/// one does, and its asker is answered once the term's first entry commits.
#[test]
fn a_read_a_follower_forwards_waits_for_the_leaders_first_commit() {
    let mut node = leader();
    let mut asked = answer(MessageType::MsgReadIndex, 2, 1, 1);
    asked.entries = vec![Entry {
        data: b"forwarded".to_vec(),
        ..Entry::default()
    }];
    node.step(asked).unwrap();
    assert_eq!(node.raft.deferred_read_count(), 1);
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    assert_eq!(
        rounds(&drain(&mut node)),
        vec![(2, b"forwarded".to_vec()), (3, b"forwarded".to_vec())]
    );
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 3, 1, 1);
    heartbeat.context = round_answer(b"forwarded", 1);
    node.step(heartbeat).unwrap();
    let answers: Vec<(u64, u64, Vec<u8>)> = drain(&mut node)
        .into_iter()
        .filter(|message| message.msg_type == MessageType::MsgReadIndexResp)
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
    assert_eq!(answers, vec![(2, 1, b"forwarded".to_vec())]);
}

/// Reads that wait for the term's first commit count against the bound of reads, and one past it
/// is refused.
#[test]
fn reads_that_wait_for_the_first_commit_count_against_the_bound() {
    let mut node = leader_with(Config {
        limits: Limits {
            pending_reads: 2,
            ..limits()
        },
        ..config(1)
    });
    node.read_index(vec![0]).unwrap();
    node.read_index(vec![1]).unwrap();
    assert!(matches!(node.read_index(vec![2]), Err(Error::Capacity(_))));
    assert_eq!(node.raft.deferred_read_count(), 2);
}

/// A leader deposed before its term's first commit lets the reads that waited for it go: they
/// were its term's, and their owner sees the term change.
#[test]
fn a_leader_deposed_before_its_first_commit_lets_its_waiting_reads_go() {
    let mut node = leader();
    node.read_index(b"early".to_vec()).unwrap();
    assert_eq!(node.raft.deferred_read_count(), 1);
    node.step(answer(MessageType::MsgHeartbeat, 2, 1, 2))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Follower);
    assert_eq!(node.raft.deferred_read_count(), 0);
    assert!(rounds(&drain(&mut node)).is_empty());
}

/// A read asked of a member with no leader to ask is refused, where an `Ok` left its owner to a
/// deadline: a follower that heard no leader, and a candidate. One a peer forwarded is dropped,
/// and that peer's owner answers its own.
#[test]
fn a_read_with_no_leader_to_ask_is_refused() {
    let mut node = follower();
    assert!(matches!(
        node.read_index(b"nowhere".to_vec()),
        Err(Error::ReadDropped)
    ));
    let mut forwarded = answer(MessageType::MsgReadIndex, 3, 2, 1);
    forwarded.entries = vec![Entry {
        data: b"forwarded".to_vec(),
        ..Entry::default()
    }];
    node.step(forwarded).unwrap();
    assert!(drain(&mut node).is_empty());
    node.campaign().unwrap();
    assert_ne!(node.raft.state(), StateRole::Follower);
    assert!(matches!(
        node.read_index(b"nowhere".to_vec()),
        Err(Error::ReadDropped)
    ));
}

/// A read, or a read's answer, without its one context contradicts what a read is, and is
/// refused.
#[test]
fn a_read_without_its_context_is_refused() {
    let mut node = reading_leader();
    assert!(matches!(
        node.step(answer(MessageType::MsgReadIndex, 2, 1, 1)),
        Err(Error::Violation(_))
    ));
    assert_eq!(node.raft.pending_read_count(), 0);
    let mut node = follower();
    assert!(matches!(
        node.step(answer(MessageType::MsgReadIndexResp, 1, 2, 1)),
        Err(Error::Violation(_))
    ));
    assert_eq!(node.raft.ready_read_count(), 0);
}

/// A leader of three that committed in its term: it answers reads.
fn reading_leader() -> RawNode<Memory> {
    committed_leader(config(1))
}
fn committed_leader(config: Config) -> RawNode<Memory> {
    let mut node = leader_with(config);
    let mut append = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    append.index = 1;
    node.step(append).unwrap();
    drain(&mut node);
    node
}
/// The rounds in `messages`: to whom each heartbeat went, and the read it asks for (a round's
/// heartbeat carries the last read's context and the round's number, `ReadOnly::round_context`).
fn rounds(messages: &[Message]) -> Vec<(u64, Vec<u8>)> {
    messages
        .iter()
        .filter(|message| message.msg_type == MessageType::MsgHeartbeat)
        .map(|message| {
            let read = crate::read::ReadOnly::of_round(&message.context)
                .map_or_else(Vec::new, |(context, _)| context.to_vec());
            (message.to, read)
        })
        .collect()
}
/// What an answer to round `round`, which asked for the read `context`, carries back.
fn round_answer(context: &[u8], round: u64) -> Vec<u8> {
    crate::read::ReadOnly::round_context(context, round).unwrap()
}
fn confirmed(node: &mut RawNode<Memory>) -> Vec<Vec<u8>> {
    let mut ready = node.ready().unwrap();
    let reads = ready
        .take_read_states()
        .into_iter()
        .map(|read| read.request_ctx)
        .collect();
    node.advance(ready).unwrap();
    reads
}

/// Twenty reads asked before the member is next asked what there is to do
/// leave in one round — a heartbeat to each of the two other members, with
/// the context of the last — and one member's answer to it confirms all
/// twenty. A round for each was forty heartbeats, of which the answers to
/// the last two did the same.
#[test]
fn reads_asked_together_leave_in_one_round_and_one_answer_confirms_them() {
    let mut node = reading_leader();
    for read in 0..20u8 {
        node.read_index(vec![read]).unwrap();
        // Nothing is sent as a read is asked.
        assert!(node.raft.messages().is_empty());
    }
    assert!(node.has_ready());
    assert_eq!(
        rounds(&drain(&mut node)),
        vec![(2, vec![19]), (3, vec![19])]
    );
    // The round is out: there is nothing more to do for the reads.
    assert!(!node.has_ready());
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    heartbeat.context = round_answer(&[19], 1);
    node.step(heartbeat).unwrap();
    assert_eq!(node.raft.pending_read_count(), 0);
    assert_eq!(
        confirmed(&mut node),
        (0..20u8).map(|read| vec![read]).collect::<Vec<_>>()
    );
    // A read asked alone leaves with the next `Ready`, and no later.
    node.read_index(b"alone".to_vec()).unwrap();
    assert_eq!(
        rounds(&drain(&mut node)),
        vec![(2, b"alone".to_vec()), (3, b"alone".to_vec())]
    );
}

/// A read asked after a round was sent is not confirmed by that round: the
/// heartbeats left before the read was asked, and say nothing of who led
/// when it was. It is asked for by the next round, which leaves with the
/// `Ready` that follows.
#[test]
fn a_read_asked_after_a_round_left_is_asked_for_by_the_next() {
    let mut node = reading_leader();
    node.read_index(b"first".to_vec()).unwrap();
    assert_eq!(rounds(&drain(&mut node)).len(), 2);
    node.read_index(b"second".to_vec()).unwrap();
    node.read_index(b"third".to_vec()).unwrap();
    // The answer to the first round arrives before the next round left.
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    heartbeat.context = round_answer(b"first", 1);
    node.step(heartbeat).unwrap();
    assert_eq!(node.raft.pending_read_count(), 2);
    let mut ready = node.ready().unwrap();
    assert_eq!(
        ready
            .take_read_states()
            .into_iter()
            .map(|read| read.request_ctx)
            .collect::<Vec<_>>(),
        vec![b"first".to_vec()]
    );
    // The same `Ready` carries one round for the two asked since.
    assert_eq!(
        rounds(&ready.take_messages()),
        vec![(2, b"third".to_vec()), (3, b"third".to_vec())]
    );
    node.advance(ready).unwrap();
    assert!(!node.has_ready());
    // A late answer to the first round confirms nothing more.
    let mut late = answer(MessageType::MsgHeartbeatResponse, 3, 1, 1);
    late.context = round_answer(b"first", 1);
    node.step(late).unwrap();
    assert_eq!(node.raft.pending_read_count(), 2);
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 3, 1, 1);
    heartbeat.context = round_answer(b"third", 2);
    node.step(heartbeat).unwrap();
    assert_eq!(
        confirmed(&mut node),
        vec![b"second".to_vec(), b"third".to_vec()]
    );
}

/// An answer to a round names the round, not only the read it asked for: an asker may ask a read
/// again under the context an earlier round carried, and a late or repeated answer to that round
/// then named a read asked after it was sent. It confirmed every read before it in the queue, all
/// asked after the round left (hyper-check's swarm, group seed 4,521: a deposed leader answered a
/// read 172 entries below the commit when it was asked).
#[test]
fn a_late_answer_to_a_round_confirms_no_read_asked_after_it_under_the_same_context() {
    let mut node = reading_leader();
    node.read_index(b"again".to_vec()).unwrap();
    assert_eq!(rounds(&drain(&mut node)).len(), 2);
    // Member 3's answer confirms it; member 2's is held back.
    let mut answer3 = answer(MessageType::MsgHeartbeatResponse, 3, 1, 1);
    answer3.context = round_answer(b"again", 1);
    node.step(answer3).unwrap();
    assert_eq!(confirmed(&mut node), vec![b"again".to_vec()]);
    // Another read, then the first asked again under its context: both wait for round 2.
    node.read_index(b"after".to_vec()).unwrap();
    node.read_index(b"again".to_vec()).unwrap();
    assert_eq!(
        rounds(&drain(&mut node)),
        vec![(2, b"again".to_vec()), (3, b"again".to_vec())]
    );
    // Member 2's answer to round 1 arrives late: it was given before either was asked.
    let mut late = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    late.context = round_answer(b"again", 1);
    node.step(late).unwrap();
    assert_eq!(node.raft.pending_read_count(), 2);
    assert!(confirmed(&mut node).is_empty());
    let mut answer2 = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
    answer2.context = round_answer(b"again", 2);
    node.step(answer2).unwrap();
    assert_eq!(
        confirmed(&mut node),
        vec![b"after".to_vec(), b"again".to_vec()]
    );
}

/// A round that was lost is asked again by the leader's own clock: its
/// heartbeat carries the last read asked, and is a round for every read
/// that waits. A leader that is deposed lets its reads go, and has no
/// round left to send.
#[test]
fn a_round_that_was_lost_is_asked_again_by_the_leaders_clock() {
    let mut node = reading_leader();
    node.read_index(b"lost".to_vec()).unwrap();
    assert_eq!(rounds(&drain(&mut node)).len(), 2);
    assert!(!node.has_ready());
    let mut asked_again = Vec::new();
    for _ in 0..2 {
        node.tick().unwrap();
        asked_again.extend(rounds(&drain(&mut node)));
    }
    assert_eq!(
        asked_again,
        vec![(2, b"lost".to_vec()), (3, b"lost".to_vec())]
    );
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 3, 1, 1);
    heartbeat.context = round_answer(b"lost", 1);
    node.step(heartbeat).unwrap();
    assert_eq!(confirmed(&mut node), vec![b"lost".to_vec()]);
    // Deposed with a read asked and its round unsent.
    node.read_index(b"unsent".to_vec()).unwrap();
    assert!(node.has_ready());
    node.step(answer(MessageType::MsgHeartbeat, 2, 1, 2))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Follower);
    assert_eq!(node.raft.pending_read_count(), 0);
    assert!(rounds(&drain(&mut node)).is_empty());
}

/// The leader's appends to `to` among `messages` that carry entries: the last index of each, and
/// what it costs the path, its record (what the window is charged).
fn appends(messages: &[Message], to: u64) -> Vec<(u64, u64)> {
    use crate::wire::Record;
    messages
        .iter()
        .filter(|message| {
            message.to == to
                && message.msg_type == MessageType::MsgAppend
                && !message.entries.is_empty()
        })
        .map(|message| {
            (
                message.entries.last().unwrap().index,
                message.encoded_len() as u64,
            )
        })
        .collect()
}
fn window(node: &RawNode<Memory>, member: u64) -> (usize, u64) {
    let progress = node.raft.tracker().get(member).unwrap();
    (progress.inflights.count(), progress.inflights.bytes())
}

/// A member is sent no more bytes ahead of its answers than the path to it
/// carries, each append charged its record, whatever their sizes (mantle
/// note 32 R16); a member that answers nothing
/// holds none of them back from a majority that does; an entry larger than
/// the bound is sent, alone, and waited for without another being sent; an
/// answer gives back what the messages it answers took, once, in whatever
/// order answers come; and a bound that changes while messages are out is
/// in force for the next.
#[test]
fn a_member_is_sent_no_more_bytes_ahead_of_its_answers_than_its_path_carries() {
    let mut node = committed_leader(Config {
        max_inflight_bytes: 1_000,
        ..config(1)
    });
    // Member 2 answered and is sent ahead of its answers; member 3 has
    // answered nothing and is probed: one message, and no more.
    let sizes = [300usize, 20, 700, 40, 300, 300, 5, 300];
    let mut sent = Vec::new();
    for (round, size) in sizes.iter().enumerate() {
        node.propose(vec![], vec![round as u8; *size]).unwrap();
        let messages = drain(&mut node);
        assert!(appends(&messages, 3).is_empty());
        sent.extend(appends(&messages, 2));
        node.check_accounting().unwrap();
        let (_, bytes) = window(&node, 2);
        // What is out passes the bound by one append at most, of one entry:
        // the page that took the last of the room.
        let most = crate::wire::MESSAGE_RECORD_FIXED_BYTES as u64 + 725;
        assert!(bytes <= 1_000 + most, "{bytes} bytes are out");
    }
    // The window filled by its bytes, with places to spare, and the
    // entries behind it wait.
    let (count, bytes) = window(&node, 2);
    assert_eq!(bytes, sent.iter().map(|(_, bytes)| *bytes).sum::<u64>());
    assert!(
        bytes >= 1_000 && count < 8,
        "{count} messages, {bytes} bytes"
    );
    let last = node.raft.log().last_index().unwrap();
    assert!(sent.last().unwrap().0 < last);
    // An answer out of date, and the same answer twice, give back nothing
    // that was not taken.
    let (first, first_bytes) = sent[0];
    let answered = |index: u64| {
        let mut answer = answer(MessageType::MsgAppendResponse, 2, 1, 1);
        answer.index = index;
        answer
    };
    node.step(answered(1)).unwrap();
    assert_eq!(window(&node, 2).1, bytes);
    node.step(answered(first)).unwrap();
    let more = drain(&mut node);
    node.step(answered(first)).unwrap();
    node.check_accounting().unwrap();
    // What it gave back was sent again, as far as the bound allows.
    let resent: u64 = appends(&more, 2).iter().map(|(_, bytes)| *bytes).sum();
    assert_eq!(window(&node, 2).1, bytes - first_bytes + resent);
    assert!(drain(&mut node).is_empty());
    // The majority of members 1 and 2 commits everything, whatever member
    // 3 does: answers skip ahead and come out of order.
    let mut answers = vec![last];
    for _ in 0..64 {
        let Some(index) = answers.pop() else { break };
        node.step(answered(index)).unwrap();
        for (index, _) in appends(&drain(&mut node), 2) {
            answers.insert(0, index);
        }
        node.check_accounting().unwrap();
    }
    for (index, _) in sent.iter().rev() {
        node.step(answered(*index)).unwrap();
    }
    node.step(answered(last)).unwrap();
    drain(&mut node);
    assert_eq!(node.raft.hard_state().commit, last);
    assert_eq!(window(&node, 2), (0, 0));
    assert_eq!(window(&node, 3), (0, 0));
    // An entry larger than the bound goes alone, and the next waits for
    // its answer: nothing spins, and nothing is sent in vain.
    node.propose(vec![], vec![9; 5_000]).unwrap();
    let large = appends(&drain(&mut node), 2);
    assert_eq!(large.len(), 1);
    assert!(large[0].1 > 5_000);
    node.propose(vec![], b"behind".to_vec()).unwrap();
    assert!(appends(&drain(&mut node), 2).is_empty());
    assert!(!node.has_ready());
    // The path is found to carry more: the bound rises, and the next
    // answer or append sends what waited.
    assert!(node.set_inflight_bytes(2, 1 << 20));
    assert!(!node.set_inflight_bytes(9, 1 << 20));
    node.propose(vec![], b"more".to_vec()).unwrap();
    let after = appends(&drain(&mut node), 2);
    assert_eq!(
        after.last().unwrap().0,
        node.raft.log().last_index().unwrap()
    );
    // And falls below what is out: nothing more until enough is answered.
    assert!(node.set_inflight_bytes(2, 16));
    node.propose(vec![], b"held".to_vec()).unwrap();
    assert!(appends(&drain(&mut node), 2).is_empty());
    node.step(answered(node.raft.log().last_index().unwrap() - 1))
        .unwrap();
    assert_eq!(appends(&drain(&mut node), 2).len(), 1);
    node.check_accounting().unwrap();
}

/// A heartbeat's answer says how far the member's log goes. Where its last
/// entry is of the leader's term the leader made that entry, and the member
/// holds the leader's log through it: the answer is an append's answer for
/// all of it, and the answers to three appends that were lost are made
/// good, exactly. An answer from a member that holds nothing new gives
/// nothing back and sends nothing — the bytes out never pass their bound
/// for a member that answers heartbeats and no append (raft-rs freed the
/// first message and sent the next at every such answer). A member whose
/// window is full and that has answered for none of it through a beat of
/// the leader's ticks is probed, with one message of what its path carries;
/// and a probe is sent again when it is told lost, or once a beat has
/// passed since it was sent, not at every heartbeat's answer.
#[test]
fn a_heartbeats_answer_gives_back_what_the_member_holds_and_nothing_more() {
    let mut node = committed_leader(Config {
        max_inflight_bytes: 1_000,
        ..config(1)
    });
    let mut sent = Vec::new();
    for round in 0..8u8 {
        node.propose(vec![], vec![round; 300]).unwrap();
        sent.extend(appends(&drain(&mut node), 2));
    }
    let (count, bytes) = window(&node, 2);
    assert!(bytes >= 1_000 && count == sent.len());
    let last = node.raft.log().last_index().unwrap();
    let held = |index: u64, term: u64| {
        let mut answer = answer(MessageType::MsgHeartbeatResponse, 2, 1, 1);
        answer.index = index;
        answer.log_term = term;
        answer
    };
    let matched = |node: &RawNode<Memory>| node.raft.tracker().get(2).unwrap().matched;
    let before = matched(&node);
    // The member holds nothing of what is out: nothing is given back and
    // nothing is sent.
    for _ in 0..2 {
        node.step(held(before, 1)).unwrap();
        assert!(appends(&drain(&mut node), 2).is_empty());
        assert_eq!(window(&node, 2), (count, bytes));
    }
    // Its last entry is of another term, or it says nothing of its log (a
    // member of raft-rs): this leader did not make that entry, and it says
    // nothing of what this leader sent.
    node.step(held(last, 9)).unwrap();
    node.step(held(0, 0)).unwrap();
    assert!(appends(&drain(&mut node), 2).is_empty());
    assert_eq!(window(&node, 2), (count, bytes));
    assert_eq!(matched(&node), before);
    // It holds through the second message, and the answers to both were
    // lost: the heartbeat's answer is theirs. What the two took is given
    // back, and what waited is sent as far as the bound allows.
    node.step(held(sent[1].0, 1)).unwrap();
    let more = appends(&drain(&mut node), 2);
    assert!(!more.is_empty());
    assert_eq!(matched(&node), sent[1].0);
    let given_back = sent[0].1 + sent[1].1;
    let resent: u64 = more.iter().map(|(_, bytes)| *bytes).sum();
    assert_eq!(window(&node, 2).1, bytes - given_back + resent);
    assert!(window(&node, 2).1 < 1_000 + 310);
    node.check_accounting().unwrap();
    // The member answers heartbeats and nothing of what is out. While no
    // beat of the leader's ticks has passed since the window filled, that
    // sends nothing; once one has, the member is probed, with one message,
    // cut to what its path carries.
    let fill = |node: &mut RawNode<Memory>| {
        let mut sent = Vec::new();
        for round in 0..8u8 {
            node.propose(vec![], vec![round; 300]).unwrap();
            sent.extend(appends(&drain(node), 2));
        }
        sent
    };
    fill(&mut node);
    assert!(node.raft.tracker().get(2).unwrap().inflights.full());
    for _ in 0..4 {
        node.step(held(sent[1].0, 1)).unwrap();
        assert!(appends(&drain(&mut node), 2).is_empty());
    }
    for _ in 0..2 {
        node.tick().unwrap();
        assert!(appends(&drain(&mut node), 2).is_empty());
    }
    node.step(held(sent[1].0, 1)).unwrap();
    let probe = appends(&drain(&mut node), 2);
    assert_eq!(probe.len(), 1);
    assert!(probe[0].1 <= 1_000 + 310);
    assert_eq!(
        node.raft.tracker().get(2).unwrap().state,
        crate::progress::ProgressState::Probe
    );
    assert_eq!(window(&node, 2), (0, 0));
    // The probe is not sent again at every heartbeat the member answers:
    // only once a beat has passed since it was sent.
    for _ in 0..4 {
        node.step(held(sent[1].0, 1)).unwrap();
        assert!(appends(&drain(&mut node), 2).is_empty());
    }
    for _ in 0..2 {
        node.tick().unwrap();
        assert!(appends(&drain(&mut node), 2).is_empty());
    }
    node.step(held(sent[1].0, 1)).unwrap();
    assert_eq!(appends(&drain(&mut node), 2), probe);
    for _ in 0..4 {
        node.step(held(sent[1].0, 1)).unwrap();
        assert!(appends(&drain(&mut node), 2).is_empty());
    }
    // It is sent again when its owner is told it was lost.
    node.report_unreachable(2).unwrap();
    node.step(held(sent[1].0, 1)).unwrap();
    assert_eq!(appends(&drain(&mut node), 2), probe);
    // And once it arrives, the member is sent ahead of its answers again.
    node.step(held(probe[0].0, 1)).unwrap();
    assert!(!appends(&drain(&mut node), 2).is_empty());
    assert_eq!(
        node.raft.tracker().get(2).unwrap().state,
        crate::progress::ProgressState::Replicate
    );
    assert_eq!(matched(&node), probe[0].0);
    node.check_accounting().unwrap();
    // A member that does not lead says how far its log goes.
    let mut member = follower();
    let mut heartbeat = answer(MessageType::MsgHeartbeat, 1, 2, 1);
    heartbeat.commit = 2;
    member.step(heartbeat).unwrap();
    let answers = drain(&mut member);
    let said = answers
        .iter()
        .find(|message| message.msg_type == MessageType::MsgHeartbeatResponse)
        .unwrap();
    assert_eq!((said.index, said.log_term), (3, 1));
    // Under the rule of raft-rs it says nothing, and a full window gives
    // up its first message at every answer.
    let mut bare = committed_leader(Config {
        max_inflight_bytes: 1_000,
        heartbeat_answers: crate::HeartbeatAnswers::Bare,
        ..config(1)
    });
    for round in 0..8u8 {
        bare.propose(vec![], vec![round; 300]).unwrap();
        drain(&mut bare);
    }
    let (count, _) = window(&bare, 2);
    bare.step(answer(MessageType::MsgHeartbeatResponse, 2, 1, 1))
        .unwrap();
    assert_eq!(appends(&drain(&mut bare), 2).len(), 1);
    assert_eq!(window(&bare, 2).0, count);
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
                ..limits()
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
            ..limits()
        },
        ..config(1)
    });
    assert!(matches!(
        RawNode::new(
            &Config {
                limits: Limits {
                    entries_per_message: 65,
                    unstable_entries: 64,
                    ..limits()
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
        Err(Error::ProposalDropped(crate::Dropped::Uncommitted))
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
        Err(Error::ProposalDropped(crate::Dropped::Uncommitted))
    );
}

#[test]
fn one_ready_is_taken_at_a_time() {
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

/// The indexes and terms of entries.
fn held(entries: &[Entry]) -> Vec<(u64, u64)> {
    entries
        .iter()
        .map(|entry| (entry.index, entry.term))
        .collect()
}
/// The last index of each append with entries to `to`.
fn sent(messages: &[Message], to: u64) -> Vec<u64> {
    appends(messages, to)
        .iter()
        .map(|(last, _)| *last)
        .collect()
}
fn with_depth(id: u64, depth: usize) -> Config {
    let mut config = config(id);
    config.limits.readies_in_flight = depth;
    config
}

/// R-4: a `Ready` is taken while earlier writes are out, gives only what no
/// earlier one gave, and the leader counts itself toward a commit only once
/// its own write is durable (I3), though its followers may commit without it
/// (thesis §10.2.1).
#[test]
fn readies_are_taken_while_writes_are_out() {
    let mut node = leader_with(with_depth(1, 2));
    assert_eq!(node.raft.log().last_index().unwrap(), 1);
    node.propose(vec![], b"a".to_vec()).unwrap();
    let first = node.ready().unwrap();
    assert_eq!(held(first.entries()), vec![(2, 1)]);
    // The leader's term is durable: what it sends leaves at once.
    assert!(first.persisted_messages().is_empty());
    let written = first.entries().to_vec();
    node.advance_issued(first).unwrap();
    assert_eq!(node.in_flight(), 1);
    // The member takes operations while the write is out.
    node.propose(vec![], b"b".to_vec()).unwrap();
    node.tick().unwrap();
    let mut second = node.ready().unwrap();
    assert_eq!(held(second.entries()), vec![(3, 1)]);
    second.take_messages();
    let written_too = second.entries().to_vec();
    let number = second.number();
    node.advance_issued(second).unwrap();
    // A third is refused at the bound, and nothing changed.
    node.propose(vec![], b"c".to_vec()).unwrap();
    let before = node.raft.log().last_index().unwrap();
    assert_eq!(node.ready(), Err(Error::Capacity("readies in flight")));
    assert_eq!(node.raft.log().last_index().unwrap(), before);
    // Its own progress is what is durable.
    assert_eq!(node.raft.tracker().get(1).unwrap().matched, 1);
    let mut acked = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    acked.index = 3;
    node.step(acked).unwrap();
    // The leader and member 2 hold the first entry durably; no majority
    // durably holds the rest.
    assert_eq!(node.raft.log().committed(), 1);
    let mut acked = answer(MessageType::MsgAppendResponse, 3, 1, 1);
    acked.index = 3;
    node.step(acked).unwrap();
    // Two followers are a majority without it.
    assert_eq!(node.raft.log().committed(), 3);
    // Committed, and given to apply only once durable here (I4).
    assert!(!node.raft.log().has_next_entries_since(1).unwrap());
    node.store_mut().append(&written);
    node.store_mut().append(&written_too);
    let light = node.on_persist(number).unwrap();
    assert_eq!(node.in_flight(), 0);
    assert_eq!(node.raft.tracker().get(1).unwrap().matched, 3);
    assert_eq!(
        held(light.committed_entries()),
        vec![(1, 1), (2, 1), (3, 1)]
    );
    // The next gives what the refused one would have.
    let ready = node.ready().unwrap();
    assert_eq!(held(ready.entries()), vec![(4, 1)]);
}

/// etcd's ABA (`newStorageAppendRespMsg`): a write taken in one term is
/// durable only after another leader's entries replaced its own and a third
/// leader's put them back. Its notice, heard in a later term, makes nothing
/// durable: the write of the entries in between is still out and will
/// replace them on disk. The notice of the last write, of this term, does.
#[test]
fn a_notice_heard_in_a_later_term_makes_nothing_durable() {
    let mut store = Memory::with_voters(&[1, 2, 3]);
    store.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]);
    store.hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 2,
    };
    let mut node = RawNode::new(&with_depth(2, 3), store).unwrap();
    let issue = |node: &mut RawNode<Memory>, from: u64, term: u64, entry_term: u64| {
        let mut append = answer(MessageType::MsgAppend, from, 2, term);
        append.index = 3;
        append.log_term = 1;
        append.commit = 2;
        append.entries = vec![entry(4, entry_term)];
        node.step(append).unwrap();
        let mut ready = node.ready().unwrap();
        assert_eq!(held(ready.entries()), vec![(4, entry_term)]);
        // A follower's answers leave once the write is durable.
        assert!(ready.messages().is_empty());
        assert!(!ready.take_persisted_messages().is_empty());
        let written = ready.entries().to_vec();
        node.advance_issued(ready).unwrap();
        written
    };
    let first = issue(&mut node, 1, 2, 2);
    let second = issue(&mut node, 3, 3, 3);
    let third = issue(&mut node, 1, 4, 2);
    assert_eq!(node.raft.term(), 4);
    // The first write is durable: the log holds (4, 2) again, but the
    // second, still out, will replace it on disk.
    node.store_mut().append(&first);
    node.on_persist(1).unwrap();
    assert_eq!(node.raft.log().persisted(), 3);
    assert_eq!(node.raft.log().unstable().entries().len(), 1);
    node.store_mut().append(&second);
    node.on_persist(2).unwrap();
    assert_eq!(node.raft.log().persisted(), 3);
    node.store_mut().append(&third);
    node.on_persist(3).unwrap();
    assert_eq!(node.raft.log().persisted(), 4);
    assert!(node.raft.log().unstable().entries().is_empty());
}

/// I2: what a notice makes for a member that does not lead leaves with it
/// only when nothing is out or unwritten; otherwise it waits for the next
/// `Ready`, whose write holds what it says.
#[test]
fn a_followers_answer_waits_for_the_write_that_holds_what_it_says() {
    let mut node = follower_with(with_depth(2, 2));
    let mut taken = Vec::new();
    for index in [4, 5] {
        let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
        append.index = index - 1;
        append.log_term = 1;
        append.commit = 2;
        append.entries = vec![entry(index, 1)];
        node.step(append).unwrap();
        let mut ready = node.ready().unwrap();
        ready.take_persisted_messages();
        taken.push(ready.entries().to_vec());
        node.advance_issued(ready).unwrap();
    }
    // A heartbeat's answer says how far the log goes: to 5, not durable.
    let mut beat = answer(MessageType::MsgHeartbeat, 1, 2, 1);
    beat.commit = 2;
    node.step(beat).unwrap();
    node.store_mut().append(&taken[0]);
    let light = node.on_persist(1).unwrap();
    assert!(light.messages().is_empty());
    assert_eq!(node.raft.messages().len(), 1);
    node.store_mut().append(&taken[1]);
    let light = node.on_persist(2).unwrap();
    assert_eq!(light.messages().len(), 1);
    assert_eq!(light.messages()[0].index, 5);
}

/// I1: a leader whose term and vote are not durable sends nothing at once,
/// even to a learner it replicates to; once they are, it does.
#[test]
fn a_leader_sends_nothing_at_once_before_its_term_is_durable() {
    let mut store = Memory::with_voters(&[1]);
    store.configuration.learners = vec![2];
    let mut node = RawNode::new(&with_depth(1, 2), store).unwrap();
    node.campaign().unwrap();
    assert_eq!(node.raft.state(), StateRole::Leader);
    let mut ready = node.ready().unwrap();
    assert_eq!(ready.hard_state().map(|hard| hard.term), Some(1));
    assert!(ready.messages().is_empty());
    assert_eq!(sent(&ready.take_persisted_messages(), 2), vec![1]);
    let written = ready.entries().to_vec();
    node.advance_issued(ready).unwrap();
    // The one voter is elected and proposes before its write is durable;
    // it commits nothing it has not made durable (I3).
    node.propose(vec![], b"x".to_vec()).unwrap();
    let mut ready = node.ready().unwrap();
    assert!(ready.messages().is_empty());
    ready.take_persisted_messages();
    let written_too = ready.entries().to_vec();
    node.advance_issued(ready).unwrap();
    assert_eq!(node.raft.log().committed(), 0);
    node.store_mut().append(&written);
    node.store_mut().hard_state.term = 1;
    node.on_persist(1).unwrap();
    assert_eq!(node.raft.log().committed(), 1);
    node.store_mut().append(&written_too);
    node.on_persist(2).unwrap();
    assert_eq!(node.raft.log().committed(), 2);
    let mut acked = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    acked.index = 2;
    node.step(acked).unwrap();
    node.propose(vec![], b"y".to_vec()).unwrap();
    let ready = node.ready().unwrap();
    assert_eq!(sent(ready.messages(), 2), vec![3]);
}

/// The answers to `leader`'s heartbeats among `messages`.
fn beat_answers(messages: &[Message]) -> Vec<u64> {
    messages
        .iter()
        .filter(|message| message.msg_type == MessageType::MsgHeartbeatResponse)
        .map(|message| message.commit)
        .collect()
}
/// A heartbeat of `term` from `from`, telling member 1 of `commit`.
fn beat_from(from: u64, term: u64, commit: u64) -> Message {
    let mut beat = answer(MessageType::MsgHeartbeat, from, 1, term);
    beat.commit = commit;
    beat
}

/// R-6, mantle's case (`1c179e8`): a member commits as leader in
/// `advance_append`, at the notice that its own write is durable, and its
/// owner writes no record of that commit (`LightReady::commit_index` is
/// volatile). It steps down in the same term by check-quorum. The core does
/// not take that commit for durable, and an answer states it only once a
/// write states it: here the write that carries the answer, whose hard
/// state names the next term, states it with it.
#[test]
fn an_answer_states_no_commit_that_no_durable_write_stated() {
    let mut node = leader_with(with_depth(1, 2));
    node.propose(vec![], b"x".to_vec()).unwrap();
    let mut ready = node.ready().unwrap();
    assert_eq!(held(ready.entries()), vec![(2, 1)]);
    let written = ready.entries().to_vec();
    ready.take_messages();
    node.advance_issued(ready).unwrap();
    // Member 2 holds both entries; the leader's write of the second is out.
    let mut acked = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    acked.index = 2;
    node.step(acked).unwrap();
    assert_eq!(node.raft.log().committed(), 1);
    let mut ready = node.ready().unwrap();
    // The commit of 1 rides this write.
    assert_eq!(ready.hard_state().map(|hard| hard.commit), Some(1));
    ready.take_messages();
    node.store_mut().append(&written);
    node.store_mut().hard_state.commit = 1;
    let light = node.advance_append(ready).unwrap();
    // Its own write durable, it commits 2 in `advance_append`: no write
    // states it, and the owner writes none.
    assert_eq!(light.commit_index(), Some(2));
    assert_eq!(node.raft.log().committed(), 2);
    assert_eq!(node.durable_commit(), 1);
    // No quorum heard for two checks: it steps down, in the same term.
    for _ in 0..40 {
        if node.raft.state() != StateRole::Leader {
            break;
        }
        node.tick().unwrap();
    }
    assert_eq!(node.raft.state(), StateRole::Follower);
    assert_eq!(node.raft.term(), 1);
    drain(&mut node);
    assert_eq!(node.durable_commit(), 1);
    assert_eq!(node.store().hard_state.commit, 1);
    // Member 3 leads term 2 and beats with the commit of 2. The answer
    // leaves with the write of the new term, which states 2.
    node.step(beat_from(3, 2, 2)).unwrap();
    let mut ready = node.ready().unwrap();
    let said = beat_answers(&ready.take_persisted_messages());
    let hard = *ready.hard_state().unwrap();
    assert_eq!((hard.term, hard.commit), (2, 2));
    assert_eq!(said, vec![2]);
    node.store_mut().hard_state = hard;
    node.advance_append(ready).unwrap();
    assert_eq!(node.durable_commit(), 2);
    // Every answer states no more than the disk states when it leaves.
    node.step(beat_from(3, 2, 2)).unwrap();
    let said = beat_answers(&drain(&mut node));
    assert_eq!(said, vec![2]);
    assert!(
        said.iter()
            .all(|commit| *commit <= node.store().hard_state.commit)
    );
}

/// R-6: a heartbeat moves a follower's commit while its write is out. The
/// notice of that write releases the answer at once, nothing being left to
/// write, and no `Ready` will state that commit (the notice gives it as
/// `LightReady::commit_index`). The answer states the commit the follower's
/// storage states, not the one the heartbeat moved; once its owner writes
/// that and says so (`RawNode::commit_durable`), its answers state it.
#[test]
fn an_answer_a_notice_releases_states_the_durable_commit() {
    let mut node = follower_with(with_depth(2, 2));
    assert_eq!(node.durable_commit(), 2);
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 3;
    append.log_term = 1;
    append.commit = 2;
    append.entries = vec![entry(4, 1)];
    node.step(append).unwrap();
    let mut ready = node.ready().unwrap();
    let written = ready.entries().to_vec();
    ready.take_persisted_messages();
    node.advance_issued(ready).unwrap();
    let mut beat = answer(MessageType::MsgHeartbeat, 1, 2, 1);
    beat.commit = 3;
    node.step(beat).unwrap();
    assert_eq!(node.raft.log().committed(), 3);
    node.store_mut().append(&written);
    let mut light = node.on_persist(1).unwrap();
    assert_eq!(light.commit_index(), Some(3));
    let sent = light.take_messages();
    assert_eq!(beat_answers(&sent), vec![2]);
    assert_eq!(node.store().hard_state.commit, 2);
    assert!(!node.has_ready());
    // The owner writes the commit and says so.
    node.store_mut().hard_state.commit = 3;
    node.commit_durable(3).unwrap();
    assert_eq!(node.durable_commit(), 3);
    // The durable commit never goes back, and none is beyond the log.
    node.commit_durable(2).unwrap();
    assert_eq!(node.durable_commit(), 3);
    assert!(matches!(node.commit_durable(5), Err(Error::Invariant(_))));
    // A heartbeat that moves nothing: its write states no commit, and its
    // answer states what the owner said is durable.
    let mut beat = answer(MessageType::MsgHeartbeat, 1, 2, 1);
    beat.commit = 3;
    node.step(beat).unwrap();
    let mut ready = node.ready().unwrap();
    assert!(ready.hard_state().is_none());
    assert_eq!(beat_answers(&ready.take_persisted_messages()), vec![3]);
}

/// R-6's apply pause (etcd's `applyingEntsPaused`): while the owner holds
/// what it was given to apply, no `Ready` or notice gives more and
/// everything else goes on; resumed, it is given what follows what it was
/// given.
#[test]
fn an_owner_that_pauses_apply_is_given_nothing_more() {
    let mut node = follower();
    let ready = node.ready().unwrap();
    assert_eq!(held(ready.committed_entries()), vec![(1, 1), (2, 1)]);
    node.pause_apply();
    assert!(node.apply_paused());
    let light = node.advance_append(ready).unwrap();
    assert!(light.committed_entries().is_empty());
    // It applied the first and holds the second.
    node.advance_apply_to(1).unwrap();
    let mut beat = answer(MessageType::MsgHeartbeat, 1, 2, 1);
    beat.commit = 3;
    node.step(beat).unwrap();
    let mut ready = node.ready().unwrap();
    assert!(ready.committed_entries().is_empty());
    assert_eq!(ready.take_persisted_messages().len(), 1);
    let light = node.advance_append(ready).unwrap();
    assert!(light.committed_entries().is_empty());
    assert!(!node.has_ready());
    node.resume_apply();
    assert!(node.has_ready());
    let ready = node.ready().unwrap();
    assert_eq!(held(ready.committed_entries()), vec![(3, 1)]);
    node.advance_append(ready).unwrap();
    node.advance_apply_to(2).unwrap();
    // A snapshot is given while paused, and replaces what the owner holds.
    node.pause_apply();
    let mut sent = answer(MessageType::MsgSnapshot, 1, 2, 1);
    sent.snapshot = Some(Box::new(snapshot(7, 1, &[1, 2, 3])));
    node.step(sent).unwrap();
    let ready = node.ready().unwrap();
    let installed = ready.snapshot().cloned().unwrap();
    assert!(ready.committed_entries().is_empty());
    node.store_mut().install(installed);
    node.advance_append(ready).unwrap();
    assert_eq!(node.given_to_apply(), 7);
    node.resume_apply();
    assert!(!node.has_ready());
}

/// R-6, `docs/durable.md` §4.2: a leader that applies before its own write
/// is durable is given its own term's entries once a majority of followers
/// commits them, copied or where the log holds them; without it, only once
/// its own write is durable (`readies_are_taken_while_writes_are_out`).
#[test]
fn a_leader_applies_its_own_committed_entries_before_its_write_is_durable() {
    for in_place in [false, true] {
        let mut config = with_depth(1, 2);
        config.apply_unpersisted = true;
        let mut node = leader_with(config);
        node.propose(vec![], b"x".to_vec()).unwrap();
        let ready = node.ready().unwrap();
        node.advance_issued(ready).unwrap();
        for member in [2, 3] {
            let mut acked = answer(MessageType::MsgAppendResponse, member, 1, 1);
            acked.index = 2;
            node.step(acked).unwrap();
        }
        assert_eq!(node.raft.log().committed(), 2);
        assert_eq!(node.raft.log().persisted(), 1);
        let ready = if in_place {
            node.ready_in_place().unwrap()
        } else {
            node.ready().unwrap()
        };
        if in_place {
            assert_eq!(ready.committed_range(), Some((1, 2)));
            assert_eq!(crate::Storage::last_index(node.store()).unwrap(), 1);
        } else {
            assert_eq!(held(ready.committed_entries()), vec![(1, 1), (2, 1)]);
        }
        node.advance_issued(ready).unwrap();
        // Not a leader: it waits for its writes again.
        node.step(beat_from(3, 2, 2)).unwrap();
        assert_eq!(node.raft.log().unpersisted_after, u64::MAX);
    }
}

#[test]
fn notices_and_issues_the_member_did_not_give_are_refused() {
    let mut config = with_depth(1, 0);
    assert!(matches!(
        RawNode::new(&config, Memory::with_voters(&[1])),
        Err(Error::Settings(_))
    ));
    config.limits.readies_in_flight = 2;
    let mut node = leader_with(config);
    assert!(matches!(node.on_persist(1), Err(Error::Invariant(_))));
    node.propose(vec![], b"x".to_vec()).unwrap();
    let ready = node.ready().unwrap();
    // Nothing is durable while a ready is taken and not issued.
    assert!(matches!(node.on_persist(0), Err(Error::Invariant(_))));
    assert!(matches!(
        node.advance_issued(Ready::default()),
        Err(Error::Invariant(_))
    ));
    let number = ready.number();
    node.advance_issued(ready).unwrap();
    assert!(matches!(
        node.on_persist(number + 1),
        Err(Error::Invariant(_))
    ));
    assert!(matches!(
        node.on_persist(number - 1),
        Err(Error::Invariant(_))
    ));
}

#[test]
fn a_change_that_cannot_be_read_is_not_proposed() {
    let mut node = leader();
    // One that a peer's leader committed all the same is refused where it
    // is applied, and nothing unwinds.
    let garbled = Entry {
        entry_type: EntryType::EntryConfChangeV2,
        data: vec![0xff; 3],
        ..Entry::default()
    };
    let mut proposal = Message {
        msg_type: MessageType::MsgPropose,
        from: 2,
        to: 1,
        ..Message::default()
    };
    proposal.entries = vec![garbled];
    assert_eq!(
        node.step(proposal),
        Err(Error::ProposalDropped(crate::Dropped::Malformed))
    );
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
    assert_eq!(sent[0].msg_type, MessageType::MsgSnapshot);
    assert_eq!(
        sent[0].snapshot.as_deref().map(proto::snapshot_index),
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
    assert!(
        asked
            .iter()
            .all(|message| message.msg_type == MessageType::MsgRequestPreVote
                && message.context.is_empty())
    );
    // The others hear their leader and refuse; the leader hands over.
    node.step(answer(MessageType::MsgTimeoutNow, 1, 2, 1))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Candidate);
    assert_eq!(node.raft.term(), 2);
    let asked = drain(&mut node);
    assert_eq!(asked.len(), 2);
    for message in &asked {
        assert_eq!(message.msg_type, MessageType::MsgRequestVote);
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

/// A member counts by the newest configuration its log holds, applied or not (`docs/raft.md`
/// §3.5): told to campaign while a change that removed its leader is in its log, committed and
/// not applied, or not committed at all, it campaigns at once and asks only the voters the change
/// left. What its owner is told stays what the owner applied.
#[test]
fn one_told_to_campaign_counts_by_the_change_its_log_holds_applied_or_not() {
    use crate::proto::ConfChangeType;
    use crate::wire::Record;
    let change = ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::RemoveNode,
            node_id: 1,
        }],
        ..Default::default()
    };
    let removal = Entry {
        entry_type: EntryType::EntryConfChangeV2,
        index: 4,
        term: 1,
        data: change.encode_to_vec(),
        ..Entry::default()
    };
    for commit in [3, 4] {
        let mut node = follower();
        let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
        append.index = 3;
        append.log_term = 1;
        append.commit = commit;
        append.entries = vec![removal.clone()];
        node.step(append).unwrap();
        assert_eq!(node.raft.configuration().voters(), [2, 3]);
        assert_eq!(node.raft.applied_configuration().voters(), [1, 2, 3]);
        drain(&mut node);
        node.step(answer(MessageType::MsgTimeoutNow, 1, 2, 1))
            .unwrap();
        assert_eq!(
            (node.raft.state(), node.raft.term()),
            (StateRole::Candidate, 2),
            "commit {commit}"
        );
        let asked = drain(&mut node);
        // The one that left is not asked.
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].to, 3);
        assert_eq!(asked[0].context, proto::CAMPAIGN_TRANSFER);
        // The owner is told what it applies.
        let stated = node.apply_conf_change(&change).unwrap();
        assert_eq!(stated.voters, vec![2, 3]);
        assert_eq!(node.raft.applied_configuration().voters(), [2, 3]);
    }
}

/// A campaign supersedes the vote requests of the member's earlier campaigns that are still
/// waiting to be taken: each asked every voter, and the campaign after it asks each again, so
/// an answer to one the member gave up can win it nothing. A member whose writes stay out for
/// many election timeouts while its owner ticks it, a device stalled, then sends one
/// campaign's requests when a `Ready` takes them again, not one campaign's for every timeout:
/// mantle's range replica on the durable shell sent 234 at once, two for each of 117
/// campaigns, after a hundred of the longest timeouts with three writes out (mantle
/// `docs/design/replica.md` §3).
#[test]
fn a_campaign_supersedes_the_requests_of_those_before_it_still_waiting() {
    let mut node = follower();
    let timeout = node.raft.randomized_election_timeout();
    // No `Ready` is taken through a hundred of the longest timeouts the core draws.
    for _ in 0..100 * 2 * timeout {
        node.tick().unwrap();
    }
    let asked: Vec<(MessageType, u64)> = node
        .raft
        .messages()
        .iter()
        .filter(|m| {
            matches!(
                m.msg_type,
                MessageType::MsgRequestPreVote | MessageType::MsgRequestVote
            )
        })
        .map(|m| (m.msg_type, m.to))
        .collect();
    assert_eq!(
        asked,
        [
            (MessageType::MsgRequestPreVote, 1),
            (MessageType::MsgRequestPreVote, 3)
        ]
    );
    // What waits still adds up to its counter.
    node.raft.msgs.check().unwrap();
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
    heartbeat.context = round_answer(b"same", 1);
    node.step(heartbeat).unwrap();
    // The quorum confirms the read once, and each asker is answered.
    let answers: Vec<(u64, u64, Vec<u8>)> = drain(&mut node)
        .into_iter()
        .filter(|message| message.msg_type == MessageType::MsgReadIndexResp)
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

/// With readies taken ahead (R-4) a proposal is appended while a write is
/// out. What a notice makes durable, for an owner that does not keep it
/// (`RawNode::on_persist`), is dropped where it was held, and the log's
/// vector keeps its room: the next proposal lands without growing it again.
#[test]
fn a_notice_leaves_the_log_its_room_for_the_next_proposal() {
    let mut node = leader_with(with_depth(1, 2));
    node.propose(vec![], b"a".to_vec()).unwrap();
    let first = node.ready().unwrap();
    let mut written = first.entries().to_vec();
    node.advance_issued(first).unwrap();
    for data in [b"b", b"c", b"d", b"e", b"f", b"g", b"h"] {
        node.propose(vec![], data.to_vec()).unwrap();
    }
    let second = node.ready().unwrap();
    written.extend_from_slice(second.entries());
    let number = second.number();
    node.advance_issued(second).unwrap();
    let room = node.raft.log().unstable.entries.capacity();
    assert!(room >= 8);
    node.store_mut().append(&written);
    node.on_persist(number).unwrap();
    assert!(node.raft.log().unstable.entries.is_empty());
    assert_eq!(node.raft.log().unstable.entries.capacity(), room);
    node.propose(vec![], b"i".to_vec()).unwrap();
    assert_eq!(node.raft.log().unstable.entries.len(), 1);
    assert_eq!(node.raft.log().unstable.entries.capacity(), room);
}

/// The vector an owner empties and gives back is the member's next queue of
/// messages, room and all; one with more room than the bound is dropped.
#[test]
fn messages_given_back_are_the_next_queue() {
    let mut node = leader();
    node.tick().unwrap();
    node.tick().unwrap();
    let mut ready = node.ready().unwrap();
    let mut given = ready.take_messages();
    assert!(!given.is_empty());
    given.reserve(64);
    let room = given.capacity();
    node.advance_append(ready).unwrap();
    node.recycle_messages(given);
    node.tick().unwrap();
    node.tick().unwrap();
    let mut ready = node.ready().unwrap();
    let messages = ready.take_messages();
    assert!(!messages.is_empty());
    assert_eq!(messages.capacity(), room);
    node.advance_append(ready).unwrap();
    // More room than may ever wait is not kept.
    let mut bounded = config(1);
    bounded.limits.pending_messages = 8;
    let mut node = leader_with(bounded);
    let before = node.raft.msgs.resident_bytes();
    node.recycle_messages(Vec::with_capacity(9));
    assert_eq!(node.raft.msgs.resident_bytes(), before);
}

// slates' regression tests for terms and indexes that have no successor (mantle note 32 §2.13,
// R6; slates `docs/bugs/2026-09-30-a-saturated-term-let-two-leaders-share-it.md`, AUD-29-26).
// slates saturated a term at its last value, and a member that campaigned there led the term a
// leader already held. Here a message naming `u64::MAX` is beyond what is counted
// (`counts_beyond_bound`), so the last term and the last index a member may reach are
// `u64::MAX - 1`, and the step past either is refused before anything changes.

/// The last term or index a message may name: one less than `u64::MAX`, which no member reaches.
const LAST: u64 = u64::MAX - 1;

/// Every message the members give is stepped into the member it is to, as if they heard each
/// other at once, until none is given. A member not in `up` hears nothing and is asked nothing.
fn exchange(nodes: &mut [RawNode<Memory>], up: &[u64]) {
    for _ in 0..1_000 {
        let mut said = Vec::new();
        for id in up {
            said.extend(drain(&mut nodes[*id as usize - 1]));
        }
        if said.is_empty() {
            return;
        }
        for message in said {
            if up.contains(&message.to) {
                // What a member refuses changes nothing; the schedule goes on.
                let _ = nodes[message.to as usize - 1].step(message);
            }
        }
    }
    panic!("the members never fell quiet");
}
/// Three voters, each at `term` and having voted for no one.
fn three_at(term: u64) -> Vec<RawNode<Memory>> {
    (1..=3)
        .map(|id| {
            let mut store = Memory::with_voters(&[1, 2, 3]);
            store.hard_state = HardState {
                term,
                vote: 0,
                commit: 0,
            };
            RawNode::new(&config(id), store).unwrap()
        })
        .collect()
}
/// What a member is: its term, its vote and its role.
fn standing(node: &RawNode<Memory>) -> (u64, u64, StateRole) {
    (node.raft.term(), node.raft.vote(), node.raft.state())
}

/// A member elected in the last term holds it alone. Every later campaign of the others is
/// refused with nothing changed: asked of it by its owner, run out by its own timer, or ordered by
/// the leader that hands over. slates' member saturated its term at the last value and won it a
/// second time with the third voter's vote.
#[test]
fn a_term_with_no_successor_cannot_campaign_and_keeps_one_leader() {
    let mut nodes = three_at(LAST - 1);
    nodes[0].campaign().unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    assert_eq!(standing(&nodes[0]), (LAST, 1, StateRole::Leader));
    for id in [2u64, 3] {
        let node = &mut nodes[id as usize - 1];
        let before = standing(node);
        assert_eq!(before, (LAST, before.1, StateRole::Follower));
        assert_eq!(node.campaign(), Err(Error::Capacity("terms")));
        assert_eq!(standing(node), before);
        // Its own timer runs out, twice over: nothing is asked of anyone.
        for _ in 0..2 * 2 * node.raft.config().election_tick {
            node.tick().unwrap();
        }
        assert_eq!(standing(node), before);
        assert!(drain(node).is_empty());
        // The leader hands over to it: refused, and nothing moves.
        assert_eq!(
            node.step(answer(MessageType::MsgTimeoutNow, 1, id, LAST)),
            Err(Error::Capacity("terms"))
        );
        assert_eq!(standing(node), before);
        assert!(drain(node).is_empty());
    }
    let leaders: Vec<u64> = nodes
        .iter()
        .filter(|node| node.raft.state() == StateRole::Leader)
        .map(|node| node.raft.id())
        .collect();
    assert_eq!(leaders, vec![1], "one leader of the last term");
}

/// A request to be voted for in a term with no successor, or anything else that names one, is
/// refused as beyond what is counted, and changes nothing: the asker could not have reached it.
#[test]
fn a_vote_asked_for_a_term_with_no_successor_is_refused() {
    let mut nodes = three_at(LAST);
    let voter = &mut nodes[1];
    let before = standing(voter);
    for kind in [
        MessageType::MsgRequestPreVote,
        MessageType::MsgRequestVote,
        MessageType::MsgAppend,
        MessageType::MsgHeartbeat,
    ] {
        assert_eq!(
            voter.step(answer(kind, 1, 2, u64::MAX)),
            Err(Error::Violation(
                "a term or an index beyond what is counted"
            ))
        );
        assert_eq!(standing(voter), before);
        assert!(drain(voter).is_empty());
    }
    // Nor does it ask for one itself.
    assert_eq!(voter.campaign(), Err(Error::Capacity("terms")));
    assert_eq!(standing(voter), before);
}

/// A member whose log ends at the last index refuses whole an append that would run past it:
/// the log is unchanged, and no entry takes an index another holds. slates' follower noted its
/// next index with a saturating sum, and two entries shared one index.
#[test]
fn an_append_past_the_last_index_is_refused_whole() {
    for (start, sent) in [
        (LAST, vec![u64::MAX]),
        (LAST - 2, vec![LAST - 1, LAST, u64::MAX]),
    ] {
        let mut store = Memory::with_voters(&[1, 2, 3]);
        store.install(snapshot(start, 3, &[1, 2, 3]));
        store.hard_state = HardState {
            term: 3,
            vote: 1,
            commit: start,
        };
        let mut follower = RawNode::new(&config(2), store).unwrap();
        let hard = follower.raft.hard_state();
        let mut append = answer(MessageType::MsgAppend, 1, 2, 3);
        append.index = start;
        append.log_term = 3;
        append.commit = start;
        append.entries = sent.iter().map(|index| entry(*index, 3)).collect();
        assert_eq!(
            follower.step(append),
            Err(Error::Violation(
                "a term or an index beyond what is counted"
            ))
        );
        unchanged(&follower, &hard, start);
    }
}

/// A leader whose log ends at the last index refuses every entry more: a proposal and a change
/// alike, the log unchanged and no change taken for pending.
#[test]
fn a_leader_at_the_last_index_refuses_new_entries() {
    let mut store = Memory::with_voters(&[1]);
    store.install(snapshot(LAST - 1, 3, &[1]));
    store.hard_state = HardState {
        term: 3,
        vote: 1,
        commit: LAST - 1,
    };
    let mut leader = RawNode::new(&config(1), store).unwrap();
    leader.campaign().unwrap();
    drain(&mut leader);
    assert_eq!(leader.raft.state(), StateRole::Leader);
    // Its first entry took the last index.
    assert_eq!(leader.raft.log().last_index().unwrap(), LAST);
    let pending = leader.raft.pending_conf_index();
    assert_eq!(
        leader.propose(vec![], b"one more".to_vec()),
        Err(Error::Capacity("the log's indexes"))
    );
    let change = ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: crate::proto::ConfChangeType::AddNode,
            node_id: 2,
        }],
        ..Default::default()
    };
    assert_eq!(
        leader.propose_conf_change(vec![], &change),
        Err(Error::Capacity("the log's indexes"))
    );
    assert_eq!(leader.raft.log().last_index().unwrap(), LAST);
    assert_eq!(leader.raft.pending_conf_index(), pending);
    assert!(drain(&mut leader).is_empty());
}

/// A member whose log already ends at the last index could not write the entry a leader's term
/// begins with: its campaign is refused before it changes anything, as a campaign for a term with
/// no successor is.
#[test]
fn a_member_with_no_index_for_a_leaders_first_entry_does_not_campaign() {
    let mut store = Memory::with_voters(&[1]);
    store.install(snapshot(LAST, 3, &[1]));
    store.hard_state = HardState {
        term: 3,
        vote: 1,
        commit: LAST,
    };
    let mut node = RawNode::new(&config(1), store).unwrap();
    let before = standing(&node);
    assert_eq!(node.campaign(), Err(Error::Capacity("the log's indexes")));
    assert_eq!(standing(&node), before);
    assert_eq!(node.raft.log().last_index().unwrap(), LAST);
    assert!(drain(&mut node).is_empty());
}

/// By suspicion (timing step L-2) a member with no term or no index to lead in is due for no
/// campaign: its detectors suspecting its leader arm nothing it could do, and a wake does nothing
/// and refuses nothing. hyper-durable fences a replica on any error its wake returns, so a refusal
/// here would stop a member for being at the end of what it may count.
#[test]
fn by_suspicion_a_member_with_no_successor_is_due_for_no_campaign() {
    let timing = crate::Timing {
        span: std::time::Duration::from_millis(10),
        round: std::time::Duration::from_millis(2),
        // A delay within the span and one vote round.
        election: std::time::Duration::from_millis(12),
    };
    for (term, last) in [(LAST, 0), (3, LAST)] {
        let mut store = Memory::with_voters(&[1, 2, 3]);
        if last > 0 {
            store.install(snapshot(last, 3, &[1, 2, 3]));
        }
        store.hard_state = HardState {
            term,
            vote: 1,
            commit: last,
        };
        let suspicion = Config {
            elections: crate::Elections::Suspicion,
            ..config(2)
        };
        let mut node = RawNode::new(&suspicion, store).unwrap();
        node.set_timing(timing).unwrap();
        // It hears its leader, and then suspects it.
        let mut beat = answer(MessageType::MsgHeartbeat, 1, 2, term);
        beat.commit = last;
        node.step(beat).unwrap();
        node.wake(0).unwrap();
        drain(&mut node);
        node.suspect(1).unwrap();
        let before = standing(&node);
        for now in [0, 1_000_000_000, 10_000_000_000] {
            assert_eq!(node.wake(now), Ok(false), "term {term}, last {last}");
            assert_eq!(node.deadline(), None);
            assert_eq!(standing(&node), before);
            assert!(drain(&mut node).is_empty());
        }
    }
}

/// The fast track proposes and holds nothing at the last index: a leader that recovered such an
/// entry at its election would have no index for its own first entry after it. A proposal stops
/// one short, refused for the log's indexes, and a peer's proposal at the last index is a
/// violation, held nowhere. slates' fast track chose the next index with a saturating sum.
#[test]
fn the_fast_track_proposes_and_holds_nothing_at_the_last_index() {
    let fast = Config {
        fast: true,
        ..config(2)
    };
    let mut store = Memory::with_voters(&[1, 2, 3]);
    store.install(snapshot(LAST - 2, 3, &[1, 2, 3]));
    store.hard_state = HardState {
        term: 3,
        vote: 1,
        commit: LAST - 2,
    };
    let mut node = RawNode::new(&fast, store).unwrap();
    let mut beat = answer(MessageType::MsgHeartbeat, 1, 2, 3);
    beat.commit = LAST - 2;
    node.step(beat).unwrap();
    drain(&mut node);
    // One below the last index may be proposed; the last may not.
    assert_eq!(node.propose_fast(vec![], b"x".to_vec()), Ok(LAST - 1));
    assert_eq!(
        node.propose_fast(vec![], b"y".to_vec()),
        Err(Error::Capacity("the log's indexes"))
    );
    let held: Vec<u64> = node.raft.proposals().map(|entry| entry.index).collect();
    assert_eq!(held, vec![LAST - 1]);
    // A peer's proposal at the last index is refused, and not held.
    let mut proposal = answer(crate::fast::FAST_PROPOSE, 3, 2, 0);
    proposal.entries = vec![Entry {
        data: b"z".to_vec(),
        ..entry(LAST, 3)
    }];
    assert!(matches!(node.step(proposal), Err(Error::Violation(_))));
    let held: Vec<u64> = node.raft.proposals().map(|entry| entry.index).collect();
    assert_eq!(held, vec![LAST - 1]);
}

// slates' regression test for independent election draws (mantle note 32 §2.13, R7; slates
// `docs/bugs/2026-09-28-correlated-election-jitter-livelocked-a-split-vote.md`). slates drew a
// follower's jitter as `(id + attempt) mod span`: two members whose ids were congruent modulo the
// span timed out together at every attempt, and their split vote never resolved (19 s on its
// multi-region profile). Here each member draws from its own SplitMix64 stream (`Config::seed`) at
// every reset; by suspicion each arming draws anew from hyper-timing's law
// (`tests/suspicion.rs`, `every_arming_draws_anew` and
// `split_votes_resolve_and_split_exactly_when_the_law_says`).

/// Two survivors of a leader, seeded congruently modulo the span their timeouts are drawn over
/// (slates' worst case), are made to time out together; they split the vote, and every later
/// round is decided by the draws each makes anew at its campaign: one in which they draw the same
/// timeout splits again, and the first in which they differ elects the one that drew the shorter,
/// at exactly its timeout. Every round is predicted from the draws before it runs, and the group
/// elects within the rounds the test allows.
#[test]
fn survivors_whose_timeouts_collide_elect_at_the_first_round_their_draws_differ() {
    let span = config(1).election_tick as u64;
    let seeded = |id: u64, seed: u64| Config { seed, ..config(id) };
    let mut nodes: Vec<RawNode<Memory>> = [(1, 1), (2, 7), (3, 7 + span)]
        .into_iter()
        .map(|(id, seed)| RawNode::new(&seeded(id, seed), Memory::with_voters(&[1, 2, 3])).unwrap())
        .collect();
    nodes[0].campaign().unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    assert_eq!(nodes[0].raft.state(), StateRole::Leader);
    // The leader is gone. Both survivors heard it last at the same moment, and are made to time
    // out together (a harness that orders elections).
    let survivors = [2u64, 3];
    for id in survivors {
        let node = &mut nodes[id as usize - 1];
        assert_eq!(node.raft.election_elapsed(), 0);
        node.raft.set_randomized_election_timeout(15).unwrap();
    }
    let (mut splits, mut rounds) = (0, 0);
    loop {
        rounds += 1;
        assert!(rounds <= 64, "no round elected: the draws stay together");
        let drawn: Vec<usize> = survivors
            .iter()
            .map(|id| nodes[*id as usize - 1].raft.randomized_election_timeout())
            .collect();
        let term = nodes[1].raft.term();
        assert_eq!(nodes[2].raft.term(), term);
        let first = *drawn.iter().min().unwrap();
        // Ticks in lockstep, everything said heard at once, until a member campaigns.
        for _ in 0..first {
            for id in survivors {
                nodes[id as usize - 1].tick().unwrap();
            }
            exchange(&mut nodes, &survivors);
        }
        let roles: Vec<StateRole> = survivors
            .iter()
            .map(|id| nodes[*id as usize - 1].raft.state())
            .collect();
        for id in survivors {
            assert_eq!(nodes[id as usize - 1].raft.term(), term + 1);
        }
        if drawn[0] == drawn[1] {
            // Together: each was granted the other's pre-vote, and then refused its vote.
            assert_eq!(roles, vec![StateRole::Candidate; 2], "round {rounds}");
            splits += 1;
            continue;
        }
        let shorter = if drawn[0] < drawn[1] { 2 } else { 3 };
        let leader = survivors
            .iter()
            .copied()
            .find(|id| nodes[*id as usize - 1].raft.state() == StateRole::Leader);
        assert_eq!(leader, Some(shorter), "round {rounds}: drawn {drawn:?}");
        break;
    }
    assert!(splits >= 1, "the forced round split");
}

// slates' regression tests for leases (mantle note 32 §2.13, R4; slates
// `docs/bugs/2026-09-29-a-yielding-voter-refused-the-voter-it-yielded-to.md`). slates cleared a
// follower's belief in its leader only at its own campaign, so a voter that yielded its timeout to a
// more central one kept its lease and refused the very voter it yielded to. The thesis's rule
// (§4.2.3, etcd's `inLease`): a member refuses a vote only within the minimum election timeout of
// hearing from a current leader, whatever its own timer does.

/// Steps every member in `up` one tick, and then lets them hear each other at once; the members
/// that campaigned in it.
fn tick_all(nodes: &mut [RawNode<Memory>], up: &[u64]) -> Vec<u64> {
    let mut campaigned = Vec::new();
    for id in up {
        let node = &mut nodes[*id as usize - 1];
        let before = node.raft.state();
        node.tick().unwrap();
        let after = node.raft.state();
        if before == StateRole::Follower && after != StateRole::Follower {
            campaigned.push(*id);
        }
    }
    exchange(nodes, up);
    campaigned
}
/// Three voters led by 1 in term 1, every member holding its log; then 1 is gone.
fn led_three(priorities: [i64; 3]) -> Vec<RawNode<Memory>> {
    let mut nodes: Vec<RawNode<Memory>> = (1..=3)
        .map(|id| {
            let config = Config {
                priority: priorities[id as usize - 1],
                ..config(id)
            };
            RawNode::new(&config, Memory::with_voters(&[1, 2, 3])).unwrap()
        })
        .collect();
    nodes[0].campaign().unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    assert_eq!(nodes[0].raft.state(), StateRole::Leader);
    nodes[0].propose(vec![], b"entry".to_vec()).unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    // The leader's heartbeat carries its commit to both, the last they hear of it.
    nodes[0].ping().unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    for id in [2usize, 3] {
        assert_eq!(nodes[id - 1].raft.election_elapsed(), 0);
        assert_eq!(nodes[id - 1].raft.leader_id(), 1);
    }
    nodes
}

/// slates' `the_most_central_survivor_wins_its_first_campaign`, as this core states priorities: the
/// leader is lost; the survivor it outranks would time out first, but its owner gives it patience
/// past its timeout (slates' yield: one timeout per voter that outranks it), and the most central
/// survivor campaigns at its own. The yielding voter has not heard a leader for the minimum election
/// timeout, so it grants the pre-vote and the vote, and the most central survivor leads at its first
/// campaign, the only campaign of the group. Before slates' fix the central survivor was refused at
/// its first two campaigns and the outranked one led.
#[test]
fn the_most_central_survivor_wins_its_first_campaign() {
    let mut nodes = led_three([0, 2, 1]);
    let election = nodes[1].raft.config().election_tick;
    nodes[1]
        .raft
        .set_randomized_election_timeout(2 * election - 1)
        .unwrap();
    nodes[2]
        .raft
        .set_randomized_election_timeout(election)
        .unwrap();
    nodes[2].raft.set_patience(2 * election);
    let mut campaigns = Vec::new();
    for tick in 1..=4 * election {
        campaigns.extend(
            tick_all(&mut nodes, &[2, 3])
                .into_iter()
                .map(|id| (tick, id)),
        );
        if nodes[1].raft.state() == StateRole::Leader {
            break;
        }
    }
    assert_eq!(nodes[1].raft.state(), StateRole::Leader, "{campaigns:?}");
    assert_eq!(campaigns, vec![(2 * election - 1, 2)]);
}

/// A member whose timeout runs out while a change it committed is not yet applied campaigns on
/// the configuration its log holds (`docs/raft.md` §3.4): an owner whose commit fence holds the
/// change (`RawNode::pause_apply`, `docs/durable.md` §4.1) no longer holds its member's campaign
/// back at a leader's loss.
#[test]
fn a_member_whose_owner_holds_a_change_campaigns_by_it() {
    let mut nodes = led_three([0, 0, 0]);
    let election = nodes[1].raft.config().election_tick;
    // Member 3's owner holds what it is given to apply; a change is committed everywhere.
    nodes[2].pause_apply();
    let change = ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: crate::proto::ConfChangeType::AddLearnerNode,
            node_id: 4,
        }],
        ..Default::default()
    };
    nodes[0].propose_conf_change(vec![], &change).unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    nodes[0].ping().unwrap();
    exchange(&mut nodes, &[1, 2, 3]);
    let committed = nodes[0].raft.log().committed();
    assert_eq!(nodes[2].raft.log().committed(), committed);
    assert!(nodes[2].raft.log().applied() < committed);
    assert!(nodes[2].raft.configuration().learners().contains(&4));
    // 1 is gone. 3 runs out first and campaigns; 2 runs out later.
    nodes[2]
        .raft
        .set_randomized_election_timeout(election)
        .unwrap();
    nodes[1]
        .raft
        .set_randomized_election_timeout(election + election / 2)
        .unwrap();
    let mut campaigns = Vec::new();
    for tick in 1..=2 * election {
        campaigns.extend(
            tick_all(&mut nodes, &[2, 3])
                .into_iter()
                .map(|id| (tick, id)),
        );
        if nodes[2].raft.state() == StateRole::Leader {
            break;
        }
    }
    assert_eq!(nodes[2].raft.state(), StateRole::Leader, "{campaigns:?}");
    assert_eq!(campaigns, vec![(election, 3)]);
}

/// The lease still holds where the thesis says it does: a member that heard its leader within the
/// minimum election timeout refuses a pre-vote (Ongaro §4.2.3, §9.6), whatever its own timer drew.
#[test]
fn a_member_that_heard_its_leader_within_the_minimum_timeout_refuses() {
    let mut nodes = led_three([0, 0, 0]);
    let election = nodes[1].raft.config().election_tick;
    nodes[2]
        .raft
        .set_randomized_election_timeout(2 * election - 1)
        .unwrap();
    for _ in 0..election - 1 {
        nodes[2].tick().unwrap();
    }
    // 2 is told to campaign by its owner one tick inside 3's lease.
    nodes[1].campaign().unwrap();
    exchange(&mut nodes, &[2, 3]);
    assert_eq!(nodes[2].raft.state(), StateRole::Follower);
    assert_eq!(nodes[2].raft.term(), 1);
    assert_ne!(nodes[1].raft.state(), StateRole::Leader);
}

// slates' regression tests for votes across a change (mantle note 32 §2.13, R5; slates
// `docs/bugs/2026-09-29-a-member-that-missed-its-promotion-refused-every-election.md`). slates'
// member refused a vote when its own configuration did not name it a voter, and kept the lease of a
// leader it heard as a learner for good; a member promoted by entries committed without it then
// refused every election of the voter that held the promotion. Thesis §4.1: "servers process
// incoming RPC requests without consulting their current configurations"; the candidate's
// configuration decides whether a vote counts.

/// Persists and applies what there is, as `drain` does, applying every committed change of the
/// configuration as its owner would; the messages.
fn drain_applying(node: &mut RawNode<Memory>) -> Vec<Message> {
    use crate::wire::Record;
    let mut messages = Vec::new();
    while node.has_ready() {
        let mut ready: Ready = node.ready().unwrap();
        let entries = ready.entries().to_vec();
        node.store_mut().append(&entries);
        if let Some(hard) = ready.hard_state() {
            let hard = *hard;
            node.store_mut().hard_state = hard;
        }
        messages.extend(ready.take_messages());
        messages.extend(ready.take_persisted_messages());
        let mut committed = ready.take_committed_entries();
        let mut light = node.advance_append(ready).unwrap();
        messages.extend(light.take_messages());
        committed.extend(light.take_committed_entries());
        for entry in &committed {
            if entry.entry_type == EntryType::EntryConfChangeV2 {
                let change = if entry.data.is_empty() {
                    ConfChangeV2::default()
                } else {
                    ConfChangeV2::decode(&entry.data).unwrap()
                };
                let state = node.apply_conf_change(&change).unwrap();
                node.store_mut().configuration = state;
            }
        }
        if let Some(last) = committed.last() {
            node.advance_apply_to(last.index).unwrap();
        }
    }
    messages
}
/// As `exchange`, applying every committed change.
fn exchange_applying(nodes: &mut [RawNode<Memory>], up: &[u64]) {
    for _ in 0..1_000 {
        let mut said = Vec::new();
        for id in up {
            said.extend(drain_applying(&mut nodes[*id as usize - 1]));
        }
        if said.is_empty() {
            return;
        }
        for message in said {
            if up.contains(&message.to) {
                let _ = nodes[message.to as usize - 1].step(message);
            }
        }
    }
    panic!("the members never fell quiet");
}

/// slates' `a_member_that_missed_its_promotion_still_votes_for_a_candidate_that_has_it`: B leads
/// {A, B} and moves the group to {A, B, C} through the joint and final configurations, each
/// committed with A's acknowledgement alone; C, which knows only {A, B} and so is no voter by its own
/// configuration, hears none of it. B is lost and A campaigns: C grants the pre-vote and the vote,
/// and A, whose configuration names C a voter, leads by it. On ticks where C never heard a leader,
/// and where it heard B as a learner and its lease lapses at the minimum election timeout; by
/// suspicion where it heard B as a learner and its lease lapses when its detectors suspect B.
#[test]
fn a_member_that_missed_its_promotion_still_votes_for_a_candidate_that_has_it() {
    let timing = crate::Timing {
        span: std::time::Duration::from_millis(10),
        round: std::time::Duration::from_millis(2),
        // A delay within the span and one vote round.
        election: std::time::Duration::from_millis(12),
    };
    for (heard_as_learner, suspicion) in [(false, false), (true, false), (true, true)] {
        let (a, b, c) = (1u64, 2u64, 3u64);
        let mut nodes: Vec<RawNode<Memory>> = (1..=3)
            .map(|id| {
                let mut store = Memory::with_voters(&[a, b]);
                if heard_as_learner {
                    store.configuration.learners = vec![c];
                }
                let elections = if suspicion {
                    crate::Elections::Suspicion
                } else {
                    crate::Elections::Ticks
                };
                let mut node = RawNode::new(
                    &Config {
                        elections,
                        ..config(id)
                    },
                    store,
                )
                .unwrap();
                if suspicion {
                    node.set_timing(timing).unwrap();
                }
                node
            })
            .collect();
        nodes[b as usize - 1].campaign().unwrap();
        let first = if heard_as_learner {
            vec![a, b, c]
        } else {
            vec![a, b]
        };
        exchange_applying(&mut nodes, &first);
        assert_eq!(nodes[b as usize - 1].raft.state(), StateRole::Leader);
        if heard_as_learner {
            assert_eq!(nodes[c as usize - 1].raft.leader_id(), b);
        }
        // The joint change and the one that leaves it, committed with A alone.
        let promote = ConfChangeV2 {
            transition: crate::proto::ConfChangeTransition::Explicit,
            changes: vec![ConfChangeSingle {
                change_type: crate::proto::ConfChangeType::AddNode,
                node_id: c,
            }],
            ..Default::default()
        };
        nodes[b as usize - 1]
            .propose_conf_change(vec![], &promote)
            .unwrap();
        exchange_applying(&mut nodes, &[a, b]);
        nodes[b as usize - 1]
            .propose_conf_change(vec![], &ConfChangeV2::default())
            .unwrap();
        exchange_applying(&mut nodes, &[a, b]);
        nodes[b as usize - 1].ping().unwrap();
        exchange_applying(&mut nodes, &[a, b]);
        let configuration = nodes[a as usize - 1].raft.configuration();
        assert!(configuration.votes(c) && !configuration.is_joint());
        assert!(!nodes[c as usize - 1].raft.configuration().votes(c));
        // B is lost. A's and C's leases of it lapse; A campaigns.
        if suspicion {
            for id in [a, c] {
                nodes[id as usize - 1].suspect(b).unwrap();
                drain_applying(&mut nodes[id as usize - 1]);
            }
        } else {
            let election = nodes[a as usize - 1].raft.config().election_tick;
            for _ in 0..election {
                for id in [a, c] {
                    nodes[id as usize - 1].tick().unwrap();
                    drain_applying(&mut nodes[id as usize - 1]);
                }
            }
        }
        nodes[a as usize - 1].campaign().unwrap();
        exchange_applying(&mut nodes, &[a, c]);
        assert_eq!(
            nodes[a as usize - 1].raft.state(),
            StateRole::Leader,
            "heard as a learner: {heard_as_learner}, by suspicion: {suspicion}"
        );
        assert_eq!(nodes[c as usize - 1].raft.vote(), a);
    }
}

/// slates' `a_member_outside_its_configuration_never_campaigns_and_its_vote_counts_nowhere_else`: a
/// member its configuration does not name never campaigns, on its timer or asked; it answers a vote
/// request as any server does, and the candidate, whose configuration does not name it either,
/// counts its grant toward nothing: it still needs a voter of its own.
#[test]
fn a_member_outside_its_configuration_never_campaigns_and_its_vote_counts_nowhere_else() {
    let mut nodes: Vec<RawNode<Memory>> = (1..=4)
        .map(|id| RawNode::new(&config(id), Memory::with_voters(&[1, 2, 3])).unwrap())
        .collect();
    let stranger = 4usize;
    assert_eq!(nodes[stranger - 1].campaign(), Err(Error::NotPromotable));
    for _ in 0..4 * nodes[stranger - 1].raft.config().election_tick {
        nodes[stranger - 1].tick().unwrap();
    }
    assert_eq!(nodes[stranger - 1].raft.state(), StateRole::Follower);
    assert!(drain(&mut nodes[stranger - 1]).is_empty());
    // 1 asks 4 alone, which grants; 1 is not elected by it.
    nodes[0].campaign().unwrap();
    let mut asked = drain(&mut nodes[0])
        .into_iter()
        .find(|message| message.msg_type == MessageType::MsgRequestPreVote)
        .unwrap();
    asked.to = 4;
    nodes[stranger - 1].step(asked).unwrap();
    let answers = drain(&mut nodes[stranger - 1]);
    assert_eq!(answers.len(), 1);
    assert!(!answers[0].reject, "it answers as any server does");
    for answer in answers {
        let _ = nodes[0].step(answer);
    }
    assert_eq!(nodes[0].raft.state(), StateRole::PreCandidate);
    assert_eq!(
        nodes[0].raft.term(),
        0,
        "a grant from outside counts for nothing"
    );
}

// slates' regression tests for replication against compaction (mantle note 32 §2.13, R20, with
// R19's conflict hints; slates `docs/bugs/2026-09-28-a-late-append-could-land-compacted-entries-on-a-log.md`).
// slates' follower appended a late append's entries below its snapshot at the end of its log, backed
// up one entry a refusal, let late replies move progress back, and credited a snapshot's recipient
// with the leader's own later snapshot.

/// A member at `term` whose log holds entries of the terms `terms` from index 1, and voters 1 to 3.
fn holding(id: u64, term: u64, terms: &[u64]) -> RawNode<Memory> {
    let mut store = Memory::with_voters(&[1, 2, 3]);
    let entries: Vec<Entry> = terms
        .iter()
        .enumerate()
        .map(|(at, term)| entry(at as u64 + 1, *term))
        .collect();
    store.append(&entries);
    store.hard_state = HardState {
        term,
        vote: 0,
        commit: 0,
    };
    RawNode::new(&config(id), store).unwrap()
}
/// Member 1 holding `terms`, elected by member 3's vote alone, its first entry appended.
fn elected_holding(term: u64, terms: &[u64]) -> RawNode<Memory> {
    let mut leader = holding(1, term, terms);
    leader.campaign().unwrap();
    drain(&mut leader);
    leader
        .step(answer(
            MessageType::MsgRequestPreVoteResponse,
            3,
            1,
            term + 1,
        ))
        .unwrap();
    drain(&mut leader);
    leader
        .step(answer(MessageType::MsgRequestVoteResponse, 3, 1, term + 1))
        .unwrap();
    assert_eq!(leader.raft.state(), StateRole::Leader);
    leader
}
/// What the leader sends `to` now; if nothing, what a beat of its clock sends (a member whose probe
/// went unanswered is probed again once a beat has passed, `HeartbeatAnswers::Position`).
fn sent_to(leader: &mut RawNode<Memory>, to: u64) -> Vec<Message> {
    let sent: Vec<Message> = drain(leader)
        .into_iter()
        .filter(|message| message.to == to)
        .collect();
    if !sent.is_empty() {
        return sent;
    }
    for _ in 0..leader.raft.config().heartbeat_tick {
        leader.tick().unwrap();
    }
    drain(leader)
        .into_iter()
        .filter(|message| message.to == to)
        .collect()
}
/// The leader's messages to `to` and `to`'s answers, back and forth until it accepts an append;
/// the refusals it took on the way. Bounded by the leader's log.
fn refusals_until_accepted(leader: &mut RawNode<Memory>, follower: &mut RawNode<Memory>) -> u64 {
    let to = follower.raft.id();
    let mut refusals = 0;
    let mut sent = sent_to(leader, to);
    for _ in 0..=leader.raft.log().last_index().unwrap() {
        for message in sent.drain(..) {
            follower.step(message).unwrap();
        }
        let answers = drain(follower);
        let refused = answers
            .iter()
            .filter(|answer| answer.msg_type == MessageType::MsgAppendResponse && answer.reject)
            .count();
        let accepted = answers
            .iter()
            .any(|answer| answer.msg_type == MessageType::MsgAppendResponse && !answer.reject);
        refusals += refused as u64;
        for answer in answers {
            leader.step(answer).unwrap();
        }
        if accepted {
            return refusals;
        }
        sent = sent_to(leader, to);
    }
    panic!("never accepted");
}
/// The leader and `follower` exchange messages until the follower holds the leader's log and its
/// commit; every append that carried entries to it, in order. Bounded by the leader's log: a round
/// for each entry, and four more for a probe, its refusal, the commit and a beat.
fn caught_up(leader: &mut RawNode<Memory>, follower: &mut RawNode<Memory>) -> Vec<Message> {
    let to = follower.raft.id();
    let mut carried = Vec::new();
    for _ in 0..=leader.raft.log().last_index().unwrap() + 4 {
        let done = follower.raft.log().last_index().unwrap()
            == leader.raft.log().last_index().unwrap()
            && follower.raft.log().committed() == leader.raft.log().committed();
        if done {
            return carried;
        }
        for message in sent_to(leader, to) {
            if message.msg_type == MessageType::MsgAppend && !message.entries.is_empty() {
                carried.push(message.clone());
            }
            follower.step(message).unwrap();
        }
        for answer in drain(follower) {
            leader.step(answer).unwrap();
        }
    }
    panic!("never caught up");
}

/// slates' `a_late_append_below_a_compacted_prefix_leaves_the_log_whole`: a late copy of an append
/// anchored below a follower's commit, after the follower compacted past its anchor, leaves the log
/// exactly as it was, and is answered with the follower's commit: every committed entry is the
/// same on every member. slates' follower pushed the compacted entries onto its log's end.
#[test]
fn a_late_append_below_a_compacted_prefix_leaves_the_log_whole() {
    let mut leader = leader();
    for value in 0..5u8 {
        leader.propose(vec![], vec![value]).unwrap();
    }
    let mut follower = RawNode::new(&config(2), Memory::with_voters(&[1, 2, 3])).unwrap();
    // The first append that carried entries to 2: its late copy comes after all the rest.
    let late = caught_up(&mut leader, &mut follower).remove(0);
    let last = leader.raft.log().last_index().unwrap();
    assert_eq!(follower.raft.log().committed(), last);
    // The follower compacts past the late copy's anchor.
    follower.store_mut().compact(last - 2, b"state".to_vec());
    let follower_config = config(2);
    let store = std::mem::take(follower.store_mut());
    let mut follower = RawNode::new(&follower_config, store).unwrap();
    let before = (
        follower.raft.log().first_index().unwrap(),
        follower.raft.log().last_index().unwrap(),
        follower.raft.hard_state(),
    );
    assert!(late.index < before.0);
    follower.step(late).unwrap();
    let answers = drain(&mut follower);
    assert_eq!(answers.len(), 1);
    assert!(!answers[0].reject);
    assert_eq!(
        answers[0].index, last,
        "it matches the leader through its commit"
    );
    let after = (
        follower.raft.log().first_index().unwrap(),
        follower.raft.log().last_index().unwrap(),
        follower.raft.hard_state(),
    );
    assert_eq!(after, before, "the log is exactly as it was");
}

/// slates' `an_empty_follower_is_found_in_one_refusal` (thesis §4.2.1, Raft §5.3): a follower whose
/// log does not reach the leader's previous index says where its log ends, and the leader backs up
/// to it in one round trip: an empty follower behind twenty entries costs one refusal, not twenty.
#[test]
fn an_empty_follower_is_found_in_one_refusal() {
    let mut leader = elected_holding(1, &[1; 20]);
    let mut follower = RawNode::new(&config(2), Memory::with_voters(&[1, 2, 3])).unwrap();
    follower
        .step(answer(MessageType::MsgHeartbeat, 1, 2, 2))
        .unwrap();
    drain(&mut follower);
    assert_eq!(refusals_until_accepted(&mut leader, &mut follower), 1);
    assert_eq!(follower.raft.log().last_index().unwrap(), 21);
}

/// slates' `a_stale_terms_run_is_skipped_in_one_refusal` (Raft §5.3): a follower holding a run of a
/// stale term names the term and where it may still agree, so the leader skips the whole run in one
/// round trip: eight diverged entries cost one refusal, not eight, and the follower's log ends as
/// the leader's.
#[test]
fn a_stale_terms_run_is_skipped_in_one_refusal() {
    let mut leader = elected_holding(3, &[1, 1, 3, 3, 3, 3, 3, 3, 3, 3]);
    let mut follower = holding(2, 2, &[1, 1, 2, 2, 2, 2, 2, 2, 2, 2]);
    assert_eq!(refusals_until_accepted(&mut leader, &mut follower), 1);
    assert_eq!(
        crate::raft::held(&follower.raft),
        crate::raft::held(&leader.raft)
    );
}

/// slates' `late_replies_never_move_progress_back` (thesis §3.5): after a follower matched through
/// ten, a late success through four and a late refusal hinting at the log's start leave its progress
/// where it was, and the next append to it is anchored at ten.
#[test]
fn late_replies_never_move_progress_back() {
    let mut leader = leader();
    for value in 0..9u8 {
        leader.propose(vec![], vec![value]).unwrap();
    }
    let mut follower = RawNode::new(&config(2), Memory::with_voters(&[1, 2, 3])).unwrap();
    caught_up(&mut leader, &mut follower);
    let progress = |leader: &RawNode<Memory>| {
        let progress = leader.raft.tracker().get(2).unwrap();
        (progress.matched, progress.next_index)
    };
    assert_eq!(progress(&leader), (10, 11));
    let mut late = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    late.index = 4;
    leader.step(late).unwrap();
    let mut refused = answer(MessageType::MsgAppendResponse, 2, 1, 1);
    refused.index = 3;
    refused.reject = true;
    refused.reject_hint = 0;
    leader.step(refused).unwrap();
    assert_eq!(progress(&leader), (10, 11));
    drain(&mut leader);
    leader.propose(vec![], b"next".to_vec()).unwrap();
    let next = drain(&mut leader)
        .into_iter()
        .find(|message| message.to == 2 && message.msg_type == MessageType::MsgAppend)
        .unwrap();
    assert_eq!(next.index, 10);
}

/// slates' `a_snapshot_reply_credits_what_the_follower_holds`: the leader credits a snapshot's
/// recipient with what the recipient says it holds, never with the leader's own snapshot when the
/// answer arrives. A leader that compacted further while the snapshot was on its way owes the
/// follower the newer one, and sends it.
#[test]
fn a_snapshot_reply_credits_what_the_follower_holds() {
    let mut leader = leader();
    for value in 0..3u8 {
        leader.propose(vec![], vec![value]).unwrap();
    }
    let mut b = RawNode::new(&config(2), Memory::with_voters(&[1, 2, 3])).unwrap();
    caught_up(&mut leader, &mut b);
    assert_eq!(leader.raft.log().committed(), 4);
    drain(&mut leader);
    // The leader compacts through 3; C is empty, and is probed into needing the snapshot.
    leader.store_mut().compact(3, b"state-3".to_vec());
    let mut c = RawNode::new(&config(3), Memory::with_voters(&[1, 2, 3])).unwrap();
    leader.ping().unwrap();
    let mut snapshot = None;
    for _ in 0..8 {
        for message in drain(&mut leader) {
            if message.to != 3 {
                continue;
            }
            if message.msg_type == MessageType::MsgSnapshot {
                snapshot = Some(message);
            } else {
                c.step(message).unwrap();
            }
        }
        if snapshot.is_some() {
            break;
        }
        for answer in drain(&mut c) {
            leader.step(answer).unwrap();
        }
    }
    let snapshot = snapshot.expect("C was sent the snapshot");
    assert_eq!(
        proto::snapshot_index(snapshot.snapshot.as_ref().unwrap()),
        3
    );
    c.step(snapshot).unwrap();
    let answers = drain(&mut c);
    let answer = answers
        .iter()
        .find(|answer| answer.msg_type == MessageType::MsgAppendResponse)
        .unwrap()
        .clone();
    assert_eq!(answer.index, 3);
    // Meanwhile the leader compacts through 4.
    leader.store_mut().compact(4, b"state-4".to_vec());
    leader.step(answer).unwrap();
    assert_eq!(leader.raft.tracker().get(3).unwrap().matched, 3);
    let again = drain(&mut leader)
        .into_iter()
        .find(|message| message.to == 3 && message.msg_type == MessageType::MsgSnapshot)
        .expect("C holds only through 3, below the leader's snapshot at 4");
    assert_eq!(proto::snapshot_index(again.snapshot.as_ref().unwrap()), 4);
}

// slates' out-of-order acknowledgement within a term (mantle note 32 R17; slates
// `docs/wip/research/consensus-enhancements.md` §3.5; `crate::ahead`): a member keeps a leader's
// entries that arrive ahead of a hole in its log, and takes them in when the hole is filled.

/// A leader and member 2 caught up with it, in a group of three; the leader proposes `count`
/// entries, and its appends to 2 are given back unsent.
fn ahead_of(
    config_of: impl Fn(u64) -> Config,
    count: u8,
) -> (RawNode<Memory>, RawNode<Memory>, Vec<Message>) {
    let mut leader = leader_with(config_of(1));
    let mut follower = RawNode::new(&config_of(2), Memory::with_voters(&[1, 2, 3])).unwrap();
    caught_up(&mut leader, &mut follower);
    let mut sent = Vec::new();
    for value in 0..count {
        leader.propose(vec![], vec![value]).unwrap();
        sent.extend(
            drain(&mut leader)
                .into_iter()
                .filter(|message| message.to == 2 && message.msg_type == MessageType::MsgAppend),
        );
    }
    (leader, follower, sent)
}
/// The entries the leader's appends among `messages` carry to member 2.
fn carried(messages: &[Message]) -> Vec<u64> {
    messages
        .iter()
        .filter(|message| message.to == 2 && message.msg_type == MessageType::MsgAppend)
        .flat_map(|message| message.entries.iter().map(|entry| entry.index))
        .collect()
}

/// slates' `a_follower_buffers_the_leaders_entries_ahead_of_a_hole_and_absorbs_them`: an append that
/// arrives ahead of the one before it is refused, and its entry kept; the one before it, once it
/// arrives, is taken with the kept entry, and its answer acknowledges both.
#[test]
fn a_follower_keeps_the_leaders_entries_ahead_of_a_hole_and_takes_them_in() {
    let (_, mut follower, sent) = ahead_of(config, 2);
    assert_eq!(sent.len(), 2, "one append an entry");
    let (behind, ahead) = (sent[0].clone(), sent[1].clone());
    let last = follower.raft.log().last_index().unwrap();
    follower.step(ahead).unwrap();
    let refused = drain(&mut follower);
    assert!(
        !refused.is_empty() && refused.iter().all(|answer| answer.reject),
        "the hole at {}",
        last + 1
    );
    assert_eq!(
        follower.raft.kept_ahead().collect::<Vec<_>>(),
        vec![last + 2]
    );
    assert_eq!(follower.raft.log().last_index().unwrap(), last);
    follower.step(behind).unwrap();
    let answers = drain(&mut follower);
    assert_eq!(answers.len(), 1);
    assert!(!answers[0].reject);
    assert_eq!(answers[0].index, last + 2, "the kept entry joined the log");
    assert_eq!(follower.raft.log().last_index().unwrap(), last + 2);
    assert_eq!(follower.raft.kept_ahead().count(), 0);
    assert_eq!(follower.raft.taken_ahead(), 1);
    follower.check_accounting().unwrap();
}

/// slates' `a_lost_batch_costs_one_resend_and_the_buffered_ones_are_not_sent_again`: a leader sends
/// three appends ahead of their answers, and the first is lost. The member refuses the other two and
/// keeps their entries; the leader sends the hole's entry again, alone; the member takes the kept
/// two with it and acknowledges all three; and nothing is sent again. Under raft-rs's rule
/// (`Ahead::Refused`, the differential's) the two are sent again after the hole is filled: the
/// divergence of `docs/raft.md` §3.3.
/// The rule for what arrives ahead of a hole changes on a running member (`RawNode::set_ahead`),
/// as an owner turns R17 on once every peer can read a kept refusal: opened under raft-rs's rule a
/// member keeps nothing ahead of a hole; switched to `Ahead::Kept` it keeps the next append that
/// arrives so, says so, and takes it in when the hole fills.
#[test]
fn the_rule_for_what_arrives_ahead_changes_on_a_running_member() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 3,
        ahead: crate::Ahead::Refused,
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 3);
    let hole = carried(&sent)[0];
    let mut ahead = sent.into_iter().skip(1);
    follower.step(ahead.next().unwrap()).unwrap();
    let answers = drain(&mut follower);
    assert!(answers.iter().all(|answer| answer.reject && !answer.kept));
    assert_eq!(
        follower.raft.kept_ahead().count(),
        0,
        "raft-rs's rule keeps nothing"
    );
    follower.set_ahead(crate::Ahead::Kept);
    follower.step(ahead.next().unwrap()).unwrap();
    let answers = drain(&mut follower);
    assert!(answers.iter().all(|answer| answer.reject && answer.kept));
    assert_eq!(
        follower.raft.kept_ahead().collect::<Vec<_>>(),
        vec![hole + 2]
    );
    for answer in answers {
        leader.step(answer).unwrap();
    }
    // The leader sends the hole again; once it fills, what was kept joins the log.
    for _ in 0..4 {
        let messages: Vec<Message> = drain(&mut leader)
            .into_iter()
            .filter(|message| message.to == 2)
            .collect();
        for message in messages {
            follower.step(message).unwrap();
        }
        for answer in drain(&mut follower) {
            leader.step(answer).unwrap();
        }
    }
    assert_eq!(follower.raft.kept_ahead().count(), 0);
    assert!(follower.raft.log().last_index().unwrap() >= hole + 2);
}

#[test]
fn a_lost_append_costs_its_own_resend_and_what_was_kept_is_not_sent_again() {
    for refused in [false, true] {
        let config_of = |id: u64| Config {
            // An append an entry, three ahead of their answers.
            max_size_per_msg: 1,
            max_inflight_msgs: 3,
            ahead: if refused {
                crate::Ahead::Refused
            } else {
                crate::Ahead::Kept
            },
            ..config(id)
        };
        let (mut leader, mut follower, sent) = ahead_of(config_of, 3);
        assert_eq!(carried(&sent).len(), 3, "three appends, one entry each");
        let hole = carried(&sent)[0];
        // The first is lost; the other two are refused.
        for append in sent.into_iter().skip(1) {
            follower.step(append).unwrap();
        }
        let answers = drain(&mut follower);
        assert!(answers.iter().all(|answer| answer.reject));
        let kept: Vec<u64> = follower.raft.kept_ahead().collect();
        let expected = if refused {
            vec![]
        } else {
            vec![hole + 1, hole + 2]
        };
        assert_eq!(kept, expected);
        for answer in answers {
            leader.step(answer).unwrap();
        }
        let resend = drain(&mut leader);
        assert_eq!(carried(&resend), vec![hole], "the hole's entry, alone");
        for message in resend.into_iter().filter(|message| message.to == 2) {
            follower.step(message).unwrap();
        }
        for answer in drain(&mut follower) {
            if !refused {
                assert_eq!(answer.index, hole + 2, "the kept two joined the log");
            }
            leader.step(answer).unwrap();
        }
        // What follows, until the member holds the leader's log.
        let mut after = Vec::new();
        for _ in 0..4 {
            let messages: Vec<Message> = drain(&mut leader)
                .into_iter()
                .filter(|message| message.to == 2)
                .collect();
            after.extend(carried(&messages));
            for message in messages {
                follower.step(message).unwrap();
            }
            for answer in drain(&mut follower) {
                leader.step(answer).unwrap();
            }
        }
        assert_eq!(follower.raft.log().last_index().unwrap(), hole + 2);
        let again = if refused {
            vec![hole + 1, hole + 2]
        } else {
            vec![]
        };
        assert_eq!(after, again, "refused: {refused}");
    }
}

/// A member that kept appends ahead of a hole and lost them (a restart drops what it kept) refuses
/// the next append ahead of the same hole: the leader's window still holds what it took as kept,
/// and sending the holes before the refused append again would send nothing. A beat with no answer
/// for that hole has the leader probe it, as for a resend a beat leaves unanswered (seed 1,318 of the group schedules in `tests/check.rs`, 2026-10-04: a leader sent new
/// appends to such a member, each refused, for as long as the group ran).
#[test]
fn a_member_that_lost_what_it_kept_ahead_is_probed() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 8,
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 4);
    let hole = carried(&sent)[0];
    // The first is lost, the other three kept and refused; the leader sends the hole again.
    for append in sent.iter().skip(1) {
        follower.step(append.clone()).unwrap();
    }
    let mut resent = Vec::new();
    for refusal in drain(&mut follower) {
        leader.step(refusal).unwrap();
        resent.extend(
            drain(&mut leader)
                .into_iter()
                .filter(|message| message.to == 2),
        );
    }
    assert_eq!(carried(&resent), vec![hole]);
    // The member restarts, losing what it kept, and then the hole arrives: it takes the hole
    // alone, and its answer says so.
    let mut follower = RawNode::new(&config_of(2), follower.store().clone()).unwrap();
    for message in resent {
        follower.step(message).unwrap();
    }
    for answer in drain(&mut follower) {
        leader.step(answer).unwrap();
    }
    drain(&mut leader);
    assert_eq!(leader.raft.tracker().get(2).unwrap().matched, hole);
    // A new entry's append is refused ahead of the hole after the hole, and the hole is in no
    // message out. Once a beat of the leader's ticks passes with no answer for it, the next
    // heartbeat's answer has the member probed from past what it holds, and it takes all it lacks.
    leader.propose(vec![], vec![9]).unwrap();
    let mut out: Vec<Message> = drain(&mut leader)
        .into_iter()
        .filter(|message| message.to == 2)
        .collect();
    let beat = config_of(1).heartbeat_tick;
    for _ in 0..4 * beat {
        let mut answers = Vec::new();
        for message in out.drain(..) {
            follower.step(message).unwrap();
            answers.extend(drain(&mut follower));
        }
        for answer in answers {
            leader.step(answer).unwrap();
        }
        leader.tick().unwrap();
        out.extend(
            drain(&mut leader)
                .into_iter()
                .filter(|message| message.to == 2),
        );
    }
    assert_eq!(
        leader.raft.tracker().get(2).unwrap().matched,
        hole + 4,
        "the member holds every entry"
    );
}

/// The leader's half: a member that keeps what arrives ahead of a hole refuses each append it
/// keeps, and its leader sends the hole alone again, once: what it sent ahead is not sent again,
/// and each append the member kept leaves the window, as a segment the receiver says it holds
/// leaves RFC 6675's pipe. The answer to the hole frees what is left. raft-rs's leader probes from
/// the member's match and sends the window again (`Ahead::Refused`).
#[test]
fn a_refusal_sends_the_hole_alone_and_what_was_kept_leaves_the_window() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 8,
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 4);
    let hole = carried(&sent)[0];
    let window = |leader: &RawNode<Memory>| {
        let progress = leader.raft.tracker().get(2).unwrap();
        (
            progress.state,
            progress.next_index,
            progress.inflights.bytes(),
        )
    };
    let (state, next, bytes) = window(&leader);
    // One entry an append, each charged alike.
    let each = bytes / 4;
    assert_eq!(bytes, 4 * each);
    // The first is lost: the other three are kept, and each is refused.
    for append in sent.iter().skip(1) {
        follower.step(append.clone()).unwrap();
    }
    let refusals = drain(&mut follower);
    assert_eq!(refusals.len(), 3);
    assert!(
        refusals
            .iter()
            .all(|refusal| refusal.reject && refusal.reject_hint == hole - 1)
    );
    let mut resent = Vec::new();
    for refusal in refusals {
        leader.step(refusal).unwrap();
        resent.extend(
            drain(&mut leader)
                .into_iter()
                .filter(|message| message.to == 2),
        );
    }
    assert_eq!(carried(&resent), vec![hole], "the hole alone, once");
    assert_eq!(
        window(&leader),
        (state, next, each),
        "what was kept left the window; the hole's first sending is out"
    );
    assert_eq!(leader.raft.tracker().get(2).unwrap().repaired, hole);
    // The hole's entry comes: the member takes the kept three with it, and its answer frees the
    // window.
    for message in resent {
        follower.step(message).unwrap();
    }
    let answers = drain(&mut follower);
    assert_eq!(answers.last().unwrap().index, hole + 3);
    for answer in answers {
        leader.step(answer).unwrap();
    }
    let progress = leader.raft.tracker().get(2).unwrap();
    assert_eq!(
        (progress.matched, progress.inflights.count()),
        (hole + 3, 0)
    );
}

/// The hole sent again may be lost too: a beat with no answer for it, the member answering
/// heartbeats and nothing of the hole, probes it from its match, as a full window that goes
/// unanswered is.
#[test]
fn a_hole_sent_again_and_unanswered_for_a_beat_is_probed() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 8,
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 3);
    let hole = carried(&sent)[0];
    follower.step(sent[1].clone()).unwrap();
    for refusal in drain(&mut follower) {
        leader.step(refusal).unwrap();
    }
    let lost = drain(&mut leader);
    assert_eq!(carried(&lost), vec![hole], "sent again, and lost");
    let beat = leader.raft.config().heartbeat_tick;
    let mut probe = Vec::new();
    for tick in 1..=beat {
        leader.tick().unwrap();
        for message in drain(&mut leader) {
            match message.msg_type {
                MessageType::MsgHeartbeat if message.to == 2 => {
                    follower.step(message).unwrap();
                    for answer in drain(&mut follower) {
                        leader.step(answer).unwrap();
                        probe.extend(
                            drain(&mut leader)
                                .into_iter()
                                .filter(|message| message.to == 2),
                        );
                    }
                }
                _ => {}
            }
        }
        if tick < beat {
            assert!(probe.is_empty(), "tick {tick}: not before a beat");
        }
    }
    let progress = leader.raft.tracker().get(2).unwrap();
    assert_eq!(progress.state, crate::progress::ProgressState::Probe);
    assert_eq!(carried(&probe).first(), Some(&hole), "probed from the hole");
}

/// A refusal the member made before it took more still tells what it kept. Here the member kept
/// the second and fourth appends, refusing each, and the first took the second with it but not the
/// fourth, past the third, which was lost: its answer reaches the leader before the refusal of the
/// fourth. The leader sends the third again, the append that carried it first and no more, keeping
/// its window, and the member takes the fourth with it; raft-rs's leader probes from the member's
/// match and sends all that followed again.
#[test]
fn a_refusal_older_than_the_members_progress_sends_what_it_lacks_and_no_more() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 8,
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 4);
    let hole = carried(&sent)[0];
    follower.step(sent[1].clone()).unwrap();
    follower.step(sent[3].clone()).unwrap();
    let refusals = drain(&mut follower);
    assert_eq!(refusals.len(), 2);
    follower.step(sent[0].clone()).unwrap();
    let answers = drain(&mut follower);
    assert_eq!(
        answers.last().unwrap().index,
        hole + 1,
        "the fourth waits on the third"
    );
    for answer in answers {
        leader.step(answer).unwrap();
    }
    drain(&mut leader);
    let window = |leader: &RawNode<Memory>| {
        let progress = leader.raft.tracker().get(2).unwrap();
        (
            progress.state,
            progress.next_index,
            progress.inflights.count(),
        )
    };
    let before = window(&leader);
    // The refusal of the fourth: it names the end the member had, before the hole it now has.
    let late = refusals.last().unwrap().clone();
    assert_eq!((late.index, late.reject_hint), (hole + 2, hole - 1));
    leader.step(late).unwrap();
    let resent: Vec<Message> = drain(&mut leader)
        .into_iter()
        .filter(|message| message.to == 2)
        .collect();
    assert_eq!(carried(&resent), vec![hole + 2], "the third, alone");
    assert_eq!(
        window(&leader),
        before,
        "the window keeps what it sent ahead"
    );
    for message in resent {
        follower.step(message).unwrap();
    }
    assert_eq!(drain(&mut follower).last().unwrap().index, hole + 3);
}

/// Two appends of a window lost: each goes again, once, as the refusal of an append sent after it
/// arrives, not one hole a round trip; the member keeps the second resend ahead of the first hole
/// and takes everything once the first arrives.
#[test]
fn every_hole_before_what_was_kept_goes_again_at_once() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 8,
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 5);
    let hole = carried(&sent)[0];
    // The first and third are lost.
    for at in [1, 3, 4] {
        follower.step(sent[at].clone()).unwrap();
    }
    let mut repairs = Vec::new();
    for refusal in drain(&mut follower) {
        leader.step(refusal).unwrap();
        repairs.extend(
            drain(&mut leader)
                .into_iter()
                .filter(|message| message.to == 2),
        );
    }
    assert_eq!(
        carried(&repairs),
        vec![hole, hole + 2],
        "both holes, each once"
    );
    // The second arrives first and is kept; the first takes everything.
    for message in repairs.into_iter().rev() {
        follower.step(message).unwrap();
    }
    assert_eq!(drain(&mut follower).last().unwrap().index, hole + 4);
}

/// A member that keeps nothing ahead of a hole (raft-rs's rule, `Ahead::Refused`, as in a group of
/// both cores) refuses an append past its end without saying it kept it; its leader probes from
/// the member's match as raft-rs does, sends what the member dropped again, and the member catches
/// up. A leader that took every refusal for a kept append would mark the dropped ones arrived and
/// never send them.
#[test]
fn a_member_that_keeps_nothing_ahead_is_probed_and_caught_up() {
    let config_of = |id: u64| Config {
        max_size_per_msg: 1,
        max_inflight_msgs: 8,
        ahead: if id == 2 {
            crate::Ahead::Refused
        } else {
            crate::Ahead::Kept
        },
        ..config(id)
    };
    let (mut leader, mut follower, sent) = ahead_of(config_of, 3);
    // The first is lost; the other two are refused, and nothing is kept.
    for append in sent.iter().skip(1) {
        follower.step(append.clone()).unwrap();
    }
    let refusals = drain(&mut follower);
    assert!(
        refusals
            .iter()
            .all(|refusal| refusal.reject && !refusal.kept)
    );
    assert_eq!(follower.raft.kept_ahead().count(), 0);
    for refusal in refusals {
        leader.step(refusal).unwrap();
    }
    for _ in 0..16 {
        let messages: Vec<Message> = drain(&mut leader)
            .into_iter()
            .filter(|message| message.to == 2)
            .collect();
        if messages.is_empty() {
            break;
        }
        for message in messages {
            follower.step(message).unwrap();
        }
        for answer in drain(&mut follower) {
            leader.step(answer).unwrap();
        }
    }
    assert_eq!(
        follower.raft.log().last_index().unwrap(),
        leader.raft.log().last_index().unwrap()
    );
}

/// The answer that acknowledges kept entries leaves with the write that holds them: they are in
/// the `Ready`'s entries, and the answer among the messages that wait for its persistence
/// (`docs/durable.md` I2 and §10: the window's slots are in the write before the acknowledgement).
#[test]
fn what_was_kept_is_acknowledged_only_with_the_write_that_holds_it() {
    let mut node = follower_with(with_depth(2, 2));
    let append = |index: u64| {
        let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
        append.index = index - 1;
        append.log_term = 1;
        append.commit = 2;
        append.entries = vec![entry(index, 1)];
        append
    };
    // 5 arrives ahead of 4: refused, and kept.
    node.step(append(5)).unwrap();
    let mut ready = node.ready().unwrap();
    assert!(ready.entries().is_empty());
    let refusals = ready.take_persisted_messages();
    assert!(refusals.iter().all(|message| message.reject));
    node.advance_append(ready).unwrap();
    // 4 fills the hole: the write holds 4 and 5, and the answer for 5 waits for it.
    node.step(append(4)).unwrap();
    let mut ready = node.ready().unwrap();
    let written: Vec<u64> = ready.entries().iter().map(|entry| entry.index).collect();
    assert_eq!(written, vec![4, 5]);
    assert!(
        ready.messages().is_empty(),
        "nothing leaves before the write"
    );
    let answers = ready.take_persisted_messages();
    assert_eq!(answers.len(), 1);
    assert_eq!((answers[0].index, answers[0].reject), (5, false));
}

/// What a member keeps ahead of a hole is bounded as what its log holds not yet durable is
/// (`Limits::unstable_entries`): the entries nearest the hole are kept, the furthest dropped,
/// whatever order the appends arrive in.
#[test]
fn what_is_kept_ahead_of_a_hole_has_a_bound() {
    let config_of = |id: u64| {
        let mut config = Config {
            max_size_per_msg: 1,
            max_inflight_msgs: 8,
            ..config(id)
        };
        config.limits.unstable_entries = 2;
        config.limits.entries_per_message = 2;
        config
    };
    let (_, mut follower, sent) = ahead_of(config_of, 5);
    let hole = carried(&sent)[0];
    // The last first, then the others but the hole.
    for append in sent[2..].iter().rev().chain(sent[1..2].iter()) {
        follower.step(append.clone()).unwrap();
    }
    assert_eq!(
        follower.raft.kept_ahead().collect::<Vec<_>>(),
        vec![hole + 1, hole + 2]
    );
    follower.check_accounting().unwrap();
}

/// What was kept was one leader's in one term: a member that moves to a later term forgets it.
#[test]
fn what_was_kept_goes_with_its_term() {
    let (_, mut follower, sent) = ahead_of(config, 2);
    follower.step(sent[1].clone()).unwrap();
    drain(&mut follower);
    assert_eq!(follower.raft.kept_ahead().count(), 1);
    let term = follower.raft.term();
    follower
        .step(answer(MessageType::MsgHeartbeat, 3, 2, term + 1))
        .unwrap();
    assert_eq!(follower.raft.term(), term + 1);
    assert_eq!(follower.raft.kept_ahead().count(), 0);
}

// slates' learner catch-up rounds (mantle note 32 R13; Ongaro's thesis §4.2.1; `crate::catchup`).

/// Every message the members in `up` give is stepped into the member it is to, once: one round of
/// replication. A message to a member that is down is lost, and its sender's owner is told
/// (`RawNode::report_unreachable`), as a transport tells it. Whether anything was said.
fn one_round(nodes: &mut [RawNode<Memory>], up: &[u64]) -> bool {
    let mut said = Vec::new();
    for id in up {
        said.extend(drain_applying(&mut nodes[*id as usize - 1]));
    }
    let any = !said.is_empty();
    for message in said {
        if up.contains(&message.to) {
            let _ = nodes[message.to as usize - 1].step(message);
        } else if message.msg_type == MessageType::MsgAppend {
            let _ = nodes[message.from as usize - 1].report_unreachable(message.to);
        }
    }
    any
}
/// Rounds among `up` until no member says anything, and once more after a round of the leader's
/// heartbeats, whose answers send what a member it was told it lost is behind by.
fn rounds_among(nodes: &mut [RawNode<Memory>], up: &[u64]) {
    for beat in [false, true] {
        if beat && nodes[0].raft.state() == StateRole::Leader {
            nodes[0].ping().unwrap();
        }
        let mut quiet = false;
        for _ in 0..1_000 {
            if !one_round(nodes, up) {
                quiet = true;
                break;
            }
        }
        assert!(quiet, "the members never fell quiet");
    }
}
/// Voters 1, 2 and 3 led by 1 holding `entries` committed entries, and `members` in all, those past
/// 3 with empty logs that know the voters. Each append carries two entries and one is out to a
/// member at a time, as slates' drive sent its followers a batch of about two entries a period
/// (slates `raft.rs`, `GAP_BUDGET`): a member behind takes rounds to catch up.
fn group_of(entries: u8, members: u64) -> Vec<RawNode<Memory>> {
    let two_entries = 2 * proto::approximate_bytes(&Entry {
        data: vec![0],
        ..Entry::default()
    }) as u64;
    let mut nodes: Vec<RawNode<Memory>> = (1..=members)
        .map(|id| {
            let config = Config {
                max_size_per_msg: two_entries,
                max_inflight_msgs: 1,
                ..config(id)
            };
            RawNode::new(&config, Memory::with_voters(&[1, 2, 3])).unwrap()
        })
        .collect();
    nodes[0].campaign().unwrap();
    rounds_among(&mut nodes, &[1, 2, 3]);
    for value in 0..entries {
        nodes[0].propose(vec![], vec![value]).unwrap();
    }
    rounds_among(&mut nodes, &[1, 2, 3]);
    nodes[0].ping().unwrap();
    rounds_among(&mut nodes, &[1, 2, 3]);
    assert_eq!(
        nodes[2].raft.log().committed(),
        nodes[0].raft.log().last_index().unwrap()
    );
    nodes
}
fn single(kind: crate::proto::ConfChangeType, member: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: kind,
            node_id: member,
        }],
        ..Default::default()
    }
}
/// The leader of `nodes` adds `member` as a learner, committed with the voters alone. What it
/// sent the learner meanwhile was lost, and its owner says so.
fn add_learner(nodes: &mut [RawNode<Memory>], member: u64) {
    nodes[0]
        .propose_conf_change(
            vec![],
            &single(crate::proto::ConfChangeType::AddLearnerNode, member),
        )
        .unwrap();
    rounds_among(nodes, &[1, 2, 3]);
    let configuration = nodes[0].raft.configuration();
    assert!(configuration.contains(member) && !configuration.votes(member));
}
/// The leader ticks once, and the voters exchange what follows: it stays in contact with them.
fn tick_with_voters(nodes: &mut [RawNode<Memory>]) {
    nodes[0].tick().unwrap();
    rounds_among(nodes, &[1, 2, 3]);
    assert_eq!(nodes[0].raft.state(), StateRole::Leader);
}

/// slates' `a_staged_member_counts_toward_no_commit` (thesis §4.2.1: "not yet counted towards
/// majorities"): with voters 2 and 3 silent, a learner being caught up that takes every entry
/// commits none of them.
#[test]
fn a_learner_being_caught_up_counts_toward_no_commit() {
    let mut nodes = group_of(4, 4);
    add_learner(&mut nodes, 4);
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Pending);
    nodes[0].propose(vec![], b"new".to_vec()).unwrap();
    let committed = nodes[0].raft.log().committed();
    for _ in 0..16 {
        if !one_round(&mut nodes, &[1, 4]) {
            break;
        }
    }
    assert_eq!(
        nodes[3].raft.log().last_index().unwrap(),
        nodes[0].raft.log().last_index().unwrap(),
        "the learner took every entry"
    );
    assert_eq!(nodes[0].raft.log().committed(), committed);
}

/// slates' `a_member_that_never_answers_is_aborted_and_staged_afresh_after` (thesis §4.2.1: "the
/// leader should also abort the change if the new server is unavailable"): a learner that never
/// answers is given up once a whole election passes with its lag not shrinking, exactly at the
/// minimum election timeout on ticks and not a tick before, said once; asked again, it is staged
/// afresh.
#[test]
fn a_learner_that_never_answers_is_given_up_after_an_election_and_staged_afresh() {
    let mut nodes = group_of(4, 4);
    add_learner(&mut nodes, 4);
    let election = nodes[0].raft.config().election_tick;
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Pending);
    for tick in 1..=election {
        tick_with_voters(&mut nodes);
        let expected = if tick < election {
            crate::CatchUp::Pending
        } else {
            crate::CatchUp::Aborted
        };
        assert_eq!(nodes[0].catch_up(4).unwrap(), expected, "tick {tick}");
    }
    assert_eq!(
        nodes[0].catch_up(4).unwrap(),
        crate::CatchUp::Pending,
        "staged afresh"
    );
}

/// slates' `a_round_that_spans_a_window_is_followed_by_one_that_counts`: a learner's first round
/// lasts a whole election, the learner unheard, so it does not count; the round that follows
/// ends at once, and does.
#[test]
fn a_round_that_spans_an_election_is_followed_by_one_that_counts() {
    let mut nodes = group_of(4, 4);
    add_learner(&mut nodes, 4);
    let election = nodes[0].raft.config().election_tick;
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Pending);
    for _ in 0..election {
        tick_with_voters(&mut nodes);
    }
    // The learner takes the log in one exchange: its first round ends, an election late, and the
    // next with it.
    rounds_among(&mut nodes, &[1, 2, 3, 4]);
    assert_eq!(
        nodes[3].raft.log().last_index().unwrap(),
        nodes[0].raft.log().last_index().unwrap()
    );
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Ready);
}

/// slates' `staging_ends_with_leadership`: the rounds are a leader's; one that steps down forgets
/// them and is told so.
#[test]
fn a_learners_rounds_end_with_their_leaders_term() {
    let mut nodes = group_of(4, 4);
    add_learner(&mut nodes, 4);
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Pending);
    let term = nodes[0].raft.term();
    nodes[0]
        .step(answer(MessageType::MsgHeartbeat, 2, 1, term + 1))
        .unwrap();
    assert_eq!(nodes[0].raft.state(), StateRole::Follower);
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::NotLeader);
    assert_eq!(nodes[0].catch_up(9).unwrap(), crate::CatchUp::NotLeader);
}

/// slates `docs/bugs/2026-09-30-one-lagging-member-held-back-every-council-promotion.md`: each
/// learner is judged alone. Learner 5 never answers and is given up; learner 4 catches up and is
/// ready, whatever 5 does; a voter is ready, and a stranger is no member.
#[test]
fn one_learner_that_cannot_catch_up_holds_back_none_that_has() {
    let mut nodes = group_of(4, 5);
    add_learner(&mut nodes, 4);
    add_learner(&mut nodes, 5);
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Pending);
    assert_eq!(nodes[0].catch_up(5).unwrap(), crate::CatchUp::Pending);
    rounds_among(&mut nodes, &[1, 2, 3, 4]);
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Ready);
    let election = nodes[0].raft.config().election_tick;
    for _ in 0..election {
        tick_with_voters(&mut nodes);
    }
    assert_eq!(nodes[0].catch_up(5).unwrap(), crate::CatchUp::Aborted);
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Ready);
    assert_eq!(nodes[0].catch_up(2).unwrap(), crate::CatchUp::Ready);
    assert_eq!(nodes[0].catch_up(9).unwrap(), crate::CatchUp::NotMember);
}

/// By suspicion the rounds are on the owner's clock, and an election is the time the law expects
/// one to take (`Timing::election`): a learner that never answers is given up once that much time
/// passes with its lag not shrinking, and not a nanosecond before.
#[test]
fn by_suspicion_a_learners_rounds_are_judged_on_the_owners_clock() {
    let timing = crate::Timing {
        span: std::time::Duration::from_millis(10),
        round: std::time::Duration::from_millis(2),
        // A delay within the span and one vote round.
        election: std::time::Duration::from_millis(12),
    };
    let mut nodes: Vec<RawNode<Memory>> = (1..=4)
        .map(|id| {
            let config = Config {
                elections: crate::Elections::Suspicion,
                ..config(id)
            };
            let mut node = RawNode::new(&config, Memory::with_voters(&[1, 2, 3])).unwrap();
            node.set_timing(timing).unwrap();
            node
        })
        .collect();
    nodes[0].campaign().unwrap();
    rounds_among(&mut nodes, &[1, 2, 3]);
    assert_eq!(nodes[0].raft.state(), StateRole::Leader);
    add_learner(&mut nodes, 4);
    let election = u64::try_from(timing.election.as_nanos()).unwrap();
    let start = 1_000_000_000;
    nodes[0].wake(start).unwrap();
    assert_eq!(nodes[0].catch_up(4).unwrap(), crate::CatchUp::Pending);
    for (at, expected) in [
        (start + election - 1, crate::CatchUp::Pending),
        (start + election, crate::CatchUp::Aborted),
    ] {
        nodes[0].wake(at).unwrap();
        rounds_among(&mut nodes, &[1, 2, 3]);
        assert_eq!(nodes[0].raft.state(), StateRole::Leader);
        assert_eq!(nodes[0].catch_up(4).unwrap(), expected, "at {at}");
    }
}

/// The thesis's Figure 4.4(a): the rounds of replication from voter 3's loss to the first commit
/// after it, where member 4 joined `staged` (a learner caught up first) or directly as a voter.
fn rounds_to_commit_after_a_loss(staged: bool) -> u64 {
    use crate::proto::ConfChangeType;
    let mut nodes = group_of(40, 4);
    let all = [1, 2, 3, 4];
    if staged {
        add_learner(&mut nodes, 4);
        let mut rounds = 0;
        while nodes[0].catch_up(4).unwrap() != crate::CatchUp::Ready {
            if !one_round(&mut nodes, &all) {
                nodes[0].ping().unwrap();
            }
            rounds += 1;
            assert!(rounds < 1_000, "the learner never caught up");
        }
    }
    nodes[0]
        .propose_conf_change(vec![], &single(ConfChangeType::AddNode, 4))
        .unwrap();
    let change = nodes[0].raft.log().last_index().unwrap();
    // Rounds among all four until the change commits, which three of 1 to 4 commit without 4;
    // then until all the leader holds is committed, as slates' test runs them.
    for until in [false, true] {
        let mut rounds = 0;
        loop {
            let held = nodes[0].raft.log().last_index().unwrap();
            let committed = nodes[0].raft.log().committed();
            let done = if until {
                committed == held
            } else {
                committed >= change
            };
            if done {
                break;
            }
            if !one_round(&mut nodes, &all) {
                nodes[0].ping().unwrap();
            }
            rounds += 1;
            assert!(rounds < 1_000, "the change never committed");
        }
    }
    // Voter 3 fails; the group of 1, 2 and 4 needs 4 for every commit.
    nodes[0].propose(vec![], b"after".to_vec()).unwrap();
    let index = nodes[0].raft.log().last_index().unwrap();
    let mut rounds = 0;
    while nodes[0].raft.log().committed() < index {
        if !one_round(&mut nodes, &[1, 2, 4]) {
            nodes[0].ping().unwrap();
        }
        rounds += 1;
        assert!(rounds < 1_000, "nothing committed after the loss");
    }
    rounds
}

/// slates' `a_staged_newcomer_leaves_no_availability_gap_where_a_direct_one_does`, the thesis's
/// Figure 4.4(a): voters 1, 2 and 3 hold forty entries, member 4 joins with an empty log and the
/// voters become 1 to 4, then 3 fails. A round here carries messages one way, so a round trip is
/// two, where slates' round was one, its reply taken at once. Added directly, 4 leaves the group
/// unable to commit for 44 rounds while it catches up, two entries an append and one out at a time
/// (slates: 21 round trips). The leader sends to 4 from the moment the change is in its log, as
/// slates' does, where it once waited until it applied the change: one round fewer (`docs/raft.md`
/// §3.5). Staged first as a learner and promoted once caught up, the group
/// commits in the first round trip after the loss (slates: one). Exact: the schedule is fixed.
#[test]
fn a_staged_newcomer_leaves_no_availability_gap_where_a_direct_one_does() {
    let direct = rounds_to_commit_after_a_loss(false);
    let staged = rounds_to_commit_after_a_loss(true);
    assert_eq!(
        staged, 2,
        "one round trip: the staged newcomer held the log already"
    );
    assert_eq!(
        direct, 44,
        "the direct newcomer's catch-up held commits back"
    );
}

// The configuration a member counts by is the newest its log holds (`docs/raft.md` §3.4).

/// A change of the configuration as an entry at `index` of `term`.
fn change_entry(index: u64, term: u64, change: &ConfChangeV2) -> Entry {
    use crate::wire::Record;
    Entry {
        entry_type: EntryType::EntryConfChangeV2,
        index,
        term,
        data: change.encode_to_vec(),
        ..Entry::default()
    }
}

/// A sole voter, elected alone.
fn sole_leader() -> RawNode<Memory> {
    let mut node = RawNode::new(&config(1), Memory::with_voters(&[1])).unwrap();
    node.campaign().unwrap();
    drain_applying(&mut node);
    assert_eq!(node.raft.state(), StateRole::Leader);
    node
}

/// A sole voter that adds a voter counts by both from the entry on: it commits neither the entry
/// nor anything after it alone, whether the change is of one voter or enters a joint
/// configuration. Counted by the configuration it had applied, it committed the entry alone, while
/// the voter it added counted elections by both (raft-dev, 2015; `docs/research/reconfiguration.md`).
#[test]
fn a_sole_voter_does_not_commit_past_a_voter_it_adds_alone() {
    use crate::proto::{ConfChangeTransition, ConfChangeType};
    for transition in [ConfChangeTransition::Auto, ConfChangeTransition::Explicit] {
        let mut node = sole_leader();
        let committed = node.raft.log().committed();
        let change = ConfChangeV2 {
            transition,
            ..single(ConfChangeType::AddNode, 2)
        };
        node.propose_conf_change(vec![], &change).unwrap();
        let at = node.raft.log().last_index().unwrap();
        node.propose(vec![], b"after".to_vec()).unwrap();
        drain_applying(&mut node);
        assert_eq!(node.raft.log().committed(), committed, "{transition:?}");
        assert!(node.raft.log().committed() < at);
        assert_eq!(node.raft.configuration().voters(), [1, 2], "{transition:?}");
        // What the owner was told is what it applied: nothing yet.
        assert_eq!(node.raft.applied_configuration().voters(), [1]);
    }
}

/// An append that replaces the entry of a change takes the configuration back to the one before it
/// (Ongaro's thesis §4.1: a server uses the latest configuration in its log, "whether or not the
/// entry is committed", and falls back when the entry is removed).
#[test]
fn an_append_that_replaces_a_change_takes_the_configuration_back() {
    use crate::proto::ConfChangeType;
    let mut node = follower();
    let mut append = answer(MessageType::MsgAppend, 1, 2, 1);
    append.index = 3;
    append.log_term = 1;
    append.commit = 2;
    append.entries = vec![change_entry(4, 1, &single(ConfChangeType::RemoveNode, 3))];
    node.step(append).unwrap();
    assert_eq!(node.raft.configuration().voters(), [1, 2]);
    // The leader of term 2 holds another entry at index 4.
    let mut append = answer(MessageType::MsgAppend, 1, 2, 2);
    append.index = 3;
    append.log_term = 1;
    append.commit = 2;
    append.entries = vec![entry(4, 2)];
    node.step(append).unwrap();
    assert_eq!(node.raft.log().last_index().unwrap(), 4);
    assert_eq!(node.raft.configuration().voters(), [1, 2, 3]);
    assert_eq!(node.raft.applied_configuration().voters(), [1, 2, 3]);
}

/// A joint configuration that leaves by itself is left once its entry is committed: the leader
/// writes the entry that leaves then, whether or not its owner has applied the joint one.
#[test]
fn a_joint_configuration_that_leaves_by_itself_is_left_once_its_entry_commits() {
    use crate::proto::{ConfChangeTransition, ConfChangeType};
    let mut nodes = led_three([0, 0, 0]);
    nodes[0].pause_apply();
    let change = ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode,
                node_id: 3,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::AddLearnerNode,
                node_id: 3,
            },
        ],
        ..ConfChangeV2::default()
    };
    nodes[0].propose_conf_change(vec![], &change).unwrap();
    let joint = nodes[0].raft.log().last_index().unwrap();
    assert!(nodes[0].raft.configuration().is_joint());
    exchange(&mut nodes, &[1, 2, 3]);
    assert!(nodes[0].raft.log().committed() >= joint);
    assert!(nodes[0].raft.log().applied() < joint);
    let leave = joint + 1;
    let written = nodes[0]
        .raft
        .log()
        .slice(leave, leave + 1, u64::MAX)
        .unwrap()
        .remove(0);
    assert_eq!(written.entry_type, EntryType::EntryConfChangeV2);
    assert!(
        written.data.is_empty(),
        "the entry that leaves states nothing"
    );
    assert!(!nodes[0].raft.configuration().is_joint());
    assert_eq!(nodes[0].raft.configuration().voters(), [1, 2]);
}

/// A leader whose newest configuration names one voter, another member, answers no read alone: it
/// leads until that configuration is committed, and the one voter may have been elected and
/// committed since (seed 47 of the hostile schedules answered a read below a commit made there).
#[test]
fn a_leader_the_newest_configuration_leaves_out_answers_no_read_alone() {
    use crate::proto::ConfChangeType;
    // A leader of {1, 2} that writes {2}: the one voter is another member.
    let mut node = RawNode::new(&config(1), Memory::with_voters(&[1, 2])).unwrap();
    node.campaign().unwrap();
    let mut asked = drain(&mut node);
    for message in asked.drain(..) {
        if message.msg_type == MessageType::MsgRequestPreVote {
            node.step(answer(
                MessageType::MsgRequestPreVoteResponse,
                2,
                1,
                message.term,
            ))
            .unwrap();
        }
    }
    for message in drain(&mut node) {
        if message.msg_type == MessageType::MsgRequestVote {
            node.step(answer(
                MessageType::MsgRequestVoteResponse,
                2,
                1,
                message.term,
            ))
            .unwrap();
        }
    }
    assert_eq!(node.raft.state(), StateRole::Leader);
    // Its first entry is committed with member 2.
    let first = node.raft.log().last_index().unwrap();
    drain(&mut node);
    let mut ack = answer(MessageType::MsgAppendResponse, 2, 1, node.raft.term());
    ack.index = first;
    node.step(ack).unwrap();
    drain(&mut node);
    assert!(node.raft.commit_to_current_term());
    node.propose_conf_change(vec![], &single(ConfChangeType::RemoveNode, 1))
        .unwrap();
    drain(&mut node);
    assert_eq!(node.raft.configuration().voters(), [2]);
    assert_eq!(node.raft.state(), StateRole::Leader);
    node.read_index(b"read".to_vec()).unwrap();
    let ready = node.ready().unwrap();
    assert!(ready.read_states().is_empty(), "answered alone");
}

/// A member a change adds takes a snapshot that does not name it: the change counts it from its
/// entry on, and the snapshot that seeds it may be older than the entry (a server processes what a
/// leader of its term sends without consulting its configuration, Ongaro's thesis §4.1).
#[test]
fn a_member_a_change_adds_takes_a_snapshot_older_than_the_change() {
    let mut node = RawNode::new(&config(4), Memory::with_voters(&[1, 2, 3])).unwrap();
    let mut seeded = answer(MessageType::MsgSnapshot, 1, 4, 1);
    seeded.snapshot = Some(Box::new(snapshot(5, 1, &[1, 2, 3])));
    node.step(seeded).unwrap();
    assert_eq!(node.raft.log().last_index().unwrap(), 5);
    assert_eq!(node.raft.log().committed(), 5);
    assert_eq!(node.raft.configuration().voters(), [1, 2, 3]);
}

/// A voter of the configuration before an uncommitted change that leaves it out may still be
/// needed: it campaigns, its own vote counting nowhere, and is elected by the voters the change
/// names (Ongaro's thesis §4.2.2). Seed 11 of the group schedules: the leader of a joint
/// configuration wrote the entry that leaves it and was lost; the members holding that entry were
/// voters of no configuration they counted by, and the one voter they named lacked the entry and
/// needed their votes, which its shorter log was refused: no member could be elected.
#[test]
fn a_voter_the_uncommitted_change_leaves_out_campaigns_and_is_elected_by_the_voters_it_names() {
    use crate::proto::ConfChangeType;
    let mut store = Memory::with_voters(&[1, 2, 3]);
    store.append(&[entry(1, 1), entry(2, 1)]);
    store.append(&[change_entry(3, 1, &single(ConfChangeType::RemoveNode, 1))]);
    store.hard_state = HardState {
        term: 1,
        vote: 0,
        commit: 2,
    };
    let mut node = RawNode::new(&config(1), store).unwrap();
    assert_eq!(node.raft.configuration().voters(), [2, 3]);
    assert!(node.raft.promotable());
    node.campaign().unwrap();
    let asked: Vec<u64> = drain(&mut node)
        .into_iter()
        .filter(|message| message.msg_type == MessageType::MsgRequestPreVote)
        .map(|message| message.to)
        .collect();
    assert_eq!(asked, vec![2, 3]);
    node.step(answer(MessageType::MsgRequestPreVoteResponse, 2, 1, 2))
        .unwrap();
    assert_eq!(
        node.raft.state(),
        StateRole::PreCandidate,
        "its own vote counts nowhere"
    );
    node.step(answer(MessageType::MsgRequestPreVoteResponse, 3, 1, 2))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Candidate);
    drain(&mut node);
    node.step(answer(MessageType::MsgRequestVoteResponse, 2, 1, 2))
        .unwrap();
    node.step(answer(MessageType::MsgRequestVoteResponse, 3, 1, 2))
        .unwrap();
    assert_eq!(node.raft.state(), StateRole::Leader);
    // Once the change is known committed, it is needed no more.
    let mut follower = RawNode::new(&config(1), {
        let mut store = Memory::with_voters(&[1, 2, 3]);
        store.append(&[entry(1, 1), entry(2, 1)]);
        store.append(&[change_entry(3, 1, &single(ConfChangeType::RemoveNode, 1))]);
        store.hard_state = HardState {
            term: 1,
            vote: 0,
            commit: 3,
        };
        store
    })
    .unwrap();
    assert!(!follower.raft.promotable());
    assert_eq!(follower.campaign(), Err(Error::NotPromotable));
}

/// The Reconfig model's shortest history for elections by the newest configuration and commitment
/// by the one applied (`docs/research/reconfiguration.md` §3, option (a)), on the core: 1 to 3 are
/// the voters, B (member 2) leads and writes the joint configuration that replaces A (member 1) by
/// D (member 4). Counted by what it applied, B committed the joint entry by A's acknowledgement;
/// A, elected by C and D under the joint configuration, then committed its own first entry by C's,
/// and B, back with the configuration that leaves A, was elected by D without it. The core counts
/// both commits by the joint configuration: neither is made.
#[test]
fn a_commit_counted_by_the_configuration_applied_is_not_made() {
    use crate::proto::{ConfChangeTransition, ConfChangeType};
    let mut nodes: Vec<RawNode<Memory>> = (1..=4)
        .map(|id| RawNode::new(&config(id), Memory::with_voters(&[1, 2, 3])).unwrap())
        .collect();
    nodes[1].campaign().unwrap();
    exchange(&mut nodes, &[1, 2]);
    assert_eq!(nodes[1].raft.state(), StateRole::Leader);
    let change = ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::AddNode,
                node_id: 4,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode,
                node_id: 1,
            },
        ],
        ..ConfChangeV2::default()
    };
    nodes[1].propose_conf_change(vec![], &change).unwrap();
    let joint = nodes[1].raft.log().last_index().unwrap();
    exchange(&mut nodes, &[1, 2]);
    assert_eq!(nodes[0].raft.log().last_index().unwrap(), joint);
    assert!(
        nodes[1].raft.log().committed() < joint,
        "the joint entry committed by A and B, a majority of the voters before it alone"
    );
    // A, holding the joint entry, is elected by C and D, and its first entry reaches C alone.
    nodes[0].campaign().unwrap();
    for _ in 0..4 {
        one_round(&mut nodes, &[1, 3, 4]);
    }
    assert_eq!(nodes[0].raft.state(), StateRole::Leader);
    let first = nodes[0].raft.log().last_index().unwrap();
    assert!(first > joint);
    exchange(&mut nodes, &[1, 3]);
    assert!(
        nodes[0].raft.log().committed() < first,
        "A's first entry committed by A and C, a majority of the voters before the change alone"
    );
}

/// A proposal dropped says why (`Dropped`): one with no entry, or whose change does not decode, is
/// the caller's bug; one a member cannot take now is to retry or redirect: a follower that knows no
/// leader, a candidate, a leader handing over, a leader the configuration it leads names no member,
/// a leader at its bound of what it holds uncommitted (`a_leader_holds_uncommitted_what_it_may_and_one_proposal_at_least`,
/// `a_change_that_cannot_be_read_is_not_proposed`).
#[test]
fn a_dropped_proposal_says_why() {
    use crate::Dropped;
    use crate::proto::ConfChangeType;
    // No entry.
    let mut node = leader();
    let empty = Message {
        msg_type: MessageType::MsgPropose,
        from: 1,
        to: 1,
        ..Message::default()
    };
    assert_eq!(
        node.step(empty),
        Err(Error::ProposalDropped(Dropped::Empty))
    );
    assert!(Dropped::Empty.is_callers_bug() && Dropped::Malformed.is_callers_bug());
    // A follower that knows no leader.
    let mut alone = RawNode::new(&config(2), Memory::with_voters(&[1, 2, 3])).unwrap();
    assert_eq!(
        alone.propose(vec![], b"x".to_vec()),
        Err(Error::ProposalDropped(Dropped::NoLeader))
    );
    // A candidate.
    let mut candidate = RawNode::new(&config(1), Memory::with_voters(&[1, 2, 3])).unwrap();
    candidate.campaign().unwrap();
    assert_eq!(candidate.raft.state(), StateRole::PreCandidate);
    assert_eq!(
        candidate.propose(vec![], b"x".to_vec()),
        Err(Error::ProposalDropped(Dropped::NoLeader))
    );
    // A leader handing over.
    let mut handing = reading_leader();
    handing
        .step(answer(MessageType::MsgTransferLeader, 3, 1, 1))
        .unwrap();
    assert_eq!(
        handing.propose(vec![], b"x".to_vec()),
        Err(Error::ProposalDropped(Dropped::Transferring))
    );
    // A leader that wrote its own removal leads until it is committed, and takes no proposal.
    let mut leaving = reading_leader();
    leaving
        .propose_conf_change(vec![], &single(ConfChangeType::RemoveNode, 1))
        .unwrap();
    assert!(!leaving.raft.configuration().contains(1));
    assert_eq!(
        leaving.propose(vec![], b"x".to_vec()),
        Err(Error::ProposalDropped(Dropped::NotMember))
    );
    // The fast track's: no data, and a member that knows no leader to propose by.
    let fast = Config {
        fast: true,
        ..config(2)
    };
    let mut proposer = RawNode::new(&fast, Memory::with_voters(&[1, 2, 3])).unwrap();
    assert_eq!(
        proposer.propose_fast(vec![], vec![]),
        Err(Error::ProposalDropped(Dropped::Empty))
    );
    assert_eq!(
        proposer.propose_fast(vec![], b"x".to_vec()),
        Err(Error::ProposalDropped(Dropped::NoLeader))
    );
    for reason in [
        Dropped::NoLeader,
        Dropped::Transferring,
        Dropped::NotMember,
        Dropped::Uncommitted,
    ] {
        assert!(!reason.is_callers_bug(), "{reason:?}");
    }
}

/// Member 2 of the highest priority holds three entries of term 1; member 3, of lower priority,
/// asks with two entries of term 2. By the log's precedence the voter grants: it could not be
/// elected instead. By raft-rs's (the tests' build alone) it refuses the shorter log, the refusal
/// that left swarm group seed 9,657 without a leader with every member up (`docs/raft.md` §3.3).
#[test]
fn a_voter_of_higher_priority_grants_a_log_more_current_however_short() {
    fn asked(raft_rs_precedence: bool) -> Message {
        let mut node = follower_with(Config {
            priority: 3,
            raft_rs_precedence,
            ..config(2)
        });
        let mut ask = answer(MessageType::MsgRequestVote, 3, 2, 3);
        ask.log_term = 2;
        ask.index = 2;
        ask.priority = 1;
        node.step(ask).unwrap();
        let mut answers: Vec<Message> = drain(&mut node)
            .into_iter()
            .filter(|message| message.msg_type == MessageType::MsgRequestVoteResponse)
            .collect();
        assert_eq!(answers.len(), 1);
        answers.remove(0)
    }
    assert!(!asked(false).reject);
    assert!(asked(true).reject);
}
