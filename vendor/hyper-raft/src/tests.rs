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
fn rounds(messages: &[Message]) -> Vec<(u64, Vec<u8>)> {
    messages
        .iter()
        .filter(|message| message.msg_type == MessageType::MsgHeartbeat)
        .map(|message| (message.to, message.context.clone()))
        .collect()
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
    heartbeat.context = vec![19];
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
    heartbeat.context = b"first".to_vec();
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
    late.context = b"first".to_vec();
    node.step(late).unwrap();
    assert_eq!(node.raft.pending_read_count(), 2);
    let mut heartbeat = answer(MessageType::MsgHeartbeatResponse, 3, 1, 1);
    heartbeat.context = b"third".to_vec();
    node.step(heartbeat).unwrap();
    assert_eq!(
        confirmed(&mut node),
        vec![b"second".to_vec(), b"third".to_vec()]
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
    heartbeat.context = b"lost".to_vec();
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

fn appends(messages: &[Message], to: u64) -> Vec<(u64, u64)> {
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
                message.entries.iter().map(proto::encoded_bytes).sum(),
            )
        })
        .collect()
}
fn window(node: &RawNode<Memory>, member: u64) -> (usize, u64) {
    let progress = node.raft.tracker().get(member).unwrap();
    (progress.inflights.count(), progress.inflights.bytes())
}

/// A member is sent no more bytes of entries ahead of its answers than the
/// path to it carries, whatever their sizes; a member that answers nothing
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
        // What is out passes the bound by one entry at most: the page that
        // took the last of the room.
        assert!(bytes < 1_000 + 710, "{bytes} bytes are out");
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

#[test]
fn one_told_to_campaign_before_it_applied_a_change_campaigns_once_it_has() {
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
    heartbeat.context = b"same".to_vec();
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
