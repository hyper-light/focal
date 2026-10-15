//! The fields focal's peers carry beyond raft-rs's (`Wire`, 27 §15.9): a refusal's `kept`
//! (field 17, R17), `lost` (field 18) and a leader's `classic` (field 19), carried only once the upgrade fence opens
//! `RAFT_KEPT_LEVEL`. Their bytes; raft-rs's own reader skipping them; the reader below the fence
//! skipping `kept` and refusing `lost`; and groups whose members raise their wire at different
//! moments, across a leader change and with the fence raised during traffic, converging with
//! nothing lost or applied twice.
use crate::envelope::{EnvelopeError, decode_message_in, encode_message_in};
use crate::{DurableNode, Message, MessageType, NodeConfig, StateRole, Wire, tests::config};
use hyper_raft::proto;
use raft_proto::eraftpb;
use raft_proto::protocompat::PbMessageExt as _;

/// A refusal of an append, as a member that kept it ahead of a hole sends it.
fn kept_refusal() -> Message {
    let mut message = proto::message(1, MessageType::MsgAppendResponse);
    message.from = 3;
    message.term = 7;
    message.index = 12;
    message.reject = true;
    message.reject_hint = 9;
    message.log_term = 6;
    message.kept = true;
    message
}

#[test]
fn kept_and_lost_have_fields_of_their_own() {
    let message = kept_refusal();
    let frozen = {
        let mut plain = message.clone();
        plain.kept = false;
        encode_message_in(&plain, Wire::Frozen).unwrap()
    };
    let kept = encode_message_in(&message, Wire::Kept).unwrap();
    // Field 17, a varint: the key (17 << 3 | 0 = 136) in two bytes, then 1.
    assert_eq!(&kept[..frozen.len()], &frozen[..]);
    assert_eq!(&kept[frozen.len()..], &[0x88, 0x01, 0x01]);
    let mut lost = message.clone();
    lost.kept = false;
    lost.lost = true;
    let lost_bytes = encode_message_in(&lost, Wire::Kept).unwrap();
    // Field 18: the key 18 << 3 = 144.
    assert_eq!(&lost_bytes[frozen.len()..], &[0x90, 0x01, 0x01]);
    // Below the fence neither is written: a message that holds one is refused, never dropped.
    assert_eq!(
        encode_message_in(&message, Wire::Frozen),
        Err(EnvelopeError::Unstated("an append kept ahead of a hole"))
    );
    assert_eq!(
        encode_message_in(&lost, Wire::Frozen),
        Err(EnvelopeError::Unstated("a member's mark"))
    );
}

/// raft-rs's own reader (raft-proto at `8e4cef1`) skips fields 17 and 18: it reads a kept refusal
/// as the refusal raft-rs itself sends, which is what a member below the fence takes it for.
#[test]
fn raft_rs_reads_a_kept_refusal_as_its_own() {
    let message = kept_refusal();
    let bytes = encode_message_in(&message, Wire::Kept).unwrap();
    let mut read = eraftpb::Message::default();
    read.merge_from_bytes(&bytes).unwrap();
    let mut plain = message.clone();
    plain.kept = false;
    let raft_rs = read.write_to_bytes().unwrap();
    assert_eq!(raft_rs, encode_message_in(&plain, Wire::Frozen).unwrap());
    assert!(read.reject);
    assert_eq!((read.index, read.reject_hint, read.log_term), (12, 9, 6));
}

#[test]
fn below_the_fence_kept_is_skipped_and_lost_refused() {
    let message = kept_refusal();
    let bytes = encode_message_in(&message, Wire::Kept).unwrap();
    let below = decode_message_in(&bytes, Wire::Frozen).unwrap();
    let mut plain = message.clone();
    plain.kept = false;
    assert_eq!(below, plain, "raft-rs's refusal, field 17 skipped");
    assert_eq!(decode_message_in(&bytes, Wire::Kept).unwrap(), message);
    let mut lost = message.clone();
    lost.kept = false;
    lost.lost = true;
    let lost_bytes = encode_message_in(&lost, Wire::Kept).unwrap();
    assert_eq!(
        decode_message_in(&lost_bytes, Wire::Frozen),
        Err(EnvelopeError::Unstated("a member's mark"))
    );
    assert_eq!(decode_message_in(&lost_bytes, Wire::Kept).unwrap(), lost);
}

/// A leader's classic commit (hyper-raft `Message::classic`) is field 19 at or above the fence. Below
/// it the field is left out, not refused, since every append of a leader holds one, and a reader
/// below the fence skips it: either way the member reads nothing known, and releases nothing it
/// approved by itself, which costs room and never safety. raft-rs's reader skips it too.
#[test]
fn a_leaders_classic_commit_is_field_19_and_nothing_below_the_fence() {
    let mut append = proto::message(2, MessageType::MsgAppend);
    append.from = 1;
    append.term = 4;
    append.commit = 30;
    append.classic = Some(25);
    let mut plain = append.clone();
    plain.classic = None;
    let frozen = encode_message_in(&append, Wire::Frozen).unwrap();
    assert_eq!(frozen, encode_message_in(&plain, Wire::Frozen).unwrap());
    let kept = encode_message_in(&append, Wire::Kept).unwrap();
    // Field 19, a varint: the key (19 << 3 | 0 = 152) in two bytes, then 25.
    assert_eq!(&kept[..frozen.len()], &frozen[..]);
    assert_eq!(&kept[frozen.len()..], &[0x98, 0x01, 25]);
    assert_eq!(decode_message_in(&kept, Wire::Kept).unwrap(), append);
    assert_eq!(decode_message_in(&kept, Wire::Frozen).unwrap(), plain);
    assert_eq!(decode_message_in(&frozen, Wire::Kept).unwrap(), plain);
    let mut read = eraftpb::Message::default();
    read.merge_from_bytes(&kept).unwrap();
    assert_eq!(read.write_to_bytes().unwrap(), frozen);
    // Nothing known and a classic commit of zero are one: neither takes a byte.
    let mut zero = append.clone();
    zero.classic = Some(0);
    assert_eq!(encode_message_in(&zero, Wire::Kept).unwrap(), frozen);
}

/// Members whose entries are at most 1 KiB, so that an append of a page (the entry's bytes and
/// 1 KiB) carries one entry of 1,000 bytes and never two.
fn small(id: u64) -> NodeConfig {
    let mut config = config(id);
    config.voters = vec![1, 2, 3];
    config.max_entry_bytes = 1024;
    config
}

/// Delivery rounds a group is given to converge once nothing more is lost: a hole costs one
/// refusal, one step back and resend, one answer and one commit (27 §15.9), and a leader change
/// its votes and the new leader's first append; four times that bounds every schedule here.
const ROUNDS: usize = 32;

struct Group {
    _dirs: Vec<tempfile::TempDir>,
    nodes: Vec<DurableNode>,
    applied: Vec<Vec<Vec<u8>>>,
    net: Vec<Message>,
}

impl Group {
    fn new() -> Self {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let nodes = dirs
            .iter()
            .enumerate()
            .map(|(i, dir)| DurableNode::open(small(i as u64 + 1), dir.path()).unwrap())
            .collect();
        let mut group = Self {
            _dirs: dirs,
            nodes,
            applied: vec![Vec::new(); 3],
            net: Vec::new(),
        };
        group.nodes[0].campaign().unwrap();
        group.settle(|_| true);
        assert_eq!(group.nodes[0].status().role, StateRole::Leader);
        group
    }
    fn drain(&mut self) {
        for (i, node) in self.nodes.iter_mut().enumerate() {
            let events = node.drain().unwrap();
            self.applied[i].extend(events.committed.into_iter().map(|entry| entry.data));
            self.net.extend(events.messages);
        }
    }
    /// One message, encoded under its sender's wire and read under its receiver's.
    fn deliver(&mut self, message: Message) {
        let from = (message.from - 1) as usize;
        let to = (message.to - 1) as usize;
        let bytes = encode_message_in(&message, self.nodes[from].wire()).unwrap();
        self.nodes[to]
            .step_authenticated(message.from, &bytes)
            .unwrap();
    }
    /// One round: everything on its way that `carried` admits is delivered, the rest lost.
    fn round(&mut self, carried: impl Fn(&Message) -> bool) -> bool {
        self.drain();
        let sent = std::mem::take(&mut self.net);
        let any = !sent.is_empty();
        for message in sent.into_iter().filter(|message| carried(message)) {
            self.deliver(message);
        }
        any
    }
    /// Rounds until nothing is sent; the rounds it took.
    fn settle(&mut self, carried: impl Fn(&Message) -> bool) -> usize {
        for rounds in 0..ROUNDS {
            if !self.round(&carried) {
                return rounds;
            }
        }
        panic!("the group did not settle in {ROUNDS} rounds");
    }
    /// The terms of a member's log, index by index.
    fn terms(&self, node: usize) -> Vec<u64> {
        let raw = &self.nodes[node].log().raw;
        let last = raw.raft.log().last_index().unwrap();
        (1..=last)
            .map(|index| raw.raft.log().term(index).unwrap())
            .collect()
    }
    fn leader(&self) -> usize {
        (0..3)
            .find(|&node| self.nodes[node].status().role == StateRole::Leader)
            .unwrap()
    }
    /// Every member holds the leader's log and has applied what it committed, each entry once.
    fn converged(&self) {
        let leader = self.leader();
        let committed = self.nodes[leader].status().committed_index;
        for node in 0..3 {
            assert_eq!(
                self.terms(node),
                self.terms(leader),
                "member {}'s log",
                node + 1
            );
            assert_eq!(self.nodes[node].status().committed_index, committed);
            assert_eq!(
                self.applied[node],
                self.applied[leader],
                "member {}",
                node + 1
            );
        }
    }
}

/// `n` proposals of `tag` at the leader, each its own append.
fn propose(group: &mut Group, tag: u8, n: u8) {
    let leader = group.leader();
    propose_at(group, leader, tag, n);
}

/// `n` proposals of `tag` at member `leader + 1`.
fn propose_at(group: &mut Group, leader: usize, tag: u8, n: u8) {
    for i in 0..n {
        let mut data = vec![tag; 1_000];
        data[0] = i;
        group.nodes[leader].propose(data).unwrap();
    }
}

/// Only appends to member 3 that begin past its log's end arrive: the first of a batch is lost,
/// the rest arrive ahead of the hole it leaves.
fn ahead_of_a_hole(group: &mut Group) -> usize {
    group.drain();
    let leader = group.leader();
    let end = group.nodes[2].log().raw.raft.log().last_index().unwrap();
    let to_three: Vec<Message> = group
        .net
        .iter()
        .filter(|message| {
            message.to == 3
                && message.msg_type == MessageType::MsgAppend
                && message
                    .entries
                    .first()
                    .is_some_and(|entry| entry.index > end + 1)
        })
        .cloned()
        .collect();
    group.net.retain(|message| message.to != 3);
    let others = std::mem::take(&mut group.net);
    for message in others {
        group.deliver(message);
    }
    let arrived = to_three.len();
    for message in to_three {
        group.deliver(message);
    }
    let _ = leader;
    arrived
}

/// A member raised to `Wire::Kept` keeps appends ahead of its hole and says so; its leader, not
/// yet raised, reads the refusal as raft-rs's, steps back and sends the hole again. The group
/// converges, nothing lost or applied twice.
#[test]
fn a_member_raised_before_its_leader_costs_a_resend() {
    let mut group = Group::new();
    group.nodes[2].set_raft_wire(Wire::Kept).unwrap();
    propose(&mut group, b'a', 4);
    assert!(
        ahead_of_a_hole(&mut group) > 0,
        "appends arrived ahead of the hole"
    );
    assert!(
        group.nodes[2].log().raw.raft.kept_ahead().count() > 0,
        "member 3 kept them"
    );
    let rounds = group.settle(|_| true);
    assert!(rounds <= ROUNDS);
    group.converged();
    assert_eq!(
        group.applied[0]
            .iter()
            .filter(|data| data[1] == b'a')
            .count(),
        4
    );
}

/// Member 3, raised, keeps what term t's leader sent ahead of its hole. That leader leaves; the
/// member elected in t+1, not raised, takes the place. A change of term forgets what was kept
/// (hyper-raft `Raft::reset`), so nothing of term t that the new leader lacked survives on member
/// 3, and the group converges on the new leader's log.
#[test]
fn what_was_kept_from_a_former_leader_does_not_survive_its_term() {
    let mut group = Group::new();
    group.nodes[2].set_raft_wire(Wire::Kept).unwrap();
    group.settle(|_| true);
    propose(&mut group, b'o', 4);
    // Term t's appends reach member 3 alone, ahead of its hole; member 2 hears none of them.
    group.drain();
    let end = group.nodes[2].log().raw.raft.log().last_index().unwrap();
    let ahead: Vec<Message> = group
        .net
        .iter()
        .filter(|message| {
            message.to == 3
                && message.msg_type == MessageType::MsgAppend
                && message
                    .entries
                    .first()
                    .is_some_and(|entry| entry.index > end + 1)
        })
        .cloned()
        .collect();
    group.net.clear();
    assert!(!ahead.is_empty());
    for message in ahead {
        group.deliver(message);
    }
    assert!(group.nodes[2].log().raw.raft.kept_ahead().count() > 0);
    group.drain();
    group.net.clear();
    // The leader leaves; member 2 is elected in t+1.
    let away = |message: &Message| message.from != 1 && message.to != 1;
    for _ in 0..2 * small(2).election_tick {
        group.nodes[2].tick().unwrap();
    }
    group.drain();
    group.net.clear();
    group.nodes[1].campaign().unwrap();
    group.settle(away);
    assert_eq!(group.nodes[1].status().role, StateRole::Leader);
    assert_eq!(
        group.nodes[2].log().raw.raft.kept_ahead().count(),
        0,
        "forgotten with the term"
    );
    // The former leader, cut off, has heard of no later term and still takes itself for leader:
    // the proposals go to member 2.
    propose_at(&mut group, 1, b'n', 3);
    group.settle(away);
    let leader_terms = group.terms(1);
    assert_eq!(group.terms(2), leader_terms);
    assert_eq!(group.applied[2], group.applied[1]);
    assert!(
        !group.applied[2].iter().any(|data| data[1] == b'o'),
        "nothing of term t the new leader lacked"
    );
    assert_eq!(
        group.applied[2]
            .iter()
            .filter(|data| data[1] == b'n')
            .count(),
        3
    );
}

/// The fence is raised during traffic: members switch to `Wire::Kept` one at a time while appends
/// are lost, and the group applies every proposal once, in order, on every member.
#[test]
fn the_fence_raised_during_traffic_loses_and_repeats_nothing() {
    let mut group = Group::new();
    let mut proposed = 0;
    for (batch, raise) in [
        (0u8, None),
        (1, Some(2)),
        (2, Some(1)),
        (3, Some(0)),
        (4, None),
    ] {
        if let Some(node) = raise {
            group.nodes[node].set_raft_wire(Wire::Kept).unwrap();
        }
        propose(&mut group, b'0' + batch, 4);
        proposed += 4;
        ahead_of_a_hole(&mut group);
        group.settle(|_| true);
    }
    group.converged();
    for node in 0..3 {
        let ours: Vec<&Vec<u8>> = group.applied[node]
            .iter()
            .filter(|data| data.len() == 1_000)
            .collect();
        assert_eq!(ours.len(), proposed, "member {}", node + 1);
        let order: Vec<(u8, u8)> = ours.iter().map(|data| (data[1], data[0])).collect();
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(order, sorted, "in order, each once");
    }
}
