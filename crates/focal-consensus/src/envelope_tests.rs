//! The envelope held to raft-rs's own codec (raft-proto 0.7 at `8e4cef1`, prost): every value it
//! generates is written to raft-proto's bytes exactly and read back to itself, what raft-proto
//! writes is read to the value it states, a value read from any bytes is the value raft-proto
//! reads from them, and arbitrary bytes never unwind.
use super::*;
use envelope::{
    decode_conf_change, decode_conf_change_v2, decode_entry, decode_hard_state, decode_snapshot,
    encode_entry, encode_hard_state, encode_snapshot,
};
use focal_sim::Seeded;
use raft_proto::eraftpb;
use raft_proto::protocompat::PbMessageExt as _;

/// Values generated per kind: a power of two past every field's choice of
/// present, absent and edge values, so that every combination recurs.
const VALUES: u64 = 4096;

fn pb_message_kind(kind: MessageType) -> i32 {
    match kind {
        MessageType::MsgFastPropose => 100,
        MessageType::MsgFastVote => 101,
        other => other_kind(other),
    }
}

fn other_kind(kind: MessageType) -> i32 {
    use eraftpb::MessageType as Pb;
    (match kind {
        MessageType::MsgHup => Pb::MsgHup,
        MessageType::MsgBeat => Pb::MsgBeat,
        MessageType::MsgPropose => Pb::MsgPropose,
        MessageType::MsgAppend => Pb::MsgAppend,
        MessageType::MsgAppendResponse => Pb::MsgAppendResponse,
        MessageType::MsgRequestVote => Pb::MsgRequestVote,
        MessageType::MsgRequestVoteResponse => Pb::MsgRequestVoteResponse,
        MessageType::MsgSnapshot => Pb::MsgSnapshot,
        MessageType::MsgHeartbeat => Pb::MsgHeartbeat,
        MessageType::MsgHeartbeatResponse => Pb::MsgHeartbeatResponse,
        MessageType::MsgUnreachable => Pb::MsgUnreachable,
        MessageType::MsgSnapStatus => Pb::MsgSnapStatus,
        MessageType::MsgCheckQuorum => Pb::MsgCheckQuorum,
        MessageType::MsgTransferLeader => Pb::MsgTransferLeader,
        MessageType::MsgTimeoutNow => Pb::MsgTimeoutNow,
        MessageType::MsgReadIndex => Pb::MsgReadIndex,
        MessageType::MsgReadIndexResp => Pb::MsgReadIndexResp,
        MessageType::MsgRequestPreVote => Pb::MsgRequestPreVote,
        MessageType::MsgRequestPreVoteResponse => Pb::MsgRequestPreVoteResponse,
        MessageType::MsgFastPropose | MessageType::MsgFastVote => unreachable!(),
    }) as i32
}

const MESSAGE_KINDS: [MessageType; 21] = [
    MessageType::MsgHup,
    MessageType::MsgBeat,
    MessageType::MsgPropose,
    MessageType::MsgAppend,
    MessageType::MsgAppendResponse,
    MessageType::MsgRequestVote,
    MessageType::MsgRequestVoteResponse,
    MessageType::MsgSnapshot,
    MessageType::MsgHeartbeat,
    MessageType::MsgHeartbeatResponse,
    MessageType::MsgUnreachable,
    MessageType::MsgSnapStatus,
    MessageType::MsgCheckQuorum,
    MessageType::MsgTransferLeader,
    MessageType::MsgTimeoutNow,
    MessageType::MsgReadIndex,
    MessageType::MsgReadIndexResp,
    MessageType::MsgRequestPreVote,
    MessageType::MsgRequestPreVoteResponse,
    MessageType::MsgFastPropose,
    MessageType::MsgFastVote,
];

fn pb_change_kind(kind: ConfChangeType) -> i32 {
    match kind {
        ConfChangeType::AddNode => eraftpb::ConfChangeType::AddNode as i32,
        ConfChangeType::RemoveNode => eraftpb::ConfChangeType::RemoveNode as i32,
        ConfChangeType::AddLearnerNode => eraftpb::ConfChangeType::AddLearnerNode as i32,
    }
}

fn pb_transition(kind: ConfChangeTransition) -> i32 {
    match kind {
        ConfChangeTransition::Auto => eraftpb::ConfChangeTransition::Auto as i32,
        ConfChangeTransition::Implicit => eraftpb::ConfChangeTransition::Implicit as i32,
        ConfChangeTransition::Explicit => eraftpb::ConfChangeTransition::Explicit as i32,
    }
}

fn pb_entry_kind(kind: EntryType) -> i32 {
    match kind {
        EntryType::EntryNormal => eraftpb::EntryType::EntryNormal as i32,
        EntryType::EntryConfChange => eraftpb::EntryType::EntryConfChange as i32,
        EntryType::EntryConfChangeV2 => eraftpb::EntryType::EntryConfChangeV2 as i32,
    }
}

/// An entry's data as raft-rs writes it: a change of configuration's record of the core's
/// read and written as raft-rs's protocol buffer.
fn pb_data(entry: &Entry) -> Vec<u8> {
    if entry.data.is_empty() {
        return Vec::new();
    }
    match entry.entry_type {
        EntryType::EntryNormal => entry.data.clone(),
        EntryType::EntryConfChange => {
            let change = ConfChange::decode(&entry.data).unwrap();
            eraftpb::ConfChange {
                change_type: pb_change_kind(change.change_type),
                node_id: change.node_id,
                context: change.context,
                id: 0,
            }
            .write_to_bytes()
            .unwrap()
        }
        EntryType::EntryConfChangeV2 => {
            let change = ConfChangeV2::decode(&entry.data).unwrap();
            eraftpb::ConfChangeV2 {
                transition: pb_transition(change.transition),
                changes: change
                    .changes
                    .iter()
                    .map(|single| eraftpb::ConfChangeSingle {
                        change_type: pb_change_kind(single.change_type),
                        node_id: single.node_id,
                    })
                    .collect(),
                context: change.context,
            }
            .write_to_bytes()
            .unwrap()
        }
    }
}

fn pb_entry(entry: &Entry) -> eraftpb::Entry {
    eraftpb::Entry {
        entry_type: pb_entry_kind(entry.entry_type),
        term: entry.term,
        index: entry.index,
        data: pb_data(entry),
        context: entry.context.clone(),
        sync_log: false,
    }
}

fn pb_conf_state(state: &ConfState) -> eraftpb::ConfState {
    eraftpb::ConfState {
        voters: state.voters.clone(),
        learners: state.learners.clone(),
        voters_outgoing: state.voters_outgoing.clone(),
        learners_next: state.learners_next.clone(),
        auto_leave: state.auto_leave,
    }
}

fn pb_snapshot(snapshot: &Snapshot) -> eraftpb::Snapshot {
    eraftpb::Snapshot {
        data: snapshot.data.clone(),
        metadata: snapshot
            .metadata
            .as_ref()
            .map(|metadata| eraftpb::SnapshotMetadata {
                conf_state: metadata.conf_state.as_ref().map(pb_conf_state),
                index: metadata.index,
                term: metadata.term,
            }),
    }
}

/// A message as focal's core wrote it: a vote request's positive priority in both fields.
fn pb_message(message: &Message) -> eraftpb::Message {
    let votes = matches!(
        message.msg_type,
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
    );
    eraftpb::Message {
        msg_type: pb_message_kind(message.msg_type),
        to: message.to,
        from: message.from,
        term: message.term,
        log_term: message.log_term,
        index: message.index,
        entries: message.entries.iter().map(pb_entry).collect(),
        commit: message.commit,
        commit_term: message.commit_term,
        snapshot: message.snapshot.as_deref().map(pb_snapshot),
        request_snapshot: message.request_snapshot,
        reject: message.reject,
        reject_hint: message.reject_hint,
        context: message.context.clone(),
        deprecated_priority: if votes {
            u64::try_from(message.priority).unwrap_or(0)
        } else {
            0
        },
        priority: message.priority,
    }
}

/// A number from each class a varint has: zero, one byte, a byte boundary, past 32 bits, the
/// largest.
fn number(rng: &mut Seeded) -> u64 {
    match rng.below(6) {
        0 => 0,
        1 => rng.below(0x80),
        2 => 0x80 + rng.below(0x3f80),
        3 => rng.below(1 << 32),
        4 => rng.next_u64(),
        _ => u64::MAX - rng.below(2),
    }
}

fn bytes(rng: &mut Seeded) -> Vec<u8> {
    let length = match rng.below(4) {
        0 => 0,
        1 => rng.below(4),
        2 => 127 + rng.below(3),
        _ => rng.below(300),
    };
    (0..length).map(|_| rng.below(256) as u8).collect()
}

fn ids(rng: &mut Seeded) -> Vec<u64> {
    (0..rng.below(5)).map(|_| number(rng)).collect()
}

fn conf_state(rng: &mut Seeded) -> ConfState {
    ConfState {
        voters: ids(rng),
        learners: ids(rng),
        voters_outgoing: ids(rng),
        learners_next: ids(rng),
        auto_leave: rng.below(2) == 1,
    }
}

fn change_kind(rng: &mut Seeded) -> ConfChangeType {
    [
        ConfChangeType::AddNode,
        ConfChangeType::RemoveNode,
        ConfChangeType::AddLearnerNode,
    ][rng.below(3) as usize]
}

fn entry(rng: &mut Seeded) -> Entry {
    let entry_type = [
        EntryType::EntryNormal,
        EntryType::EntryConfChange,
        EntryType::EntryConfChangeV2,
    ][rng.below(3) as usize];
    let data = match (entry_type, rng.below(4)) {
        (_, 0) => Vec::new(),
        (EntryType::EntryNormal, _) => bytes(rng),
        (EntryType::EntryConfChange, _) => ConfChange {
            change_type: change_kind(rng),
            node_id: number(rng),
            context: bytes(rng),
        }
        .encode_to_vec(),
        (EntryType::EntryConfChangeV2, _) => ConfChangeV2 {
            transition: [
                ConfChangeTransition::Auto,
                ConfChangeTransition::Implicit,
                ConfChangeTransition::Explicit,
            ][rng.below(3) as usize],
            changes: (0..rng.below(4))
                .map(|_| ConfChangeSingle {
                    change_type: change_kind(rng),
                    node_id: number(rng),
                })
                .collect(),
            context: bytes(rng),
        }
        .encode_to_vec(),
    };
    Entry {
        entry_type,
        term: number(rng),
        index: number(rng),
        data,
        context: bytes(rng),
    }
}

fn snapshot(rng: &mut Seeded) -> Snapshot {
    Snapshot {
        data: bytes(rng),
        metadata: (rng.below(4) != 0).then(|| SnapshotMetadata {
            conf_state: (rng.below(3) != 0).then(|| conf_state(rng)),
            index: number(rng),
            term: number(rng),
        }),
    }
}

fn message(rng: &mut Seeded) -> Message {
    let priority = match rng.below(4) {
        0 => 0,
        1 => i64::try_from(rng.below(1 << 40)).unwrap(),
        2 => -i64::try_from(rng.below(1 << 40)).unwrap() - 1,
        _ => rng.next_u64() as i64,
    };
    Message {
        msg_type: MESSAGE_KINDS[rng.below(MESSAGE_KINDS.len() as u64) as usize],
        to: number(rng),
        from: number(rng),
        term: number(rng),
        log_term: number(rng),
        index: number(rng),
        entries: (0..rng.below(4)).map(|_| entry(rng)).collect(),
        commit: number(rng),
        commit_term: number(rng),
        snapshot: (rng.below(3) == 0).then(|| Box::new(snapshot(rng))),
        request_snapshot: number(rng),
        reject: rng.below(2) == 1,
        lost: false,
        reject_hint: number(rng),
        context: bytes(rng),
        priority,
    }
}

/// `entry` as it comes back from the log: a record of the empty change is no bytes there, which
/// the core reads as the same change.
fn logged(entry: &Entry) -> Entry {
    let empty = match entry.entry_type {
        EntryType::EntryNormal => false,
        EntryType::EntryConfChange => {
            entry.data.is_empty()
                || ConfChange::decode(&entry.data).unwrap() == ConfChange::default()
        }
        EntryType::EntryConfChangeV2 => {
            entry.data.is_empty()
                || ConfChangeV2::decode(&entry.data).unwrap() == ConfChangeV2::default()
        }
    };
    Entry {
        data: if empty {
            Vec::new()
        } else {
            entry.data.clone()
        },
        ..entry.clone()
    }
}

fn logged_message(message: &Message) -> Message {
    Message {
        entries: message.entries.iter().map(logged).collect(),
        ..message.clone()
    }
}

/// What raft-rs reads from bytes, as the core's message; none where raft-rs refuses them or
/// names a kind the core does not.
fn prost_message(bytes: &[u8]) -> Option<Message> {
    let mut read = eraftpb::Message::default();
    read.merge_from_bytes(bytes).ok()?;
    // The oracle's value, written by the envelope's own reading of raft-proto's canonical bytes.
    decode_message(&read.write_to_bytes().ok()?).ok()
}

#[test]
fn every_value_is_written_as_raft_rs_writes_it_and_read_back_to_itself() {
    let mut rng = Seeded::new(0x0e17_e10f);
    for _ in 0..VALUES {
        let entry = entry(&mut rng);
        let written = encode_entry(&entry).unwrap();
        assert_eq!(
            written,
            pb_entry(&entry).write_to_bytes().unwrap(),
            "{entry:?}"
        );
        assert_eq!(decode_entry(&written).unwrap(), logged(&entry));

        let state = HardState {
            term: number(&mut rng),
            vote: number(&mut rng),
            commit: number(&mut rng),
        };
        let written = encode_hard_state(&state).unwrap();
        let pb = eraftpb::HardState {
            term: state.term,
            vote: state.vote,
            commit: state.commit,
        };
        assert_eq!(written, pb.write_to_bytes().unwrap());
        assert_eq!(decode_hard_state(&written).unwrap(), state);

        let snapshot = snapshot(&mut rng);
        let written = encode_snapshot(&snapshot).unwrap();
        assert_eq!(written, pb_snapshot(&snapshot).write_to_bytes().unwrap());
        assert_eq!(decode_snapshot(&written).unwrap(), snapshot);

        let message = message(&mut rng);
        let written = encode_message(&message).unwrap();
        assert_eq!(
            written,
            pb_message(&message).write_to_bytes().unwrap(),
            "{message:?}"
        );
        assert_eq!(envelope::message_len(&message).unwrap(), written.len());
        assert_eq!(decode_message(&written).unwrap(), logged_message(&message));
    }
}

/// A change of configuration read from raft-rs's bytes is the change, and the empty change is
/// no bytes in either encoding.
#[test]
fn changes_read_from_raft_rs_bytes() {
    let mut rng = Seeded::new(7);
    for _ in 0..VALUES {
        let single = ConfChange {
            change_type: change_kind(&mut rng),
            node_id: number(&mut rng),
            context: bytes(&mut rng),
        };
        let pb = eraftpb::ConfChange {
            change_type: pb_change_kind(single.change_type),
            node_id: single.node_id,
            context: single.context.clone(),
            // raft-rs's unused id, which the core has no place for: read past.
            id: number(&mut rng),
        };
        assert_eq!(
            decode_conf_change(&pb.write_to_bytes().unwrap()).unwrap(),
            single
        );
    }
    assert_eq!(decode_conf_change_v2(&[]).unwrap(), ConfChangeV2::default());
    assert_eq!(decode_conf_change(&[]).unwrap(), ConfChange::default());
}

/// Bytes raft-rs's own reader takes but its writer does not make: fields out of order, a
/// scalar given twice (the last wins), a message field given twice (merged), numbers unpacked,
/// fields no message has (skipped by their wire type), raft-rs's older priority alone.
#[test]
fn what_any_protocol_buffer_writer_may_write_reads_as_raft_rs_reads_it() {
    let canonical = [
        // to 5 (twice: 4 then 5), from 1 before to, kind MsgRequestVote last.
        &[0x18, 1, 0x10, 4, 0x10, 5, 0x08, 5][..],
        // an unknown varint field 99, an unknown fixed64 field 98, an unknown fixed32 field 97,
        // an unknown length-delimited field 96, then term 3.
        &[
            0x98, 0x06, 1, 0x91, 0x06, 1, 2, 3, 4, 5, 6, 7, 8, 0x8d, 0x06, 1, 2, 3, 4, 0x82, 0x06,
            2, 9, 9, 0x20, 3,
        ][..],
        // the older priority alone: 9.
        &[0x08, 5, 0x70, 9][..],
        // a snapshot given twice: its data then its metadata's index, merged.
        &[0x4a, 3, 0x0a, 1, 7, 0x4a, 4, 0x12, 2, 0x10, 6][..],
        // a snapshot whose configuration lists voters unpacked, then packed.
        &[0x4a, 10, 0x12, 8, 0x0a, 6, 0x08, 1, 0x0a, 2, 2, 3][..],
    ];
    for bytes in canonical {
        let ours = decode_message(bytes).unwrap();
        assert_eq!(Some(ours), prost_message(bytes), "{bytes:?}");
    }
    assert_eq!(decode_message(&[0x08, 5, 0x70, 9]).unwrap().priority, 9);
    let merged = decode_message(&[0x4a, 3, 0x0a, 1, 7, 0x4a, 4, 0x12, 2, 0x10, 6]).unwrap();
    let snapshot = merged.snapshot.unwrap();
    assert_eq!(
        (snapshot.data.as_slice(), metadata_of(&snapshot).index),
        (&[7u8][..], 6)
    );
}

/// What the envelope reads from any bytes is what raft-rs reads from them: every prefix of every
/// generated message, and every message with one byte changed. Neither unwinds.
#[test]
fn what_the_envelope_reads_from_any_bytes_raft_rs_reads_alike() {
    let mut rng = Seeded::new(0x5eed);
    for _ in 0..VALUES / 8 {
        let written = encode_message(&message(&mut rng)).unwrap();
        let mut variants: Vec<Vec<u8>> = (0..written.len())
            .map(|cut| written[..cut].to_vec())
            .collect();
        for _ in 0..8 {
            if written.is_empty() {
                break;
            }
            let mut changed = written.clone();
            let at = rng.below(changed.len() as u64) as usize;
            changed[at] = rng.below(256) as u8;
            variants.push(changed);
        }
        for bytes in variants {
            if let Ok(ours) = envelope::decode_message(&bytes) {
                assert_eq!(Some(ours), prost_message(&bytes), "{bytes:?}");
            }
        }
    }
}

/// A kind the core does not name, a group (which proto3 has none of), a varint past ten bytes,
/// a field numbered zero, a known field in another wire type, a length past the bytes: each is
/// refused, as a peer's malformed message.
#[test]
fn what_no_raft_rs_message_holds_is_refused() {
    let refused: [&[u8]; 8] = [
        &[0x08, 77],
        &[0x3a, 2, 0x08, 9],
        &[0x0b, 0x0c],
        &[
            0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ],
        &[0x00, 1],
        &[0x12, 1, 0],
        &[0x62, 5, 1],
        &[0x10],
    ];
    for bytes in refused {
        assert!(
            matches!(
                decode_message(bytes),
                Err(ConsensusError::MalformedMessage(_))
            ),
            "{bytes:?}"
        );
    }
    let mut lost = message(&mut Seeded::new(3));
    lost.lost = true;
    assert_eq!(
        encode_message(&lost),
        Err(EnvelopeError::Unstated("a member's mark"))
    );
}
