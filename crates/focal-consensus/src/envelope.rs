//! The envelope: the core's values as focal's WAL and wire hold them.
//!
//! The core is hyper-raft (27 §4.2), whose messages, entries, hard states and snapshots are its
//! own types. focal's WAL records and peer messages have held them as raft-rs 0.7's protocol
//! buffers (`eraftpb.proto` at raft-rs `8e4cef1`) since its groups first ran, and a node changes
//! no byte it writes before the cluster's upgrade fence opens a successor encoding (18 §1, 24
//! §21). So every value crosses here, both ways:
//!
//! - **Written** exactly as raft-proto's prost codec writes it (the protocol buffers encoding
//!   guide, <https://protobuf.dev/programming-guides/encoding/>): fields in the order of their
//!   numbers, a scalar at its default, empty bytes and an empty repeated field left out, repeated
//!   numbers packed, a message field written whenever it is present. `envelope_tests` holds every
//!   value it generates to raft-proto's own bytes.
//! - **Read** as any protocol-buffer reader reads it: fields in any order, the last of a scalar
//!   winning, a message field met twice merged, repeated numbers packed or not, unknown fields
//!   skipped by their wire type. Refused: bytes that end inside a field, a varint past ten bytes,
//!   a field number outside 1 to 2^29 − 1, a known field of another wire type, a group (no
//!   raft-rs message has one, and proto3 has none), and a kind the core does not name.
//!
//! A change of configuration is a protocol buffer in focal's log (`ConfChange`, `ConfChangeV2`)
//! and a hyper-raft record in the core's, so an entry that states one is translated with it. The
//! empty change is no bytes in raft-rs's encoding, and the core reads no bytes as the empty change
//! (`hyper_raft::proto::change_of`): a record of it crosses as no bytes and comes back as none. What
//! the core holds and raft-rs's encoding cannot state is refused, never dropped: a member's mark
//! (`Message::lost`), and an append kept ahead of a hole (`Message::kept`), until the cluster's
//! upgrade fence opens fields of focal's own for them ([`Wire`]).

use hyper_raft::proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
    Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
};
use hyper_raft::wire::{self, Record};
use thiserror::Error;

/// Why bytes are not a value, or a value has no bytes.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum EnvelopeError {
    /// The bytes end inside a field.
    #[error("the bytes end inside a field")]
    Truncated,
    /// A varint runs past the ten bytes a 64-bit value takes.
    #[error("a varint past ten bytes")]
    Varint,
    /// A field numbered outside the encoding's numbers, 1 to 2^29 − 1 (the protocol buffers
    /// language guide, "Assigning Field Numbers").
    #[error("field number {0}")]
    FieldNumber(u64),
    /// A field of a known number arrives in another wire type.
    #[error("{value} field {field} in wire type {wire}")]
    WireType {
        /// The value read.
        value: &'static str,
        /// The field's number.
        field: u64,
        /// Its wire type.
        wire: u64,
    },
    /// A group, or a wire type the encoding does not define.
    #[error("wire type {0}, which no raft-rs message holds")]
    Wire(u64),
    /// A kind the core does not name.
    #[error("{what} {value}, which the core does not name")]
    Unknown {
        /// The field.
        what: &'static str,
        /// Its value.
        value: i64,
    },
    /// The core holds what raft-rs's encoding cannot state.
    #[error("{0}, which raft-rs's encoding cannot state")]
    Unstated(&'static str),
    /// A change of configuration in the core's own encoding does not read.
    #[error("a change of configuration: {0}")]
    Change(wire::DecodeError),
    /// A length past what the machine can hold.
    #[error("a length past this machine's")]
    Length,
    /// The allocator refused a buffer.
    #[error("memory for the value")]
    Memory,
}

impl EnvelopeError {
    /// What a peer's message that fails here is refused for, as a fixed description.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Truncated => "the bytes end inside a field",
            Self::Varint => "a varint past ten bytes",
            Self::FieldNumber(_) => "a field number the encoding has not",
            Self::WireType { .. } => "a field in the wrong wire type",
            Self::Wire(_) => "a wire type no raft-rs message holds",
            Self::Unknown { .. } => "a kind of message, entry or change the core does not name",
            Self::Unstated(_) => "what raft-rs's encoding cannot state",
            Self::Change(_) => "a change of configuration that does not read",
            Self::Length => "a length past this machine's",
            Self::Memory => "no memory for the message",
        }
    }
}

impl From<wire::DecodeError> for EnvelopeError {
    fn from(error: wire::DecodeError) -> Self {
        Self::Change(error)
    }
}

type Result<T> = core::result::Result<T, EnvelopeError>;

/// The largest field number the encoding has, 2^29 − 1 (the language guide, "Assigning Field
/// Numbers").
const LAST_FIELD: u64 = (1 << 29) - 1;
/// Wire types, the encoding guide's "Message Structure".
const VARINT: u64 = 0;
const I64: u64 = 1;
const LEN: u64 = 2;
const I32: u64 = 5;

/// The fast track's message kinds, numbered past raft-rs's last (18) as focal's core numbered
/// them (focal-raft's `FAST_PROPOSE` and `FAST_VOTE`), which focal's logs and peers hold.
const FAST_PROPOSE: i32 = 100;
const FAST_VOTE: i32 = 101;

// ---------------------------------------------------------------------------------------------
// Writing.

/// The bytes `value` takes as a varint: seven bits a byte, one byte for zero.
fn varint_len(value: u64) -> usize {
    let bits = u64::BITS.saturating_sub((value | 1).leading_zeros());
    usize::try_from(bits.div_ceil(7)).unwrap_or(10)
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        let [low, ..] = value.to_le_bytes();
        out.push(low | 0x80);
        value >>= 7;
    }
    let [low, ..] = value.to_le_bytes();
    out.push(low);
}

fn key(field: u64, wire: u64) -> u64 {
    (field << 3) | wire
}

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or(EnvelopeError::Length)
}

fn as_u64(length: usize) -> Result<u64> {
    u64::try_from(length).map_err(|_| EnvelopeError::Length)
}

/// The bytes of a varint field, left out at zero.
fn varint_field_len(field: u64, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        varint_len(key(field, VARINT)).saturating_add(varint_len(value))
    }
}
/// The bytes of a varint field written with its presence: zero included.
fn present_varint_field_len(field: u64, value: u64) -> usize {
    varint_len(key(field, VARINT)).saturating_add(varint_len(value))
}

/// The bytes of a length-delimited field of `length` bytes, present or not.
fn delimited_len(field: u64, length: usize) -> Result<usize> {
    add(
        add(varint_len(key(field, LEN)), varint_len(as_u64(length)?))?,
        length,
    )
}

/// The bytes of a bytes field, left out when empty.
fn bytes_field_len(field: u64, bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        Ok(0)
    } else {
        delimited_len(field, bytes.len())
    }
}

/// The body of a packed repeated varint field.
fn packed_body_len(values: &[u64]) -> Result<usize> {
    values
        .iter()
        .try_fold(0usize, |sum, value| add(sum, varint_len(*value)))
}

/// The bytes of a packed repeated varint field, left out when empty.
fn packed_field_len(field: u64, values: &[u64]) -> Result<usize> {
    if values.is_empty() {
        Ok(0)
    } else {
        delimited_len(field, packed_body_len(values)?)
    }
}

fn put_varint_field(out: &mut Vec<u8>, field: u64, value: u64) {
    if value != 0 {
        put_varint(out, key(field, VARINT));
        put_varint(out, value);
    }
}

fn put_delimited_head(out: &mut Vec<u8>, field: u64, length: usize) -> Result<()> {
    put_varint(out, key(field, LEN));
    put_varint(out, as_u64(length)?);
    Ok(())
}

fn put_bytes_field(out: &mut Vec<u8>, field: u64, bytes: &[u8]) -> Result<()> {
    if !bytes.is_empty() {
        put_delimited_head(out, field, bytes.len())?;
        out.extend_from_slice(bytes);
    }
    Ok(())
}

fn put_packed_field(out: &mut Vec<u8>, field: u64, values: &[u64]) -> Result<()> {
    if !values.is_empty() {
        put_delimited_head(out, field, packed_body_len(values)?)?;
        for value in values {
            put_varint(out, *value);
        }
    }
    Ok(())
}

/// An `int32` as prost writes it: a negative value as its 64-bit two's complement.
fn int32(value: i32) -> u64 {
    u64::from_le_bytes(i64::from(value).to_le_bytes())
}

/// An `int64` as prost writes it.
fn int64(value: i64) -> u64 {
    u64::from_le_bytes(value.to_le_bytes())
}

fn message_kind(kind: MessageType) -> i32 {
    match kind {
        MessageType::MsgHup => 0,
        MessageType::MsgBeat => 1,
        MessageType::MsgPropose => 2,
        MessageType::MsgAppend => 3,
        MessageType::MsgAppendResponse => 4,
        MessageType::MsgRequestVote => 5,
        MessageType::MsgRequestVoteResponse => 6,
        MessageType::MsgSnapshot => 7,
        MessageType::MsgHeartbeat => 8,
        MessageType::MsgHeartbeatResponse => 9,
        MessageType::MsgUnreachable => 10,
        MessageType::MsgSnapStatus => 11,
        MessageType::MsgCheckQuorum => 12,
        MessageType::MsgTransferLeader => 13,
        MessageType::MsgTimeoutNow => 14,
        MessageType::MsgReadIndex => 15,
        MessageType::MsgReadIndexResp => 16,
        MessageType::MsgRequestPreVote => 17,
        MessageType::MsgRequestPreVoteResponse => 18,
        MessageType::MsgFastPropose => FAST_PROPOSE,
        MessageType::MsgFastVote => FAST_VOTE,
    }
}

fn entry_kind(kind: EntryType) -> i32 {
    match kind {
        EntryType::EntryNormal => 0,
        EntryType::EntryConfChange => 1,
        EntryType::EntryConfChangeV2 => 2,
    }
}

fn change_kind(kind: ConfChangeType) -> i32 {
    match kind {
        ConfChangeType::AddNode => 0,
        ConfChangeType::RemoveNode => 1,
        ConfChangeType::AddLearnerNode => 2,
    }
}

fn transition_kind(kind: ConfChangeTransition) -> i32 {
    match kind {
        ConfChangeTransition::Auto => 0,
        ConfChangeTransition::Implicit => 1,
        ConfChangeTransition::Explicit => 2,
    }
}

fn conf_state_len(state: &ConfState) -> Result<usize> {
    let mut length = packed_field_len(1, &state.voters)?;
    length = add(length, packed_field_len(2, &state.learners)?)?;
    length = add(length, packed_field_len(3, &state.voters_outgoing)?)?;
    length = add(length, packed_field_len(4, &state.learners_next)?)?;
    add(length, varint_field_len(5, u64::from(state.auto_leave)))
}

fn put_conf_state(out: &mut Vec<u8>, state: &ConfState) -> Result<()> {
    put_packed_field(out, 1, &state.voters)?;
    put_packed_field(out, 2, &state.learners)?;
    put_packed_field(out, 3, &state.voters_outgoing)?;
    put_packed_field(out, 4, &state.learners_next)?;
    put_varint_field(out, 5, u64::from(state.auto_leave));
    Ok(())
}

fn metadata_len(metadata: &SnapshotMetadata) -> Result<usize> {
    let mut length = match &metadata.conf_state {
        Some(state) => delimited_len(1, conf_state_len(state)?)?,
        None => 0,
    };
    length = add(length, varint_field_len(2, metadata.index))?;
    add(length, varint_field_len(3, metadata.term))
}

fn put_metadata(out: &mut Vec<u8>, metadata: &SnapshotMetadata) -> Result<()> {
    if let Some(state) = &metadata.conf_state {
        put_delimited_head(out, 1, conf_state_len(state)?)?;
        put_conf_state(out, state)?;
    }
    put_varint_field(out, 2, metadata.index);
    put_varint_field(out, 3, metadata.term);
    Ok(())
}

fn snapshot_len(snapshot: &Snapshot) -> Result<usize> {
    let length = bytes_field_len(1, &snapshot.data)?;
    match &snapshot.metadata {
        Some(metadata) => add(length, delimited_len(2, metadata_len(metadata)?)?),
        None => Ok(length),
    }
}

fn put_snapshot(out: &mut Vec<u8>, snapshot: &Snapshot) -> Result<()> {
    put_bytes_field(out, 1, &snapshot.data)?;
    if let Some(metadata) = &snapshot.metadata {
        put_delimited_head(out, 2, metadata_len(metadata)?)?;
        put_metadata(out, metadata)?;
    }
    Ok(())
}

/// A change of configuration's data as focal's log holds it: the core's record translated, the
/// empty change as no bytes (the core states a leader's leave so, and raft-rs's encoding of the
/// empty change is none).
fn change_to_log(kind: EntryType, data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    match kind {
        EntryType::EntryNormal => out.extend_from_slice(data),
        EntryType::EntryConfChange => {
            let change = ConfChange::decode(data)?;
            let length = add(
                add(
                    varint_field_len(2, int32(change_kind(change.change_type))),
                    varint_field_len(3, change.node_id),
                )?,
                bytes_field_len(4, &change.context)?,
            )?;
            out.try_reserve_exact(length)
                .map_err(|_| EnvelopeError::Memory)?;
            put_varint_field(&mut out, 2, int32(change_kind(change.change_type)));
            put_varint_field(&mut out, 3, change.node_id);
            put_bytes_field(&mut out, 4, &change.context)?;
        }
        EntryType::EntryConfChangeV2 => out = encode_conf_change_v2(&ConfChangeV2::decode(data)?)?,
    }
    Ok(out)
}

fn single_len(single: &ConfChangeSingle) -> Result<usize> {
    add(
        varint_field_len(1, int32(change_kind(single.change_type))),
        varint_field_len(2, single.node_id),
    )
}

/// `change` in raft-rs's encoding, as an entry of focal's log holds it.
pub fn encode_conf_change_v2(change: &ConfChangeV2) -> Result<Vec<u8>> {
    let singles = change
        .changes
        .iter()
        .map(single_len)
        .collect::<Result<Vec<usize>>>()?;
    let mut length = varint_field_len(1, int32(transition_kind(change.transition)));
    for single in &singles {
        length = add(length, delimited_len(2, *single)?)?;
    }
    length = add(length, bytes_field_len(3, &change.context)?)?;
    let mut out = Vec::new();
    out.try_reserve_exact(length)
        .map_err(|_| EnvelopeError::Memory)?;
    put_varint_field(&mut out, 1, int32(transition_kind(change.transition)));
    for (single, length) in change.changes.iter().zip(&singles) {
        put_delimited_head(&mut out, 2, *length)?;
        put_varint_field(&mut out, 1, int32(change_kind(single.change_type)));
        put_varint_field(&mut out, 2, single.node_id);
    }
    put_bytes_field(&mut out, 3, &change.context)?;
    Ok(out)
}

/// The bytes `change` takes in raft-rs's encoding, as an entry of focal's log holds it.
pub fn conf_change_v2_len(change: &ConfChangeV2) -> Result<usize> {
    let mut length = varint_field_len(1, int32(transition_kind(change.transition)));
    for single in &change.changes {
        length = add(length, delimited_len(2, single_len(single)?)?)?;
    }
    add(length, bytes_field_len(3, &change.context)?)
}

/// An entry's data as the log holds it: borrowed, or a change translated.
enum LogData<'a> {
    Same(&'a [u8]),
    Translated(Vec<u8>),
}

impl LogData<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            LogData::Same(bytes) => bytes,
            LogData::Translated(bytes) => bytes,
        }
    }
}

fn log_data(entry: &Entry) -> Result<LogData<'_>> {
    if entry.entry_type == EntryType::EntryNormal || entry.data.is_empty() {
        Ok(LogData::Same(&entry.data))
    } else {
        change_to_log(entry.entry_type, &entry.data).map(LogData::Translated)
    }
}

fn entry_len(entry: &Entry, data: &[u8]) -> Result<usize> {
    let mut length = varint_field_len(1, int32(entry_kind(entry.entry_type)));
    length = add(length, varint_field_len(2, entry.term))?;
    length = add(length, varint_field_len(3, entry.index))?;
    length = add(length, bytes_field_len(4, data)?)?;
    add(length, bytes_field_len(6, &entry.context)?)
}

fn put_entry(out: &mut Vec<u8>, entry: &Entry, data: &[u8]) -> Result<()> {
    put_varint_field(out, 1, int32(entry_kind(entry.entry_type)));
    put_varint_field(out, 2, entry.term);
    put_varint_field(out, 3, entry.index);
    put_bytes_field(out, 4, data)?;
    put_bytes_field(out, 6, &entry.context)
}

/// The priority raft-rs 0.7 also states in its older field, as focal's core sent it: a vote
/// request's positive priority (focal-raft `push`), which a raft-rs reader that knows only the
/// older field takes from there.
fn deprecated_priority(message: &Message) -> u64 {
    match message.msg_type {
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
            u64::try_from(message.priority).unwrap_or(0)
        }
        _ => 0,
    }
}

/// `entry` as a WAL record's payload.
pub fn encode_entry(entry: &Entry) -> Result<Vec<u8>> {
    let data = log_data(entry)?;
    let length = entry_len(entry, data.bytes())?;
    let mut out = Vec::new();
    out.try_reserve_exact(length)
        .map_err(|_| EnvelopeError::Memory)?;
    put_entry(&mut out, entry, data.bytes())?;
    Ok(out)
}

/// `state` as a WAL record's payload.
pub fn encode_hard_state(state: &HardState) -> Result<Vec<u8>> {
    let length = add(
        add(
            varint_field_len(1, state.term),
            varint_field_len(2, state.vote),
        )?,
        varint_field_len(3, state.commit),
    )?;
    let mut out = Vec::new();
    out.try_reserve_exact(length)
        .map_err(|_| EnvelopeError::Memory)?;
    put_varint_field(&mut out, 1, state.term);
    put_varint_field(&mut out, 2, state.vote);
    put_varint_field(&mut out, 3, state.commit);
    Ok(out)
}

/// `snapshot` as a WAL record's payload.
pub fn encode_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>> {
    let length = snapshot_len(snapshot)?;
    let mut out = Vec::new();
    out.try_reserve_exact(length)
        .map_err(|_| EnvelopeError::Memory)?;
    put_snapshot(&mut out, snapshot)?;
    Ok(out)
}

/// What a message's encoding is made of, measured once for its length and its writing.
struct Layout<'a> {
    /// Each entry's data as the log holds it, and the entry's length.
    entries: Vec<(LogData<'a>, usize)>,
    /// The snapshot's length, when the message carries one.
    snapshot: Option<usize>,
    /// The whole message's length.
    length: usize,
}

/// Which of the core's messages' fields focal's peers carry beyond raft-rs's: none until the
/// cluster's upgrade fence opens `RAFT_KEPT_LEVEL` (focal-node `upgrade`, 24 §21), then a refusal's
/// `kept` (field 17, R17) and `lost` (field 18, R-5). eraftpb at raft-rs `8e4cef1` numbers its
/// fields 1 to 16.
///
/// Members apply the fence at different moments, so a member already raised sends `kept` to one
/// that is not. That member reads field 17 as raft-rs reads a field it does not know, skipped: the
/// refusal is then raft-rs's own (the append's index, a hint at the member's last matching entry),
/// whose answer is to step back to the hint and send again, so a member raised early costs its
/// peers one resend and never a stall (27 §15.9). `lost` is refused below the fence: read without
/// its flag, a refusal for lost entries would let a leader count acknowledgements the member no
/// longer holds, and focal never writes one before the fence, nor after it without hyper-log's
/// marks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Wire {
    /// raft-rs's fields alone: what a member writes and reads below the fence.
    #[default]
    Frozen,
    /// raft-rs's fields, `kept` and `lost`: at or above the fence.
    Kept,
}

/// Field 17 of a message: an append kept ahead of a hole (`Message::kept`).
const KEPT_FIELD: u64 = 17;
/// Field 18 of a message: a refusal for entries lost at rest (`Message::lost`).
const LOST_FIELD: u64 = 18;
/// Field 19 of a message: the index through which the leader knows its log committed by a
/// classic quorum (`Message::classic`). Written with its presence, zero included, and only under
/// `Wire::Kept`; below the fence a reader skips it as raft-rs skips a field it does not know, and
/// a message without it says nothing known, which never releases what a member holds (hyper-raft
/// `docs/raft.md`, "Releasing what a member holds"). The fast track needs it, so a group has the
/// fast track only above the fence.
const CLASSIC_FIELD: u64 = 19;

fn layout(message: &Message, wire: Wire) -> Result<Layout<'_>> {
    if wire == Wire::Frozen {
        if message.lost {
            return Err(EnvelopeError::Unstated("a member's mark"));
        }
        if message.kept {
            return Err(EnvelopeError::Unstated("an append kept ahead of a hole"));
        }
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(message.entries.len())
        .map_err(|_| EnvelopeError::Memory)?;
    for entry in &message.entries {
        let data = log_data(entry)?;
        let length = entry_len(entry, data.bytes())?;
        entries.push((data, length));
    }
    let mut length = varint_field_len(1, int32(message_kind(message.msg_type)));
    length = add(length, varint_field_len(2, message.to))?;
    length = add(length, varint_field_len(3, message.from))?;
    length = add(length, varint_field_len(4, message.term))?;
    length = add(length, varint_field_len(5, message.log_term))?;
    length = add(length, varint_field_len(6, message.index))?;
    for (_, entry) in &entries {
        length = add(length, delimited_len(7, *entry)?)?;
    }
    length = add(length, varint_field_len(8, message.commit))?;
    let snapshot = match &message.snapshot {
        Some(snapshot) => Some(snapshot_len(snapshot)?),
        None => None,
    };
    if let Some(snapshot) = snapshot {
        length = add(length, delimited_len(9, snapshot)?)?;
    }
    length = add(length, varint_field_len(10, u64::from(message.reject)))?;
    length = add(length, varint_field_len(11, message.reject_hint))?;
    length = add(length, bytes_field_len(12, &message.context)?)?;
    length = add(length, varint_field_len(13, message.request_snapshot))?;
    length = add(length, varint_field_len(14, deprecated_priority(message)))?;
    length = add(length, varint_field_len(15, message.commit_term))?;
    length = add(length, varint_field_len(16, int64(message.priority)))?;
    length = add(
        length,
        varint_field_len(KEPT_FIELD, u64::from(message.kept)),
    )?;
    length = add(
        length,
        varint_field_len(LOST_FIELD, u64::from(message.lost)),
    )?;
    if wire == Wire::Kept
        && let Some(classic) = message.classic
    {
        length = add(length, present_varint_field_len(CLASSIC_FIELD, classic))?;
    }
    Ok(Layout {
        entries,
        snapshot,
        length,
    })
}

/// The bytes `message` takes as it goes to a peer, counting every field it holds: what a member
/// charges for a message it holds, whichever wire it later goes under.
pub fn message_len(message: &Message) -> Result<usize> {
    Ok(layout(message, Wire::Kept)?.length)
}

/// `message` as it goes to a peer below the fence ([`Wire::Frozen`]).
pub fn encode_message(message: &Message) -> Result<Vec<u8>> {
    encode_message_in(message, Wire::Frozen)
}

/// `message` as it goes to a peer under `wire`.
pub fn encode_message_in(message: &Message, wire: Wire) -> Result<Vec<u8>> {
    let layout = layout(message, wire)?;
    let mut out = Vec::new();
    out.try_reserve_exact(layout.length)
        .map_err(|_| EnvelopeError::Memory)?;
    put_varint_field(&mut out, 1, int32(message_kind(message.msg_type)));
    put_varint_field(&mut out, 2, message.to);
    put_varint_field(&mut out, 3, message.from);
    put_varint_field(&mut out, 4, message.term);
    put_varint_field(&mut out, 5, message.log_term);
    put_varint_field(&mut out, 6, message.index);
    for (entry, (data, entry_length)) in message.entries.iter().zip(&layout.entries) {
        put_delimited_head(&mut out, 7, *entry_length)?;
        put_entry(&mut out, entry, data.bytes())?;
    }
    put_varint_field(&mut out, 8, message.commit);
    if let (Some(snapshot), Some(snapshot_length)) = (&message.snapshot, layout.snapshot) {
        put_delimited_head(&mut out, 9, snapshot_length)?;
        put_snapshot(&mut out, snapshot)?;
    }
    put_varint_field(&mut out, 10, u64::from(message.reject));
    put_varint_field(&mut out, 11, message.reject_hint);
    put_bytes_field(&mut out, 12, &message.context)?;
    put_varint_field(&mut out, 13, message.request_snapshot);
    put_varint_field(&mut out, 14, deprecated_priority(message));
    put_varint_field(&mut out, 15, message.commit_term);
    put_varint_field(&mut out, 16, int64(message.priority));
    put_varint_field(&mut out, KEPT_FIELD, u64::from(message.kept));
    put_varint_field(&mut out, LOST_FIELD, u64::from(message.lost));
    if wire == Wire::Kept
        && let Some(classic) = message.classic
    {
        put_varint(&mut out, key(CLASSIC_FIELD, VARINT));
        put_varint(&mut out, classic);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Reading.

/// The fields of one encoded message, read in order.
struct Fields<'a> {
    bytes: &'a [u8],
}

/// One field's value as its wire type gives it.
enum Value<'a> {
    Varint(u64),
    Delimited(&'a [u8]),
    /// A fixed-width value of this wire type; no field of raft-rs's messages is one, so it is
    /// only ever skipped.
    Fixed(u64),
}

impl<'a> Fields<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64u32).step_by(7) {
            let (byte, rest) = self.bytes.split_first().ok_or(EnvelopeError::Truncated)?;
            self.bytes = rest;
            // The tenth byte holds the 64th bit alone.
            if shift == 63 && *byte > 1 {
                return Err(EnvelopeError::Varint);
            }
            value |= u64::from(byte & 0x7f)
                .checked_shl(shift)
                .ok_or(EnvelopeError::Varint)?;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(EnvelopeError::Varint)
    }

    fn take(&mut self, length: u64) -> Result<&'a [u8]> {
        let length = usize::try_from(length).map_err(|_| EnvelopeError::Truncated)?;
        let (taken, rest) = self
            .bytes
            .split_at_checked(length)
            .ok_or(EnvelopeError::Truncated)?;
        self.bytes = rest;
        Ok(taken)
    }

    /// The next field's number and value; none at the end.
    fn next(&mut self) -> Result<Option<(u64, Value<'a>)>> {
        if self.bytes.is_empty() {
            return Ok(None);
        }
        let tag = self.varint()?;
        let field = tag >> 3;
        if field == 0 || field > LAST_FIELD {
            return Err(EnvelopeError::FieldNumber(field));
        }
        let value = match tag & 7 {
            VARINT => Value::Varint(self.varint()?),
            I64 => {
                self.take(8)?;
                Value::Fixed(I64)
            }
            LEN => {
                let length = self.varint()?;
                Value::Delimited(self.take(length)?)
            }
            I32 => {
                self.take(4)?;
                Value::Fixed(I32)
            }
            wire => return Err(EnvelopeError::Wire(wire)),
        };
        Ok(Some((field, value)))
    }
}

fn wire_of(value: &Value<'_>) -> u64 {
    match value {
        Value::Varint(_) => VARINT,
        Value::Delimited(_) => LEN,
        Value::Fixed(wire) => *wire,
    }
}

fn varint_of(what: &'static str, field: u64, value: Value<'_>) -> Result<u64> {
    match value {
        Value::Varint(value) => Ok(value),
        other => Err(EnvelopeError::WireType {
            value: what,
            field,
            wire: wire_of(&other),
        }),
    }
}

fn delimited_of<'a>(what: &'static str, field: u64, value: Value<'a>) -> Result<&'a [u8]> {
    match value {
        Value::Delimited(bytes) => Ok(bytes),
        other => Err(EnvelopeError::WireType {
            value: what,
            field,
            wire: wire_of(&other),
        }),
    }
}

/// An `int32` as prost reads one: the varint's low 32 bits. An enum field is read as such a
/// number and named once the whole value is read, so the last occurrence wins and only it must
/// name a kind, as prost reads one.
fn as_int32(value: u64) -> i32 {
    let [a, b, c, d, ..] = value.to_le_bytes();
    i32::from_le_bytes([a, b, c, d])
}

/// An `int64` as prost reads one.
fn as_int64(value: u64) -> i64 {
    i64::from_le_bytes(value.to_le_bytes())
}

fn copy(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len())
        .map_err(|_| EnvelopeError::Memory)?;
    out.extend_from_slice(bytes);
    Ok(out)
}

fn push<T>(values: &mut Vec<T>, value: T) -> Result<()> {
    values.try_reserve(1).map_err(|_| EnvelopeError::Memory)?;
    values.push(value);
    Ok(())
}

/// A repeated varint field's values, packed or not.
fn repeated(what: &'static str, field: u64, value: Value<'_>, values: &mut Vec<u64>) -> Result<()> {
    match value {
        Value::Varint(value) => push(values, value),
        Value::Delimited(bytes) => {
            let mut packed = Fields::new(bytes);
            while !packed.bytes.is_empty() {
                let value = packed.varint()?;
                push(values, value)?;
            }
            Ok(())
        }
        Value::Fixed(wire) => Err(EnvelopeError::WireType {
            value: what,
            field,
            wire,
        }),
    }
}

fn message_type_of(value: i32) -> Result<MessageType> {
    Ok(match value {
        0 => MessageType::MsgHup,
        1 => MessageType::MsgBeat,
        2 => MessageType::MsgPropose,
        3 => MessageType::MsgAppend,
        4 => MessageType::MsgAppendResponse,
        5 => MessageType::MsgRequestVote,
        6 => MessageType::MsgRequestVoteResponse,
        7 => MessageType::MsgSnapshot,
        8 => MessageType::MsgHeartbeat,
        9 => MessageType::MsgHeartbeatResponse,
        10 => MessageType::MsgUnreachable,
        11 => MessageType::MsgSnapStatus,
        12 => MessageType::MsgCheckQuorum,
        13 => MessageType::MsgTransferLeader,
        14 => MessageType::MsgTimeoutNow,
        15 => MessageType::MsgReadIndex,
        16 => MessageType::MsgReadIndexResp,
        17 => MessageType::MsgRequestPreVote,
        18 => MessageType::MsgRequestPreVoteResponse,
        FAST_PROPOSE => MessageType::MsgFastPropose,
        FAST_VOTE => MessageType::MsgFastVote,
        other => {
            return Err(EnvelopeError::Unknown {
                what: "message kind",
                value: i64::from(other),
            });
        }
    })
}

fn entry_type_of(value: i32) -> Result<EntryType> {
    Ok(match value {
        0 => EntryType::EntryNormal,
        1 => EntryType::EntryConfChange,
        2 => EntryType::EntryConfChangeV2,
        other => {
            return Err(EnvelopeError::Unknown {
                what: "entry kind",
                value: i64::from(other),
            });
        }
    })
}

fn change_type_of(value: i32) -> Result<ConfChangeType> {
    Ok(match value {
        0 => ConfChangeType::AddNode,
        1 => ConfChangeType::RemoveNode,
        2 => ConfChangeType::AddLearnerNode,
        other => {
            return Err(EnvelopeError::Unknown {
                what: "change kind",
                value: i64::from(other),
            });
        }
    })
}

fn transition_of(value: i32) -> Result<ConfChangeTransition> {
    Ok(match value {
        0 => ConfChangeTransition::Auto,
        1 => ConfChangeTransition::Implicit,
        2 => ConfChangeTransition::Explicit,
        other => {
            return Err(EnvelopeError::Unknown {
                what: "transition",
                value: i64::from(other),
            });
        }
    })
}

fn merge_conf_state(state: &mut ConfState, bytes: &[u8]) -> Result<()> {
    let mut fields = Fields::new(bytes);
    while let Some((field, value)) = fields.next()? {
        match field {
            1 => repeated("ConfState", field, value, &mut state.voters)?,
            2 => repeated("ConfState", field, value, &mut state.learners)?,
            3 => repeated("ConfState", field, value, &mut state.voters_outgoing)?,
            4 => repeated("ConfState", field, value, &mut state.learners_next)?,
            5 => state.auto_leave = varint_of("ConfState", field, value)? != 0,
            _ => {}
        }
    }
    Ok(())
}

fn merge_metadata(metadata: &mut SnapshotMetadata, bytes: &[u8]) -> Result<()> {
    let mut fields = Fields::new(bytes);
    while let Some((field, value)) = fields.next()? {
        match field {
            1 => merge_conf_state(
                metadata.conf_state.get_or_insert_with(ConfState::default),
                delimited_of("SnapshotMetadata", field, value)?,
            )?,
            2 => metadata.index = varint_of("SnapshotMetadata", field, value)?,
            3 => metadata.term = varint_of("SnapshotMetadata", field, value)?,
            _ => {}
        }
    }
    Ok(())
}

fn merge_snapshot(snapshot: &mut Snapshot, bytes: &[u8]) -> Result<()> {
    let mut fields = Fields::new(bytes);
    while let Some((field, value)) = fields.next()? {
        match field {
            1 => snapshot.data = copy(delimited_of("Snapshot", field, value)?)?,
            2 => merge_metadata(
                snapshot
                    .metadata
                    .get_or_insert_with(SnapshotMetadata::default),
                delimited_of("Snapshot", field, value)?,
            )?,
            _ => {}
        }
    }
    Ok(())
}

/// A change of configuration's data as the core holds it: the log's protocol buffer read and
/// written as the core's record; no bytes stay no bytes.
fn change_from_log(kind: EntryType, data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    match kind {
        EntryType::EntryNormal => copy(data),
        EntryType::EntryConfChange => {
            let mut change = ConfChange::default();
            let mut kind = 0i32;
            let mut fields = Fields::new(data);
            while let Some((field, value)) = fields.next()? {
                match field {
                    2 => kind = as_int32(varint_of("ConfChange", field, value)?),
                    3 => change.node_id = varint_of("ConfChange", field, value)?,
                    4 => change.context = copy(delimited_of("ConfChange", field, value)?)?,
                    // 1 is raft-rs's unused `id`, which the core has no place for.
                    _ => {}
                }
            }
            change.change_type = change_type_of(kind)?;
            Ok(change.encode_to_vec())
        }
        EntryType::EntryConfChangeV2 => {
            let mut change = ConfChangeV2::default();
            let mut transition = 0i32;
            let mut fields = Fields::new(data);
            while let Some((field, value)) = fields.next()? {
                match field {
                    1 => transition = as_int32(varint_of("ConfChangeV2", field, value)?),
                    2 => {
                        let mut single = ConfChangeSingle::default();
                        let mut kind = 0i32;
                        let mut inner = Fields::new(delimited_of("ConfChangeV2", field, value)?);
                        while let Some((field, value)) = inner.next()? {
                            match field {
                                1 => kind = as_int32(varint_of("ConfChangeSingle", field, value)?),
                                2 => single.node_id = varint_of("ConfChangeSingle", field, value)?,
                                _ => {}
                            }
                        }
                        single.change_type = change_type_of(kind)?;
                        push(&mut change.changes, single)?;
                    }
                    3 => change.context = copy(delimited_of("ConfChangeV2", field, value)?)?,
                    _ => {}
                }
            }
            change.transition = transition_of(transition)?;
            Ok(change.encode_to_vec())
        }
    }
}

fn read_entry(bytes: &[u8]) -> Result<Entry> {
    let mut entry = Entry::default();
    let mut kind = 0i32;
    let mut data: &[u8] = &[];
    let mut fields = Fields::new(bytes);
    while let Some((field, value)) = fields.next()? {
        match field {
            1 => kind = as_int32(varint_of("Entry", field, value)?),
            2 => entry.term = varint_of("Entry", field, value)?,
            3 => entry.index = varint_of("Entry", field, value)?,
            4 => data = delimited_of("Entry", field, value)?,
            6 => entry.context = copy(delimited_of("Entry", field, value)?)?,
            // 5 is raft-rs's deprecated `sync_log`, which the core has no place for.
            _ => {}
        }
    }
    entry.entry_type = entry_type_of(kind)?;
    entry.data = if entry.entry_type == EntryType::EntryNormal {
        copy(data)?
    } else {
        change_from_log(entry.entry_type, data)?
    };
    Ok(entry)
}

/// The entry a WAL record's payload holds.
pub fn decode_entry(bytes: &[u8]) -> Result<Entry> {
    read_entry(bytes)
}

/// The hard state a WAL record's payload holds.
pub fn decode_hard_state(bytes: &[u8]) -> Result<HardState> {
    let mut state = HardState::default();
    let mut fields = Fields::new(bytes);
    while let Some((field, value)) = fields.next()? {
        match field {
            1 => state.term = varint_of("HardState", field, value)?,
            2 => state.vote = varint_of("HardState", field, value)?,
            3 => state.commit = varint_of("HardState", field, value)?,
            _ => {}
        }
    }
    Ok(state)
}

/// The snapshot a WAL record's payload holds.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot> {
    let mut snapshot = Snapshot::default();
    merge_snapshot(&mut snapshot, bytes)?;
    Ok(snapshot)
}

/// The message a peer sent, read below the fence ([`Wire::Frozen`]).
pub fn decode_message(bytes: &[u8]) -> Result<Message> {
    decode_message_in(bytes, Wire::Frozen)
}

/// The message a peer sent, read under `wire`.
pub fn decode_message_in(bytes: &[u8], wire: Wire) -> Result<Message> {
    let mut message = Message::default();
    let mut kind = 0i32;
    let mut deprecated_priority = 0u64;
    let mut fields = Fields::new(bytes);
    while let Some((field, value)) = fields.next()? {
        match field {
            1 => kind = as_int32(varint_of("Message", field, value)?),
            2 => message.to = varint_of("Message", field, value)?,
            3 => message.from = varint_of("Message", field, value)?,
            4 => message.term = varint_of("Message", field, value)?,
            5 => message.log_term = varint_of("Message", field, value)?,
            6 => message.index = varint_of("Message", field, value)?,
            7 => {
                let entry = read_entry(delimited_of("Message", field, value)?)?;
                push(&mut message.entries, entry)?;
            }
            8 => message.commit = varint_of("Message", field, value)?,
            9 => merge_snapshot(
                message
                    .snapshot
                    .get_or_insert_with(|| Box::new(Snapshot::default())),
                delimited_of("Message", field, value)?,
            )?,
            10 => message.reject = varint_of("Message", field, value)? != 0,
            11 => message.reject_hint = varint_of("Message", field, value)?,
            12 => message.context = copy(delimited_of("Message", field, value)?)?,
            13 => message.request_snapshot = varint_of("Message", field, value)?,
            14 => deprecated_priority = varint_of("Message", field, value)?,
            15 => message.commit_term = varint_of("Message", field, value)?,
            16 => message.priority = as_int64(varint_of("Message", field, value)?),
            KEPT_FIELD => {
                let kept = varint_of("Message", field, value)? != 0;
                // Below the fence, skipped as raft-rs skips a field it does not know (`Wire`).
                message.kept = kept && wire == Wire::Kept;
            }
            LOST_FIELD => {
                let lost = varint_of("Message", field, value)? != 0;
                if lost && wire == Wire::Frozen {
                    return Err(EnvelopeError::Unstated("a member's mark"));
                }
                message.lost = lost;
            }
            CLASSIC_FIELD => {
                let classic = varint_of("Message", field, value)?;
                // Below the fence, skipped: nothing known.
                if wire == Wire::Kept {
                    message.classic = Some(classic);
                }
            }
            _ => {}
        }
    }
    message.msg_type = message_type_of(kind)?;
    // raft-rs 0.7's reading of the two priorities (focal-raft `priority_of`): the newer field
    // where it is set, the older where it is not.
    if message.priority == 0 {
        message.priority = i64::try_from(deprecated_priority).unwrap_or(i64::MAX);
    }
    Ok(message)
}

/// The change of one member an entry of focal's log states, read from raft-rs's encoding.
pub fn decode_conf_change(bytes: &[u8]) -> Result<ConfChange> {
    if bytes.is_empty() {
        return Ok(ConfChange::default());
    }
    Ok(ConfChange::decode(&change_from_log(
        EntryType::EntryConfChange,
        bytes,
    )?)?)
}

/// The change of any number an entry of focal's log states, read from raft-rs's encoding.
pub fn decode_conf_change_v2(bytes: &[u8]) -> Result<ConfChangeV2> {
    if bytes.is_empty() {
        return Ok(ConfChangeV2::default());
    }
    Ok(ConfChangeV2::decode(&change_from_log(
        EntryType::EntryConfChangeV2,
        bytes,
    )?)?)
}

/// A value focal's WAL holds in raft-rs's encoding.
pub trait Enveloped {
    /// The value's bytes in the WAL.
    fn envelope(&self) -> Result<Vec<u8>>;
}

impl Enveloped for Entry {
    fn envelope(&self) -> Result<Vec<u8>> {
        encode_entry(self)
    }
}

impl Enveloped for HardState {
    fn envelope(&self) -> Result<Vec<u8>> {
        encode_hard_state(self)
    }
}

impl Enveloped for Snapshot {
    fn envelope(&self) -> Result<Vec<u8>> {
        encode_snapshot(self)
    }
}
