//! Accounting for owned Raft buffers. Retained bytes use actual capacities;
//! each mutation first reserves clone/fanout headroom before entering Raft.
use crate::{
    ConsensusError, Entry, MAX_MEMBERS, Message, NodeConfig, NodeEvents, Snapshot, storage::RamLog,
};
use focal_memory::{
    ALLOCATOR_OVERHEAD as OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget,
};
use hyper_raft::{
    Outgoing, Raft, RawNode, Storage,
    progress::{Progress, Tracker},
    proto::{self, ConfChangeType, ConfState, HardState},
};
use std::mem::size_of;

// Every fixed allowance below names the structures it stands for and the
// allocations whose bookkeeping (`ALLOCATOR_OVERHEAD` apiece) it carries;
// none is a chosen number (the audit's F16). `tests/allowances.rs` holds
// each to what the counting allocator measures.

/// A snapshot beyond its data: the snapshot and the independently retained
/// copy of its configuration, with the bookkeeping of the data buffer and
/// the eight member lists.
const SNAPSHOT_BYTES: usize = size_of::<Snapshot>() + size_of::<ConfState>() + 9 * OVERHEAD;
/// The events a drain delivers, beyond what they carry: the structure and
/// the bookkeeping of its five lists.
const EVENTS_BYTES: usize = size_of::<NodeEvents>() + 5 * OVERHEAD;
/// The core's own containers, whose bookkeeping its resident bytes do not
/// count: the message queue, the read states, the read-only queue and its
/// pending reads, the held proposals and their bytes, the votes, the
/// decided indexes, the displaced entries and the holders.
const RAW_CONTAINERS: usize = 10;
/// What the node keeps beside the core's resident bytes: the node's own
/// state around the core (the core's own size is in its resident bytes)
/// and its containers' bookkeeping.
const RAW_BYTES: usize =
    size_of::<RawNode<RamLog>>() - size_of::<Raft<RamLog>>() + RAW_CONTAINERS * OVERHEAD;
/// What one member costs the core beyond its resident bytes: the
/// bookkeeping of its slot in the tracker and of its in-flight window.
const MEMBER_BOOKKEEPING: usize = 2 * OVERHEAD;
/// What a transition copies for a member whose progress it makes: the
/// tracker's row for it (its progress, its vote, its match, its place among
/// the members), an in-flight window of `window` indexes, and their
/// bookkeeping.
pub(super) fn member_bytes(window: usize) -> Result<usize, ConsensusError> {
    const ROW: usize =
        size_of::<(u64, Progress)>() + size_of::<(u64, bool)>() + 2 * size_of::<u64>();
    add(
        add(ROW, mul(window, size_of::<(u64, u64)>())?)?,
        MEMBER_BOOKKEEPING,
    )
}
/// The structures one transition builds and drops, and the bookkeeping of
/// their lists: the Ready (four lists and the light Ready's two), the
/// events (five), the drain's phase, the records list and the append's
/// receipt, the hard and soft states.
const TRANSITION_BYTES: usize = size_of::<hyper_raft::Ready>()
    + size_of::<NodeEvents>()
    + size_of::<crate::persistence::PendingDrain>()
    + size_of::<HardState>()
    + size_of::<hyper_raft::SoftState>()
    + 13 * OVERHEAD;
/// One record a transition appends, beyond its payload: the record and the
/// bookkeeping of its payload buffer.
const RECORD_BYTES: usize = size_of::<focal_log::Record>() + OVERHEAD;
/// A configuration's members validated: two ordered sets over the ids, whose
/// leaves are at least half full so a key takes at most two slots of its
/// size and a share of its node's header, and the two sorts' buffers of one
/// id each.
const VALIDATION_SLOTS_PER_MEMBER: usize = 8;
const VALIDATION_BYTES_PER_MEMBER: usize = VALIDATION_SLOTS_PER_MEMBER * size_of::<u64>();
/// A member's id, once in each of two places.
const ID_TWICE: usize = 2 * size_of::<u64>();
/// An entry decoded, beyond its bytes: the entry and the bookkeeping of its
/// two buffers; among a message's entries, its slot twice (the entries list
/// grows by doubling).
const ENTRY_BYTES: usize = size_of::<Entry>() + 2 * OVERHEAD;
const ENTRY_SCRATCH: usize = 2 * size_of::<Entry>() + 2 * OVERHEAD;
/// The identity record's encoding of a configuration: its scalar fields and
/// ids take at most their in-memory size in varints, and each member's id at
/// most ten bytes in whichever list it is; the buffer grows by doubling, so
/// twice the encoding.
const IDENTITY_BYTES: usize = 2 * size_of::<NodeConfig>();
const IDENTITY_BYTES_PER_MEMBER: usize = 2 * 10;
/// What opening a group allocates before its first drain prices it again
/// (`raw_bytes`): the node, the smallest message queue, the identity
/// record with its append (two records' worth of slots), the record buffer
/// and the containers' bookkeeping.
const INITIAL_BYTES: usize = size_of::<crate::DurableNode>()
    + Outgoing::SMALLEST * size_of::<Message>()
    + IDENTITY_BYTES
    + 2 * RECORD_BYTES
    + RAW_CONTAINERS * OVERHEAD;

fn add(a: usize, b: usize) -> Result<usize, ConsensusError> {
    a.checked_add(b).ok_or(ConsensusError::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, ConsensusError> {
    a.checked_mul(b).ok_or(ConsensusError::Capacity)
}
pub(super) fn reserve(
    budget: &MemoryBudget,
    kind: BudgetKind,
    lane: BudgetLane,
    bytes: usize,
) -> Result<Allocation, ConsensusError> {
    budget
        .reserve(kind, lane, bytes)
        .map(|reservation| reservation.commit())
        .map_err(|_| ConsensusError::Capacity)
}
pub(super) fn entry_bytes(entry: &Entry) -> Result<usize, ConsensusError> {
    add(
        add(entry.data.capacity(), entry.context.capacity())?,
        std::mem::size_of::<Entry>(),
    )
}
pub(super) fn snapshot_bytes(snapshot: &Snapshot) -> Result<usize, ConsensusError> {
    let conf = crate::conf_of(crate::metadata_of(snapshot));
    let members = [
        conf.voters.capacity(),
        conf.voters_outgoing.capacity(),
        conf.learners.capacity(),
        conf.learners_next.capacity(),
    ]
    .into_iter()
    .try_fold(0, add)?;
    // Each member's id in the state and in its retained clone.
    add(
        add(snapshot.data.capacity(), mul(members, ID_TWICE)?)?,
        SNAPSHOT_BYTES,
    )
}
/// A message as the core prices it too (`proto::message_bytes`), so one
/// moved between the two costs the same at both.
pub(super) fn message_bytes(message: &Message) -> Result<usize, ConsensusError> {
    let mut bytes = add(proto::MESSAGE_ALLOWANCE, message.context.capacity())?;
    bytes = add(
        bytes,
        mul(message.entries.capacity(), std::mem::size_of::<Entry>())?,
    )?;
    for entry in &message.entries {
        bytes = add(bytes, entry_bytes(entry)?)?;
    }
    // A snapshot rides boxed, held whenever the message carries one.
    if let Some(snapshot) = message.snapshot.as_deref() {
        bytes = add(bytes, snapshot_bytes(snapshot)?)?;
    }
    Ok(bytes)
}
pub(super) fn raw_bytes(raw: &RawNode<RamLog>) -> Result<usize, ConsensusError> {
    let members = raw.raft.tracker().len();
    // What the core says it holds, by capacity: its queue of messages, what
    // is not yet durable, the reads that wait and what it knows of each
    // member. Above it: the node's own state and what an allocator keeps
    // for each of those buffers, the members' apiece.
    add(
        add(RAW_BYTES, mul(members, MEMBER_BOOKKEEPING)?)?,
        raw.raft.resident_bytes(),
    )
}
pub(super) fn events_bytes(events: &NodeEvents) -> Result<usize, ConsensusError> {
    let mut bytes = EVENTS_BYTES;
    bytes = add(
        bytes,
        mul(events.messages.capacity(), std::mem::size_of::<Message>())?,
    )?;
    for message in &events.messages {
        bytes = add(bytes, message_bytes(message)?)?;
    }
    bytes = add(
        bytes,
        mul(
            events.committed.capacity(),
            std::mem::size_of::<crate::CommittedEntry>(),
        )?,
    )?;
    for entry in &events.committed {
        bytes = add(bytes, entry.data.capacity())?;
    }
    bytes = add(
        bytes,
        mul(
            events.membership.capacity(),
            std::mem::size_of::<crate::AppliedMembership>(),
        )?,
    )?;
    for membership in &events.membership {
        bytes = add(bytes, membership.context.capacity())?;
        bytes = add(bytes, membership.before.charged_bytes()?)?;
        bytes = add(bytes, membership.after.charged_bytes()?)?;
    }
    bytes = add(
        bytes,
        mul(
            events.read_states.capacity(),
            std::mem::size_of::<crate::ReadBarrier>(),
        )?,
    )?;
    for read in &events.read_states {
        bytes = add(bytes, read.context.capacity())?;
    }
    bytes = add(
        bytes,
        mul(
            events.displaced.capacity(),
            std::mem::size_of::<crate::CommittedEntry>(),
        )?,
    )?;
    for entry in &events.displaced {
        bytes = add(bytes, entry.data.capacity())?;
    }
    if let Some(snapshot) = &events.snapshot {
        bytes = add(bytes, snapshot.data.capacity())?;
        bytes = add(bytes, snapshot.configuration.charged_bytes()?)?;
    }
    Ok(bytes)
}
/// What one drive of the shell may hand to the owner at most ([27] §15.7, contract (g)): the
/// messages and reads the replica holds (`held`, what its budget holds for it), which leave its
/// count for the events'; and the entries it applies, which the shell bounds by its page, one
/// page or one larger entry (`max_committed_size_per_ready`), counted as the core counts an
/// entry, its fixed bytes beside its data and context. They are no more than the `unapplied`
/// entries above what it applied, nor more than the page holds at the fewest bytes an entry
/// takes, each with its structure and its data's bookkeeping. What a change's configurations
/// take is counted when the events are (`events_bytes`).
///
/// [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
pub(super) fn drive_bytes(
    config: &NodeConfig,
    held: usize,
    unapplied: u64,
) -> Result<usize, ConsensusError> {
    let fixed = hyper_raft::wire::ENTRY_FIXED_BYTES;
    let page = usize::try_from(crate::COMMITTED_PAGE_BYTES)
        .unwrap_or(usize::MAX)
        .max(add(config.max_entry_bytes, fixed)?);
    let fit = page
        .checked_div(fixed)
        .ok_or(ConsensusError::Capacity)
        .and_then(|fit| add(fit, 1))?;
    let count = usize::try_from(unapplied).unwrap_or(usize::MAX).min(fit);
    let data = count
        .checked_mul(add(config.max_entry_bytes, fixed)?)
        .map_or(page, |all| all.min(page));
    let each = add(
        size_of::<crate::CommittedEntry>().max(size_of::<crate::AppliedMembership>()),
        OVERHEAD,
    )?;
    add(add(add(held, data)?, mul(count, each)?)?, EVENTS_BYTES)
}
/// What opening a group allocates: the node and its queue, the identity
/// record and its append, and for each member its validation, its identity
/// bytes, its id in the log's configuration and its row in the tracker
/// (its in-flight window is allocated when its progress is made, and
/// priced by that transition's staging).
pub(super) fn initial_bytes(config: &NodeConfig) -> Result<usize, ConsensusError> {
    let members = add(config.voters.len(), config.learners.len())?;
    let each = add(
        add(VALIDATION_BYTES_PER_MEMBER, IDENTITY_BYTES_PER_MEMBER)?,
        add(size_of::<u64>(), member_bytes(0)?)?,
    )?;
    add(INITIAL_BYTES, mul(members, each)?)
}
/// What a transition's sends copy out of the log (`Raft::send_append`,
/// `bcast_append`, `send_append_all`): a page to each member behind this
/// one — the core's bytes and entries a message at most, and the entries'
/// own slots — from what the member is known to hold, or the snapshot to
/// one behind the log; and to the one member whose answer the transition
/// may be, as many pages as its window admits: its places, and no more
/// pages than hold the bytes it is bounded by and one page beyond them
/// (`Inflights::full`: what is sent passes the bound by one entry at most).
/// A member behind the log is sent the snapshot, or, once its answer says
/// it holds the snapshot, the pages from the log's first entry: the larger
/// of the two, and the window beyond the first page as for any member.
/// Pages are read from the running totals the storage keeps beside its
/// entries; the entries not yet durable are counted whole when a page
/// reaches them. Nothing is walked but the members.
fn sends_bytes(raw: &RawNode<RamLog>) -> Result<usize, ConsensusError> {
    let raft = &raw.raft;
    let log = raft.log();
    let store = raw.store();
    let core = raft.config();
    let per_message = core.limits.entries_per_message.max(1);
    let page = usize::try_from(core.max_size_per_msg)
        .unwrap_or(usize::MAX)
        .saturating_add(per_message.saturating_mul(std::mem::size_of::<Entry>()));
    let window = core.max_inflight_msgs.max(1);
    let first = log.first_index().map_err(ConsensusError::Raft)?;
    let last = log.last_index().map_err(ConsensusError::Raft)?;
    let durable = Storage::last_index(store)?;
    let pending = add(log.unstable().payload(), log.unstable().encoded_bytes())?;
    let snapshot = store
        .snapshot
        .data
        .capacity()
        .max(log.unstable().snapshot_bytes());
    let pages_from = |next: u64, pages: usize| -> Result<usize, ConsensusError> {
        let entries = per_message.saturating_mul(pages);
        let high = next
            .saturating_add(u64::try_from(entries).unwrap_or(u64::MAX))
            .min(last.saturating_add(1));
        if next >= high {
            return Ok(0);
        }
        let cap = page.saturating_mul(pages);
        let stored =
            store.bytes_between(next, high.min(durable.saturating_add(1)), entries, cap)?;
        let not_yet_durable = if high > durable.saturating_add(1) {
            pending
        } else {
            0
        };
        Ok(add(stored, not_yet_durable)?.min(cap))
    };
    let mut pages = 0usize;
    let mut window_more = 0usize;
    for (member, progress) in raft.tracker().iter() {
        if member == raft.id() {
            continue;
        }
        // A member's answer may reject what was sent and move its next
        // index back to what it is known to hold: the page is priced from
        // its matched index, the lowest an answer can reset it to. Below
        // the log, the snapshot; and the answer that it holds the snapshot
        // moves it to the log's first entry, whose pages may be larger.
        let from = progress.matched.saturating_add(1).min(progress.next_index);
        let behind = progress.pending_request_snapshot != 0 || from < first;
        let from = if behind { first } else { from };
        // An answer may make the member one that is sent ahead of its
        // answers, with an empty window: priced by the bound, not by what
        // is left of it.
        let bounded = progress
            .inflights
            .byte_cap()
            .checked_div(core.max_size_per_msg.max(1))
            .unwrap_or(u64::MAX)
            .saturating_add(2);
        let admitted = window.min(usize::try_from(bounded).unwrap_or(usize::MAX));
        let one = pages_from(from, 1)?;
        let all = pages_from(from, admitted)?;
        pages = add(pages, if behind { one.max(snapshot) } else { one })?;
        window_more = window_more.max(all.saturating_sub(one));
    }
    add(pages, window_more)
}
/// The bytes of the log's last entry: what a member that joins is sent
/// first, its progress beginning at the last index (`Tracker::apply`).
fn last_entry_bytes(raw: &RawNode<RamLog>) -> Result<usize, ConsensusError> {
    if let Some(entry) = raw.raft.log().unstable().entries().last() {
        return entry_bytes(entry);
    }
    let last = Storage::last_index(raw.store())?;
    raw.store()
        .bytes_between(last, last.saturating_add(1), 1, usize::MAX)
}
pub(super) fn staging_bytes(
    raw: &RawNode<RamLog>,
    config: &NodeConfig,
    incoming: usize,
    new_members: usize,
) -> Result<usize, ConsensusError> {
    let members = add(raw.raft.tracker().len(), new_members)?.max(1);
    let log = raw.raft.log();
    // What a transition copies, each copy named, and nothing the size of
    // the history (the audit's F16): the entries not yet durable go into
    // the Ready, the WAL's records and the prepared storage; the proposals
    // this member holds by itself likewise; the page the Ready gives of the
    // durable entries above the applied is read from storage and delivered
    // in the events, a page at most; a snapshot on its way goes
    // into the Ready, the prepared storage and the events; what the leader sends is
    // priced from each member's progress (`sends_bytes`); a member whose
    // progress this transition makes is sent the last entry, and its pages
    // come in later transitions, priced then; a proposal's bytes join the
    // entries not yet durable and one message a peer; the queue of
    // messages grows by its own rule. The counters that say what the core
    // holds are kept as it changes, so asking walks nothing but the
    // members.
    let unstable = add(log.unstable().payload(), log.unstable().encoded_bytes())?;
    // A snapshot on its way goes into the Ready, the prepared storage and
    // the events delivered.
    let arriving = log.unstable().snapshot_bytes();
    let held = raw.raft.held_bytes();
    let page = usize::try_from(crate::COMMITTED_PAGE_BYTES).unwrap_or(usize::MAX);
    // What a transition may give to apply: every durable entry above the
    // applied — the transition itself may commit them, a campaign the whole
    // of them — two pages at most: the Ready's, and the page its advance
    // gives (`LightReady`), which the drain applies before it hands over.
    // Each page is the longest prefix that fits a page, and no entry is
    // larger than one, so the two fit twice a page. Reserving one, a node
    // whose log held two pages above its applied could not reopen: both
    // were delivered by its first drain (5,976 entries and 5,960, 33.8 MB
    // against 33.5 reserved). The drain hands over before a third
    // (`drain_progress`).
    let durable = Storage::last_index(raw.store())?;
    let committed_page = raw.store().bytes_between(
        log.applied().saturating_add(1),
        durable.max(log.committed()).saturating_add(1),
        usize::MAX,
        mul(page, 2)?,
    )?;
    let sends = sends_bytes(raw)?;
    let message = usize::try_from(raw.raft.config().max_size_per_msg).unwrap_or(usize::MAX);
    let joining = mul(new_members, last_entry_bytes(raw)?.min(message))?;
    // The queue's slots for the messages a transition may queue — two a
    // member: an append and a heartbeat, a vote and its answer — and the
    // allowance each carries beyond its slot (its entries are priced by the
    // sends); the queue grows by its own rule.
    let queued = mul(members, 2)?;
    let queue = add(
        raw.raft.outgoing().growth_of(queued),
        mul(queued, proto::MESSAGE_ALLOWANCE)?,
    )?;
    let mut bytes = raw_bytes(raw)?;
    bytes = add(bytes, queue)?;
    bytes = add(bytes, mul(unstable, 2)?)?;
    bytes = add(bytes, mul(arriving, 3)?)?;
    bytes = add(bytes, mul(held, 3)?)?;
    bytes = add(bytes, mul(committed_page, 2)?)?;
    bytes = add(bytes, sends)?;
    bytes = add(bytes, joining)?;
    bytes = add(bytes, mul(incoming, add(members, 8)?)?)?;
    bytes = add(
        bytes,
        mul(members, member_bytes(config.max_inflight_messages)?)?,
    )?;
    // The transition's own structures, and a record for each entry not yet
    // durable, the snapshot and the hard state.
    let records = mul(add(log.unstable().entries().len(), 2)?, RECORD_BYTES)?;
    add(add(bytes, records)?, TRANSITION_BYTES)
}
/// The members a committed change adds that the core does not track yet,
/// counted from the change's record as the core holds it, read in place
/// (`hyper_raft::wire::changes_stated`): each change of kind `AddNode` or
/// `AddLearnerNode` naming a member the tracker lacks. A change that does
/// not read adds no one here: the core refuses it when it is applied.
pub(super) fn members_added(entry: &Entry, tracker: &Tracker) -> usize {
    let Ok(changes) = hyper_raft::wire::changes_stated(entry) else {
        return 0;
    };
    changes
        .filter(|change| {
            matches!(
                change.change_type,
                ConfChangeType::AddNode | ConfChangeType::AddLearnerNode
            ) && change.node_id != 0
                && tracker.get(change.node_id).is_none()
        })
        .count()
        .min(MAX_MEMBERS)
}

fn varint(input: &mut &[u8]) -> Result<u64, ConsensusError> {
    let mut value = 0u64;
    for shift in (0..70u32).step_by(7) {
        let (byte, tail) = input.split_first().ok_or(ConsensusError::MalformedMessage(
            "truncated protobuf integer",
        ))?;
        *input = tail;
        if shift == 63 && *byte > 1 {
            return Err(ConsensusError::MalformedMessage(
                "protobuf integer overflow",
            ));
        }
        value |= u64::from(*byte & 0x7f)
            .checked_shl(shift)
            .ok_or(ConsensusError::Capacity)?;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ConsensusError::MalformedMessage(
        "protobuf integer overflow",
    ))
}
/// Walks the fields of an encoded protobuf message: `visit` is given each
/// field's number, wire type, integer (a varint's value; zero otherwise)
/// and bytes (a length-delimited field's; empty otherwise).
fn fields(
    mut bytes: &[u8],
    mut visit: impl FnMut(u64, u8, u64, &[u8]) -> Result<(), ConsensusError>,
) -> Result<(), ConsensusError> {
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        let field = tag >> 3;
        if field == 0 {
            return Err(ConsensusError::MalformedMessage("zero protobuf field"));
        }
        let wire = (tag & 7) as u8;
        let (value, length) = match wire {
            0 => (varint(&mut bytes)?, 0),
            1 => (0, 8),
            2 => (
                0,
                usize::try_from(varint(&mut bytes)?).map_err(|_| ConsensusError::Capacity)?,
            ),
            5 => (0, 4),
            _ => {
                return Err(ConsensusError::MalformedMessage(
                    "unsupported protobuf group/wire type",
                ));
            }
        };
        if wire == 0 {
            visit(field, wire, value, &[])?;
        } else {
            let (nested, tail) = bytes
                .split_at_checked(length)
                .ok_or(ConsensusError::MalformedMessage("truncated protobuf field"))?;
            bytes = tail;
            visit(field, wire, value, nested)?;
        }
    }
    Ok(())
}
/// The workspace decoding a snapshot record takes: twice its bytes and
/// the snapshot's structures.
fn snapshot_scratch(bytes: &[u8]) -> Result<usize, ConsensusError> {
    let mut members = 0usize;
    fields(bytes, |field, wire, _, metadata| {
        if field == 2 && wire == 2 {
            fields(metadata, |field, wire, _, conf| {
                if field == 1 && wire == 2 {
                    members = add(members, conf_members(conf)?)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    })?;
    add(mul(bytes.len(), 2)?, snapshot_structs(members)?)
}
/// What decoding a snapshot builds beyond its bytes: the snapshot and its
/// configuration, each member's id in a list reserved at its length hint,
/// and the bookkeeping of the data buffer and the four lists.
fn snapshot_structs(members: usize) -> Result<usize, ConsensusError> {
    const STRUCTS: usize = size_of::<Snapshot>() + size_of::<ConfState>() + 5 * OVERHEAD;
    add(mul(members, ID_TWICE)?, STRUCTS)
}
/// The workspace decoding a message takes: twice its bytes (its buffers
/// grow by doubling), the message with its buffers' bookkeeping, each
/// entry's slot twice (the entries list doubles too) with the bookkeeping
/// of its two buffers, and a snapshot's structures (its bytes are among
/// the message's).
pub(super) fn message_scratch(bytes: &[u8]) -> Result<usize, ConsensusError> {
    let mut extra = proto::MESSAGE_ALLOWANCE;
    fields(bytes, |field, wire, _, nested| {
        if field == 7 && wire == 2 {
            extra = add(extra, ENTRY_SCRATCH)?;
        }
        if field == 9 && wire == 2 {
            let mut members = 0usize;
            fields(nested, |field, wire, _, metadata| {
                if field == 2 && wire == 2 {
                    fields(metadata, |field, wire, _, conf| {
                        if field == 1 && wire == 2 {
                            members = add(members, conf_members(conf)?)?;
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
            extra = add(extra, snapshot_structs(members)?)?;
        }
        Ok(())
    })?;
    add(mul(bytes.len(), 2)?, extra)
}
/// The members an encoded configuration names across its four lists,
/// refused past twice the most a configuration may hold.
fn conf_members(conf: &[u8]) -> Result<usize, ConsensusError> {
    let mut members = 0usize;
    fields(conf, |field, wire, _, mut packed| {
        if (1..=4).contains(&field) {
            if wire == 0 {
                members = add(members, 1)?;
            } else if wire == 2 {
                while !packed.is_empty() {
                    varint(&mut packed)?;
                    members = add(members, 1)?;
                }
            }
            if members > 2 * MAX_MEMBERS {
                return Err(ConsensusError::Capacity);
            }
        }
        Ok(())
    })?;
    Ok(members)
}
pub(super) fn replay_scratch(record: &focal_log::Record) -> Result<usize, ConsensusError> {
    use focal_log::RecordKind;
    if record.kind == RecordKind::Snapshot {
        return snapshot_scratch(&record.payload);
    }
    if record.kind == RecordKind::Identity {
        // Postcard's Vec decoder can reserve its length hint before consuming
        // elements. Bound both bootstrap arrays without allocating first.
        let (_, tail) = postcard::take_from_bytes::<u64>(&record.payload)?;
        let (_, tail) = postcard::take_from_bytes::<[u8; 16]>(tail)?;
        let (_, mut tail) = postcard::take_from_bytes::<[u8; 16]>(tail)?;
        let mut members = 0usize;
        for _ in 0..2 {
            let (count, next) = postcard::take_from_bytes::<usize>(tail)?;
            members = add(members, count)?;
            if members > 1024 {
                return Err(ConsensusError::Capacity);
            }
            tail = next;
            for _ in 0..count {
                let (_, next) = postcard::take_from_bytes::<u64>(tail)?;
                tail = next;
            }
        }
        // The configuration decoded: each member's id in a list reserved at
        // its length hint, the structure and the two lists' bookkeeping.
        const IDENTITY_SCRATCH: usize = size_of::<NodeConfig>() + 2 * OVERHEAD;
        return add(mul(members, ID_TWICE)?, IDENTITY_SCRATCH);
    }
    // An entry, a proposal or a hard state decoded: twice its bytes (its
    // buffers grow by doubling), the entry and its two buffers' bookkeeping.
    add(mul(record.payload.len(), 2)?, ENTRY_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DurableNode,
        tests::{Cluster, config},
    };
    use hyper_raft::StateRole;

    /// A change naming the most members the core admits — which it then
    /// refuses, the group holding three — is priced by what the transition
    /// copies for each: its progress, its places in the queue, its share
    /// of the incoming bytes and the last entry it is sent; not a page and
    /// the snapshot apiece, which priced the audit's malformed message at
    /// 8.6 GB and refused it for capacity before the core could refuse it.
    #[test]
    fn a_member_that_joins_is_priced_its_progress_and_the_last_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = DurableNode::open(config(1), dir.path()).unwrap();
        node.campaign().unwrap();
        node.drain().unwrap();
        node.propose(vec![7; 300]).unwrap();
        node.drain().unwrap();
        let none = staging_bytes(&node.log().raw, &node.log().config, 657, 0).unwrap();
        let most = staging_bytes(&node.log().raw, &node.log().config, 657, MAX_MEMBERS).unwrap();
        let last = last_entry_bytes(&node.log().raw).unwrap();
        assert!((300..1024).contains(&last), "{last}");
        // Two places in the queue a member (`Outgoing::growth_of` reserves
        // twice the count) and the two messages' allowances, its share of
        // the incoming bytes, its row and window in the tracker and the
        // last entry.
        let each = 4 * std::mem::size_of::<Message>()
            + 2 * proto::MESSAGE_ALLOWANCE
            + 657
            + member_bytes(node.log().config.max_inflight_messages).unwrap()
            + last;
        assert_eq!(most - none, MAX_MEMBERS * each);
        assert!(most < 8 * 1024 * 1024, "{most}");
    }

    /// A member behind the leader is priced the page it is sent and, for
    /// the one whose answer the transition may be, the window of pages;
    /// the pages read from the running totals are what a walk of the
    /// entries says.
    #[test]
    fn a_member_behind_is_priced_its_pages_from_the_running_totals() {
        let mut cluster = Cluster::new();
        cluster.nodes[0].campaign().unwrap();
        cluster.pump(None);
        assert_eq!(cluster.nodes[0].status().role, StateRole::Leader);
        // Member 3 hears nothing while forty entries commit on the other
        // two; the leader then learns it could not be reached and probes it
        // from where it was.
        for round in 0..40u8 {
            cluster.nodes[0]
                .propose(vec![round; 1024 + usize::from(round) * 16])
                .unwrap();
            cluster.pump(Some(3));
        }
        cluster.nodes[0].report_unreachable(3).unwrap();
        let leader = &cluster.nodes[0];
        let raft = &leader.log().raw.raft;
        let core = raft.config();
        let store = leader.log().raw.store();
        assert!(raft.log().unstable().entries().is_empty());
        let last = raft.log().last_index().unwrap();
        let page = |pages: usize| {
            usize::try_from(core.max_size_per_msg).unwrap() * pages
                + core.limits.entries_per_message * pages * std::mem::size_of::<Entry>()
        };
        let walk = |next: u64, pages: usize| {
            store
                .entries
                .iter()
                .filter(|entry| entry.index >= next && entry.index <= last)
                .take(core.limits.entries_per_message * pages)
                .map(|entry| entry_bytes(entry).unwrap())
                .sum::<usize>()
                .min(page(pages))
        };
        let (mut expected, mut more, mut behind) = (0usize, 0usize, 0usize);
        for (member, progress) in raft.tracker().iter() {
            if member == raft.id() {
                continue;
            }
            assert!(progress.next_index >= raft.log().first_index().unwrap());
            let one = walk(progress.next_index, 1);
            let all = walk(progress.next_index, core.max_inflight_msgs);
            if progress.next_index <= last {
                behind += 1;
                assert!(one > 0);
                assert_eq!(all, one, "forty entries are one page");
            }
            expected += one;
            more = more.max(all - one);
        }
        assert_eq!(behind, 1, "member 3 alone is behind");
        assert_eq!(sends_bytes(&leader.log().raw).unwrap(), expected + more);
        // The estimate names that page once, whatever lies behind it.
        let with = staging_bytes(&leader.log().raw, &leader.log().config, 0, 0).unwrap();
        assert!(with >= expected, "{with} < {expected}");
    }
}
