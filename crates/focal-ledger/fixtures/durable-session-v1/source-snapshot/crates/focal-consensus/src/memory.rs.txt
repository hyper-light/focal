//! Accounting for owned Prost/Raft buffers. Retained bytes use actual capacities;
//! each mutation first reserves clone/fanout headroom before entering Raft.
use crate::{ConsensusError, Entry, Message, NodeConfig, NodeEvents, Snapshot, storage::RamLog};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use raft::RawNode;

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
    let conf = snapshot.get_metadata().get_conf_state();
    let members = [
        conf.voters.capacity(),
        conf.voters_outgoing.capacity(),
        conf.learners.capacity(),
        conf.learners_next.capacity(),
    ]
    .into_iter()
    .try_fold(0, add)?;
    // Snapshot metadata plus the independently retained ConfState clone.
    add(add(snapshot.data.capacity(), mul(members, 16)?)?, 1024)
}
pub(super) fn message_bytes(message: &Message) -> Result<usize, ConsensusError> {
    let mut bytes = add(512, message.context.capacity())?;
    bytes = add(
        bytes,
        mul(message.entries.capacity(), std::mem::size_of::<Entry>())?,
    )?;
    for entry in &message.entries {
        bytes = add(bytes, entry_bytes(entry)?)?;
    }
    if !message.get_snapshot().is_empty() {
        bytes = add(bytes, snapshot_bytes(message.get_snapshot())?)?;
    }
    Ok(bytes)
}
pub(super) fn raw_bytes(raw: &RawNode<RamLog>) -> Result<usize, ConsensusError> {
    let members = raw.raft.prs().iter().len();
    // Includes progress-map/configuration storage and scalar RawNode state.
    let mut bytes = add(4096, mul(members, 2048)?)?;
    bytes = add(bytes, raw.raft.inflight_buffers_size())?;
    bytes = add(
        bytes,
        mul(raw.raft.msgs.capacity(), std::mem::size_of::<Message>())?,
    )?;
    for message in &raw.raft.msgs {
        bytes = add(bytes, message_bytes(message)?)?;
    }
    let unstable = &raw.raft.raft_log.unstable;
    bytes = add(
        bytes,
        mul(unstable.entries.capacity(), std::mem::size_of::<Entry>())?,
    )?;
    for entry in &unstable.entries {
        bytes = add(bytes, entry_bytes(entry)?)?;
    }
    if let Some(snapshot) = &unstable.snapshot {
        bytes = add(bytes, snapshot_bytes(snapshot)?)?;
    }
    // ReadOnly's map/ack set is private upstream. Every admitted context is <=1KiB;
    // account its bounded map and peer-ack overhead, in addition to visible output.
    let reads = add(raw.raft.pending_read_count(), raw.raft.ready_read_count())?;
    bytes = add(bytes, mul(reads, add(4096, mul(members, 64)?)?)?)?;
    bytes = add(bytes, mul(raw.raft.read_states.capacity(), 64)?)?;
    for read in &raw.raft.read_states {
        bytes = add(bytes, read.request_ctx.capacity())?;
    }
    Ok(bytes)
}
pub(super) fn events_bytes(events: &NodeEvents) -> Result<usize, ConsensusError> {
    let mut bytes = 1024usize;
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
    if let Some(snapshot) = &events.snapshot {
        bytes = add(bytes, snapshot.data.capacity())?;
        bytes = add(bytes, snapshot.configuration.charged_bytes()?)?;
    }
    Ok(bytes)
}
pub(super) fn initial_bytes(config: &NodeConfig) -> Result<usize, ConsensusError> {
    add(
        16384,
        mul(add(config.voters.len(), config.learners.len())?, 4096)?,
    )
}
pub(super) fn staging_bytes(
    raw: &RawNode<RamLog>,
    config: &NodeConfig,
    incoming: usize,
    new_members: usize,
) -> Result<usize, ConsensusError> {
    let members = add(raw.raft.prs().iter().len(), new_members)?.max(1);
    // A transition can copy retained entries to one outbound batch per peer,
    // Ready, encoded WAL records, prepared storage, and committed application output.
    // No history-sized temporary allocation escapes this guard.
    let history = raw.store().resident_bytes()?;
    let batch = history.min(config.max_entry_bytes.saturating_add(1024));
    let snapshot = raw.store().snapshot.data.capacity();
    let mut bytes = mul(history, 2)?;
    bytes = add(bytes, mul(add(batch, snapshot)?, add(members, 2)?)?)?;
    bytes = add(bytes, mul(incoming, add(members, 8)?)?)?;
    bytes = add(bytes, mul(raw_bytes(raw)?, 6)?)?;
    bytes = add(
        bytes,
        mul(members, add(mul(config.max_inflight_messages, 8)?, 4096)?)?,
    )?;
    add(bytes, 65536)
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
fn fields(
    mut bytes: &[u8],
    mut visit: impl FnMut(u64, u8, &[u8]) -> Result<(), ConsensusError>,
) -> Result<(), ConsensusError> {
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        let field = tag >> 3;
        if field == 0 {
            return Err(ConsensusError::MalformedMessage("zero protobuf field"));
        }
        let wire = (tag & 7) as u8;
        let length = match wire {
            0 => {
                let original = bytes;
                varint(&mut bytes)?;
                original
                    .len()
                    .checked_sub(bytes.len())
                    .ok_or(ConsensusError::Capacity)?
            }
            1 => 8,
            2 => usize::try_from(varint(&mut bytes)?).map_err(|_| ConsensusError::Capacity)?,
            5 => 4,
            _ => {
                return Err(ConsensusError::MalformedMessage(
                    "unsupported protobuf group/wire type",
                ));
            }
        };
        if wire == 0 {
            visit(field, wire, &[])?;
        } else {
            let (value, tail) = bytes
                .split_at_checked(length)
                .ok_or(ConsensusError::MalformedMessage("truncated protobuf field"))?;
            bytes = tail;
            visit(field, wire, value)?;
        }
    }
    Ok(())
}
fn snapshot_scratch(bytes: &[u8]) -> Result<usize, ConsensusError> {
    let mut members = 0usize;
    fields(bytes, |field, wire, metadata| {
        if field == 2 && wire == 2 {
            fields(metadata, |field, wire, conf| {
                if field == 1 && wire == 2 {
                    fields(conf, |field, wire, mut packed| {
                        if (1..=4).contains(&field) {
                            if wire == 0 {
                                members = add(members, 1)?;
                            } else if wire == 2 {
                                while !packed.is_empty() {
                                    varint(&mut packed)?;
                                    members = add(members, 1)?;
                                }
                            }
                            if members > 2048 {
                                return Err(ConsensusError::Capacity);
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }
        Ok(())
    })?;
    add(add(mul(bytes.len(), 2)?, mul(members, 32)?)?, 4096)
}
pub(super) fn message_scratch(bytes: &[u8]) -> Result<usize, ConsensusError> {
    let mut extra = 4096usize;
    fields(bytes, |field, wire, nested| {
        if field == 7 && wire == 2 {
            extra = add(extra, mul(std::mem::size_of::<Entry>(), 2)?)?;
        }
        if field == 9 && wire == 2 {
            extra = add(extra, snapshot_scratch(nested)?)?;
        }
        Ok(())
    })?;
    add(mul(bytes.len(), 2)?, extra)
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
        return add(mul(members, 16)?, 16384);
    }
    add(mul(record.payload.len(), 2)?, 4096)
}
