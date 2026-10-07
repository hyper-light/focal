//! The conversion of a node's groups from focal-log's WAL to hyper-log and the group files ([27]
//! §15.8, steps 3 and 5): each group read through focal-log's replay, the same replay a member opens
//! on, then written as the shell opens it, and read back and compared value for value.
//!
//! One group is held in memory at a time, under the caller's budget. What the old log holds of a
//! group and where it goes ([27] §15.2):
//! - its identity, fast track and decoder records: `groups/<id>/meta`, written first, so nothing of
//!   the group is in hyper-log before its records are durable (O1);
//! - its snapshot: `groups/<id>/image`, the image with its point and configuration, written before
//!   the log's start moves to its point (O3);
//! - its entries, hard state and proposals: the group's log in hyper-log, the entries a frame at a
//!   time, the hard state and proposals last, as a `Ready` writes them.
//!
//! A group on the fast track is refused: the shell does not carry it yet (`ShellNode::open`), and a
//! conversion that dropped its proposals would forget what the member approved by itself.
//!
//! Nothing here touches the old WAL's files or its fence: the caller commits the conversion (fence
//! version 3) only after [`verify`] passes, and until then the old WAL is the node's log.
//!
//! [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
use std::path::Path;

use focal_log::{LogicalLogId, RecordKind, SharedWal};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_platform::fs::FileMedium;
use hyper_block::file::DeviceFile;
use hyper_durable::{
    ClaimError, ENTRY_OVERHEAD, Entries, Fault, GroupStore, LogStore, Point, Write,
};
use hyper_log::Log;
use hyper_raft::proto::{Entry, HardState, Snapshot};

use super::*;
use crate::group_files::{self, GroupFileError, GroupRecords, ImagePoint};
use crate::storage::RamLog;

/// What a conversion copied: groups, entries and bytes, for the node's record of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Converted {
    /// Groups copied.
    pub groups: usize,
    /// Entries copied, every group's.
    pub entries: u64,
    /// The entries' data and context bytes.
    pub entry_bytes: u64,
    /// The images' bytes.
    pub image_bytes: u64,
}

/// One group as focal-log holds it: its records, and its log as a member's replay leaves it.
struct OldGroup {
    records: GroupRecords,
    storage: RamLog,
}

/// The group's id in hyper-log: focal's 16 bytes, as the shell claims it (`ShellNode::open`).
fn group_of(id: [u8; 16]) -> u128 {
    u128::from_be_bytes(id)
}

/// Reads group `id` through focal-log's replay, as `DurableNode::open_on_wal_in` does: its first
/// record states what the group is, and every other is replayed by the same rules. `None` for a
/// log that holds nothing of the group.
fn read_old(
    wal: &SharedWal,
    id: LogicalLogId,
    budget: &MemoryBudget,
) -> Result<Option<OldGroup>, ConsensusError> {
    let lease = wal.lease(id)?;
    let mut opened: Option<(NodeConfig, RamLog)> = None;
    let mut fast = false;
    let mut floor = None;
    let mut transition = None;
    let mut failed = None;
    lease.replay(|record| {
        if failed.is_some() {
            return Ok(());
        }
        let step = (|| {
            if record.log != id {
                return Err(ConsensusError::Configuration(
                    "stream belongs to another group",
                ));
            }
            let bytes = memory::replay_scratch(&record)?;
            let _decode =
                memory::reserve(budget, BudgetKind::Recovery, BudgetLane::Completion, bytes)?;
            match opened.as_mut() {
                None => {
                    // A group's first record is its identity, as its first open and every
                    // checkpoint write it (`checkpoint.rs`).
                    if record.kind != RecordKind::Identity {
                        return Err(ConsensusError::Corruption("missing group identity"));
                    }
                    let config: NodeConfig = postcard::from_bytes(&record.payload)?;
                    if config.group_id != id.0 {
                        return Err(ConsensusError::Configuration(
                            "stream belongs to another group",
                        ));
                    }
                    let storage = RamLog::new(&config, budget.clone())?;
                    opened = Some((config, storage));
                    Ok(())
                }
                Some((config, storage)) => {
                    let mut stated = Some(config.clone());
                    replay_record(
                        storage,
                        &mut stated,
                        &mut fast,
                        &mut floor,
                        &mut transition,
                        record,
                    )
                }
            }
        })();
        if let Err(error) = step {
            failed = Some(error);
        }
        Ok(())
    })?;
    if let Some(error) = failed {
        return Err(error);
    }
    let Some((identity, storage)) = opened else {
        return Ok(None);
    };
    storage.validate()?;
    Ok(Some(OldGroup {
        records: GroupRecords {
            identity,
            fast,
            decoder_floor: floor,
            decoder_transition: transition
                .map(|pair: decoder::DecoderPair| (pair.predecessor, pair.successor)),
        },
        storage,
    }))
}

/// The point and configuration of the group's snapshot; none while the group's log is complete
/// from its first entry.
fn image_point(snapshot: &Snapshot) -> Option<ImagePoint> {
    if proto::snapshot_is_empty(snapshot) {
        return None;
    }
    let metadata = metadata_of(snapshot);
    Some(ImagePoint {
        index: metadata.index,
        term: metadata.term,
        configuration: conf_of(metadata).clone(),
    })
}

/// A store's refusal as the conversion's error: a refusal for room is the node's log too small for
/// a group it holds, which the conversion states, never retries.
fn store_error(fault: &Fault) -> ConsensusError {
    match fault {
        Fault::Room(_) | Fault::Behind | Fault::Held => ConsensusError::Capacity,
        Fault::Failed(_) => ConsensusError::Configuration("hyper-log refused a converted write"),
    }
}

fn file_error(error: GroupFileError) -> ConsensusError {
    match error {
        GroupFileError::Corrupt { reason, .. } => ConsensusError::Corruption(reason),
        GroupFileError::Bound { .. } => ConsensusError::Capacity,
        GroupFileError::Encoding(error) => ConsensusError::Encoding(error),
        GroupFileError::Io(error) => ConsensusError::Log(focal_log::LogError::Io(error)),
    }
}

/// Claims group `id` in `log`, which must hold nothing of it: a conversion writes into a log made
/// for it (step 2 removes any earlier attempt's).
fn claim_empty(
    log: &Log<DeviceFile>,
    id: [u8; 16],
) -> Result<GroupStore<DeviceFile>, ConsensusError> {
    let store = match GroupStore::claim(log, group_of(id)) {
        Ok(store) => store,
        Err(ClaimError::Damaged) => {
            return Err(ConsensusError::Corruption(
                "the group's acknowledged records are damaged",
            ));
        }
        Err(ClaimError::Log(_)) => {
            return Err(ConsensusError::Configuration(
                "the node's log refused the group",
            ));
        }
    };
    Ok(store)
}

/// An entry's bytes as hyper-log charges them in a frame.
fn encoded(entry: &Entry) -> Result<usize, ConsensusError> {
    entry
        .data
        .len()
        .checked_add(entry.context.len())
        .and_then(|n| n.checked_add(ENTRY_OVERHEAD))
        .ok_or(ConsensusError::Capacity)
}

/// Writes one group as the shell opens it: its records, then its image, then its log, each durable
/// before the next (O1, O3).
fn write_new(
    old: &OldGroup,
    root: &Path,
    log: &Log<DeviceFile>,
    converted: &mut Converted,
) -> Result<(), ConsensusError> {
    if old.records.fast {
        return Err(ConsensusError::Configuration(
            "a group on the fast track cannot move to the shell yet",
        ));
    }
    let id = old.records.identity.group_id;
    let mut medium = FileMedium;
    let dir = group_files::create(&mut medium, root, id).map_err(file_error)?;
    group_files::write_records(&mut medium, &dir, &old.records).map_err(file_error)?;
    let storage = &old.storage;
    let start = match image_point(&storage.snapshot) {
        Some(point) => {
            group_files::write_image(
                &mut medium,
                &dir,
                &point,
                &storage.snapshot.data,
                IMAGE_BYTES,
            )
            .map_err(file_error)?;
            converted.image_bytes = converted.image_bytes.saturating_add(
                u64::try_from(storage.snapshot.data.len()).map_err(|_| ConsensusError::Capacity)?,
            );
            Some(Point {
                index: point.index,
                term: point.term,
            })
        }
        None => None,
    };
    let mut store = claim_empty(log, id)?;
    let view = store.view().map_err(|fault| store_error(&fault))?;
    if view.start.index > 0 || view.last > 0 || view.hard_state != HardState::default() {
        return Err(ConsensusError::Corruption(
            "the conversion's log already holds the group",
        ));
    }
    // A frame's worth of entries a write, so the copy holds one frame beside the group.
    let room = log
        .entry_room()
        .map_err(|_| ConsensusError::Configuration("the node's log has no entry room"))?;
    let frame = log
        .frame_room()
        .map_err(|_| ConsensusError::Configuration("the node's log has no frame room"))?;
    let (front, back) = storage.entries.as_slices();
    let mut first_write = true;
    for slice in [front, back] {
        let mut begin = 0usize;
        while begin < slice.len() {
            let mut end = begin;
            let mut bytes = 0usize;
            while let Some(entry) = slice.get(end) {
                let size = encoded(entry)?;
                if size > room {
                    return Err(ConsensusError::Configuration(
                        "the node's log cannot hold one of the group's entries in a frame",
                    ));
                }
                let next = bytes.checked_add(size).ok_or(ConsensusError::Capacity)?;
                if end > begin && next > frame {
                    break;
                }
                bytes = next;
                end = end.saturating_add(1);
            }
            let chunk = slice.get(begin..end).ok_or(ConsensusError::Capacity)?;
            let first = chunk
                .first()
                .map(|entry| entry.index)
                .ok_or(ConsensusError::Capacity)?;
            let write = Write {
                start: if first_write { start } else { None },
                entries: Some(Entries {
                    first,
                    entries: chunk,
                }),
                hard_state: None,
                proposals: &[],
            };
            store
                .write_now(&write)
                .map_err(|fault| store_error(&fault))?;
            first_write = false;
            converted.entries = converted
                .entries
                .saturating_add(u64::try_from(chunk.len()).map_err(|_| ConsensusError::Capacity)?);
            converted.entry_bytes = converted
                .entry_bytes
                .saturating_add(u64::try_from(bytes).map_err(|_| ConsensusError::Capacity)?);
            begin = end;
        }
    }
    // The hard state last, as a `Ready` writes it: after the entries it may commit. The start too,
    // where no entry carried it.
    let write = Write {
        start: if first_write { start } else { None },
        entries: None,
        hard_state: (storage.hard_state != HardState::default()).then_some(storage.hard_state),
        proposals: &[],
    };
    if !write.is_empty() {
        store
            .write_now(&write)
            .map_err(|fault| store_error(&fault))?;
    }
    converted.groups = converted.groups.saturating_add(1);
    Ok(())
}

/// Steps 3 and 4 of the conversion: every group of `wal` written under `root` and into `log`, one
/// group held at a time. Returns what was copied.
pub fn copy_groups(
    wal: &SharedWal,
    root: &Path,
    log: &Log<DeviceFile>,
    budget: &MemoryBudget,
) -> Result<Converted, ConsensusError> {
    let mut converted = Converted::default();
    for id in wal.logs()? {
        let Some(old) = read_old(wal, id, budget)? else {
            continue;
        };
        write_new(&old, root, log, &mut converted)?;
    }
    Ok(converted)
}

/// Compares one group's new home with what focal-log holds of it, value for value.
fn verify_group(old: &OldGroup, root: &Path, log: &Log<DeviceFile>) -> Result<(), ConsensusError> {
    let mismatch = |what: &'static str| Err(ConsensusError::Corruption(what));
    let id = old.records.identity.group_id;
    let medium = FileMedium;
    let dir = group_files::group_dir(root, id);
    let records = group_files::read_records(&medium, &dir).map_err(file_error)?;
    if records.as_ref() != Some(&old.records) {
        return mismatch("converted records differ");
    }
    let storage = &old.storage;
    let image = group_files::read_image(&medium, &dir, IMAGE_BYTES).map_err(file_error)?;
    match (image_point(&storage.snapshot), image) {
        (None, None) => {}
        (Some(point), Some((read, data))) => {
            if read != point || data != storage.snapshot.data {
                return mismatch("converted image differs");
            }
        }
        _ => return mismatch("converted image differs"),
    }
    let store = match GroupStore::claim(log, group_of(id)) {
        Ok(store) => store,
        Err(_) => return mismatch("converted group did not open"),
    };
    let view = store.view().map_err(|fault| store_error(&fault))?;
    let start = image_point(&storage.snapshot).map_or(Point::default(), |point| Point {
        index: point.index,
        term: point.term,
    });
    if view.start != start
        || view.last != storage.last_index()?
        || view.hard_state != storage.hard_state
    {
        return mismatch("converted log's bounds or hard state differ");
    }
    let mut expected = storage.entries.iter();
    let mut read = Vec::new();
    let mut next = start.index.saturating_add(1);
    let high = view.last.saturating_add(1);
    let page = u64::try_from(
        log.frame_room()
            .map_err(|_| ConsensusError::Configuration("the node's log has no frame room"))?,
    )
    .map_err(|_| ConsensusError::Capacity)?;
    while next < high {
        read.clear();
        store
            .entries(next, high, page, &mut read)
            .map_err(|_| ConsensusError::Corruption("converted entries do not read"))?;
        if read.is_empty() {
            return mismatch("converted entries do not read");
        }
        for entry in &read {
            if expected.next() != Some(entry) {
                return mismatch("converted entry differs");
            }
        }
        next =
            next.saturating_add(u64::try_from(read.len()).map_err(|_| ConsensusError::Capacity)?);
    }
    if expected.next().is_some() {
        return mismatch("converted log lacks entries");
    }
    let mut proposals = Vec::new();
    store
        .proposals(&mut proposals)
        .map_err(|_| ConsensusError::Corruption("converted proposals do not read"))?;
    if !proposals.is_empty() {
        return mismatch("converted group holds proposals");
    }
    Ok(())
}

/// Step 5 of the conversion: `log` and the group files under `root`, opened as a restart opens them,
/// hold every group of `wal` and nothing else, each equal value for value.
pub fn verify(
    wal: &SharedWal,
    root: &Path,
    log: &Log<DeviceFile>,
    budget: &MemoryBudget,
) -> Result<(), ConsensusError> {
    let mut groups = 0usize;
    for id in wal.logs()? {
        let Some(old) = read_old(wal, id, budget)? else {
            continue;
        };
        verify_group(&old, root, log)?;
        groups = groups.saturating_add(1);
    }
    let held = log
        .groups()
        .map_err(|_| ConsensusError::Corruption("the converted log's groups do not read"))?;
    if held.len() != groups {
        return Err(ConsensusError::Corruption(
            "the converted log holds groups the WAL does not",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
#[path = "convert_tests.rs"]
mod tests;
