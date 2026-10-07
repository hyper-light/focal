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

/// Format: focal-log's directory under a node's data directory, never removed ([27] §15.3).
pub const WAL_DIR: &str = "wal";
/// Format: hyper-log's directory under a node's data directory.
pub const RAFT_DIR: &str = "raft";
/// Format: hyper-log's file, every group of the data directory on its device.
pub const LOG_FILE: &str = "raft/log";
/// Format: where the converted WAL's segments wait for the operator to remove them.
pub const CONVERTED_DIR: &str = "wal-converted";

/// The log a conversion writes: its configuration (from the device and the node, [27] §15.3) and
/// the file's alignment.
#[derive(Clone, Copy, Debug)]
pub struct LogPlan {
    pub config: hyper_log::Config,
    pub align: hyper_block::buf::Alignment,
}

/// Why a data directory was not converted. Every refusal leaves the WAL the node's log.
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// The volume cannot hold the new log beside the WAL (step 1): nothing was written.
    #[error("the conversion needs {needed} bytes free beside the WAL, and the volume has {free}")]
    Disk { needed: u64, free: u64 },
    #[error("conversion: {0}")]
    Consensus(#[from] ConsensusError),
    #[error("conversion's WAL: {0}")]
    Wal(#[from] focal_log::LogError),
    #[error("conversion's hyper-log: {0}")]
    Log(hyper_log::LogError),
    #[error("conversion I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// What a conversion of a data directory did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Converted now: what was copied, and the segments moved aside.
    Converted { copied: Converted, moved: usize },
    /// Already past the commit point; the segments a crash left behind it were moved.
    Finished { moved: usize },
}

/// The hyper-log log's id for the node `identity` names: the same for every attempt, so a
/// conversion repeated after a crash names the log the first one would have.
pub fn log_id(identity: focal_log::WalIdentity) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"focal.hyper-log.v1");
    hasher.update(&identity.cluster);
    hasher.update(&identity.node.to_le_bytes());
    hasher.update(&identity.stream.to_le_bytes());
    let mut id = [0u8; 16];
    id.copy_from_slice(hasher.finalize().as_bytes().get(..16).unwrap_or(&[0; 16]));
    u128::from_le_bytes(id)
}

/// The bytes a conversion writes beside the WAL, at most: every live byte of the WAL once more,
/// and for each write a frame's header and its padding to a block, and each group's records.
/// Writes: the entries' frames (the live bytes over a frame's room, and one more), and each
/// group's start and hard state alone.
fn needed_bytes(
    live: u64,
    groups: usize,
    plan: &LogPlan,
    frame_room: usize,
) -> Result<u64, ConvertError> {
    let block = u64::try_from(plan.align.get()).map_err(|_| ConsensusError::Capacity)?;
    let room = u64::try_from(frame_room.max(1)).map_err(|_| ConsensusError::Capacity)?;
    let groups = u64::try_from(groups).map_err(|_| ConsensusError::Capacity)?;
    let frames = live
        .checked_div(room)
        .ok_or(ConsensusError::Capacity)?
        .checked_add(1)
        .and_then(|n| n.checked_add(groups.checked_mul(2)?))
        .ok_or(ConsensusError::Capacity)?;
    let meta = u64::try_from(group_files::META_BOUND).map_err(|_| ConsensusError::Capacity)?;
    frames
        .checked_mul(block.checked_mul(2).ok_or(ConsensusError::Capacity)?)
        .and_then(|n| n.checked_add(live))
        .and_then(|n| n.checked_add(groups.checked_mul(meta.checked_add(block)?)?))
        .ok_or(ConvertError::Consensus(ConsensusError::Capacity))
}

fn open_log_file(path: &Path, plan: &LogPlan) -> Result<DeviceFile, ConvertError> {
    DeviceFile::open(
        path,
        true,
        hyper_block::file::CachingRequest::PreferDirect,
        plan.align,
    )
    .map_err(|error| ConvertError::Io(std::io::Error::other(error.to_string())))
}

/// Converts the data directory `root` from focal-log's WAL (`root/wal`) to hyper-log and the group
/// files (`root/raft`): [27] §15.8, steps 1 to 7, in order, the WAL untouched until the commit point.
/// The WAL must be closed; this holds its lock while it reads it. A directory already past the
/// commit point has only its segments moved (step 7), as after a crash.
pub fn convert_data_dir(
    root: &Path,
    identity: focal_log::WalIdentity,
    plan: &LogPlan,
    budget: &MemoryBudget,
) -> Result<Outcome, ConvertError> {
    let wal_dir = root.join(WAL_DIR);
    let converted_dir = root.join(CONVERTED_DIR);
    let id = log_id(identity);
    match focal_log::conversion::storage(&wal_dir)? {
        focal_log::conversion::Storage::Converted { log } if log == id => {
            let moved = focal_log::conversion::move_segments(&wal_dir, &converted_dir)?;
            return Ok(Outcome::Finished { moved });
        }
        focal_log::conversion::Storage::Converted { .. } => {
            return Err(focal_log::LogError::Identity.into());
        }
        focal_log::conversion::Storage::Wal => {}
    }
    let wal = SharedWal::open_with_budget(
        &wal_dir,
        focal_log::WalOptions::new(identity),
        focal_log::WalWriterLimits::default(),
        budget.clone(),
    )?;
    // Step 1: the new store fits beside the old, or nothing is written.
    let groups = wal.logs()?.len();
    let live = wal.stats()?.live_bytes;
    let raft = root.join(RAFT_DIR);
    // A frame holds a segment less its header block and the frame's header (hyper-log's
    // `frame_room`); a segment less two blocks is at most that, so the frames are not undercounted.
    let block = u64::try_from(plan.align.get()).map_err(|_| ConsensusError::Capacity)?;
    let frame_room = plan
        .config
        .segment_bytes
        .checked_sub(block.checked_mul(2).ok_or(ConsensusError::Capacity)?)
        .and_then(|room| usize::try_from(room).ok())
        .ok_or(ConsensusError::Configuration(
            "a segment of the new log holds no frame",
        ))?;
    let needed = needed_bytes(live, groups, plan, frame_room)?;
    let free = focal_platform::available_space(root).unwrap_or(0);
    if free < needed {
        return Err(ConvertError::Disk { needed, free });
    }
    // Step 2: an earlier attempt's store is removed whole; the fence is still version 2, so nothing
    // in it was acknowledged.
    if raft.exists() {
        std::fs::remove_dir_all(&raft)?;
        focal_platform::sync_dir(root)?;
    }
    focal_log::create_durable_directory(&raft)?;
    // Steps 3 and 4: every group copied, durable as written.
    let log_path = root.join(LOG_FILE);
    let log = hyper_log::Log::create(open_log_file(&log_path, plan)?, plan.config, id)
        .map_err(ConvertError::Log)?;
    focal_platform::sync_dir(&raft)?;
    let copied = copy_groups(&wal, root, &log, budget)?;
    log.close().map_err(ConvertError::Log)?;
    if raft.join("groups").exists() {
        focal_platform::sync_dir(&raft.join("groups"))?;
    }
    // Step 5: opened as a restart opens it, and compared.
    let (log, _) = hyper_log::Log::open(open_log_file(&log_path, plan)?, plan.config, id)
        .map_err(ConvertError::Log)?;
    verify(&wal, root, &log, budget)?;
    log.close().map_err(ConvertError::Log)?;
    drop(wal);
    // Step 6: the commit point. Step 7: the old segments aside.
    focal_log::conversion::commit(&wal_dir, identity, id)?;
    let moved = focal_log::conversion::move_segments(&wal_dir, &converted_dir)?;
    Ok(Outcome::Converted { copied, moved })
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
