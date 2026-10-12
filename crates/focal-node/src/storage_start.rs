//! The storage a node's start opens for its groups (27 §15.8, "When it runs"; §15.11): focal-log's
//! WAL below the storage level the node last recorded (`storage_level`), the shell's hyper-log log
//! at it, converted first where the WAL is still the node's log, the log's growth admitted by the
//! node's disk (`focal_consensus::DiskGrowth`).
use std::path::Path;

use focal_consensus::storage_open::{OpenedStorage, StorageFacts};
use focal_log::WalIdentity;
use focal_memory::MemoryBudget;

use crate::embedded::NodeError;

/// The memory the node's storage is given: the envelope focal-log's writer had, which the shell's
/// log takes in its place, its completion reserve the log's cache.
const STORAGE_LIMIT: usize = 256 * 1024 * 1024;
const STORAGE_RESERVE: usize = 64 * 1024 * 1024;

/// What the node states of itself for its log: the most groups it may be placed (every session
/// its fleet admits, its root, and every partition a deployment plan may place), and its owners'
/// longest checkpoint cadence.
pub(crate) fn facts() -> StorageFacts {
    StorageFacts {
        max_groups: crate::fleet::MAX_SESSIONS
            .saturating_add(1)
            .saturating_add(crate::deployment::plan::MAX_PARTITIONS),
        cadence_entries: crate::control_host::CHECKPOINT_INTERVAL
            .max(crate::fleet::CHECKPOINT_AFTER_ENTRIES),
    }
}

/// Opens the storage of the node `identity` names in the data directory `root`, its memory
/// within `budget`, its shell's keys under the root key file `key_file` (29 §2).
///
/// `genesis_level` is the fence the cluster held from its genesis, which never lowers: a cluster
/// founded at the storage level runs every node's groups on the shell from the node's first
/// start, the founder's and each joiner's alike. It is recorded before the storage is chosen;
/// left to the controller's first observation of the fence, as it had been, the node opened the
/// WAL at its first start and converted only at its next.
pub(crate) fn open(
    root: &Path,
    identity: WalIdentity,
    budget: &MemoryBudget,
    key_file: &Path,
    genesis_level: u32,
) -> Result<OpenedStorage, NodeError> {
    if genesis_level >= crate::upgrade::STORAGE_LEVEL {
        crate::storage_level::record(root, genesis_level)?;
    }
    let disk = crate::network_service::disk_budget().map_err(NodeError::Content)?;
    let fence_open = crate::storage_level::storage_opened(root)?;
    Ok(focal_consensus::storage_open::open_node_storage(
        root,
        identity,
        facts(),
        budget
            .child(STORAGE_LIMIT, STORAGE_RESERVE)
            .map_err(focal_consensus::storage_open::OpenError::Memory)?,
        disk,
        fence_open,
        key_file,
    )?)
}

/// The WAL a test's node opened on, for a test that exercises focal-log itself: it founds its
/// node below the storage level (`upgrade::announce_for_test(BELOW_STORAGE_LEVEL)`), so its
/// storage is focal-log's.
#[cfg(test)]
#[allow(clippy::panic)]
pub(crate) fn test_wal(storage: &focal_consensus::NodeStorage) -> &focal_log::SharedWal {
    match storage {
        focal_consensus::NodeStorage::Wal(wal) => wal,
        focal_consensus::NodeStorage::Shell(_) => {
            panic!("a test's node opened on the shell where it expected its WAL")
        }
    }
}
/// A group member a test opens by hand on its node's storage, whichever the node's level gave
/// it, as a hosted session's member is opened (`network_service`).
#[cfg(test)]
pub(crate) fn test_member(
    config: focal_consensus::NodeConfig,
    storage: &focal_consensus::NodeStorage,
) -> Result<focal_consensus::DurableNode, focal_consensus::ConsensusError> {
    let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024)
        .map_err(|_| focal_consensus::ConsensusError::Capacity)?;
    storage.open_member(config, &budget, focal_ledger::entry_needs)
}
