//! Where a node's groups live when it starts, and the storage it opens for them (27 §15.8, "When
//! it runs"; §15.11). Below the upgrade fence's storage level the node runs on focal-log's WAL, as
//! before. At it, a data directory that still holds the WAL is converted first, on the one code
//! path `focal convert storage` runs; then, and for a directory founded at the level, the node's
//! groups live in one hyper-log log, whose growth its `DiskBudget` admits (`DiskGrowth`).
//!
//! The log's configuration comes from facts that do not change between starts, so a log reopened
//! is read with the configuration it was written with: the device's block, read from the node's
//! identity file (on the volume the log lives on, there before the log is); the volume's whole
//! size, which bounds the file's slots (a file is never larger than its volume, so its slots are
//! always within the bound, and recovery refuses, typed, a file past one); the node's admission
//! bound on groups; and its owners' longest checkpoint cadence. The cache is charged to the
//! storage's envelope for the log's life.
use std::path::{Path, PathBuf};

use focal_memory::{Allocation, BudgetKind, BudgetLane, DiskBudget, MemoryBudget, MemoryError};

use crate::convert::{self, ConvertError, LogPlan, Start};
use crate::node_log::{self, NodeLog};
use crate::{ConsensusError, DiskGrowth, NodeConfig, NodeStorage, ShellLog, ShellStorage};

/// The file the node's identity is kept in: always on the data directory's volume, and there
/// before any log is, so the device's block is read from it.
const IDENTITY_FILE: &str = "IDENTITY";

/// What the node states of itself for its log: facts of its configuration, not of its load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageFacts {
    /// The most groups the node may be placed: every session its fleet admits, its root, and
    /// every partition a plan may place on it.
    pub max_groups: usize,
    /// The longest checkpoint cadence among the node's owners, in entries.
    pub cadence_entries: u64,
}

/// A node's storage, opened.
pub struct OpenedStorage {
    /// The handle every group is opened through.
    pub storage: NodeStorage,
    /// The shell's log, where the node runs on it: the owner holds it for the node's life and
    /// closes it after every group that writes through it has stopped.
    pub log: Option<ShellLog>,
    /// The log's cache, charged to the storage's envelope while the log lives.
    pub cache: Option<Allocation>,
    /// The keys of the stores sealed beside the log, where the node runs on the shell (29 §3).
    pub keys: Option<focal_seal::StoreKeys>,
}

impl std::fmt::Debug for OpenedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedStorage")
            .field("storage", &self.storage)
            .field("log", &self.log.is_some())
            .finish()
    }
}

/// Why a node's storage was not opened. Every refusal leaves the data directory as it was found,
/// but where a conversion's own steps say otherwise (`convert_data_dir`).
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error(transparent)]
    Convert(#[from] ConvertError),
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error("the node's WAL: {0}")]
    Wal(#[from] focal_log::LogError),
    #[error("the node's log: {0}")]
    Log(hyper_log::LogError),
    #[error("the node's log file: {0}")]
    Device(#[from] hyper_block::DiskError),
    #[error("the node's storage memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("the node's keys: {0}")]
    Keys(#[from] focal_seal::SealSetupError),
}

/// The configuration of the log a node with `facts` keeps in the data directory `root`, whose
/// storage envelope is `budget`.
pub fn log_plan(
    root: &Path,
    facts: StorageFacts,
    budget: &MemoryBudget,
    key_file: &Path,
) -> Result<LogPlan, OpenError> {
    let block = node_log::device_block(&root.join(IDENTITY_FILE))?;
    let disk_bytes = focal_platform::total_space(root).ok_or(ConsensusError::Configuration(
        "the volume of the data directory reports no size",
    ))?;
    let stats = budget.stats();
    let cache_bytes = u64::try_from(stats.completion_reserve)
        .map_err(|_| ConsensusError::Configuration("a cache past what this machine addresses"))?;
    // Every group a node runs takes `NodeConfig`'s limits: no owner sets others.
    let groups = NodeConfig::single(1, [0; 16], [0; 16]);
    let config = node_log::config(
        &groups,
        &NodeLog {
            block,
            disk_bytes,
            max_groups: facts.max_groups,
            cadence_entries: facts.cadence_entries,
            cache_bytes,
        },
    )?;
    let align = hyper_block::buf::Alignment::new(block)
        .map_err(|_| ConsensusError::Configuration("the device's block is not a power of two"))?;
    Ok(LogPlan {
        config,
        align,
        key_file: key_file.to_path_buf(),
    })
}

/// Opens the storage of node `identity` in the data directory `root`: on focal-log's WAL below
/// the storage level (`fence_open` false), and on the shell at it, converting first where the WAL
/// is still the node's log. `budget` is the storage's envelope, `disk` the node's disk, and
/// `key_file` the root key file the shell's keys open under (29 §2).
pub fn open_node_storage(
    root: &Path,
    identity: focal_log::WalIdentity,
    facts: StorageFacts,
    budget: MemoryBudget,
    disk: DiskBudget,
    fence_open: bool,
    key_file: &Path,
) -> Result<OpenedStorage, OpenError> {
    match convert::start(root, identity, fence_open)? {
        Start::Wal => {
            let wal = focal_log::SharedWal::open_with_budgets(
                root.join(convert::WAL_DIR),
                focal_log::WalOptions::new(identity),
                focal_log::WalWriterLimits::default(),
                budget,
                disk,
            )?;
            Ok(OpenedStorage {
                storage: NodeStorage::Wal(wal),
                log: None,
                cache: None,
                keys: None,
            })
        }
        start => {
            let plan = log_plan(root, facts, &budget, key_file)?;
            if start == Start::Convert {
                convert::convert_data_dir(root, identity, &plan, &budget)?;
            }
            open_shell(root, identity, &plan, budget, disk)
        }
    }
}

fn open_shell(
    root: &Path,
    identity: focal_log::WalIdentity,
    plan: &LogPlan,
    budget: MemoryBudget,
    disk: DiskBudget,
) -> Result<OpenedStorage, OpenError> {
    let cache_bytes = usize::try_from(
        plan.config
            .group_cache
            .saturating_mul(u64::try_from(plan.config.max_groups).unwrap_or(u64::MAX)),
    )
    .map_err(|_| ConsensusError::Configuration("a cache past what this machine addresses"))?;
    let cache = budget
        .reserve(BudgetKind::Payload, BudgetLane::Completion, cache_bytes)?
        .commit();
    let path: PathBuf = root.join(convert::LOG_FILE);
    let exists = path.exists();
    if !exists {
        focal_log::create_durable_directory(&root.join(convert::RAFT_DIR))?;
    }
    let file = hyper_block::file::DeviceFile::open(
        &path,
        !exists,
        hyper_block::file::CachingRequest::PreferDirect,
        plan.align,
    )?;
    let ((parent, auth), keys) = focal_seal::open_or_create(root, &plan.key_file)?.split();
    let with = hyper_log::With {
        sealing: Some(hyper_log::Sealing { parent, auth }),
        growth: Some(Box::new(DiskGrowth::new(disk.clone(), root.to_path_buf())?)),
    };
    let id = convert::log_id(identity);
    let log = if exists {
        hyper_log::Log::open_with(file, plan.config, id, with)
            .map(|(log, _)| log)
            .map_err(|refused| OpenError::Log(refused.error))?
    } else {
        let log = hyper_log::Log::create_with(file, plan.config, id, with)
            .map_err(|refused| OpenError::Log(refused.error))?;
        focal_platform::sync_dir(&root.join(convert::RAFT_DIR))
            .map_err(|error| OpenError::Wal(focal_log::LogError::Io(error)))?;
        log
    };
    let shell = ShellStorage::new(root, &log, disk, identity, budget)?;
    Ok(OpenedStorage {
        storage: NodeStorage::Shell(shell),
        log: Some(log),
        cache: Some(cache),
        keys: Some(keys),
    })
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
#[path = "storage_open_tests.rs"]
mod tests;
