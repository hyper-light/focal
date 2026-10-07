//! Where a node's groups live (27 §15.3, §15.11): focal-log's WAL, or the data directory's
//! hyper-log under hyper-durable's shell. Every owner of the node holds one handle, a clone of the
//! node's, opens its members through it, and is told the same writer as every other owner, so
//! nothing past the open names the backend.
use std::path::{Path, PathBuf};

use focal_log::{SharedWal, WalIdentity, WalWriterId};
use focal_memory::{DiskBudget, MemoryBudget, OwnerId};

use crate::{ConsensusError, DurableNode, NodeConfig, RestoredLog, ShellLog, ShellLogOpener};

/// Process-local provenance of the log a shell handle writes: drawn when the node opens its log,
/// never serialized and never reused after the log is opened again, as focal-log's
/// [`WalWriterId`] is. A member opened through the handle carries it, so an owner admits only
/// members of the log it was given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogWriterId(OwnerId);

/// The writer a member's durable state goes through, whichever backend holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageWriter {
    /// focal-log's shared WAL.
    Wal(WalWriterId),
    /// The node's hyper-log log under the shell.
    Log(LogWriterId),
}

/// The node's hyper-log log, as its owners reach it: the data directory its groups' files live
/// in, the opener that claims a group (`Log::opener`), the disk envelope, the identity the log was
/// made for, the memory budget the log is charged to, and its writer.
#[derive(Clone, Debug)]
pub struct ShellStorage {
    root: PathBuf,
    opener: ShellLogOpener,
    disk: DiskBudget,
    identity: WalIdentity,
    budget: MemoryBudget,
    writer: LogWriterId,
}

impl ShellStorage {
    /// The handle on `log`, opened for node `identity` with its groups' files under `root`, its
    /// disk drawn from `disk` and its memory charged within `budget`.
    pub fn new(
        root: &Path,
        log: &ShellLog,
        disk: DiskBudget,
        identity: WalIdentity,
        budget: MemoryBudget,
    ) -> Result<Self, ConsensusError> {
        Ok(Self {
            root: root.to_path_buf(),
            opener: log.opener(),
            disk,
            identity,
            budget,
            writer: LogWriterId(OwnerId::new().map_err(|_| ConsensusError::Capacity)?),
        })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn opener(&self) -> &ShellLogOpener {
        &self.opener
    }
    pub fn disk(&self) -> &DiskBudget {
        &self.disk
    }
    pub fn writer(&self) -> LogWriterId {
        self.writer
    }
}

/// Where the node's groups live.
#[derive(Clone)]
pub enum NodeStorage {
    /// focal-log's WAL: below the upgrade fence's storage level.
    Wal(SharedWal),
    /// The node's hyper-log log under the shell.
    Shell(ShellStorage),
}

impl std::fmt::Debug for NodeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wal(wal) => f.debug_tuple("Wal").field(&wal.writer_id()).finish(),
            Self::Shell(shell) => f.debug_tuple("Shell").field(shell).finish(),
        }
    }
}

impl NodeStorage {
    /// The writer every member opened through this handle goes through.
    pub fn writer(&self) -> StorageWriter {
        match self {
            Self::Wal(wal) => StorageWriter::Wal(wal.writer_id()),
            Self::Shell(shell) => StorageWriter::Log(shell.writer),
        }
    }
    /// The node and cluster the storage was made for.
    pub fn identity(&self) -> Result<WalIdentity, ConsensusError> {
        match self {
            Self::Wal(wal) => Ok(wal.identity()?),
            Self::Shell(shell) => Ok(shell.identity),
        }
    }
    /// The disk envelope the storage draws from, for the node's other durable owners.
    pub fn disk_budget(&self) -> DiskBudget {
        match self {
            Self::Wal(wal) => wal.disk_budget(),
            Self::Shell(shell) => shell.disk.clone(),
        }
    }
    /// Free bytes on the volume, past what a write has been promised, from a sample the disk
    /// envelope refreshes at its bounded cadence; zero while the volume cannot be sampled.
    pub fn available_bytes(&self) -> Result<u64, ConsensusError> {
        match self {
            Self::Wal(wal) => Ok(wal.available_bytes()?),
            Self::Shell(shell) => {
                let root = &shell.root;
                shell
                    .disk
                    .refresh_with(|| focal_platform::available_space(root));
                Ok(shell.disk.uncommitted_free())
            }
        }
    }
    /// Whether the storage's own memory is charged within `parent`.
    pub fn is_budgeted_within(&self, parent: &MemoryBudget) -> bool {
        match self {
            Self::Wal(wal) => wal.is_budgeted_within(parent),
            Self::Shell(shell) => shell.budget.is_within(parent),
        }
    }
    /// A member of group `config`, its memory charged within `parent`. `needs` names the decoder
    /// an entry of the owner's needs, where the shell holds a write for it (27 §15.5, O2); on
    /// focal-log the owner confirms decoders itself.
    pub fn open_member(
        &self,
        config: NodeConfig,
        parent: &MemoryBudget,
        needs: fn(&[u8]) -> Option<[u8; 32]>,
    ) -> Result<DurableNode, ConsensusError> {
        match self {
            Self::Wal(wal) => DurableNode::open_on_wal_in(config, wal.clone(), parent),
            Self::Shell(shell) => DurableNode::open_on_shell(config, shell, parent, needs),
        }
    }
    /// A member of group `config` whose log begins at a restored image (26 §6).
    pub fn restore_member(
        &self,
        config: NodeConfig,
        parent: &MemoryBudget,
        needs: fn(&[u8]) -> Option<[u8; 32]>,
        image: RestoredLog,
    ) -> Result<DurableNode, ConsensusError> {
        match self {
            Self::Wal(wal) => DurableNode::restore_on_wal_in(config, wal.clone(), parent, image),
            Self::Shell(shell) => {
                DurableNode::restore_on_shell(config, shell, parent, needs, image)
            }
        }
    }
}

/// A handle on `log` for this crate's tests: groups' files under `root`, disk drawn from `disk`.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(crate) fn test_shell(root: &Path, log: &ShellLog, disk: DiskBudget) -> ShellStorage {
    ShellStorage::new(
        root,
        log,
        disk,
        WalIdentity {
            cluster: [0; 16],
            node: 1,
            stream: 0,
        },
        MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
    )
    .unwrap()
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
#[path = "node_storage_tests.rs"]
mod tests;
