//! One bounded physical directory owner performs its own WAL recovery and
//! activation before entering the existing control loop. Startup returns its
//! registered handle synchronously; there is no detached bootstrap task.
use super::*;
use crate::directory_bootstrap::{DirectoryBootstrapError, PartitionBootstrapPermit};
use focal_log::SharedWal;

const DIRECTORY_STACK_BYTES: usize = 2 * 1024 * 1024;

/// The existing progress watch owns queue/stack accounting across the physical
/// owner, host clones and egress. This receiver cannot be detached from its
/// allowance; frames additionally retain their individual transport permits.
pub struct DirectoryReplication {
    receiver: async_mpsc::Receiver<ControlReplicationFrame>,
    _progress: watch::Receiver<ControlProgressState>,
}
impl DirectoryReplication {
    pub async fn recv(&mut self) -> Option<ControlReplicationFrame> {
        self.receiver.recv().await
    }
    pub fn try_recv(&mut self) -> Result<ControlReplicationFrame, async_mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
    pub fn close(&mut self) {
        self.receiver.close();
    }
}
impl ControlHost {
    /// The permit comes from the live root's committed ReadIndex authority. WAL
    /// open/activation runs on the returned physical control owner thread. The
    /// caller registers that owner before awaiting readiness; any startup error
    /// marks progress stopped and closes queued callers without granting success.
    pub fn spawn_directory(
        permit: PartitionBootstrapPermit,
        wal: SharedWal,
        budget: MemoryBudget,
    ) -> Result<(Self, ControlOwner, DirectoryReplication), DirectoryBootstrapError> {
        let plan = permit.plan();
        let identity = plan.identity()?;
        let config = ControlHostConfig::new(plan.namespace());
        config.validate()?;
        let limits = Self::wire_limits();
        let queue_items = config
            .queue_items
            .checked_add(config.replication_queue)
            .ok_or(DirectoryBootstrapError::Capacity)?;
        let bytes = queue_items
            .checked_mul(size_of::<Work>().saturating_add(128))
            .and_then(|bytes| {
                config
                    .replication_queue
                    .checked_mul(size_of::<ControlReplicationFrame>().saturating_add(128))
                    .and_then(|egress| bytes.checked_add(egress))
            })
            .and_then(|bytes| {
                bytes.checked_add(size_of::<Owner<crate::cluster::NoDirectoryAuthority>>())
            })
            .and_then(|bytes| bytes.checked_add(DIRECTORY_STACK_BYTES))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(DirectoryBootstrapError::Capacity)?;
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
            .map_err(|_| DirectoryBootstrapError::Capacity)?
            .commit();
        let (sender, receiver) = mpsc::sync_channel(config.queue_items);
        let (peers, incoming) = mpsc::sync_channel(config.replication_queue);
        let (outbound, outgoing) = async_mpsc::channel(config.replication_queue);
        let (progress, changes) = watch::channel(ControlProgressState {
            value: ControlProgress {
                identity,
                node: plan.founder_node(),
                leader: 0,
                term: 0,
                applied_index: 0,
                revisions: ControlRevisions::default(),
                dropped_replication: 0,
                stopped: false,
            },
            _allocation: Some(allocation),
        });
        let host = Self {
            sender,
            peers,
            progress: changes.clone(),
            config: config.clone(),
            limits: limits.clone(),
            budget: budget.clone(),
        };
        let thread = std::thread::Builder::new()
            .name(format!("focal-directory-{}", plan.founder_node()))
            .stack_size(DIRECTORY_STACK_BYTES)
            .spawn(move || {
                let _outcome = catch_unwind(AssertUnwindSafe(
                    || -> Result<(), DirectoryBootstrapError> {
                        let opened = permit.open(wal, &budget)?;
                        let owner = Owner {
                            replica: opened.into_replica(),
                            initial: None,
                            verifier: crate::cluster::NoDirectoryAuthority,
                            config,
                            limits,
                            budget,
                            pending: VecDeque::new(),
                            directory: None,
                            authority_refresh: None,
                            outbound,
                            progress: progress.clone(),
                            nonce: 0,
                            dropped: 0,
                        };
                        owner.run(receiver, incoming);
                        Ok(())
                    },
                ));
                // This boundary is fail-stop, including dependency unwinds. The
                // last actual prefix stays visible; no startup error is a grant.
                progress.send_modify(|state| state.value.stopped = true);
            })
            .map_err(|_| DirectoryBootstrapError::Unavailable)?;
        Ok((
            host,
            ControlOwner(thread),
            DirectoryReplication {
                receiver: outgoing,
                _progress: changes,
            },
        ))
    }
}
