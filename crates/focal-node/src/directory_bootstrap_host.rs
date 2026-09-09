//! One bounded local bootstrap request shares the control owner's quorum and
//! completion budget. It is neither a wire operation nor a separate executor.
use super::*;
use crate::directory_bootstrap::{
    DirectoryBootstrapError, FirstDirectoryPlan, PartitionBootstrapPermit,
    authorize_first_directory,
};

type DirectoryResult = Result<PartitionBootstrapPermit, DirectoryBootstrapError>;
pub(super) type DirectoryReply = oneshot::Sender<(DirectoryResult, Allocation)>;

fn admission_error(error: ControlError) -> DirectoryBootstrapError {
    use focal_consensus::ConsensusError;
    use focal_memory::MemoryError;
    match error {
        ControlError::Capacity
        | ControlError::Busy
        | ControlError::Memory(
            MemoryError::Capacity { .. }
            | MemoryError::DiskCapacity { .. }
            | MemoryError::AllocationFailed,
        )
        | ControlError::Consensus(ConsensusError::Capacity) => DirectoryBootstrapError::Capacity,
        ControlError::NotReady
        | ControlError::Consensus(
            ConsensusError::PersistencePending | ConsensusError::LearnerBehind,
        ) => DirectoryBootstrapError::NotReady,
        ControlError::Consensus(ConsensusError::NotLeader { .. }) => {
            DirectoryBootstrapError::Unavailable
        }
        error => DirectoryBootstrapError::Control(error),
    }
}

pub(super) fn normalize_admission(error: DirectoryBootstrapError) -> DirectoryBootstrapError {
    match error {
        DirectoryBootstrapError::Control(error) => admission_error(error),
        error => error,
    }
}

pub(super) struct PendingDirectory {
    plan: FirstDirectoryPlan,
    context: Vec<u8>,
    term: u64,
    deadline: Instant,
    response: DirectoryReply,
    input: Allocation,
}
impl PendingDirectory {
    pub(super) fn finish(self, result: DirectoryResult) {
        // Sending is not consumption: retain queue/response storage while the
        // caller is suspended. The successful permit owns its larger snapshot.
        let _ = self.response.send((result, self.input));
    }
}

impl ControlHost {
    /// Authorize the initial partition from a fresh root quorum prefix. A
    /// descriptive plan cannot open a group without this owned capability.
    pub async fn prepare_directory(&self, plan: FirstDirectoryPlan) -> DirectoryResult {
        let input = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 1024)
            .map_err(|_| DirectoryBootstrapError::Capacity)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::PrepareDirectory {
                plan,
                response,
                input,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => DirectoryBootstrapError::Capacity,
                mpsc::TrySendError::Disconnected(_) => DirectoryBootstrapError::Unavailable,
            })?;
        let (result, _input) = receive
            .await
            .map_err(|_| DirectoryBootstrapError::Unavailable)?;
        result
    }
}

impl<V: AuthorityVerifier> Owner<V> {
    pub(super) fn prepare_directory(
        &mut self,
        plan: FirstDirectoryPlan,
        response: DirectoryReply,
        input: Allocation,
    ) {
        let result = (|| {
            if self.directory.is_some()
                || self
                    .pending
                    .len()
                    .checked_add(usize::from(self.authority_refresh.is_some()))
                    .is_none_or(|count| count >= self.config.pending_requests)
            {
                return Err(DirectoryBootstrapError::Capacity);
            }
            let status = self.replica.status();
            if self.replica.identity().scope != ControlScope::Root
                || status.role != StateRole::Leader
            {
                return Err(DirectoryBootstrapError::Unavailable);
            }
            let deadline = Instant::now()
                .checked_add(self.config.request_timeout)
                .ok_or(DirectoryBootstrapError::Capacity)?;
            self.nonce = self
                .nonce
                .checked_add(1)
                .ok_or(DirectoryBootstrapError::Capacity)?;
            let mut context = b"focal.control.directory-bootstrap.v1\0".to_vec();
            context.extend_from_slice(&self.nonce.to_be_bytes());
            self.replica
                .read_index(context.clone())
                .map_err(admission_error)?;
            Ok((context, status.term, deadline))
        })();
        match result {
            Ok((context, term, deadline)) => {
                self.directory = Some(PendingDirectory {
                    plan,
                    context,
                    term,
                    deadline,
                    response,
                    input,
                });
            }
            Err(error) => {
                let _ = response.send((Err(error), input));
            }
        }
    }

    pub(super) fn complete_directory(&mut self, events: &ControlEvents) {
        let Some(pending) = self.directory.take() else {
            return;
        };
        if pending.response.is_closed() {
            return;
        }
        let status = self.replica.status();
        if pending.term != status.term
            || status.role != StateRole::Leader
            || Instant::now() >= pending.deadline
        {
            pending.finish(Err(DirectoryBootstrapError::Unavailable));
        } else if events.read_states.iter().any(|read| {
            read.context == pending.context && read.index <= self.replica.applied_index()
        }) {
            let result = crate::network_bootstrap::unix_time()
                .map_err(|_| DirectoryBootstrapError::Unavailable)
                .and_then(|now| {
                    authorize_first_directory(&self.replica, pending.plan, now, &self.budget)
                })
                .map_err(normalize_admission);
            pending.finish(result);
        } else {
            self.directory = Some(pending);
        }
    }
}

#[cfg(test)]
#[path = "directory_bootstrap_host_tests.rs"]
mod tests;
