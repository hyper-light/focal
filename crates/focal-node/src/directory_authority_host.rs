//! Live authority refresh uses the existing partition owner and durable retry
//! client. Cancellation cannot replace an unresolved installation intent.
use super::directory_bootstrap_host::normalize_admission;
use super::*;
use crate::directory_bootstrap::{
    DirectoryAuthorityChange, DirectoryAuthorityReceipt, DirectoryBootstrapError,
    PartitionBootstrapPermit,
};

type RefreshResult = Result<DirectoryAuthorityReceipt, DirectoryBootstrapError>;
pub(super) type RefreshReply = oneshot::Sender<(RefreshResult, Allocation)>;

enum RefreshPhase {
    Initial(Box<PartitionBootstrapPermit>),
    Writing(Box<DirectoryAuthorityChange>),
    Committed(Box<DirectoryAuthorityChange>, ControlReceipt),
    Reconcile(Box<DirectoryAuthorityChange>),
}

pub(super) struct PendingAuthority {
    phase: Option<RefreshPhase>,
    context: Option<Vec<u8>>,
    term: u64,
    deadline: Instant,
    response: Option<RefreshReply>,
    reply_charge: Option<Allocation>,
    _input: Allocation,
}
impl PendingAuthority {
    fn respond(&mut self, result: RefreshResult) {
        if let (Some(response), Some(charge)) = (self.response.take(), self.reply_charge.take()) {
            let _ = response.send((result, charge));
        }
    }
    pub(super) fn stop(mut self) {
        self.respond(Err(DirectoryBootstrapError::Unavailable));
    }
}

impl ControlHost {
    /// Install an opaque fresh-root permit into this already-open directory.
    /// Success includes both a durable receipt and a fresh destination barrier.
    /// Canceling the caller does not cancel or replace an admitted proposal.
    pub async fn refresh_directory(&self, permit: PartitionBootstrapPermit) -> RefreshResult {
        let mut input = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 2048)
            .map_err(|_| DirectoryBootstrapError::Capacity)?
            .commit();
        let reply_charge = input
            .split_off(512)
            .map_err(|_| DirectoryBootstrapError::Capacity)?;
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::RefreshDirectory {
                permit: Box::new(permit),
                response,
                input,
                reply_charge,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => DirectoryBootstrapError::Capacity,
                mpsc::TrySendError::Disconnected(_) => DirectoryBootstrapError::Unavailable,
            })?;
        let (result, _reply_charge) = receive
            .await
            .map_err(|_| DirectoryBootstrapError::Unavailable)?;
        result
    }
}

impl<V: AuthorityVerifier> Owner<V> {
    pub(super) fn refresh_directory(
        &mut self,
        permit: Box<PartitionBootstrapPermit>,
        response: RefreshReply,
        input: Allocation,
        reply_charge: Allocation,
    ) {
        let result = (|| {
            if self.authority_refresh.is_some()
                || self
                    .pending
                    .len()
                    .checked_add(usize::from(self.directory.is_some()))
                    .is_none_or(|count| count >= self.config.pending_requests)
            {
                return Err(DirectoryBootstrapError::Capacity);
            }
            if self.config.namespace != permit.plan().namespace()
                || self.config.route_epoch != RouteEpoch(1)
                || self.replica.identity() != permit.plan().identity()?
            {
                return Err(DirectoryBootstrapError::Unauthorized);
            }
            if self.replica.status().role != StateRole::Leader {
                return Err(DirectoryBootstrapError::Unavailable);
            }
            Instant::now()
                .checked_add(self.config.request_timeout)
                .ok_or(DirectoryBootstrapError::Capacity)
        })();
        match result {
            Ok(deadline) => {
                self.authority_refresh = Some(PendingAuthority {
                    phase: Some(RefreshPhase::Initial(permit)),
                    context: None,
                    term: self.replica.status().term,
                    deadline,
                    response: Some(response),
                    reply_charge: Some(reply_charge),
                    _input: input,
                });
            }
            Err(error) => {
                let _ = response.send((Err(error), reply_charge));
            }
        }
    }

    pub(super) fn complete_authority_refresh(&mut self, events: &ControlEvents) {
        let Some(mut pending) = self.authority_refresh.take() else {
            return;
        };
        match self.advance_authority_refresh(&mut pending, events) {
            Ok(Some(receipt)) => pending.respond(Ok(receipt)),
            Err(error) => pending.respond(Err(normalize_admission(error))),
            Ok(None) => self.authority_refresh = Some(pending),
        }
    }

    fn advance_authority_refresh(
        &mut self,
        pending: &mut PendingAuthority,
        events: &ControlEvents,
    ) -> Result<Option<DirectoryAuthorityReceipt>, DirectoryBootstrapError> {
        let initial = matches!(pending.phase, Some(RefreshPhase::Initial(_)));
        let canceled = pending
            .response
            .as_ref()
            .is_some_and(|reply| reply.is_closed());
        if canceled || Instant::now() >= pending.deadline {
            pending.respond(Err(DirectoryBootstrapError::Unavailable));
            if initial {
                return Err(DirectoryBootstrapError::Unavailable);
            }
            // The original proposal and allowance remain live until its
            // committed receipt or a fresh reconciliation barrier settles it.
        }
        let status = self.replica.status();
        if pending.term != status.term || status.role != StateRole::Leader {
            pending.respond(Err(DirectoryBootstrapError::Unavailable));
            if initial {
                return Err(DirectoryBootstrapError::Unavailable);
            }
            pending.context = None;
            pending.term = status.term;
            if status.role != StateRole::Leader {
                return Ok(None);
            }
        }
        if let Some(RefreshPhase::Writing(change)) = pending.phase.as_ref() {
            let id = change
                .request
                .as_ref()
                .ok_or(DirectoryBootstrapError::Inconsistent)?
                .id;
            let receipt = self.replica.receipt(id)?;
            if receipt.is_none() && self.replica.has_pending() {
                return Ok(None);
            }
            let Some(RefreshPhase::Writing(change)) = pending.phase.take() else {
                return Err(DirectoryBootstrapError::Inconsistent);
            };
            pending.phase = Some(match receipt {
                Some(receipt) => RefreshPhase::Committed(change, receipt),
                None => RefreshPhase::Reconcile(change),
            });
            pending.context = None;
        }
        if pending.context.is_none() {
            self.nonce = self
                .nonce
                .checked_add(1)
                .ok_or(DirectoryBootstrapError::Capacity)?;
            let mut context = b"focal.control.directory-refresh.v1\0".to_vec();
            context.extend_from_slice(&self.nonce.to_be_bytes());
            match self.replica.read_index(context.clone()) {
                Ok(()) => pending.context = Some(context),
                Err(ControlError::NotReady | ControlError::Busy | ControlError::Capacity)
                | Err(ControlError::Consensus(
                    focal_consensus::ConsensusError::PersistencePending
                    | focal_consensus::ConsensusError::Capacity,
                ))
                | Err(ControlError::Memory(
                    focal_memory::MemoryError::Capacity { .. }
                    | focal_memory::MemoryError::AllocationFailed,
                )) => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(None);
        }
        if !events.read_states.iter().any(|read| {
            pending.context.as_ref() == Some(&read.context)
                && read.index <= self.replica.applied_index()
        }) {
            return Ok(None);
        }
        pending.context = None;
        let now = crate::network_bootstrap::unix_time()
            .map_err(|_| DirectoryBootstrapError::Unavailable)?;
        match pending
            .phase
            .take()
            .ok_or(DirectoryBootstrapError::Inconsistent)?
        {
            RefreshPhase::Initial(permit) => {
                let change = Box::new(permit.prepare_refresh(&self.replica, now, &self.budget)?);
                let Some(request) = change.request.as_ref() else {
                    return change
                        .complete(&self.replica, change.previous, now, &self.budget)
                        .map(Some);
                };
                // Retain the original exact request. The one submission clone
                // has a separate bounded structural allowance until consumed.
                let bytes = postcard::experimental::serialized_size(request)
                    .map_err(|_| DirectoryBootstrapError::Capacity)?
                    .checked_add(4096)
                    .and_then(|bytes| bytes.checked_mul(16))
                    .ok_or(DirectoryBootstrapError::Capacity)?;
                let _clone = self
                    .budget
                    .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
                    .map_err(|_| DirectoryBootstrapError::Capacity)?
                    .commit();
                let submission = self.replica.submit(request.clone(), &self.verifier)?;
                pending.phase = Some(match submission {
                    ControlSubmission::Existing(receipt) => {
                        RefreshPhase::Committed(change, receipt)
                    }
                    ControlSubmission::Pending(_) => RefreshPhase::Writing(change),
                });
                Ok(None)
            }
            RefreshPhase::Committed(change, receipt) => change
                .complete(&self.replica, receipt, now, &self.budget)
                .map(Some),
            RefreshPhase::Reconcile(change) => {
                let id = change
                    .request
                    .as_ref()
                    .ok_or(DirectoryBootstrapError::Inconsistent)?
                    .id;
                if let Some(receipt) = self.replica.receipt(id)? {
                    change
                        .complete(&self.replica, receipt, now, &self.budget)
                        .map(Some)
                } else if self.replica.has_pending() {
                    pending.phase = Some(RefreshPhase::Writing(change));
                    Ok(None)
                } else {
                    // A current-term barrier has settled the previous log
                    // prefix. No receipt and no pending proposal is now a
                    // definite non-commit; a fresh permit may select this ID.
                    Err(DirectoryBootstrapError::Unavailable)
                }
            }
            RefreshPhase::Writing(_) => Err(DirectoryBootstrapError::Inconsistent),
        }
    }
}

#[cfg(test)]
#[path = "directory_authority_tests.rs"]
mod tests;
