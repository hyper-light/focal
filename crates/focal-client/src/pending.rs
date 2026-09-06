//! Synchronous durable identity for a single manual-client operation. Hold this
//! owner on the CLI's OS thread; perform network awaits between its fsync calls.
//! Dropping a wait or this owner never retracts or forgets an admitted request.
mod files;
use crate::{MutationReply, Operation, RequestEnvelope};
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// This journal supports the ordinary actor wire capability (one MiB per
/// request/receipt). Larger negotiated capabilities require an explicit format
/// extension; they are rejected before any operation directory is created.
pub const MAX_OPERATION_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_OPERATION_RECEIPT_BYTES: usize = 1024 * 1024;
pub(super) const MAX_STATE_BYTES: usize = 4 * 1024 * 1024 + 256;

#[derive(Debug, thiserror::Error)]
pub enum PendingError {
    #[error("operation journal I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("operation journal is locked by another client")]
    Locked,
    #[error("operation already exists; open its exact saved identity to retry")]
    Exists,
    #[error("operation journal requires an owner-private directory and files")]
    Permissions,
    #[error("operation journal is missing, corrupt, or uses an unsupported format")]
    Corrupt,
    #[error("operation belongs to a different cluster, principal, or ledger")]
    ContextMismatch,
    #[error("operation exceeds this journal's bounded wire capability")]
    Capacity,
    #[error("manual operation requires fixed epoch one and cannot advance an epoch floor")]
    EpochPolicy,
    #[error("receipt does not prove the exact pending request committed")]
    ReceiptMismatch,
    #[error("reply does not contain a durable receipt; the request remains pending")]
    NotCommitted,
    #[error("operation journal had an ambiguous write; reopen it before further transmission")]
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationContext {
    pub cluster: [u8; 16],
    pub principal: ParticipantId,
    pub ledger: LedgerId,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationStage {
    OpenEpoch,
    Command,
    Completed,
}

#[derive(Serialize, Deserialize)]
struct State {
    version: u16,
    generation: u64,
    context: OperationContext,
    open_epoch: RequestEnvelope,
    request: RequestEnvelope,
    epoch_receipt: Option<MutationReceipt>,
    receipt: Option<MutationReceipt>,
}
impl State {
    fn stage(&self) -> OperationStage {
        match (&self.epoch_receipt, &self.receipt) {
            (_, Some(_)) => OperationStage::Completed,
            (Some(_), None) => OperationStage::Command,
            (None, None) => OperationStage::OpenEpoch,
        }
    }
    fn next(&self) -> Option<&RequestEnvelope> {
        match self.stage() {
            OperationStage::OpenEpoch => Some(&self.open_epoch),
            OperationStage::Command => Some(&self.request),
            OperationStage::Completed => None,
        }
    }
    fn validate(&self, expected: &OperationContext) -> Result<(), PendingError> {
        if self.context != *expected {
            return Err(PendingError::ContextMismatch);
        }
        if self.version != 1
            || self.generation == 0
            || self.context.principal.is_zero()
            || self.context.ledger.tenant.is_zero()
            || self.context.ledger.session.is_zero()
            || self.context.cluster == [0; 16]
        {
            return Err(PendingError::Corrupt);
        }
        for request in [&self.open_epoch, &self.request] {
            if request.ledger != self.context.ledger
                || request.protocol != focal_wire::PROTOCOL_VERSION
                || request.request_id.is_zero()
                || request.route_epoch.0 == 0
            {
                return Err(PendingError::ContextMismatch);
            }
            if request.request_epoch != RequestEpoch(1) {
                return Err(PendingError::EpochPolicy);
            }
            bounded_size(request, MAX_OPERATION_REQUEST_BYTES)?;
        }
        if self.open_epoch.request_id == self.request.request_id {
            return Err(PendingError::ReceiptMismatch);
        }
        if !matches!(
            self.open_epoch.operation,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1)
            }
        ) || !matches!(self.request.operation, Operation::Submit { .. })
            || matches!(
                self.request.operation,
                Operation::Submit {
                    command: Command::NegotiateEpoch { .. } | Command::AdvanceEpochFloor { .. },
                    ..
                }
            )
        {
            return Err(PendingError::EpochPolicy);
        }
        if let Some(receipt) = &self.epoch_receipt {
            validate_receipt(&self.context, &self.open_epoch, receipt)?;
        }
        if let Some(receipt) = &self.receipt {
            if self
                .epoch_receipt
                .as_ref()
                .is_none_or(|epoch| receipt.sequence <= epoch.sequence)
            {
                return Err(PendingError::Corrupt);
            }
            validate_receipt(&self.context, &self.request, receipt)?;
        }
        let expected_generation = match self.stage() {
            OperationStage::OpenEpoch => 1,
            OperationStage::Command => 2,
            OperationStage::Completed => 3,
        };
        if self.generation != expected_generation {
            return Err(PendingError::Corrupt);
        }
        Ok(())
    }
}

/// A single owner, retaining its private filesystem lock during network waits.
/// No Debug implementation prints request/evidence bytes. Creation never opens
/// an existing directory; retry never creates a missing one. Thus losing even
/// the entire operation directory cannot silently mint replacement identities.
pub struct OperationJournal {
    state: State,
    directory: files::Directory,
    failed: bool,
}
impl OperationJournal {
    pub fn create(
        path: impl AsRef<Path>,
        context: OperationContext,
        open_epoch: RequestEnvelope,
        request: RequestEnvelope,
    ) -> Result<Self, PendingError> {
        let state = State {
            version: 1,
            generation: 1,
            context,
            open_epoch,
            request,
            epoch_receipt: None,
            receipt: None,
        };
        state.validate(&context)?;
        let bytes = encode(&state)?;
        let directory = files::Directory::create(path.as_ref())?;
        directory.install(&bytes, true)?;
        Ok(Self {
            state,
            directory,
            failed: false,
        })
    }
    pub fn open(path: impl AsRef<Path>, expected: &OperationContext) -> Result<Self, PendingError> {
        let directory = files::Directory::open(path.as_ref())?;
        let state = read_state(&directory, expected)?;
        directory.recover_marker()?;
        Ok(Self {
            state,
            directory,
            failed: false,
        })
    }
    /// Complete only the store's unready journal creation from its already
    /// durable prepared envelopes. Existing valid state and receipts survive.
    pub(crate) fn resume_prepared(
        path: &Path,
        context: OperationContext,
        open_epoch: RequestEnvelope,
        request: RequestEnvelope,
    ) -> Result<Self, PendingError> {
        let mut state = State {
            version: 1,
            generation: 1,
            context,
            open_epoch,
            request,
            epoch_receipt: None,
            receipt: None,
        };
        // Validate and bound every supplied byte before any filesystem repair.
        state.validate(&context)?;
        let bytes = encode(&state)?;
        let directory = files::Directory::resume_prepared(path)?;
        if directory.unpublished()? {
            directory.install(&bytes, true)?;
        } else {
            let saved = read_state(&directory, &context)?;
            if saved.open_epoch != state.open_epoch || saved.request != state.request {
                return Err(PendingError::Corrupt);
            }
            directory.recover_marker()?;
            state = saved;
        }
        Ok(Self {
            state,
            directory,
            failed: false,
        })
    }
    pub fn path(&self) -> &Path {
        self.directory.path()
    }
    pub fn context(&self) -> OperationContext {
        self.state.context
    }
    pub(crate) fn matches_requests(
        &self,
        open_epoch: &RequestEnvelope,
        request: &RequestEnvelope,
    ) -> bool {
        self.state.open_epoch == *open_epoch && self.state.request == *request
    }
    pub fn stage(&self) -> OperationStage {
        self.state.stage()
    }
    pub fn receipt(&self) -> Option<&MutationReceipt> {
        self.state.receipt.as_ref()
    }
    pub fn epoch_receipt(&self) -> Option<&MutationReceipt> {
        self.state.epoch_receipt.as_ref()
    }
    pub fn next_request(&self) -> Result<Option<&RequestEnvelope>, PendingError> {
        self.check()?;
        Ok(self.state.next())
    }
    /// The business request is stable even before epoch admission and after
    /// completion. Inspection must query this key, not whichever step is next.
    pub fn business_request(&self) -> Result<&RequestEnvelope, PendingError> {
        self.check()?;
        Ok(&self.state.request)
    }
    /// Bind an observed receipt to the saved command without changing recovery
    /// state. A retained local receipt also fences inconsistent remote outcomes.
    pub fn validate_business_receipt(&self, receipt: &MutationReceipt) -> Result<(), PendingError> {
        self.check()?;
        validate_receipt(&self.state.context, &self.state.request, receipt)?;
        if self
            .state
            .receipt
            .as_ref()
            .is_some_and(|saved| saved != receipt)
            || self
                .state
                .epoch_receipt
                .as_ref()
                .is_some_and(|epoch| receipt.sequence <= epoch.sequence)
        {
            return Err(PendingError::ReceiptMismatch);
        }
        Ok(())
    }
    /// Record only a complete committed/duplicate receipt. Refusal, pending,
    /// missing authority, and unknown outcomes never clear or advance state.
    /// Persisting a receipt fails closed before the next request is exposed.
    pub fn record_reply(&mut self, reply: &MutationReply) -> Result<OperationStage, PendingError> {
        self.check()?;
        let receipt = match reply {
            MutationReply::Committed(receipt) => receipt,
            MutationReply::Domain(DomainOutcome::Duplicate(receipt)) => receipt,
            _ => return Err(PendingError::NotCommitted),
        };
        // Re-recording an exact saved receipt is harmless, including a delayed
        // epoch receipt arriving after the command's receipt was recorded.
        for existing in [&self.state.epoch_receipt, &self.state.receipt]
            .into_iter()
            .flatten()
        {
            if existing.key == receipt.key {
                return if existing == receipt {
                    Ok(self.stage())
                } else {
                    Err(PendingError::ReceiptMismatch)
                };
            }
        }
        let request = self.state.next().ok_or(PendingError::ReceiptMismatch)?;
        validate_receipt(&self.state.context, request, receipt)?;
        if self
            .state
            .epoch_receipt
            .as_ref()
            .is_some_and(|epoch| receipt.sequence <= epoch.sequence)
        {
            return Err(PendingError::ReceiptMismatch);
        }
        let next_generation = self
            .state
            .generation
            .checked_add(1)
            .ok_or(PendingError::Capacity)?;
        let stage = self.state.stage();
        let candidate = receipt.clone();
        // Move the new receipt into the state temporarily for bounded encoding;
        // restore it on every refused/failed write before any caller can inspect.
        self.state.generation = next_generation;
        match stage {
            OperationStage::OpenEpoch => self.state.epoch_receipt = Some(candidate),
            OperationStage::Command => self.state.receipt = Some(candidate),
            OperationStage::Completed => return Err(PendingError::ReceiptMismatch),
        }
        let encoded = encode(&self.state);
        let result = match encoded {
            Ok(bytes) => self.directory.install(&bytes, false),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            self.state.generation = next_generation
                .checked_sub(1)
                .ok_or(PendingError::Corrupt)?;
            match stage {
                OperationStage::OpenEpoch => self.state.epoch_receipt = None,
                OperationStage::Command => self.state.receipt = None,
                OperationStage::Completed => {}
            }
            self.failed = true;
            return Err(error);
        }
        Ok(self.stage())
    }
    fn check(&self) -> Result<(), PendingError> {
        if self.failed {
            Err(PendingError::Failed)
        } else {
            Ok(())
        }
    }
}
fn bounded_size(value: &impl Serialize, max: usize) -> Result<usize, PendingError> {
    let size = postcard::experimental::serialized_size(value).map_err(|_| PendingError::Corrupt)?;
    if size > max {
        Err(PendingError::Capacity)
    } else {
        Ok(size)
    }
}
fn read_state(
    directory: &files::Directory,
    expected: &OperationContext,
) -> Result<State, PendingError> {
    let bytes = directory.read()?;
    let (state, remaining): (State, _) =
        postcard::take_from_bytes(&bytes).map_err(|_| PendingError::Corrupt)?;
    if !remaining.is_empty() {
        return Err(PendingError::Corrupt);
    }
    state.validate(expected)?;
    // Alternative encodings cannot change saved envelope bytes on recovery.
    if encode(&state)? != bytes {
        return Err(PendingError::Corrupt);
    }
    Ok(state)
}
fn encode(state: &State) -> Result<Vec<u8>, PendingError> {
    let size = bounded_size(state, MAX_STATE_BYTES)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| PendingError::Capacity)?;
    bytes.resize(size, 0);
    postcard::to_slice(state, &mut bytes).map_err(|_| PendingError::Corrupt)?;
    Ok(bytes)
}
fn validate_receipt(
    context: &OperationContext,
    request: &RequestEnvelope,
    receipt: &MutationReceipt,
) -> Result<(), PendingError> {
    bounded_size(receipt, MAX_OPERATION_RECEIPT_BYTES)?;
    let key = RequestKey {
        principal: context.principal,
        epoch: request.request_epoch,
        id: request.request_id,
    };
    if receipt.ledger != context.ledger || receipt.key != key || receipt.sequence.0 == 0 {
        return Err(PendingError::ReceiptMismatch);
    }
    let (expected_revision, command) = match &request.operation {
        Operation::OpenEpoch { epoch } if *epoch == RequestEpoch(1) => {
            if receipt.outcome != CommandResult::EpochAdmitted(*epoch) {
                return Err(PendingError::ReceiptMismatch);
            }
            (None, Command::NegotiateEpoch { epoch: *epoch })
        }
        Operation::Submit {
            expected_revision,
            command,
        } => (*expected_revision, command.clone()),
        _ => return Err(PendingError::ReceiptMismatch),
    };
    // command_hash deliberately excludes this context's runtime authority,
    // cause, logical time and refreshed custody; no forged authority is sent.
    let input = AuthenticatedInput {
        ledger: context.ledger,
        principal: context.principal,
        request_epoch: request.request_epoch,
        request_id: request.request_id,
        expected_revision,
        authority: AuthorityContext {
            runtime: false,
            cause: Cause::Root(RootCommandId::default()),
            policy_revision: 0,
            logical_time: 0,
            evidence: Vec::new(),
        },
        command,
    };
    if command_hash(&input).map_err(|_| PendingError::Corrupt)? != receipt.command_hash {
        return Err(PendingError::ReceiptMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
