#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Deterministic domain reducer and audited ordered-epoch execution. Authority,
//! time, IDs and evaluator results are inputs. Owned row updates publish atomically.
mod access;
mod admission;
#[cfg(test)]
mod admission_tests;
mod durable_v1;
mod epoch;
mod execution;
mod execution_v1;
#[cfg(test)]
mod execution_v1_tests;
mod managed;
pub mod native;
mod overlay;
mod pending;
mod reconciliation;
#[cfg(test)]
mod tests;
pub use access::{AccessFootprint, AccessKey, StateTable, TrackedResult};
pub use epoch::{EpochError, EpochLimits, EpochOutput, EpochPlan, EpochReport};
pub use execution_v1::least_fixpoint;
use focal_model::*;
pub use managed::{ManagedApplyResult, PreparedManagedMutation, StagedManagedMutation};
pub use pending::{CoreView, PendingState, RowPatch, StagedMutation, StagingError};
pub use reconciliation::{
    ReceiptResolutionView, ReconcileResultView, ReconciliationError, ReconciliationView,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochWindow {
    pub minimum: RequestEpoch,
    pub admitted: BTreeSet<RequestEpoch>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub claims: BTreeMap<ClaimId, Claim>,
    pub validations: BTreeMap<ValidationId, Validation>,
    pub artifacts: BTreeMap<ArtifactId, Artifact>,
    pub testaments: BTreeMap<TestamentId, Testament>,
    pub evidence_sets: BTreeMap<EvidenceSetId, EvidenceSet>,
    pub runs: BTreeMap<ValidationRunId, ValidationRun>,
    pub monitors: BTreeMap<MonitorId, Monitor>,
    pub identities: BTreeMap<(ObjectKind, ContentHash), ObjectId>,
    pub epochs: BTreeMap<ParticipantId, EpochWindow>,
    pub receipts: BTreeMap<RequestKey, MutationReceipt>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedMutation {
    pub schema: u16,
    pub base: SessionSeq,
    pub input: AuthenticatedInput,
    pub command_hash: ContentHash,
    /// Conservative complete session conflict key. Parallel refinements must retain serial equivalence.
    pub footprint: Footprint,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Footprint {
    pub ledger: LedgerId,
    pub session_exclusive: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResult {
    pub receipt: MutationReceipt,
    pub deltas: Vec<Delta>,
    pub effects: Vec<EffectIntent>,
}
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("canonical identity: {0}")]
    Canonical(#[from] CanonicalError),
    #[error("unsupported persisted schema {0}")]
    UnsupportedSchema(u16),
    #[error("sequence discontinuity: expected {expected:?}, got {actual:?}")]
    Sequence {
        expected: SessionSeq,
        actual: SessionSeq,
    },
    #[error("prepared base is stale")]
    StalePreparation,
    #[error("committed command violates deterministic admission: {0}")]
    Determinism(DomainOutcome),
    #[error("codec error: {0}")]
    Codec(#[from] postcard::Error),
    #[error("checkpoint checksum mismatch")]
    Checksum,
    #[error("counter exhausted")]
    Exhausted,
}
mod state_kind {
    pub trait Sealed {}
}
/// The owner has exactly one representation. Native rows cannot enter a V1
/// checkpoint or acquire the legacy mutation APIs through a mutable sidecar.
pub trait CoreState: state_kind::Sealed {
    type Limits;
}
impl state_kind::Sealed for State {}
impl CoreState for State {
    type Limits = Limits;
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "S: Serialize, S::Limits: Serialize",
    deserialize = "S: Deserialize<'de>, S::Limits: Deserialize<'de>"
))]
pub struct Core<S: CoreState = State> {
    state: S,
    limits: S::Limits,
}
impl Core {
    pub fn new(ledger: LedgerId, limits: Limits) -> Self {
        Self {
            limits,
            state: State {
                ledger,
                sequence: SessionSeq(0),
                claims: BTreeMap::new(),
                validations: BTreeMap::new(),
                artifacts: BTreeMap::new(),
                testaments: BTreeMap::new(),
                evidence_sets: BTreeMap::new(),
                runs: BTreeMap::new(),
                monitors: BTreeMap::new(),
                identities: BTreeMap::new(),
                epochs: BTreeMap::new(),
                receipts: BTreeMap::new(),
            },
        }
    }
    pub fn snapshot(&self) -> &State {
        &self.state
    }
    pub fn sequence(&self) -> SessionSeq {
        self.state.sequence
    }
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
    pub fn prepare(&self, input: &AuthenticatedInput) -> Result<PreparedMutation, DomainOutcome> {
        self.prepare_recorded(
            input,
            &access::Recorder::disabled(self.state.ledger, self.state.sequence),
        )
    }
    /// Executes the same admission/reducer while recording actual accesses.
    /// The bounded report is diagnostic/planner input; it does not grant parallel
    /// eligibility or change the persisted session-exclusive legacy footprint.
    pub fn prepare_tracked(
        &self,
        input: &AuthenticatedInput,
        max_entries: usize,
    ) -> TrackedResult<PreparedMutation, DomainOutcome> {
        let recorder = access::Recorder::new(self.state.ledger, self.state.sequence, max_entries);
        let result = self.prepare_recorded(input, &recorder);
        TrackedResult {
            result,
            accesses: recorder.finish(),
        }
    }
    fn prepare_recorded(
        &self,
        input: &AuthenticatedInput,
        recorder: &access::Recorder,
    ) -> Result<PreparedMutation, DomainOutcome> {
        self.stage_recorded(input, &[], self.sequence(), recorder, false)
            .map(|staged| staged.prepared)
    }
    pub fn apply(
        &mut self,
        sequence: SessionSeq,
        prepared: PreparedMutation,
    ) -> Result<ApplyResult, CoreError> {
        let recorder = access::Recorder::disabled(self.state.ledger, self.state.sequence);
        self.apply_recorded(sequence, prepared, &recorder, false)
    }
    /// Retained cloned-map serial oracle for differential verification. Ordinary
    /// apply uses owned row overlays and does not clone the full state.
    pub fn apply_serial(
        &mut self,
        sequence: SessionSeq,
        prepared: PreparedMutation,
    ) -> Result<ApplyResult, CoreError> {
        let recorder = access::Recorder::disabled(self.state.ledger, self.state.sequence);
        self.apply_recorded(sequence, prepared, &recorder, true)
    }
    pub fn apply_tracked(
        &mut self,
        sequence: SessionSeq,
        prepared: PreparedMutation,
        max_entries: usize,
    ) -> TrackedResult<ApplyResult, CoreError> {
        let recorder = access::Recorder::new(self.state.ledger, self.state.sequence, max_entries);
        let result = self.apply_recorded(sequence, prepared, &recorder, false);
        TrackedResult {
            result,
            accesses: recorder.finish(),
        }
    }
    fn apply_recorded(
        &mut self,
        sequence: SessionSeq,
        prepared: PreparedMutation,
        recorder: &access::Recorder,
        serial_oracle: bool,
    ) -> Result<ApplyResult, CoreError> {
        let state = access::ReadState::new(&self.state, recorder);
        let version = execution::Version::from_schema(prepared.schema)?;
        let expected = SessionSeq(
            state
                .sequence()
                .0
                .checked_add(1)
                .ok_or(CoreError::Exhausted)?,
        );
        if sequence != expected {
            return Err(CoreError::Sequence {
                expected,
                actual: sequence,
            });
        }
        if prepared.base != state.sequence() {
            return Err(CoreError::StalePreparation);
        }
        if prepared.input.ledger != state.ledger()
            || version.legacy_hash(&prepared.input)? != prepared.command_hash
        {
            return Err(CoreError::Checksum);
        }
        recorder.read(AccessKey::Limits);
        let mut reference = serial_oracle.then(|| self.state.clone());
        let overlay = if let Some(state) = &mut reference {
            access::WriteState::new(state, recorder)
        } else {
            access::WriteState::overlay(&self.state, &[], sequence, recorder)
        };
        let (mut draft, outcome, deltas, effects) = version
            .execute_on(overlay, &self.limits, &prepared.input, sequence)
            .map_err(CoreError::Determinism)?;
        let key = RequestKey {
            principal: prepared.input.principal,
            epoch: prepared.input.request_epoch,
            id: prepared.input.request_id,
        };
        let receipt = MutationReceipt {
            ledger: state.ledger(),
            key,
            sequence,
            command_hash: prepared.command_hash,
            outcome,
        };
        draft.receipts.insert(key, receipt.clone());
        draft.set_sequence(sequence);
        if let Some(writes) = draft.into_writes() {
            writes.publish(&mut self.state);
            self.state.sequence = sequence;
        } else {
            self.state = reference.ok_or(CoreError::Checksum)?;
        }
        Ok(ApplyResult {
            receipt,
            deltas,
            effects,
        })
    }
    pub fn encode_checkpoint(&self) -> Result<Vec<u8>, CoreError> {
        durable_v1::encode_checkpoint(self)
    }
    pub fn decode_checkpoint(bytes: &[u8]) -> Result<Self, CoreError> {
        durable_v1::decode_checkpoint(bytes)
    }
    pub fn normalized_bytes(&self) -> Result<Vec<u8>, CoreError> {
        Ok(postcard::to_allocvec(&self.state)?)
    }
}

/// Cloned pending-state oracle retained only for differential unit tests.
#[cfg(test)]
#[derive(Debug, Clone)]
struct EffectiveCore {
    committed: Core,
    effective: Core,
    pending: VecDeque<PreparedMutation>,
}
#[cfg(test)]
impl EffectiveCore {
    pub fn new(core: Core) -> Self {
        Self {
            committed: core.clone(),
            effective: core,
            pending: VecDeque::new(),
        }
    }
    pub fn prepare(
        &mut self,
        input: &AuthenticatedInput,
    ) -> Result<PreparedMutation, DomainOutcome> {
        let prepared = self.effective.prepare(input)?;
        let seq = SessionSeq(
            self.effective
                .sequence()
                .0
                .checked_add(1)
                .ok_or_else(|| refuse(ErrorCode::Capacity, "session sequence exhausted"))?,
        );
        self.effective
            .apply(seq, prepared.clone())
            .map_err(|e| refuse(ErrorCode::InvalidTransition, e.to_string()))?;
        self.pending.push_back(prepared.clone());
        Ok(prepared)
    }
    pub fn commit_next(&mut self) -> Result<Option<ApplyResult>, CoreError> {
        let Some(first) = self.pending.front().cloned() else {
            return Ok(None);
        };
        let result = self.committed.apply(
            SessionSeq(
                self.committed
                    .sequence()
                    .0
                    .checked_add(1)
                    .ok_or(CoreError::Exhausted)?,
            ),
            first,
        )?;
        self.pending.pop_front();
        Ok(Some(result))
    }
    pub fn discard_pending(&mut self) {
        self.pending.clear();
        self.effective = self.committed.clone()
    }
    pub fn committed(&self) -> &Core {
        &self.committed
    }
    pub fn effective(&self) -> &Core {
        &self.effective
    }
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}
pub(crate) fn refuse(code: ErrorCode, detail: impl Into<String>) -> DomainOutcome {
    DomainOutcome::refuse(code, detail)
}
