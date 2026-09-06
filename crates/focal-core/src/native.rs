//! Sole owner for native lifecycle rows. This is a typed, in-process boundary;
//! Session activation and a qualified native durable codec remain separate work.
//! There is no native serde implementation, arbitrary row insertion, or second
//! mutable representation alongside the legacy Core.
mod claim_changes;
mod history;
mod intent;
mod owned;
mod prepare;
mod reporting;
mod result_owned;
#[cfg(test)]
mod report_tests;
#[cfg(test)]
mod tests;
mod transactions;
#[cfg(test)]
mod validation_tests;

use crate::{Core, CoreState, state_kind};
use focal_memory::{
    BudgetStats, MemoryBudget, MemoryError, PreparedRange, RangeConfig, RangeId, RangeStats,
    RangeStore, SnapshotLease,
};
use focal_model::lifecycle::{
    Binding, ContractError, Principal,
    aggregation::RegistrationSet,
    claim::ClaimState,
    creation::{EffectiveClaims, Proposal},
    validation,
};
use focal_model::{
    ArtifactId, ClaimId, ClaimStatus, ContentHash, LedgerId, RequestKey, SessionSeq, TestamentId,
    ValidationId,
};
use history::StoredEvent;
use owned::{OwnedClaim, OwnedDeclaration, OwnedEvaluation, OwnedEvent};
pub use result_owned::{NativeAccepted, NativeArtifact, NativeArtifactInput};
use result_owned::{OwnedAccepted, OwnedArtifact};

/// Internal resource limits. Deployment profiles derive these from the node's
/// allowance; they are not another set of mandatory end-user configuration.
#[derive(Debug, Clone, Copy)]
pub struct NativeLimits {
    pub range: RangeConfig,
    pub pending: usize,
    pub plan_nodes: usize,
    pub plan_edges: usize,
    pub preparation_bytes: usize,
    pub claims: usize,
    pub outcomes: usize,
    pub definitions: usize,
    pub evaluations: usize,
    pub evaluations_per_claim: usize,
    pub artifacts: usize,
    pub results: usize,
}
impl Default for NativeLimits {
    fn default() -> Self {
        Self {
            range: RangeConfig::default(),
            pending: 32,
            plan_nodes: 256,
            plan_edges: 4096,
            preparation_bytes: 4 * 1024 * 1024,
            claims: 1_000_000,
            outcomes: 1_000_000,
            definitions: 4_000_000,
            evaluations: 8_000_000,
            evaluations_per_claim: 4096,
            artifacts: 8_000_000,
            results: 8_000_000,
        }
    }
}

pub struct NativeState {
    ledger: LedgerId,
    rows: RangeStore<Key, Row>,
    budget: MemoryBudget,
}
impl std::fmt::Debug for NativeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeState")
            .field("ledger", &self.ledger)
            .field("range", &self.rows.id())
            .field("prefix", &self.rows.prefix())
            .field("entries", &self.rows.len())
            .finish_non_exhaustive()
    }
}
impl state_kind::Sealed for NativeState {}
impl CoreState for NativeState {
    type Limits = NativeLimits;
}

#[derive(Debug)]
pub struct NativeInput {
    pub request: RequestKey,
    pub command: NativeCommand,
}
/// Supplied by the trusted publishing owner, never decoded from participant
/// intent. Logical time advances monotonically at the effective ledger prefix.
#[derive(Debug, Clone, Copy)]
pub struct NativeContext {
    pub principal: Principal,
    pub logical_time: u64,
}
#[derive(Debug)]
pub enum NativeCommand {
    /// Core assigns every creation position. Definitions must begin at revision
    /// one; their immutable content and acceptance identities remain pinned.
    Create {
        claims: Vec<Proposal>,
        declarations: Vec<validation::Declaration>,
    },
    /// Root-issuer control of the actual stored owned closure, including children
    /// created in an earlier unpublished candidate in the supplied chain.
    Cancel {
        expected: Binding,
    },
    Post {
        expected: Binding,
    },
    BeginAdmission {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
    },
    ReportAdmission {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOperation {
    Create,
    Cancel,
    Post,
    BeginAdmission,
    ReportAdmission,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeOutcome {
    pub ledger: LedgerId,
    pub request: RequestKey,
    pub sequence: SessionSeq,
    pub logical_time: u64,
    pub operation: NativeOperation,
    /// Private native semantic intent identity; not a V1 hash or a wire codec.
    pub intent: ContentHash,
    pub created: u32,
    pub changed: u32,
    pub definitions: u32,
    pub evaluations: u32,
    pub artifacts: u32,
    pub results: u32,
    pub events: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEventKind {
    Created,
    ChildRegistered,
    Superseded,
    Cancelled,
    Posted,
    PostFailed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeEvent {
    pub request: RequestKey,
    pub sequence: SessionSeq,
    pub ordinal: u32,
    pub fact: NativeFact,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeFact {
    Artifact { binding: Binding },
    Accepted { key: NativeResultKey },
    Claim(NativeClaimEvent),
    Definition {
        binding: Binding,
        claim: ClaimId,
        index: u32,
        intent: ContentHash,
    },
    Evaluation {
        kind: NativeEvaluationEventKind,
        key: EvaluationKey,
        before: Option<Binding>,
        after: Binding,
        state: validation::State,
        phase: validation::Phase,
        attempt: Option<validation::Attempt>,
        fence: Option<validation::AuthorityFence>,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEvaluationEventKind {
    Materialized,
    Begun,
    Reported,
    AuthorityFenced,
}
impl NativeEvent {
    pub fn claim_event(self) -> Option<NativeClaimEvent> {
        match self.fact {
            NativeFact::Claim(event) => Some(event),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeClaimEvent {
    pub kind: NativeEventKind,
    pub owned_child: Option<Binding>,
    pub before: Option<Binding>,
    pub after: Binding,
    pub status: ClaimStatus,
}

/// Lookup identity; the retained row additionally pins full target content,
/// revisions and receipt. A definition can have multiple independent targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvaluationKey {
    pub claim: ClaimId,
    pub validation: ValidationId,
    pub target: EvaluationTarget,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EvaluationTarget {
    Admission,
    Increment {
        artifact: ArtifactId,
    },
    Work {
        response: TestamentId,
        slot: u32,
        artifact: ArtifactId,
    },
    MissingSlot {
        response: TestamentId,
        slot: u32,
    },
    Delivery {
        response: TestamentId,
    },
}
impl EvaluationKey {
    fn of(claim: ClaimId, evaluation: &validation::EvaluationState) -> Self {
        Self {
            claim,
            validation: ValidationId(evaluation.binding().object.0),
            target: EvaluationTarget::of(evaluation.target()),
            generation: evaluation.generation(),
        }
    }
}
impl EvaluationTarget {
    fn of(target: validation::Target) -> Self {
        use validation::Target;
        match target {
            Target::Admission { .. } => EvaluationTarget::Admission,
            Target::Increment { artifact, .. } => EvaluationTarget::Increment {
                artifact: ArtifactId(artifact.object.0),
            },
            Target::Artifact {
                response,
                slot,
                artifact,
            } => EvaluationTarget::Work {
                response: TestamentId(response.object.0),
                slot,
                artifact: ArtifactId(artifact.object.0),
            },
            Target::MissingSlot { response, slot } => EvaluationTarget::MissingSlot {
                response: TestamentId(response.object.0),
                slot,
            },
            Target::Delivery { response } => EvaluationTarget::Delivery {
                response: TestamentId(response.object.0),
            },
        }
    }
}

/// Address of an immutable accepted attempt, separate from its mutable evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeResultKey {
    pub evaluation: EvaluationKey,
    pub revision: focal_model::ObjectRevision,
}
impl NativeResultKey {
    pub fn of(result: validation::AcceptedResult) -> Self {
        Self {
            evaluation: EvaluationKey {
                claim: result.claim(), validation: result.validation(),
                target: EvaluationTarget::of(result.target()), generation: result.generation(),
            },
            revision: result.binding().revision,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Meta,
    Claim(ClaimId),
    Definition(ValidationId),
    Evaluation(EvaluationKey),
    Artifact(ArtifactId),
    ArtifactIdentity(ContentHash),
    Accepted(NativeResultKey),
    Outcome(RequestKey),
    Event(SessionSeq, u32),
    End,
}
#[derive(Debug, Default, Clone, Copy)]
struct Meta {
    claims: usize,
    outcomes: usize,
    definitions: usize,
    evaluations: usize,
    artifacts: usize,
    results: usize,
    logical_time: u64,
}
#[derive(Debug)]
enum Row {
    Meta(Meta),
    Claim(OwnedClaim),
    Definition(OwnedDeclaration),
    Evaluation(OwnedEvaluation),
    Artifact(OwnedArtifact),
    ArtifactIdentity(ArtifactId),
    Accepted(OwnedAccepted),
    Outcome(NativeOutcome),
    Event(OwnedEvent),
}

/// All allocated candidate rows, outcomes and events have one immutable root.
/// Dropping a candidate releases its page permits without touching live state.
pub struct NativePrepared {
    range: PreparedRange<Key, Row>,
    outcome: NativeOutcome,
}
impl std::fmt::Debug for NativePrepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePrepared")
            .field("base", &self.range.base_prefix())
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}
impl NativePrepared {
    pub fn outcome(&self) -> NativeOutcome {
        self.outcome
    }
    pub fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.range.get(&Key::Claim(id)))
    }
    pub fn artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.range.get(&Key::Artifact(id)))
    }
    pub fn result(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.range.get(&Key::Accepted(key)))
    }
    pub fn recorded(&self, key: RequestKey) -> Option<NativeOutcome> {
        as_outcome(self.range.get(&Key::Outcome(key)))
    }
    pub fn definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.range.get(&Key::Definition(id)))
    }
    pub fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.range.get(&Key::Evaluation(key)))
    }
}
#[derive(Debug)]
pub enum NativePreparation {
    Prepared(NativePrepared),
    /// A pending match must await the original candidate's durability barrier.
    Existing {
        outcome: NativeOutcome,
        committed: bool,
    },
}
#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("native lifecycle admission: {0}")]
    Contract(#[from] ContractError),
    #[error("native owner memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("native evidence: {0}")]
    Evidence(#[from] focal_evidence::NativeEvidenceError),
    #[error("request key was already used for a different native intent")]
    RequestConflict,
    #[error("native owner bound exceeded: {0}")]
    Capacity(&'static str),
}
/// Publication refusal retains ownership of the entire prepared candidate.
#[derive(Debug)]
pub struct NativePublishError {
    pub error: MemoryError,
    pub prepared: NativePrepared,
}
impl std::fmt::Display for NativePublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for NativePublishError {}

/// Expiring capability to a fixed native prefix. Borrowed rows cannot escape a
/// projection call. No Clone implementation is needed on native claim rows.
#[derive(Debug)]
pub struct NativeRead {
    lease: SnapshotLease<Key, Row>,
}
impl NativeRead {
    pub fn sequence(&self) -> SessionSeq {
        SessionSeq(self.lease.prefix())
    }
    pub fn with_claim<T>(
        &self,
        id: ClaimId,
        now: u64,
        project: impl FnOnce(&ClaimState) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Claim(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_claim(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn recorded(
        &self,
        request: RequestKey,
        now: u64,
    ) -> Result<Option<NativeOutcome>, MemoryError> {
        let key = Key::Outcome(request);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                (entry.key == key)
                    .then(|| as_outcome(Some(&entry.value)))
                    .flatten()
            })
            .map(Option::flatten)
    }
    pub fn with_definition<T>(
        &self,
        id: ValidationId,
        now: u64,
        project: impl FnOnce(&validation::Declaration) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Definition(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_definition(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_evaluation<T>(
        &self,
        id: EvaluationKey,
        now: u64,
        project: impl FnOnce(&validation::EvaluationState) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Evaluation(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_evaluation(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

impl Core<NativeState> {
    pub fn new_native(
        ledger: LedgerId,
        range: RangeId,
        limits: NativeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeError> {
        if ledger.tenant.is_zero() || ledger.session.is_zero() {
            return Err(ContractError::WrongLedger.into());
        }
        if limits.pending == 0
            || limits.plan_nodes == 0
            || limits.plan_edges == 0
            || limits.preparation_bytes == 0
            || limits.claims == 0
            || limits.outcomes == 0
            || limits.definitions == 0
            || limits.evaluations == 0
            || limits.evaluations_per_claim == 0
            || limits.artifacts == 0
            || limits.results == 0
            || limits.range.max_batch_entries < 4
        {
            return Err(MemoryError::InvalidConfiguration("native limits must be nonzero").into());
        }
        let rows = RangeStore::new(range, 0, limits.range, budget.clone())?;
        Ok(Self {
            state: NativeState {
                ledger,
                rows,
                budget,
            },
            limits,
        })
    }
    pub fn native_sequence(&self) -> SessionSeq {
        SessionSeq(self.state.rows.prefix())
    }
    pub fn native_claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.state.rows.get(&Key::Claim(id)))
    }
    pub fn native_outcome(&self, request: RequestKey) -> Option<NativeOutcome> {
        as_outcome(self.state.rows.get(&Key::Outcome(request)))
    }
    pub fn native_definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.state.rows.get(&Key::Definition(id)))
    }
    pub fn native_evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.state.rows.get(&Key::Evaluation(key)))
    }
    pub fn native_artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.state.rows.get(&Key::Artifact(id)))
    }
    pub fn native_result(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.state.rows.get(&Key::Accepted(key)))
    }
    pub fn native_event(&self, sequence: SessionSeq, ordinal: u32) -> Option<NativeEvent> {
        match self.state.rows.get(&Key::Event(sequence, ordinal)) {
            Some(Row::Event(event)) => event.get().map(|row| row.expand(self.state.ledger)),
            _ => None,
        }
    }
    pub fn native_budget(&self) -> BudgetStats {
        self.state.budget.stats()
    }
    pub fn native_stats(&self) -> RangeStats {
        self.state.rows.stats()
    }
    pub fn validate_native_chain(&self, pending: &[&NativePrepared]) -> Result<(), NativeError> {
        if pending.len() > self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        self.state
            .rows
            .validate_chain(pending.iter().map(|item| &item.range))?;
        Ok(())
    }
    /// The external log owner calls this only after durability. This method has
    /// no codec or IO; it performs no allocation, including producing its result.
    #[allow(clippy::result_large_err)] // Return the owned candidate without allocating on refusal.
    pub fn publish_native(
        &mut self,
        prepared: NativePrepared,
    ) -> Result<NativeOutcome, NativePublishError> {
        let NativePrepared { range, outcome } = prepared;
        self.state
            .rows
            .publish_recoverable(range)
            .map_err(|(error, range)| NativePublishError {
                error,
                prepared: NativePrepared { range, outcome },
            })?;
        Ok(outcome)
    }
    pub fn pin_native(&mut self, now: u64, ttl: u64) -> Result<NativeRead, MemoryError> {
        self.state
            .rows
            .pin(now, ttl)
            .map(|lease| NativeRead { lease })
    }
    pub fn release_native(&mut self, read: &NativeRead) -> Result<(), MemoryError> {
        self.state.rows.release(&read.lease)
    }
    pub fn advance_native_clock(&mut self, now: u64) -> Result<usize, MemoryError> {
        self.state.rows.advance_clock(now)
    }
}

fn as_claim(row: Option<&Row>) -> Option<&ClaimState> {
    match row {
        Some(Row::Claim(claim)) => claim.claim(),
        _ => None,
    }
}
fn as_outcome(row: Option<&Row>) -> Option<NativeOutcome> {
    match row {
        Some(Row::Outcome(outcome)) => Some(*outcome),
        _ => None,
    }
}
fn as_definition(row: Option<&Row>) -> Option<&validation::Declaration> {
    match row {
        Some(Row::Definition(value)) => value.get(),
        _ => None,
    }
}
fn as_evaluation(row: Option<&Row>) -> Option<&validation::EvaluationState> {
    match row {
        Some(Row::Evaluation(value)) => value.get(),
        _ => None,
    }
}
struct View<'a> {
    state: &'a NativeState,
    tail: Option<&'a NativePrepared>,
}
impl View<'_> {
    fn get(&self, key: Key) -> Option<&Row> {
        match self.tail {
            Some(tail) => tail.range.get(&key),
            None => self.state.rows.get(&key),
        }
    }
    fn meta(&self) -> Meta {
        match self.get(Key::Meta) {
            Some(Row::Meta(meta)) => *meta,
            _ => Meta::default(),
        }
    }
    fn owned_claim(&self, id: ClaimId) -> Result<&OwnedClaim, NativeError> {
        match self.get(Key::Claim(id)) {
            Some(Row::Claim(row)) => Ok(row),
            _ => Err(ContractError::InvalidTarget.into()),
        }
    }
    fn definition(&self, id: ValidationId) -> Result<&validation::Declaration, NativeError> {
        as_definition(self.get(Key::Definition(id))).ok_or(ContractError::InvalidPolicy.into())
    }
    fn evaluation(&self, key: EvaluationKey) -> Result<&validation::EvaluationState, NativeError> {
        as_evaluation(self.get(Key::Evaluation(key))).ok_or(ContractError::InvalidTarget.into())
    }
}
impl EffectiveClaims for View<'_> {
    fn ledger(&self) -> LedgerId {
        self.state.ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.tail
            .map_or(SessionSeq(self.state.rows.prefix()), |tail| {
                SessionSeq(tail.range.prefix())
            })
    }
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.get(Key::Claim(id)))
    }
}

fn as_artifact(row: Option<&Row>) -> Option<&NativeArtifact> {
    match row { Some(Row::Artifact(value)) => value.get(), _ => None }
}
fn as_result(row: Option<&Row>) -> Option<&NativeAccepted> {
    match row { Some(Row::Accepted(value)) => value.get(), _ => None }
}
impl NativeRead {
    pub fn with_artifact<T>(&self, id: ArtifactId, now: u64,
        project: impl FnOnce(&NativeArtifact) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Artifact(id);
        self.lease.project_next(&key, false, &Key::End, now, |entry| {
            if entry.key == key { as_artifact(Some(&entry.value)).map(project) } else { None }
        }).map(Option::flatten)
    }
    pub fn with_result<T>(&self, id: NativeResultKey, now: u64,
        project: impl FnOnce(&NativeAccepted) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Accepted(id);
        self.lease.project_next(&key, false, &Key::End, now, |entry| {
            if entry.key == key { as_result(Some(&entry.value)).map(project) } else { None }
        }).map(Option::flatten)
    }
}
