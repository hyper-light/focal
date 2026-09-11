//! Sole owner for native lifecycle rows. This is a typed, in-process boundary;
//! Session activation and a qualified native durable codec remain separate work.
//! There is no native serde implementation, arbitrary row insertion, or second
//! mutable representation alongside the legacy Core.
mod admission_authority;
#[cfg(test)]
mod admission_authority_tests;
mod admission_budget;
mod admission_graph;
mod admission_view;
mod adoption;
mod artifact_intent;
mod audit;
mod audit_bundle;
mod authored;
mod authored_reads;
mod claim_changes;
mod claim_deadlines;
mod cohort_budget;
mod cohort_seals;
mod completion_book;
mod completion_envelope;
mod completion_index;
mod completion_schemas;
mod control_graph;
mod creation_result;
mod deadlines;
mod delivery;
mod delivery_owned;
#[cfg(test)]
mod event_budget_tests;
pub mod event_record;
#[cfg(any(test, feature = "test-support"))]
pub mod fixtures;
#[cfg(test)]
mod funding_tests;
mod graph_effects;
mod history;
mod import;
mod incoming_graph;
mod increment_authority;
mod increment_seal;
mod increments;
mod index_rows;
mod index_scan;
pub mod input_codec;
mod intent;
mod layout;
pub use layout::NativeLocation;
#[cfg(test)]
mod layout_tests;
mod missing_owned;
mod missing_results;
mod monitor_commands;
mod monitor_deadlines;
mod monitor_index;
mod mutation;
mod object_journal;
mod owned;
mod owner;
#[cfg(test)]
mod owner_record_buffer_tests;
mod prepare;
mod prepare_budget;
mod projection;
mod projection_quote;
mod projection_visits;
mod projection_work;
pub mod ranges;
mod receipt;
#[cfg(test)]
mod receipt_tests;
pub mod record_codec;
mod report_artifact;
#[cfg(test)]
mod report_tests;
mod reporting;
mod respondent_envelope;
mod respondent_state;
mod response_budget;
mod response_input;
mod response_owned;
mod response_reads;
#[cfg(test)]
mod response_tests;
mod responses;
mod result_owned;
mod retired_cycles;
pub mod retirement;
#[cfg(test)]
#[path = "native/retirement_tests.rs"]
mod retirement_tests;
mod scope_release;
#[cfg(test)]
mod scope_release_tests;
#[cfg(test)]
mod tests;
mod transactions;
#[cfg(test)]
mod validation_tests;
mod whole_work;
mod work_artifacts;
mod work_authority;
mod work_checks;
mod work_failures;
mod work_owned;
mod work_reporting;

use crate::{Core, CoreState, state_kind};
pub use audit::{NativeAudit, NativeAuditPublication};
pub use audit_bundle::NativeResultTestament;
use audit_bundle::OwnedResultTestament;
pub use authored::{NativeAuthoredProposal, NativeContentProfile};
use creation_result::OwnedCreationResult;
pub use creation_result::{NativeCreatedFamily, NativeCreatedObject, NativeCreationResult};
pub use delivery_owned::NativeDeliveryResult;
use delivery_owned::OwnedDeliveryResult;
pub use event_record::delta as native_delta;
use focal_memory::{
    BudgetStats, MemoryBudget, MemoryError, RangeConfig, RangeId, RangeStats, RangeStore,
};
use focal_model::lifecycle::evidence::{
    self, EvidenceFailure, Response, ResponseState, WorkArtifactState,
};
use focal_model::lifecycle::{
    Binding, ContractError, Principal,
    aggregation::RegistrationSet,
    claim::{ClaimState, ReceiptEntitlement},
    creation::{EffectiveClaims, Proposal},
    scope, validation,
};
use focal_model::{
    ArtifactId, ClaimId, ClaimStatus, ContentHash, Deadline, LedgerId, MonitorId, ParticipantId,
    ReceiptFence, ReceiptId, RequestEpoch, RequestId, RequestKey, SessionSeq, TestamentId, TimerId,
    ValidationId, WaitPredicate,
};
use history::StoredEvent;
pub use import::{ImportError, ImportRequest, Imported, InlinePayload, import, inline_payloads};
pub use index_rows::{artifact_kind_hash, scope_key_hash};
pub use index_scan::{NativeIndexHit, NativeIndexScan};
pub use missing_owned::NativeMissingResult;
use missing_owned::OwnedMissingResult;
use owned::{
    OwnedClaim, OwnedClaimContent, OwnedDeclaration, OwnedEvaluation, OwnedEvent, OwnedLegacy,
};
pub use owner::{
    NativeCandidate, NativeOwner, NativeOwnerError, NativeOwnerInitError, NativeOwnerIntoCoreError,
    NativeStaging, NativeView,
};
pub use projection_quote::NativeProjectionQuote;
pub use response_input::{
    NativeMonitorSource, NativeMonitorSourcePlan, NativeResponseInput, NativeResponsePlan,
    NativeResponseSource, NativeResponseSourcePlan, NativeResponseSpec, NativeSourceQuote,
};
pub use response_owned::NativeResponseRecord;
use response_owned::OwnedResponse;
use response_reads::{as_diagnostic, as_response, as_work};
pub use result_owned::{NativeAccepted, NativeArtifact, NativeArtifactInput};
use result_owned::{OwnedAccepted, OwnedArtifact};
pub use work_owned::{NativeDiagnostic, NativeWork};
use work_owned::{OwnedDiagnostic, OwnedWork};

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
    pub events: usize,
    pub definitions: usize,
    pub evaluations: usize,
    pub evaluations_per_claim: usize,
    pub artifacts: usize,
    pub results: usize,
    pub receipts: usize,
    pub responses: usize,
    pub monitors: usize,
    pub monitor_links: usize,
    pub work_artifacts_per_cycle: usize,
    pub diagnostics_per_cycle: usize,
    pub response_summary_bytes: usize,
    /// Inputs one artifact may cite, and so the `ArtifactInput` index rows its
    /// admission writes (22 §7); never above the model's fixed ceiling.
    pub artifact_inputs: usize,
    /// Frozen legacy rows an imported ledger may retain (23 §5.2).
    pub legacy_rows: usize,
    pub legacy_row_bytes: usize,
    /// Members a range group may have (25 §4): every write envelope is
    /// derived for this many, so splits never invalidate a promise.
    pub max_ranges: usize,
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
            events: 16_000_000,
            definitions: 4_000_000,
            evaluations: 8_000_000,
            evaluations_per_claim: 4096,
            artifacts: 8_000_000,
            results: 8_000_000,
            receipts: 8_000_000,
            responses: 4_000_000,
            monitors: 4_000_000,
            monitor_links: 16_000_000,
            work_artifacts_per_cycle: 256,
            diagnostics_per_cycle: 64,
            response_summary_bytes: 64 * 1024,
            artifact_inputs: 16,
            legacy_rows: 4_000_000,
            legacy_row_bytes: 1024 * 1024,
            max_ranges: 64,
        }
    }
}

pub struct NativeState {
    ledger: LedgerId,
    profile: NativeContentProfile,
    rows: ranges::NativeRanges,
    budget: MemoryBudget,
}
impl std::fmt::Debug for NativeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeState")
            .field("ledger", &self.ledger)
            .field("profile", &self.profile)
            .field("range", &self.rows.id())
            .field("members", &self.rows.members())
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
/// Trusted timer delivery. The owner resolves the current evaluation revision;
/// this is deliberately separate from participant-authored commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeDeadlineInput {
    pub evaluation: EvaluationKey,
    pub deadline: Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeDeadlineKey {
    pub evaluation: EvaluationKey,
    pub timer: TimerId,
    pub generation: u64,
}
impl NativeDeadlineInput {
    pub fn key(self) -> NativeDeadlineKey {
        NativeDeadlineKey {
            evaluation: self.evaluation,
            timer: self.deadline.timer,
            generation: self.deadline.generation,
        }
    }
}
/// Trusted delivery of the claim's own authored deadline. The effective owner
/// resolves its current revision and checks deadlock precedence before expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeClaimDeadlineInput {
    pub claim: ClaimId,
    pub deadline: Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeClaimDeadlineKey {
    pub claim: ClaimId,
    pub timer: TimerId,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeMonitorDeadlineInput {
    pub claim: ClaimId,
    pub monitor: MonitorId,
    pub deadline: Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeMonitorDeadlineKey {
    pub claim: ClaimId,
    pub monitor: MonitorId,
    pub timer: TimerId,
    pub generation: u64,
}
impl NativeMonitorDeadlineInput {
    pub fn key(self) -> NativeMonitorDeadlineKey {
        NativeMonitorDeadlineKey {
            claim: self.claim,
            monitor: self.monitor,
            timer: self.deadline.timer,
            generation: self.deadline.generation,
        }
    }
}
impl NativeClaimDeadlineInput {
    pub fn key(self) -> NativeClaimDeadlineKey {
        NativeClaimDeadlineKey {
            claim: self.claim,
            timer: self.deadline.timer,
            generation: self.deadline.generation,
        }
    }
}
/// Disjoint exact-invocation namespaces. A timer can never impersonate an
/// actor request or consume its retry identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeInvocation {
    Request(RequestKey),
    EvaluationDeadline(NativeDeadlineKey),
    ClaimDeadline(NativeClaimDeadlineKey),
    MonitorDeadline(NativeMonitorDeadlineKey),
    /// The one-time translation of a sealed legacy prefix (23 §5). Never a
    /// participant request and never a timer; it owns native sequence one.
    Import,
    /// A committed retirement (26 §4) that moved the family rooted at the
    /// named claim to the archive; a session decision, never a request or
    /// a timer, owning the native sequence its publication consumed.
    Retirement(ClaimId),
}
/// Custody request identity of an imported artifact (23 §5.2): derived from
/// the ledger and artifact under a private domain, principal = producer, epoch
/// one. It is never minted by a participant and never consumes a retry slot.
pub fn import_request(
    ledger: LedgerId,
    artifact: ArtifactId,
    producer: ParticipantId,
) -> RequestKey {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.import.request.v1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&artifact.0);
    let mut id = [0u8; 16];
    hash.finalize_xof().fill(&mut id);
    RequestKey {
        principal: producer,
        epoch: RequestEpoch(1),
        id: RequestId(id),
    }
}
impl From<RequestKey> for NativeInvocation {
    fn from(request: RequestKey) -> Self {
        Self::Request(request)
    }
}
impl From<NativeDeadlineKey> for NativeInvocation {
    fn from(deadline: NativeDeadlineKey) -> Self {
        Self::EvaluationDeadline(deadline)
    }
}
impl From<NativeClaimDeadlineKey> for NativeInvocation {
    fn from(deadline: NativeClaimDeadlineKey) -> Self {
        Self::ClaimDeadline(deadline)
    }
}
impl From<NativeMonitorDeadlineKey> for NativeInvocation {
    fn from(deadline: NativeMonitorDeadlineKey) -> Self {
        Self::MonitorDeadline(deadline)
    }
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
    RegisterMonitor {
        expected: Binding,
        receipt: Option<ReceiptFence>,
        id: MonitorId,
        roots: Vec<WaitPredicate>,
        deadline: Deadline,
    },
    RebindMonitor {
        expected: Binding,
        receipt: Option<ReceiptFence>,
        id: MonitorId,
        predecessor: Binding,
        successor: Binding,
    },
    CancelMonitor {
        expected: Binding,
        receipt: Option<ReceiptFence>,
        id: MonitorId,
    },
    /// Release a terminal claim's owned scope after its children are released.
    /// This does not terminalize children or invent evaluation results.
    ReleaseScope {
        expected: Binding,
    },
    GenerateResultTestament {
        claim: Binding,
        id: TestamentId,
    },
    PostResultTestament {
        expected: Binding,
    },
    /// Explicit claimant request to assess the exact received response.
    /// External check execution remains the evaluator's responsibility.
    EnterWholeWork {
        claim: Binding,
        expected: Binding,
    },
    /// Claimant freezes Increment target membership before WholeWork entry.
    /// Existing evaluations retain their independent completion authority.
    SealIncrementTargets {
        claim: Binding,
    },
    FailWorkProduction {
        claim: Binding,
        slot: u32,
        diagnostic: focal_model::ArtifactRef,
    },
    RejectWork {
        claim: Binding,
        expected: Binding,
        reason: EvidenceFailure,
        artifact: NativeArtifactInput,
    },
    SubmitWork {
        claim: Binding,
        slot: u32,
        artifact: NativeArtifactInput,
    },
    SubmitDiagnostic {
        claim: Binding,
        reason: EvidenceFailure,
        artifact: NativeArtifactInput,
    },
    ReceiveWork {
        claim: Binding,
        expected: Binding,
    },
    CloseResponse {
        claim: Binding,
        response: Binding,
        report: NativeResponseInput,
    },
    PostResponse {
        claim: Binding,
        expected: Binding,
    },
    ReceiveResponse {
        claim: Binding,
        expected: Binding,
    },
    /// First responsibility receipt. The owner assigns epoch one and retains the
    /// unique ID allocation even after later authority changes.
    AcquireReceipt {
        expected: Binding,
        receipt: ReceiptId,
    },
    /// The claimant replaces current responsibility. The owner assigns the next
    /// receipt epoch and fences old evaluations without manufacturing testimony.
    AdoptReceipt {
        expected: Binding,
        previous: ReceiptFence,
        receipt: ReceiptId,
        holder: ParticipantId,
    },
    /// Core assigns every creation position. Definitions must begin at revision
    /// one; their immutable content and acceptance identities remain pinned.
    Create {
        claims: Vec<Proposal>,
        declarations: Vec<validation::Declaration>,
    },
    /// Complete authored bodies, grouped by their actual parent. The owner
    /// derives lifecycle projections and assigns the creation cut.
    CreateAuthored {
        claims: Vec<NativeAuthoredProposal>,
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
    BeginIncrement {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
    },
    ReportIncrement {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
    ReportAdmission {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
    /// The designated evaluator begins one exact received-work check. The first
    /// begin enters its response using the actual evaluator capability.
    BeginWork {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
    },
    ReportWork {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOperation {
    RegisterMonitor,
    RebindMonitor,
    CancelMonitor,
    MonitorDeadline,
    ReleaseScope,
    GenerateResultTestament,
    PostResultTestament,
    EnterWholeWork,
    SealIncrementTargets,
    BeginIncrement,
    ReportIncrement,
    FailWorkProduction,
    RejectWork,
    SubmitWork,
    SubmitDiagnostic,
    ReceiveWork,
    CloseResponse,
    PostResponse,
    ReceiveResponse,
    AcquireReceipt,
    AdoptReceipt,
    Create,
    Cancel,
    Post,
    BeginAdmission,
    ReportAdmission,
    BeginWork,
    ReportWork,
    EvaluationDeadline,
    ClaimDeadline,
    Import,
    /// A family of claims retired to the archive (26 §4).
    Retire,
}
/// Address of one frozen legacy row retained by import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeLegacyKey {
    Testament(TestamentId),
    EvidenceSet(focal_model::EvidenceSetId),
    Run { validation: ValidationId, run: u32 },
    Definition(ValidationId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeOutcome {
    pub ledger: LedgerId,
    pub invocation: NativeInvocation,
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
    pub receipts: u32,
    pub responses: u32,
    pub result_testaments: u32,
    pub events: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEventKind {
    Monitor(NativeMonitorEvent),
    OwnerReleased,
    Validating,
    LocallyComplete,
    ValidationIncomplete,
    ValidationFailed,
    ValidationErrored,
    DependencyFailed,
    Created,
    ChildRegistered,
    Superseded,
    Cancelled,
    Posted,
    PostFailed,
    Received,
    ReceiptAdopted,
    Satisfied,
    TestamentGenerated,
    TestamentAcknowledged,
    ResponseObserved,
    Expired,
    Deadlocked,
    /// A legacy status fact carried by import; the value is the legacy
    /// sequence at which the legacy engine recorded it (23 §5.2).
    Imported(SessionSeq),
}
/// Ledger-local monitor facts carry their exact model cuts without duplicating
/// owner bindings. Each is one ordinary claim revision in the same journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeMonitorEvent {
    Registered {
        id: MonitorId,
        cut: focal_model::lifecycle::claim::ClaimCut,
    },
    Rebound {
        id: MonitorId,
        change: scope::Rebinding,
    },
    Released {
        id: MonitorId,
        cut: focal_model::lifecycle::claim::ClaimCut,
    },
    Cancelled {
        id: MonitorId,
        cancellation: scope::MonitorCancellation,
    },
}
impl NativeMonitorEvent {
    pub fn from_scope(event: scope::Event) -> Result<Self, ContractError> {
        match event {
            scope::Event::Registered { id, cut } => Ok(Self::Registered { id, cut }),
            scope::Event::Rebound { id, change } => Ok(Self::Rebound { id, change }),
            scope::Event::MonitorReleased { id, cut } => Ok(Self::Released { id, cut }),
            scope::Event::MonitorCancelled { id, cancellation } => {
                Ok(Self::Cancelled { id, cancellation })
            }
            _ => Err(ContractError::InvalidTransition),
        }
    }
    pub fn into_scope(self) -> scope::Event {
        match self {
            Self::Registered { id, cut } => scope::Event::Registered { id, cut },
            Self::Rebound { id, change } => scope::Event::Rebound { id, change },
            Self::Released { id, cut } => scope::Event::MonitorReleased { id, cut },
            Self::Cancelled { id, cancellation } => {
                scope::Event::MonitorCancelled { id, cancellation }
            }
        }
    }
    pub fn id(self) -> MonitorId {
        match self {
            Self::Registered { id, .. }
            | Self::Rebound { id, .. }
            | Self::Released { id, .. }
            | Self::Cancelled { id, .. } => id,
        }
    }
    pub fn cut(self) -> focal_model::lifecycle::claim::ClaimCut {
        match self {
            Self::Registered { cut, .. } | Self::Released { cut, .. } => cut,
            Self::Rebound { change, .. } => change.cut,
            Self::Cancelled { cancellation, .. } => cancellation.cut,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeEvent {
    pub invocation: NativeInvocation,
    pub sequence: SessionSeq,
    pub ordinal: u32,
    pub fact: NativeFact,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeFact {
    ResultTestament {
        claim: ClaimId,
        before: Option<Binding>,
        after: Binding,
        state: focal_model::lifecycle::audit::ResultTestamentState,
    },
    Missing {
        key: NativeResultKey,
    },
    Registrations {
        claim: Binding,
    },
    Delivery {
        key: NativeResultKey,
    },
    Work {
        claim: ClaimId,
        before: Option<Binding>,
        after: Binding,
        state: WorkArtifactState,
    },
    Diagnostic {
        claim: ClaimId,
        binding: Binding,
        reason: EvidenceFailure,
    },
    Response {
        claim: ClaimId,
        before: Option<Binding>,
        after: Binding,
        state: ResponseState,
    },
    Receipt {
        claim: Binding,
        fence: ReceiptFence,
        holder: ParticipantId,
    },
    ReceiptAdopted {
        claim: Binding,
        previous: ReceiptEntitlement,
        replacement: ReceiptEntitlement,
        cause: ContentHash,
    },
    Artifact {
        binding: Binding,
    },
    Accepted {
        key: NativeResultKey,
    },
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
    MissingTarget,
    Materialized,
    Begun,
    Reported,
    AuthorityFenced,
    Sealed,
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
pub struct NativeGraphCapture {
    /// Exclusive current-journal boundary of the actual graph snapshot. A
    /// batched reduction retains one shared boundary; recapture advances it.
    pub before_ordinal: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeClaimEvent {
    pub kind: NativeEventKind,
    pub graph: Option<NativeGraphCapture>,
    pub owned_child: Option<Binding>,
    pub before: Option<Binding>,
    pub after: Binding,
    pub status: ClaimStatus,
}
impl NativeClaimEvent {
    pub(super) fn check_graph_capture(self, ordinal: u32) -> Result<(), ContractError> {
        let required = matches!(
            self.kind,
            NativeEventKind::DependencyFailed
                | NativeEventKind::Deadlocked
                | NativeEventKind::Satisfied
                | NativeEventKind::Expired
        );
        if self.graph.is_some() != required
            || self
                .graph
                .is_some_and(|capture| capture.before_ordinal > ordinal)
        {
            return Err(ContractError::InvalidCut);
        }
        Ok(())
    }
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
    /// The lookup identity of a pinned evaluation target.
    pub fn of(target: validation::Target) -> Self {
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

/// Immutable allocation of an execution receipt. This row survives later
/// receipt control so an old identity can never be recycled for another claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReceipt {
    pub claim: ClaimId,
    pub fence: ReceiptFence,
    pub holder: ParticipantId,
    pub acquired: SessionSeq,
}

/// Exact owner-resolved responsibility and work cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct NativeCycleKey {
    pub claim: ClaimId,
    pub receipt: ReceiptId,
    pub epoch: u64,
    pub cycle: u32,
}
impl NativeCycleKey {
    pub(super) fn of(parent: &evidence::Parent) -> Self {
        Self {
            claim: parent.claim,
            receipt: parent.receipt.receipt,
            epoch: parent.receipt.epoch,
            cycle: parent.next_cycle,
        }
    }
}
/// Owner-maintained membership; every entry is retained under this exact cycle.
/// Appending evidence edits one head and row, without copying a growing list.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct NativeCycle {
    pub work_head: Option<ArtifactId>,
    pub work_count: usize,
    pub diagnostic_head: Option<ArtifactId>,
    pub diagnostic_count: usize,
    pub response: Option<TestamentId>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RetiredCycleHead {
    pub head: Option<NativeCycleKey>,
    pub count: usize,
    pub work_count: usize,
}
#[derive(Debug, Clone, Copy)]
pub(super) struct RetiredCycle {
    pub holder: ParticipantId,
    pub next: Option<NativeCycleKey>,
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
                claim: result.claim(),
                validation: result.validation(),
                target: EvaluationTarget::of(result.target()),
                generation: result.generation(),
            },
            revision: result.binding().revision,
        }
    }
}

/// Ordered by the storage layout ([`layout`]): affinity, family, fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    IncomingHead(ClaimId),
    IncomingLink(ClaimId, ClaimId),
    Monitor(focal_model::MonitorId),
    MonitorHead(ClaimId),
    MonitorLink(ClaimId, focal_model::MonitorId),
    MissingResult(NativeResultKey),
    Meta,
    Claim(ClaimId),
    Definition(ValidationId),
    Evaluation(EvaluationKey),
    Artifact(ArtifactId),
    ArtifactIdentity(ContentHash),
    Accepted(NativeResultKey),
    DeliveryResult(NativeResultKey),
    Receipt(ReceiptId),
    Cycle(NativeCycleKey),
    RetiredCycleHead(ClaimId),
    RetiredCycle(NativeCycleKey),
    Work(ArtifactId),
    WorkSlot(NativeCycleKey, u32),
    Diagnostic(ArtifactId),
    Response(TestamentId),
    ResultTestament(TestamentId),
    ClaimResultTestament(ClaimId),
    Outcome(NativeInvocation),
    Event(SessionSeq, u32),
    ClaimContent(ClaimId),
    ClaimIdentity(u16, ContentHash),
    DefinitionIdentity(u16, ContentHash),
    CreationResult(NativeInvocation),
    LegacyTestament(TestamentId),
    LegacyEvidenceSet(focal_model::EvidenceSetId),
    LegacyRun(ValidationId, u32),
    LegacyDefinition(ValidationId),
    /// Secondary index families (doc 22 §7). Unit rows derived from exactly
    /// one primary row; replay and recovery check them against it.
    ByIssuer(ParticipantId, ClaimId),
    BySubject(ParticipantId, ClaimId),
    ByStatus(u16, ClaimId),
    ByAction(u16, ClaimId),
    ByScope(u16, ContentHash, ClaimId),
    ByRelation(u16, ClaimId, ClaimId),
    ByProducer(ParticipantId, ArtifactId),
    ByArtifactKind(ContentHash, ArtifactId),
    BySchema(ContentHash, ArtifactId),
    ArtifactInput(focal_model::ObjectId, ArtifactId),
    ByEvaluator(ParticipantId, ValidationId),
    ByVerdict(u16, NativeResultKey),
    ByCreated(u16, SessionSeq, focal_model::ObjectId),
    /// A trusted timer that is due at the logical time and has not been
    /// consumed: the claim's deadline, an evaluation's declaration deadline
    /// or a monitor's deadline (22 §7).
    DueTimer(u64, TimerTarget),
    /// The identity index of a primary object family (22 §7): one unit row
    /// per claim, artifact or declaration under its family's bucket, so a
    /// listing in identity order is one contiguous scan although each
    /// object's own rows sit under the object (25 §3).
    ByObject(u16, focal_model::ObjectId),
    /// The typed continuation of a claim retired to the archive (26 §4):
    /// written where the claim's rows were, so a reference resolves to the
    /// bundle that holds them rather than to nothing.
    Retired(ClaimId),
    End,
}

/// The primary row a due timer belongs to. Its deadline identity (timer and
/// generation) is read from that row when the timer is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimerTarget {
    Claim(ClaimId),
    Evaluation(EvaluationKey),
    Monitor(ClaimId, MonitorId),
}
impl TimerTarget {
    /// The invocation delivering this timer with `deadline`.
    pub fn invocation(self, deadline: Deadline) -> NativeInvocation {
        match self {
            Self::Claim(claim) => NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
                claim,
                timer: deadline.timer,
                generation: deadline.generation,
            }),
            Self::Evaluation(evaluation) => {
                NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
                    evaluation,
                    timer: deadline.timer,
                    generation: deadline.generation,
                })
            }
            Self::Monitor(claim, monitor) => {
                NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey {
                    claim,
                    monitor,
                    timer: deadline.timer,
                    generation: deadline.generation,
                })
            }
        }
    }
}

/// Separate immutable definition/descriptor pages from frequently rewritten
/// lifecycle rows. This is an owner-local storage layout, not a wire partition
/// or placement choice. Creating another immutable object may rewrite its own
/// namespace; a lifecycle-only mutation must share those retained pages.
fn page_partition(key: &Key) -> u64 {
    match key {
        Key::Definition(_) | Key::DefinitionIdentity(..) => 1,
        Key::Artifact(_) | Key::ArtifactIdentity(_) => 2,
        Key::ClaimContent(_) | Key::ClaimIdentity(..) => 3,
        Key::CreationResult(_) => 4,
        Key::LegacyTestament(_)
        | Key::LegacyEvidenceSet(_)
        | Key::LegacyRun(..)
        | Key::LegacyDefinition(_) => 5,
        Key::ByIssuer(..)
        | Key::BySubject(..)
        | Key::ByStatus(..)
        | Key::ByAction(..)
        | Key::ByScope(..)
        | Key::ByRelation(..)
        | Key::ByProducer(..)
        | Key::ByArtifactKind(..)
        | Key::BySchema(..)
        | Key::ArtifactInput(..)
        | Key::ByEvaluator(..)
        | Key::ByVerdict(..)
        | Key::ByCreated(..)
        | Key::DueTimer(..)
        | Key::ByObject(..) => 6,
        _ => 0,
    }
}
#[derive(Debug, Default, Clone, Copy)]
struct Meta {
    claims: usize,
    outcomes: usize,
    events: usize,
    definitions: usize,
    evaluations: usize,
    artifacts: usize,
    results: usize,
    receipts: usize,
    responses: usize,
    result_testaments: usize,
    monitors: usize,
    monitor_links: usize,
    creation_results: usize,
    legacy: usize,
    logical_time: u64,
}
/// A resumable position in the committed rows, opaque to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeRowCursor(Key);
/// One content object the committed rows name (26 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentRoot {
    /// An artifact's payload held as a content object.
    Artifact {
        artifact: ArtifactId,
        pointer: focal_model::lifecycle::artifact_descriptor::ContentPointer,
    },
    /// An artifact's payload held inline: the object every replica sealed
    /// at admission under the canonical chunking, so its root is the same
    /// on every node and the record names it.
    Inline {
        artifact: ArtifactId,
        pointer: focal_model::lifecycle::artifact_descriptor::ContentPointer,
    },
    /// A retired family's archive bundle, by the retired claim, content
    /// root and length.
    Bundle {
        claim: ClaimId,
        root: ContentHash,
        bytes: u64,
    },
}
impl ContentRoot {
    /// The object's content root.
    pub fn root(&self) -> ContentHash {
        match self {
            Self::Artifact { pointer, .. } | Self::Inline { pointer, .. } => pointer.root,
            Self::Bundle { root, .. } => *root,
        }
    }
}
/// One page of [`Core::native_content_roots`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentRootsPage {
    pub roots: Vec<ContentRoot>,
    pub next: Option<NativeRowCursor>,
    pub visited: usize,
}
/// The continuation left where a retired claim's rows were (26 §4): the
/// archive bundle that holds them, the retention prefix the retirement
/// named, the claim's final binding and status, and the native sequence
/// the retirement was published at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredClaim {
    /// The content root of the archive bundle, an object of the ledger's
    /// tenant domain under the checkpoint class.
    pub bundle: ContentHash,
    /// The bundle's length: with the root, what names the object.
    pub bytes: u64,
    pub through: SessionSeq,
    pub binding: Binding,
    pub status: ClaimStatus,
    pub retired_at: SessionSeq,
    /// Event rows that left the core with this member; outcome rows keep
    /// counting them, so validation reconciles the two.
    pub events: u32,
}
#[derive(Debug)]
enum Row {
    IncomingHead(incoming_graph::IncomingHead),
    IncomingLink(incoming_graph::IncomingLink),
    Monitor(monitor_index::MonitorAllocation),
    MonitorHead(monitor_index::MonitorHead),
    MonitorLink(Option<monitor_index::MonitorLink>),
    MissingResult(OwnedMissingResult),
    Meta(Meta),
    Claim(OwnedClaim),
    Definition(OwnedDeclaration),
    Evaluation(OwnedEvaluation),
    Artifact(OwnedArtifact),
    ArtifactIdentity(ArtifactId),
    Accepted(OwnedAccepted),
    DeliveryResult(OwnedDeliveryResult),
    Receipt(NativeReceipt),
    Cycle(NativeCycle),
    RetiredCycleHead(RetiredCycleHead),
    RetiredCycle(RetiredCycle),
    Work(OwnedWork),
    WorkSlot(ArtifactId),
    Diagnostic(OwnedDiagnostic),
    Response(OwnedResponse),
    ResultTestament(OwnedResultTestament),
    ClaimResultTestament(TestamentId),
    Outcome(NativeOutcome),
    Event(OwnedEvent),
    ClaimContent(OwnedClaimContent),
    ClaimIdentity(ClaimId),
    DefinitionIdentity(ValidationId),
    CreationResult(OwnedCreationResult),
    LegacyTestament(OwnedLegacy),
    LegacyEvidenceSet(OwnedLegacy),
    LegacyRun(OwnedLegacy),
    LegacyDefinition(OwnedLegacy),
    /// The unit value of every secondary index row.
    Index,
    Retired(RetiredClaim),
}

/// All allocated candidate rows, outcomes and events have one immutable root.
/// Dropping a candidate releases its page permits without touching live state.
pub struct NativePrepared {
    fragments: ranges::Fragments,
    outcome: NativeOutcome,
    writes: mutation::WriteSet,
}
impl std::fmt::Debug for NativePrepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePrepared")
            .field("base", &self.fragments.base_prefix())
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}
impl NativePrepared {
    /// Number of actual storage writes, including metadata, indices and history.
    pub fn mutation_count(&self) -> usize {
        self.writes.len()
    }
    /// The positions, in layout order, of the group members this mutation
    /// writes (25 §6): what a movement fence checks before admission.
    pub fn touched_members(&self) -> impl Iterator<Item = usize> + '_ {
        self.fragments.touched_members()
    }
    pub fn mutation_heap_bytes(&self) -> usize {
        self.writes.heap_bytes()
    }
    pub fn content_profile(&self) -> NativeContentProfile {
        self.writes.profile()
    }
    pub fn outcome(&self) -> NativeOutcome {
        self.outcome
    }
    pub fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.fragments.get(&Key::Claim(id)))
    }
    pub fn receipt(&self, id: ReceiptId) -> Option<NativeReceipt> {
        as_receipt(self.fragments.get(&Key::Receipt(id)))
    }
    pub fn artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.fragments.get(&Key::Artifact(id)))
    }
    pub fn result(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.fragments.get(&Key::Accepted(key)))
    }
    pub fn recorded(&self, key: impl Into<NativeInvocation>) -> Option<NativeOutcome> {
        as_outcome(self.fragments.get(&Key::Outcome(key.into())))
    }
    pub fn definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.fragments.get(&Key::Definition(id)))
    }
    pub fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.fragments.get(&Key::Evaluation(key)))
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
    lease: ranges::RangeLeases,
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
        request: impl Into<NativeInvocation>,
        now: u64,
    ) -> Result<Option<NativeOutcome>, MemoryError> {
        let key = Key::Outcome(request.into());
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

/// Shared local and recovered owner configuration checks.
fn checked_native_limits(
    ledger: LedgerId,
    mut limits: NativeLimits,
) -> Result<NativeLimits, NativeError> {
    if ledger.tenant.is_zero() || ledger.session.is_zero() {
        return Err(ContractError::WrongLedger.into());
    }
    if limits.pending == 0
        || limits.plan_nodes == 0
        || limits.plan_edges == 0
        || limits.preparation_bytes == 0
        || limits.claims == 0
        || limits.outcomes == 0
        || limits.events == 0
        || limits.definitions == 0
        || limits.evaluations == 0
        || limits.evaluations_per_claim == 0
        || limits.artifacts == 0
        || limits.results == 0
        || limits.receipts == 0
        || limits.responses == 0
        || limits.work_artifacts_per_cycle == 0
        || limits.diagnostics_per_cycle == 0
        || limits.artifact_inputs == 0
        || limits.response_summary_bytes == 0
        || limits.max_ranges == 0
        || limits.max_ranges > ranges::MAX_LAYOUT_MEMBERS
        || limits.range.max_batch_entries < 4
    {
        return Err(MemoryError::InvalidConfiguration("native limits must be nonzero").into());
    }
    // Native mutations already bound nested row heaps through Scratch;
    // moved claim/event singleton containers have their separate precharge.
    // Preserve tighter node-derived limits and isolate larger admitted rows
    // so an unrelated small write cannot copy their payloads as neighbors.
    let entry_ceiling = prepare::add(
        prepare::add(
            limits.preparation_bytes,
            prepare::add(
                OwnedClaim::container_charge(),
                OwnedEvent::container_charge(),
            )?,
        )?,
        size_of::<focal_memory::Entry<Key, Row>>(),
    )?;
    limits.range.page_bytes = limits.range.page_bytes.min(64 * 1024);
    limits.range.max_entry_bytes = limits.range.max_entry_bytes.min(entry_ceiling);
    Ok(limits)
}

impl Core<NativeState> {
    pub fn new_native(
        ledger: LedgerId,
        range: RangeId,
        limits: NativeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeError> {
        Self::new_native_profile(
            ledger,
            range,
            limits,
            budget,
            NativeContentProfile::ProjectionOnly,
        )
    }
    /// Start an empty native owner that requires complete authored content.
    /// This never promotes a projection-only root or activates a durable codec.
    pub fn new_native_authored(
        ledger: LedgerId,
        range: RangeId,
        limits: NativeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeError> {
        Self::new_native_profile(
            ledger,
            range,
            limits,
            budget,
            NativeContentProfile::AuthoredV1,
        )
    }
    fn new_native_profile(
        ledger: LedgerId,
        range: RangeId,
        mut limits: NativeLimits,
        budget: MemoryBudget,
        profile: NativeContentProfile,
    ) -> Result<Self, NativeError> {
        limits = checked_native_limits(ledger, limits)?;
        let rows = ranges::NativeRanges::new(range, limits.range, &budget, page_partition)?;
        Ok(Self {
            state: NativeState {
                ledger,
                profile,
                rows,
                budget,
            },
            limits,
        })
    }
    pub fn native_sequence(&self) -> SessionSeq {
        SessionSeq(self.state.rows.prefix())
    }
    /// The trusted clock of the last committed record; zero before any record.
    pub fn native_logical_time(&self) -> u64 {
        match self.state.rows.get(&Key::Meta) {
            Some(Row::Meta(meta)) => meta.logical_time,
            _ => 0,
        }
    }
    /// Frozen V1 bytes retained by import (23 §5.2), never reinterpreted here.
    pub fn native_legacy_bytes(&self, key: NativeLegacyKey) -> Option<&[u8]> {
        let key = match key {
            NativeLegacyKey::Testament(id) => Key::LegacyTestament(id),
            NativeLegacyKey::EvidenceSet(id) => Key::LegacyEvidenceSet(id),
            NativeLegacyKey::Run { validation, run } => Key::LegacyRun(validation, run),
            NativeLegacyKey::Definition(id) => Key::LegacyDefinition(id),
        };
        match self.state.rows.get(&key) {
            Some(
                Row::LegacyTestament(row)
                | Row::LegacyEvidenceSet(row)
                | Row::LegacyRun(row)
                | Row::LegacyDefinition(row),
            ) => Some(row.bytes()),
            _ => None,
        }
    }
    pub fn native_claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.state.rows.get(&Key::Claim(id)))
    }
    pub fn native_outcome(&self, request: impl Into<NativeInvocation>) -> Option<NativeOutcome> {
        as_outcome(self.state.rows.get(&Key::Outcome(request.into())))
    }
    /// The continuation of a claim that retired to the archive (26 §4).
    pub fn native_retired(&self, id: ClaimId) -> Option<&RetiredClaim> {
        match self.state.rows.get(&Key::Retired(id)) {
            Some(Row::Retired(value)) => Some(value),
            _ => None,
        }
    }
    /// The content roots the committed rows name (26 §5): every artifact
    /// held as a content object and every continuation's bundle, walked at
    /// most `max_visits` rows at a time from `cursor` (exclusive) on. The
    /// page's `next` resumes the walk; `None` once every row was visited.
    pub fn native_content_roots(
        &self,
        cursor: Option<NativeRowCursor>,
        max_visits: usize,
    ) -> Result<ContentRootsPage, NativeError> {
        let mut roots = Vec::new();
        let mut visited = 0usize;
        let mut last = None;
        let mut truncated = false;
        let mut push = |root: ContentRoot| -> Result<(), NativeError> {
            roots
                .try_reserve_exact(1)
                .map_err(|_| NativeError::Capacity("content roots"))?;
            roots.push(root);
            Ok(())
        };
        let mut walk = |entry: &focal_memory::Entry<Key, Row>| -> Result<bool, NativeError> {
            if visited >= max_visits {
                return Ok(false);
            }
            visited = visited.saturating_add(1);
            last = Some(entry.key);
            match (&entry.key, &entry.value) {
                (Key::Artifact(id), Row::Artifact(owned)) => {
                    use focal_model::lifecycle::artifact_descriptor::PayloadSpec;
                    if let Some(artifact) = owned.get() {
                        match artifact.descriptor().payload() {
                            PayloadSpec::Content(pointer) => {
                                push(ContentRoot::Artifact {
                                    artifact: *id,
                                    pointer,
                                })?;
                            }
                            PayloadSpec::Inline(_) => {
                                // An inline payload was sealed as an object
                                // at admission; the row's custody names it.
                                push(ContentRoot::Inline {
                                    artifact: *id,
                                    pointer: artifact.custody().payload(),
                                })?;
                            }
                        }
                    }
                }
                (Key::Retired(claim), Row::Retired(value)) => push(ContentRoot::Bundle {
                    claim: *claim,
                    root: value.bundle,
                    bytes: value.bytes,
                })?,
                _ => {}
            }
            Ok(true)
        };
        match cursor {
            None => {
                for entry in self.state.rows.entries() {
                    if !walk(entry)? {
                        truncated = true;
                        break;
                    }
                }
            }
            Some(cursor) => {
                for entry in self.state.rows.entries_from(&cursor.0, true) {
                    if !walk(entry)? {
                        truncated = true;
                        break;
                    }
                }
            }
        }
        Ok(ContentRootsPage {
            roots,
            next: if truncated {
                last.map(NativeRowCursor)
            } else {
                None
            },
            visited,
        })
    }
    pub fn native_definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.state.rows.get(&Key::Definition(id)))
    }
    pub fn native_evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.state.rows.get(&Key::Evaluation(key)))
    }
    pub fn native_receipt(&self, id: ReceiptId) -> Option<NativeReceipt> {
        as_receipt(self.state.rows.get(&Key::Receipt(id)))
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
            .validate_chain(pending.iter().map(|item| &item.fragments))?;
        Ok(())
    }
    /// The external log owner calls this only after durability. This method has
    /// no codec or IO; it performs no allocation, including producing its result.
    #[allow(clippy::result_large_err)] // Return the owned candidate without allocating on refusal.
    pub fn publish_native(
        &mut self,
        prepared: NativePrepared,
    ) -> Result<NativeOutcome, NativePublishError> {
        let NativePrepared {
            fragments,
            outcome,
            writes,
        } = prepared;
        self.state
            .rows
            .publish_recoverable(fragments)
            .map_err(|(error, fragments)| NativePublishError {
                error,
                prepared: NativePrepared {
                    fragments,
                    outcome,
                    writes,
                },
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
    /// The range layout the rows are held in (25 §4).
    pub fn native_layout(&self) -> &ranges::RangeLayout {
        self.state.rows.layout()
    }
    /// The member (position and identity) holding an object's rows (25 §6).
    pub fn native_member_for(&self, location: layout::NativeLocation) -> (usize, RangeId) {
        let layout = self.state.rows.layout();
        let index = layout.route_affinity(&layout::location_affinity(location));
        (
            index,
            layout.member_id(index).unwrap_or(self.state.rows.id()),
        )
    }
    /// The statistics of one member's store.
    pub fn native_member_stats(&self, index: usize) -> Option<RangeStats> {
        self.state.rows.member_stats(index)
    }
    /// An affinity dividing one member near its middle, if any (25 §8).
    pub fn native_member_split_point(&self, index: usize) -> Option<ranges::Affinity> {
        self.state.rows.member_split_point(index)
    }
    /// A digest of one member's rows at the current prefix (25 §6).
    pub fn native_member_digest(
        &self,
        index: usize,
        limits: record_codec::EncodingLimits,
    ) -> Result<ContentHash, record_codec::CodecError> {
        record_codec::checkpoint::member_digest(self, index, limits)
    }
    /// Add a range boundary at affinity `at` (a claim's affinity is its
    /// identity's bytes), naming the member from `at` on `id`. Refused
    /// under read leases, at an existing boundary, for a known identity or
    /// past the configured member bound (25 §4).
    pub fn split_native_range(
        &mut self,
        at: ranges::Affinity,
        id: RangeId,
    ) -> Result<(), NativeError> {
        self.state.rows.split(
            at,
            id,
            self.limits.max_ranges,
            focal_memory::BudgetLane::Ordinary,
            prepare::copy,
        )
    }
    /// Give range member `index` the durable identity `id`: what a session
    /// does once its genesis is known, so every replica names its origin
    /// member alike.
    pub fn rename_native_member(&mut self, index: usize, id: RangeId) -> Result<(), NativeError> {
        self.state.rows.rename_member(index, id)
    }
    /// Remove the boundary after member `index`, joining it with the next
    /// under its identity.
    pub fn merge_native_range(&mut self, index: usize) -> Result<(), NativeError> {
        self.state
            .rows
            .merge(index, focal_memory::BudgetLane::Ordinary)
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
impl<'a> View<'a> {
    fn get(&self, key: Key) -> Option<&'a Row> {
        match self.tail {
            Some(tail) => tail.fragments.get(&key),
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
    /// Ordered rows of the effective prefix at or after `start`. The caller
    /// bounds what it consumes; nothing is allocated per row.
    fn entries_from(
        &self,
        start: Key,
    ) -> impl Iterator<Item = &'a focal_memory::Entry<Key, Row>> + use<'a> {
        let (committed, tail) = match self.tail {
            Some(tail) => (None, Some(tail.fragments.entries_from(&start, false))),
            None => (Some(self.state.rows.entries_from(&start, false)), None),
        };
        committed
            .into_iter()
            .flatten()
            .chain(tail.into_iter().flatten())
    }
    /// Committed claims whose authored `kind` relation targets `target`
    /// (the relation index of 22 §7), in identity order.
    fn relation_sources(
        &self,
        kind: focal_model::RelationKind,
        target: ClaimId,
    ) -> impl Iterator<Item = ClaimId> + use<'a> {
        let code = kind.code();
        self.entries_from(Key::ByRelation(code, target, ClaimId([0; 16])))
            .take_while(move |entry| matches!(entry.key, Key::ByRelation(k, t, _) if k == code && t == target))
            .filter(|entry| matches!(entry.value, Row::Index))
            .filter_map(|entry| match entry.key {
                Key::ByRelation(_, _, source) => Some(source),
                _ => None,
            })
    }
    /// Every evaluation of `claim` in key order with its state.
    fn evaluations_of(
        &self,
        claim: ClaimId,
    ) -> impl Iterator<Item = (EvaluationKey, &'a validation::EvaluationState)> + use<'a> {
        self.entries_from(Key::Evaluation(EvaluationKey {
            claim,
            validation: ValidationId([0; 16]),
            target: EvaluationTarget::Admission,
            generation: 0,
        }))
        .take_while(move |entry| matches!(entry.key, Key::Evaluation(key) if key.claim == claim))
        .filter_map(|entry| match (&entry.key, &entry.value) {
            (Key::Evaluation(key), Row::Evaluation(state)) => {
                state.get().map(|state| (*key, state))
            }
            _ => None,
        })
    }
}
impl EffectiveClaims for View<'_> {
    fn ledger(&self) -> LedgerId {
        self.state.ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.tail
            .map_or(SessionSeq(self.state.rows.prefix()), |tail| {
                SessionSeq(tail.fragments.prefix())
            })
    }
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.get(Key::Claim(id)))
    }
    fn cause_escalation(&self, parent: ClaimId) -> focal_model::Escalation {
        authored_reads::content(self.get(Key::ClaimContent(parent)))
            .and_then(|content| content.policy())
            .map_or(focal_model::Escalation::Holder, |policy| policy.escalation)
    }
    fn is_designated_evaluator(&self, parent: ClaimId, actor: ParticipantId) -> bool {
        authored_reads::content(self.get(Key::ClaimContent(parent))).is_some_and(|content| {
            content.requirements().iter().any(|pin| {
                as_definition(self.get(Key::Definition(pin.id)))
                    .is_some_and(|definition| definition.designates(actor))
            })
        })
    }
}

fn as_artifact(row: Option<&Row>) -> Option<&NativeArtifact> {
    match row {
        Some(Row::Artifact(value)) => value.get(),
        _ => None,
    }
}
fn as_result(row: Option<&Row>) -> Option<&NativeAccepted> {
    match row {
        Some(Row::Accepted(value)) => value.get(),
        _ => None,
    }
}
impl NativeRead {
    pub fn with_artifact<T>(
        &self,
        id: ArtifactId,
        now: u64,
        project: impl FnOnce(&NativeArtifact) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Artifact(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_artifact(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_result<T>(
        &self,
        id: NativeResultKey,
        now: u64,
        project: impl FnOnce(&NativeAccepted) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Accepted(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_result(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

fn as_receipt(row: Option<&Row>) -> Option<NativeReceipt> {
    match row {
        Some(Row::Receipt(value)) => Some(*value),
        _ => None,
    }
}
impl NativeRead {
    pub fn receipt(&self, id: ReceiptId, now: u64) -> Result<Option<NativeReceipt>, MemoryError> {
        let key = Key::Receipt(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_receipt(Some(&entry.value))
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}
