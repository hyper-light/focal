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
#[cfg(test)]
mod funding_tests;
mod graph_effects;
mod history;
mod incoming_graph;
mod increment_authority;
mod increment_seal;
mod increments;
pub mod input_codec;
mod intent;
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
mod prepare;
mod prepare_budget;
mod projection;
mod projection_quote;
mod projection_visits;
mod projection_work;
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
use focal_memory::{
    BudgetStats, MemoryBudget, MemoryError, PreparedRange, RangeConfig, RangeId, RangeStats,
    RangeStore, SnapshotLease,
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
    ReceiptFence, ReceiptId, RequestKey, SessionSeq, TestamentId, TimerId, ValidationId,
    WaitPredicate,
};
use history::StoredEvent;
pub use missing_owned::NativeMissingResult;
use missing_owned::OwnedMissingResult;
use owned::{OwnedClaim, OwnedClaimContent, OwnedDeclaration, OwnedEvaluation, OwnedEvent};
pub use owner::{
    NativeCandidate, NativeOwner, NativeOwnerError, NativeOwnerInitError, NativeStaging, NativeView,
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
        }
    }
}

pub struct NativeState {
    ledger: LedgerId,
    profile: NativeContentProfile,
    rows: RangeStore<Key, Row>,
    budget: MemoryBudget,
}
impl std::fmt::Debug for NativeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeState")
            .field("ledger", &self.ledger)
            .field("profile", &self.profile)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    End,
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
    logical_time: u64,
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
}

/// All allocated candidate rows, outcomes and events have one immutable root.
/// Dropping a candidate releases its page permits without touching live state.
pub struct NativePrepared {
    range: PreparedRange<Key, Row>,
    outcome: NativeOutcome,
    writes: mutation::WriteSet,
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
    /// Number of actual storage writes, including metadata, indices and history.
    pub fn mutation_count(&self) -> usize {
        self.writes.len()
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
        as_claim(self.range.get(&Key::Claim(id)))
    }
    pub fn receipt(&self, id: ReceiptId) -> Option<NativeReceipt> {
        as_receipt(self.range.get(&Key::Receipt(id)))
    }
    pub fn artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.range.get(&Key::Artifact(id)))
    }
    pub fn result(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.range.get(&Key::Accepted(key)))
    }
    pub fn recorded(&self, key: impl Into<NativeInvocation>) -> Option<NativeOutcome> {
        as_outcome(self.range.get(&Key::Outcome(key.into())))
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
fn checked_native_limits(ledger: LedgerId, mut limits: NativeLimits) -> Result<NativeLimits, NativeError> {
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
        || limits.response_summary_bytes == 0
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
        let rows =
            RangeStore::new_partitioned(range, 0, limits.range, budget.clone(), page_partition)?;
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
    pub fn native_claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.state.rows.get(&Key::Claim(id)))
    }
    pub fn native_outcome(&self, request: impl Into<NativeInvocation>) -> Option<NativeOutcome> {
        as_outcome(self.state.rows.get(&Key::Outcome(request.into())))
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
        let NativePrepared {
            range,
            outcome,
            writes,
        } = prepared;
        self.state
            .rows
            .publish_recoverable(range)
            .map_err(|(error, range)| NativePublishError {
                error,
                prepared: NativePrepared {
                    range,
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
