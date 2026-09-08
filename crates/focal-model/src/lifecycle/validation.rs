//! Pure target-bound validation contract from architecture document 17 §6.
//!
//! Owner views below are effective committed/staged facts, never client-authored
//! authorization. Successful transitions must be staged with their result evidence
//! and parent consequences; this module neither executes a validator nor publishes
//! a result. Request deduplication belongs to the outer mutation layer.
use super::{Binding, ContractError, Principal};
use crate::{
    ArtifactId, ArtifactRef, ClaimId, ContentHash, Deadline, HandlerRef, LedgerId, ParticipantId,
    ReceiptFence, ValidationId, ValidationKind, ValidationMode, ValidationPhase, ValidatorId,
    VerdictValue,
};

#[path = "validation_admission.rs"]
mod admission;
#[path = "validation_adoption.rs"]
mod adoption;
#[path = "validation_claim_deadline.rs"]
mod claim_deadline;
#[path = "validation_deadline.rs"]
mod deadline;
#[path = "validation_delivery.rs"]
mod delivery;
#[path = "validation_increment.rs"]
mod increment;
#[path = "validation_seal.rs"]
mod seal;
#[path = "validation_snapshot.rs"]
mod snapshot;
#[path = "validation_work.rs"]
mod work;
pub use seal::SealTransition;
pub use snapshot::{AcceptedResultSnapshotV1, EvaluationSnapshotV1};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Ready,
    Validating,
    ValidatingQualityBar,
    Validated,
    ValidationIncomplete,
    ValidationFailed,
    ValidationFailedNotRequired,
    Errored,
    ErroredNotRequired,
    QualityBarValidationFailed,
    QualityBarValidationFailedNotRequired,
}

impl State {
    pub const fn is_terminal(self) -> bool {
        match self {
            Self::Ready | Self::Validating | Self::ValidatingQualityBar => false,
            Self::Validated
            | Self::ValidationIncomplete
            | Self::ValidationFailed
            | Self::ValidationFailedNotRequired
            | Self::Errored
            | Self::ErroredNotRequired
            | Self::QualityBarValidationFailed
            | Self::QualityBarValidationFailedNotRequired => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Programmatic,
    Quality,
    Delivery,
    MissingTarget,
}

/// Exact immutable target references. Revisions identify the pinned target view,
/// not a request to prevent unrelated later lifecycle history on those objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Artifact {
        response: Binding,
        slot: u32,
        artifact: Binding,
    },
    MissingSlot {
        response: Binding,
        slot: u32,
    },
    Delivery {
        response: Binding,
    },
    Admission {
        claim: Binding,
    },
    Increment {
        claim: Binding,
        artifact: Binding,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetDeclaration<'a> {
    WholeWorkSlot { index: u32, name: &'a str },
    Delivery,
    Admission,
    Increment,
}

#[derive(Debug, Clone, Copy)]
pub struct HandlerPolicy<'a> {
    pub handler: &'a HandlerRef,
    /// Attempts at this handler before moving to the next declared fallback.
    pub attempts: u32,
    pub proof_schema: ContentHash,
    pub diagnostic_schema: ContentHash,
}

#[derive(Debug, Clone, Copy)]
pub struct PhasePolicy<'a> {
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
    pub handlers: &'a [HandlerPolicy<'a>],
    /// Only an expressly declared capability requires an owner-installed grant.
    pub required_policy: Option<ContentHash>,
}

#[derive(Debug, Clone, Copy)]
pub enum Program<'a> {
    Delivery,
    Programmatic {
        check: PhasePolicy<'a>,
        quality: Option<PhasePolicy<'a>>,
    },
    Agentic {
        check: PhasePolicy<'a>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub handlers: u32,
    pub attempts: u32,
    pub slot_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct DeclarationSpec<'a> {
    pub binding: Binding,
    pub claim: ClaimId,
    pub issuer: ParticipantId,
    pub declaration_index: u32,
    pub kind: ValidationKind,
    pub phase: ValidationPhase,
    pub mode: ValidationMode,
    pub target: TargetDeclaration<'a>,
    pub program: Program<'a>,
    pub deadline: Deadline,
}

#[path = "validation_definition.rs"]
mod definition;
pub(super) use definition::DefinitionStamp;
#[cfg(test)]
use definition::OwnedHandlerPolicy;
use definition::OwnedProgram;
pub(in crate::lifecycle) use definition::stamp_begin;
pub use definition::{Declaration, DeclarationPlan, PhasePolicyView, ProgramView};
pub use definition::{
    DeclarationFields, DeclarationHandlers, DeclarationSource, DeclarationSourcePlan, HandlerValue,
    PhaseFields, PolicyPhase, PolicySource, ProgramFields,
};
pub(in crate::lifecycle) use definition::{PolicyEvent, check_declaration_fields, visit_program};
#[path = "validation_checked.rs"]
mod checked;
pub use checked::{CheckedDeclaration, CheckedDeclarationTarget};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentState {
    Open,
    Failed { cause: ContentHash },
    LocallyComplete { cause: ContentHash },
    Cancelled,
    Revoked,
    Superseded,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    AdmissionPosted,
    IncrementEligible,
    ResponseGenerated,
    ResponsePosted,
    ResponseReceived(ResponseReadiness),
    ArtifactFailed {
        artifact: Binding,
        cause: ContentHash,
    },
}

/// A received response and exact immutable manifest selection, obtained only from
/// privately constructed response state. A matching schema never supplies a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseReadiness {
    target: Target,
    claim: ClaimId,
    receipt: ReceiptFence,
}

impl ResponseReadiness {
    pub fn from_evaluation(
        response: &super::evidence::ResponseEvaluation<'_>,
        target: Target,
    ) -> Result<Self, ContractError> {
        Self::checked(
            response.binding(),
            response.claim(),
            response.receipt(),
            response.manifest(),
            target,
        )
    }

    pub fn from_received(
        response: &super::evidence::Response,
        target: Target,
        claim: &super::claim::ClaimState,
        acceptance: &super::aggregation::ClaimDecision<'_>,
    ) -> Result<Self, ContractError> {
        claim.binding().check(&acceptance.binding())?;
        if claim.acceptance() != acceptance.acceptance() {
            return Err(ContractError::InvalidPolicy);
        }
        if response.identity().claim.0 != claim.binding().object.0
            || response.identity().binding.ledger != claim.binding().ledger
            || claim.receipt().map(|receipt| receipt.fence) != Some(response.identity().receipt)
        {
            return Err(ContractError::InvalidTarget);
        }
        if !matches!(target, Target::Delivery { .. }) {
            claim.check_increment_entry(acceptance)?;
        }
        use super::evidence::ResponseState;
        match response.state() {
            ResponseState::Received
            | ResponseState::Validating
            | ResponseState::Validated
            | ResponseState::ValidationIncomplete
            | ResponseState::ValidationFailed
            | ResponseState::ValidationErrored => {}
            ResponseState::Generated | ResponseState::Posted => {
                return Err(ContractError::InvalidTransition);
            }
        }
        let identity = response.identity();
        claim.received_report(identity.binding, identity.receipt, response.report_stamp())?;
        Self::checked(
            identity.binding,
            identity.claim,
            identity.receipt,
            response.manifest(),
            target,
        )
    }

    fn checked(
        response: Binding,
        claim: ClaimId,
        receipt: ReceiptFence,
        manifest: &[super::evidence::SlotBinding],
        target: Target,
    ) -> Result<Self, ContractError> {
        let expected = match target {
            Target::Artifact { response, .. }
            | Target::MissingSlot { response, .. }
            | Target::Delivery { response } => response,
            Target::Admission { .. } | Target::Increment { .. } => {
                return Err(ContractError::InvalidTarget);
            }
        };
        // Lifecycle revisions may advance after the target view was pinned;
        // response/content identity and receipt must remain exactly bound.
        expected.check(&Binding {
            revision: expected.revision,
            ..response
        })?;
        match target {
            Target::Artifact { slot, artifact, .. } => {
                let reference = ArtifactRef {
                    id: ArtifactId(artifact.object.0),
                    hash: artifact.content,
                };
                if !manifest
                    .iter()
                    .any(|entry| entry.slot == slot && entry.artifact == reference)
                {
                    return Err(ContractError::InvalidManifest);
                }
            }
            Target::MissingSlot { slot, .. } => {
                if manifest.iter().any(|entry| entry.slot == slot) {
                    return Err(ContractError::InvalidManifest);
                }
            }
            Target::Delivery { .. } => {}
            Target::Admission { .. } | Target::Increment { .. } => {
                return Err(ContractError::InvalidTarget);
            }
        }
        Ok(Self {
            target,
            claim,
            receipt,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cohort {
    Open,
    Sealed { cause: ContentHash },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceReason {
    Cancellation,
    Revocation,
    Supersession,
    Expiry,
    ReceiptAdoption,
    Evaluation,
    Deadline(Deadline),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityFence {
    pub reason: FenceReason,
    pub cause: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityState {
    Live,
    Fenced(AuthorityFence),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyEvidence {
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
    pub policy: ContentHash,
}

/// Installed evaluator/definition/receipt/generation facts. A designation grants
/// no right to author a respondent's evidence or impersonate the issuer's receipt.
#[derive(Debug, Clone, Copy)]
pub struct Authority {
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
    pub generation: u64,
    pub receipt: Option<ReceiptFence>,
    pub deadline: Deadline,
    pub policy_evidence: Option<PolicyEvidence>,
    pub state: AuthorityState,
}

#[derive(Debug, Clone, Copy)]
pub struct OwnerState {
    pub evaluation: Binding,
    pub target: Target,
    pub parent: ParentState,
    pub readiness: Readiness,
    pub cohort: Cohort,
    pub authority: Authority,
    /// A committed logical-time input, never a clock read in this contract.
    pub logical_time: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Suppression {
    MissingTarget,
    ParentFailure(ContentHash),
    ArtifactFailure(ContentHash),
    CohortSealed(ContentHash),
}

#[derive(Debug, Clone, Copy)]
pub struct Materialization<'a> {
    pub binding: Binding,
    pub target: Target,
    /// Owner-resolved immutable slot name; schemas are never slot selectors.
    pub slot_name: Option<&'a str>,
    pub generation: u64,
    pub receipt: Option<ReceiptFence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attempt {
    pub phase: Phase,
    pub index: u32,
    pub handler: ValidatorId,
    pub version: ContentHash,
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceKind {
    Proof,
    Diagnostic,
}

/// Trusted immutable result-artifact metadata and installed custody facts.
#[derive(Debug, Clone, Copy)]
pub struct EvidenceFacts {
    pub binding: Binding,
    pub claim: ClaimId,
    pub validation: ValidationId,
    pub target: Target,
    pub generation: u64,
    pub attempt: Attempt,
    pub producer: ParticipantId,
    pub value: VerdictValue,
    pub kind: EvidenceKind,
    pub schema: ContentHash,
    /// None means custody has not been established; revision zero is not durable.
    pub custody_revision: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct Report {
    pub generation: u64,
    pub attempt: Attempt,
    pub value: VerdictValue,
    pub evidence: ArtifactRef,
}

/// A successful authority preflight for one exact current report attempt. It
/// supplies the schema to verify before custody IO or funding selection; it is
/// not evidence custody, an accepted verdict, or a publication capability.
/// `report` repeats authority checks against its current immutable owner frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportAuthorization {
    attempt: Attempt,
    schema: ContentHash,
    kind: EvidenceKind,
}
impl ReportAuthorization {
    pub fn attempt(self) -> Attempt {
        self.attempt
    }
    pub fn schema(self) -> ContentHash {
        self.schema
    }
}

/// Recorded acceptance capability, emitted by a checked transition or restored
/// from verified history. Intermediate attempts are audit facts, not acceptance
/// witnesses; `is_terminal` distinguishes them. Snapshot restoration alone does
/// not establish the enclosing history's authority or artifact custody.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedResult {
    definition: DefinitionStamp,
    binding: Binding,
    ledger: LedgerId,
    claim: ClaimId,
    target: Target,
    validation: ValidationId,
    declaration_index: u32,
    mode: ValidationMode,
    verdict: VerdictValue,
    phase: Phase,
    attempt: Option<u32>,
    generation: u64,
    receipt: Option<ReceiptFence>,
    evidence: Option<ArtifactRef>,
    programmatic_evidence: Option<ArtifactRef>,
    reporter: Option<ParticipantId>,
    resulting_state: State,
}

impl AcceptedResult {
    pub(super) fn definition_stamp(self) -> DefinitionStamp {
        self.definition
    }
    pub fn binding(self) -> Binding {
        self.binding
    }
    pub fn ledger(self) -> LedgerId {
        self.ledger
    }
    pub fn claim(self) -> ClaimId {
        self.claim
    }
    pub fn target(self) -> Target {
        self.target
    }
    pub fn validation(self) -> ValidationId {
        self.validation
    }
    pub fn declaration_index(self) -> u32 {
        self.declaration_index
    }
    pub fn mode(self) -> ValidationMode {
        self.mode
    }
    pub fn verdict(self) -> VerdictValue {
        self.verdict
    }
    pub fn phase(self) -> Phase {
        self.phase
    }
    pub fn attempt(self) -> Option<u32> {
        self.attempt
    }
    pub fn generation(self) -> u64 {
        self.generation
    }
    pub fn receipt(self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn evidence(self) -> Option<ArtifactRef> {
        self.evidence
    }
    pub fn programmatic_evidence(self) -> Option<ArtifactRef> {
        self.programmatic_evidence
    }
    pub fn reporter(self) -> Option<ParticipantId> {
        self.reporter
    }
    pub fn resulting_state(self) -> State {
        self.resulting_state
    }
    pub fn is_terminal(self) -> bool {
        self.resulting_state.is_terminal()
    }
    pub fn evidence_kind(self) -> Option<EvidenceKind> {
        self.evidence.map(|_| match self.verdict {
            VerdictValue::Pass | VerdictValue::Fail => EvidenceKind::Proof,
            VerdictValue::Incomplete | VerdictValue::Error => EvidenceKind::Diagnostic,
        })
    }
}

/// Temporary checked view over an owned definition and a detached native row.
/// Retain `into_state()` beside the definition; recover a view with `bind` when
/// evaluating a transition. Neither operation allocates or copies policy buffers.
#[derive(Debug, Clone, Copy)]
pub struct Evaluation<'a> {
    declaration: &'a Declaration,
    stored: EvaluationState,
}

/// Independently retainable native evaluation row. Every field is private; a
/// temporary view can only be recovered by checking its actual owned definition.
/// This has no wire representation or durable identity contract yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationState {
    definition: DefinitionStamp,
    binding: Binding,
    target: Target,
    generation: u64,
    receipt: Option<ReceiptFence>,
    state: State,
    phase: Phase,
    handler: usize,
    handler_attempt: u32,
    attempt: u32,
    begun: bool,
    suppression: Option<Suppression>,
    sealed: Option<ContentHash>,
    fence: Option<AuthorityFence>,
    programmatic_evidence: Option<ArtifactRef>,
    last_result: Option<AcceptedResult>,
}

#[derive(Debug, Clone, Copy)]
pub struct Transition<'a> {
    pub next: Evaluation<'a>,
    pub result: Option<AcceptedResult>,
}

impl EvaluationState {
    /// Reattach only the exact semantic definition admitted at materialization.
    /// The supplied content stamp alone never establishes that equivalence.
    pub fn bind<'a>(self, declaration: &'a Declaration) -> Result<Evaluation<'a>, ContractError> {
        if self.definition != declaration.definition_stamp() {
            return Err(ContractError::ContentConflict);
        }
        Ok(Evaluation {
            declaration,
            stored: self,
        })
    }
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn state(&self) -> State {
        self.state
    }
    pub fn target(&self) -> Target {
        self.target
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }
    pub fn receipt(&self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn has_begun(&self) -> bool {
        self.begun
    }
    pub fn fence(&self) -> Option<AuthorityFence> {
        self.fence
    }
    pub fn last_result(&self) -> Option<AcceptedResult> {
        self.last_result
    }
    pub fn sealed(&self) -> Option<ContentHash> {
        self.sealed
    }

    /// Derive control from an actual checked ownership closure. A caller cannot
    /// manufacture an OwnerState to cancel another claim's evaluation here.
    /// Terminal results and an earlier authority fence remain exact history.
    pub fn cancel(
        self,
        declaration: &Declaration,
        cancellation: &super::ownership::Cancellation<'_>,
    ) -> Result<Self, ContractError> {
        let evaluation = self.bind(declaration)?;
        let claim = cancellation.claim();
        claim.acceptance().check_declaration(declaration)?;
        if evaluation.claim().0 != claim.binding().object.0
            || evaluation.ledger() != claim.binding().ledger
        {
            return Err(ContractError::InvalidTarget);
        }
        if self.state.is_terminal() || self.fence.is_some() {
            return Ok(self);
        }
        let mut next = self;
        next.binding = self.binding.next()?;
        next.fence = Some(AuthorityFence {
            reason: FenceReason::Cancellation,
            cause: cancellation.cut().cause,
        });
        Ok(next)
    }

    /// Only a creation plan that actually changes an open predecessor to
    /// Superseded grants this control. An Amends link or a successor of an
    /// already-terminal predecessor does not manufacture a new fence.
    pub fn supersede(
        self,
        declaration: &Declaration,
        previous: &super::claim::ClaimState,
        supersession: &super::creation::Supersession,
    ) -> Result<Self, ContractError> {
        supersession.check(previous)?;
        let evaluation = self.bind(declaration)?;
        previous.acceptance().check_declaration(declaration)?;
        if evaluation.claim().0 != previous.binding().object.0
            || evaluation.ledger() != previous.binding().ledger
        {
            return Err(ContractError::InvalidTarget);
        }
        if self.state.is_terminal() || self.fence.is_some() {
            return Ok(self);
        }
        let mut next = self;
        next.binding = self.binding.next()?;
        next.fence = Some(AuthorityFence {
            reason: FenceReason::Supersession,
            cause: supersession.cut().cause,
        });
        Ok(next)
    }
}

// Read-only access preserves existing getter ergonomics. There is deliberately
// no DerefMut: swapping detached rows must always pass EvaluationState::bind.
impl std::ops::Deref for Evaluation<'_> {
    type Target = EvaluationState;
    fn deref(&self) -> &Self::Target {
        &self.stored
    }
}

impl<'a> Evaluation<'a> {
    /// Native Admission materialization derives its target and receipt absence
    /// from the actual owning claim. Other target families require their own
    /// response/artifact owner proof and cannot enter through this helper.
    pub fn materialize_admission(
        principal: Principal,
        declaration: &'a Declaration,
        claim: &super::claim::ClaimState,
        generation: u64,
    ) -> Result<Self, ContractError> {
        claim.acceptance().check_declaration(declaration)?;
        if declaration.target() != TargetDeclaration::Admission
            || claim.status() != crate::ClaimStatus::Posted
            || claim.local_complete()
            || claim.receipt().is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        Self::materialize(
            principal,
            declaration,
            Materialization {
                binding: declaration.binding(),
                target: Target::Admission {
                    claim: claim.binding(),
                },
                slot_name: None,
                generation,
                receipt: None,
            },
        )
    }

    /// Installed facts for the native Admission begin path. The publishing
    /// owner supplies only its effective claim and committed logical time; no
    /// participant override chooses readiness, deadline or evaluator authority.
    pub fn admission_owner(
        &self,
        claim: &super::claim::ClaimState,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        claim.acceptance().check_declaration(self.declaration)?;
        let Target::Admission { claim: target } = self.target else {
            return Err(ContractError::InvalidTarget);
        };
        Binding {
            revision: target.revision,
            ..claim.binding()
        }
        .check(&target)?;
        if claim.binding().revision < target.revision {
            return Err(ContractError::StaleRevision);
        }
        if claim.status() != crate::ClaimStatus::Posted
            || claim.local_complete()
            || claim.receipt().is_some()
            || self.receipt.is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        // This native owner frame has no installed policy-evidence registry.
        // Qualify every reachable phase before responsibility begins: accepting
        // a grant-free programmatic phase must not strand its required quality
        // continuation. Explicit owner frames still check actual phase grants.
        let requires_policy = match &self.declaration.spec.program {
            OwnedProgram::Delivery => false,
            OwnedProgram::Programmatic { check, quality } => {
                check.required_policy.is_some()
                    || quality
                        .as_ref()
                        .is_some_and(|quality| quality.required_policy.is_some())
            }
            OwnedProgram::Agentic { check } => check.required_policy.is_some(),
        };
        if requires_policy {
            return Err(ContractError::InvalidPolicy);
        }
        let policy = self.declaration.policy(self.phase)?;
        Ok(OwnerState {
            evaluation: self.binding,
            target: self.target,
            parent: ParentState::Open,
            readiness: Readiness::AdmissionPosted,
            cohort: self
                .sealed
                .map_or(Cohort::Open, |cause| Cohort::Sealed { cause }),
            authority: Authority {
                evaluator: policy.evaluator,
                definition: policy.definition,
                generation: self.generation,
                receipt: None,
                deadline: self.declaration.spec.deadline,
                policy_evidence: None,
                state: self
                    .fence
                    .map_or(AuthorityState::Live, AuthorityState::Fenced),
            },
            logical_time,
        })
    }

    pub fn materialize(
        principal: Principal,
        declaration: &'a Declaration,
        materialization: Materialization<'_>,
    ) -> Result<Self, ContractError> {
        principal.require_actor(declaration.spec.issuer)?;
        declaration.spec.binding.check(&materialization.binding)?;
        if materialization.generation == 0 {
            return Err(ContractError::StaleEvaluation);
        }
        check_target(
            declaration,
            materialization.target,
            materialization.slot_name,
        )?;
        match (declaration.target(), materialization.receipt) {
            (TargetDeclaration::Admission, None) => {}
            (TargetDeclaration::Admission, Some(_)) | (_, None) => {
                return Err(ContractError::StaleReceipt);
            }
            (_, Some(receipt)) if receipt.receipt.is_zero() || receipt.epoch == 0 => {
                return Err(ContractError::StaleReceipt);
            }
            (_, Some(_)) => {}
        }
        Ok(Self {
            declaration,
            stored: EvaluationState {
                definition: declaration.definition_stamp(),
                binding: materialization.binding,
                target: materialization.target,
                generation: materialization.generation,
                receipt: materialization.receipt,
                state: State::Ready,
                phase: declaration.first_phase(),
                handler: 0,
                handler_attempt: 0,
                attempt: 0,
                begun: false,
                suppression: None,
                sealed: None,
                fence: None,
                programmatic_evidence: None,
                last_result: None,
            },
        })
    }

    pub fn into_state(self) -> EvaluationState {
        self.stored
    }
    pub(super) fn definition_stamp(&self) -> DefinitionStamp {
        self.stored.definition
    }
    pub fn binding(&self) -> Binding {
        self.stored.binding
    }
    pub fn ledger(&self) -> LedgerId {
        self.binding.ledger
    }
    pub fn claim(&self) -> ClaimId {
        self.declaration.spec.claim
    }
    pub fn validation(&self) -> ValidationId {
        ValidationId(self.binding.object.0)
    }
    pub fn declaration_index(&self) -> u32 {
        self.declaration.spec.declaration_index
    }
    pub fn declared_phase(&self) -> ValidationPhase {
        self.declaration.spec.phase
    }
    pub fn current_phase(&self) -> Phase {
        self.phase
    }
    pub fn receipt(&self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn deadline(&self) -> Deadline {
        self.declaration.spec.deadline
    }
    pub fn issuer(&self) -> ParticipantId {
        self.declaration.spec.issuer
    }
    pub fn attempt_index(&self) -> Option<u32> {
        self.begun.then_some(self.attempt)
    }
    pub fn attempt_bound(&self) -> u32 {
        self.declaration.attempt_bound()
    }
    pub fn evaluator(&self) -> Result<ParticipantId, ContractError> {
        if self.phase == Phase::Delivery {
            Ok(self.issuer())
        } else {
            Ok(self.declaration.policy(self.phase)?.evaluator)
        }
    }
    pub fn state(&self) -> State {
        self.state
    }
    pub fn target(&self) -> Target {
        self.target
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn mode(&self) -> ValidationMode {
        self.declaration.spec.mode
    }
    pub fn suppression(&self) -> Option<Suppression> {
        self.suppression
    }
    pub fn sealed(&self) -> Option<ContentHash> {
        self.sealed
    }
    pub fn fence(&self) -> Option<AuthorityFence> {
        self.fence
    }
    pub fn has_begun(&self) -> bool {
        self.begun
    }
    pub fn last_result(&self) -> Option<AcceptedResult> {
        self.last_result
    }
    pub fn audit_finished(&self) -> bool {
        self.state.is_terminal() || self.fence.is_some() || (!self.begun && self.sealed.is_some())
    }

    pub fn current_attempt(&self) -> Result<Attempt, ContractError> {
        if !self.begun || self.state.is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        self.attempt_at(self.phase, self.handler, self.attempt)
    }

    fn attempt_at(
        &self,
        phase: Phase,
        handler: usize,
        index: u32,
    ) -> Result<Attempt, ContractError> {
        let policy = self.declaration.policy(phase)?;
        let step = policy
            .handlers
            .get(handler)
            .ok_or(ContractError::InvalidPolicy)?;
        Ok(Attempt {
            phase,
            index,
            handler: step.handler.id,
            version: step.handler.version,
            evaluator: policy.evaluator,
            definition: policy.definition,
        })
    }

    fn check_frame(&self, expected: &Binding, owner: &OwnerState) -> Result<(), ContractError> {
        self.binding.check(expected)?;
        self.binding.check(&owner.evaluation)?;
        if self.target != owner.target {
            return Err(ContractError::InvalidTarget);
        }
        Ok(())
    }

    fn check_authority(&self, owner: &OwnerState, phase: Phase) -> Result<(), ContractError> {
        if owner.authority.receipt != self.receipt {
            return Err(ContractError::StaleReceipt);
        }
        if owner.authority.generation != self.generation
            || owner.authority.deadline != self.declaration.spec.deadline
            || owner.authority.state != AuthorityState::Live
            || self.fence.is_some()
            || owner.logical_time >= self.declaration.spec.deadline.at
        {
            return Err(ContractError::StaleEvaluation);
        }
        match owner.parent {
            ParentState::Cancelled
            | ParentState::Revoked
            | ParentState::Superseded
            | ParentState::Expired => return Err(ContractError::StaleEvaluation),
            ParentState::Open
            | ParentState::Failed { .. }
            | ParentState::LocallyComplete { .. } => {}
        }
        if phase == Phase::Delivery {
            if owner.authority.evaluator != self.declaration.spec.issuer
                || owner.authority.definition != self.declaration.spec.binding.content
            {
                return Err(ContractError::StaleEvaluation);
            }
        } else {
            let policy = self.declaration.policy(phase)?;
            if owner.authority.evaluator != policy.evaluator
                || owner.authority.definition != policy.definition
            {
                return Err(ContractError::StaleEvaluation);
            }
            if let Some(required) = policy.required_policy
                && owner.authority.policy_evidence
                    != Some(PolicyEvidence {
                        evaluator: policy.evaluator,
                        definition: policy.definition,
                        policy: required,
                    })
            {
                return Err(ContractError::InvalidPolicy);
            }
        }
        Ok(())
    }

    fn check_readiness(&self, readiness: Readiness) -> Result<(), ContractError> {
        match (self.target, readiness) {
            (Target::Admission { .. }, Readiness::AdmissionPosted)
            | (Target::Increment { .. }, Readiness::IncrementEligible) => Ok(()),
            (
                Target::Artifact { .. } | Target::MissingSlot { .. } | Target::Delivery { .. },
                Readiness::ResponseReceived(received),
            ) if received.target == self.target
                && received.claim == self.declaration.spec.claim
                && Some(received.receipt) == self.receipt =>
            {
                Ok(())
            }
            _ => Err(ContractError::InvalidTransition),
        }
    }

    fn unbegun_suppression(
        &self,
        owner: &OwnerState,
    ) -> Result<Option<Suppression>, ContractError> {
        if let Some(cause) = self.sealed {
            return Ok(Some(Suppression::CohortSealed(cause)));
        }
        if let Cohort::Sealed { cause } = owner.cohort {
            return Ok(Some(Suppression::CohortSealed(cause)));
        }
        match owner.parent {
            ParentState::Failed { cause } => return Ok(Some(Suppression::ParentFailure(cause))),
            ParentState::LocallyComplete { cause } => {
                return Ok(Some(Suppression::CohortSealed(cause)));
            }
            _ => {}
        }
        if let Readiness::ArtifactFailed { artifact, cause } = owner.readiness {
            match self.target {
                Target::Artifact {
                    artifact: bound, ..
                } if bound == artifact => return Ok(Some(Suppression::ArtifactFailure(cause))),
                _ => return Err(ContractError::InvalidTarget),
            }
        }
        Ok(None)
    }

    pub fn begin(
        &self,
        principal: Principal,
        expected: &Binding,
        owner: &OwnerState,
    ) -> Result<Transition<'a>, ContractError> {
        self.check_frame(expected, owner)?;
        if self.state != State::Ready || self.begun || self.suppression.is_some() {
            return Err(ContractError::InvalidTransition);
        }
        if self.phase == Phase::Delivery {
            return Err(ContractError::InvalidTransition);
        }
        self.check_authority(owner, self.phase)?;
        let evaluator = self.declaration.policy(self.phase)?.evaluator;
        if matches!(self.target, Target::MissingSlot { .. })
            && principal == Principal::Actor(self.declaration.spec.issuer)
        {
            principal.require_actor(self.declaration.spec.issuer)?;
        } else {
            principal.require_actor(evaluator)?;
        }
        let mut next = *self;
        next.stored.binding = self.binding.next()?;
        if let Some(reason) = self.unbegun_suppression(owner)? {
            next.stored.suppression = Some(reason);
            return Ok(Transition { next, result: None });
        }
        self.check_readiness(owner.readiness)?;
        if matches!(self.target, Target::MissingSlot { .. }) {
            if self.mode() == ValidationMode::Observe {
                next.stored.suppression = Some(Suppression::MissingTarget);
                return Ok(Transition { next, result: None });
            }
            next.stored.state = State::ValidationIncomplete;
            let result = next.accepted(VerdictValue::Incomplete, Phase::MissingTarget, None, None);
            next.stored.last_result = Some(result);
            return Ok(Transition {
                next,
                result: Some(result),
            });
        }
        next.stored.begun = true;
        next.stored.state = match self.phase {
            Phase::Programmatic => State::Validating,
            Phase::Quality => State::ValidatingQualityBar,
            _ => return Err(ContractError::InvalidTransition),
        };
        Ok(Transition { next, result: None })
    }

    /// Pure delivery consumes the issuer's actual response-receipt fact. It never
    /// invokes a handler and cannot be used by an evidence-check requirement.
    pub fn receive_delivery(
        &self,
        principal: Principal,
        expected: &Binding,
        owner: &OwnerState,
    ) -> Result<Transition<'a>, ContractError> {
        self.check_frame(expected, owner)?;
        principal.require_actor(self.declaration.spec.issuer)?;
        if self.state != State::Ready || self.phase != Phase::Delivery || self.suppression.is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        self.check_authority(owner, Phase::Delivery)?;
        if self.unbegun_suppression(owner)?.is_some() {
            return Err(ContractError::InvalidTransition);
        }
        self.check_readiness(owner.readiness)?;
        let mut next = *self;
        next.stored.binding = self.binding.next()?;
        next.stored.state = State::Validated;
        let result = next.accepted(VerdictValue::Pass, Phase::Delivery, None, None);
        next.stored.last_result = Some(result);
        Ok(Transition {
            next,
            result: Some(result),
        })
    }

    /// Check the exact frame, begun attempt, installed authority, actor and
    /// report coordinates before allocating or inspecting result evidence.
    /// This does not change state or validate the artifact's identity/custody.
    pub fn authorize_report(
        &self,
        principal: Principal,
        expected: &Binding,
        owner: &OwnerState,
        report: Report,
    ) -> Result<ReportAuthorization, ContractError> {
        self.check_frame(expected, owner)?;
        let attempt = self.current_attempt()?;
        self.check_authority(owner, self.phase)?;
        principal.require_actor(attempt.evaluator)?;
        if report.generation != self.generation || report.attempt != attempt {
            return Err(ContractError::StaleEvaluation);
        }
        let policy = self.declaration.policy(self.phase)?;
        let step = policy
            .handlers
            .get(self.handler)
            .ok_or(ContractError::InvalidPolicy)?;
        let (kind, schema) = match report.value {
            VerdictValue::Pass | VerdictValue::Fail => (EvidenceKind::Proof, step.proof_schema),
            VerdictValue::Incomplete | VerdictValue::Error => {
                (EvidenceKind::Diagnostic, step.diagnostic_schema)
            }
        };
        Ok(ReportAuthorization {
            attempt,
            schema,
            kind,
        })
    }

    pub fn report(
        &self,
        principal: Principal,
        expected: &Binding,
        owner: &OwnerState,
        report: Report,
        evidence: &EvidenceFacts,
    ) -> Result<Transition<'a>, ContractError> {
        let authorization = self.authorize_report(principal, expected, owner, report)?;
        let attempt = authorization.attempt();
        // The checked declaration is immutable; these lookups cannot change
        // between preflight and transition and allocate no policy copies.
        let policy = self.declaration.policy(self.phase)?;
        let step = policy
            .handlers
            .get(self.handler)
            .ok_or(ContractError::InvalidPolicy)?;
        self.check_evidence(report, evidence, &authorization)?;
        let mut next = *self;
        next.stored.binding = self.binding.next()?;
        match report.value {
            VerdictValue::Pass => {
                if self.phase == Phase::Programmatic {
                    next.stored.programmatic_evidence = Some(report.evidence);
                    if matches!(
                        self.declaration.spec.program,
                        OwnedProgram::Programmatic {
                            quality: Some(_),
                            ..
                        }
                    ) {
                        next.stored.phase = Phase::Quality;
                        next.stored.handler = 0;
                        next.stored.handler_attempt = 0;
                        next.stored.attempt = self.next_attempt()?;
                        next.stored.state = State::ValidatingQualityBar;
                    } else {
                        next.stored.state = State::Validated;
                    }
                } else {
                    next.stored.state = State::Validated;
                }
            }
            VerdictValue::Fail => {
                next.stored.state = match (self.phase, self.mode()) {
                    (Phase::Programmatic, ValidationMode::Required) => State::ValidationFailed,
                    (Phase::Programmatic, ValidationMode::Observe) => {
                        State::ValidationFailedNotRequired
                    }
                    (Phase::Quality, ValidationMode::Required) => State::QualityBarValidationFailed,
                    (Phase::Quality, ValidationMode::Observe) => {
                        State::QualityBarValidationFailedNotRequired
                    }
                    _ => return Err(ContractError::InvalidTransition),
                };
            }
            VerdictValue::Incomplete => next.stored.state = State::ValidationIncomplete,
            VerdictValue::Error => {
                let local = self
                    .handler_attempt
                    .checked_add(1)
                    .ok_or(ContractError::Capacity)?;
                if local < step.attempts {
                    next.stored.handler_attempt = local;
                    next.stored.attempt = self.next_attempt()?;
                } else {
                    let handler = self.handler.checked_add(1).ok_or(ContractError::Capacity)?;
                    if handler < policy.handlers.len() {
                        next.stored.handler = handler;
                        next.stored.handler_attempt = 0;
                        next.stored.attempt = self.next_attempt()?;
                    } else {
                        next.stored.state = match self.mode() {
                            ValidationMode::Required => State::Errored,
                            ValidationMode::Observe => State::ErroredNotRequired,
                        };
                    }
                }
            }
        }
        let mut result = next.accepted(
            report.value,
            self.phase,
            Some(self.attempt),
            Some(report.evidence),
        );
        result.reporter = Some(attempt.evaluator);
        next.stored.last_result = Some(result);
        Ok(Transition {
            next,
            result: Some(result),
        })
    }

    fn next_attempt(&self) -> Result<u32, ContractError> {
        let next = self.attempt.checked_add(1).ok_or(ContractError::Capacity)?;
        if next >= self.declaration.attempts {
            return Err(ContractError::Capacity);
        }
        Ok(next)
    }

    fn check_evidence(
        &self,
        report: Report,
        evidence: &EvidenceFacts,
        authorization: &ReportAuthorization,
    ) -> Result<(), ContractError> {
        if evidence.binding.ledger != self.binding.ledger {
            return Err(ContractError::WrongLedger);
        }
        if evidence.binding.object != crate::ObjectId(report.evidence.id.0) {
            return Err(ContractError::WrongObject);
        }
        if evidence.binding.content != report.evidence.hash {
            return Err(ContractError::ContentConflict);
        }
        if evidence.claim != self.declaration.spec.claim
            || evidence.validation != ValidationId(self.binding.object.0)
            || evidence.target != self.target
        {
            return Err(ContractError::InvalidTarget);
        }
        if evidence.generation != self.generation || evidence.attempt != report.attempt {
            return Err(ContractError::StaleEvaluation);
        }
        if evidence.producer != report.attempt.evaluator {
            return Err(ContractError::WrongActor);
        }
        if evidence.value != report.value {
            return Err(ContractError::ContentConflict);
        }
        if !matches!(evidence.custody_revision, Some(revision) if revision > 0) {
            return Err(ContractError::MissingEvidence);
        }
        if (evidence.kind, evidence.schema) != (authorization.kind, authorization.schema) {
            return Err(ContractError::MissingEvidence);
        }
        Ok(())
    }

    fn accepted(
        &self,
        verdict: VerdictValue,
        phase: Phase,
        attempt: Option<u32>,
        evidence: Option<ArtifactRef>,
    ) -> AcceptedResult {
        AcceptedResult {
            definition: self.stored.definition,
            binding: self.binding,
            ledger: self.binding.ledger,
            claim: self.declaration.spec.claim,
            target: self.target,
            validation: ValidationId(self.binding.object.0),
            declaration_index: self.declaration.spec.declaration_index,
            mode: self.mode(),
            verdict,
            phase,
            attempt,
            generation: self.generation,
            receipt: self.receipt,
            evidence,
            programmatic_evidence: self.programmatic_evidence,
            reporter: None,
            resulting_state: self.state,
        }
    }

    /// Owner-derived cohort fact, not an external status-setting command. Sealing
    /// preserves begun chains; unbegun checks stay Ready with an explicit reason.
    pub fn record_seal(
        &self,
        expected: &Binding,
        owner: &OwnerState,
    ) -> Result<Self, ContractError> {
        self.check_frame(expected, owner)?;
        let Cohort::Sealed { cause } = owner.cohort else {
            return Err(ContractError::InvalidTransition);
        };
        let mut next = *self;
        next.stored = seal::record(self.stored, cause)?;
        Ok(next)
    }

    /// Owner-derived explicit authority/deadline fence. It closes the begun audit
    /// obligation without inventing Error, Fail, cancellation or result evidence.
    pub fn record_fence(
        &self,
        expected: &Binding,
        owner: &OwnerState,
    ) -> Result<Self, ContractError> {
        self.check_frame(expected, owner)?;
        let AuthorityState::Fenced(fence) = owner.authority.state else {
            return Err(ContractError::InvalidTransition);
        };
        if self.fence.is_some() || self.state.is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        if let FenceReason::Deadline(deadline) = fence.reason
            && (deadline != self.declaration.spec.deadline || owner.logical_time < deadline.at)
        {
            return Err(ContractError::StaleEvaluation);
        }
        let mut next = *self;
        next.stored.binding = self.binding.next()?;
        next.stored.fence = Some(fence);
        Ok(next)
    }
}

fn check_target(
    declaration: &Declaration,
    target: Target,
    slot_name: Option<&str>,
) -> Result<(), ContractError> {
    let ledger = declaration.spec.binding.ledger;
    let valid = |binding: Binding| binding.ledger == ledger && !binding.object.is_zero();
    let own_claim =
        |binding: Binding| valid(binding) && binding.object.0 == declaration.spec.claim.0;
    let matches = match (declaration.target(), target) {
        (
            TargetDeclaration::WholeWorkSlot { index, name },
            Target::Artifact {
                response,
                slot,
                artifact,
            },
        ) => index == slot && slot_name == Some(name) && valid(response) && valid(artifact),
        (
            TargetDeclaration::WholeWorkSlot { index, name },
            Target::MissingSlot { response, slot },
        ) => index == slot && slot_name == Some(name) && valid(response),
        (TargetDeclaration::Delivery, Target::Delivery { response }) => {
            slot_name.is_none() && valid(response)
        }
        (TargetDeclaration::Admission, Target::Admission { claim }) => {
            slot_name.is_none() && own_claim(claim)
        }
        (TargetDeclaration::Increment, Target::Increment { claim, artifact }) => {
            slot_name.is_none() && own_claim(claim) && valid(artifact)
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ContractError::InvalidTarget)
    }
}

#[cfg(test)]
#[path = "validation_tests.rs"]
pub(super) mod tests;

#[cfg(test)]
#[path = "validation_retention_tests.rs"]
mod retention_tests;

#[cfg(test)]
#[path = "validation_authorization_tests.rs"]
mod authorization_tests;
