//! Claim phase and authority contract. This is a semantic owner API, not a wire
//! command decoder. Response callbacks are private to the lifecycle owner and
//! consume facts verified by the response/manifest contract. Graph eligibility,
//! immutable standing policy and aggregation witnesses require owner validation
//! before publication; these methods do not invent those witnesses.
use super::evidence::{Diagnostic, Parent, ReportStamp, ResponseDiagnostic};
#[path = "claim_adoption.rs"]
mod adoption;
pub use adoption::ReceiptAdoption;
#[path = "claim_memory.rs"]
mod memory;
#[path = "claim_posting.rs"]
mod posting;
#[path = "claim_snapshot.rs"]
mod snapshot;
use super::{Binding, ContractError, Principal};
use super::{aggregation, graph, scope, succession, validation};
#[cfg(test)]
use crate::ClaimId;
use crate::{
    ClaimStatus, ContentHash, Deadline, EvidenceAttestation, ParticipantId, ReceiptFence,
    SessionSeq, TestamentId,
};
pub use snapshot::{
    ClaimHydrationPlan, ClaimResponseSnapshotV1, ClaimResponseSource, ClaimResponseValue,
    ClaimSnapshotV1, ClaimTerminalSnapshotV1,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptEntitlement {
    pub holder: ParticipantId,
    pub fence: ReceiptFence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimCut {
    pub position: SessionSeq,
    pub cause: ContentHash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimTerminalCut {
    Explicit(ClaimCut),
    Required(aggregation::TerminalCut),
    Graph(graph::TerminalCut),
}

impl ClaimCut {
    fn check(self) -> Result<(), ContractError> {
        if self.position.0 == 0 {
            Err(ContractError::InvalidCut)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseLink {
    pub testament: TestamentId,
    pub content: ContentHash,
    pub receipt: ReceiptFence,
    pub cycle: u32,
    pub prior: Option<TestamentId>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseRecord {
    link: ResponseLink,
    stamp: ReportStamp,
    posted: bool,
    received: bool,
}

/// Exact authored history, checked against the private report stamp. These
/// facts are observations of this claim's record, never participant input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::lifecycle) struct ResponseHistory {
    posted: bool,
    received: bool,
}

impl ResponseHistory {
    pub(in crate::lifecycle) fn posted(self) -> bool {
        self.posted
    }
    pub(in crate::lifecycle) fn received(self) -> bool {
        self.received
    }
}

/// Installed owner facts at one exact effective revision. These are resolved
/// from immutable standing/admission declarations and the current graph, never
/// accepted as participant-authored permission flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateState {
    Passed,
    Pending,
    Failed,
}
impl PredicateState {
    fn require_pass(self) -> Result<(), ContractError> {
        match self {
            Self::Passed => Ok(()),
            Self::Pending => Err(ContractError::InvalidTransition),
            Self::Failed => Err(ContractError::InvalidPolicy),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingStanding {
    pub binding: Binding,
    pub standing: PredicateState,
    pub target: PredicateState,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryFailure {
    Post,
    Receipt,
}

/// A real failure to assemble or publish a response is an incident, not a
/// fabricated response or a validation verdict. The owner must publish this
/// record atomically with its checked claim revision change. A subsequent
/// respondent-authored failure response remains an ordinary response cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosingIncident {
    expected: Binding,
    next: Binding,
    diagnostic: ResponseDiagnostic,
    cut: ClaimCut,
}
impl ClosingIncident {
    pub fn claim(&self) -> Binding {
        self.expected
    }
    pub fn diagnostic(&self) -> &ResponseDiagnostic {
        &self.diagnostic
    }
    pub fn cut(&self) -> ClaimCut {
        self.cut
    }
}

/// Actor intents cannot select an aggregate status or impersonate an evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimIntent {
    Post {
        standing: PostingStanding,
    },
    Progress {
        receipt: ReceiptFence,
    },
    AdoptReceipt {
        previous: ReceiptFence,
        replacement: ReceiptEntitlement,
    },
    Cancel {
        cut: ClaimCut,
    },
    Revoke {
        cut: ClaimCut,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseEvent {
    Generated,
    Posted,
    Received,
}

/// Only lifecycle-owner reduction can supply these verified cross-object facts.
/// They are deliberately not Actor intents or an arbitrary requested status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DerivedClaimFact {
    EvaluatorBegan { receipt: ReceiptFence },
    LocalCompletion { sequence: SessionSeq },
    GraphSatisfied { cut: ClaimCut },
    PostFailed { cut: ClaimCut },
    ReceiptFailed { cut: ClaimCut },
}

/// Complete immutable generation input. Graph, lineage and acceptance cannot be
/// replaced by a partial view after generation. Creation sequence is owner-assigned.
#[derive(Debug)]
pub struct ClaimDefinition {
    pub binding: Binding,
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub deadline: Option<Deadline>,
    pub max_responses: u32,
    pub created: SessionSeq,
    pub graph: graph::Declaration,
    pub lineage: succession::Lineage,
    pub acceptance: aggregation::AcceptancePolicy,
    pub scope_limits: scope::ScopeLimits,
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub struct ClaimState {
    binding: Binding,
    issuer: ParticipantId,
    subject: ParticipantId,
    created: SessionSeq,
    graph: graph::Declaration,
    lineage: succession::Lineage,
    acceptance: aggregation::AcceptancePolicy,
    scopes: scope::Registry,
    status: ClaimStatus,
    receipt: Option<ReceiptEntitlement>,
    responses: Vec<ResponseRecord>,
    max_responses: u32,
    deadline: Option<Deadline>,
    local_complete: bool,
    local_sealed_at: Option<SessionSeq>,
    terminal_cut: Option<ClaimTerminalCut>,
}

impl ClaimState {
    /// Historical contract fixtures may deliberately construct malformed rows.
    /// Production construction is exclusively through `creation::CreationPlan`.
    #[cfg(test)]
    pub fn generate(
        principal: Principal,
        definition: ClaimDefinition,
    ) -> Result<Self, ContractError> {
        if !matches!(definition.lineage.cause(), crate::Cause::Root(_)) {
            return Err(ContractError::InvalidTarget);
        }
        Self::generate_defined(principal, definition)
    }

    pub(super) fn generate_defined(
        principal: Principal,
        definition: ClaimDefinition,
    ) -> Result<Self, ContractError> {
        let ClaimDefinition {
            binding,
            issuer,
            subject,
            deadline,
            max_responses,
            created,
            graph,
            lineage,
            acceptance,
            scope_limits,
        } = definition;
        principal.require_actor(issuer)?;
        if issuer.is_zero()
            || subject.is_zero()
            || binding.object.is_zero()
            || binding.ledger.tenant.is_zero()
            || binding.ledger.session.is_zero()
        {
            return Err(ContractError::InvalidTarget);
        }
        if created.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        if max_responses == 0 {
            return Err(ContractError::Capacity);
        }
        if deadline.is_some_and(|value| value.timer.is_zero() || value.generation == 0) {
            return Err(ContractError::InvalidTarget);
        }
        lineage.check_binding(&binding)?;
        acceptance.check(binding, issuer)?;
        let scopes = scope::Registry::new(binding, scope_limits)?;
        Ok(Self {
            binding,
            issuer,
            subject,
            created,
            graph,
            lineage,
            acceptance,
            scopes,
            status: ClaimStatus::Generated,
            receipt: None,
            responses: Vec::new(),
            max_responses,
            deadline,
            local_complete: false,
            local_sealed_at: None,
            terminal_cut: None,
        })
    }
    #[cfg(test)]
    pub fn generate_child(
        &mut self,
        expected: &Binding,
        principal: Principal,
        receipt: Option<ReceiptFence>,
        definition: ClaimDefinition,
        cut: ClaimCut,
    ) -> Result<Self, ContractError> {
        self.open(expected)?;
        principal.require_actor(definition.issuer)?;
        if definition.lineage.cause() != &crate::Cause::Claim(ClaimId(self.binding.object.0))
            || definition.created != cut.position
        {
            return Err(ContractError::InvalidTarget);
        }
        if principal != Principal::Actor(self.issuer) {
            let receipt = receipt.ok_or(ContractError::StaleReceipt)?;
            principal.require_actor(self.receipt_matches(receipt)?.holder)?;
        } else if let Some(receipt) = receipt {
            self.receipt_matches(receipt)?;
        }
        let child = Self::generate_defined(principal, definition)?;
        let transition = scope::Registry::prepare_child(self, &child, expected, receipt, cut)?;
        self.apply_scope(expected, transition, &[&child])?;
        Ok(child)
    }

    pub fn apply_scope(
        &mut self,
        expected: &Binding,
        transition: scope::Transition,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.binding.check(expected)?;
        transition.check(self, peers)?;
        let binding = self.binding.next()?;
        transition.install(&mut self.scopes);
        self.binding = binding;
        Ok(())
    }

    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn issuer(&self) -> ParticipantId {
        self.issuer
    }
    pub fn subject(&self) -> ParticipantId {
        self.subject
    }
    pub fn created(&self) -> SessionSeq {
        self.created
    }
    pub fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }
    pub fn graph(&self) -> &graph::Declaration {
        &self.graph
    }
    pub fn lineage(&self) -> &succession::Lineage {
        &self.lineage
    }
    pub fn acceptance(&self) -> &aggregation::AcceptancePolicy {
        &self.acceptance
    }
    pub fn scopes(&self) -> &scope::Registry {
        &self.scopes
    }
    pub fn released(&self) -> bool {
        self.scopes.released()
    }
    pub fn status(&self) -> ClaimStatus {
        self.status
    }
    /// Native claim terminality; this does not couple independent object
    /// receipt or audit transitions to the historical V1 status helpers.
    pub fn is_terminal(&self) -> bool {
        terminal(self.status)
    }
    pub fn receipt(&self) -> Option<ReceiptEntitlement> {
        self.receipt
    }
    pub fn local_complete(&self) -> bool {
        self.local_complete
    }
    /// First local outcome cut, retained when graph release or a later control
    /// makes an already locally complete claim terminal.
    pub fn local_sealed_at(&self) -> Option<SessionSeq> {
        self.local_sealed_at
    }
    pub fn terminal_cut(&self) -> Option<ClaimTerminalCut> {
        self.terminal_cut
    }
    pub fn latest_response(&self) -> Option<ResponseLink> {
        self.responses.last().map(|row| row.link)
    }
    pub fn response_count(&self) -> usize {
        self.responses.len()
    }
    pub fn max_responses(&self) -> u32 {
        self.max_responses
    }

    fn open(&self, expected: &Binding) -> Result<(), ContractError> {
        self.binding.check(expected)?;
        if terminal(self.status) {
            Err(ContractError::InvalidTransition)
        } else {
            Ok(())
        }
    }
    fn working(&self) -> Result<(), ContractError> {
        if self.local_complete {
            Err(ContractError::InvalidTransition)
        } else {
            Ok(())
        }
    }
    fn receipt_matches(&self, receipt: ReceiptFence) -> Result<ReceiptEntitlement, ContractError> {
        self.receipt
            .filter(|current| current.fence == receipt)
            .ok_or(ContractError::StaleReceipt)
    }
    fn terminalize(&mut self, status: ClaimStatus, cut: ClaimCut) -> Result<(), ContractError> {
        cut.check()?;
        if cut.position < self.created {
            return Err(ContractError::InvalidCut);
        }
        if self
            .local_sealed_at
            .is_some_and(|sealed| cut.position < sealed)
        {
            return Err(ContractError::InvalidCut);
        }
        let binding = self.binding.next()?;
        self.binding = binding;
        self.status = status;
        self.local_sealed_at.get_or_insert(cut.position);
        self.terminal_cut = Some(ClaimTerminalCut::Explicit(cut));
        Ok(())
    }

    pub(super) fn cancellation_binding(
        &self,
        cut: ClaimCut,
    ) -> Result<Option<Binding>, ContractError> {
        cut.check()?;
        if cut.position < self.created
            || self
                .local_sealed_at
                .is_some_and(|sealed| cut.position < sealed)
        {
            return Err(ContractError::InvalidCut);
        }
        if terminal(self.status) {
            Ok(None)
        } else {
            self.binding.next().map(Some)
        }
    }

    /// Apply derived ownership authority to a copied current row. The Core owner
    /// publishes the complete checked plan atomically; this releases no scope.
    pub fn apply_cancellation(
        &mut self,
        cancellation: &super::ownership::Cancellation<'_>,
    ) -> Result<(), ContractError> {
        cancellation.check(self)?;
        if let Some(binding) = cancellation.next_binding() {
            let cut = cancellation.cut();
            self.binding = binding;
            self.status = ClaimStatus::Cancelled;
            self.local_sealed_at.get_or_insert(cut.position);
            self.terminal_cut = Some(ClaimTerminalCut::Explicit(cut));
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        expected: &Binding,
        principal: Principal,
        intent: ClaimIntent,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        match intent {
            ClaimIntent::Cancel { cut } | ClaimIntent::Revoke { cut } => {
                principal.require_actor(self.issuer)?;
                if matches!(intent, ClaimIntent::Cancel { .. })
                    && !self.scopes.children().is_empty()
                {
                    return Err(ContractError::InvalidTransition);
                }
                let status = if matches!(intent, ClaimIntent::Cancel { .. }) {
                    ClaimStatus::Cancelled
                } else {
                    ClaimStatus::Revoked
                };
                self.terminalize(status, cut)
            }
            ClaimIntent::Post { standing } => {
                principal.require_actor(self.issuer)?;
                self.binding.check(&standing.binding)?;
                standing.standing.require_pass()?;
                standing.target.require_pass()?;
                if self.status != ClaimStatus::Generated {
                    return Err(ContractError::InvalidTransition);
                }
                let binding = self.binding.next()?;
                self.binding = binding;
                self.status = ClaimStatus::Posted;
                Ok(())
            }
            ClaimIntent::Progress { receipt } => {
                self.working()?;
                let entitlement = self.receipt_matches(receipt)?;
                principal.require_actor(entitlement.holder)?;
                if !matches!(self.status, ClaimStatus::Received | ClaimStatus::Progressed) {
                    return Err(ContractError::InvalidTransition);
                }
                let binding = self.binding.next()?;
                self.binding = binding;
                self.status = ClaimStatus::Progressed;
                Ok(())
            }
            ClaimIntent::AdoptReceipt {
                previous,
                replacement,
            } => {
                let binding = self.adoption_binding(principal, previous, replacement)?;
                self.binding = binding;
                self.receipt = Some(replacement);
                Ok(())
            }
        }
    }

    pub(super) fn check_increment_entry(
        &self,
        decision: &aggregation::ClaimDecision<'_>,
    ) -> Result<(), ContractError> {
        self.binding.check(&decision.binding())?;
        if decision.acceptance() != &self.acceptance {
            return Err(ContractError::InvalidPolicy);
        }
        if !decision.increments_ready() {
            return Err(ContractError::InvalidTransition);
        }
        for witness in decision.nonartifact_witnesses() {
            if matches!(
                witness.result().target(),
                validation::Target::Increment { .. }
            ) {
                self.receipt_matches(witness.receipt().ok_or(ContractError::StaleReceipt)?)?;
            }
        }
        Ok(())
    }

    pub fn request_evaluation(
        &mut self,
        expected: &Binding,
        principal: Principal,
        decision: &aggregation::ClaimDecision<'_>,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        self.working()?;
        principal.require_actor(self.issuer)?;
        self.check_increment_entry(decision)?;
        if self.status != ClaimStatus::TestamentAcknowledged {
            return Err(ContractError::InvalidTransition);
        }
        self.current_delivery()?;
        let binding = self.binding.next()?;
        self.binding = binding;
        self.status = ClaimStatus::Validating;
        Ok(())
    }

    pub fn acquire_receipt(
        &mut self,
        expected: &Binding,
        principal: Principal,
        fence: ReceiptFence,
        admission: &aggregation::AdmissionDecision<'_>,
        start: &graph::Start<'_>,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        principal.require_actor(self.subject)?;
        self.binding.check(&admission.binding())?;
        if admission.acceptance() != &self.acceptance {
            return Err(ContractError::InvalidPolicy);
        }
        if admission.outcome() != aggregation::AdmissionOutcome::Passed {
            return Err(ContractError::InvalidTransition);
        }
        start.check(self, peers)?;
        if self.status != ClaimStatus::Posted || self.receipt.is_some() {
            return Err(ContractError::InvalidTransition);
        }
        if fence.receipt.is_zero() || fence.epoch == 0 {
            return Err(ContractError::StaleReceipt);
        }
        let binding = self.binding.next()?;
        self.binding = binding;
        self.receipt = Some(ReceiptEntitlement {
            holder: self.subject,
            fence,
        });
        self.status = ClaimStatus::Received;
        Ok(())
    }

    pub fn apply_admission(
        &mut self,
        expected: &Binding,
        decision: &aggregation::AdmissionDecision<'_>,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        self.binding.check(&decision.binding())?;
        if decision.acceptance() != &self.acceptance {
            return Err(ContractError::InvalidPolicy);
        }
        if self.status != ClaimStatus::Posted {
            return Err(ContractError::InvalidTransition);
        }
        match decision.outcome() {
            aggregation::AdmissionOutcome::Pending | aggregation::AdmissionOutcome::Passed => {
                Ok(())
            }
            aggregation::AdmissionOutcome::Blocked(cut) => {
                if cut.sequence().0 == 0
                    || cut.sequence() < self.created
                    || !matches!(
                        cut.cause().key().target,
                        aggregation::CauseTarget::Admission
                    )
                {
                    return Err(ContractError::InvalidCut);
                }
                let binding = self.binding.next()?;
                self.binding = binding;
                self.status = ClaimStatus::PostFailed;
                self.local_sealed_at = Some(cut.sequence());
                self.terminal_cut = Some(ClaimTerminalCut::Required(cut));
                Ok(())
            }
        }
    }

    pub(super) fn supersede_verified(
        &mut self,
        expected: &Binding,
        cut: ClaimCut,
    ) -> Result<(), ContractError> {
        self.binding.check(expected)?;
        if terminal(self.status) {
            return Ok(());
        }
        self.terminalize(ClaimStatus::Superseded, cut)
    }

    fn current_delivery(&self) -> Result<(), ContractError> {
        let receipt = self.receipt.ok_or(ContractError::StaleReceipt)?;
        if self
            .responses
            .iter()
            .any(|row| row.received && row.link.receipt == receipt.fence)
        {
            Ok(())
        } else {
            Err(ContractError::StaleReceipt)
        }
    }

    /// A real typed diagnostic, verified durable by trusted custody ingress,
    /// supports a narrowly scoped participant report. It does not confer a
    /// generic ability to fail unrelated phases or act for another participant.
    pub fn report_boundary_failure(
        &mut self,
        expected: &Binding,
        principal: Principal,
        boundary: BoundaryFailure,
        diagnostic: Diagnostic,
        custody: &EvidenceAttestation,
        cut: ClaimCut,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        if diagnostic.artifact.id.is_zero()
            || custody.custody_revision == 0
            || !custody.durable
            || !custody.schema_valid
            || custody.descriptor_hash != diagnostic.artifact.hash
        {
            return Err(ContractError::MissingEvidence);
        }
        let fact = match boundary {
            BoundaryFailure::Post => {
                principal.require_actor(self.issuer)?;
                DerivedClaimFact::PostFailed { cut }
            }
            BoundaryFailure::Receipt => {
                principal.require_actor(self.subject)?;
                DerivedClaimFact::ReceiptFailed { cut }
            }
        };
        self.apply_derived(expected, fact)
    }

    /// Checks an incident before any mutation. Work failure itself is reported
    /// through Response::close; this path is for trouble producing that report.
    pub fn plan_closing_failure(
        &self,
        expected: &Binding,
        principal: Principal,
        diagnostic: ResponseDiagnostic,
        cut: ClaimCut,
    ) -> Result<ClosingIncident, ContractError> {
        self.open(expected)?;
        self.working()?;
        let parent = Parent::from_claim(self)?;
        parent.require_open_response()?;
        principal.require_actor(parent.holder)?;
        diagnostic.check_parent(&parent)?;
        cut.check()?;
        if cut.position < self.created {
            return Err(ContractError::InvalidCut);
        }
        Ok(ClosingIncident {
            expected: self.binding,
            next: self.binding.next()?,
            diagnostic,
            cut,
        })
    }

    /// The incident and its diagnostic must be retained by the same owner
    /// publication. Status, response collection and terminal cut stay unchanged.
    pub fn apply_closing_incident(
        &mut self,
        incident: &ClosingIncident,
    ) -> Result<(), ContractError> {
        self.open(&incident.expected)?;
        self.working()?;
        incident
            .diagnostic
            .check_parent(&Parent::from_claim(self)?)?;
        self.binding = incident.next;
        Ok(())
    }

    /// Consume the least-fixpoint proof against all exact effective read rows.
    pub fn graph_release(
        &mut self,
        expected: &Binding,
        witness: &graph::Release<'_>,
        peers: &[&ClaimState],
        sequence: SessionSeq,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        witness.check(self, peers)?;
        witness.check_cut(sequence)?;
        let cut = ClaimCut {
            position: sequence,
            cause: witness.fingerprint()?,
        };
        self.apply_derived(expected, DerivedClaimFact::GraphSatisfied { cut })
    }

    pub fn dependency_failed(
        &mut self,
        expected: &Binding,
        witness: &graph::DependencyFailure<'_>,
        peers: &[&ClaimState],
        sequence: SessionSeq,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        witness.check(self, peers)?;
        self.graph_terminalize(witness.cut(sequence)?)
    }

    pub fn break_deadlock(
        &mut self,
        expected: &Binding,
        witness: &graph::Deadlock<'_>,
        peers: &[&ClaimState],
        sequence: SessionSeq,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        witness.check(self, peers)?;
        self.graph_terminalize(witness.cut(sequence)?)
    }

    fn graph_terminalize(&mut self, cut: graph::TerminalCut) -> Result<(), ContractError> {
        if cut.sequence() < self.created
            || self
                .local_sealed_at
                .is_some_and(|sealed| cut.sequence() < sealed)
        {
            return Err(ContractError::InvalidCut);
        }
        let binding = self.binding.next()?;
        self.binding = binding;
        self.status = match cut.kind() {
            graph::FailureKind::DependencyFailed => ClaimStatus::DependencyFailed,
            graph::FailureKind::Deadlocked => ClaimStatus::Deadlocked,
        };
        self.local_sealed_at.get_or_insert(cut.sequence());
        self.terminal_cut = Some(ClaimTerminalCut::Graph(cut));
        Ok(())
    }

    /// A begun evaluation is private-construction evidence of the designated
    /// participant's authorized begin, not a caller-requested claim status.
    pub fn observe_evaluation(
        &mut self,
        expected: &Binding,
        evaluation: &validation::Evaluation<'_>,
        decision: &aggregation::ClaimDecision<'_>,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        if evaluation.ledger() != self.binding.ledger {
            return Err(ContractError::WrongLedger);
        }
        if evaluation.claim().0 != self.binding.object.0 {
            return Err(ContractError::WrongObject);
        }
        if evaluation.declared_phase() != crate::ValidationPhase::WholeWork
            || !evaluation.has_begun()
            || evaluation.fence().is_some()
            || evaluation.sealed().is_some()
            || !matches!(
                evaluation.state(),
                validation::State::Validating | validation::State::ValidatingQualityBar
            )
        {
            return Err(ContractError::InvalidTransition);
        }
        self.check_increment_entry(decision)?;
        let receipt = evaluation.receipt().ok_or(ContractError::StaleReceipt)?;
        self.receipt_matches(receipt)?;
        let response = match evaluation.target() {
            validation::Target::Artifact { response, .. } => response,
            _ => return Err(ContractError::InvalidTarget),
        };
        self.received_response(response, receipt)?;
        if self.status == ClaimStatus::Validating {
            self.working()?;
            return Ok(());
        }
        self.apply_derived(expected, DerivedClaimFact::EvaluatorBegan { receipt })
    }

    fn received_response(
        &self,
        response: Binding,
        receipt: ReceiptFence,
    ) -> Result<(), ContractError> {
        if response.ledger != self.binding.ledger {
            return Err(ContractError::WrongLedger);
        }
        self.receipt_matches(receipt)?;
        if self.responses.iter().any(|row| {
            row.received
                && row.link.testament.0 == response.object.0
                && row.link.content == response.content
                && row.link.receipt == receipt
        }) {
            Ok(())
        } else {
            Err(ContractError::InvalidTarget)
        }
    }

    pub(super) fn received_report(
        &self,
        response: Binding,
        receipt: ReceiptFence,
        stamp: ReportStamp,
    ) -> Result<(), ContractError> {
        self.received_response(response, receipt)?;
        let row = self
            .responses
            .iter()
            .find(|row| row.link.testament.0 == response.object.0)
            .ok_or(ContractError::InvalidTarget)?;
        if row.stamp != stamp {
            return Err(ContractError::ContentConflict);
        }
        Ok(())
    }

    /// Check the complete immutable response link without treating a later
    /// receipt observation as acceptance. Old receipt history remains readable
    /// after adoption; the projection separately checks current eligibility.
    pub(in crate::lifecycle) fn response_history(
        &self,
        response: &super::evidence::Response,
    ) -> Result<ResponseHistory, ContractError> {
        let identity = response.identity();
        if identity.binding.ledger != self.binding.ledger {
            return Err(ContractError::WrongLedger);
        }
        if identity.claim.0 != self.binding.object.0 {
            return Err(ContractError::WrongObject);
        }
        let index = usize::try_from(identity.cycle)
            .map_err(|_| ContractError::Capacity)?
            .checked_sub(1)
            .ok_or(ContractError::InvalidTarget)?;
        let record = self
            .responses
            .get(index)
            .ok_or(ContractError::InvalidTarget)?;
        let link = ResponseLink {
            testament: TestamentId(identity.binding.object.0),
            content: identity.binding.content,
            receipt: identity.receipt,
            cycle: identity.cycle,
            prior: identity.prior,
        };
        if record.link != link {
            return Err(ContractError::InvalidTarget);
        }
        if record.stamp != response.report_stamp() {
            return Err(ContractError::ContentConflict);
        }
        Ok(ResponseHistory {
            posted: record.posted,
            received: record.received,
        })
    }

    /// Check this exact immutable report against the recorded cycle and return
    /// its posting and receipt observations. This grants no current authority;
    /// old-receipt history remains inspectable after adoption.
    pub fn recorded_response(
        &self,
        response: &super::evidence::Response,
    ) -> Result<(bool, bool), ContractError> {
        let history = self.response_history(response)?;
        Ok((history.posted, history.received))
    }

    /// Apply only a decision produced by the checked aggregate. The token pins
    /// exact acceptance/terminal evidence; the owner publishes both records in
    /// the same commit. This does not satisfy graph predicates on its own.
    pub fn apply_aggregate(
        &mut self,
        expected: &Binding,
        decision: &aggregation::ClaimDecision<'_>,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        self.working()?;
        self.binding.check(&decision.binding())?;
        if decision.acceptance() != &self.acceptance {
            return Err(ContractError::InvalidPolicy);
        }
        for witness in decision.nonartifact_witnesses() {
            let result = witness.result();
            if result.ledger() != self.binding.ledger || result.claim().0 != self.binding.object.0 {
                return Err(ContractError::InvalidTarget);
            }
            match result.target() {
                validation::Target::Admission { claim } => {
                    if claim.ledger != self.binding.ledger
                        || claim.object != self.binding.object
                        || claim.content != self.binding.content
                        || witness.receipt().is_some()
                    {
                        return Err(ContractError::InvalidTarget);
                    }
                }
                validation::Target::Increment { claim, artifact } => {
                    if claim.ledger != self.binding.ledger
                        || claim.object != self.binding.object
                        || claim.content != self.binding.content
                        || artifact.ledger != self.binding.ledger
                    {
                        return Err(ContractError::InvalidTarget);
                    }
                    self.receipt_matches(witness.receipt().ok_or(ContractError::StaleReceipt)?)?;
                }
                _ => return Err(ContractError::InvalidTarget),
            }
        }
        if self.status != ClaimStatus::Validating {
            return Err(ContractError::InvalidTransition);
        }
        if let Some(delivery) = decision.delivery() {
            self.received_response(delivery.response(), delivery.receipt())?;
        }
        for witness in decision.witnesses() {
            let row = self
                .responses
                .iter()
                .find(|row| row.link.testament == witness.response())
                .ok_or(ContractError::InvalidTarget)?;
            if !row.received {
                return Err(ContractError::InvalidTarget);
            }
            self.receipt_matches(row.link.receipt)?;
        }
        match decision.outcome() {
            aggregation::AggregateOutcome::Pending => Ok(()),
            aggregation::AggregateOutcome::LocalComplete { sequence } => {
                if sequence.0 == 0 || sequence < self.created {
                    return Err(ContractError::InvalidCut);
                }
                if decision.delivery().is_none() {
                    return Err(ContractError::MissingEvidence);
                }
                self.apply_derived(expected, DerivedClaimFact::LocalCompletion { sequence })
            }
            aggregation::AggregateOutcome::Blocked(cut) => {
                if cut.sequence().0 == 0 || cut.sequence() < self.created {
                    return Err(ContractError::InvalidCut);
                }
                match cut.cause().key().target {
                    aggregation::CauseTarget::Response(response) => {
                        let row = self.responses.iter().find(|row| row.link.testament == response).ok_or(ContractError::InvalidTarget)?;
                        if !row.received { return Err(ContractError::InvalidTarget); }
                        self.receipt_matches(row.link.receipt)?;
                    }
                    aggregation::CauseTarget::Admission => return Err(ContractError::InvalidTransition),
                    aggregation::CauseTarget::Increment { artifact, content } => {
                        if !decision.nonartifact_witnesses().iter().any(|witness| matches!(witness.result().target(), validation::Target::Increment { artifact: target, .. } if target.object.0 == artifact.0 && target.content == content)) { return Err(ContractError::MissingEvidence); }
                    }
                }
                let status = match cut.cause().kind() {
                    aggregation::BlockingKind::Incomplete => ClaimStatus::ValidationIncomplete,
                    aggregation::BlockingKind::Failed => ClaimStatus::ValidationFailed,
                    aggregation::BlockingKind::Errored => ClaimStatus::ValidationErrored,
                };
                let binding = self.binding.next()?;
                self.binding = binding;
                self.status = status;
                self.local_sealed_at = Some(cut.sequence());
                self.terminal_cut = Some(ClaimTerminalCut::Required(cut));
                Ok(())
            }
        }
    }

    /// Matching committed deadline facts come from the owner, never a Node's
    /// enrollment or an external wall-clock observation. No Actor can request a
    /// generic failure status through this operation.
    pub fn expire(
        &mut self,
        expected: &Binding,
        deadline: Deadline,
        fired_at: u64,
        cut: ClaimCut,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        if self.deadline != Some(deadline) || fired_at < deadline.at {
            return Err(ContractError::InvalidCut);
        }
        self.terminalize(ClaimStatus::Expired, cut)
    }

    /// Expire only from an exact due monitor and a checked absence of a wait
    /// SCC. Its deadline may precede the claim's own immutable deadline.
    pub fn expire_monitor(
        &mut self,
        expected: &Binding,
        expiry: &scope::MonitorExpiry<'_>,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        expiry.check(self, peers)?;
        self.terminalize(ClaimStatus::Expired, expiry.cut())
    }

    pub(super) fn record_response(
        &mut self,
        expected: &Binding,
        principal: Principal,
        link: ResponseLink,
        stamp: ReportStamp,
        event: ResponseEvent,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        self.working()?;
        let entitlement = self.receipt_matches(link.receipt)?;
        match event {
            ResponseEvent::Generated | ResponseEvent::Posted => {
                principal.require_actor(entitlement.holder)?
            }
            ResponseEvent::Received => principal.require_actor(self.issuer)?,
        }
        let binding = self.binding.next()?;
        match event {
            ResponseEvent::Generated => {
                let cycle = u32::try_from(self.responses.len())
                    .map_err(|_| ContractError::Capacity)?
                    .checked_add(1)
                    .ok_or(ContractError::Capacity)?;
                if cycle > self.max_responses {
                    return Err(ContractError::Capacity);
                }
                if link.testament.is_zero()
                    || link.cycle != cycle
                    || link.prior != self.latest_response().map(|row| row.testament)
                    || self
                        .responses
                        .iter()
                        .any(|row| row.link.testament == link.testament)
                {
                    return Err(ContractError::InvalidTarget);
                }
                if (cycle == 1
                    && !matches!(self.status, ClaimStatus::Received | ClaimStatus::Progressed))
                    || (cycle != 1
                        && !matches!(
                            self.status,
                            ClaimStatus::TestamentGenerated
                                | ClaimStatus::TestamentAcknowledged
                                | ClaimStatus::Validating
                        ))
                {
                    return Err(ContractError::InvalidTransition);
                }
                self.responses
                    .try_reserve(1)
                    .map_err(|_| ContractError::Capacity)?;
                self.responses.push(ResponseRecord {
                    link,
                    stamp,
                    posted: false,
                    received: false,
                });
                if cycle == 1 {
                    self.status = ClaimStatus::TestamentGenerated;
                }
            }
            ResponseEvent::Posted | ResponseEvent::Received => {
                // Delivery order is independent of authored cycle order. The
                // first actual receipt under the checked current entitlement
                // advances the pending phase; other response histories remain
                // unchanged, including an earlier still-unreceived response.
                let row = self
                    .responses
                    .iter_mut()
                    .find(|row| row.link.testament == link.testament)
                    .ok_or(ContractError::InvalidTarget)?;
                if row.link != link {
                    return Err(ContractError::InvalidTarget);
                }
                if row.stamp != stamp {
                    return Err(ContractError::ContentConflict);
                }
                match event {
                    ResponseEvent::Posted if !row.posted && !row.received => {
                        row.posted = true;
                    }
                    ResponseEvent::Received if row.posted && !row.received => {
                        row.received = true;
                        if self.status == ClaimStatus::TestamentGenerated {
                            self.status = ClaimStatus::TestamentAcknowledged;
                        }
                    }
                    _ => return Err(ContractError::InvalidTransition),
                }
            }
        }
        self.binding = binding;
        Ok(())
    }

    pub(super) fn apply_derived(
        &mut self,
        expected: &Binding,
        fact: DerivedClaimFact,
    ) -> Result<(), ContractError> {
        self.open(expected)?;
        match fact {
            DerivedClaimFact::EvaluatorBegan { receipt } => {
                self.working()?;
                self.receipt_matches(receipt)?;
                if self.status != ClaimStatus::TestamentAcknowledged {
                    return Err(ContractError::InvalidTransition);
                }
                self.current_delivery()?;
                let binding = self.binding.next()?;
                self.binding = binding;
                self.status = ClaimStatus::Validating;
                Ok(())
            }
            DerivedClaimFact::LocalCompletion { sequence } => {
                self.working()?;
                if sequence.0 == 0 || sequence < self.created {
                    return Err(ContractError::InvalidCut);
                }
                if self.status != ClaimStatus::Validating {
                    return Err(ContractError::InvalidTransition);
                }
                let binding = self.binding.next()?;
                self.binding = binding;
                self.local_complete = true;
                self.local_sealed_at = Some(sequence);
                Ok(())
            }
            DerivedClaimFact::GraphSatisfied { cut } => {
                if self.status != ClaimStatus::Validating || !self.local_complete {
                    return Err(ContractError::InvalidTransition);
                }
                self.terminalize(ClaimStatus::Satisfied, cut)
            }
            DerivedClaimFact::PostFailed { cut } => {
                if !matches!(self.status, ClaimStatus::Generated | ClaimStatus::Posted) {
                    return Err(ContractError::InvalidTransition);
                }
                self.terminalize(ClaimStatus::PostFailed, cut)
            }
            DerivedClaimFact::ReceiptFailed { cut } => {
                if self.status != ClaimStatus::Posted {
                    return Err(ContractError::InvalidTransition);
                }
                self.terminalize(ClaimStatus::ReceiptFailed, cut)
            }
        }
    }
}

fn terminal(status: ClaimStatus) -> bool {
    match status {
        ClaimStatus::Generated
        | ClaimStatus::Posted
        | ClaimStatus::Received
        | ClaimStatus::Progressed
        | ClaimStatus::TestamentGenerated
        | ClaimStatus::TestamentAcknowledged
        | ClaimStatus::Validating => false,
        ClaimStatus::Satisfied
        | ClaimStatus::PostFailed
        | ClaimStatus::ReceiptFailed
        | ClaimStatus::TestamentGenerationFailed
        | ClaimStatus::ValidationIncomplete
        | ClaimStatus::ValidationFailed
        | ClaimStatus::ValidationErrored
        | ClaimStatus::Cancelled
        | ClaimStatus::Expired
        | ClaimStatus::Revoked
        | ClaimStatus::Superseded
        | ClaimStatus::DependencyFailed
        | ClaimStatus::Deadlocked => true,
    }
}

#[cfg(test)]
#[path = "claim_tests.rs"]
pub(super) mod tests;
