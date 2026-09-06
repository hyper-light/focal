//! Work evidence and response delivery progress independently of claim status.
//!
//! Inputs describing a parent and its evidence set are effective owner views, not
//! client-authored permission claims. Closing returns one bounded publication
//! plan: no caller may publish its response without all attachment replacements.

use super::{Binding, ContractError, Principal};
use crate::{
    ArtifactRef, ClaimId, ClaimStatus, EvidenceAttestation, LedgerId, ParticipantId, ReceiptFence,
    TestamentId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkArtifactState {
    Generated,
    GenerationFailed,
    Received,
    ReceiptFailed,
    Attached,
    Validating,
    Validated,
    ValidationFailed,
}

impl WorkArtifactState {
    pub const ALL: &'static [Self] = &[
        Self::Generated,
        Self::GenerationFailed,
        Self::Received,
        Self::ReceiptFailed,
        Self::Attached,
        Self::Validating,
        Self::Validated,
        Self::ValidationFailed,
    ];
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::GenerationFailed | Self::ReceiptFailed | Self::Validated | Self::ValidationFailed
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseState {
    Generated,
    Posted,
    Received,
    Validating,
    Validated,
    ValidationIncomplete,
    ValidationFailed,
    ValidationErrored,
}

impl ResponseState {
    pub const ALL: &'static [Self] = &[
        Self::Generated,
        Self::Posted,
        Self::Received,
        Self::Validating,
        Self::Validated,
        Self::ValidationIncomplete,
        Self::ValidationFailed,
        Self::ValidationErrored,
    ];
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Validated
                | Self::ValidationIncomplete
                | Self::ValidationFailed
                | Self::ValidationErrored
        )
    }
    pub const fn delivered(self) -> bool {
        matches!(
            self,
            Self::Received
                | Self::Validating
                | Self::Validated
                | Self::ValidationIncomplete
                | Self::ValidationFailed
                | Self::ValidationErrored
        )
    }
}

/// Effective claim entitlement. Owner code resolves this from committed/pending
/// claim state; it must not be deserialized from a participant's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parent {
    pub ledger: LedgerId,
    pub claim: ClaimId,
    pub issuer: ParticipantId,
    pub holder: ParticipantId,
    pub receipt: ReceiptFence,
    pub status: ClaimStatus,
    pub local_complete: bool,
    pub latest_response: Option<TestamentId>,
    /// The next cycle is derived by the owner, not selected by a caller.
    pub next_cycle: u32,
}

impl Parent {
    pub fn from_claim(claim: &super::claim::ClaimState) -> Result<Self, ContractError> {
        let receipt = claim.receipt().ok_or(ContractError::StaleReceipt)?;
        let next_cycle = u32::try_from(claim.response_count())
            .map_err(|_| ContractError::Capacity)?
            .checked_add(1)
            .ok_or(ContractError::Capacity)?;
        Ok(Self {
            ledger: claim.binding().ledger,
            claim: ClaimId(claim.binding().object.0),
            issuer: claim.issuer(),
            holder: receipt.holder,
            receipt: receipt.fence,
            status: claim.status(),
            local_complete: claim.local_complete(),
            latest_response: claim.latest_response().map(|row| row.testament),
            next_cycle,
        })
    }
    fn check_identity(&self, ledger: LedgerId, claim: ClaimId) -> Result<(), ContractError> {
        if ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || claim.is_zero()
            || self.issuer.is_zero()
            || self.holder.is_zero()
            || self.receipt.receipt.is_zero()
            || self.receipt.epoch == 0
        {
            return Err(ContractError::InvalidTarget);
        }
        if self.ledger != ledger {
            return Err(ContractError::WrongLedger);
        }
        if self.claim != claim {
            return Err(ContractError::WrongObject);
        }
        Ok(())
    }
    fn check_receipt(&self, receipt: ReceiptFence) -> Result<(), ContractError> {
        if self.receipt != receipt {
            return Err(ContractError::StaleReceipt);
        }
        Ok(())
    }
    fn require_open_response(&self) -> Result<(), ContractError> {
        if self.local_complete
            || !matches!(
                self.status,
                ClaimStatus::Received
                    | ClaimStatus::Progressed
                    | ClaimStatus::TestamentGenerated
                    | ClaimStatus::TestamentAcknowledged
                    | ClaimStatus::Validating
            )
        {
            return Err(ContractError::InvalidTransition);
        }
        Ok(())
    }
}

/// A declaration index identifies a slot whose immutable name was validated at
/// claim admission. Schema similarity never selects a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotBinding {
    pub slot: u32,
    pub artifact: ArtifactRef,
}

/// Typed failure records use real durable diagnostics, never a placeholder hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceFailure {
    Production,
    Structure,
    Metadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Diagnostic {
    pub reason: EvidenceFailure,
    pub artifact: ArtifactRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkArtifact {
    binding: Binding,
    claim: ClaimId,
    slot: u32,
    cycle: u32,
    producer: ParticipantId,
    receipt: ReceiptFence,
    state: WorkArtifactState,
    attachment: Option<Binding>,
    diagnostic: Option<Diagnostic>,
    terminal: Option<(crate::SessionSeq, super::aggregation::ArtifactOutcome)>,
}

fn check_evidence(
    reference: ArtifactRef,
    evidence: &EvidenceAttestation,
) -> Result<(), ContractError> {
    if reference.id.is_zero()
        || evidence.custody_revision == 0
        || !evidence.durable
        || !evidence.schema_valid
        || evidence.descriptor_hash != reference.hash
    {
        return Err(ContractError::MissingEvidence);
    }
    Ok(())
}

impl WorkArtifact {
    pub fn generate(
        binding: Binding,
        parent: &Parent,
        principal: Principal,
        slot: u32,
        receipt: ReceiptFence,
        evidence: &EvidenceAttestation,
    ) -> Result<Self, ContractError> {
        parent.check_identity(binding.ledger, parent.claim)?;
        parent.require_open_response()?;
        parent.check_receipt(receipt)?;
        principal.require_actor(parent.holder)?;
        check_evidence(
            ArtifactRef {
                id: crate::ArtifactId(binding.object.0),
                hash: binding.content,
            },
            evidence,
        )?;
        Ok(Self {
            binding,
            claim: parent.claim,
            slot,
            cycle: parent.next_cycle,
            producer: parent.holder,
            receipt,
            state: WorkArtifactState::Generated,
            attachment: None,
            diagnostic: None,
            terminal: None,
        })
    }

    pub fn generation_failed(
        binding: Binding,
        parent: &Parent,
        principal: Principal,
        slot: u32,
        receipt: ReceiptFence,
        diagnostic: Diagnostic,
        evidence: &EvidenceAttestation,
    ) -> Result<Self, ContractError> {
        parent.check_identity(binding.ledger, parent.claim)?;
        if binding.object.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        parent.require_open_response()?;
        parent.check_receipt(receipt)?;
        principal.require_actor(parent.holder)?;
        if diagnostic.reason != EvidenceFailure::Production {
            return Err(ContractError::InvalidPolicy);
        }
        check_evidence(diagnostic.artifact, evidence)?;
        Ok(Self {
            binding,
            claim: parent.claim,
            slot,
            cycle: parent.next_cycle,
            producer: parent.holder,
            receipt,
            state: WorkArtifactState::GenerationFailed,
            attachment: None,
            diagnostic: Some(diagnostic),
            terminal: None,
        })
    }

    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn state(&self) -> WorkArtifactState {
        self.state
    }
    pub fn slot(&self) -> u32 {
        self.slot
    }
    pub fn attachment(&self) -> Option<TestamentId> {
        self.attachment.map(|binding| TestamentId(binding.object.0))
    }
    pub fn diagnostic(&self) -> Option<Diagnostic> {
        self.diagnostic
    }
    pub fn terminal(&self) -> Option<(crate::SessionSeq, super::aggregation::ArtifactOutcome)> {
        self.terminal
    }
    pub fn reference(&self) -> ArtifactRef {
        ArtifactRef {
            id: crate::ArtifactId(self.binding.object.0),
            hash: self.binding.content,
        }
    }

    pub fn receive(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
    ) -> Result<Self, ContractError> {
        self.binding.check(expected)?;
        parent.check_identity(self.binding.ledger, self.claim)?;
        principal.require_actor(parent.issuer)?;
        if self.state != WorkArtifactState::Generated {
            return Err(ContractError::InvalidTransition);
        }
        Ok(Self {
            binding: self.binding.next()?,
            state: WorkArtifactState::Received,
            ..*self
        })
    }

    pub fn reject_receipt(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
        diagnostic: Diagnostic,
        evidence: &EvidenceAttestation,
    ) -> Result<Self, ContractError> {
        self.binding.check(expected)?;
        parent.check_identity(self.binding.ledger, self.claim)?;
        principal.require_actor(parent.issuer)?;
        if !matches!(
            self.state,
            WorkArtifactState::Generated | WorkArtifactState::Received
        ) {
            return Err(ContractError::InvalidTransition);
        }
        if !matches!(
            diagnostic.reason,
            EvidenceFailure::Structure | EvidenceFailure::Metadata
        ) {
            return Err(ContractError::InvalidPolicy);
        }
        check_evidence(diagnostic.artifact, evidence)?;
        Ok(Self {
            binding: self.binding.next()?,
            state: WorkArtifactState::ReceiptFailed,
            diagnostic: Some(diagnostic),
            ..*self
        })
    }

    /// Entry is an issuer request for deterministic advancement. Evaluator entry
    /// uses its independently checked validation begin in the owner transaction.
    pub fn begin(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
        response: &Response,
    ) -> Result<Self, ContractError> {
        self.binding.check(expected)?;
        parent.check_identity(self.binding.ledger, self.claim)?;
        parent.check_receipt(self.receipt)?;
        principal.require_actor(parent.issuer)?;
        parent.require_open_response()?;
        if self.state != WorkArtifactState::Attached || response.state != ResponseState::Validating
        {
            return Err(ContractError::InvalidTransition);
        }
        response.contains(self)?;
        Ok(Self {
            binding: self.binding.next()?,
            state: WorkArtifactState::Validating,
            ..*self
        })
    }

    pub fn observe_evaluation(
        &self,
        expected: &Binding,
        parent: &Parent,
        response: &Response,
        evaluation: &super::validation::Evaluation<'_>,
    ) -> Result<Self, ContractError> {
        self.binding.check(expected)?;
        parent.check_identity(self.binding.ledger, self.claim)?;
        parent.check_receipt(self.receipt)?;
        parent.require_open_response()?;
        if self.state != WorkArtifactState::Attached || response.state != ResponseState::Validating
        {
            return Err(ContractError::InvalidTransition);
        }
        response.contains(self)?;
        response.check_evaluation(parent, evaluation)?;
        let super::validation::Target::Artifact { slot, artifact, .. } = evaluation.target() else {
            return Err(ContractError::InvalidTarget);
        };
        if slot != self.slot
            || artifact.ledger != self.binding.ledger
            || artifact.object != self.binding.object
            || artifact.content != self.binding.content
        {
            return Err(ContractError::InvalidTarget);
        }
        Ok(Self {
            binding: self.binding.next()?,
            state: WorkArtifactState::Validating,
            ..*self
        })
    }

    /// Deterministic consequences of exact accepted check results. A later
    /// result remains audit evidence and cannot repaint an existing terminal cut.
    pub fn apply_aggregate(
        &self,
        expected: &Binding,
        update: &super::aggregation::ResponseUpdate,
    ) -> Result<Self, ContractError> {
        use super::aggregation::ArtifactOutcome;
        self.binding.check(expected)?;
        let decision = update
            .outcome_for_slot(self.slot)
            .ok_or(ContractError::InvalidTarget)?;
        if decision.claim().ledger != self.binding.ledger
            || decision.claim().object.0 != self.claim.0
            || decision.artifact() != self.reference()
            || update.delivery().receipt() != self.receipt
        {
            return Err(ContractError::InvalidTarget);
        }
        let attached = self.attachment.ok_or(ContractError::InvalidTransition)?;
        if attached.ledger != decision.response().ledger
            || attached.object != decision.response().object
            || attached.content != decision.response().content
        {
            return Err(ContractError::InvalidTarget);
        }
        if self.state.is_terminal() {
            return Ok(*self);
        }
        if self.state != WorkArtifactState::Validating {
            return Err(ContractError::InvalidTransition);
        }
        let state = match decision.outcome() {
            ArtifactOutcome::Pending => return Ok(*self),
            ArtifactOutcome::Passed => WorkArtifactState::Validated,
            ArtifactOutcome::Blocked(_) => WorkArtifactState::ValidationFailed,
        };
        Ok(Self {
            binding: self.binding.next()?,
            state,
            terminal: Some((decision.sequence(), decision.outcome())),
            ..*self
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseIdentity {
    pub binding: Binding,
    pub claim: ClaimId,
    pub receipt: ReceiptFence,
    pub cycle: u32,
    pub prior: Option<TestamentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    identity: ResponseIdentity,
    respondent: ParticipantId,
    state: ResponseState,
    manifest: Vec<SlotBinding>,
    terminal: Option<super::aggregation::ResponseOutcome>,
}

/// Bounded replacements are checked in full before a close can be published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosePlan {
    pub response: Response,
    pub attachments: Vec<WorkArtifact>,
}

impl Response {
    pub fn close(
        identity: ResponseIdentity,
        parent: &Parent,
        principal: Principal,
        current: &[WorkArtifact],
        expected_manifest: &[SlotBinding],
        max_artifacts: usize,
    ) -> Result<ClosePlan, ContractError> {
        parent.check_identity(identity.binding.ledger, identity.claim)?;
        if identity.binding.object.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        parent.check_receipt(identity.receipt)?;
        parent.require_open_response()?;
        principal.require_actor(parent.holder)?;
        if identity.cycle != parent.next_cycle
            || identity.prior != parent.latest_response
            || identity.cycle.checked_add(1).is_none()
        {
            return Err(ContractError::InvalidManifest);
        }
        if current.len() > max_artifacts || expected_manifest.len() > max_artifacts {
            return Err(ContractError::Capacity);
        }
        if current.len() != expected_manifest.len() {
            return Err(ContractError::InvalidManifest);
        }
        let mut previous_slot = None;
        for (artifact, expected) in current.iter().zip(expected_manifest) {
            parent.check_identity(artifact.binding.ledger, artifact.claim)?;
            parent.check_receipt(artifact.receipt)?;
            if artifact.producer != parent.holder {
                return Err(ContractError::WrongActor);
            }
            if artifact.cycle != identity.cycle {
                return Err(ContractError::InvalidManifest);
            }
            if !matches!(
                artifact.state,
                WorkArtifactState::Generated | WorkArtifactState::Received
            ) || artifact.attachment.is_some()
            {
                return Err(ContractError::InvalidTransition);
            }
            if artifact.slot != expected.slot
                || artifact.reference() != expected.artifact
                || previous_slot.is_some_and(|prior| prior >= artifact.slot)
            {
                return Err(ContractError::InvalidManifest);
            }
            artifact.binding.next()?;
            previous_slot = Some(artifact.slot);
        }
        let mut manifest = Vec::new();
        let mut attachments = Vec::new();
        manifest
            .try_reserve_exact(current.len())
            .map_err(|_| ContractError::Capacity)?;
        attachments
            .try_reserve_exact(current.len())
            .map_err(|_| ContractError::Capacity)?;
        for artifact in current {
            manifest.push(SlotBinding {
                slot: artifact.slot,
                artifact: artifact.reference(),
            });
            attachments.push(WorkArtifact {
                binding: artifact.binding.next()?,
                state: WorkArtifactState::Attached,
                attachment: Some(identity.binding),
                ..*artifact
            });
        }
        Ok(ClosePlan {
            response: Self {
                identity,
                respondent: parent.holder,
                state: ResponseState::Generated,
                manifest,
                terminal: None,
            },
            attachments,
        })
    }

    pub fn identity(&self) -> ResponseIdentity {
        self.identity
    }
    pub fn state(&self) -> ResponseState {
        self.state
    }
    pub fn manifest(&self) -> &[SlotBinding] {
        &self.manifest
    }
    pub fn terminal(&self) -> Option<super::aggregation::ResponseOutcome> {
        self.terminal
    }

    pub fn evaluation(&self) -> Result<ResponseEvaluation<'_>, ContractError> {
        if !matches!(
            self.state,
            ResponseState::Validating
                | ResponseState::Validated
                | ResponseState::ValidationIncomplete
                | ResponseState::ValidationFailed
                | ResponseState::ValidationErrored
        ) {
            return Err(ContractError::InvalidTransition);
        }
        Ok(ResponseEvaluation { response: self })
    }

    fn check(&self, expected: &Binding, parent: &Parent) -> Result<(), ContractError> {
        self.identity.binding.check(expected)?;
        parent.check_identity(self.identity.binding.ledger, self.identity.claim)?;
        parent.check_receipt(self.identity.receipt)
    }

    // The manifest is immutable and borrowed by the plan; applying a status plan
    // moves the existing response instead of cloning its complete manifest.
    pub fn plan_post(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
    ) -> Result<ResponseTransition, ContractError> {
        self.check(expected, parent)?;
        parent.require_open_response()?;
        principal.require_actor(parent.holder)?;
        principal.require_actor(self.respondent)?;
        if self.state != ResponseState::Generated {
            return Err(ContractError::InvalidTransition);
        }
        self.plan(ResponseState::Posted)
    }

    pub fn plan_receive(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
    ) -> Result<ResponseTransition, ContractError> {
        self.check(expected, parent)?;
        principal.require_actor(parent.issuer)?;
        if self.state != ResponseState::Posted {
            return Err(ContractError::InvalidTransition);
        }
        self.plan(ResponseState::Received)
    }

    pub fn plan_begin(
        &self,
        expected: &Binding,
        claim: &super::claim::ClaimState,
        principal: Principal,
        acceptance: &super::aggregation::ClaimDecision<'_>,
    ) -> Result<ResponseTransition, ContractError> {
        let parent = self.check_acceptance(claim, acceptance)?;
        self.plan_begin_with_parent(expected, &parent, principal)
    }

    fn check_acceptance(
        &self,
        claim: &super::claim::ClaimState,
        acceptance: &super::aggregation::ClaimDecision<'_>,
    ) -> Result<Parent, ContractError> {
        claim.check_increment_entry(acceptance)?;
        Parent::from_claim(claim)
    }

    fn plan_begin_with_parent(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
    ) -> Result<ResponseTransition, ContractError> {
        self.check(expected, parent)?;
        parent.require_open_response()?;
        principal.require_actor(parent.issuer)?;
        if self.state != ResponseState::Received {
            return Err(ContractError::InvalidTransition);
        }
        self.plan(ResponseState::Validating)
    }

    /// Low-level role/manifest fixtures do not construct a complete claim owner.
    /// Production entry always passes the checked immutable acceptance decision.
    #[cfg(test)]
    pub(crate) fn plan_begin_fixture(
        &self,
        expected: &Binding,
        parent: &Parent,
        principal: Principal,
    ) -> Result<ResponseTransition, ContractError> {
        self.plan_begin_with_parent(expected, parent, principal)
    }

    fn check_evaluation(
        &self,
        parent: &Parent,
        evaluation: &super::validation::Evaluation<'_>,
    ) -> Result<(), ContractError> {
        parent.check_identity(self.identity.binding.ledger, self.identity.claim)?;
        parent.check_receipt(self.identity.receipt)?;
        parent.require_open_response()?;
        if evaluation.ledger() != self.identity.binding.ledger
            || evaluation.claim() != self.identity.claim
            || evaluation.receipt() != Some(self.identity.receipt)
        {
            return Err(ContractError::InvalidTarget);
        }
        if !evaluation.has_begun()
            || evaluation.state().is_terminal()
            || evaluation.fence().is_some()
            || evaluation.sealed().is_some()
            || evaluation.declared_phase() != crate::ValidationPhase::WholeWork
        {
            return Err(ContractError::InvalidTransition);
        }
        let super::validation::Target::Artifact { response, .. } = evaluation.target() else {
            return Err(ContractError::InvalidTarget);
        };
        if response.ledger != self.identity.binding.ledger
            || response.object != self.identity.binding.object
            || response.content != self.identity.binding.content
        {
            return Err(ContractError::InvalidTarget);
        }
        Ok(())
    }

    /// A checked designated evaluator begin has the same aggregate-entry effect
    /// as the issuer's request; Focal does not invoke the evaluator's tool.
    pub fn plan_evaluation(
        &self,
        expected: &Binding,
        claim: &super::claim::ClaimState,
        evaluation: &super::validation::Evaluation<'_>,
        acceptance: &super::aggregation::ClaimDecision<'_>,
    ) -> Result<ResponseTransition, ContractError> {
        let parent = self.check_acceptance(claim, acceptance)?;
        self.identity.binding.check(expected)?;
        self.check_evaluation(&parent, evaluation)?;
        if self.state != ResponseState::Received {
            return Err(ContractError::InvalidTransition);
        }
        self.plan(ResponseState::Validating)
    }

    fn plan(&self, next: ResponseState) -> Result<ResponseTransition, ContractError> {
        Ok(ResponseTransition {
            expected: self.identity.binding,
            before: self.state,
            next,
            binding: self.identity.binding.next()?,
            terminal: None,
        })
    }

    pub fn plan_aggregate(
        &self,
        expected: &Binding,
        update: &super::aggregation::ResponseUpdate,
    ) -> Result<Option<ResponseTransition>, ContractError> {
        use super::aggregation::{BlockingKind, ResponseOutcome};
        self.identity.binding.check(expected)?;
        let source = update.response_binding();
        if source.ledger != self.identity.binding.ledger
            || source.object != self.identity.binding.object
            || source.content != self.identity.binding.content
            || update.claim_binding().object.0 != self.identity.claim.0
            || update.delivery().receipt() != self.identity.receipt
        {
            return Err(ContractError::InvalidTarget);
        }
        if self.state.is_terminal() {
            return Ok(None);
        }
        if self.state != ResponseState::Validating {
            return Err(ContractError::InvalidTransition);
        }
        let next = match update.response_outcome() {
            ResponseOutcome::Evaluating => return Ok(None),
            ResponseOutcome::Validated { .. } => ResponseState::Validated,
            ResponseOutcome::Blocked(cut) => match cut.cause().kind() {
                BlockingKind::Incomplete => ResponseState::ValidationIncomplete,
                BlockingKind::Failed => ResponseState::ValidationFailed,
                BlockingKind::Errored => ResponseState::ValidationErrored,
            },
        };
        Ok(Some(ResponseTransition {
            terminal: Some(update.response_outcome()),
            ..self.plan(next)?
        }))
    }

    pub fn apply(&mut self, transition: ResponseTransition) -> Result<(), ContractError> {
        self.identity.binding.check(&transition.expected)?;
        if self.state != transition.before {
            return Err(ContractError::InvalidTransition);
        }
        self.identity.binding = transition.binding;
        self.state = transition.next;
        self.terminal = transition.terminal;
        Ok(())
    }

    fn contains(&self, artifact: &WorkArtifact) -> Result<(), ContractError> {
        if self.identity.binding.ledger != artifact.binding.ledger
            || self.identity.claim != artifact.claim
            || self.identity.receipt != artifact.receipt
            || artifact.attachment.is_none_or(|binding| {
                binding.object != self.identity.binding.object
                    || binding.content != self.identity.binding.content
                    || binding.ledger != self.identity.binding.ledger
            })
        {
            return Err(ContractError::InvalidTarget);
        }
        let slot = self
            .manifest
            .binary_search_by_key(&artifact.slot, |entry| entry.slot)
            .ok()
            .and_then(|index| self.manifest.get(index));
        if slot.is_none_or(|entry| entry.artifact != artifact.reference()) {
            return Err(ContractError::InvalidTarget);
        }
        Ok(())
    }
}

/// A checked transition cannot be constructed by a caller or replayed against a
/// different response/revision. The owner publishes it alongside related rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseTransition {
    expected: Binding,
    before: ResponseState,
    next: ResponseState,
    binding: Binding,
    terminal: Option<super::aggregation::ResponseOutcome>,
}

/// Proof that a privately constructed response passed separate post, receipt and
/// evaluation entry. An aggregate cannot turn a Generated manifest into evidence.
#[derive(Debug, Clone, Copy)]
pub struct ResponseEvaluation<'a> {
    response: &'a Response,
}

impl ResponseEvaluation<'_> {
    pub fn binding(&self) -> Binding {
        self.response.identity.binding
    }
    pub fn claim(&self) -> ClaimId {
        self.response.identity.claim
    }
    pub fn receipt(&self) -> ReceiptFence {
        self.response.identity.receipt
    }
    pub fn manifest(&self) -> &[SlotBinding] {
        &self.response.manifest
    }
}

impl super::claim::ClaimState {
    /// Consumes a checked response's exact lifecycle fact. Storage publication of
    /// this claim change and the response/attachment plan is one owner transaction.
    pub fn observe_response(
        &mut self,
        expected: &Binding,
        principal: Principal,
        response: &Response,
    ) -> Result<(), ContractError> {
        use super::claim::{ResponseEvent, ResponseLink};
        let identity = response.identity();
        if self.binding().ledger != identity.binding.ledger {
            return Err(ContractError::WrongLedger);
        }
        if self.binding().object.0 != identity.claim.0 {
            return Err(ContractError::WrongObject);
        }
        let event = match response.state() {
            ResponseState::Generated => ResponseEvent::Generated,
            ResponseState::Posted => ResponseEvent::Posted,
            ResponseState::Received => ResponseEvent::Received,
            _ => return Err(ContractError::InvalidTransition),
        };
        self.record_response(
            expected,
            principal,
            ResponseLink {
                testament: TestamentId(identity.binding.object.0),
                content: identity.binding.content,
                receipt: identity.receipt,
                cycle: identity.cycle,
                prior: identity.prior,
            },
            event,
        )
    }
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;
