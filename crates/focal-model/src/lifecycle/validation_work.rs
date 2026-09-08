//! WholeWork targets come from an actual received response and its exact work
//! attachment. A separately checked acceptance decision gates initial entry;
//! already-begun reports retain their own target and authority fences.
use super::*;
use crate::ClaimStatus;
use crate::lifecycle::{
    aggregation::{ArtifactOutcome, ClaimDecision},
    claim::ClaimState,
    evidence::{Response, ResponseState, WorkArtifact, WorkArtifactState},
};

#[cfg(test)]
#[path = "validation_work_tests.rs"]
mod tests;

fn open_claim(claim: &ClaimState) -> Result<(), ContractError> {
    if claim.local_complete()
        || !matches!(
            claim.status(),
            ClaimStatus::TestamentAcknowledged | ClaimStatus::Validating
        )
    {
        return Err(ContractError::InvalidTransition);
    }
    Ok(())
}

fn received(claim: &ClaimState, response: &Response) -> Result<(), ContractError> {
    claim.acceptance().check(claim.binding(), claim.issuer())?;
    let identity = response.identity();
    if identity.claim.0 != claim.binding().object.0 {
        return Err(ContractError::WrongObject);
    }
    if !matches!(
        response.state(),
        ResponseState::Received
            | ResponseState::Validating
            | ResponseState::Validated
            | ResponseState::ValidationIncomplete
            | ResponseState::ValidationFailed
            | ResponseState::ValidationErrored
    ) {
        return Err(ContractError::InvalidTransition);
    }
    claim.received_report(identity.binding, identity.receipt, response.report_stamp())?;
    if identity.cycle == 0
        || identity.cycle > claim.max_responses()
        || usize::try_from(identity.cycle).map_err(|_| ContractError::Capacity)?
            > claim.response_count()
    {
        return Err(ContractError::StaleEvaluation);
    }
    Ok(())
}

fn attachment(
    claim: &ClaimState,
    response: &Response,
    work: &WorkArtifact,
    slot: u32,
) -> Result<(), ContractError> {
    response.contains(work)?;
    let receipt = claim.receipt().ok_or(ContractError::StaleReceipt)?;
    if work.receipt() != receipt.fence || work.producer() != receipt.holder {
        return Err(ContractError::StaleReceipt);
    }
    if work.cycle() != response.identity().cycle || work.slot() != slot {
        return Err(ContractError::InvalidTarget);
    }
    match work.state() {
        WorkArtifactState::Attached
        | WorkArtifactState::Validating
        | WorkArtifactState::Validated
        | WorkArtifactState::ValidationFailed => Ok(()),
        WorkArtifactState::Generated
        | WorkArtifactState::GenerationFailed
        | WorkArtifactState::Received
        | WorkArtifactState::ReceiptFailed => Err(ContractError::InvalidTransition),
    }
}

fn current_binding(current: Binding, pinned: Binding) -> Result<(), ContractError> {
    Binding {
        revision: pinned.revision,
        ..current
    }
    .check(&pinned)?;
    if current.revision < pinned.revision {
        return Err(ContractError::StaleRevision);
    }
    Ok(())
}

impl<'a> Evaluation<'a> {
    /// Settle structural manifest absence without invoking an external handler.
    /// Handler deadlines and policy grants govern execution, not the immutable
    /// fact that this received response omitted its declared slot. Explicit
    /// evaluation fences and sealed cohorts remain unchanged for later audit.
    pub fn settle_missing(
        &self,
        principal: Principal,
        expected: &Binding,
        claim: &ClaimState,
        response: &Response,
        acceptance: &ClaimDecision<'_>,
    ) -> Result<Transition<'a>, ContractError> {
        principal.require_actor(claim.issuer())?;
        self.settle_missing_checked(expected, claim, response, Some(acceptance))
    }

    /// Structural absence follows the same checked response entry as attached
    /// work. No participant acts as the claimant on this derived path, and no
    /// external attempt, proof artifact, or report is invented.
    pub fn settle_missing_entered(
        &self,
        expected: &Binding,
        claim: &ClaimState,
        response: &Response,
        entry: &crate::lifecycle::evidence::ResponseEntry,
    ) -> Result<Transition<'a>, ContractError> {
        entry.check(claim, response)?;
        self.settle_missing_checked(expected, claim, response, None)
    }

    fn settle_missing_checked(
        &self,
        expected: &Binding,
        claim: &ClaimState,
        response: &Response,
        acceptance: Option<&ClaimDecision<'_>>,
    ) -> Result<Transition<'a>, ContractError> {
        self.binding.check(expected)?;
        if !matches!(self.target, Target::MissingSlot { .. }) {
            return Err(ContractError::InvalidTarget);
        }
        self.work_target(claim, response, None)?;
        open_claim(claim)?;
        if let Some(acceptance) = acceptance {
            ResponseReadiness::from_received(response, self.target, claim, acceptance)?;
        }
        if self.state != State::Ready || self.begun || self.last_result.is_some() {
            return Err(ContractError::InvalidTransition);
        }
        if self.fence.is_some() || self.sealed.is_some() {
            return Ok(Transition {
                next: *self,
                result: None,
            });
        }
        if self.suppression.is_some() {
            return Err(ContractError::InvalidTransition);
        }
        let mut next = *self;
        next.stored.binding = self.binding.next()?;
        if self.mode() == ValidationMode::Observe {
            next.stored.suppression = Some(Suppression::MissingTarget);
            return Ok(Transition { next, result: None });
        }
        next.stored.state = State::ValidationIncomplete;
        let result = next.accepted(VerdictValue::Incomplete, Phase::MissingTarget, None, None);
        next.stored.last_result = Some(result);
        Ok(Transition {
            next,
            result: Some(result),
        })
    }

    /// Materialize the exact slot selected by the immutable declaration. A
    /// missing manifest slot has a MissingSlot target, never a placeholder work
    /// artifact. Present slots require the actual attached row for its binding.
    /// This records a cohort member; it does not grant Increment-gate readiness.
    pub fn materialize_work(
        principal: Principal,
        declaration: &'a Declaration,
        claim: &ClaimState,
        response: &Response,
        work: Option<&WorkArtifact>,
    ) -> Result<Self, ContractError> {
        principal.require_actor(claim.issuer())?;
        claim.acceptance().check_declaration(declaration)?;
        let TargetDeclaration::WholeWorkSlot { index, name } = declaration.target() else {
            return Err(ContractError::InvalidTarget);
        };
        open_claim(claim)?;
        received(claim, response)?;
        if !matches!(
            response.state(),
            ResponseState::Received | ResponseState::Validating
        ) {
            return Err(ContractError::InvalidTransition);
        }
        let identity = response.identity();
        let present = response
            .manifest()
            .binary_search_by_key(&index, |entry| entry.slot)
            .is_ok();
        let target = match (present, work) {
            (true, Some(work)) => {
                attachment(claim, response, work, index)?;
                if !matches!(
                    work.state(),
                    WorkArtifactState::Attached | WorkArtifactState::Validating
                ) {
                    return Err(ContractError::InvalidTransition);
                }
                Target::Artifact {
                    response: identity.binding,
                    slot: index,
                    artifact: work.binding(),
                }
            }
            (false, None) => Target::MissingSlot {
                response: identity.binding,
                slot: index,
            },
            (true, None) | (false, Some(_)) => return Err(ContractError::InvalidManifest),
        };
        // This private source check does not claim that Increment obligations
        // have settled. Only work_owner consumes that separate checked gate.
        ResponseReadiness::checked(
            identity.binding,
            identity.claim,
            identity.receipt,
            response.manifest(),
            target,
        )?;
        Self::materialize(
            principal,
            declaration,
            Materialization {
                binding: declaration.binding(),
                target,
                slot_name: Some(name),
                generation: u64::from(identity.cycle),
                receipt: Some(identity.receipt),
            },
        )
    }

    fn work_target(
        &self,
        claim: &ClaimState,
        response: &Response,
        work: Option<&WorkArtifact>,
    ) -> Result<(), ContractError> {
        claim.acceptance().check_declaration(self.declaration)?;
        let TargetDeclaration::WholeWorkSlot { index, .. } = self.declaration.target() else {
            return Err(ContractError::InvalidTarget);
        };
        received(claim, response)?;
        let pinned_response = match (self.target, work) {
            (
                Target::Artifact {
                    response: pinned,
                    slot,
                    artifact,
                },
                Some(work),
            ) if slot == index => {
                attachment(claim, response, work, slot)?;
                current_binding(work.binding(), artifact)?;
                pinned
            }
            (
                Target::MissingSlot {
                    response: pinned,
                    slot,
                },
                None,
            ) if slot == index => pinned,
            _ => return Err(ContractError::InvalidTarget),
        };
        current_binding(response.identity().binding, pinned_response)?;
        if self.receipt != Some(response.identity().receipt) {
            return Err(ContractError::StaleReceipt);
        }
        if self.generation != u64::from(response.identity().cycle) {
            return Err(ContractError::StaleEvaluation);
        }
        ResponseReadiness::checked(
            response.identity().binding,
            response.identity().claim,
            response.identity().receipt,
            response.manifest(),
            self.target,
        )?;
        Ok(())
    }

    /// A real received response proves manifest identity; a separately checked
    /// current ClaimDecision proves all Required Increment targets are final.
    /// The designated Actor still calls begin; missing Required slots use its
    /// existing artifact-free Incomplete path, also authorized to the issuer.
    pub fn work_owner(
        &self,
        claim: &ClaimState,
        response: &Response,
        work: Option<&WorkArtifact>,
        acceptance: &ClaimDecision<'_>,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        self.work_target(claim, response, work)?;
        open_claim(claim)?;
        let source = ResponseReadiness::from_received(response, self.target, claim, acceptance)?;
        let requires_policy = match &self.declaration.spec.program {
            OwnedProgram::Delivery => return Err(ContractError::InvalidPolicy),
            OwnedProgram::Programmatic { check, quality } => {
                check.required_policy.is_some()
                    || quality
                        .as_ref()
                        .is_some_and(|policy| policy.required_policy.is_some())
            }
            OwnedProgram::Agentic { check } => check.required_policy.is_some(),
        };
        if requires_policy {
            return Err(ContractError::InvalidPolicy);
        }
        let readiness = match work {
            Some(work) if work.state() == WorkArtifactState::ValidationFailed => {
                let Target::Artifact { artifact, .. } = self.target else {
                    return Err(ContractError::InvalidTarget);
                };
                let (sequence, ArtifactOutcome::Blocked(_)) =
                    work.terminal().ok_or(ContractError::InvalidCut)?
                else {
                    return Err(ContractError::InvalidCut);
                };
                Readiness::ArtifactFailed {
                    artifact,
                    cause: context(work.binding(), sequence, b"artifact")?,
                }
            }
            Some(work)
                if work.state() == WorkArtifactState::Validated
                    && self.mode() == ValidationMode::Required =>
            {
                return Err(ContractError::InvalidTransition);
            }
            _ => Readiness::ResponseReceived(source),
        };
        self.work_frame(ParentState::Open, readiness, logical_time)
    }

    /// Reporting continues an already-begun pinned attempt. Ordinary terminal
    /// work/response/claim states preserve evidence authority; explicit controls,
    /// adoption, deadline or retained evaluation fences still prohibit reports.
    /// No new Increment-gate decision can erase an already-begun report.
    pub fn work_report_owner(
        &self,
        claim: &ClaimState,
        response: &Response,
        work: Option<&WorkArtifact>,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        self.current_attempt()?;
        self.work_target(claim, response, work)?;
        let readiness = ResponseReadiness::from_evaluation(&response.evaluation()?, self.target)?;
        let parent = match claim.status() {
            ClaimStatus::Generated
            | ClaimStatus::Posted
            | ClaimStatus::Received
            | ClaimStatus::Progressed
            | ClaimStatus::TestamentGenerated => return Err(ContractError::InvalidTransition),
            ClaimStatus::Cancelled
            | ClaimStatus::Revoked
            | ClaimStatus::Superseded
            | ClaimStatus::Expired => return Err(ContractError::StaleEvaluation),
            ClaimStatus::PostFailed
            | ClaimStatus::ReceiptFailed
            | ClaimStatus::TestamentGenerationFailed
            | ClaimStatus::ValidationIncomplete
            | ClaimStatus::ValidationFailed
            | ClaimStatus::ValidationErrored
            | ClaimStatus::DependencyFailed
            | ClaimStatus::Deadlocked => ParentState::Failed {
                cause: claim_context(claim)?,
            },
            ClaimStatus::Satisfied => ParentState::LocallyComplete {
                cause: claim_context(claim)?,
            },
            ClaimStatus::TestamentAcknowledged | ClaimStatus::Validating => {
                if claim.local_complete() {
                    ParentState::LocallyComplete {
                        cause: claim_context(claim)?,
                    }
                } else {
                    ParentState::Open
                }
            }
        };
        self.work_frame(parent, Readiness::ResponseReceived(readiness), logical_time)
    }

    fn work_frame(
        &self,
        parent: ParentState,
        readiness: Readiness,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        let policy = self.declaration.policy(self.phase)?;
        if policy.required_policy.is_some() {
            return Err(ContractError::InvalidPolicy);
        }
        let owner = OwnerState {
            evaluation: self.binding,
            target: self.target,
            parent,
            readiness,
            cohort: self
                .sealed
                .map_or(Cohort::Open, |cause| Cohort::Sealed { cause }),
            authority: Authority {
                evaluator: policy.evaluator,
                definition: policy.definition,
                generation: self.generation,
                receipt: self.receipt,
                deadline: self.deadline(),
                policy_evidence: None,
                state: self
                    .fence
                    .map_or(AuthorityState::Live, AuthorityState::Fenced),
            },
            logical_time,
        };
        self.check_authority(&owner, self.phase)?;
        Ok(owner)
    }
}

fn claim_context(claim: &ClaimState) -> Result<ContentHash, ContractError> {
    let sequence = claim.local_sealed_at().ok_or(ContractError::InvalidCut)?;
    if sequence < claim.created() {
        return Err(ContractError::InvalidCut);
    }
    context(claim.binding(), sequence, b"claim")
}

// Process-local suppression/continuation identity: immutable source and original
// terminal sequence, excluding its advancing lifecycle revision. It does not
// mint a new accepted result, artifact or persisted business cause.
fn context(
    binding: Binding,
    sequence: crate::SessionSeq,
    role: &[u8],
) -> Result<ContentHash, ContractError> {
    if sequence.0 == 0 {
        return Err(ContractError::InvalidCut);
    }
    let mut hash = blake3::Hasher::new_derive_key("focal native WholeWork owner context");
    hash.update(role);
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&sequence.0.to_be_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}
