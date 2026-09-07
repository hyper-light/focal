//! Increment owner facts from actual claim and work rows. These helpers neither
//! execute a validator nor change work/claim acceptance when a report fails.
use super::*;
use crate::ClaimStatus;
use crate::lifecycle::{
    claim::ClaimState,
    evidence::{WorkArtifact, WorkArtifactState},
};

#[cfg(test)]
#[path = "validation_increment_tests.rs"]
mod tests;

fn work_identity(claim: &ClaimState, work: &WorkArtifact) -> Result<(), ContractError> {
    claim.acceptance().check(claim.binding(), claim.issuer())?;
    if work.binding().ledger != claim.binding().ledger {
        return Err(ContractError::WrongLedger);
    }
    if work.claim().0 != claim.binding().object.0 {
        return Err(ContractError::WrongObject);
    }
    if !claim.acceptance().has_slot(work.slot()) {
        return Err(ContractError::InvalidTarget);
    }
    let receipt = claim.receipt().ok_or(ContractError::StaleReceipt)?;
    if receipt.fence != work.receipt() || receipt.holder != work.producer() {
        return Err(ContractError::StaleReceipt);
    }
    // Closing advances the next response cycle. Earlier immutable work remains
    // eligible; a future or unconfigured cycle does not become owner evidence.
    let response_count =
        u32::try_from(claim.response_count()).map_err(|_| ContractError::Capacity)?;
    if work.cycle() == 0
        || work.cycle() > claim.max_responses()
        || (work.cycle() > response_count && work.cycle().checked_sub(response_count) != Some(1))
    {
        return Err(ContractError::StaleEvaluation);
    }
    Ok(())
}

fn start_eligible(claim: &ClaimState, work: &WorkArtifact) -> Result<(), ContractError> {
    // This is not a new response: the last authored cycle can already be closed.
    // Do not compute a next cycle or consume a response slot just to begin its
    // outstanding Increment evaluation.
    if claim.local_complete()
        || !matches!(
            claim.status(),
            ClaimStatus::Received
                | ClaimStatus::Progressed
                | ClaimStatus::TestamentGenerated
                | ClaimStatus::TestamentAcknowledged
                | ClaimStatus::Validating
        )
    {
        return Err(ContractError::InvalidTransition);
    }
    match work.state() {
        WorkArtifactState::Generated
        | WorkArtifactState::Received
        | WorkArtifactState::Attached
        // Receipt rejection preserves the submitted output and its diagnostic.
        // Its already-registered Increment checks can inspect that immutable
        // source without attaching or advancing the terminal work artifact.
        | WorkArtifactState::ReceiptFailed => Ok(()),
        WorkArtifactState::GenerationFailed
        | WorkArtifactState::Validating
        | WorkArtifactState::Validated
        | WorkArtifactState::ValidationFailed => Err(ContractError::InvalidTransition),
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
    /// The owner resolves the actual submitted work first. Its immutable cycle
    /// assigns this evaluation's generation; the issuer's committed declaration
    /// supplies every evaluator, handler, policy and deadline.
    pub fn materialize_increment(
        principal: Principal,
        declaration: &'a Declaration,
        claim: &ClaimState,
        work: &WorkArtifact,
    ) -> Result<Self, ContractError> {
        principal.require_actor(claim.issuer())?;
        claim.acceptance().check_declaration(declaration)?;
        if declaration.target() != TargetDeclaration::Increment {
            return Err(ContractError::InvalidTarget);
        }
        work_identity(claim, work)?;
        // Materialization precedes work exposure. A later receipt rejection
        // permits existing Increment checks to finish, not a new target cohort.
        if work.state() == WorkArtifactState::ReceiptFailed {
            return Err(ContractError::InvalidTransition);
        }
        start_eligible(claim, work)?;
        Self::materialize(
            principal,
            declaration,
            Materialization {
                binding: declaration.binding(),
                target: Target::Increment {
                    claim: claim.binding(),
                    artifact: work.binding(),
                },
                slot_name: None,
                generation: u64::from(work.cycle()),
                receipt: Some(work.receipt()),
            },
        )
    }

    fn increment_target(
        &self,
        claim: &ClaimState,
        work: &WorkArtifact,
    ) -> Result<(), ContractError> {
        claim.acceptance().check_declaration(self.declaration)?;
        if self.declaration.target() != TargetDeclaration::Increment {
            return Err(ContractError::InvalidTarget);
        }
        let Target::Increment {
            claim: pinned_claim,
            artifact: pinned_work,
        } = self.target
        else {
            return Err(ContractError::InvalidTarget);
        };
        work_identity(claim, work)?;
        current_binding(claim.binding(), pinned_claim)?;
        current_binding(work.binding(), pinned_work)?;
        if self.receipt != Some(work.receipt()) {
            return Err(ContractError::StaleReceipt);
        }
        if self.generation != u64::from(work.cycle()) {
            return Err(ContractError::StaleEvaluation);
        }
        Ok(())
    }

    /// Derive new-attempt eligibility without caller-authored readiness flags.
    /// ReceiptFailed remains inspectable by its existing Increment checks: it
    /// retains a real output, unlike GenerationFailed. This does not authorize
    /// attachment or WholeWork state progression for the rejected artifact.
    /// All reachable phases must work without policy evidence: this native owner
    /// helper has no committed policy-grant registry to authorize a later phase.
    pub fn increment_owner(
        &self,
        claim: &ClaimState,
        work: &WorkArtifact,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        self.increment_target(claim, work)?;
        start_eligible(claim, work)?;
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
        self.increment_frame(ParentState::Open, logical_time)
    }

    /// Continue an already-begun attempt through ordinary business outcomes.
    /// The original work target and receipt stay pinned after response closure;
    /// control, adoption, deadline and retained evaluation fences still revoke
    /// authority. The caller captures `current_attempt()` before `report`.
    pub fn increment_report_owner(
        &self,
        claim: &ClaimState,
        work: &WorkArtifact,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        self.current_attempt()?;
        self.increment_target(claim, work)?;
        let parent = match claim.status() {
            ClaimStatus::Generated | ClaimStatus::Posted => {
                return Err(ContractError::InvalidTransition);
            }
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
                cause: continuation_context(claim)?,
            },
            ClaimStatus::Satisfied => ParentState::LocallyComplete {
                cause: continuation_context(claim)?,
            },
            ClaimStatus::Received
            | ClaimStatus::Progressed
            | ClaimStatus::TestamentGenerated
            | ClaimStatus::TestamentAcknowledged
            | ClaimStatus::Validating => {
                if claim.local_complete() {
                    ParentState::LocallyComplete {
                        cause: continuation_context(claim)?,
                    }
                } else {
                    ParentState::Open
                }
            }
        };
        self.increment_frame(parent, logical_time)
    }

    fn increment_frame(
        &self,
        parent: ParentState,
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
            readiness: Readiness::IncrementEligible,
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

// A local owner-category identity, not a new durable cause or business cut.
// Later lifecycle revisions must not repaint the original failure/completion.
fn continuation_context(claim: &ClaimState) -> Result<ContentHash, ContractError> {
    let sequence = claim.local_sealed_at().ok_or(ContractError::InvalidCut)?;
    if sequence.0 == 0 || sequence < claim.created() {
        return Err(ContractError::InvalidCut);
    }
    let binding = claim.binding();
    let mut hash = blake3::Hasher::new_derive_key("focal native Increment report parent");
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&sequence.0.to_be_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}
