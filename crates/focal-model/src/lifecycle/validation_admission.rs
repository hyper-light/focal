//! Reporting is a continuation of an already begun Admission evaluation. Claim
//! receipt or ordinary failure does not retroactively revoke its pinned attempt.
use super::*;
use crate::ClaimStatus;
use crate::lifecycle::claim::ClaimState;

impl Evaluation<'_> {
    /// Resolve only the actual owner's claim and logical time. The caller still
    /// resolves membership and verified evidence, then calls `report`, which
    /// checks the authenticated reporter and exact supplied attempt. Capture
    /// `current_attempt()` before reporting: the next state can be terminal or
    /// can already describe a different quality/fallback attempt.
    ///
    /// Admission carries no respondent receipt. Acquiring the first receipt is
    /// allowed while Observe checks finish. An adoption operation that revokes
    /// an Admission evaluation must install its authority fence in the same
    /// owner transaction; an epoch value alone cannot distinguish adoption from
    /// a first receipt. Every retained fence is checked here.
    pub fn admission_report_owner(
        &self,
        claim: &ClaimState,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        self.current_attempt()?;
        claim.acceptance().check(claim.binding(), claim.issuer())?;
        claim.acceptance().check_declaration(self.declaration)?;
        if self.declaration.target() != TargetDeclaration::Admission {
            return Err(ContractError::InvalidTarget);
        }
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
        if self.receipt.is_some() {
            return Err(ContractError::StaleReceipt);
        }
        let parent = match claim.status() {
            ClaimStatus::Generated => return Err(ContractError::InvalidTransition),
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
            ClaimStatus::Posted
            | ClaimStatus::Received
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
        let policy = self.declaration.policy(self.phase)?;
        if policy.required_policy.is_some() {
            return Err(ContractError::InvalidPolicy);
        }
        let owner = OwnerState {
            evaluation: self.binding,
            target: self.target,
            parent,
            // This is the pinned Admission prerequisite, not permission for a
            // new begin. Reporting does not reevaluate start readiness.
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
        };
        self.check_authority(&owner, self.phase)?;
        Ok(owner)
    }
}

/// A process-local binding for the truthful failure/completion owner category.
/// It names the immutable claim and original first seal, remaining stable across
/// later lifecycle revisions; it neither replaces its
/// terminal cause nor becomes a new durable result or business cut. Begun report
/// authorization does not suppress or mutate the parent using this value.
fn continuation_context(claim: &ClaimState) -> Result<ContentHash, ContractError> {
    let sequence = claim.local_sealed_at().ok_or(ContractError::InvalidCut)?;
    if sequence.0 == 0 || sequence < claim.created() {
        return Err(ContractError::InvalidCut);
    }
    let binding = claim.binding();
    let mut hash = blake3::Hasher::new_derive_key("focal native Admission report parent");
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&sequence.0.to_be_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

#[cfg(test)]
#[path = "validation_admission_tests.rs"]
mod tests;
