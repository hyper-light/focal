//! Cohort recording derived from an actual sealed claim. This neither closes
//! report authority nor invents a result. The owner supplies the complete actual
//! registration set and publishes every transition with its original history.
use super::*;
use crate::lifecycle::claim::ClaimState;

/// Checked exact state transition, including an unchanged terminal/sealed row.
/// Both values are retained so a report followed by a seal can prove its real
/// intermediate state. This has no wire identity; an owner retaining a buffer
/// of these tokens must charge its full value capacity before construction.
#[derive(Debug, Clone, Copy)]
pub struct SealTransition {
    previous: EvaluationState,
    next: EvaluationState,
}

impl SealTransition {
    pub fn before(&self) -> Binding {
        self.previous.binding
    }
    pub fn previous(&self) -> EvaluationState {
        self.previous
    }
    pub fn next(&self) -> EvaluationState {
        self.next
    }
    pub fn changed(&self) -> bool {
        self.previous != self.next
    }

    /// The caller resolves the real before/after rows. Binding equality alone
    /// cannot substitute another attempt, authority fence or evidence chain.
    pub fn check(
        &self,
        before: &EvaluationState,
        after: &EvaluationState,
    ) -> Result<(), ContractError> {
        self.previous.binding.check(&before.binding)?;
        self.next.binding.check(&after.binding)?;
        if *before != self.previous || *after != self.next {
            return Err(ContractError::ContentConflict);
        }
        let expected = if self.changed() {
            record(
                *before,
                self.next.sealed.ok_or(ContractError::InvalidTransition)?,
            )?
        } else {
            if before.sealed.is_none() && !before.state.is_terminal() {
                return Err(ContractError::InvalidTransition);
            }
            *before
        };
        if expected != *after {
            return Err(ContractError::ContentConflict);
        }
        Ok(())
    }
}

impl EvaluationState {
    /// Record this claim's original cohort seal without an actor-supplied owner
    /// frame. The publishing owner must resolve actual registry membership; this
    /// method proves the immutable declaration and claim frame and preserves
    /// the exact target, receipt, generation, attempt, evidence and prior fence.
    /// Historical receipt adoption, evaluator deadlines and handler policies
    /// do not prevent cohort recording or authorize any additional report.
    pub fn seal_claim(
        self,
        declaration: &Declaration,
        expected: &Binding,
        claim: &ClaimState,
    ) -> Result<SealTransition, ContractError> {
        self.binding.check(expected)?;
        let evaluation = self.bind(declaration)?;
        if evaluation.ledger() != claim.binding().ledger {
            return Err(ContractError::WrongLedger);
        }
        if evaluation.claim().0 != claim.binding().object.0 {
            return Err(ContractError::WrongObject);
        }
        claim.acceptance().check(claim.binding(), claim.issuer())?;
        claim.acceptance().check_declaration(declaration)?;
        if let Target::Admission { claim: pinned } | Target::Increment { claim: pinned, .. } =
            self.target
        {
            Binding {
                revision: pinned.revision,
                ..claim.binding()
            }
            .check(&pinned)?;
            if claim.binding().revision < pinned.revision {
                return Err(ContractError::StaleRevision);
            }
        }
        let cause = claim_cause(claim)?;
        if self.sealed == Some(ContentHash([0; 32])) {
            return Err(ContractError::InvalidCut);
        }
        let next = if self.sealed.is_some() || self.state.is_terminal() {
            self
        } else {
            record(self, cause)?
        };
        let transition = SealTransition {
            previous: self,
            next,
        };
        transition.check(&self, &next)?;
        Ok(transition)
    }
}

/// Shared primitive for the legacy checked owner view and actual-claim path.
/// No other state fields change, including a pre-existing suppression or fence.
pub(super) fn record(
    previous: EvaluationState,
    cause: ContentHash,
) -> Result<EvaluationState, ContractError> {
    if cause == ContentHash([0; 32]) {
        return Err(ContractError::InvalidCut);
    }
    if previous.sealed.is_some() || previous.state.is_terminal() {
        return Err(ContractError::InvalidTransition);
    }
    let mut next = previous;
    next.binding = previous.binding.next()?;
    next.sealed = Some(cause);
    if !previous.begun && previous.suppression.is_none() {
        next.suppression = Some(Suppression::CohortSealed(cause));
    }
    Ok(next)
}

fn claim_cause(claim: &ClaimState) -> Result<ContentHash, ContractError> {
    if !claim.local_complete() && !claim.is_terminal() {
        return Err(ContractError::InvalidTransition);
    }
    let sequence = claim.local_sealed_at().ok_or(ContractError::InvalidCut)?;
    if sequence.0 == 0 || claim.created().0 == 0 || sequence < claim.created() {
        return Err(ContractError::InvalidCut);
    }
    let binding = claim.binding();
    // Process-local cohort identity, stable across graph release and subsequent
    // claim revisions. It does not replace a terminal cause or durable hash.
    let mut hash = blake3::Hasher::new_derive_key("focal native evaluation cohort seal");
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&sequence.0.to_be_bytes());
    let cause = ContentHash(*hash.finalize().as_bytes());
    if cause == ContentHash([0; 32]) {
        return Err(ContractError::InvalidCut);
    }
    Ok(cause)
}
