//! Exact authored evaluation deadlines close authority without manufacturing a
//! result. The enclosing owner proves registry membership and trusted timer
//! delivery; this contract consumes no participant-selected readiness or policy.
use super::*;
use crate::lifecycle::claim::{ClaimCut, ClaimState};

impl Evaluation<'_> {
    /// Fence this retained target/generation at its immutable declared deadline.
    /// `fired_at` and `cut` come from the trusted publishing owner, which also
    /// checks complete current registration and once-only timer consumption.
    /// This method neither authorizes that ingress nor performs publication.
    ///
    /// Receipt adoption, local sealing and parent terminality do not rewrite a
    /// historical deadline. No current receipt, handler grant, readiness or live
    /// report authority is needed. Terminal results and an earlier explicit
    /// fence remain unchanged after the firing and source frame are checked.
    pub fn fence_deadline(
        &self,
        expected: &Binding,
        claim: &ClaimState,
        deadline: Deadline,
        fired_at: u64,
        cut: ClaimCut,
    ) -> Result<EvaluationState, ContractError> {
        self.binding.check(expected)?;
        if self.ledger() != claim.binding().ledger {
            return Err(ContractError::WrongLedger);
        }
        if self.claim().0 != claim.binding().object.0 {
            return Err(ContractError::WrongObject);
        }
        claim.acceptance().check(claim.binding(), claim.issuer())?;
        claim.acceptance().check_declaration(self.declaration)?;
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
        if deadline != self.deadline() || fired_at < deadline.at {
            return Err(ContractError::InvalidCut);
        }
        if cut.position.0 == 0
            || cut.cause == ContentHash([0; 32])
            || cut.position < claim.created()
            || claim
                .local_sealed_at()
                .is_some_and(|sealed| cut.position < sealed)
        {
            return Err(ContractError::InvalidCut);
        }
        if self.state.is_terminal() || self.fence.is_some() {
            return Ok(self.stored);
        }
        let mut next = self.stored;
        next.binding = self.binding.next()?;
        next.fence = Some(AuthorityFence {
            reason: FenceReason::Deadline(deadline),
            cause: cut.cause,
        });
        Ok(next)
    }
}

#[cfg(test)]
#[path = "validation_deadline_tests.rs"]
mod tests;
