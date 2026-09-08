//! Claim expiry revokes its exact registered evaluations independently of their
//! own deadlines. The enclosing owner proves registry membership and publishes
//! the actual claim transition and these fences atomically.
use super::*;
use crate::ClaimStatus;
use crate::lifecycle::claim::{ClaimState, ClaimTerminalCut};

impl EvaluationState {
    /// Consume an actual expired claim, never caller-selected authority flags.
    /// The claim's private state and explicit terminal cut prove that its
    /// exact authored claim or monitor deadline was applied. Graph failures
    /// cannot use this path: begun reports survive Deadlocked/DependencyFailed.
    pub fn expire_claim(
        self,
        declaration: &Declaration,
        expired: &ClaimState,
    ) -> Result<Self, ContractError> {
        let evaluation = self.bind(declaration)?;
        if evaluation.ledger() != expired.binding().ledger {
            return Err(ContractError::WrongLedger);
        }
        if evaluation.claim().0 != expired.binding().object.0 {
            return Err(ContractError::WrongObject);
        }
        expired
            .acceptance()
            .check(expired.binding(), expired.issuer())?;
        expired.acceptance().check_declaration(declaration)?;
        let Some(ClaimTerminalCut::Explicit(cut)) = expired.terminal_cut() else {
            return Err(ContractError::InvalidTransition);
        };
        if expired.status() != ClaimStatus::Expired
            || cut.position.0 == 0
            || cut.cause == ContentHash([0; 32])
            || cut.position < expired.created()
            || expired
                .local_sealed_at()
                .is_none_or(|sealed| cut.position < sealed)
        {
            return Err(ContractError::InvalidCut);
        }
        if let Target::Admission { claim: pinned } | Target::Increment { claim: pinned, .. } =
            evaluation.target()
        {
            Binding {
                revision: pinned.revision,
                ..expired.binding()
            }
            .check(&pinned)?;
            if expired.binding().revision < pinned.revision {
                return Err(ContractError::StaleRevision);
            }
        }
        if self.state.is_terminal() || self.fence.is_some() {
            return Ok(self);
        }
        let mut next = self;
        next.binding = self.binding.next()?;
        next.fence = Some(AuthorityFence {
            reason: FenceReason::Expiry,
            cause: cut.cause,
        });
        Ok(next)
    }
}
