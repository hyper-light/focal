//! Baseline native posting from the actual owner-held claim.
//!
//! The native profile has no configurable standing/target capability policy.
//! An authenticated issuer may post its structurally valid Generated claim to
//! its nonzero participant address; this requires no global participant registry.
//! Future configurable policy must extend this checked boundary before use, not
//! reuse it as a bypass. Evaluator capability requirements belong to evaluation
//! begin and are never established by posting.
use super::{Binding, ClaimState, ClaimStatus, ContractError, Principal};
use crate::{Cause, ObjectKind};

struct PostingPlan {
    next: Binding,
}

impl PostingPlan {
    fn prepare(
        claim: &ClaimState,
        principal: Principal,
        expected: Binding,
    ) -> Result<Self, ContractError> {
        claim.binding.check(&expected)?;
        principal.require_actor(claim.issuer)?;
        if claim.status != ClaimStatus::Generated
            || claim.receipt.is_some()
            || !claim.responses.is_empty()
            || claim.local_complete
            || claim.local_sealed_at.is_some()
            || claim.terminal_cut.is_some()
            || claim.scopes.released()
        {
            return Err(ContractError::InvalidTransition);
        }
        if claim.binding.ledger.tenant.is_zero() || claim.binding.ledger.session.is_zero() {
            return Err(ContractError::WrongLedger);
        }
        if claim.issuer.is_zero()
            || claim.subject.is_zero()
            || claim.binding.object.is_zero()
            || claim.created.0 == 0
        {
            return Err(ContractError::InvalidTarget);
        }
        if claim.binding.revision.0 == 0 {
            return Err(ContractError::StaleRevision);
        }
        claim.acceptance.check(claim.binding, claim.issuer)?;
        // Child registration may advance a Generated parent's lifecycle
        // revision. Its original immutable lineage binding remains unchanged.
        claim.lineage.check_binding(&Binding {
            revision: claim.lineage.binding().revision,
            ..claim.binding
        })?;
        match claim.lineage.cause() {
            Cause::Root(id) if id.is_zero() => return Err(ContractError::InvalidTarget),
            Cause::Claim(id) if id.is_zero() || id.0 == claim.binding.object.0 => {
                return Err(ContractError::InvalidTarget);
            }
            Cause::Root(_) | Cause::Claim(_) => {}
        }
        for correction in claim.lineage.corrections() {
            if correction.predecessor.ledger != claim.binding.ledger {
                return Err(ContractError::WrongLedger);
            }
            if correction.predecessor.kind != ObjectKind::Claim
                || correction.predecessor.id.is_zero()
                || correction.predecessor.id == claim.binding.object
            {
                return Err(ContractError::InvalidTarget);
            }
        }
        Ok(Self {
            next: claim.binding.next()?,
        })
    }
}

impl ClaimState {
    /// Post an actual claim under the native baseline structural policy.
    ///
    /// The owner first verifies its retained full definitions against this
    /// claim's acceptance manifest, then publishes this copied row with every
    /// Admission materialization, registry entry, event and request outcome.
    /// Mandatory creation already established complete immutable ancestry; this
    /// method checks its retained identity without accepting a caller's graph or
    /// permission flags. It neither acquires a receipt nor starts an evaluator.
    pub fn post_owned(
        &mut self,
        principal: Principal,
        expected: Binding,
    ) -> Result<(), ContractError> {
        let plan = PostingPlan::prepare(self, principal, expected)?;
        self.binding = plan.next;
        self.status = ClaimStatus::Posted;
        Ok(())
    }
}

#[cfg(test)]
#[path = "claim_posting_tests.rs"]
mod tests;
