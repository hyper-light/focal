//! Receipt replacement fences every live member of the actual owner registry,
//! including Admission's receipt-free audit obligations. It creates no result.
use super::*;
use crate::lifecycle::claim::ReceiptAdoption;

impl EvaluationState {
    /// The enclosing owner proves complete stored registry membership. An old
    /// target/generation is not readmitted under the replacement receipt; its
    /// original evidence remains visible, with an exact authority fence.
    pub fn adopt_receipt(
        self,
        declaration: &Declaration,
        adoption: &ReceiptAdoption<'_>,
    ) -> Result<Self, ContractError> {
        let evaluation = self.bind(declaration)?;
        let claim = adoption.claim();
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
            if pinned.revision > claim.binding().revision {
                return Err(ContractError::StaleRevision);
            }
        }
        match (self.target, self.receipt) {
            (Target::Admission { .. }, None) => {}
            (Target::Admission { .. }, Some(_)) | (_, None) => {
                return Err(ContractError::StaleReceipt);
            }
            (_, Some(receipt)) => {
                let previous = adoption.previous().fence;
                if receipt.receipt.is_zero()
                    || receipt.epoch == 0
                    || receipt.epoch > previous.epoch
                    || (receipt.epoch == previous.epoch && receipt != previous)
                {
                    return Err(ContractError::StaleReceipt);
                }
            }
        }
        if self.state.is_terminal() || self.fence.is_some() {
            return Ok(self);
        }
        let mut next = self;
        next.binding = self.binding.next()?;
        next.fence = Some(AuthorityFence {
            reason: FenceReason::ReceiptAdoption,
            cause: adoption.cut().cause,
        });
        Ok(next)
    }
}

#[cfg(test)]
#[path = "validation_adoption_tests.rs"]
mod tests;
