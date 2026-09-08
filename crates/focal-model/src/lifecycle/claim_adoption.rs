//! Exact issuer-authorized receipt replacement. The token borrows the complete
//! immutable source; the owner applies it to a charged copy and publishes all
//! evaluation fences and receipt records in the same transaction.
use super::*;

#[derive(Debug)]
pub struct ReceiptAdoption<'a> {
    original: &'a ClaimState,
    next: Binding,
    previous: ReceiptEntitlement,
    replacement: ReceiptEntitlement,
    cut: ClaimCut,
}

impl ReceiptAdoption<'_> {
    pub fn claim(&self) -> &ClaimState {
        self.original
    }
    pub fn binding(&self) -> Binding {
        self.original.binding()
    }
    pub fn next_binding(&self) -> Binding {
        self.next
    }
    pub fn previous(&self) -> ReceiptEntitlement {
        self.previous
    }
    pub fn replacement(&self) -> ReceiptEntitlement {
        self.replacement
    }
    pub fn cut(&self) -> ClaimCut {
        self.cut
    }
    pub fn check(&self, current: &ClaimState) -> Result<(), ContractError> {
        current.binding.check(&self.original.binding)?;
        if current != self.original {
            return Err(ContractError::ContentConflict);
        }
        Ok(())
    }
}

impl ClaimState {
    // Shared legacy/native guard. The caller has already checked open(expected).
    pub(super) fn adoption_binding(
        &self,
        principal: Principal,
        previous: ReceiptFence,
        replacement: ReceiptEntitlement,
    ) -> Result<Binding, ContractError> {
        self.working()?;
        principal.require_actor(self.issuer)?;
        self.receipt_matches(previous)?;
        if replacement.holder.is_zero()
            || replacement.fence.receipt.is_zero()
            || replacement.fence.receipt == previous.receipt
            || replacement.fence.epoch <= previous.epoch
        {
            return Err(ContractError::StaleReceipt);
        }
        self.binding.next()
    }

    /// Native replacement advances exactly one epoch. The old Actor intent
    /// continues to accept its historical monotonically increasing epochs.
    /// No source allocation or mutation occurs in this preflight.
    pub fn prepare_receipt_adoption(
        &self,
        expected: &Binding,
        principal: Principal,
        previous: ReceiptFence,
        replacement: ReceiptEntitlement,
        cut: ClaimCut,
    ) -> Result<ReceiptAdoption<'_>, ContractError> {
        self.open(expected)?;
        let next = self.adoption_binding(principal, previous, replacement)?;
        if previous
            .epoch
            .checked_add(1)
            .ok_or(ContractError::Capacity)?
            != replacement.fence.epoch
        {
            return Err(ContractError::StaleReceipt);
        }
        cut.check()?;
        if cut.cause == ContentHash([0; 32]) || cut.position < self.created {
            return Err(ContractError::InvalidCut);
        }
        Ok(ReceiptAdoption {
            original: self,
            next,
            previous: self.receipt_matches(previous)?,
            replacement,
            cut,
        })
    }

    pub fn apply_receipt_adoption(
        &mut self,
        adoption: &ReceiptAdoption<'_>,
    ) -> Result<(), ContractError> {
        adoption.check(self)?;
        self.binding = adoption.next;
        self.receipt = Some(adoption.replacement);
        Ok(())
    }
}
