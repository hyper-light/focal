//! Allocation-free native request binding. This process-local fingerprint is
//! not a successor content identity, wire format or durable schema commitment.
use super::*;
use crate::ContentHash;

pub(super) struct Fingerprint(blake3::Hasher);
impl Fingerprint {
    fn field(&mut self, value: &[u8]) {
        // usize widens to u128 without truncation on every supported target.
        self.0.update(&(value.len() as u128).to_be_bytes());
        self.0.update(value);
    }
    fn count(&mut self, count: usize) {
        self.field(&(count as u128).to_be_bytes());
    }
    fn binding_identity(&mut self, value: Binding) {
        self.field(&value.ledger.tenant.0);
        self.field(&value.ledger.session.0);
        self.field(&value.object.0);
        self.field(&value.content.0);
    }
    fn mode(&mut self, mode: ValidationMode) {
        self.field(match mode {
            ValidationMode::Observe => b"observe",
            ValidationMode::Required => b"required",
        });
    }
    pub(super) fn start(claim: Binding, issuer: ParticipantId, declarations: usize) -> Self {
        let mut hash = Fingerprint(blake3::Hasher::new_derive_key(
            "focal/native/acceptance-intent/1",
        ));
        hash.field(b"claim");
        hash.binding_identity(claim);
        hash.field(&issuer.0);
        hash.field(b"declarations");
        hash.count(declarations);
        hash
    }
    pub(super) fn declaration(&mut self, declaration: DeclaredObligation) {
        self.field(declaration.definition.as_bytes());
        self.binding_identity(declaration.binding);
        self.field(&declaration.binding.revision.0.to_be_bytes());
        self.field(&declaration.index.to_be_bytes());
        self.mode(declaration.mode);
        match declaration.target {
            ObligationTarget::Slot(slot) => {
                self.field(b"slot");
                self.field(&slot.to_be_bytes());
            }
            ObligationTarget::Delivery => self.field(b"delivery"),
            ObligationTarget::Admission => self.field(b"admission"),
            ObligationTarget::Increment => self.field(b"increment"),
        }
    }
    pub(super) fn slots(&mut self, count: usize) {
        self.field(b"slots");
        self.count(count);
    }
    pub(super) fn slot(&mut self, slot: crate::lifecycle::claim_descriptor::ClaimSlotFields) {
        self.field(&slot.slot.to_be_bytes());
        self.field(&slot.missing_declaration_index.to_be_bytes());
        self.mode(slot.mode);
        self.count(slot.checks);
    }
    pub(super) fn check(&mut self, check: CheckPolicy) {
        self.field(&check.declaration_index.to_be_bytes());
        self.field(&check.validation.0);
        self.mode(check.mode);
    }
    pub(super) fn finish(self) -> ContentHash {
        ContentHash(*self.0.finalize().as_bytes())
    }
    pub(super) fn finish_slots<'a>(
        mut self,
        slots: impl ExactSizeIterator<Item = SlotPolicy<'a>>,
    ) -> ContentHash {
        use crate::lifecycle::claim_descriptor::ClaimSlotSource;
        self.slots(slots.len());
        for slot in slots {
            self.slot(slot.fields());
            for check in slot.checks {
                self.check(*check);
            }
        }
        self.finish()
    }
}

impl AcceptancePolicy {
    /// Bind every immutable declaration and ordered slot/check to the native
    /// authored intent. Claim creation revision is assigned by the owner and is
    /// excluded; pinned validation-definition revisions remain significant.
    pub fn intent_fingerprint(&self) -> ContentHash {
        let mut hash = Fingerprint::start(self.claim, self.issuer, self.declarations.len());
        for declaration in &self.declarations {
            hash.declaration(*declaration);
        }
        hash.finish_slots(self.slots())
    }
}

#[cfg(test)]
#[path = "acceptance_intent_tests.rs"]
mod tests;
