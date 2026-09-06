//! Allocation-free native request binding. This process-local fingerprint is
//! not a successor content identity, wire format or durable schema commitment.
use super::*;
use crate::ContentHash;

struct Fingerprint(blake3::Hasher);
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
}

impl AcceptancePolicy {
    /// Bind every immutable declaration and ordered slot/check to the native
    /// authored intent. Claim creation revision is assigned by the owner and is
    /// excluded; pinned validation-definition revisions remain significant.
    pub fn intent_fingerprint(&self) -> ContentHash {
        let mut hash = Fingerprint(blake3::Hasher::new_derive_key(
            "focal/native/acceptance-intent/1",
        ));
        hash.field(b"claim");
        hash.binding_identity(self.claim);
        hash.field(&self.issuer.0);
        hash.field(b"declarations");
        hash.count(self.declarations.len());
        for declaration in &self.declarations {
            hash.field(declaration.definition.as_bytes());
            hash.binding_identity(declaration.binding);
            hash.field(&declaration.binding.revision.0.to_be_bytes());
            hash.field(&declaration.index.to_be_bytes());
            hash.mode(declaration.mode);
            match declaration.target {
                ObligationTarget::Slot(slot) => {
                    hash.field(b"slot");
                    hash.field(&slot.to_be_bytes());
                }
                ObligationTarget::Delivery => hash.field(b"delivery"),
                ObligationTarget::Admission => hash.field(b"admission"),
                ObligationTarget::Increment => hash.field(b"increment"),
            }
        }
        hash.field(b"slots");
        hash.count(self.slots.len());
        for slot in &self.slots {
            hash.field(&slot.slot.to_be_bytes());
            hash.field(&slot.missing_declaration_index.to_be_bytes());
            hash.mode(slot.mode);
            hash.count(slot.checks.len());
            for check in &slot.checks {
                hash.field(&check.declaration_index.to_be_bytes());
                hash.field(&check.validation.0);
                hash.mode(check.mode);
            }
        }
        ContentHash(*hash.0.finalize().as_bytes())
    }
}

#[cfg(test)]
#[path = "acceptance_intent_tests.rs"]
mod tests;
