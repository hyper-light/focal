use super::*;
use crate::lifecycle::memory as bytes;

impl AcceptancePolicy {
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        let mut count = bytes::add(
            bytes::allocation::<OwnedSlot>(self.slots.len()),
            bytes::allocation::<DeclaredObligation>(self.declarations.len()),
        )?;
        for slot in &self.slots {
            count = bytes::add(count, bytes::allocation::<CheckPolicy>(slot.checks.len()))?;
        }
        Ok(count)
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        let mut count = bytes::add(
            bytes::allocation::<OwnedSlot>(self.slots.capacity()),
            bytes::allocation::<DeclaredObligation>(self.declarations.capacity()),
        )?;
        for slot in &self.slots {
            count = bytes::add(
                count,
                bytes::allocation::<CheckPolicy>(slot.checks.capacity()),
            )?;
        }
        Ok(count)
    }
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        let mut size = bytes::add(
            bytes::array::<OwnedSlot>(self.slots.len())?,
            bytes::array::<DeclaredObligation>(self.declarations.len())?,
        )?;
        for slot in &self.slots {
            size = bytes::add(size, bytes::array::<CheckPolicy>(slot.checks.len())?)?;
        }
        Ok(size)
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        let mut size = bytes::add(
            bytes::array::<OwnedSlot>(self.slots.capacity())?,
            bytes::array::<DeclaredObligation>(self.declarations.capacity())?,
        )?;
        for slot in &self.slots {
            size = bytes::add(size, bytes::array::<CheckPolicy>(slot.checks.capacity())?)?;
        }
        Ok(size)
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let mut slots = bytes::reserve(self.slots.len())?;
        for slot in &self.slots {
            slots.push(OwnedSlot {
                slot: slot.slot,
                missing_declaration_index: slot.missing_declaration_index,
                mode: slot.mode,
                checks: bytes::copy(&slot.checks)?,
            });
        }
        let copied = Self {
            claim: self.claim,
            issuer: self.issuer,
            slots,
            declarations: bytes::copy(&self.declarations)?,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}

#[cfg(test)]
impl AcceptancePolicy {
    pub(in crate::lifecycle) fn memory_fixture(claim: Binding, issuer: ParticipantId) -> Self {
        use crate::lifecycle::validation::{self as v, DeclarationSpec, Program};
        use crate::{ObjectId, ValidationKind, ValidationPhase};
        let spec = DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(1000),
                ..claim
            },
            claim: ClaimId(claim.object.0),
            issuer,
            declaration_index: 0,
            ..v::tests::specification(ValidationMode::Required, v::tests::programmatic(false))
        };
        let declarations = [
            Declaration::new(Principal::Actor(issuer), spec, v::tests::limits()).unwrap(),
            Declaration::new(
                Principal::Actor(issuer),
                DeclarationSpec {
                    binding: Binding {
                        object: ObjectId::from_u128(1001),
                        ..claim
                    },
                    declaration_index: 1,
                    kind: ValidationKind::Receipt,
                    phase: ValidationPhase::WholeWork,
                    target: TargetDeclaration::Delivery,
                    program: Program::Delivery,
                    ..spec
                },
                v::tests::limits(),
            )
            .unwrap(),
        ];
        let checks = [CheckPolicy {
            declaration_index: 0,
            validation: ValidationId::from_u128(1000),
            mode: ValidationMode::Required,
        }];
        let slot = match spec.target {
            TargetDeclaration::WholeWorkSlot { index, .. } => index,
            _ => panic!("slot fixture"),
        };
        let mut policy = Self::new(
            claim,
            issuer,
            &[SlotPolicy {
                slot,
                missing_declaration_index: 20,
                mode: ValidationMode::Required,
                checks: &checks,
            }],
            &declarations,
            Limits {
                max_slots: 4,
                max_checks: 8,
                max_results: 16,
                max_updates: 8,
            },
        )
        .unwrap();
        policy.slots.reserve_exact(8);
        policy.slots[0].checks.reserve_exact(8);
        policy.declarations.reserve_exact(8);
        policy
    }
}
