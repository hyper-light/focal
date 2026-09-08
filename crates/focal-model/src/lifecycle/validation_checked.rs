//! Opaque declaration metadata for bounded acceptance planning. Only checked
//! owned declarations or successfully prepared source plans can produce it.
use super::*;
use crate::lifecycle::validation_descriptor::{ValidationSource, ValidationSourcePlan};

#[cfg(test)]
#[path = "validation_checked_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckedDeclarationTarget {
    Slot(u32),
    Delivery,
    Admission,
    Increment,
}

/// Owns no buffers and borrows no source. The private stamp retains complete
/// target names and policy identity beyond the scalar metadata exposed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedDeclaration {
    binding: Binding,
    claim: ClaimId,
    issuer: ParticipantId,
    declaration_index: u32,
    kind: ValidationKind,
    phase: ValidationPhase,
    mode: ValidationMode,
    target: CheckedDeclarationTarget,
    stamp: DefinitionStamp,
}

impl CheckedDeclaration {
    fn from_checked(fields: DeclarationFields<'_>, stamp: DefinitionStamp) -> Self {
        Self {
            binding: fields.binding,
            claim: fields.claim,
            issuer: fields.issuer,
            declaration_index: fields.declaration_index,
            kind: fields.kind,
            phase: fields.phase,
            mode: fields.mode,
            target: match fields.target {
                TargetDeclaration::WholeWorkSlot { index, .. } => {
                    CheckedDeclarationTarget::Slot(index)
                }
                TargetDeclaration::Delivery => CheckedDeclarationTarget::Delivery,
                TargetDeclaration::Admission => CheckedDeclarationTarget::Admission,
                TargetDeclaration::Increment => CheckedDeclarationTarget::Increment,
            },
            stamp,
        }
    }
    pub fn binding(self) -> Binding {
        self.binding
    }
    pub fn claim(self) -> ClaimId {
        self.claim
    }
    pub fn issuer(self) -> ParticipantId {
        self.issuer
    }
    pub fn declaration_index(self) -> u32 {
        self.declaration_index
    }
    pub fn kind(self) -> ValidationKind {
        self.kind
    }
    pub fn declared_phase(self) -> ValidationPhase {
        self.phase
    }
    pub fn mode(self) -> ValidationMode {
        self.mode
    }
    pub fn target(self) -> CheckedDeclarationTarget {
        self.target
    }
    pub(in crate::lifecycle) fn definition_stamp(self) -> DefinitionStamp {
        self.stamp
    }
}

impl Declaration {
    /// Copies already checked metadata without traversing the declaration body.
    pub fn checked_declaration(&self) -> CheckedDeclaration {
        CheckedDeclaration::from_checked(self.fields(), self.definition_stamp())
    }
}

impl<'a, S: DeclarationSource<'a>> DeclarationSourcePlan<'_, 'a, S> {
    /// Copies the successful preparation result without reading the source again.
    pub fn checked_declaration(&self) -> CheckedDeclaration {
        CheckedDeclaration::from_checked(self.fields(), self.checked_stamp())
    }
}

impl<'a, S: ValidationSource<'a>> ValidationSourcePlan<'_, 'a, S> {
    /// Rechecks the actual authored source and computes its declaration stamp
    /// with the derived content binding. Returns exact additional model visits;
    /// adapters must separately debit their shared parsing allowance.
    pub fn checked_declaration(
        &self,
        max_visits: usize,
    ) -> Result<(CheckedDeclaration, usize), ContractError> {
        let (fields, stamp, visits) = self.checked_declaration_parts(max_visits)?;
        Ok((CheckedDeclaration::from_checked(fields, stamp), visits))
    }
}
