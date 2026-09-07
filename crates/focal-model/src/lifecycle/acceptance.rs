//! Owned declaration manifest and bounded, append-only evaluation registration.
//! The claim owner retains this registry with its effective mutation state. All
//! materialization goes through `materialize`; sealing closes the increment target
//! set, not the independently progressing evaluation lifecycles.
use super::{CheckPolicy, Limits, SlotPolicy, reserved, same_content, validate_policies};
#[path = "acceptance_intent.rs"]
mod intent;
#[path = "acceptance_memory.rs"]
mod memory;
#[path = "acceptance_support.rs"]
mod support;
use crate::lifecycle::validation::{
    AcceptedResult, Declaration, DefinitionStamp, Evaluation, Materialization, Target,
    TargetDeclaration,
};
use crate::lifecycle::{Binding, ContractError, Principal};
use crate::{ClaimId, ParticipantId, ReceiptFence, ValidationId, ValidationMode};

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub(super) struct OwnedSlot {
    pub slot: u32,
    pub missing_declaration_index: u32,
    pub mode: ValidationMode,
    pub checks: Vec<CheckPolicy>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationTarget {
    Slot(u32),
    Delivery,
    Admission,
    Increment,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredObligation {
    definition: DefinitionStamp,
    binding: Binding,
    index: u32,
    mode: ValidationMode,
    target: ObligationTarget,
}
impl DeclaredObligation {
    pub(in crate::lifecycle) fn definition_stamp(self) -> DefinitionStamp {
        self.definition
    }
    pub fn binding(self) -> Binding {
        self.binding
    }
    pub fn index(self) -> u32 {
        self.index
    }
    pub fn mode(self) -> ValidationMode {
        self.mode
    }
    pub fn target(self) -> ObligationTarget {
        self.target
    }
}

/// Complete immutable claim acceptance manifest. It is owned by ClaimState,
/// rather than reconstructed from whichever results a caller happens to supply.
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub struct AcceptancePolicy {
    claim: Binding,
    issuer: ParticipantId,
    pub(super) slots: Vec<OwnedSlot>,
    declarations: Vec<DeclaredObligation>,
}
impl AcceptancePolicy {
    /// Includes declared output slots with no explicit validation checks.
    pub fn has_slot(&self, slot: u32) -> bool {
        self.slots
            .binary_search_by_key(&slot, |row| row.slot)
            .is_ok()
    }
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn new(
        claim: Binding,
        issuer: ParticipantId,
        slots: &[SlotPolicy<'_>],
        declarations: &[Declaration],
        limits: Limits,
    ) -> Result<Self, ContractError> {
        validate_policies(slots, limits)?;
        if claim.object.is_zero() || issuer.is_zero() || declarations.len() > limits.max_checks {
            return Err(ContractError::InvalidPolicy);
        }
        let mut records = reserved(declarations.len())?;
        for declaration in declarations {
            if declaration.claim() != ClaimId(claim.object.0)
                || declaration.binding().ledger != claim.ledger
                || declaration.issuer() != issuer
            {
                return Err(ContractError::InvalidPolicy);
            }
            let target = match declaration.target() {
                TargetDeclaration::WholeWorkSlot { index, .. } => ObligationTarget::Slot(index),
                TargetDeclaration::Delivery => ObligationTarget::Delivery,
                TargetDeclaration::Admission => ObligationTarget::Admission,
                TargetDeclaration::Increment => ObligationTarget::Increment,
            };
            records.push(DeclaredObligation {
                definition: declaration.definition_stamp(),
                binding: declaration.binding(),
                index: declaration.declaration_index(),
                mode: declaration.mode(),
                target,
            });
        }
        Self::from_records(claim, issuer, slots, records, limits)
    }
    fn from_records(
        claim: Binding,
        issuer: ParticipantId,
        slots: &[SlotPolicy<'_>],
        mut declarations: Vec<DeclaredObligation>,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        validate_policies(slots, limits)?;
        if declarations.len() > limits.max_checks {
            return Err(ContractError::Capacity);
        }
        declarations.sort_unstable_by_key(|record| record.index);
        for (position, record) in declarations.iter().enumerate() {
            if record.binding.object.is_zero()
                || record.binding.ledger != claim.ledger
                || slots
                    .iter()
                    .any(|slot| slot.missing_declaration_index == record.index)
                || declarations.iter().take(position).any(|old| {
                    old.index == record.index || old.binding.object == record.binding.object
                })
            {
                return Err(ContractError::InvalidPolicy);
            }
            if let ObligationTarget::Slot(slot) = record.target
                && !slots.iter().any(|policy| {
                    policy.slot == slot
                        && policy.checks.iter().any(|check| {
                            check.declaration_index == record.index
                                && check.validation.0 == record.binding.object.0
                                && check.mode == record.mode
                        })
                })
            {
                return Err(ContractError::InvalidPolicy);
            }
        }
        if declarations
            .iter()
            .filter(|record| {
                record.target == ObligationTarget::Delivery
                    && record.mode == ValidationMode::Required
            })
            .count()
            == 0
        {
            return Err(ContractError::InvalidPolicy);
        }
        let mut owned = reserved(slots.len())?;
        for slot in slots {
            let mut checks = reserved(slot.checks.len())?;
            for check in slot.checks {
                if !declarations.iter().any(|record| {
                    record.index == check.declaration_index
                        && record.binding.object.0 == check.validation.0
                        && record.mode == check.mode
                        && record.target == ObligationTarget::Slot(slot.slot)
                }) {
                    return Err(ContractError::InvalidPolicy);
                }
                checks.push(*check);
            }
            owned.push(OwnedSlot {
                slot: slot.slot,
                missing_declaration_index: slot.missing_declaration_index,
                mode: slot.mode,
                checks,
            });
        }
        Ok(Self {
            claim,
            issuer,
            slots: owned,
            declarations,
        })
    }
    pub fn check(&self, claim: Binding, issuer: ParticipantId) -> Result<(), ContractError> {
        same_content(self.claim, claim)?;
        if self.issuer != issuer {
            return Err(ContractError::WrongActor);
        }
        Ok(())
    }
    pub fn claim(&self) -> Binding {
        self.claim
    }
    pub fn issuer(&self) -> ParticipantId {
        self.issuer
    }
    pub fn declarations(&self) -> &[DeclaredObligation] {
        &self.declarations
    }
    pub(super) fn within(&self, limits: Limits) -> Result<(), ContractError> {
        if self.slots.len() > limits.max_slots
            || self.declarations.len() > limits.max_checks
            || limits.max_results == 0
            || limits.max_updates == 0
        {
            return Err(ContractError::Capacity);
        }
        Ok(())
    }
    pub(super) fn copy(&self) -> Result<Self, ContractError> {
        self.try_copy(self.copy_charge()?)
    }
    pub(super) fn declaration(
        &self,
        validation: ValidationId,
    ) -> Result<DeclaredObligation, ContractError> {
        self.declarations
            .iter()
            .find(|row| row.binding.object.0 == validation.0)
            .copied()
            .ok_or(ContractError::InvalidPolicy)
    }
    pub(super) fn check_result(
        &self,
        result: AcceptedResult,
    ) -> Result<DeclaredObligation, ContractError> {
        if result.claim() != ClaimId(self.claim.object.0) || result.ledger() != self.claim.ledger {
            return Err(ContractError::InvalidTarget);
        }
        let record = self.declaration(result.validation())?;
        same_content(record.binding, result.binding())?;
        if record.definition_stamp() != result.definition_stamp()
            || record.index != result.declaration_index()
            || record.mode != result.mode()
        {
            return Err(ContractError::InvalidPolicy);
        }
        let target = match result.target() {
            Target::Artifact { slot, .. } | Target::MissingSlot { slot, .. } => {
                ObligationTarget::Slot(slot)
            }
            Target::Delivery { .. } => ObligationTarget::Delivery,
            Target::Admission { claim } => {
                same_content(self.claim, claim)?;
                ObligationTarget::Admission
            }
            Target::Increment { claim, artifact } => {
                same_content(self.claim, claim)?;
                if artifact.ledger != claim.ledger {
                    return Err(ContractError::WrongLedger);
                }
                ObligationTarget::Increment
            }
        };
        if record.target != target {
            return Err(ContractError::InvalidTarget);
        }
        Ok(record)
    }
    #[cfg(test)]
    pub(crate) fn fixture(
        claim: Binding,
        issuer: ParticipantId,
        slots: &[SlotPolicy<'_>],
        delivery: Binding,
        delivery_index: u32,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        let mut records = Vec::new();
        records.push(DeclaredObligation {
            definition: DefinitionStamp::fixture(),
            binding: delivery,
            index: delivery_index,
            mode: ValidationMode::Required,
            target: ObligationTarget::Delivery,
        });
        for slot in slots {
            for check in slot.checks {
                records.push(DeclaredObligation {
                    definition: DefinitionStamp::fixture(),
                    binding: Binding {
                        object: crate::ObjectId(check.validation.0),
                        ..claim
                    },
                    index: check.declaration_index,
                    mode: check.mode,
                    target: ObligationTarget::Slot(slot.slot),
                });
            }
        }
        Self::from_records(claim, issuer, slots, records, limits)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredEvaluation {
    definition: DefinitionStamp,
    binding: Binding,
    target: Target,
    generation: u64,
    receipt: Option<ReceiptFence>,
    index: u32,
    mode: ValidationMode,
}
impl RegisteredEvaluation {
    pub(in crate::lifecycle) fn definition_stamp(self) -> DefinitionStamp {
        self.definition
    }
    pub fn binding(self) -> Binding {
        self.binding
    }
    pub fn target(self) -> Target {
        self.target
    }
    pub fn generation(self) -> u64 {
        self.generation
    }
    pub fn receipt(self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn declaration_index(self) -> u32 {
        self.index
    }
    pub fn mode(self) -> ValidationMode {
        self.mode
    }
    pub(super) fn matches(self, result: AcceptedResult) -> bool {
        self.definition == result.definition_stamp()
            && self.binding.object == result.binding().object
            && self.binding.content == result.binding().content
            && self.target == result.target()
            && self.generation == result.generation()
            && self.receipt == result.receipt()
    }
}
pub(super) fn same_target(left: Target, right: Target) -> bool {
    match (left, right) {
        (Target::Admission { claim: left }, Target::Admission { claim: right })
        | (Target::Delivery { response: left }, Target::Delivery { response: right }) => {
            same_content(left, right).is_ok()
        }
        (
            Target::Increment {
                claim: left,
                artifact: a,
            },
            Target::Increment {
                claim: right,
                artifact: b,
            },
        ) => same_content(left, right).is_ok() && same_content(a, b).is_ok(),
        (
            Target::Artifact {
                response: left,
                slot: x,
                artifact: a,
            },
            Target::Artifact {
                response: right,
                slot: y,
                artifact: b,
            },
        ) => x == y && same_content(left, right).is_ok() && same_content(a, b).is_ok(),
        (
            Target::MissingSlot {
                response: left,
                slot: x,
            },
            Target::MissingSlot {
                response: right,
                slot: y,
            },
        ) => x == y && same_content(left, right).is_ok(),
        _ => false,
    }
}

/// Append-only owner registry. Registration precedes exposing an evaluation to
/// peers; after sealing, existing runs may finish but no new target can appear.
/// It must be retained, never rebuilt from a request's selected result subset.
#[derive(Debug)]
pub struct EvaluationRegistry {
    policy: AcceptancePolicy,
    rows: Vec<RegisteredEvaluation>,
    max_rows: usize,
    sealed: bool,
    increments_sealed: bool,
}
impl EvaluationRegistry {
    pub fn new(
        claim: &crate::lifecycle::claim::ClaimState,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        Self::from_policy(claim.acceptance(), limits)
    }
    pub(super) fn from_policy(
        policy: &AcceptancePolicy,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        policy.within(limits)?;
        Ok(Self {
            policy: policy.copy()?,
            rows: reserved(limits.max_results)?,
            max_rows: limits.max_results,
            sealed: false,
            increments_sealed: false,
        })
    }
    pub fn policy(&self) -> &AcceptancePolicy {
        &self.policy
    }
    pub fn rows(&self) -> &[RegisteredEvaluation] {
        &self.rows
    }
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }
    pub fn increment_targets_sealed(&self) -> bool {
        self.increments_sealed
    }
    pub fn sealed_targets(&self) -> Option<SealedTargets<'_>> {
        self.sealed.then_some(SealedTargets { registry: self })
    }
    pub fn seal_increment_targets(&mut self) {
        self.increments_sealed = true;
    }
    pub fn materialize<'a>(
        &mut self,
        principal: Principal,
        declaration: &'a Declaration,
        materialization: Materialization<'_>,
    ) -> Result<Evaluation<'a>, ContractError> {
        let evaluation = Evaluation::materialize(principal, declaration, materialization)?;
        self.register(&evaluation)?;
        Ok(evaluation)
    }
    /// Imports a checked owner materialization before it is externally exposed.
    pub fn register(&mut self, evaluation: &Evaluation<'_>) -> Result<(), ContractError> {
        if evaluation.state() != crate::lifecycle::validation::State::Ready
            || evaluation.has_begun()
            || evaluation.sealed().is_some()
            || evaluation.fence().is_some()
            || evaluation.last_result().is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        let record = self.policy.declaration(evaluation.validation())?;
        same_content(record.binding, evaluation.binding())?;
        if evaluation.claim() != ClaimId(self.policy.claim.object.0)
            || evaluation.ledger() != self.policy.claim.ledger
            || record.definition_stamp() != evaluation.definition_stamp()
            || record.index != evaluation.declaration_index()
            || record.mode != evaluation.mode()
        {
            return Err(ContractError::InvalidPolicy);
        }
        let target = match evaluation.target() {
            Target::Artifact { slot, .. } | Target::MissingSlot { slot, .. } => {
                ObligationTarget::Slot(slot)
            }
            Target::Delivery { .. } => ObligationTarget::Delivery,
            Target::Admission { claim } => {
                same_content(self.policy.claim, claim)?;
                ObligationTarget::Admission
            }
            Target::Increment { claim, .. } => {
                same_content(self.policy.claim, claim)?;
                ObligationTarget::Increment
            }
        };
        if target != record.target {
            return Err(ContractError::InvalidTarget);
        }
        let row = RegisteredEvaluation {
            definition: evaluation.definition_stamp(),
            binding: evaluation.binding(),
            target: evaluation.target(),
            generation: evaluation.generation(),
            receipt: evaluation.receipt(),
            index: record.index,
            mode: record.mode,
        };
        if let Some(old) = self.rows.iter().find(|old| {
            old.binding.object == row.binding.object && same_target(old.target, row.target)
        }) {
            return if *old == row {
                Ok(())
            } else {
                Err(ContractError::StaleEvaluation)
            };
        }
        if self.sealed || (self.increments_sealed && record.target == ObligationTarget::Increment) {
            return Err(ContractError::InvalidTransition);
        }
        if self.rows.len() >= self.max_rows {
            return Err(ContractError::Capacity);
        }
        self.rows.push(row);
        Ok(())
    }
    pub fn seal_targets(&mut self) -> SealedTargets<'_> {
        self.sealed = true;
        self.increments_sealed = true;
        SealedTargets { registry: self }
    }
    pub(super) fn check_result(&self, result: AcceptedResult) -> Result<(), ContractError> {
        self.policy.check_result(result)?;
        if !self.rows.iter().any(|row| row.matches(result)) {
            return Err(ContractError::StaleEvaluation);
        }
        Ok(())
    }
}
/// The complete registered target set, never a caller-selected slice.
pub struct SealedTargets<'a> {
    registry: &'a EvaluationRegistry,
}
impl SealedTargets<'_> {
    pub fn rows(&self) -> &[RegisteredEvaluation] {
        self.registry.rows()
    }
    pub fn policy(&self) -> &AcceptancePolicy {
        self.registry.policy()
    }
}

/// Test fixture still passes the real mandatory Receipt declaration admission.
#[cfg(test)]
pub(crate) fn acceptance_for(claim: Binding, issuer: ParticipantId) -> AcceptancePolicy {
    use crate::lifecycle::validation::{DeclarationSpec, Program};
    use crate::{Deadline, ObjectId, TimerId, ValidationKind, ValidationPhase};
    let declaration = Declaration::new(
        Principal::Actor(issuer),
        DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(900),
                ..claim
            },
            claim: ClaimId(claim.object.0),
            issuer,
            declaration_index: 900,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: TargetDeclaration::Delivery,
            program: Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 100,
            },
        },
        crate::lifecycle::validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap();
    AcceptancePolicy::new(
        claim,
        issuer,
        &[],
        &[declaration],
        Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 32,
            max_updates: 8,
        },
    )
    .unwrap()
}

#[cfg(test)]
#[path = "acceptance_retention_tests.rs"]
mod retention_tests;
