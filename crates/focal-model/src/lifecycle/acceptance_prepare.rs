//! Complete borrowed acceptance validation and charged owned construction.
use super::*;
use crate::ContentHash;
use crate::lifecycle::validation::{CheckedDeclaration, CheckedDeclarationTarget};
#[path = "acceptance_source.rs"]
mod source;
use crate::lifecycle::{graph::VisitBudget, memory as bytes};
pub use source::{AcceptanceSource, AcceptanceSourcePasses, AcceptanceSourcePlan, PolicyShape};

#[derive(Debug, Clone, Copy)]
enum Declarations<'a> {
    Owned(&'a [Declaration]),
    Borrowed(&'a [&'a Declaration]),
}
impl<'a> Declarations<'a> {
    fn len(self) -> usize {
        match self {
            Self::Owned(rows) => rows.len(),
            Self::Borrowed(rows) => rows.len(),
        }
    }
    fn get(self, index: usize) -> Option<&'a Declaration> {
        match self {
            Self::Owned(rows) => rows.get(index),
            Self::Borrowed(rows) => rows.get(index).copied(),
        }
    }
    fn iter(self) -> DeclarationIter<'a> {
        DeclarationIter {
            source: self,
            index: 0,
        }
    }
}
#[derive(Clone)]
struct DeclarationIter<'a> {
    source: Declarations<'a>,
    index: usize,
}
impl<'a> Iterator for DeclarationIter<'a> {
    type Item = &'a Declaration;
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.source.get(self.index)?;
        self.index = self.index.checked_add(1)?;
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.len();
        (n, Some(n))
    }
}
impl ExactSizeIterator for DeclarationIter<'_> {
    fn len(&self) -> usize {
        self.source.len().saturating_sub(self.index)
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::lifecycle) struct SourceShape {
    pub heap: usize,
    pub allocations: usize,
    pub charge: usize,
    pub intent: ContentHash,
    pub build_visits: usize,
}
#[derive(Debug)]
pub struct AcceptancePlan<'a> {
    claim: Binding,
    issuer: ParticipantId,
    slots: &'a [SlotPolicy<'a>],
    declarations: Declarations<'a>,
    shape: SourceShape,
}
fn record(declaration: &Declaration) -> DeclaredObligation {
    record_checked(declaration.checked_declaration())
}
fn record_checked(declaration: CheckedDeclaration) -> DeclaredObligation {
    DeclaredObligation {
        definition: declaration.definition_stamp(),
        binding: declaration.binding(),
        index: declaration.declaration_index(),
        mode: declaration.mode(),
        target: match declaration.target() {
            CheckedDeclarationTarget::Slot(index) => ObligationTarget::Slot(index),
            CheckedDeclarationTarget::Delivery => ObligationTarget::Delivery,
            CheckedDeclarationTarget::Admission => ObligationTarget::Admission,
            CheckedDeclarationTarget::Increment => ObligationTarget::Increment,
        },
    }
}
fn check_frame(
    claim: Binding,
    issuer: ParticipantId,
    declaration: CheckedDeclaration,
) -> Result<(), ContractError> {
    if declaration.claim() != ClaimId(claim.object.0)
        || declaration.issuer() != issuer
        || declaration.binding().ledger != claim.ledger
        || declaration.binding().object.is_zero()
    {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}
fn check_distinct(
    left: DeclaredObligation,
    right: DeclaredObligation,
) -> Result<(), ContractError> {
    if left.index == right.index || left.binding.object == right.binding.object {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}

fn matches_check(row: DeclaredObligation, check: CheckPolicy, slot: u32) -> bool {
    row.index == check.declaration_index
        && row.binding.object.0 == check.validation.0
        && row.mode == check.mode
        && row.target == ObligationTarget::Slot(slot)
}

/// Shared source validation for existing owned declarations and actual authored
/// descriptors. Iterators borrow; no handler arrays or temporary records copy.
pub(in crate::lifecycle) fn check_sources<'a>(
    claim: Binding,
    issuer: ParticipantId,
    slots: impl ExactSizeIterator<Item = SlotPolicy<'a>> + Clone,
    declarations: impl ExactSizeIterator<Item = &'a Declaration> + Clone,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<SourceShape, ContractError> {
    crate::lifecycle::aggregation::validate_policy_sources(slots.clone(), limits, visits)?;
    if claim.object.is_zero() || issuer.is_zero() || declarations.len() > limits.max_checks {
        return Err(ContractError::InvalidPolicy);
    }
    let mut delivery = false;
    for (position, declaration) in declarations.clone().enumerate() {
        visits.charge(1)?;
        let row = record(declaration);
        check_frame(claim, issuer, declaration.checked_declaration())?;
        for slot in slots.clone() {
            visits.charge(1)?;
            if slot.missing_declaration_index == row.index {
                return Err(ContractError::InvalidPolicy);
            }
        }
        for previous in declarations.clone().take(position) {
            visits.charge(1)?;
            check_distinct(record(previous), row)?;
        }
        if let ObligationTarget::Slot(index) = row.target {
            let mut found = false;
            for slot in slots.clone() {
                visits.charge(1)?;
                if slot.slot == index {
                    for check in slot.checks {
                        visits.charge(1)?;
                        if matches_check(row, *check, index) {
                            found = true;
                            break;
                        }
                    }
                }
                if found {
                    break;
                }
            }
            if !found {
                return Err(ContractError::InvalidPolicy);
            }
        }
        delivery |= declaration.target() == TargetDeclaration::Delivery
            && declaration.mode() == ValidationMode::Required;
    }
    if !delivery {
        return Err(ContractError::InvalidPolicy);
    }
    for slot in slots.clone() {
        visits.charge(1)?;
        for check in slot.checks {
            visits.charge(1)?;
            let mut found = false;
            for declaration in declarations.clone() {
                visits.charge(1)?;
                if matches_check(record(declaration), *check, slot.slot) {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(ContractError::InvalidPolicy);
            }
        }
    }
    let mut heap = bytes::add(
        bytes::array::<OwnedSlot>(slots.len())?,
        bytes::array::<DeclaredObligation>(declarations.len())?,
    )?;
    let mut allocations = bytes::add(
        bytes::allocation::<OwnedSlot>(slots.len()),
        bytes::allocation::<DeclaredObligation>(declarations.len()),
    )?;
    let mut checks = 0;
    for slot in slots.clone() {
        visits.charge(1)?;
        heap = bytes::add(heap, bytes::array::<CheckPolicy>(slot.checks.len())?)?;
        allocations = bytes::add(
            allocations,
            bytes::allocation::<CheckPolicy>(slot.checks.len()),
        )?;
        checks = bytes::add(checks, slot.checks.len())?;
    }
    let mut hash = intent::Fingerprint::start(claim, issuer, declarations.len());
    let mut previous = None;
    for _ in 0..declarations.len() {
        visits.charge(1)?;
        let next = next_declaration(declarations.clone(), previous, visits)?;
        previous = Some(next.declaration_index());
        hash.declaration(record(next));
    }
    visits.charge(bytes::add(slots.len(), checks)?)?;
    let build_visits = build_visits(declarations.len(), slots.len(), checks)?;
    Ok(SourceShape {
        heap,
        allocations,
        charge: bytes::total::<AcceptancePolicy>(heap)?,
        intent: hash.finish_slots(slots),
        build_visits,
    })
}
fn next_declaration<'a>(
    declarations: impl Iterator<Item = &'a Declaration>,
    previous: Option<u32>,
    visits: &mut VisitBudget,
) -> Result<&'a Declaration, ContractError> {
    let mut next: Option<&Declaration> = None;
    for declaration in declarations {
        visits.charge(1)?;
        let index = declaration.declaration_index();
        if previous.is_none_or(|old| index > old)
            && next.is_none_or(|old| index < old.declaration_index())
        {
            next = Some(declaration);
        }
    }
    next.ok_or(ContractError::InvalidPolicy)
}
impl AcceptancePolicy {
    pub fn prepare<'a>(
        claim: Binding,
        issuer: ParticipantId,
        slots: &'a [SlotPolicy<'a>],
        declarations: &'a [Declaration],
        limits: Limits,
    ) -> Result<AcceptancePlan<'a>, ContractError> {
        prepare(
            claim,
            issuer,
            slots,
            Declarations::Owned(declarations),
            limits,
            &mut VisitBudget::new(usize::MAX),
        )
    }
    pub(in crate::lifecycle) fn prepare_borrowed<'a>(
        claim: Binding,
        issuer: ParticipantId,
        slots: &'a [SlotPolicy<'a>],
        declarations: &'a [&'a Declaration],
        limits: Limits,
        visits: &mut VisitBudget,
    ) -> Result<AcceptancePlan<'a>, ContractError> {
        prepare(
            claim,
            issuer,
            slots,
            Declarations::Borrowed(declarations),
            limits,
            visits,
        )
    }
}
fn prepare<'a>(
    claim: Binding,
    issuer: ParticipantId,
    slots: &'a [SlotPolicy<'a>],
    declarations: Declarations<'a>,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<AcceptancePlan<'a>, ContractError> {
    let shape = check_sources(
        claim,
        issuer,
        slots.iter().copied(),
        declarations.iter(),
        limits,
        visits,
    )?;
    Ok(AcceptancePlan {
        claim,
        issuer,
        slots,
        declarations,
        shape,
    })
}
fn reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    let values = bytes::reserve::<T>(count)?;
    if values.capacity() != count {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}
/// The counted build below performs n canonical selections of n sources, n
/// outer selection steps, s slot copies plus c check copies, s retained-heap
/// inspections, and a final hash of n declarations, s slots and c checks.
fn build_visits(n: usize, s: usize, c: usize) -> Result<usize, ContractError> {
    let square = n.checked_mul(n).ok_or(ContractError::Capacity)?;
    let declarations = n.checked_mul(2).ok_or(ContractError::Capacity)?;
    let slots = s.checked_mul(3).ok_or(ContractError::Capacity)?;
    let checks = c.checked_mul(2).ok_or(ContractError::Capacity)?;
    bytes::add(
        bytes::add(square, declarations)?,
        bytes::add(slots, checks)?,
    )
}
impl AcceptancePlan<'_> {
    pub fn construction_charge(&self) -> usize {
        self.shape.charge
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.shape.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.shape.allocations
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.shape.intent
    }
    pub fn build(self, max_bytes: usize) -> Result<AcceptancePolicy, ContractError> {
        self.build_with_visits(max_bytes, &mut VisitBudget::new(usize::MAX))
    }
    pub(in crate::lifecycle) fn build_with_visits(
        self,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<AcceptancePolicy, ContractError> {
        bytes::fits(self.shape.charge, max_bytes)?;
        bytes::fits(self.shape.build_visits, visits.remaining())?;
        let mut declarations = reserve(self.declarations.len())?;
        let mut previous = None;
        // Fallible canonical selection provides an explicit counted bound, with
        // no comparator that can continue sorting after budget exhaustion.
        for _ in 0..self.declarations.len() {
            visits.charge(1)?;
            let next = next_declaration(self.declarations.iter(), previous, visits)?;
            previous = Some(next.declaration_index());
            declarations.push(record(next));
        }
        let mut slots = reserve(self.slots.len())?;
        let mut checks_count = 0;
        for slot in self.slots {
            visits.charge(1)?;
            visits.charge(slot.checks.len())?;
            checks_count = bytes::add(checks_count, slot.checks.len())?;
            let mut checks = reserve(slot.checks.len())?;
            checks.extend_from_slice(slot.checks);
            slots.push(OwnedSlot {
                slot: slot.slot,
                missing_declaration_index: slot.missing_declaration_index,
                mode: slot.mode,
                checks,
            });
        }
        let policy = AcceptancePolicy {
            claim: self.claim,
            issuer: self.issuer,
            slots,
            declarations,
        };
        visits.charge(self.slots.len())?;
        bytes::fits(policy.retained_bytes()?, self.shape.charge)?;
        visits.charge(bytes::add(
            bytes::add(self.declarations.len(), self.slots.len())?,
            checks_count,
        )?)?;
        if policy.intent_fingerprint() != self.shape.intent {
            return Err(ContractError::InvalidPolicy);
        }
        Ok(policy)
    }
}
#[cfg(test)]
#[path = "acceptance_prepare_tests.rs"]
mod tests;
