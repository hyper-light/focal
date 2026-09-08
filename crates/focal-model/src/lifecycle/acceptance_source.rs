//! Complete acceptance correspondence over repeated checked declaration values
//! and borrowed slot/check sources. Repeated passes verify the entire sequence,
//! so a mutable provider cannot substitute a different cohort between searches.
use super::*;
use crate::lifecycle::{
    aggregation,
    claim_descriptor::{ClaimSlotFields, ClaimSlotSource},
};

#[cfg(test)]
#[path = "acceptance_source_tests.rs"]
mod tests;

/// Factories and steps must be bounded and allocation-free. Declaration values
/// are capabilities from actual checked model inputs, never caller summaries.
/// Adapter decoding and preparation work needs a separate cumulative allowance
/// across all callbacks; the model budget measures acceptance work only.
pub trait AcceptanceSource {
    type Slot<'s>: ClaimSlotSource
    where
        Self: 's;
    type Slots<'s>: Iterator<Item = Result<Self::Slot<'s>, ContractError>>
    where
        Self: 's;
    type Declarations<'s>: Iterator<Item = Result<CheckedDeclaration, ContractError>>
    where
        Self: 's;
    fn slot_count(&self) -> usize;
    fn declaration_count(&self) -> usize;
    fn slots(&self) -> Self::Slots<'_>;
    fn declarations(&self) -> Self::Declarations<'_>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counts {
    slots: usize,
    declarations: usize,
}
impl Counts {
    fn read(
        source: &impl AcceptanceSource,
        claim: Binding,
        issuer: ParticipantId,
        limits: Limits,
        visits: &mut VisitBudget,
    ) -> Result<Self, ContractError> {
        visits.charge(2)?;
        let value = Self {
            slots: source.slot_count(),
            declarations: source.declaration_count(),
        };
        if value.slots > limits.max_slots || limits.max_results == 0 || limits.max_updates == 0 {
            return Err(ContractError::Capacity);
        }
        if claim.object.is_zero() || issuer.is_zero() || value.declarations > limits.max_checks {
            return Err(ContractError::InvalidPolicy);
        }
        Ok(value)
    }
}
#[derive(Debug, Clone, Copy)]
struct Shape {
    counts: Counts,
    slots: SlotShape,
    declarations_hash: ContentHash,
    quote: SourceShape,
    inspection_visits: usize,
    inspection_passes: AcceptanceSourcePasses,
    build_passes: AcceptanceSourcePasses,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlotShape {
    hash: ContentHash,
    heap: usize,
    allocations: usize,
    checks: usize,
}

/// Complete source scans. Every slot scan visits all slot/check values; every
/// declaration scan consumes the full declared cohort and its terminal probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptanceSourcePasses {
    pub declarations: usize,
    pub slots: usize,
}

#[derive(Debug)]
pub struct AcceptanceSourcePlan<'s, S: AcceptanceSource> {
    source: &'s S,
    claim: Binding,
    issuer: ParticipantId,
    limits: Limits,
    shape: Shape,
}
impl<'s, S: AcceptanceSource> AcceptanceSourcePlan<'s, S> {
    pub fn claim(&self) -> Binding {
        self.claim
    }
    pub fn issuer(&self) -> ParticipantId {
        self.issuer
    }
    pub fn construction_charge(&self) -> usize {
        self.shape.quote.charge
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.shape.quote.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.shape.quote.allocations
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.shape.quote.intent
    }
    pub fn inspection_passes(&self) -> AcceptanceSourcePasses {
        self.shape.inspection_passes
    }
    pub fn build_passes(&self) -> AcceptanceSourcePasses {
        self.shape.build_passes
    }
    pub fn inspection_visits(&self) -> usize {
        self.shape.inspection_visits
    }
    pub fn build_visits(&self) -> usize {
        self.shape.quote.build_visits
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<AcceptancePolicy, ContractError> {
        build(self, max_bytes, max_visits)
    }
}
impl AcceptancePolicy {
    pub fn prepare_source<'s, S: AcceptanceSource>(
        claim: Binding,
        issuer: ParticipantId,
        source: &'s S,
        limits: Limits,
        max_visits: usize,
    ) -> Result<AcceptanceSourcePlan<'s, S>, ContractError> {
        let shape = inspect(source, claim, issuer, limits, max_visits)?;
        Ok(AcceptanceSourcePlan {
            source,
            claim,
            issuer,
            limits,
            shape,
        })
    }
}

fn scaled(value: usize, count: usize) -> Result<usize, ContractError> {
    value.checked_mul(count).ok_or(ContractError::Capacity)
}
fn next<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<T, ContractError> {
    visits.charge(1)?;
    values.next().ok_or(ContractError::InvalidPolicy)?
}
fn end<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    match values.next() {
        None => Ok(()),
        Some(Err(error)) => Err(error),
        Some(Ok(_)) => Err(ContractError::InvalidPolicy),
    }
}

#[derive(Clone, Copy)]
enum SlotEvent {
    Slot(ClaimSlotFields),
    Check(ClaimSlotFields, CheckPolicy),
}

// Fixed charges cover source callbacks, scalar predicates and two complete
// fingerprint consumers. Factory/end probes have separate one-visit debits.
fn scan_slots<S: AcceptanceSource>(
    source: &S,
    parent: (Binding, ParticipantId),
    count: usize,
    limits: Limits,
    expected: Option<SlotShape>,
    visits: &mut VisitBudget,
    mut consume: impl FnMut(SlotEvent, &mut VisitBudget) -> Result<(), ContractError>,
) -> Result<SlotShape, ContractError> {
    let (claim, issuer) = parent;
    visits.charge(512)?;
    let mut hash = intent::Fingerprint::start(claim, issuer, 0);
    hash.slots(count);
    let mut shape = SlotShape {
        hash: ContentHash([0; 32]),
        heap: bytes::array::<OwnedSlot>(count)?,
        allocations: bytes::allocation::<OwnedSlot>(count),
        checks: 0,
    };
    visits.charge(1)?;
    let mut slots = source.slots();
    let mut previous = None;
    for _ in 0..count {
        let slot = next(&mut slots, visits)?;
        visits.charge(256)?;
        let fields = slot.fields();
        aggregation::check_policy_order(previous, fields.slot)?;
        previous = Some(fields.slot);
        shape.checks = bytes::add(shape.checks, fields.checks)?;
        bytes::fits(shape.checks, limits.max_checks)?;
        shape.heap = bytes::add(shape.heap, bytes::array::<CheckPolicy>(fields.checks)?)?;
        shape.allocations = bytes::add(
            shape.allocations,
            bytes::allocation::<CheckPolicy>(fields.checks),
        )?;
        hash.slot(fields);
        consume(SlotEvent::Slot(fields), visits)?;
        visits.charge(1)?;
        let mut checks = slot.checks();
        let mut previous_check = None;
        for _ in 0..fields.checks {
            let check = next(&mut checks, visits)?;
            visits.charge(256)?;
            aggregation::check_policy_order(previous_check, check.declaration_index)?;
            aggregation::check_policy_validation(check)?;
            previous_check = Some(check.declaration_index);
            hash.check(check);
            consume(SlotEvent::Check(fields, check), visits)?;
        }
        end(&mut checks, visits)?;
    }
    end(&mut slots, visits)?;
    shape.hash = hash.finish();
    if expected.is_some_and(|expected| expected != shape) {
        return Err(ContractError::ContentConflict);
    }
    Ok(shape)
}
fn scan_declarations<S: AcceptanceSource>(
    source: &S,
    claim: Binding,
    issuer: ParticipantId,
    count: usize,
    expected: Option<ContentHash>,
    visits: &mut VisitBudget,
    mut consume: impl FnMut(usize, DeclaredObligation, &mut VisitBudget) -> Result<(), ContractError>,
) -> Result<ContentHash, ContractError> {
    visits.charge(512)?;
    let mut hash = intent::Fingerprint::start(claim, issuer, count);
    visits.charge(1)?;
    let mut declarations = source.declarations();
    for position in 0..count {
        let declaration = next(&mut declarations, visits)?;
        visits.charge(512)?;
        check_frame(claim, issuer, declaration)?;
        let row = record_checked(declaration);
        hash.declaration(row);
        consume(position, row, visits)?;
    }
    end(&mut declarations, visits)?;
    let hash = hash.finish();
    if expected.is_some_and(|expected| hash != expected) {
        return Err(ContractError::ContentConflict);
    }
    Ok(hash)
}

#[derive(Clone, Copy)]
struct Frame<'s, S> {
    source: &'s S,
    claim: Binding,
    issuer: ParticipantId,
    counts: Counts,
    limits: Limits,
    slots: SlotShape,
    declarations_hash: ContentHash,
}
impl<S: AcceptanceSource> Frame<'_, S> {
    fn slots(
        &self,
        visits: &mut VisitBudget,
        consume: impl FnMut(SlotEvent, &mut VisitBudget) -> Result<(), ContractError>,
    ) -> Result<(), ContractError> {
        scan_slots(
            self.source,
            (self.claim, self.issuer),
            self.counts.slots,
            self.limits,
            Some(self.slots),
            visits,
            consume,
        )?;
        Ok(())
    }
    fn declarations(
        &self,
        visits: &mut VisitBudget,
        consume: impl FnMut(usize, DeclaredObligation, &mut VisitBudget) -> Result<(), ContractError>,
    ) -> Result<(), ContractError> {
        scan_declarations(
            self.source,
            self.claim,
            self.issuer,
            self.counts.declarations,
            Some(self.declarations_hash),
            visits,
            consume,
        )?;
        Ok(())
    }
    fn select(
        &self,
        previous: Option<u32>,
        visits: &mut VisitBudget,
    ) -> Result<DeclaredObligation, ContractError> {
        let mut selected: Option<DeclaredObligation> = None;
        self.declarations(visits, |_, row, _| {
            if previous.is_none_or(|old| row.index > old)
                && selected.is_none_or(|old| row.index < old.index)
            {
                selected = Some(row);
            }
            Ok(())
        })?;
        selected.ok_or(ContractError::InvalidPolicy)
    }
}

fn inspect<S: AcceptanceSource>(
    source: &S,
    claim: Binding,
    issuer: ParticipantId,
    limits: Limits,
    max_visits: usize,
) -> Result<Shape, ContractError> {
    let mut visits = VisitBudget::new(max_visits);
    let counts = Counts::read(source, claim, issuer, limits, &mut visits)?;
    let slots = scan_slots(
        source,
        (claim, issuer),
        counts.slots,
        limits,
        None,
        &mut visits,
        |_, _| Ok(()),
    )?;
    let declarations_hash = scan_declarations(
        source,
        claim,
        issuer,
        counts.declarations,
        None,
        &mut visits,
        |_, _, _| Ok(()),
    )?;
    let frame = Frame {
        source,
        claim,
        issuer,
        counts,
        limits,
        slots,
        declarations_hash,
    };
    let mut delivery = false;
    frame.declarations(&mut visits, |position, row, visits| {
        frame.declarations(visits, |other, previous, _| {
            if other < position {
                check_distinct(previous, row)?;
            }
            Ok(())
        })?;
        let mut found = !matches!(row.target, ObligationTarget::Slot(_));
        frame.slots(visits, |event, _| {
            match event {
                SlotEvent::Slot(slot) => {
                    aggregation::check_policy_index(slot.missing_declaration_index, row.index)?
                }
                SlotEvent::Check(slot, check) => {
                    if matches_check(row, check, slot.slot) {
                        found = true;
                    }
                }
            }
            Ok(())
        })?;
        if !found {
            return Err(ContractError::InvalidPolicy);
        }
        delivery |=
            row.target == ObligationTarget::Delivery && row.mode == ValidationMode::Required;
        Ok(())
    })?;
    if !delivery {
        return Err(ContractError::InvalidPolicy);
    }
    frame.slots(&mut visits, |event, visits| {
        match event {
            SlotEvent::Slot(slot) => frame.slots(visits, |event, _| {
                if let SlotEvent::Slot(old) = event
                    && old.slot < slot.slot
                {
                    aggregation::check_policy_index(
                        old.missing_declaration_index,
                        slot.missing_declaration_index,
                    )?;
                }
                Ok(())
            })?,
            SlotEvent::Check(slot, check) => {
                frame.slots(visits, |event, _| {
                    match event {
                        SlotEvent::Slot(slot) => aggregation::check_policy_index(
                            slot.missing_declaration_index,
                            check.declaration_index,
                        )?,
                        SlotEvent::Check(old_slot, old) => {
                            if (old_slot.slot, old.declaration_index)
                                < (slot.slot, check.declaration_index)
                            {
                                aggregation::check_policy_pair(old, check)?;
                            }
                        }
                    }
                    Ok(())
                })?;
                let mut found = false;
                frame.declarations(visits, |_, declaration, _| {
                    if matches_check(declaration, check, slot.slot) {
                        found = true;
                    }
                    Ok(())
                })?;
                if !found {
                    return Err(ContractError::InvalidPolicy);
                }
            }
        }
        Ok(())
    })?;
    visits.charge(512)?;
    let mut hash = intent::Fingerprint::start(claim, issuer, counts.declarations);
    let mut previous = None;
    for _ in 0..counts.declarations {
        let row = frame.select(previous, &mut visits)?;
        visits.charge(512)?;
        previous = Some(row.index);
        hash.declaration(row);
    }
    hash.slots(counts.slots);
    frame.slots(&mut visits, |event, _| {
        match event {
            SlotEvent::Slot(fields) => hash.slot(fields),
            SlotEvent::Check(_, check) => hash.check(check),
        }
        Ok(())
    })?;
    let heap = bytes::add(
        slots.heap,
        bytes::array::<DeclaredObligation>(counts.declarations)?,
    )?;
    let allocations = bytes::add(
        slots.allocations,
        bytes::allocation::<DeclaredObligation>(counts.declarations),
    )?;
    let inspection_visits = max_visits
        .checked_sub(visits.remaining())
        .ok_or(ContractError::Capacity)?;
    // Complete reinspection, canonical copying, final-owned hashing and all
    // buffer/capacity walks coexist. Adapter passes are quoted independently.
    let build_visits = bytes::add(
        scaled(inspection_visits, 4)?,
        bytes::add(scaled(heap, 2)?, 1024)?,
    )?;
    let inspection_passes = AcceptanceSourcePasses {
        declarations: bytes::add(
            2,
            bytes::add(scaled(counts.declarations, 2)?, slots.checks)?,
        )?,
        slots: bytes::add(
            3,
            bytes::add(counts.declarations, bytes::add(counts.slots, slots.checks)?)?,
        )?,
    };
    let build_passes = AcceptanceSourcePasses {
        declarations: bytes::add(inspection_passes.declarations, counts.declarations)?,
        slots: bytes::add(inspection_passes.slots, 1)?,
    };
    Ok(Shape {
        inspection_passes,
        build_passes,
        counts,
        slots,
        declarations_hash,
        inspection_visits,
        quote: SourceShape {
            heap,
            allocations,
            charge: bytes::total::<AcceptancePolicy>(heap)?,
            intent: hash.finish(),
            build_visits,
        },
    })
}

fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), ContractError> {
    if values.len() == values.capacity() {
        return Err(ContractError::Capacity);
    }
    values.push(value);
    Ok(())
}
fn build<S: AcceptanceSource>(
    plan: AcceptanceSourcePlan<'_, S>,
    max_bytes: usize,
    max_visits: usize,
) -> Result<AcceptancePolicy, ContractError> {
    let shape = plan.shape;
    bytes::fits(shape.quote.charge, max_bytes)?;
    bytes::fits(shape.quote.build_visits, max_visits)?;
    let mut visits = VisitBudget::new(shape.quote.build_visits);
    let actual = inspect(
        plan.source,
        plan.claim,
        plan.issuer,
        plan.limits,
        visits.remaining(),
    )?;
    visits.charge(actual.inspection_visits)?;
    if actual.counts != shape.counts
        || actual.slots != shape.slots
        || actual.declarations_hash != shape.declarations_hash
        || actual.quote.intent != shape.quote.intent
    {
        return Err(ContractError::ContentConflict);
    }
    let frame = Frame {
        source: plan.source,
        claim: plan.claim,
        issuer: plan.issuer,
        counts: shape.counts,
        limits: plan.limits,
        slots: shape.slots,
        declarations_hash: shape.declarations_hash,
    };
    let mut declarations = reserve(shape.counts.declarations)?;
    let mut previous = None;
    for _ in 0..shape.counts.declarations {
        let row = frame.select(previous, &mut visits)?;
        previous = Some(row.index);
        visits.charge(1)?;
        push(&mut declarations, row)?;
    }
    let mut slots = reserve(shape.counts.slots)?;
    let mut heap = bytes::add(
        bytes::array::<DeclaredObligation>(shape.counts.declarations)?,
        bytes::array::<OwnedSlot>(shape.counts.slots)?,
    )?;
    let mut allocations = bytes::add(
        bytes::allocation::<DeclaredObligation>(shape.counts.declarations),
        bytes::allocation::<OwnedSlot>(shape.counts.slots),
    )?;
    frame.slots(&mut visits, |event, visits| {
        visits.charge(1)?;
        match event {
            SlotEvent::Slot(fields) => {
                heap = bytes::add(heap, bytes::array::<CheckPolicy>(fields.checks)?)?;
                allocations =
                    bytes::add(allocations, bytes::allocation::<CheckPolicy>(fields.checks))?;
                bytes::fits(heap, shape.quote.heap)?;
                bytes::fits(allocations, shape.quote.allocations)?;
                let checks = reserve(fields.checks)?;
                push(
                    &mut slots,
                    OwnedSlot {
                        slot: fields.slot,
                        missing_declaration_index: fields.missing_declaration_index,
                        mode: fields.mode,
                        checks,
                    },
                )?;
            }
            SlotEvent::Check(fields, check) => {
                let slot = slots.last_mut().ok_or(ContractError::InvalidPolicy)?;
                if slot.slot != fields.slot {
                    return Err(ContractError::InvalidPolicy);
                }
                push(&mut slot.checks, check)?;
            }
        }
        Ok(())
    })?;
    let policy = AcceptancePolicy {
        claim: plan.claim,
        issuer: plan.issuer,
        slots,
        declarations,
    };
    visits.charge(bytes::add(scaled(shape.counts.slots, 2)?, 1)?)?;
    bytes::fits(policy.retained_bytes()?, shape.quote.charge)?;
    bytes::fits(policy.heap_allocations()?, shape.quote.allocations)?;
    visits.charge(bytes::add(
        512,
        bytes::add(
            scaled(shape.counts.declarations, 512)?,
            scaled(bytes::add(shape.counts.slots, shape.slots.checks)?, 256)?,
        )?,
    )?)?;
    if policy.intent_fingerprint() != shape.quote.intent {
        return Err(ContractError::ContentConflict);
    }
    Ok(policy)
}
