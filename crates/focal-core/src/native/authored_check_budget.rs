//! Bounds the publication audit separately from construction. Every term comes
//! from retained input dimensions; no lookup resets the audit's shared budget.
use super::*;

fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b)
        .ok_or(NativeError::Capacity("authored audit visits"))
}
fn pairs(n: usize) -> Result<usize, NativeError> {
    let previous = n.saturating_sub(1);
    let half = |value: usize| {
        value
            .checked_div(2)
            .ok_or(NativeError::Capacity("authored audit visits"))
    };
    if n.is_multiple_of(2) {
        mul(half(n)?, previous)
    } else {
        mul(n, half(previous)?)
    }
}

#[derive(Default)]
pub(super) struct BodyShape {
    pub relations: usize,
    pub slots: usize,
    pub checks: usize,
    pub scopes: usize,
    pub pins: usize,
}

pub(super) fn body_shape(
    body: &ClaimDescriptor,
    limits: NativeLimits,
    visits: &mut VisitBudget,
) -> Result<BodyShape, NativeError> {
    let mut shape = BodyShape {
        relations: body.relations().len(),
        slots: body.slots().len(),
        scopes: body.scopes().len(),
        pins: body.requirements().len(),
        checks: 0,
    };
    within(shape.relations, limits.plan_edges)?;
    within(shape.scopes, limits.plan_edges)?;
    within(
        shape.pins,
        limits.definitions.min(limits.range.max_batch_entries),
    )?;
    within(shape.slots, limits.work_artifacts_per_cycle)?;
    for slot in body.slots() {
        visits.charge(1)?;
        shape.checks = add(shape.checks, slot.checks.len())?;
        within(shape.checks, limits.evaluations_per_claim)?;
    }
    Ok(shape)
}

pub(super) fn state_shape(
    state: &ClaimState,
    limits: NativeLimits,
    visits: &mut VisitBudget,
) -> Result<usize, NativeError> {
    within(state.graph().obligations().len(), limits.plan_edges)?;
    within(state.lineage().corrections().len(), limits.plan_edges)?;
    within(
        state.acceptance().declarations().len(),
        limits.definitions.min(limits.range.max_batch_entries),
    )?;
    within(
        state.acceptance().slot_count(),
        limits.work_artifacts_per_cycle,
    )?;
    let mut checks = 0;
    for slot in state.acceptance().slots() {
        visits.charge(1)?;
        checks = add(checks, slot.checks.len())?;
        within(checks, limits.evaluations_per_claim)?;
    }
    Ok(checks)
}

pub(super) fn claim_heap_visits(body: &ClaimDescriptor) -> Result<usize, NativeError> {
    // retained_heap_bytes and heap_allocations each visit scopes and slots.
    mul(2, add(body.scopes().len(), body.slots().len())?)
}

pub(super) fn quote(
    plan: &super::super::super::transactions::Plan,
    extras: &Extras,
    limits: NativeLimits,
) -> Result<usize, NativeError> {
    let e = extras.rows.len();
    let r = plan.rows.len();
    within(e, limits.range.max_batch_entries)?;
    within(r, limits.plan_nodes)?;
    let prefix = extras
        .control_graph
        .as_ref()
        .map_or(0, |proof| proof.prefix().len());
    within(prefix, limits.range.max_batch_entries)?;
    let mut inspection = VisitBudget::new(limits.plan_edges);
    let (mut claims, mut definitions, mut claim_indices, mut definition_indices, mut objects) =
        (0, 0, 0, 0, 0);
    let (mut pins, mut pin_pairs, mut max_pins, mut max_declarations) = (0, 0, 0, 0);
    let (mut body_walks, mut heap_walks, mut state_checks) = (0, 0, 0);
    for extra in &extras.rows {
        inspection.charge(1)?;
        match &extra.row {
            Row::ClaimContent(owned) => {
                claims = add(claims, 1)?;
                let body = owned.get().ok_or(ContractError::InvalidPolicy)?;
                let shape = body_shape(body, limits, &mut inspection)?;
                pins = add(pins, shape.pins)?;
                pin_pairs = add(pin_pairs, mul(shape.pins, shape.pins)?)?;
                max_pins = max_pins.max(shape.pins);
                body_walks = add(
                    body_walks,
                    add(shape.relations, add(shape.slots, shape.checks)?)?,
                )?;
                heap_walks = add(heap_walks, claim_heap_visits(body)?)?;
            }
            Row::Definition(_) => definitions = add(definitions, 1)?,
            Row::ClaimIdentity(_) => claim_indices = add(claim_indices, 1)?,
            Row::DefinitionIdentity(_) => definition_indices = add(definition_indices, 1)?,
            Row::CreationResult(result) => {
                within(result.get().entries().len(), limits.range.max_batch_entries)?;
                objects = add(objects, result.get().entries().len())?;
            }
            _ => {}
        }
    }
    for state in &plan.rows {
        inspection.charge(1)?;
        state_checks = add(state_checks, state_shape(state, limits, &mut inspection)?)?;
        max_declarations = max_declarations.max(state.acceptance().declarations().len());
    }
    // One initial result lookup; claim identity and per-pin lookups; definition
    // owner/index lookups; both index reverse lookups; result identity lookups.
    let lookups = add(
        1,
        add(
            claims,
            add(
                pins,
                add(
                    mul(2, definitions)?,
                    add(claim_indices, add(definition_indices, objects)?)?,
                )?,
            )?,
        )?,
    )?;
    let mut audit = mul(e, lookups)?;
    for term in [
        add(pairs(e)?, e)?,
        add(pairs(r)?, r)?,        // Unique keys and final claim rows.
        e,                         // Dispatch over extra rows.
        add(mul(2, e)?, objects)?, // Fingerprint shape walk and cached identity scan.
        mul(claims, r)?,           // Locate each claim's final state.
        body_walks,
        state_checks,
        heap_walks,
        pin_pairs,
        mul(pins, max_declarations)?, // Pins and acceptance membership.
        mul(definitions, max_pins)?,  // Definition-to-owner requirement membership.
        mul(definitions, prefix)?,    // Original definition facts in control history.
        objects,                      // Final result membership walk.
        add(claims, pins)?,           // Fixed pair/declaration consistency guards.
    ] {
        audit = add(audit, term)?;
    }
    let inspection_used = limits
        .plan_edges
        .checked_sub(inspection.remaining())
        .ok_or(ContractError::Capacity)?;
    within(add(inspection_used, audit)?, limits.plan_edges)?;
    Ok(audit)
}
