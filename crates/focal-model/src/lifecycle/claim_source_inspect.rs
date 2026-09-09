use super::*;
/// Largest authored follow-up budget of one claim.
pub const MAX_FOLLOW_UPS: u16 = 1024;

pub(in crate::lifecycle::claim_descriptor) fn inspect<'a>(
    source: &impl ClaimSource<'a>,
    limits: Limits,
    maximum: usize,
) -> Result<(ClaimFields<'a>, Shape), ContractError> {
    let mut visits = VisitBudget::new(maximum);
    inspect_with(source, limits, &mut visits)
}

pub(super) fn inspect_with<'a>(
    source: &impl ClaimSource<'a>,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<(ClaimFields<'a>, Shape), ContractError> {
    let initial = visits.remaining();
    visits.charge(FIELDS)?;
    let fields = source.fields();
    if fields.ledger.tenant.is_zero() || fields.ledger.session.is_zero() {
        return Err(ContractError::WrongLedger);
    }
    if fields.id.is_zero() || fields.occurrence.is_zero() {
        return Err(ContractError::InvalidTarget);
    }
    // Schema 1 carries no follow-up policy; schema 2 may carry one and only
    // then may relations name exact evidence.
    match (fields.schema, fields.policy) {
        (1, None) | (2, _) => {}
        _ => return Err(ContractError::InvalidPolicy),
    }
    if fields
        .policy
        .is_some_and(|policy| policy.max_follow_ups > MAX_FOLLOW_UPS)
    {
        return Err(ContractError::InvalidPolicy);
    }
    if fields.description.len() > limits.description_bytes {
        return Err(ContractError::Capacity);
    }
    visits.charge(scaled(fields.description.len(), 3)?)?;
    text(fields.description, limits.description_bytes)?;
    if fields
        .deadline
        .is_some_and(|value| value.timer.is_zero() || value.generation == 0)
    {
        return Err(ContractError::InvalidTarget);
    }
    let counts = Counts::read(source, limits, visits)?;
    let mut heap = counts.heap(fields.description.len())?;
    bytes::fits(
        bytes::total::<ClaimDescriptor>(heap)?,
        limits.construction_bytes,
    )?;
    let mut allocations = counts.allocations(fields.description.len())?;
    let mut hash = identity::Hash::new(fields);
    let mut roles = RoleBuilder::default();
    // Copy visits account for the same headers/callbacks, one copy of every
    // authored byte/value and the final retained-capacity scans. No scratch walk
    // or second raw pass is needed when constructing the descriptor.
    let mut copy_visits = bytes::add(FIELDS + 4 + 8 + 16, scaled(fields.description.len(), 3)?)?;
    copy_visits = bytes::add(
        copy_visits,
        scaled(bytes::add(counts.scopes, counts.slots)?, 4)?,
    )?;

    hash.count(counts.relations);
    visits.charge(1)?;
    let mut values = source.relations();
    let mut previous: Option<Relation> = None;
    for _ in 0..counts.relations {
        visits.charge(RELATION)?;
        let value = next(&mut values, visits)?;
        if previous.as_ref().is_some_and(|old| old >= &value) {
            return Err(ContractError::InvalidManifest);
        }
        roles.relation(fields.ledger, fields.id, &value)?;
        // Exact evidence targets and corrective `invalidates` relations exist
        // only from descriptor schema 2.
        if fields.schema < 2
            && (matches!(value.target, RelationTarget::Evidence(_))
                || value.kind == RelationKind::Invalidates)
        {
            return Err(ContractError::InvalidPolicy);
        }
        hash.relation(&value);
        previous = Some(value);
    }
    end(&mut values, visits)?;
    let roles = roles.finish()?;
    copy_visits = bytes::add(copy_visits, scaled(counts.relations, RELATION + 1)?)?;

    hash.count(counts.scopes);
    visits.charge(1)?;
    let mut values = source.scopes();
    let mut previous = None;
    for _ in 0..counts.scopes {
        visits.charge(SCOPE)?;
        let value = next(&mut values, visits)?;
        if value.key.len() > limits.scope_key_bytes {
            return Err(ContractError::Capacity);
        }
        visits.charge(scaled(value.key.len(), 6)?)?;
        text(value.key, limits.scope_key_bytes)?;
        if value.key.trim() != value.key || previous.is_some_and(|old| old >= value) {
            return Err(ContractError::InvalidManifest);
        }
        heap = bytes::add(heap, value.key.len())?;
        bytes::fits(
            bytes::total::<ClaimDescriptor>(heap)?,
            limits.construction_bytes,
        )?;
        allocations = bytes::add(allocations, bytes::allocation::<u8>(value.key.len()))?;
        copy_visits = bytes::add(
            copy_visits,
            bytes::add(SCOPE + 1, scaled(value.key.len(), 2)?)?,
        )?;
        hash.scope(value);
        previous = Some(value);
    }
    end(&mut values, visits)?;

    hash.count(counts.requirements);
    visits.charge(1)?;
    let mut values = source.requirements();
    for position in 0..counts.requirements {
        visits.charge(REQUIREMENT)?;
        let value = next(&mut values, visits)?;
        if value.id.is_zero() || value.specification.0 == [0; 32] {
            return Err(ContractError::InvalidTarget);
        }
        unique_requirement(source, counts.requirements, position, value, visits)?;
        hash.requirement(value);
    }
    end(&mut values, visits)?;
    copy_visits = bytes::add(copy_visits, scaled(counts.requirements, REQUIREMENT + 1)?)?;

    hash.count(counts.slots);
    visits.charge(1)?;
    let mut slots = source.slots();
    let mut previous = None;
    let mut total_checks = 0;
    for position in 0..counts.slots {
        let slot = next(&mut slots, visits)?;
        let header = slot_fields(&slot, limits, visits)?;
        aggregation::check_policy_order(previous, header.slot)?;
        previous = Some(header.slot);
        total_checks = bytes::add(total_checks, header.checks)?;
        if total_checks > limits.checks {
            return Err(ContractError::Capacity);
        }
        unique_slot(source, counts.slots, position, header, limits, visits)?;
        heap = bytes::add(heap, bytes::array::<CheckPolicy>(header.checks)?)?;
        bytes::fits(
            bytes::total::<ClaimDescriptor>(heap)?,
            limits.construction_bytes,
        )?;
        allocations = bytes::add(allocations, bytes::allocation::<CheckPolicy>(header.checks))?;
        copy_visits = bytes::add(
            copy_visits,
            bytes::add(SLOT + 3, scaled(header.checks, CHECK + 1)?)?,
        )?;
        hash.slot(header);
        visits.charge(1)?;
        let mut checks = slot.checks();
        let mut previous = None;
        for check_position in 0..header.checks {
            visits.charge(CHECK)?;
            let check = next(&mut checks, visits)?;
            aggregation::check_policy_order(previous, check.declaration_index)?;
            aggregation::check_policy_validation(check)?;
            previous = Some(check.declaration_index);
            pinned(source, counts.requirements, check, visits)?;
            unique_check(
                source,
                counts.slots,
                (position, check_position),
                check,
                limits,
                visits,
            )?;
            hash.check(check);
        }
        end(&mut checks, visits)?;
    }
    end(&mut slots, visits)?;
    let content_hash = hash.finish(fields.deadline);
    let charge = bytes::total::<ClaimDescriptor>(heap)?;
    bytes::fits(charge, limits.construction_bytes)?;
    let inspection_visits = initial
        .checked_sub(visits.remaining())
        .ok_or(ContractError::Capacity)?;
    let build_visits = bytes::add(copy_visits, inspection_visits)?;
    Ok((
        fields,
        Shape {
            counts,
            roles,
            heap,
            allocations,
            charge,
            content_hash,
            inspection_visits,
            build_visits,
        },
    ))
}

fn unique_requirement<'a>(
    source: &impl ClaimSource<'a>,
    count: usize,
    position: usize,
    value: RequirementRef,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    let mut values = source.requirements();
    for index in 0..count {
        visits.charge(1)?;
        let old = next(&mut values, visits)?;
        if (index == position && old != value) || (index != position && old.id == value.id) {
            return Err(ContractError::InvalidTarget);
        }
    }
    end(&mut values, visits)
}

fn pinned<'a>(
    source: &impl ClaimSource<'a>,
    count: usize,
    check: CheckPolicy,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    let mut values = source.requirements();
    let mut found = false;
    for _ in 0..count {
        visits.charge(1)?;
        found |= next(&mut values, visits)?.id == check.validation;
    }
    end(&mut values, visits)?;
    if !found {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}

fn unique_slot<'a>(
    source: &impl ClaimSource<'a>,
    count: usize,
    position: usize,
    value: ClaimSlotFields,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    let mut slots = source.slots();
    for index in 0..count {
        let slot = next(&mut slots, visits)?;
        let old = slot_fields(&slot, limits, visits)?;
        if index == position {
            if old != value {
                return Err(ContractError::InvalidManifest);
            }
        } else {
            aggregation::check_policy_index(
                old.missing_declaration_index,
                value.missing_declaration_index,
            )?;
        }
    }
    end(&mut slots, visits)
}

fn unique_check<'a>(
    source: &impl ClaimSource<'a>,
    count: usize,
    position: (usize, usize),
    value: CheckPolicy,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    let mut slots = source.slots();
    let mut total = 0;
    for slot_position in 0..count {
        let slot = next(&mut slots, visits)?;
        let header = slot_fields(&slot, limits, visits)?;
        total = bytes::add(total, header.checks)?;
        if total > limits.checks {
            return Err(ContractError::Capacity);
        }
        aggregation::check_policy_index(header.missing_declaration_index, value.declaration_index)?;
        visits.charge(1)?;
        let mut checks = slot.checks();
        for check_position in 0..header.checks {
            visits.charge(1)?;
            let old = next(&mut checks, visits)?;
            if (slot_position, check_position) == position {
                if old != value {
                    return Err(ContractError::InvalidManifest);
                }
            } else {
                aggregation::check_policy_pair(old, value)?;
            }
        }
        end(&mut checks, visits)?;
    }
    end(&mut slots, visits)
}
