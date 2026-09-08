use super::*;

// Only final descriptor allocations consume this allowance. A changed source
// cannot allocate more than the prepared total, including partially built rows.
struct Heap {
    left: usize,
}
impl Heap {
    fn reserve<T>(&mut self, count: usize) -> Result<Vec<T>, ContractError> {
        self.left = self
            .left
            .checked_sub(bytes::array::<T>(count)?)
            .ok_or(ContractError::Capacity)?;
        reserve(count)
    }
    fn string(&mut self, value: &str) -> Result<String, ContractError> {
        let mut owned = self.reserve(value.len())?;
        owned.extend_from_slice(value.as_bytes());
        String::from_utf8(owned).map_err(|_| ContractError::InvalidManifest)
    }
}

pub(in crate::lifecycle::claim_descriptor) fn build<'a>(
    source: &impl ClaimSource<'a>,
    expected: ClaimFields<'a>,
    limits: Limits,
    shape: Shape,
    max_bytes: usize,
    max_visits: usize,
) -> Result<ClaimDescriptor, ContractError> {
    bytes::fits(shape.charge, max_bytes)?;
    bytes::fits(shape.build_visits, max_visits)?;
    let mut visits = VisitBudget::new(max_visits);
    visits.charge(FIELDS)?;
    let fields = source.fields();
    visits.charge(scaled(expected.description.len(), 3)?)?;
    if fields != expected {
        return Err(ContractError::InvalidManifest);
    }
    let counts = Counts::read(source, limits, &mut visits)?;
    if counts != shape.counts {
        return Err(ContractError::InvalidManifest);
    }
    let mut heap = Heap { left: shape.heap };
    let description = heap.string(fields.description)?;

    let mut relations = heap.reserve(counts.relations)?;
    visits.charge(1)?;
    let mut values = source.relations();
    for _ in 0..counts.relations {
        visits.charge(RELATION)?;
        relations.push(next(&mut values, &mut visits)?);
    }
    end(&mut values, &mut visits)?;

    let mut scopes = heap.reserve(counts.scopes)?;
    visits.charge(1)?;
    let mut values = source.scopes();
    for _ in 0..counts.scopes {
        visits.charge(SCOPE)?;
        let value = next(&mut values, &mut visits)?;
        if value.key.len() > limits.scope_key_bytes {
            return Err(ContractError::Capacity);
        }
        visits.charge(scaled(value.key.len(), 2)?)?;
        scopes.push(AuthoredScope {
            kind: value.kind,
            key: heap.string(value.key)?,
        });
    }
    end(&mut values, &mut visits)?;

    let mut requirements = heap.reserve(counts.requirements)?;
    visits.charge(1)?;
    let mut values = source.requirements();
    for _ in 0..counts.requirements {
        visits.charge(REQUIREMENT)?;
        requirements.push(next(&mut values, &mut visits)?);
    }
    end(&mut values, &mut visits)?;

    let mut slots = heap.reserve(counts.slots)?;
    visits.charge(1)?;
    let mut values = source.slots();
    let mut total_checks = 0;
    for _ in 0..counts.slots {
        let value = next(&mut values, &mut visits)?;
        let fields = slot_fields(&value, limits, &mut visits)?;
        total_checks = bytes::add(total_checks, fields.checks)?;
        if total_checks > limits.checks {
            return Err(ContractError::Capacity);
        }
        let mut checks = heap.reserve(fields.checks)?;
        visits.charge(1)?;
        let mut supplied = value.checks();
        for _ in 0..fields.checks {
            visits.charge(CHECK)?;
            checks.push(next(&mut supplied, &mut visits)?);
        }
        end(&mut supplied, &mut visits)?;
        slots.push(AuthoredSlot {
            slot: fields.slot,
            missing_declaration_index: fields.missing_declaration_index,
            mode: fields.mode,
            checks,
        });
    }
    end(&mut values, &mut visits)?;
    if heap.left != 0 {
        return Err(ContractError::InvalidManifest);
    }

    let mut descriptor = ClaimDescriptor {
        ledger: fields.ledger,
        id: fields.id,
        schema: fields.schema,
        occurrence: fields.occurrence,
        description,
        relations,
        scopes,
        requirements,
        slots,
        deadline: fields.deadline,
        roles: shape.roles.copy(),
        content_hash: shape.content_hash,
    };
    // Check the result, rather than trusting any earlier pass over a possibly
    // stateful source. This walk borrows final buffers and allocates nothing.
    let (_, actual) =
        inspection::inspect_with(&adapters::OwnedSource(&descriptor), limits, &mut visits)?;
    if actual.counts != shape.counts
        || actual.heap != shape.heap
        || actual.allocations != shape.allocations
        || actual.charge != shape.charge
        || actual.roles != shape.roles
        || actual.content_hash != shape.content_hash
    {
        return Err(ContractError::InvalidManifest);
    }
    visits.charge(bytes::add(
        16,
        scaled(bytes::add(counts.scopes, counts.slots)?, 4)?,
    )?)?;
    if descriptor.retained_bytes()? != shape.charge
        || descriptor.heap_allocations()? != shape.allocations
    {
        return Err(ContractError::Capacity);
    }
    descriptor.roles = actual.roles;
    descriptor.content_hash = actual.content_hash;
    Ok(descriptor)
}
