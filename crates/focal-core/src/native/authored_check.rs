use super::*;
use focal_model::{ObjectKind, RelationKind};

#[path = "authored_check_budget.rs"]
mod budget;

fn find<'a>(
    extras: &'a Extras,
    key: Key,
    visits: &mut VisitBudget,
) -> Result<Option<&'a super::super::prepare::Extra>, NativeError> {
    visits.charge(extras.rows.len())?;
    Ok(extras.rows.iter().find(|row| row.key == key))
}

fn owner_shape(
    body: &ClaimDescriptor,
    profile: super::super::owned::ClaimContentProfile,
) -> Result<(), NativeError> {
    match (body.cause(), profile.owner) {
        (focal_model::Cause::Root(_), None) => Ok(()),
        (focal_model::Cause::Claim(parent), Some(owner))
            if owner.expected.ledger == body.ledger()
                && owner.expected.object.0 == parent.0
                && owner.expected.content.0 != [0; 32]
                && owner.expected.revision.0 != 0
                && owner
                    .receipt
                    .is_none_or(|receipt| !receipt.receipt.is_zero() && receipt.epoch != 0) =>
        {
            Ok(())
        }
        _ => Err(ContractError::InvalidTarget.into()),
    }
}

fn is_content(row: &super::super::prepare::Extra) -> bool {
    matches!(
        row.key,
        Key::ClaimContent(_)
            | Key::ClaimIdentity(..)
            | Key::DefinitionIdentity(..)
            | Key::CreationResult(_)
    ) || matches!(&row.row, Row::Definition(value) if value.descriptor().is_some())
}

pub(in crate::native) fn pair(
    body: &ClaimDescriptor,
    profile: super::super::owned::ClaimContentProfile,
    state: &ClaimState,
    visits: &mut VisitBudget,
) -> Result<(), NativeError> {
    visits.charge(1)?;
    owner_shape(body, profile)?;
    if !same_identity(body.binding(), state.binding())
        || body.issuer() != state.issuer()
        || body.subject() != state.subject()
        || body.deadline() != state.deadline()
        || profile.max_responses != state.max_responses()
        || profile.scope_limits != state.scopes().limits()
        || body.cause() != state.lineage().cause()
        || body.requirements().len() != state.acceptance().declarations().len()
    {
        return Err(ContractError::ContentConflict.into());
    }
    let mut graph = state.graph().obligations().iter();
    let mut corrections = state.lineage().corrections().iter();
    for relation in body.relations() {
        visits.charge(1)?;
        match relation.kind {
            RelationKind::DependsOn | RelationKind::Awaits => {
                let RelationTarget::Object(target) = relation.target else {
                    return Err(ContractError::InvalidTarget.into());
                };
                let edge = graph.next().ok_or(ContractError::InvalidPolicy)?;
                let kind = match relation.kind {
                    RelationKind::DependsOn => focal_model::lifecycle::graph::Kind::DependsOn,
                    _ => focal_model::lifecycle::graph::Kind::Awaits,
                };
                if edge.kind != kind || edge.target.0 != target.id.0 {
                    return Err(ContractError::ContentConflict.into());
                }
            }
            RelationKind::Supersedes | RelationKind::Amends => {
                let RelationTarget::Object(target) = relation.target else {
                    return Err(ContractError::InvalidTarget.into());
                };
                let correction = corrections.next().ok_or(ContractError::InvalidPolicy)?;
                let kind = match relation.kind {
                    RelationKind::Supersedes => {
                        focal_model::lifecycle::succession::CorrectionKind::Supersedes
                    }
                    _ => focal_model::lifecycle::succession::CorrectionKind::Amends,
                };
                if correction.kind != kind || correction.predecessor != target {
                    return Err(ContractError::ContentConflict.into());
                }
            }
            _ => {}
        }
    }
    if graph.next().is_some() || corrections.next().is_some() {
        return Err(ContractError::InvalidPolicy.into());
    }
    let mut slots = state.acceptance().slots();
    for slot in body.slots() {
        visits.charge(1)?;
        let stored = slots.next().ok_or(ContractError::InvalidPolicy)?;
        visits.charge(add(stored.checks.len(), slot.checks.len())?)?;
        if stored.slot != slot.slot
            || stored.mode != slot.mode
            || stored.missing_declaration_index != slot.missing_declaration_index
            || stored.checks != slot.checks
        {
            return Err(ContractError::ContentConflict.into());
        }
    }
    if slots.next().is_some() {
        return Err(ContractError::InvalidPolicy.into());
    }
    Ok(())
}

pub(in crate::native) fn declaration_pair(
    body: &ClaimDescriptor,
    state: &ClaimState,
    descriptor: &ValidationDescriptor,
    visits: &mut VisitBudget,
) -> Result<(), NativeError> {
    visits.charge(add(1, body.requirements().len())?)?;
    let declaration = descriptor.declaration();
    if declaration.claim() != body.id()
        || declaration.issuer() != body.issuer()
        || declaration.binding().ledger != body.ledger()
        || declaration.binding().content != descriptor.content_hash()
        || !body.requirements().iter().any(|pin| {
            pin.id.0 == descriptor.binding().object.0
                && pin.specification == descriptor.specification_hash()
        })
    {
        return Err(ContractError::ContentConflict.into());
    }
    visits.charge(state.acceptance().declarations().len())?;
    state.acceptance().check_declaration(declaration)?;
    Ok(())
}

pub(in crate::native) fn check_plan(
    plan: &super::super::transactions::Plan,
    extras: &Extras,
    view: &View<'_>,
    meta: Meta,
    outcome: NativeOutcome,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let Some(proof) = &extras.authored else {
        within(extras.rows.len(), limits.range.max_batch_entries)?;
        if extras.rows.iter().any(is_content)
            || (view.state.profile == NativeContentProfile::AuthoredV1
                && outcome.operation == NativeOperation::Create)
        {
            return Err(ContractError::InvalidPolicy.into());
        }
        return Ok(());
    };
    let mut visits = VisitBudget::new(budget::quote(plan, extras, limits)?);
    for (position, extra) in extras.rows.iter().enumerate() {
        visits.charge(add(position, 1)?)?;
        if extras
            .rows
            .iter()
            .take(position)
            .any(|old| old.key == extra.key)
        {
            return Err(ContractError::InvalidPolicy.into());
        }
    }
    for (position, state) in plan.rows.iter().enumerate() {
        visits.charge(add(position, 1)?)?;
        if plan
            .rows
            .iter()
            .take(position)
            .any(|old| old.binding().object == state.binding().object)
        {
            return Err(ContractError::InvalidPolicy.into());
        }
    }
    let mut fingerprint_visits = extras.rows.len();
    visits.charge(extras.rows.len())?;
    for extra in &extras.rows {
        if let Row::CreationResult(result) = &extra.row {
            fingerprint_visits = add(fingerprint_visits, result.get().entries().len())?;
        }
    }
    visits.charge(fingerprint_visits)?;
    if retained_fingerprint(extras)? != proof.fingerprint {
        return Err(ContractError::ContentConflict.into());
    }
    if view.state.profile != NativeContentProfile::AuthoredV1
        || outcome.operation != NativeOperation::Create
        || proof.prefix != view.prefix()
        || NativeInvocation::from(proof.request) != outcome.invocation
        || proof.claims != plan.created
        || meta.claims != add(view.meta().claims, proof.claims)?
        || meta.definitions != add(view.meta().definitions, proof.definitions)?
        || meta.creation_results != add(view.meta().creation_results, 1)?
    {
        return Err(ContractError::InvalidPolicy.into());
    }
    let result_key = Key::CreationResult(proof.request.into());
    let result = find(extras, result_key, &mut visits)?.ok_or(ContractError::InvalidPolicy)?;
    let Row::CreationResult(result) = &result.row else {
        return Err(ContractError::InvalidPolicy.into());
    };
    if result.get().entries().len() != proof.objects || view.get(result_key).is_some() {
        return Err(ContractError::InvalidPolicy.into());
    }
    let (mut claims, mut definitions, mut claim_indices, mut definition_indices, mut results) =
        (0, 0, 0, 0, 0);
    for extra in &extras.rows {
        visits.charge(1)?;
        match (&extra.key, &extra.row) {
            (Key::ClaimContent(id), Row::ClaimContent(owned)) => {
                claims = add(claims, 1)?;
                let body = owned.get().ok_or(ContractError::InvalidPolicy)?;
                let profile = owned.profile().ok_or(ContractError::InvalidPolicy)?;
                visits.charge(plan.rows.len())?;
                let state = plan
                    .rows
                    .iter()
                    .find(|row| row.binding().object.0 == id.0)
                    .ok_or(ContractError::InvalidTarget)?;
                visits.charge(budget::claim_heap_visits(body)?)?;
                if body.id() != *id
                    || view.get(extra.key).is_some()
                    || view.claim(*id).is_some()
                    || extra.heap != owned.heap_charge()?
                    || extra.fact.is_some()
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
                pair(body, profile, state, &mut visits)?;
                if !matches!(find(extras, Key::ClaimIdentity(body.schema(), body.content_hash()), &mut visits)?.map(|row| &row.row), Some(Row::ClaimIdentity(found)) if found == id)
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
                for pin in body.requirements() {
                    let definition = find(extras, Key::Definition(pin.id), &mut visits)?
                        .ok_or(ContractError::InvalidPolicy)?;
                    let descriptor =
                        super::super::authored_reads::descriptor(Some(&definition.row))
                            .ok_or(ContractError::InvalidPolicy)?;
                    declaration_pair(body, state, descriptor, &mut visits)?;
                }
            }
            (Key::Definition(id), Row::Definition(owned)) => {
                definitions = add(definitions, 1)?;
                let descriptor = owned.descriptor().ok_or(ContractError::InvalidPolicy)?;
                let declaration = descriptor.declaration();
                if declaration.binding().object.0 != id.0
                    || view.get(extra.key).is_some()
                    || extra.heap != owned.heap_charge()?
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
                let owner = find(extras, Key::ClaimContent(declaration.claim()), &mut visits)?
                    .ok_or(ContractError::InvalidPolicy)?;
                let Row::ClaimContent(owner) = &owner.row else {
                    return Err(ContractError::InvalidPolicy.into());
                };
                let owner = owner.get().ok_or(ContractError::InvalidPolicy)?;
                visits.charge(owner.requirements().len())?;
                if !owner.requirements().iter().any(|pin| pin.id == *id) {
                    return Err(ContractError::InvalidPolicy.into());
                }
                if !matches!(find(extras, Key::DefinitionIdentity(descriptor.schema(), descriptor.content_hash()), &mut visits)?.map(|row| &row.row), Some(Row::DefinitionIdentity(found)) if found == id)
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
                let fact = NativeFact::Definition {
                    binding: declaration.binding(),
                    claim: declaration.claim(),
                    index: declaration.declaration_index(),
                    intent: declaration.intent_fingerprint(),
                };
                let present = if let Some(control) = &extras.control_graph {
                    visits.charge(control.prefix().len())?;
                    control.prefix().contains(&fact)
                } else {
                    extra.fact == Some(fact)
                };
                if !present {
                    return Err(ContractError::InvalidManifest.into());
                }
            }
            (Key::ClaimIdentity(schema, content), Row::ClaimIdentity(id)) => {
                claim_indices = add(claim_indices, 1)?;
                let body = find(extras, Key::ClaimContent(*id), &mut visits)?
                    .and_then(|row| super::super::authored_reads::content(Some(&row.row)))
                    .ok_or(ContractError::InvalidPolicy)?;
                if body.schema() != *schema
                    || body.content_hash() != *content
                    || view.get(extra.key).is_some()
                    || extra.heap != 0
                    || extra.fact.is_some()
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            (Key::DefinitionIdentity(schema, content), Row::DefinitionIdentity(id)) => {
                definition_indices = add(definition_indices, 1)?;
                let body = find(extras, Key::Definition(*id), &mut visits)?
                    .and_then(|row| super::super::authored_reads::descriptor(Some(&row.row)))
                    .ok_or(ContractError::InvalidPolicy)?;
                if body.schema() != *schema
                    || body.content_hash() != *content
                    || view.get(extra.key).is_some()
                    || extra.heap != 0
                    || extra.fact.is_some()
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            (Key::CreationResult(key), Row::CreationResult(owned)) => {
                results = add(results, 1)?;
                if *key != outcome.invocation
                    || extra.heap != owned.heap_charge()?
                    || extra.fact.is_some()
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            _ if is_content(extra) => return Err(ContractError::InvalidPolicy.into()),
            _ => {}
        }
    }
    if (
        claims,
        definitions,
        claim_indices,
        definition_indices,
        results,
    ) != (
        proof.claims,
        proof.definitions,
        proof.claims,
        proof.definitions,
        1,
    ) {
        return Err(ContractError::InvalidPolicy.into());
    }
    for object in result.get().entries() {
        visits.charge(1)?;
        let key = match object.family {
            NativeCreatedFamily::Claim => Key::ClaimIdentity(object.schema, object.content),
            NativeCreatedFamily::Validation => {
                Key::DefinitionIdentity(object.schema, object.content)
            }
        };
        let row = if proof.claims == 0 {
            view.get(key)
        } else {
            find(extras, key, &mut visits)?.map(|extra| &extra.row)
        };
        let resolved = match row {
            Some(Row::ClaimIdentity(id)) => ObjectId(id.0),
            Some(Row::DefinitionIdentity(id)) => ObjectId(id.0),
            _ => return Err(ContractError::InvalidPolicy.into()),
        };
        if resolved != object.resolved || (proof.claims != 0 && object.requested != resolved) {
            return Err(ContractError::ContentConflict.into());
        }
    }
    Ok(())
}

/// Reconstruction checks actual rows and both directions of identity membership;
/// an enum flag or matching counters cannot turn omitted text into authored data.
pub(in crate::native) fn check_storage(core: &Core<NativeState>) -> Result<(), NativeError> {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let strict = core.state.profile == NativeContentProfile::AuthoredV1;
    let limits = core.limits;
    let meta = view.meta();
    if (!strict && meta.creation_results != 0) || meta.creation_results > meta.outcomes {
        return Err(ContractError::InvalidPolicy.into());
    }
    within(meta.claims, limits.claims)?;
    within(meta.definitions, limits.definitions)?;
    within(meta.creation_results, limits.outcomes)?;
    let (
        mut claims,
        mut definitions,
        mut contents,
        mut claim_indices,
        mut definition_indices,
        mut results,
    ) = (0, 0, 0, 0, 0, 0);
    for entry in core.state.rows.entries() {
        // Reconstruction traverses the actual owner once. Nested authored
        // checks share one bounded allowance per row/claim, not per pin.
        let mut visits = VisitBudget::new(limits.plan_edges);
        match (&entry.key, &entry.value) {
            (Key::Claim(id), Row::Claim(_)) if strict => {
                claims = add(claims, 1)?;
                let Some(Row::ClaimContent(owned)) = view.get(Key::ClaimContent(*id)) else {
                    return Err(ContractError::InvalidPolicy.into());
                };
                let body = owned.get().ok_or(ContractError::InvalidPolicy)?;
                let state = view.claim(*id).ok_or(ContractError::InvalidTarget)?;
                budget::body_shape(body, limits, &mut visits)?;
                budget::state_shape(state, limits, &mut visits)?;
                pair(
                    body,
                    owned.profile().ok_or(ContractError::InvalidPolicy)?,
                    state,
                    &mut visits,
                )?;
                for pin in body.requirements() {
                    visits.charge(1)?;
                    let descriptor =
                        super::super::authored_reads::descriptor(view.get(Key::Definition(pin.id)))
                            .ok_or(ContractError::InvalidPolicy)?;
                    declaration_pair(body, state, descriptor, &mut visits)?;
                }
                for relation in body.relations() {
                    visits.charge(1)?;
                    if let RelationTarget::Object(target) = relation.target
                        && (target.ledger != view.ledger()
                            || target.kind != ObjectKind::Claim
                            || view.claim(ClaimId(target.id.0)).is_none())
                    {
                        return Err(ContractError::InvalidTarget.into());
                    }
                }
            }
            (Key::ClaimContent(id), Row::ClaimContent(owned)) if strict => {
                contents = add(contents, 1)?;
                let body = owned.get().ok_or(ContractError::InvalidPolicy)?;
                budget::body_shape(body, limits, &mut visits)?;
                visits.charge(budget::claim_heap_visits(body)?)?;
                owner_shape(body, owned.profile().ok_or(ContractError::InvalidPolicy)?)?;
                if entry.heap_bytes != owned.heap_charge()?
                    || body.id() != *id
                    || view.claim(*id).is_none()
                    || !matches!(view.get(Key::ClaimIdentity(body.schema(), body.content_hash())), Some(Row::ClaimIdentity(found)) if found == id)
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            (Key::Definition(id), Row::Definition(owned)) if strict => {
                definitions = add(definitions, 1)?;
                let body = owned.descriptor().ok_or(ContractError::InvalidPolicy)?;
                let claim = super::super::authored_reads::content(
                    view.get(Key::ClaimContent(body.declaration().claim())),
                )
                .ok_or(ContractError::InvalidTarget)?;
                within(
                    claim.requirements().len(),
                    limits.definitions.min(limits.range.max_batch_entries),
                )?;
                visits.charge(claim.requirements().len())?;
                if entry.heap_bytes != owned.heap_charge()?
                    || body.binding().object.0 != id.0
                    || !claim.requirements().iter().any(|pin| pin.id == *id)
                    || !matches!(view.get(Key::DefinitionIdentity(body.schema(), body.content_hash())), Some(Row::DefinitionIdentity(found)) if found == id)
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            (Key::ClaimIdentity(schema, hash), Row::ClaimIdentity(id)) if strict => {
                claim_indices = add(claim_indices, 1)?;
                let body = super::super::authored_reads::content(view.get(Key::ClaimContent(*id)))
                    .ok_or(ContractError::InvalidPolicy)?;
                if entry.heap_bytes != 0 || body.schema() != *schema || body.content_hash() != *hash
                {
                    return Err(ContractError::ContentConflict.into());
                }
            }
            (Key::DefinitionIdentity(schema, hash), Row::DefinitionIdentity(id)) if strict => {
                definition_indices = add(definition_indices, 1)?;
                let body = super::super::authored_reads::descriptor(view.get(Key::Definition(*id)))
                    .ok_or(ContractError::InvalidPolicy)?;
                if entry.heap_bytes != 0 || body.schema() != *schema || body.content_hash() != *hash
                {
                    return Err(ContractError::ContentConflict.into());
                }
            }
            (Key::CreationResult(key), Row::CreationResult(result)) if strict => {
                results = add(results, 1)?;
                within(result.get().entries().len(), limits.range.max_batch_entries)?;
                if entry.heap_bytes != result.heap_charge()? {
                    return Err(ContractError::InvalidPolicy.into());
                }
                let outcome =
                    as_outcome(view.get(Key::Outcome(*key))).ok_or(ContractError::InvalidPolicy)?;
                if outcome.operation != NativeOperation::Create || outcome.invocation != *key {
                    return Err(ContractError::InvalidPolicy.into());
                }
                for object in result.get().entries() {
                    visits.charge(1)?;
                    let matches = match object.family {
                        NativeCreatedFamily::Claim => {
                            matches!(view.get(Key::ClaimIdentity(object.schema, object.content)), Some(Row::ClaimIdentity(id)) if id.0 == object.resolved.0)
                        }
                        NativeCreatedFamily::Validation => {
                            matches!(view.get(Key::DefinitionIdentity(object.schema, object.content)), Some(Row::DefinitionIdentity(id)) if id.0 == object.resolved.0)
                        }
                    };
                    if !matches {
                        return Err(ContractError::ContentConflict.into());
                    }
                }
            }
            (Key::Outcome(key), Row::Outcome(outcome))
                if strict && outcome.operation == NativeOperation::Create =>
            {
                if !matches!(
                    view.get(Key::CreationResult(*key)),
                    Some(Row::CreationResult(_))
                ) {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            (
                _,
                Row::ClaimContent(_)
                | Row::ClaimIdentity(_)
                | Row::DefinitionIdentity(_)
                | Row::CreationResult(_),
            ) => return Err(ContractError::InvalidPolicy.into()),
            (_, Row::Definition(owned)) if owned.descriptor().is_some() => {
                return Err(ContractError::InvalidPolicy.into());
            }
            _ => {}
        }
    }
    if strict
        && (
            claims,
            definitions,
            contents,
            claim_indices,
            definition_indices,
            results,
        ) != (
            meta.claims,
            meta.definitions,
            meta.claims,
            meta.claims,
            meta.definitions,
            meta.creation_results,
        )
    {
        return Err(ContractError::InvalidPolicy.into());
    }
    if meta.creation_results > meta.outcomes {
        return Err(ContractError::InvalidPolicy.into());
    }
    Ok(())
}
