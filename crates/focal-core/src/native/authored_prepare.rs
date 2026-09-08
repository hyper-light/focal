use super::super::transactions::{Plan, increment};
use super::*;

fn claim_resolution(
    view: &View<'_>,
    proposal: &NativeAuthoredProposal,
) -> Result<Option<ClaimId>, NativeError> {
    let descriptor = &proposal.content;
    let key = Key::ClaimIdentity(descriptor.schema(), descriptor.content_hash());
    if let Some(old) = view.claim(descriptor.id())
        && old.binding().content != descriptor.content_hash()
    {
        return Err(ContractError::ContentConflict.into());
    }
    match view.get(key) {
        None => {
            if view.get(Key::Claim(descriptor.id())).is_some()
                || view.get(Key::ClaimContent(descriptor.id())).is_some()
            {
                return Err(ContractError::ContentConflict.into());
            }
            Ok(None)
        }
        Some(Row::ClaimIdentity(id)) => {
            let Some(Row::ClaimContent(stored)) = view.get(Key::ClaimContent(*id)) else {
                return Err(ContractError::InvalidPolicy.into());
            };
            let body = stored.get().ok_or(ContractError::InvalidPolicy)?;
            let profile = stored.profile().ok_or(ContractError::InvalidPolicy)?;
            let state = view.claim(*id).ok_or(ContractError::InvalidTarget)?;
            if body.content_hash() != descriptor.content_hash()
                || body.schema() != descriptor.schema()
                || !same_identity(body.binding(), state.binding())
                || profile.max_responses != proposal.max_responses
                || profile.scope_limits != proposal.scope_limits
                || profile.owner != proposal.owner
            {
                return Err(ContractError::ContentConflict.into());
            }
            Ok(Some(*id))
        }
        Some(_) => Err(ContractError::InvalidPolicy.into()),
    }
}

fn definition_resolution(
    view: &View<'_>,
    descriptor: &ValidationDescriptor,
) -> Result<Option<ValidationId>, NativeError> {
    let id = ValidationId(descriptor.binding().object.0);
    if let Some(old) = as_definition(view.get(Key::Definition(id)))
        && old.binding().content != descriptor.content_hash()
    {
        return Err(ContractError::ContentConflict.into());
    }
    match view.get(Key::DefinitionIdentity(
        descriptor.schema(),
        descriptor.content_hash(),
    )) {
        None => {
            if view.get(Key::Definition(id)).is_some() {
                return Err(ContractError::ContentConflict.into());
            }
            Ok(None)
        }
        Some(Row::DefinitionIdentity(id)) => {
            let stored = super::super::authored_reads::descriptor(view.get(Key::Definition(*id)))
                .ok_or(ContractError::InvalidPolicy)?;
            if stored.content_hash() != descriptor.content_hash()
                || stored.schema() != descriptor.schema()
            {
                return Err(ContractError::ContentConflict.into());
            }
            Ok(Some(*id))
        }
        Some(_) => Err(ContractError::InvalidPolicy.into()),
    }
}

fn push_mapping(
    rows: &mut Vec<NativeCreatedObject>,
    family: NativeCreatedFamily,
    schema: u16,
    content: ContentHash,
    requested: ObjectId,
    resolved: ObjectId,
) -> Result<(), NativeError> {
    if rows.len() == rows.capacity() {
        return Err(ContractError::Capacity.into());
    }
    rows.push(NativeCreatedObject {
        ordinal: u32::try_from(rows.len()).map_err(|_| ContractError::Capacity)?,
        family,
        schema,
        content,
        requested,
        resolved,
    });
    Ok(())
}

fn references(
    claims: &[NativeAuthoredProposal],
    view: &View<'_>,
    visits: &mut VisitBudget,
) -> Result<(), NativeError> {
    for claim in claims {
        for relation in claim.content.relations() {
            visits.charge(1)?;
            let RelationTarget::Object(target) = relation.target else {
                continue;
            };
            if target.ledger != view.ledger() || target.kind != focal_model::ObjectKind::Claim {
                return Err(ContractError::InvalidTarget.into());
            }
            let mut found = view.claim(ClaimId(target.id.0)).is_some();
            if !found {
                for candidate in claims {
                    visits.charge(1)?;
                    if candidate.content.id().0 == target.id.0 {
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                return Err(ContractError::InvalidTarget.into());
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One owner-resolved atomic creation context.
pub(in crate::native) fn prepare(
    claims: Vec<NativeAuthoredProposal>,
    request: RequestKey,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Plan, NativeError> {
    if view.state.profile != NativeContentProfile::AuthoredV1 || extras.authored.is_some() {
        return Err(ContractError::InvalidPolicy.into());
    }
    scratch.charge(array::<NativeAuthoredProposal>(claims.capacity())?)?;
    let mut total = claims.len();
    for claim in &claims {
        if claim.content.ledger() != view.ledger() {
            return Err(ContractError::WrongLedger.into());
        }
        scratch.charge(nested(
            claim.content.retained_heap_bytes()?,
            claim.content.heap_allocations()?,
        )?)?;
        scratch.charge(array::<ValidationDescriptor>(
            claim.declarations.capacity(),
        )?)?;
        total = add(total, claim.declarations.len())?;
        for descriptor in &claim.declarations {
            scratch.charge(nested(
                descriptor.retained_heap_bytes()?,
                descriptor.heap_allocations()?,
            )?)?;
        }
    }
    let mut visits = VisitBudget::new(limits.plan_edges);
    let mut proposals = scratch.reserve(claims.len())?;
    let mut mapping = scratch.reserve(total)?;
    let mut found = 0;
    for claim in &claims {
        let projection = AuthoredCreationPlan::prepare(
            context.principal,
            &claim.content,
            &claim.declarations,
            authored_creation::Profile {
                max_responses: claim.max_responses,
                scope_limits: claim.scope_limits,
                created: cut.position,
            },
            super::limits(limits, visits.remaining(), scratch.remaining()?),
        )?;
        visits.charge(projection.visits()?)?;
        let charge = projection.construction_bytes();
        scratch.charge(charge)?;
        proposals.push(creation::Proposal {
            definition: projection.build(charge)?.into_definition(),
            owner: claim.owner,
        });
        visits.charge(1)?;
        let resolved = claim_resolution(view, claim)?;
        found = add(found, usize::from(resolved.is_some()))?;
        push_mapping(
            &mut mapping,
            NativeCreatedFamily::Claim,
            claim.content.schema(),
            claim.content.content_hash(),
            ObjectId(claim.content.id().0),
            ObjectId(resolved.unwrap_or(claim.content.id()).0),
        )?;
        for descriptor in &claim.declarations {
            visits.charge(1)?;
            let resolved = definition_resolution(view, descriptor)?;
            found = add(found, usize::from(resolved.is_some()))?;
            push_mapping(
                &mut mapping,
                NativeCreatedFamily::Validation,
                descriptor.schema(),
                descriptor.content_hash(),
                descriptor.binding().object,
                resolved.map_or(descriptor.binding().object, |id| ObjectId(id.0)),
            )?;
        }
    }
    // Validate complete family-scoped identity membership before installing rows.
    // This also refuses same-content duplicates within an otherwise new batch.
    for (index, object) in mapping.iter().enumerate() {
        for earlier in mapping.iter().take(index) {
            visits.charge(1)?;
            if earlier.family == object.family
                && earlier.schema == object.schema
                && earlier.content == object.content
            {
                return Err(ContractError::ContentConflict.into());
            }
        }
    }
    if found != 0 && found != total {
        return Err(ContractError::ContentConflict.into());
    }
    let result_charge = NativeCreationResult::construction_heap(total)?;
    let result_visits = NativeCreationResult::inspection_visits(total)?;
    visits.charge(result_visits)?;
    let result = NativeCreationResult::from_owned(mapping, total, result_visits, result_charge)?;
    if found == 0 {
        references(&claims, view, &mut visits)?;
    }
    let created = if found == 0 { claims.len() } else { 0 };
    let definitions = if found == 0 {
        total.checked_sub(created).ok_or(ContractError::Capacity)?
    } else {
        0
    };
    let mut rows = Vec::new();
    if found == 0 {
        increment(&mut meta.claims, created, limits.claims, "claims")?;
        increment(
            &mut meta.definitions,
            definitions,
            limits.definitions,
            "definitions",
        )?;
        let plan = creation::CreationPlan::prepare(
            context.principal,
            proposals,
            view,
            cut,
            creation::Limits {
                nodes: limits.plan_nodes,
                edge_visits: visits.remaining(),
                bytes: scratch.remaining()?,
            },
        )?;
        scratch.charge(plan.retained_bytes()?)?;
        for replacement in plan.rows() {
            if replacement.status() != ClaimStatus::Superseded {
                continue;
            }
            let previous = view
                .claim(ClaimId(replacement.binding().object.0))
                .ok_or(ContractError::InvalidTarget)?;
            if let Some(token) = plan.supersession(previous.binding())? {
                super::super::transactions::fence_evaluations(
                    previous,
                    view,
                    extras,
                    scratch,
                    |state, definition| state.supersede(definition, previous, &token),
                )?;
            }
        }
        for claim in claims {
            let id = claim.content.id();
            let schema = claim.content.schema();
            let content = claim.content.content_hash();
            scratch.charge(OwnedClaimContent::container_charge())?;
            let owned = OwnedClaimContent::new(
                claim.content,
                claim.max_responses,
                claim.scope_limits,
                claim.owner,
            )?;
            extras.push(super::super::prepare::Extra {
                key: Key::ClaimContent(id),
                heap: owned.heap_charge()?,
                row: Row::ClaimContent(owned),
                fact: None,
            })?;
            extras.push(super::super::prepare::Extra {
                key: Key::ClaimIdentity(schema, content),
                heap: 0,
                row: Row::ClaimIdentity(id),
                fact: None,
            })?;
            for descriptor in claim.declarations {
                let binding = descriptor.binding();
                let schema = descriptor.schema();
                let content = descriptor.content_hash();
                let declaration = descriptor.declaration();
                let fact = NativeFact::Definition {
                    binding,
                    claim: declaration.claim(),
                    index: declaration.declaration_index(),
                    intent: declaration.intent_fingerprint(),
                };
                scratch.charge(OwnedDeclaration::authored_container_charge())?;
                let owned = OwnedDeclaration::new_authored(descriptor)?;
                extras.push(super::super::prepare::Extra {
                    key: Key::Definition(ValidationId(binding.object.0)),
                    heap: owned.heap_charge()?,
                    row: Row::Definition(owned),
                    fact: Some(fact),
                })?;
                extras.push(super::super::prepare::Extra {
                    key: Key::DefinitionIdentity(schema, content),
                    heap: 0,
                    row: Row::DefinitionIdentity(ValidationId(binding.object.0)),
                    fact: None,
                })?;
            }
        }
        rows = plan.into_rows();
        super::super::incoming_graph::stage_created(&rows, view, extras, scratch, limits)?;
        rows = super::super::control_graph::prepare(
            rows,
            view,
            NativeOperation::Create,
            cut,
            limits,
            extras,
            scratch,
        )?;
    }
    scratch.charge(OwnedCreationResult::container_charge())?;
    let owned = OwnedCreationResult::new(result)?;
    increment(
        &mut meta.creation_results,
        1,
        limits.outcomes,
        "creation results",
    )?;
    extras.push(super::super::prepare::Extra {
        key: Key::CreationResult(request.into()),
        heap: owned.heap_charge()?,
        row: Row::CreationResult(owned),
        fact: None,
    })?;
    extras.authored = Some(Proof {
        fingerprint: retained_fingerprint(extras)?,
        prefix: view.prefix(),
        request,
        claims: created,
        definitions,
        objects: total,
    });
    let registry =
        super::super::control_graph::created_registries(&rows, view, extras, limits, scratch)?;
    Ok(Plan {
        rows,
        registry,
        created,
    })
}
