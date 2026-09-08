use super::fields::{Frame, Projection, Projections};
use super::*;
use focal_model::lifecycle::{aggregation::AcceptanceSource, validation::DeclarationSource};

struct Summary {
    bytes: usize,
    allocations: usize,
    additional: LegacyCreationWork,
    intent: ContentHash,
    input: Option<NativeInput>,
}
fn add_heap(
    summary: &mut Summary,
    heap: usize,
    allocations: usize,
    maximum: usize,
) -> Result<(), DecodeError> {
    summary.bytes = sum(summary.bytes, sum(heap, product(allocations, ALLOCATION)?)?)?;
    summary.allocations = sum(summary.allocations, allocations)?;
    if summary.bytes > maximum {
        return Err(CodecError::Capacity.into());
    }
    Ok(())
}
fn unique_projection(
    frame: &Frame<'_>,
    current: &Projection<'_>,
    position: usize,
    work: &Work,
) -> Result<(), DecodeError> {
    for (index, previous) in Projections::new(frame.projections, work).enumerate() {
        work.structure.charge(1)?;
        let previous = previous?;
        if index != position && previous.binding.object == current.binding.object {
            return Err(ContractError::InvalidTarget.into());
        }
    }
    Ok(())
}
fn declaration_membership(
    frame: &Frame<'_>,
    checked: validation::CheckedDeclaration,
    position: usize,
    work: &Work,
) -> Result<(), DecodeError> {
    let mut count = 0;
    for projection in Projections::new(frame.projections, work) {
        work.structure.charge(1)?;
        if projection?.binding.object.0 == checked.claim().0 {
            count = sum(count, 1)?;
        }
    }
    if count != 1 {
        return Err(ContractError::InvalidTarget.into());
    }
    for (index, previous) in cohort::Bodies::new(frame.declarations, work).enumerate() {
        work.structure.charge(1)?;
        let previous = previous?;
        if index != position
            && previous.source().fields().binding.object == checked.binding().object
        {
            return Err(ContractError::InvalidTarget.into());
        }
    }
    Ok(())
}
fn graph_and_lineage(
    value: &Projection<'_>,
    native: NativeLimits,
    work: &Work,
) -> Result<(graph::Declaration, succession::Lineage), DecodeError> {
    let mut obligations = reserve(value.obligations.count)?;
    for next in Values::new(value.obligations, &work.source, fields::obligation) {
        work.structure.charge(1)?;
        if obligations.len() == value.obligations.count {
            return Err(CodecError::Capacity.into());
        }
        obligations.push(next?);
    }
    if obligations.len() != value.obligations.count {
        return Err(ContractError::InvalidManifest.into());
    }
    let graph = work.model(&work.structure, |visits| {
        graph::Declaration::from_owned_sorted(obligations, native.plan_edges, visits)
    })?;
    let mut corrections = reserve(value.corrections.count)?;
    for next in Values::new(value.corrections, &work.source, fields::correction) {
        work.structure.charge(1)?;
        if corrections.len() == value.corrections.count {
            return Err(CodecError::Capacity.into());
        }
        corrections.push(next?);
    }
    if corrections.len() != value.corrections.count {
        return Err(ContractError::InvalidManifest.into());
    }
    let cause = match &value.cause {
        Cause::Root(root) => Cause::Root(*root),
        Cause::Claim(claim) => Cause::Claim(*claim),
    };
    let lineage = work.model(&work.structure, |visits| {
        succession::Lineage::from_owned_sorted(
            value.lineage,
            cause,
            corrections,
            native.plan_edges,
            visits,
        )
    })?;
    Ok((graph, lineage))
}

fn run(
    bytes: &[u8],
    header: InputHeader,
    native: NativeLimits,
    limits: LegacyCreationLimits,
    work: &Work,
    build: bool,
) -> Result<Summary, DecodeError> {
    let maximum = limits.bytes.min(native.preparation_bytes);
    let frame = Frame::read(bytes, header, native, work)?;
    let request = header.request.ok_or(CodecError::InvalidTag("request"))?;
    let principal = Principal::Actor(request.principal);
    let mut summary = Summary {
        bytes: sum(
            size_of::<NativeInput>(),
            sum(
                vector_bytes::<creation::Proposal>(frame.projections.count)?,
                vector_bytes::<validation::Declaration>(frame.declarations.count)?,
            )?,
        )?,
        allocations: sum(
            usize::from(frame.projections.count != 0),
            usize::from(frame.declarations.count != 0),
        )?,
        additional: LegacyCreationWork::zero(),
        intent: ContentHash([0; 32]),
        input: None,
    };
    if summary.bytes > maximum {
        return Err(CodecError::Capacity.into());
    }
    // A build can reach here only through an immutable, fully prepared frame
    // whose complete final footprint and work quote were checked before entry.
    let mut claims = if build {
        reserve(frame.projections.count)?
    } else {
        Vec::new()
    };
    let mut declarations = if build {
        reserve(frame.declarations.count)?
    } else {
        Vec::new()
    };
    work.structure.charge(64)?;
    let mut creation = creation::CreationIntent::new(frame.projections.count)?;
    for (position, projection) in Projections::new(frame.projections, work).enumerate() {
        let projection = projection?;
        projection.check(header, native, work)?;
        unique_projection(&frame, &projection, position, work)?;
        let mut source = cohort::Cohort {
            slots: projection.slots,
            declarations: frame.declarations,
            count: 0,
            claim: ClaimId(projection.binding.object.0),
            principal,
            limits: limits.declaration,
            work,
        };
        let before = work.used()?;
        let mut matched = 0;
        for value in source.declarations() {
            value?;
            matched = sum(matched, 1)?;
        }
        let declaration_pass = work.used()?.subtract(before)?;
        source.count = matched;
        let acceptance = aggregation::AcceptancePolicy::prepare_source(
            projection.acceptance,
            projection.acceptance_issuer,
            &source,
            limits.acceptance,
            work.acceptance.remaining(),
        )?;
        work.acceptance.charge(acceptance.inspection_visits())?;
        let fingerprint = acceptance.intent_fingerprint();
        add_heap(
            &mut summary,
            acceptance.construction_heap_bytes(),
            acceptance.construction_heap_allocations(),
            maximum,
        )?;
        let graph_allocations = sum(
            usize::from(projection.obligations.count != 0),
            usize::from(projection.corrections.count != 0),
        )?;
        add_heap(
            &mut summary,
            sum(
                product(size_of::<graph::Obligation>(), projection.obligations.count)?,
                product(
                    size_of::<succession::Correction>(),
                    projection.corrections.count,
                )?,
            )?,
            graph_allocations,
            maximum,
        )?;
        let passes = acceptance.build_passes();
        let mut additional = declaration_pass.multiply(passes.declarations)?;
        // Each slot source scan consumes its 13-byte header with count checks,
        // its skipped 21-byte encoded check span, and each decoded check (24).
        additional.source = sum(
            additional.source,
            product(
                sum(
                    product(projection.slots.count, 19)?,
                    product(projection.checks, 45)?,
                )?,
                passes.slots,
            )?,
        )?;
        additional.acceptance = sum(additional.acceptance, acceptance.build_visits())?;
        additional.source = sum(
            additional.source,
            sum(
                product(projection.obligations.count, 19)?,
                product(projection.corrections.count, 56)?,
            )?,
        )?;
        additional.structure = sum(
            additional.structure,
            sum(
                3,
                product(
                    sum(projection.obligations.count, projection.corrections.count)?,
                    2,
                )?,
            )?,
        )?;
        summary.additional = summary.additional.add(additional)?;
        projection.hash(fingerprint, &mut creation, work)?;
        if build {
            let charge = acceptance.construction_charge();
            let visits = acceptance.build_visits();
            work.acceptance.charge(visits)?;
            let acceptance = acceptance.build(charge, visits)?;
            let (graph, lineage) = graph_and_lineage(&projection, native, work)?;
            if claims.len() == frame.projections.count {
                return Err(CodecError::Capacity.into());
            }
            claims.push(creation::Proposal {
                definition: ClaimDefinition {
                    binding: projection.binding,
                    issuer: projection.issuer,
                    subject: projection.subject,
                    deadline: projection.deadline,
                    max_responses: projection.max_responses,
                    created: SessionSeq(0),
                    graph,
                    lineage,
                    acceptance,
                    scope_limits: projection.scope_limits,
                },
                owner: projection.owner,
            });
        }
    }
    work.structure.charge(256)?;
    let mut hash = crate::native::intent::request_hasher(header.ledger, request);
    hash.update(&[0]);
    hash.update(&creation.finish()?.0);
    hash.update(
        &u64::try_from(frame.declarations.count)
            .map_err(|_| CodecError::Capacity)?
            .to_le_bytes(),
    );
    for (position, body) in cohort::Bodies::new(frame.declarations, work).enumerate() {
        let mut body = body?;
        let (info, owned) = cohort::process(&mut body, principal, limits.declaration, work, build)?;
        declaration_membership(&frame, info.checked, position, work)?;
        add_heap(&mut summary, info.heap, info.allocations, maximum)?;
        summary.additional.declarations = sum(summary.additional.declarations, info.model_build)?;
        summary.additional.source = sum(summary.additional.source, info.source_build)?;
        work.structure.charge(32)?;
        hash.update(&info.intent.0);
        if let Some(owned) = owned {
            if declarations.len() == frame.declarations.count {
                return Err(CodecError::Capacity.into());
            }
            declarations.push(owned);
        }
    }
    summary.intent = ContentHash(*hash.finalize().as_bytes());
    // Owned reinspection covers the same final body once more. Sixteen times
    // encoded bytes covers all native identity framing, including short fields;
    // one fixed allowance per row covers metadata and retained-capacity scans.
    let final_work = sum(
        product(bytes.len(), 16)?,
        product(
            sum(sum(frame.projections.count, frame.declarations.count)?, 1)?,
            1024,
        )?,
    )?;
    summary.additional.structure = sum(summary.additional.structure, final_work)?;
    if build {
        work.structure.charge(final_work)?;
        if claims.len() != frame.projections.count || declarations.len() != frame.declarations.count
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let input = NativeInput {
            request,
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        };
        if crate::native::intent::fingerprint(header.ledger, &input)? != summary.intent {
            return Err(ContractError::InvalidManifest.into());
        }
        crate::native::intent::bound_input(&input.command, native)?;
        if retained(&input)? != (summary.bytes, summary.allocations) {
            return Err(CodecError::Capacity.into());
        }
        summary.input = Some(input);
    }
    Ok(summary)
}

fn retained(input: &NativeInput) -> Result<(usize, usize), DecodeError> {
    let NativeCommand::Create {
        claims,
        declarations,
    } = &input.command
    else {
        return Err(ContractError::InvalidManifest.into());
    };
    let mut bytes = sum(
        size_of::<NativeInput>(),
        sum(
            vector_bytes::<creation::Proposal>(claims.capacity())?,
            vector_bytes::<validation::Declaration>(declarations.capacity())?,
        )?,
    )?;
    let mut allocations = sum(
        usize::from(claims.capacity() != 0),
        usize::from(declarations.capacity() != 0),
    )?;
    for value in claims {
        for (heap, count) in [
            (
                value.definition.graph.retained_heap_bytes()?,
                value.definition.graph.heap_allocations()?,
            ),
            (
                value.definition.lineage.retained_heap_bytes()?,
                value.definition.lineage.heap_allocations()?,
            ),
            (
                value.definition.acceptance.retained_heap_bytes()?,
                value.definition.acceptance.heap_allocations()?,
            ),
        ] {
            bytes = sum(bytes, sum(heap, product(count, ALLOCATION)?)?)?;
            allocations = sum(allocations, count)?;
        }
    }
    for value in declarations {
        bytes = sum(
            bytes,
            sum(
                value.retained_heap_bytes()?,
                product(value.heap_allocations()?, ALLOCATION)?,
            )?,
        )?;
        allocations = sum(allocations, value.heap_allocations()?)?;
    }
    Ok((bytes, allocations))
}

pub(super) fn prepare<'a>(
    bytes: &'a [u8],
    header: InputHeader,
    native: NativeLimits,
    limits: LegacyCreationLimits,
) -> Result<LegacyCreationPlan<'a>, DecodeError> {
    let work = Work::new(limits.work);
    let summary = run(bytes, header, native, limits, &work, false)?;
    let preparation = work.used()?;
    let construction = preparation.add(summary.additional)?;
    let quote = LegacyCreationQuote {
        bytes: summary.bytes,
        allocations: summary.allocations,
        preparation,
        construction,
    };
    Ok(LegacyCreationPlan {
        bytes,
        header,
        native,
        limits,
        quote,
        intent: summary.intent,
    })
}
pub(super) fn build(
    plan: LegacyCreationPlan<'_>,
    max_bytes: usize,
    allowance: LegacyCreationWork,
) -> Result<NativeInput, DecodeError> {
    if plan.quote.bytes > max_bytes {
        return Err(CodecError::Capacity.into());
    }
    plan.quote.construction.fits(allowance)?;
    let work = Work::new(plan.quote.construction);
    let summary = run(
        plan.bytes,
        plan.header,
        plan.native,
        plan.limits,
        &work,
        true,
    )?;
    if summary.bytes != plan.quote.bytes
        || summary.allocations != plan.quote.allocations
        || summary.intent != plan.intent
    {
        return Err(ContractError::InvalidManifest.into());
    }
    summary
        .input
        .ok_or_else(|| ContractError::InvalidManifest.into())
}
