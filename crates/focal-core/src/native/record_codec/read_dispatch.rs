//! Scoped decoding for every retained native row family. Bodies and dependencies
//! stay borrowed; only checked final rows escape. Policy and audit scratch space
//! are reserved independently of the caller's final row allowance.
use super::*;
use bytes::Cursor;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    aggregation, artifact_descriptor, claim::ClaimState, claim_descriptor,
    evidence::ResponseLimits, validation, validation_descriptor,
};
use read_evidence as evidence;
use read_source::Meter;

#[path = "read_dispatch_objects.rs"]
pub(super) mod objects;
#[cfg(test)]
#[path = "read_dispatch_tests.rs"]
mod tests;
pub(super) use super::read_index::Origin as ArtifactOrigin;
use objects::Access;

/// Immutable dependencies may come from a complete checkpoint, a changed row,
/// or the actual unchanged base claim. Changed rows take precedence even before
/// their full lifecycle state has been restored.
pub(super) enum ClaimDependency<'a> {
    Raw(&'a [u8]),
    Retained(&'a ClaimState),
}
/// Each lookup must debit `meter` before traversing the restored store/index.
/// A staged changed claim must yield its original raw body until built; it must
/// never fall back to the old base row. Deleted and missing claims are errors.
pub(super) trait Objects {
    fn ledger(&self) -> LedgerId;
    fn prefix(&self) -> SessionSeq;
    fn get(&self, key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError>;
    fn claim_dependency(
        &self,
        id: ClaimId,
        meter: &Meter,
    ) -> Result<ClaimDependency<'_>, NativeError>;
    fn artifact_origin(&self, id: ArtifactId, meter: &Meter)
    -> Result<ArtifactOrigin, NativeError>;
}
#[derive(Debug, Clone, Copy)]
pub(super) struct Limits {
    pub(super) native: NativeLimits,
    pub(super) acceptance: aggregation::Limits,
    pub(super) artifact: artifact_descriptor::Limits,
    pub(super) claim: claim_descriptor::Limits,
    pub(super) declaration: validation::Limits,
    pub(super) validation: validation_descriptor::Limits,
    pub(super) response: ResponseLimits,
    pub(super) creation_objects: usize,
}
pub(super) struct Context<'a, O, C> {
    pub(super) objects: &'a O,
    pub(super) custody: &'a C,
    pub(super) workspace: &'a MemoryBudget,
    pub(super) workspace_lane: BudgetLane,
    pub(super) parsing: &'a Meter,
    pub(super) source: &'a Meter,
    pub(super) model: &'a Meter,
    pub(super) lookup: &'a Meter,
    pub(super) limits: Limits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Quote {
    pub(super) heap_bytes: usize,
    pub(super) workspace_bytes: usize,
}
fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b)
        .ok_or(NativeError::Capacity("recovery dispatch work"))
}
fn debit(meter: &Meter, work: usize) -> Result<(), NativeError> {
    meter.charge(work).map_err(evidence::codec)
}
/// Reconcile actual cursor work even when a native decoder refuses. Parsing a
/// value never silently accepts trailing bytes, including present empty links.
fn parse<'a, T>(
    body: &'a [u8],
    meter: &Meter,
    read: impl FnOnce(&mut Cursor<'a>) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    let mut cursor = Cursor::new(body, body.len(), meter.remaining()).map_err(evidence::codec)?;
    let result = read(&mut cursor);
    let used = cursor.visits_used();
    debit(meter, used)?;
    let value = result?;
    cursor.finish().map_err(evidence::codec)?;
    Ok(value)
}
fn quoted(
    heap_bytes: usize,
    workspace_bytes: usize,
    build: Option<(Quote, usize)>,
) -> Result<Quote, NativeError> {
    let quote = Quote {
        heap_bytes,
        workspace_bytes,
    };
    if let Some((expected, allowance)) = build
        && (quote != expected || heap_bytes > allowance)
    {
        return Err(NativeError::Capacity("recovery row reservation"));
    }
    Ok(quote)
}
/// A refused opaque model/source pass consumes its offered allowance. Successful
/// passes reconcile their explicit quote. Neither path can reset a row's budget.
fn inspect<P>(
    context: &Context<'_, impl Objects, impl evidence::Custody>,
    read: impl FnOnce(usize, usize) -> Result<(P, evidence::Quote), NativeError>,
) -> Result<(P, evidence::Quote), NativeError> {
    let model = context.model.remaining();
    let source = context.source.remaining();
    let result = read(model, source);
    match result {
        Ok((plan, quote)) => {
            debit(context.model, quote.model_inspection_visits)?;
            debit(context.source, quote.source_inspection_visits)?;
            Ok((plan, quote))
        }
        Err(error) => {
            debit(context.model, model)?;
            debit(context.source, source)?;
            Err(error)
        }
    }
}
fn build_evidence(
    context: &Context<'_, impl Objects, impl evidence::Custody>,
    quote: evidence::Quote,
    build: impl FnOnce(usize, usize) -> Result<Row, NativeError>,
) -> Result<Row, NativeError> {
    debit(context.model, quote.model_build_visits)?;
    debit(context.source, quote.source_build_visits)?;
    build(quote.heap_bytes, quote.model_build_visits)
}

pub(super) fn prepare<O: Objects, C: evidence::Custody>(
    row: &EncodedRow<'_>,
    context: &Context<'_, O, C>,
) -> Result<Quote, NativeError> {
    let access = Access::new(context.objects, context.lookup);
    let result = process(row, context, &access, None).map(|(quote, _)| quote);
    access.finish(result)
}
/// The caller holds the complete retained reservation before entry. The callback
/// is the only escape for a checked owned row; it receives its actual heap size.
pub(super) fn with_build<O: Objects, C: evidence::Custody, T>(
    row: &EncodedRow<'_>,
    context: &Context<'_, O, C>,
    expected: Quote,
    retained_allowance: usize,
    finish: impl FnOnce(Row, usize) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    let access = Access::new(context.objects, context.lookup);
    let built = process(row, context, &access, Some((expected, retained_allowance)));
    let (quote, built) = access.finish(built)?;
    let value = built.ok_or_else(invalid)?;
    // Includes all nested allocation counters and immutable identity checks.
    // Heap size bounds collection elements; fixed inline work has its own debit.
    debit(context.model, add(quote.heap_bytes, 4096)?)?;
    objects::check(row.key, &value, context.objects.ledger())?;
    let actual = objects::heap(&value)?;
    if actual > quote.heap_bytes || actual > retained_allowance {
        return Err(NativeError::Capacity("recovery actual heap"));
    }
    finish(value, actual)
}

fn raw_claim<'a, O: Objects>(
    id: ClaimId,
    body: &'a [u8],
    access: &Access<'_, O>,
    parsing: &Meter,
) -> Result<read_claim::Input<'a>, NativeError> {
    let input = parse(body, parsing, |c| {
        read_claim::Input::read(c).map_err(evidence::codec)
    })?;
    if input.fields.binding.ledger != access.ledger()
        || input.fields.binding.object.0 != id.0
        || input.acceptance.ledger != input.fields.binding.ledger
        || input.acceptance.object != input.fields.binding.object
        || input.acceptance.content != input.fields.binding.content
        || input.acceptance_issuer != input.fields.issuer
    {
        return Err(invalid());
    }
    Ok(input)
}
fn claim_issuer<O: Objects>(
    id: ClaimId,
    access: &Access<'_, O>,
    parsing: &Meter,
) -> Result<focal_model::ParticipantId, NativeError> {
    match access.claim_dependency(id)? {
        ClaimDependency::Raw(body) => Ok(raw_claim(id, body, access, parsing)?.fields.issuer),
        ClaimDependency::Retained(claim) => Ok(claim.issuer()),
    }
}
/// Unchanged policies remain borrowed from their actual retained claims. A raw
/// changed claim supplies a temporary immutable policy, never a synthetic
/// ClaimState; its scratch reservation outlives every scoped consumer.
fn with_policy<O: Objects, C: evidence::Custody, T>(
    claim: ClaimId,
    context: &Context<'_, O, C>,
    access: &Access<'_, O>,
    consume: impl FnOnce(&aggregation::AcceptancePolicy, usize) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    let body = match access.claim_dependency(claim)? {
        ClaimDependency::Raw(body) => body,
        ClaimDependency::Retained(claim) => return consume(claim.acceptance(), 0),
    };
    let input = raw_claim(claim, body, access, context.parsing)?;
    let source = input.acceptance_source(access, context.source);
    let plan = context.model.model(|visits| {
        let plan = aggregation::AcceptancePolicy::prepare_source_with(
            input.acceptance,
            input.acceptance_issuer,
            &source,
            context.limits.acceptance,
            visits,
            read_claim::policy_shape(input.fields.origin),
        )?;
        let used = plan.inspection_visits();
        Ok((plan, used))
    })?;
    let headers = plan
        .construction_heap_allocations()
        .checked_mul(crate::native::prepare::ALLOCATION)
        .ok_or(NativeError::Capacity("recovery policy allocation count"))?;
    let workspace_bytes = add(plan.construction_heap_bytes(), headers)?;
    let _workspace = context.workspace.reserve(
        BudgetKind::Recovery,
        context.workspace_lane,
        workspace_bytes,
    )?;
    let charge = plan.construction_charge();
    let visits = plan.build_visits();
    debit(context.model, visits)?;
    let policy = plan.build(charge, visits)?;
    // The model plan verifies actual owned capacities before returning. Its
    // quoted allocation count is also the native allocator bookkeeping charge.
    consume(&policy, workspace_bytes)
}

fn process<O: Objects, C: evidence::Custody>(
    encoded: &EncodedRow<'_>,
    context: &Context<'_, O, C>,
    access: &Access<'_, O>,
    build: Option<(Quote, usize)>,
) -> Result<(Quote, Option<Row>), NativeError> {
    debit(context.model, 16)?;
    if encoded.deleted() || encoded.key == Key::End {
        return Err(invalid());
    }
    let body = encoded.body();
    let ledger = context.objects.ledger();
    macro_rules! evidence_plan {
        ($plan:expr, $quote:expr, $workspace:expr) => {{
            let (plan, value_quote) = ($plan, $quote);
            let quote = quoted(value_quote.heap_bytes, $workspace, build)?;
            let row = if build.is_some() {
                Some(build_evidence(context, value_quote, |bytes, visits| {
                    plan.build(bytes, visits)
                })?)
            } else {
                None
            };
            Ok((quote, row))
        }};
    }
    match encoded.key {
        Key::Claim(id) => {
            let input = parse(body, context.parsing, |c| {
                read_claim::Input::read(c).map_err(evidence::codec)
            })?;
            if input.fields.binding.ledger != ledger || input.fields.binding.object.0 != id.0 {
                return Err(invalid());
            }
            let limits = read_claim::Limits {
                native: context.limits.native,
                acceptance: context.limits.acceptance,
            };
            let value_quote = input.quote(access, limits, context.source, context.model)?;
            let quote = quoted(value_quote.heap_bytes, 0, build)?;
            let row = if build.is_some() {
                Some(
                    input
                        .build(
                            access,
                            limits,
                            context.source,
                            context.model,
                            quote.heap_bytes,
                        )?
                        .0,
                )
            } else {
                None
            };
            Ok((quote, row))
        }
        Key::Evaluation(key) => {
            let input = parse(body, context.parsing, |c| {
                read_evaluation::Input::read(c).map_err(evidence::codec)
            })?;
            let declaration = access.declaration(key.validation)?;
            let plan = input.prepare(key, declaration, context.model)?;
            let quote = quoted(plan.heap_bytes(), 0, build)?;
            let row = if build.is_some() {
                debit(context.model, 1)?;
                Some(plan.build(quote.heap_bytes)?.0)
            } else {
                None
            };
            Ok((quote, row))
        }
        Key::ResultTestament(id) => {
            let input = parse(body, context.parsing, |c| {
                read_audit::AuditBody::read(c).map_err(evidence::codec)
            })?;
            if input.snapshot.binding.object.0 != id.0 || input.snapshot.binding.ledger != ledger {
                return Err(invalid());
            }
            if input.members.count > context.limits.native.evaluations_per_claim
                || input.results.count > context.limits.native.results
                || input.snapshot.cohort.result_capacity
                    > u64::try_from(context.limits.native.results)
                        .map_err(|_| NativeError::Capacity("audit result limit"))?
            {
                return Err(NativeError::Capacity("audit row limits"));
            }
            let value_quote = input.quote().map_err(evidence::codec)?;
            let quote = quoted(
                value_quote.retained_bytes,
                value_quote.workspace_bytes,
                build,
            )?;
            let row = if build.is_some() {
                let _workspace = context.workspace.reserve(
                    BudgetKind::Recovery,
                    context.workspace_lane,
                    quote.workspace_bytes,
                )?;
                let (value, _) = input
                    .build_with_meters(access, value_quote, context.source, context.model)
                    .map_err(evidence::codec)?;
                Some(Row::ResultTestament(value))
            } else {
                None
            };
            Ok((quote, row))
        }
        Key::Artifact(id) => {
            let mut input = parse(body, context.parsing, evidence::artifact)?;
            let origin = context.objects.artifact_origin(id, context.lookup)?;
            let fields = input.fields();
            if fields.ledger != ledger
                || fields.id != id
                || origin.binding.revision != focal_model::ObjectRevision(1)
                || origin.binding.ledger != ledger
                || origin.binding.object.0 != id.0
                || origin.position.sequence.0 == 0
                || origin.position.sequence > context.objects.prefix()
            {
                return Err(invalid());
            }
            let request = origin.request.resolve(ledger, id, fields.producer);
            let (plan, value_quote) = inspect(context, |model, source| {
                let plan = input.prepare(request, context.limits.artifact, model, source)?;
                let quote = plan.quote();
                Ok((plan, quote))
            })?;
            let quote = quoted(value_quote.heap_bytes, 0, build)?;
            let row = if build.is_some() {
                Some(build_evidence(context, value_quote, |bytes, visits| {
                    plan.build(bytes, visits, context.custody)
                })?)
            } else {
                None
            };
            if let Some(Row::Artifact(value)) = &row {
                let descriptor = value.get().ok_or_else(invalid)?.descriptor();
                if descriptor.content_hash() != origin.binding.content {
                    return Err(ContractError::ContentConflict.into());
                }
            }
            Ok((quote, row))
        }
        Key::Definition(_) => {
            let mut input = parse(body, context.parsing, evidence::definition)?;
            let issuer = claim_issuer(input.claim(), access, context.parsing)?;
            let (plan, value_quote) = inspect(context, |model, source| {
                let plan = input.prepare(
                    issuer,
                    context.limits.declaration,
                    context.limits.validation,
                    model,
                    source,
                )?;
                let quote = plan.quote();
                Ok((plan, quote))
            })?;
            evidence_plan!(plan, value_quote, 0)
        }
        Key::ClaimContent(_) => {
            let offered = context.source.remaining();
            let parsed = parse(body, context.parsing, |c| {
                evidence::claim_content(c, offered)
            });
            let mut input = match parsed {
                Ok(input) => {
                    debit(context.source, input.parse_source_visits())?;
                    input
                }
                Err(error) => {
                    debit(context.source, offered)?;
                    return Err(error);
                }
            };
            let (plan, value_quote) = inspect(context, |model, source| {
                let plan = input.prepare(context.limits.claim, model, source)?;
                let quote = plan.quote();
                Ok((plan, quote))
            })?;
            evidence_plan!(plan, value_quote, 0)
        }
        Key::Work(_) => {
            let input = parse(body, context.parsing, evidence::work)?;
            let evidence::ScalarInput::Work(snapshot, _) = input else {
                return Err(invalid());
            };
            with_policy(snapshot.claim, context, access, |policy, workspace| {
                let dependencies = objects::Policy { access, policy };
                let (plan, value_quote) = inspect(context, |model, _source| {
                    let plan = input.prepare(&dependencies, model)?;
                    let quote = plan.quote();
                    Ok((plan, quote))
                })?;
                evidence_plan!(plan, value_quote, workspace)
            })
        }
        Key::Diagnostic(_) | Key::Accepted(_) | Key::DeliveryResult(_) | Key::MissingResult(_) => {
            let input = parse(body, context.parsing, |c| match encoded.key {
                Key::Diagnostic(_) => evidence::diagnostic(c),
                Key::Accepted(_) => evidence::accepted(c),
                Key::DeliveryResult(_) => evidence::delivery(c),
                Key::MissingResult(_) => evidence::missing(c),
                _ => Err(invalid()),
            })?;
            let (plan, value_quote) = inspect(context, |model, _source| {
                let plan = input.prepare(access, model)?;
                let quote = plan.quote();
                Ok((plan, quote))
            })?;
            evidence_plan!(plan, value_quote, 0)
        }
        Key::Response(_) => {
            let input = parse(body, context.parsing, |c| {
                evidence::response(
                    c,
                    context.limits.native.response_summary_bytes,
                    context.source.remaining(),
                )
            })?;
            with_policy(
                input.fields().identity.claim,
                context,
                access,
                |policy, workspace| {
                    // The policy pass spent shared source work; the response adapter
                    // receives only the remaining allowance for its own callbacks.
                    let input = parse(body, context.parsing, |c| {
                        evidence::response(
                            c,
                            context.limits.native.response_summary_bytes,
                            context.source.remaining(),
                        )
                    })?;
                    let (plan, value_quote) = inspect(context, |model, _source| {
                        let plan = input.prepare(policy, access, context.limits.response, model)?;
                        let quote = plan.quote();
                        Ok((plan, quote))
                    })?;
                    evidence_plan!(plan, value_quote, workspace)
                },
            )
        }
        Key::CreationResult(_) => {
            let input = parse(body, context.parsing, |c| {
                evidence::creation(c, context.source.remaining())
            })?;
            let (plan, value_quote) = inspect(context, |model, _source| {
                let plan = input.prepare(context.limits.creation_objects, model)?;
                let quote = plan.quote();
                Ok((plan, quote))
            })?;
            evidence_plan!(plan, value_quote, 0)
        }
        Key::Event(..) => {
            let plan = parse(body, context.parsing, |c| {
                read_rows::EventPlan::read(encoded.key, c, ledger)
            })?;
            let quote = quoted(plan.heap_bytes(), 0, build)?;
            let row = if build.is_some() {
                let visits = plan.build_visits();
                debit(context.model, visits)?;
                Some(plan.build(quote.heap_bytes, visits)?.0)
            } else {
                None
            };
            Ok((quote, row))
        }
        Key::IncomingHead(_)
        | Key::IncomingLink(..)
        | Key::Monitor(_)
        | Key::MonitorHead(_)
        | Key::MonitorLink(..)
        | Key::Meta
        | Key::ArtifactIdentity(_)
        | Key::Receipt(_)
        | Key::Cycle(_)
        | Key::RetiredCycleHead(_)
        | Key::Retired(_)
        | Key::RetiredCycle(_)
        | Key::WorkSlot(..)
        | Key::ClaimResultTestament(_)
        | Key::Outcome(_)
        | Key::ClaimIdentity(..)
        | Key::DefinitionIdentity(..)
        | Key::ByIssuer(..)
        | Key::BySubject(..)
        | Key::ByStatus(..)
        | Key::ByAction(..)
        | Key::ByScope(..)
        | Key::ByRelation(..)
        | Key::ByProducer(..)
        | Key::ByArtifactKind(..)
        | Key::BySchema(..)
        | Key::ArtifactInput(..)
        | Key::ByEvaluator(..)
        | Key::ByVerdict(..)
        | Key::ByCreated(..)
        | Key::DueTimer(..)
        | Key::ByObject(..) => {
            let row = parse(body, context.parsing, |c| {
                read_rows::read_fixed(encoded.key, c, ledger)
            })?
            .ok_or_else(invalid)?;
            let quote = quoted(0, 0, build)?;
            Ok((quote, build.map(|_| row)))
        }
        Key::LegacyTestament(_)
        | Key::LegacyEvidenceSet(_)
        | Key::LegacyRun(..)
        | Key::LegacyDefinition(_) => {
            let bytes = parse(body, context.parsing, |c| {
                let len = c
                    .count(context.limits.native.legacy_row_bytes)
                    .map_err(evidence::codec)?;
                c.take(len).map_err(evidence::codec)
            })?;
            let quote = quoted(OwnedLegacy::charge(bytes.len())?, 0, build)?;
            let row = if build.is_some() {
                let owned = OwnedLegacy::new(bytes)?;
                Some(match encoded.key {
                    Key::LegacyTestament(_) => Row::LegacyTestament(owned),
                    Key::LegacyEvidenceSet(_) => Row::LegacyEvidenceSet(owned),
                    Key::LegacyDefinition(_) => Row::LegacyDefinition(owned),
                    _ => Row::LegacyRun(owned),
                })
            } else {
                None
            };
            Ok((quote, row))
        }
        Key::End => Err(invalid()),
    }
}
