//! Owner-resolved native mutations. All rows are detached until the common
//! range publication installs claim, registry, definition, evaluation and history.
use super::prepare::{ALLOCATION, Extras, Scratch, add, array, heap};
use super::*;
use focal_model::ObjectRevision;
use focal_model::lifecycle::{
    claim::{ClaimCut, ClaimState},
    creation, ownership,
};

pub(super) struct Plan {
    pub rows: Vec<ClaimState>,
    pub registry: Option<(ClaimId, RegistrationSet)>,
    pub created: usize,
}

pub(super) fn increment(
    value: &mut usize,
    count: usize,
    max: usize,
    name: &'static str,
) -> Result<(), NativeError> {
    let next = add(*value, count)?;
    if next > max {
        return Err(NativeError::Capacity(name));
    }
    *value = next;
    Ok(())
}
pub(super) fn registry_heap(registry: &RegistrationSet) -> Result<usize, NativeError> {
    add(
        registry.retained_heap_bytes()?,
        registry
            .heap_allocations()?
            .checked_mul(ALLOCATION)
            .ok_or(NativeError::Capacity("registry heap"))?,
    )
}
pub(super) fn copy_registry(
    view: &View<'_>,
    claim: &ClaimState,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<RegistrationSet, NativeError> {
    let id = ClaimId(claim.binding().object.0);
    match view.get(Key::Claim(id)) {
        Some(Row::Claim(row)) => {
            let registry = row.registrations().ok_or(ContractError::InvalidTarget)?;
            registry.check(claim)?;
            let charge = registry_heap(registry)?;
            scratch.charge(charge)?;
            let copied = registry.try_copy(registry.retained_bytes()?)?;
            if registry_heap(&copied)? > charge {
                return Err(NativeError::Capacity("registry copy"));
            }
            Ok(copied)
        }
        None => Ok(RegistrationSet::new(
            claim,
            limits.evaluations_per_claim,
            size_of::<RegistrationSet>(),
        )?),
        Some(_) => Err(ContractError::InvalidTarget.into()),
    }
}

/// No caller supplies the evaluation list. The authoritative compact membership
/// on this exact effective claim names every independent evaluation row.
fn fence_evaluations(
    claim: &ClaimState,
    view: &View<'_>,
    extras: &mut Extras,
    scratch: &mut Scratch,
    mut fence: impl FnMut(
        validation::EvaluationState,
        &validation::Declaration,
    ) -> Result<validation::EvaluationState, ContractError>,
) -> Result<(), NativeError> {
    let id = ClaimId(claim.binding().object.0);
    let registry = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    registry.check(claim)?;
    for registered in registry.rows() {
        let declaration = view.definition(ValidationId(registered.binding().object.0))?;
        let key = key_for_registered(id, *registered);
        let previous = *view.evaluation(key)?;
        registered.check_state(previous, declaration)?;
        let next = fence(previous, declaration)?;
        if next != previous {
            extras.evaluation(id, declaration, Some(previous.binding()), next, scratch)?;
        }
    }
    Ok(())
}

pub(super) fn key_for_registered(
    claim: ClaimId,
    row: focal_model::lifecycle::aggregation::RegisteredEvaluation,
) -> EvaluationKey {
    EvaluationKey {
        claim,
        validation: ValidationId(row.binding().object.0),
        generation: row.generation(),
        target: EvaluationTarget::of(row.target()),
    }
}

fn check_retained_definitions(claim: &ClaimState, view: &View<'_>) -> Result<(), NativeError> {
    for summary in claim.acceptance().declarations() {
        let declaration = view.definition(ValidationId(summary.binding().object.0))?;
        claim.acceptance().check_declaration(declaration)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One internal transaction context; no transport-facing options.
pub(super) fn prepare(
    command: NativeCommand,
    request: RequestKey,
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Plan, NativeError> {
    match command {
        NativeCommand::EnterWholeWork { claim, expected } => super::whole_work::enter(
            view, context, claim, expected, cut, limits, meta, extras, scratch,
        ),
        NativeCommand::SealIncrementTargets { claim } => {
            super::increment_seal::prepare(view, context, claim, limits, scratch)
        }
        NativeCommand::SubmitWork { .. }
        | NativeCommand::FailWorkProduction { .. }
        | NativeCommand::RejectWork { .. }
        | NativeCommand::SubmitDiagnostic { .. }
        | NativeCommand::ReceiveWork { .. } => super::work_artifacts::prepare(
            command, request, evidence, context, cut, view, limits, meta, extras, scratch,
        ),
        NativeCommand::CloseResponse { .. }
        | NativeCommand::PostResponse { .. }
        | NativeCommand::ReceiveResponse { .. } => {
            super::responses::prepare(command, context, view, limits, meta, extras, scratch)
        }
        NativeCommand::AcquireReceipt { expected, receipt } => super::receipt::prepare(
            expected, receipt, context, cut, view, limits, meta, extras, scratch,
        ),
        NativeCommand::ReportAdmission {
            claim,
            key,
            expected,
            report,
            artifact,
        }
        | NativeCommand::ReportIncrement {
            claim,
            key,
            expected,
            report,
            artifact,
        } => super::reporting::prepare(
            claim, key, expected, report, artifact, request, evidence, context, cut, view, limits,
            meta, extras, scratch,
        ),
        NativeCommand::Create {
            mut claims,
            declarations,
        } => {
            // Retain the complete authored cohort, never reconstruct definitions
            // from acceptance summaries or accept a partial policy-shaped set.
            scratch.charge(array::<validation::Declaration>(declarations.capacity())?)?;
            for (index, declaration) in declarations.iter().enumerate() {
                let binding = declaration.binding();
                if binding.revision != ObjectRevision(1) {
                    return Err(ContractError::StaleRevision.into());
                }
                if binding.ledger != view.ledger() {
                    return Err(ContractError::WrongLedger.into());
                }
                if declarations
                    .iter()
                    .take(index)
                    .any(|old| old.binding().object == binding.object)
                    || view
                        .get(Key::Definition(ValidationId(binding.object.0)))
                        .is_some()
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
                if !claims
                    .iter()
                    .any(|proposal| proposal.definition.binding.object.0 == declaration.claim().0)
                {
                    return Err(ContractError::InvalidPolicy.into());
                }
                scratch.charge(add(
                    declaration.retained_heap_bytes()?,
                    declaration
                        .heap_allocations()?
                        .checked_mul(ALLOCATION)
                        .ok_or(NativeError::Capacity("definition heap"))?,
                )?)?;
            }
            for proposal in &claims {
                proposal.definition.acceptance.check_declarations(
                    declarations
                        .iter()
                        .filter(|row| row.claim().0 == proposal.definition.binding.object.0),
                )?;
            }
            let created = claims.len();
            increment(&mut meta.claims, created, limits.claims, "claims")?;
            increment(
                &mut meta.definitions,
                declarations.len(),
                limits.definitions,
                "definitions",
            )?;
            for proposal in &mut claims {
                proposal.definition.created = cut.position;
            }
            let plan = creation::CreationPlan::prepare(
                context.principal,
                claims,
                view,
                cut,
                creation::Limits {
                    nodes: limits.plan_nodes,
                    edge_visits: limits.plan_edges,
                    bytes: scratch.remaining()?,
                },
            )?;
            scratch.charge(plan.retained_bytes()?)?;
            for replacement in plan.rows() {
                if replacement.status() != ClaimStatus::Superseded {
                    continue;
                }
                let Some(previous) = view.claim(ClaimId(replacement.binding().object.0)) else {
                    return Err(ContractError::InvalidTarget.into());
                };
                if let Some(token) = plan.supersession(previous.binding())? {
                    fence_evaluations(previous, view, extras, scratch, |state, definition| {
                        state.supersede(definition, previous, &token)
                    })?;
                }
            }
            for declaration in declarations {
                let binding = declaration.binding();
                let fact = NativeFact::Definition {
                    binding,
                    claim: declaration.claim(),
                    index: declaration.declaration_index(),
                    intent: declaration.intent_fingerprint(),
                };
                scratch.charge(OwnedDeclaration::container_charge())?;
                let row = OwnedDeclaration::new(declaration)?;
                let heap = row.heap_charge()?;
                extras.push(prepare::Extra {
                    key: Key::Definition(ValidationId(binding.object.0)),
                    row: Row::Definition(row),
                    heap,
                    fact: Some(fact),
                })?;
            }
            let rows = plan.into_rows();
            super::incoming_graph::stage_created(&rows, view, extras, scratch, limits)?;
            Ok(Plan {
                rows,
                registry: None,
                created,
            })
        }
        NativeCommand::Cancel { expected } => {
            let plan = ownership::CancellationPlan::prepare(
                view,
                expected,
                context.principal,
                cut,
                ownership::Limits {
                    nodes: limits.plan_nodes,
                    edge_visits: limits.plan_edges,
                    bytes: scratch.remaining()?,
                },
            )?;
            plan.check(view)?;
            scratch.charge(plan.retained_bytes()?)?;
            let count = plan
                .transitions()
                .iter()
                .filter(|token| token.changes_state())
                .count();
            let mut rows = scratch.reserve::<ClaimState>(count)?;
            for token in plan.transitions() {
                // Even an already-terminal descendant may own a live evaluation.
                fence_evaluations(token.claim(), view, extras, scratch, |state, definition| {
                    state.cancel(definition, token)
                })?;
                if !token.changes_state() {
                    continue;
                }
                scratch.charge(heap(token.claim())?)?;
                let mut row = token.claim().try_copy(token.claim().retained_bytes()?)?;
                row.apply_cancellation(token)?;
                rows.push(row);
            }
            Ok(Plan {
                rows,
                registry: None,
                created: 0,
            })
        }
        NativeCommand::Post { expected } => {
            let id = ClaimId(expected.object.0);
            let old = view.claim(id).ok_or(ContractError::InvalidTarget)?;
            old.binding().check(&expected)?;
            check_retained_definitions(old, view)?;
            let mut rows = scratch.reserve::<ClaimState>(1)?;
            scratch.charge(heap(old)?)?;
            let mut claim = old.try_copy(old.retained_bytes()?)?;
            let mut registry = copy_registry(view, old, limits, scratch)?;
            if !registry.rows().is_empty() {
                return Err(ContractError::InvalidTransition.into());
            }
            claim.post_owned(context.principal, expected)?;
            for summary in claim.acceptance().declarations() {
                let declaration = view.definition(ValidationId(summary.binding().object.0))?;
                if declaration.target() != validation::TargetDeclaration::Admission {
                    continue;
                }
                let evaluation = validation::Evaluation::materialize_admission(
                    context.principal,
                    declaration,
                    &claim,
                    1,
                )?;
                let state = evaluation.into_state();
                if view
                    .get(Key::Evaluation(EvaluationKey::of(id, &state)))
                    .is_some()
                {
                    return Err(ContractError::StaleEvaluation.into());
                }
                let old_heap = registry_heap(&registry)?;
                // register charges both old and replacement buffers. Its inline
                // value lives on the stack, so include that in its local allowance.
                let allowance = add(
                    add(scratch.remaining()?, old_heap)?,
                    size_of::<RegistrationSet>(),
                )?;
                if !registry.register(&claim, &state.bind(declaration)?, allowance)? {
                    return Err(ContractError::StaleEvaluation.into());
                }
                let new_heap = registry_heap(&registry)?;
                scratch.used = scratch
                    .used
                    .checked_sub(old_heap)
                    .ok_or(NativeError::Capacity("registry charge"))?;
                scratch.charge(new_heap)?;
                increment(&mut meta.evaluations, 1, limits.evaluations, "evaluations")?;
                extras.evaluation(id, declaration, None, state, scratch)?;
            }
            rows.push(claim);
            Ok(Plan {
                rows,
                registry: Some((id, registry)),
                created: 0,
            })
        }
        NativeCommand::BeginAdmission {
            claim,
            key,
            expected,
        }
        | NativeCommand::BeginIncrement {
            claim,
            key,
            expected,
        } => {
            let begun = if matches!(key.target, EvaluationTarget::Increment { .. }) {
                super::increment_authority::begin(view, context, claim, key, expected, limits)?
            } else {
                super::admission_authority::begin(view, context, claim, key, expected, limits)?
            };
            extras.evaluation(
                key.claim,
                begun.registered.definition,
                Some(begun.registered.state.binding()),
                begun.next,
                scratch,
            )?;
            Ok(Plan {
                rows: Vec::new(),
                registry: None,
                created: 0,
            })
        }
    }
}
