//! Compose checked model construction beneath the complete row reservation.
//! Immutable buffers move directly into ClaimState; no second definition copy
//! or temporary array of response/registration values is needed.
use super::*;
use crate::native::{NativeError, NativeLimits, OwnedClaim, Row};
use focal_memory::MemoryError;

const ALLOCATION: usize = 4 * size_of::<usize>();
#[derive(Clone, Copy)]
pub(in crate::native::record_codec) struct Limits {
    pub(in crate::native::record_codec) native: NativeLimits,
    pub(in crate::native::record_codec) acceptance: aggregation::Limits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::native::record_codec) struct ClaimQuote {
    pub(in crate::native::record_codec) heap_bytes: usize,
}
fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b).ok_or(NativeError::Capacity("claim recovery charge"))
}
fn nested(heap: usize, allocations: usize) -> Result<usize, NativeError> {
    add(heap, allocations.checked_mul(ALLOCATION).ok_or(NativeError::Capacity("claim recovery allocations"))?)
}
fn vector<T>(count: usize) -> Result<usize, NativeError> {
    nested(count.checked_mul(size_of::<T>()).ok_or(NativeError::Capacity("claim recovery vector"))?, usize::from(count != 0))
}
fn fits(actual: usize, maximum: usize) -> Result<(), NativeError> {
    if actual > maximum { Err(NativeError::Capacity("claim recovery allowance")) } else { Ok(()) }
}
fn collect<T>(mut source: impl Iterator<Item = Result<T, ContractError>>, count: usize) -> Result<Vec<T>, NativeError> {
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|_| MemoryError::AllocationFailed)?;
    if values.capacity() != count { return Err(NativeError::Capacity("claim recovery allocation")); }
    for _ in 0..count { values.push(source.next().ok_or(ContractError::InvalidManifest)??); }
    match source.next() {
        None => Ok(values),
        Some(Err(error)) => Err(error.into()),
        Some(Ok(_)) => Err(ContractError::InvalidManifest.into()),
    }
}
impl Input<'_> {
    pub(in crate::native::record_codec) fn quote<D: Objects + ?Sized>(
        &self, objects: &D, limits: Limits, source: &Meter, model: &Meter,
    ) -> Result<ClaimQuote, NativeError> {
        process(*self, objects, limits, source, model, None).map(|(quote, _)| quote)
    }
    pub(in crate::native::record_codec) fn build<D: Objects + ?Sized>(
        self, objects: &D, limits: Limits, source: &Meter, model: &Meter, allowance: usize,
    ) -> Result<(Row, usize), NativeError> {
        let (_, row) = process(self, objects, limits, source, model, Some(allowance))?;
        let value = row.ok_or(ContractError::InvalidManifest)?;
        let actual = value.heap_charge()?;
        Ok((Row::Claim(value), actual))
    }
}

fn process<D: Objects + ?Sized>(
    input: Input<'_>, objects: &D, limits: Limits, source: &Meter,
    model: &Meter, build: Option<usize>,
) -> Result<(ClaimQuote, Option<OwnedClaim>), NativeError> {
    let fields = input.fields;
    model.charge(64).map_err(model_error)?;
    if fields.responses > usize::try_from(fields.max_responses).map_err(|_| ContractError::Capacity)?
        || fields.max_responses == 0 || input.registrations.max_rows > limits.native.evaluations_per_claim
        || input.registrations.rows > input.registrations.max_rows
        || input.registrations.max_rows == 0 {
        return Err(ContractError::Capacity.into());
    }
    input.lineage.check(&fields.binding)?;
    Binding { revision: fields.binding.revision, ..input.acceptance }.check(&fields.binding)?;
    if input.acceptance_issuer != fields.issuer { return Err(ContractError::InvalidPolicy.into()); }
    model.budget(|visits| graph::Declaration::check_sorted_values(
        Values::new(input.obligations, source, obligation), input.obligations.count,
        limits.native.plan_edges, visits,
    ))?;
    model.budget(|visits| succession::Lineage::check_sorted_values(
        input.lineage, &input.cause, Values::new(input.corrections, source, correction),
        input.corrections.count, limits.native.plan_edges, visits,
    ))?;
    let acceptance_source = input.acceptance_source(objects, source);
    let acceptance = model.model(|visits| {
        let plan = aggregation::AcceptancePolicy::prepare_source(input.acceptance,
            input.acceptance_issuer, &acceptance_source, limits.acceptance, visits)?;
        let used = plan.inspection_visits();
        Ok((plan, used))
    })?;
    let scope_source = input.scopes.source(source);
    let scopes = model.model(|visits| {
        let plan = focal_model::lifecycle::scope::Registry::prepare_hydration_v1(
            fields.binding, fields.created, input.scopes.fields.limits, &scope_source, visits)?;
        let used = plan.inspection_visits();
        Ok((plan, used))
    })?;
    let heap = add(OwnedClaim::container_charge(), add(
        add(vector::<graph::Obligation>(input.obligations.count)?, vector::<succession::Correction>(input.corrections.count)?)?,
        add(nested(acceptance.construction_heap_bytes(), acceptance.construction_heap_allocations())?,
            add(nested(scopes.construction_heap_bytes(), scopes.construction_heap_allocations())?,
                add(claim::ClaimState::hydration_response_heap_charge_v1(fields.responses)?,
                    aggregation::RegistrationSet::hydration_heap_charge_v1(input.registrations.rows)?)?)?)?)?;
    let quote = ClaimQuote { heap_bytes: heap };
    let Some(allowance) = build else { return Ok((quote, None)); };
    fits(heap, allowance)?;
    let acceptance_charge = acceptance.construction_charge();
    let acceptance_visits = acceptance.build_visits();
    model.charge(acceptance_visits).map_err(model_error)?;
    let acceptance = acceptance.build(acceptance_charge, acceptance_visits)?;
    let obligations = collect(Values::new(input.obligations, source, obligation), input.obligations.count)?;
    let graph = model.budget(|visits| graph::Declaration::from_owned_sorted(obligations, limits.native.plan_edges, visits))?;
    let corrections = collect(Values::new(input.corrections, source, correction), input.corrections.count)?;
    let lineage = model.budget(|visits| succession::Lineage::from_owned_sorted(
        input.lineage, input.cause, corrections, limits.native.plan_edges, visits))?;
    let definition = claim::ClaimDefinition {
        binding: fields.binding, issuer: fields.issuer, subject: fields.subject,
        deadline: fields.deadline, max_responses: fields.max_responses, created: fields.created,
        graph, lineage, acceptance, scope_limits: input.scopes.fields.limits,
    };
    let responses = input.response_source(objects, source);
    let plan = model.model(|visits| {
        let plan = claim::ClaimState::prepare_hydration_v1(definition, fields, &responses, scopes, visits)?;
        let used = plan.inspection_visits();
        Ok((plan, used))
    })?;
    let claim_charge = plan.construction_charge()?;
    let claim_visits = plan.build_visits()?;
    model.charge(claim_visits).map_err(model_error)?;
    let state = plan.build(claim_charge, claim_visits)?;
    let members = input.registration_source(objects, source);
    let registrations = model.model(|visits| {
        let plan = aggregation::RegistrationSet::prepare_hydration_v1(&state,
            input.registrations, &members, limits.native.evaluations_per_claim, visits)?;
        let used = plan.inspection_visits();
        Ok((plan, used))
    })?;
    let registration_charge = registrations.construction_charge()?;
    let registration_visits = registrations.build_visits()?;
    model.charge(registration_visits).map_err(model_error)?;
    let registrations = registrations.build(registration_charge, registration_visits)?;
    let owned = OwnedClaim::new(state, registrations)?;
    fits(owned.heap_charge()?, heap)?;
    Ok((quote, Some(owned)))
}
