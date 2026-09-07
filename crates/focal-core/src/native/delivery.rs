//! Atomic pure Receipt materialization and pass from explicit claimant receipt.
//! All declarations come from the claim's complete immutable acceptance policy.
use super::delivery_owned::{NativeDeliveryResult, OwnedDeliveryResult};
use super::prepare::{Extra, Extras, Scratch, add};
use super::*;

#[allow(clippy::too_many_arguments)] // Exact borrowed owner transaction, no caller-authored authority.
pub(super) fn prepare(
    context: NativeContext,
    view: &View<'_>,
    claim: &ClaimState,
    response: &Response,
    limits: NativeLimits,
    meta: &mut Meta,
    registry: &mut RegistrationSet,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<(), NativeError> {
    context.principal.require_actor(claim.issuer())?;
    registry.check(claim)?;
    if claim.is_terminal() || claim.local_complete() {
        return Err(ContractError::InvalidTransition.into());
    }
    let declarations = claim.acceptance().declarations();
    if declarations.len() > limits.plan_edges {
        return Err(NativeError::Capacity("delivery declaration visits"));
    }
    // Missing or substituted retained definitions must not silently shrink the
    // complete Receipt cohort. Validate the whole set before staging any member.
    for summary in declarations {
        let declaration = view.definition(ValidationId(summary.binding().object.0))?;
        claim.acceptance().check_declaration(declaration)?;
    }
    let id = ClaimId(claim.binding().object.0);
    let sequence = SessionSeq(
        view.prefix()
            .0
            .checked_add(1)
            .ok_or(NativeError::Capacity("delivery publication sequence"))?,
    );
    // ReceiveResponse moves its observation fact ahead of all these extras.
    // Keep each internal result's original event position for later projection.
    let mut ordinal = u32::try_from(add(extras.events(), 1)?)
        .map_err(|_| NativeError::Capacity("delivery publication ordinal"))?;
    for summary in declarations {
        let declaration = view.definition(ValidationId(summary.binding().object.0))?;
        if declaration.target() != validation::TargetDeclaration::Delivery {
            continue;
        }
        let ready = validation::Evaluation::materialize_delivery(
            context.principal,
            declaration,
            claim,
            response,
        )?;
        let key = EvaluationKey::of(id, &ready.into_state());
        if view.get(Key::Evaluation(key)).is_some() {
            return Err(ContractError::StaleEvaluation.into());
        }
        let owner = ready.delivery_owner(claim, response, context.logical_time)?;
        let old_heap = transactions::registry_heap(registry)?;
        // The response owner precharges/copies the complete expanded cohort
        // once. Registration may fill that buffer but cannot grow it here.
        let allowance = add(old_heap, size_of::<RegistrationSet>())?;
        if !registry.register(claim, &ready, allowance)? {
            return Err(ContractError::StaleEvaluation.into());
        }
        if transactions::registry_heap(registry)? != old_heap {
            return Err(NativeError::Capacity("delivery registry growth"));
        }
        // Receipt remains observable after a declared deadline. Retain Ready
        // plus its immutable deadline, without inventing Pass or a terminal
        // failure. Deadline/fence publication belongs to a separate owner action.
        let (next, result) = if context.logical_time >= ready.deadline().at {
            (ready.into_state(), None)
        } else {
            let transition = ready.receive_delivery(context.principal, &ready.binding(), &owner)?;
            let result = transition.result.ok_or(ContractError::InvalidTransition)?;
            (transition.next.into_state(), Some(result))
        };
        transactions::increment(&mut meta.evaluations, 1, limits.evaluations, "evaluations")?;
        scratch.charge(OwnedEvaluation::container_charge())?;
        let row = OwnedEvaluation::new(next)?;
        let heap = row.heap_charge()?;
        extras.push(Extra {
            key: Key::Evaluation(key),
            row: Row::Evaluation(row),
            heap,
            fact: Some(NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Materialized,
                key,
                before: None,
                after: ready.binding(),
                state: validation::State::Ready,
                phase: validation::Phase::Delivery,
                attempt: None,
                fence: None,
            }),
        })?;
        ordinal = ordinal
            .checked_add(1)
            .ok_or(NativeError::Capacity("delivery publication ordinal"))?;
        if let Some(result) = result {
            let result = NativeDeliveryResult::new(result, sequence, ordinal)?;
            let result_key = NativeResultKey::of(result.result());
            if view.get(Key::DeliveryResult(result_key)).is_some() {
                return Err(ContractError::StaleEvaluation.into());
            }
            transactions::increment(&mut meta.results, 1, limits.results, "results")?;
            scratch.charge(OwnedDeliveryResult::container_charge())?;
            let row = OwnedDeliveryResult::new(result)?;
            let heap = row.heap_charge()?;
            extras.push(Extra {
                key: Key::DeliveryResult(result_key),
                row: Row::DeliveryResult(row),
                heap,
                fact: Some(NativeFact::Delivery { key: result_key }),
            })?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or(NativeError::Capacity("delivery publication ordinal"))?;
        }
    }
    Ok(())
}
