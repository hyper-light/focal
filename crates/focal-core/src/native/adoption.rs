//! Issuer-authorized responsibility transfer from one actual receipt allocation
//! to the next. All authority fences, registry and retained cycle changes share
//! the claim/receipt publication; no evidence or respondent report is invented.
use super::prepare::{Extra, Extras, Scratch, heap, within};
use super::*;
use focal_model::lifecycle::claim::ClaimCut;

fn pinned(actual: Binding, expected: Binding) -> Result<(), NativeError> {
    Binding {
        revision: expected.revision,
        ..actual
    }
    .check(&expected)?;
    if actual.revision < expected.revision {
        return Err(ContractError::StaleRevision.into());
    }
    Ok(())
}

/// Complete membership is supplied by the stored registry, never by the
/// participant. Resolve each member's original target and receipt allocation
/// even when an earlier result or fence means that row will remain unchanged.
fn target(
    view: &View<'_>,
    registered: &admission_authority::Registered<'_>,
    limits: NativeLimits,
    visits: &mut usize,
) -> Result<(), NativeError> {
    let state = registered.state;
    let claim = ClaimId(registered.parent.binding().object.0);
    let Some(receipt) = state.receipt() else {
        return if matches!(state.target(), validation::Target::Admission { .. }) {
            Ok(())
        } else {
            Err(ContractError::StaleReceipt.into())
        };
    };
    let allocated =
        as_receipt(view.get(Key::Receipt(receipt.receipt))).ok_or(ContractError::StaleReceipt)?;
    if allocated.claim != claim
        || allocated.fence != receipt
        || allocated.holder.is_zero()
        || allocated.acquired.0 == 0
        || allocated.acquired > view.prefix()
    {
        return Err(ContractError::StaleReceipt.into());
    }
    match state.target() {
        validation::Target::Admission { .. } => Err(ContractError::StaleReceipt.into()),
        validation::Target::Increment { artifact, .. } => {
            let work =
                increment_authority::work_product(view, claim, ArtifactId(artifact.object.0))?;
            pinned(work.state.binding(), artifact)?;
            if work.state.receipt() != receipt
                || work.state.producer() != allocated.holder
                || state.generation() != u64::from(work.state.cycle())
            {
                return Err(ContractError::InvalidTarget.into());
            }
            Ok(())
        }
        validation::Target::Artifact { artifact, .. } => {
            let (response, work) = work_authority::completion_target(view, registered, limits)?;
            pinned(work.state.binding(), artifact)?;
            if response.respondent() != allocated.holder {
                return Err(ContractError::StaleReceipt.into());
            }
            Ok(())
        }
        validation::Target::MissingSlot {
            response: expected, ..
        }
        | validation::Target::Delivery { response: expected } => {
            let record = response_reads::as_response_record(
                view.get(Key::Response(TestamentId(expected.object.0))),
            )
            .ok_or(ContractError::MissingEvidence)?;
            let response = record.response();
            let identity = response.identity();
            pinned(identity.binding, expected)?;
            let received = record.received().ok_or(ContractError::InvalidCut)?;
            if identity.claim != claim
                || identity.receipt != receipt
                || response.respondent() != allocated.holder
                || state.generation() != u64::from(identity.cycle)
                || received.sequence.0 == 0
                || received.sequence > view.prefix()
            {
                return Err(ContractError::InvalidTarget.into());
            }
            match (response.state(), record.entered()) {
                (ResponseState::Received, None) => {}
                (
                    ResponseState::Validating
                    | ResponseState::Validated
                    | ResponseState::ValidationIncomplete
                    | ResponseState::ValidationFailed
                    | ResponseState::ValidationErrored,
                    Some(entered),
                ) if entered > received && entered.sequence <= view.prefix() => {}
                _ => return Err(ContractError::InvalidTransition.into()),
            }
            let cycle = NativeCycleKey {
                claim,
                receipt: receipt.receipt,
                epoch: receipt.epoch,
                cycle: identity.cycle,
            };
            if !matches!(view.get(Key::Cycle(cycle)), Some(Row::Cycle(row))
                if row.response == Some(TestamentId(identity.binding.object.0))
                    && row.work_count <= limits.work_artifacts_per_cycle)
            {
                return Err(ContractError::InvalidManifest.into());
            }
            if let validation::Target::MissingSlot { slot, .. } = state.target() {
                within(response.manifest().len(), limits.work_artifacts_per_cycle)?;
                *visits = visits
                    .checked_sub(response.manifest().len())
                    .ok_or(NativeError::Capacity("adoption missing-slot visits"))?;
                if response
                    .manifest()
                    .binary_search_by_key(&slot, |entry| entry.slot)
                    .is_ok()
                {
                    return Err(ContractError::InvalidTarget.into());
                }
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)] // One private, bounded owner transaction frame.
pub(super) fn prepare(
    expected: Binding,
    previous: ReceiptFence,
    receipt: ReceiptId,
    holder: ParticipantId,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let id = ClaimId(expected.object.0);
    let old = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    let replacement = ReceiptEntitlement {
        holder,
        fence: ReceiptFence {
            receipt,
            epoch: previous
                .epoch
                .checked_add(1)
                .ok_or(ContractError::Capacity)?,
        },
    };
    let token =
        old.prepare_receipt_adoption(&expected, context.principal, previous, replacement, cut)?;
    let allocated =
        as_receipt(view.get(Key::Receipt(previous.receipt))).ok_or(ContractError::StaleReceipt)?;
    if allocated.claim != id
        || allocated.fence != previous
        || allocated.holder != token.previous().holder
        || allocated.acquired.0 == 0
        || allocated.acquired > view.prefix()
        || view.get(Key::Receipt(receipt)).is_some()
    {
        return Err(ContractError::StaleReceipt.into());
    }
    if old
        .deadline()
        .is_some_and(|deadline| context.logical_time >= deadline.at)
    {
        return Err(ContractError::InvalidTransition.into());
    }
    // Replacement responsibility includes a fresh authored closing response.
    // Never transfer a claim which has exhausted its immutable cycle allowance.
    let response_count =
        u32::try_from(old.response_count()).map_err(|_| ContractError::Capacity)?;
    if response_count >= old.max_responses() {
        return Err(ContractError::InvalidTransition.into());
    }
    response_budget::check_receipt_shape(
        response_budget::delivery_count(old, limits)?,
        work_checks::count(old, limits)?,
        limits,
    )?;
    response_budget::work_limit(limits)?;
    response_budget::check_increment_shape(increments::count(old, limits)?, 0, limits)?;
    let stored = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    response_budget::check_registration_capacity_in(view, old, stored, limits)?;
    within(stored.rows().len(), limits.evaluations_per_claim)?;
    // Bound each immutable declaration check and the constant indexed target
    // lookups before traversing any registered member. No nested registry scan.
    let per_member = old
        .acceptance()
        .declarations()
        .len()
        .checked_add(16)
        .ok_or(NativeError::Capacity("adoption visits"))?;
    let visits = stored
        .rows()
        .len()
        .checked_mul(per_member)
        .ok_or(NativeError::Capacity("adoption visits"))?;
    within(visits, limits.plan_edges)?;
    let mut visits = limits
        .plan_edges
        .checked_sub(visits)
        .ok_or(NativeError::Capacity("adoption visits"))?;
    let mut rows = scratch.reserve::<ClaimState>(1)?;
    scratch.charge(heap(old)?)?;
    let mut claim = old.try_copy(old.retained_bytes()?)?;
    claim.apply_receipt_adoption(&token)?;
    let mut registry = transactions::copy_registry(view, old, limits, scratch)?;
    registry.adopt_receipt(&token)?;
    retired_cycles::stage(view, old, &claim, limits, extras, scratch)?;
    transactions::increment(&mut meta.receipts, 1, limits.receipts, "receipts")?;
    extras.push(Extra {
        key: Key::Receipt(receipt),
        row: Row::Receipt(NativeReceipt {
            claim: id,
            fence: replacement.fence,
            holder,
            acquired: cut.position,
        }),
        heap: 0,
        fact: Some(NativeFact::ReceiptAdopted {
            claim: claim.binding(),
            previous: token.previous(),
            replacement,
            cause: cut.cause,
        }),
    })?;
    for (registration_index, member) in stored.rows().iter().enumerate() {
        let key = transactions::key_for_registered(id, *member);
        let definition = view.definition(key.validation)?;
        let state = view.evaluation(key)?;
        member.check_state(*state, definition)?;
        let next = state.adopt_receipt(definition, &token)?;
        target(
            view,
            &admission_authority::Registered {
                parent: old,
                definition,
                registry: stored,
                registration_index,
                state,
            },
            limits,
            &mut visits,
        )?;
        if next != *state {
            extras.evaluation(id, definition, Some(state.binding()), next, scratch)?;
        }
    }
    let mut replacements = transactions::RegistryOverrides::new();
    replacements.insert(&claim, registry, limits.plan_nodes, scratch)?;
    rows.push(claim);
    Ok(transactions::Plan {
        rows,
        registry: replacements,
        created: 0,
    })
}
