//! Retained open cycles abandoned by an actual receipt replacement. Closed
//! cycles remain reachable through immutable response lineage; this index keeps
//! their unclosed siblings addressable without scanning the ledger.
use super::prepare::{Extra, Extras, Scratch, add, within};
use super::*;

pub(super) fn head(
    view: &View<'_>,
    claim: ClaimId,
    limits: NativeLimits,
) -> Result<RetiredCycleHead, ContractError> {
    let row = match view.get(Key::RetiredCycleHead(claim)) {
        None => RetiredCycleHead {
            head: None,
            count: 0,
            work_count: 0,
        },
        Some(Row::RetiredCycleHead(row)) => *row,
        Some(_) => return Err(ContractError::InvalidManifest),
    };
    if row.head.is_some() != (row.count != 0)
        || (row.count == 0 && row.work_count != 0)
        || row.count > limits.plan_edges
        || row.work_count > limits.plan_edges
    {
        return Err(ContractError::InvalidManifest);
    }
    Ok(row)
}

/// Exactly three indexed lookups. Strictly descending receipt epochs prove
/// uniqueness and exclude both the current open cycle and repeated links.
pub(super) fn link(
    view: &View<'_>,
    claim: &ClaimState,
    key: NativeCycleKey,
    previous_epoch: u64,
    previous_cycle: u32,
    limits: NativeLimits,
) -> Result<(RetiredCycle, NativeCycle), ContractError> {
    if key.claim.0 != claim.binding().object.0
        || key.receipt.is_zero()
        || key.epoch == 0
        || key.epoch >= previous_epoch
        || key.cycle == 0
        || key.cycle > previous_cycle
        || key.cycle > claim.max_responses()
    {
        return Err(ContractError::InvalidManifest);
    }
    let Some(Row::RetiredCycle(link)) = view.get(Key::RetiredCycle(key)) else {
        return Err(ContractError::MissingEvidence);
    };
    let Some(Row::Cycle(cycle)) = view.get(Key::Cycle(key)) else {
        return Err(ContractError::MissingEvidence);
    };
    let receipt =
        as_receipt(view.get(Key::Receipt(key.receipt))).ok_or(ContractError::StaleReceipt)?;
    if link.holder.is_zero()
        || receipt.claim != key.claim
        || receipt.holder != link.holder
        || receipt.fence
            != (ReceiptFence {
                receipt: key.receipt,
                epoch: key.epoch,
            })
        || receipt.acquired.0 == 0
        || receipt.acquired > view.prefix()
        || cycle.response.is_some()
        || (cycle.work_count == 0 && cycle.diagnostic_count == 0)
        || cycle.work_head.is_some() != (cycle.work_count != 0)
        || cycle.diagnostic_head.is_some() != (cycle.diagnostic_count != 0)
        || cycle.work_count > limits.work_artifacts_per_cycle
        || cycle.diagnostic_count > limits.diagnostics_per_cycle
    {
        return Err(ContractError::InvalidManifest);
    }
    Ok((*link, *cycle))
}

fn take(visits: &mut usize, count: usize) -> Result<(), NativeError> {
    *visits = visits
        .checked_sub(count)
        .ok_or(NativeError::Capacity("retired cycle visits"))?;
    Ok(())
}

/// Validates complete prior membership and appends only a real nonempty open
/// cycle. The caller holds the ordinary construction allowance for Extras; both
/// new index rows are inline and allocate no independent payload.
pub(super) fn stage(
    view: &View<'_>,
    source: &ClaimState,
    next: &ClaimState,
    limits: NativeLimits,
    extras: &mut Extras,
    _scratch: &mut Scratch,
) -> Result<(), NativeError> {
    source.binding().next()?.check(&next.binding())?;
    let previous = source.receipt().ok_or(ContractError::StaleReceipt)?;
    let replacement = next.receipt().ok_or(ContractError::StaleReceipt)?;
    if previous.fence.receipt == replacement.fence.receipt
        || previous.fence.epoch.checked_add(1) != Some(replacement.fence.epoch)
        || replacement.holder.is_zero()
        || source.status() != next.status()
        || source.response_count() != next.response_count()
    {
        return Err(ContractError::StaleReceipt.into());
    }
    let id = ClaimId(source.binding().object.0);
    let mut visits = limits.plan_edges;
    take(&mut visits, 1)?;
    let old = head(view, id, limits)?;
    let count = u32::try_from(source.response_count()).map_err(|_| ContractError::Capacity)?;
    if count > source.max_responses() {
        return Err(ContractError::InvalidManifest.into());
    }
    let maximum_cycle = if count == source.max_responses() {
        count
    } else {
        count.checked_add(1).ok_or(ContractError::Capacity)?
    };
    let mut cursor = old.head;
    let mut epoch = previous.fence.epoch;
    let mut cycle_number = maximum_cycle;
    let mut works = 0usize;
    for _ in 0..old.count {
        take(&mut visits, 3)?;
        let key = cursor.ok_or(ContractError::InvalidManifest)?;
        let (row, cycle) = link(view, source, key, epoch, cycle_number, limits)?;
        works = add(works, cycle.work_count)?;
        cursor = row.next;
        epoch = key.epoch;
        cycle_number = key.cycle;
    }
    if cursor.is_some() || works != old.work_count {
        return Err(ContractError::InvalidManifest.into());
    }
    if count == source.max_responses() {
        return Ok(());
    }
    let parent = evidence::Parent::from_claim(source)?;
    let key = NativeCycleKey::of(&parent);
    take(&mut visits, 2)?;
    if view.get(Key::RetiredCycle(key)).is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    let cycle = match view.get(Key::Cycle(key)) {
        None => return Ok(()),
        Some(Row::Cycle(row)) => *row,
        Some(_) => return Err(ContractError::InvalidManifest.into()),
    };
    if cycle.response.is_some()
        || cycle.work_head.is_some() != (cycle.work_count != 0)
        || cycle.diagnostic_head.is_some() != (cycle.diagnostic_count != 0)
    {
        return Err(ContractError::InvalidManifest.into());
    }
    within(cycle.work_count, limits.work_artifacts_per_cycle)?;
    within(cycle.diagnostic_count, limits.diagnostics_per_cycle)?;
    let mut work = cycle.work_head;
    for _ in 0..cycle.work_count {
        take(&mut visits, 2)?;
        let id = work.ok_or(ContractError::InvalidManifest)?;
        let row = as_work(view.get(Key::Work(id))).ok_or(ContractError::MissingEvidence)?;
        let state = &row.state;
        if state.reference().id != id
            || state.binding().ledger != parent.ledger
            || state.claim() != key.claim
            || state.receipt() != parent.receipt
            || state.cycle() != key.cycle
            || state.producer() != parent.holder
            || !source.acceptance().has_slot(state.slot())
            || state.attachment().is_some()
            || !matches!(
                state.state(),
                WorkArtifactState::Generated
                    | WorkArtifactState::Received
                    | WorkArtifactState::GenerationFailed
                    | WorkArtifactState::ReceiptFailed
            )
            || !matches!(view.get(Key::WorkSlot(key, state.slot())), Some(Row::WorkSlot(found)) if *found == id)
        {
            return Err(ContractError::InvalidManifest.into());
        }
        work = row.next;
    }
    let mut diagnostic = cycle.diagnostic_head;
    for _ in 0..cycle.diagnostic_count {
        take(&mut visits, 1)?;
        let id = diagnostic.ok_or(ContractError::InvalidManifest)?;
        let row =
            as_diagnostic(view.get(Key::Diagnostic(id))).ok_or(ContractError::MissingEvidence)?;
        if row.diagnostic.artifact().id != id {
            return Err(ContractError::InvalidManifest.into());
        }
        row.diagnostic.check_parent(&parent)?;
        diagnostic = row.next;
    }
    if work.is_some() || diagnostic.is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    if cycle.work_count == 0 && cycle.diagnostic_count == 0 {
        return Ok(());
    }
    let count = add(old.count, 1)?;
    let work_count = add(old.work_count, cycle.work_count)?;
    within(count, limits.plan_edges)?;
    within(work_count, limits.plan_edges)?;
    let registry = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidPolicy)?;
    let required = super::response_budget::required_with_retired(next, limits, work_count)?;
    within(required, registry.max_rows())?;
    within(required, limits.evaluations_per_claim)?;
    extras.push(Extra {
        key: Key::RetiredCycle(key),
        row: Row::RetiredCycle(RetiredCycle {
            holder: parent.holder,
            next: old.head,
        }),
        heap: 0,
        fact: None,
    })?;
    extras.push(Extra {
        key: Key::RetiredCycleHead(id),
        row: Row::RetiredCycleHead(RetiredCycleHead {
            head: Some(key),
            count,
            work_count,
        }),
        heap: 0,
        fact: None,
    })
}
