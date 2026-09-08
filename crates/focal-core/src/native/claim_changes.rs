use super::prepare::*;
use super::*;
use focal_memory::{Change, Entry};
use focal_model::ObjectRevision;

#[path = "history_assembly.rs"]
mod history_assembly;
pub(super) use history_assembly::visit_history;

#[path = "original_plan.rs"]
mod original_plan;
pub(super) use original_plan::{OriginalPlan, SealedChanges};

#[derive(Clone, Copy)]
pub(super) struct History {
    binding: Binding,
    status: ClaimStatus,
}

pub(super) fn event_count(
    rows: &[ClaimState],
    view: &View<'_>,
    operation: NativeOperation,
) -> Result<usize, NativeError> {
    if history_assembly::explicit(operation) {
        return Ok(0);
    }
    let mut count = 0;
    for row in rows {
        match view.claim(ClaimId(row.binding().object.0)) {
            Some(_) if operation == NativeOperation::SealIncrementTargets => count = add(count, 1)?,
            None => {
                count = add(count, 1)?;
                if matches!(row.lineage().cause(), focal_model::Cause::Claim(_)) {
                    count = add(count, 1)?;
                }
            }
            Some(old)
                if old.status() != row.status()
                    || (old.binding() != row.binding()
                        && matches!(
                            operation,
                            NativeOperation::CloseResponse
                                | NativeOperation::AdoptReceipt
                                | NativeOperation::PostResponse
                                | NativeOperation::ReceiveResponse
                        )) =>
            {
                count = add(count, 1)?
            }
            Some(_) => {}
        }
    }
    Ok(count)
}

fn record_fact(
    changes: &mut Vec<Change<Key, Row>>,
    outcome: NativeOutcome,
    ordinal: &mut u32,
    fact: NativeFact,
) -> Result<(), NativeError> {
    let item = NativeEvent {
        invocation: outcome.invocation,
        sequence: outcome.sequence,
        ordinal: *ordinal,
        fact,
    };
    // An event count or capacity inconsistency must never invoke Vec growth.
    if *ordinal >= outcome.events || changes.len() == changes.capacity() {
        return Err(NativeError::Capacity("event preparation"));
    }
    let stored = OwnedEvent::new(StoredEvent::pack(item)?)?;
    let heap = stored.heap_charge()?;
    changes.push(Change::Put(Entry::new(
        Key::Event(outcome.sequence, *ordinal),
        Row::Event(stored),
        heap,
    )));
    *ordinal = ordinal
        .checked_add(1)
        .ok_or(NativeError::Capacity("event ordinal"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Internal staging context, already bounded by the owner.
#[cfg(test)]
pub(super) fn changes(
    plan: transactions::Plan,
    extras: Extras,
    meta: Meta,
    outcome: NativeOutcome,
    view: &View<'_>,
    limits: NativeLimits,
    allowance: usize,
    scratch: &mut Scratch,
) -> Result<Vec<Change<Key, Row>>, NativeError> {
    original_plan::OriginalPlan::check(plan, extras, meta, outcome, view, limits, scratch)?
        .into_changes(allowance, scratch)
}

fn append_rows(
    changes: &mut Vec<Change<Key, Row>>,
    extras: &mut Extras,
) -> Result<(), NativeError> {
    for extra in extras.rows.drain(..) {
        if changes.len() == changes.capacity() {
            return Err(NativeError::Capacity("extra row preparation"));
        }
        changes.push(Change::Put(Entry::new(extra.key, extra.row, extra.heap)));
    }
    Ok(())
}

/// Every intermediate model transition must remain inspectable even when one
/// atomic transaction publishes only the final row. Verify a complete exact
/// revision chain rather than guessing one transition from the final status.
fn check_journal(
    rows: &[ClaimState],
    extras: &Extras,
    view: &View<'_>,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let journal = extras
        .journal
        .as_ref()
        .ok_or(ContractError::InvalidTransition)?;
    let mut visits = limits.plan_edges;
    for fact in journal {
        if let NativeFact::Claim(event) = fact {
            visits = visits
                .checked_sub(rows.len())
                .ok_or(NativeError::Capacity("claim history visits"))?;
            if !rows
                .iter()
                .any(|row| row.binding().object == event.after.object)
            {
                return Err(ContractError::InvalidTarget.into());
            }
        }
    }
    for row in rows {
        let controlled = extras.control_graph.as_ref();
        if let Some(proof) = controlled {
            visits = visits
                .checked_sub(proof.originals().len())
                .ok_or(NativeError::Capacity("control claim history visits"))?;
        }
        let old = controlled
            .and_then(|proof| {
                proof
                    .originals()
                    .iter()
                    .find(|source| source.binding().object == row.binding().object)
            })
            .or_else(|| view.claim(ClaimId(row.binding().object.0)))
            .ok_or(ContractError::InvalidTarget)?;
        let mut binding = old.binding();
        let mut status = old.status();
        let suffix = journal
            .get(controlled.map_or(0, |proof| proof.prefix().len())..)
            .ok_or(ContractError::InvalidManifest)?;
        for fact in suffix {
            visits = visits
                .checked_sub(1)
                .ok_or(NativeError::Capacity("claim history visits"))?;
            let NativeFact::Claim(event) = fact else {
                continue;
            };
            if event.after.object != binding.object {
                continue;
            }
            if event.owned_child.is_some()
                || event.before != Some(binding)
                || event.after != binding.next()?
            {
                return Err(ContractError::StaleRevision.into());
            }
            if status.is_terminal()
                && !matches!(
                    event.kind,
                    NativeEventKind::Monitor(_) | NativeEventKind::OwnerReleased
                )
            {
                return Err(ContractError::InvalidTransition.into());
            }
            let valid = matches!(
                (event.kind, event.status),
                (
                    NativeEventKind::Validating | NativeEventKind::LocallyComplete,
                    ClaimStatus::Validating
                ) | (NativeEventKind::Satisfied, ClaimStatus::Satisfied)
                    | (NativeEventKind::PostFailed, ClaimStatus::PostFailed)
                    | (
                        NativeEventKind::ValidationIncomplete,
                        ClaimStatus::ValidationIncomplete
                    )
                    | (
                        NativeEventKind::ValidationFailed,
                        ClaimStatus::ValidationFailed
                    )
                    | (
                        NativeEventKind::ValidationErrored,
                        ClaimStatus::ValidationErrored
                    )
                    | (
                        NativeEventKind::DependencyFailed,
                        ClaimStatus::DependencyFailed
                    )
                    | (NativeEventKind::Expired, ClaimStatus::Expired)
                    | (NativeEventKind::Deadlocked, ClaimStatus::Deadlocked)
            );
            let released = event.kind == NativeEventKind::OwnerReleased
                && event.status == status
                && old.is_terminal()
                && !old.scopes().released()
                && row.scopes().released();
            let monitor = if let NativeEventKind::Monitor(monitor) = event.kind {
                if event.status != status {
                    return Err(ContractError::InvalidTransition.into());
                }
                let depth = row
                    .scopes()
                    .limits()
                    .scopes
                    .checked_ilog2()
                    .map(|n| n.checked_add(1).ok_or(ContractError::Capacity))
                    .transpose()?
                    .unwrap_or(0);
                visits = visits
                    .checked_sub(
                        usize::try_from(depth)
                            .map_err(|_| ContractError::Capacity)?
                            .checked_add(1)
                            .ok_or(ContractError::Capacity)?,
                    )
                    .ok_or(NativeError::Capacity("claim history visits"))?;
                super::monitor_commands::check_event(row, monitor)?;
                true
            } else {
                false
            };
            if !valid && !released && !monitor {
                return Err(ContractError::InvalidTransition.into());
            }
            binding = event.after;
            status = event.status;
        }
        binding.check(&row.binding())?;
        if status != row.status() {
            return Err(ContractError::InvalidTransition.into());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "control_graph_checks_tests.rs"]
mod control_graph_checks_tests;
