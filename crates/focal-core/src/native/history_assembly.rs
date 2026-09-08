//! One source for original native event order. OriginalPlan owns the immutable
//! payloads through this emission pass, so publication needs no fact-capture
//! buffer. This visitor allocates no buffers: its caller must precharge the
//! actual History workspace capacity and any sink. No seal history is added here.
use super::*;

pub(super) fn explicit(operation: NativeOperation) -> bool {
    matches!(
        operation,
        NativeOperation::EnterWholeWork
            | NativeOperation::BeginWork
            | NativeOperation::ReportWork
            | NativeOperation::ClaimDeadline
            | NativeOperation::MonitorDeadline
            | NativeOperation::RegisterMonitor
            | NativeOperation::RebindMonitor
            | NativeOperation::CancelMonitor
            | NativeOperation::ReleaseScope
    )
}

fn extras_first(operation: NativeOperation) -> bool {
    matches!(
        operation,
        NativeOperation::ReportAdmission
            | NativeOperation::CloseResponse
            | NativeOperation::PostResponse
            | NativeOperation::ReceiveResponse
    )
}

/// Visit every original fact at its original ordinal. Rows are final, strictly
/// ordered claim states; extras remain borrowed and untouched. Explicit object
/// journals still require their existing semantic check before publication.
/// A sink may receive a prefix before a later error; discard that unpublished
/// output on failure. Workspace pushes cannot grow its buffer; the caller must
/// provide a precharged sink that refuses exhaustion instead of growing.
pub(in crate::native) fn visit_history(
    rows: &[ClaimState],
    extras: &Extras,
    view: &View<'_>,
    operation: NativeOperation,
    workspace: &mut Vec<History>,
    mut emit: impl FnMut(NativeFact) -> Result<(), NativeError>,
) -> Result<usize, NativeError> {
    if workspace.capacity() < rows.len() {
        return Err(NativeError::Capacity("claim history workspace"));
    }
    let mut previous = None;
    for row in rows {
        if previous.is_some_and(|id| id >= row.binding().object) {
            return Err(ContractError::InvalidTarget.into());
        }
        previous = Some(row.binding().object);
    }
    let controlled = matches!(
        operation,
        NativeOperation::Create | NativeOperation::Cancel | NativeOperation::Post
    ) && extras.control_graph.is_some();
    let admission =
        operation == NativeOperation::ReportAdmission && extras.admission_graph.is_some();
    if (extras.control_graph.is_some() && !controlled)
        || (extras.admission_graph.is_some() && !admission)
        || (controlled && admission)
        || extras.journal.is_some() != (explicit(operation) || controlled || admission)
    {
        return Err(ContractError::InvalidTransition.into());
    }
    let mut count = 0usize;
    let mut record = |fact| -> Result<(), NativeError> {
        let next = add(count, 1)?;
        emit(fact)?;
        count = next;
        Ok(())
    };
    if let Some(journal) = &extras.journal {
        if extras.rows.iter().any(|row| row.fact.is_some()) {
            return Err(ContractError::InvalidManifest.into());
        }
        for fact in journal {
            record(*fact)?;
        }
        return Ok(count);
    }
    workspace.clear();
    if extras_first(operation) {
        for fact in extras.rows.iter().filter_map(|extra| extra.fact) {
            record(fact)?;
        }
    }
    // All creations precede child registrations, even when a child sorts before
    // its parent. Each registration advances the actual parent's prior binding.
    for row in rows {
        let old = view.claim(ClaimId(row.binding().object.0));
        let state = old.map_or(
            History {
                binding: Binding {
                    revision: ObjectRevision(1),
                    ..row.binding()
                },
                status: ClaimStatus::Generated,
            },
            |old| History {
                binding: old.binding(),
                status: old.status(),
            },
        );
        if old.is_none() {
            record(claim_fact(NativeEventKind::Created, None, None, state))?;
        }
        // The complete final-row count was checked before any push.
        workspace.push(state);
    }
    for row in rows {
        if view.claim(ClaimId(row.binding().object.0)).is_some() {
            continue;
        }
        let focal_model::Cause::Claim(parent) = row.lineage().cause() else {
            continue;
        };
        let index = rows
            .binary_search_by_key(&parent.0, |row| row.binding().object.0)
            .map_err(|_| ContractError::InvalidTarget)?;
        let owner = rows.get(index).ok_or(ContractError::InvalidTarget)?;
        let child = owner
            .scopes()
            .children()
            .binary_search_by_key(&ClaimId(row.binding().object.0), |child| child.id())
            .ok()
            .and_then(|index| owner.scopes().children().get(index))
            .ok_or(ContractError::InvalidTarget)?;
        let state = workspace
            .get_mut(index)
            .ok_or(ContractError::InvalidTarget)?;
        let before = state.binding;
        state.binding = before.next()?;
        record(claim_fact(
            NativeEventKind::ChildRegistered,
            Some(child.binding()),
            Some(before),
            *state,
        ))?;
    }
    for (row, state) in rows.iter().zip(workspace.iter_mut()) {
        if operation == NativeOperation::SealIncrementTargets {
            record(NativeFact::Registrations {
                claim: row.binding(),
            })?;
        }
        if state.binding != row.binding() {
            let kind = if operation == NativeOperation::AdoptReceipt {
                if state.status != row.status() {
                    return Err(ContractError::InvalidTransition.into());
                }
                NativeEventKind::ReceiptAdopted
            } else if state.status == row.status()
                && matches!(
                    operation,
                    NativeOperation::CloseResponse
                        | NativeOperation::PostResponse
                        | NativeOperation::ReceiveResponse
                )
            {
                NativeEventKind::ResponseObserved
            } else {
                match row.status() {
                    ClaimStatus::Cancelled => NativeEventKind::Cancelled,
                    ClaimStatus::Superseded => NativeEventKind::Superseded,
                    ClaimStatus::Posted => NativeEventKind::Posted,
                    ClaimStatus::PostFailed => NativeEventKind::PostFailed,
                    ClaimStatus::Received => NativeEventKind::Received,
                    ClaimStatus::Satisfied => NativeEventKind::Satisfied,
                    ClaimStatus::TestamentGenerated => NativeEventKind::TestamentGenerated,
                    ClaimStatus::TestamentAcknowledged => NativeEventKind::TestamentAcknowledged,
                    _ => return Err(ContractError::InvalidTransition.into()),
                }
            };
            let before = state.binding;
            state.binding = before.next()?;
            state.status = row.status();
            record(claim_fact(kind, None, Some(before), *state))?;
        }
        state.binding.check(&row.binding())?;
        if state.status != row.status() {
            return Err(ContractError::InvalidTransition.into());
        }
    }
    if !extras_first(operation) {
        for fact in extras.rows.iter().filter_map(|extra| extra.fact) {
            record(fact)?;
        }
    }
    Ok(count)
}

fn claim_fact(
    kind: NativeEventKind,
    owned_child: Option<Binding>,
    before: Option<Binding>,
    after: History,
) -> NativeFact {
    NativeFact::Claim(NativeClaimEvent {
        kind,
        graph: None,
        owned_child,
        before,
        after: after.binding,
        status: after.status,
    })
}

#[cfg(test)]
#[path = "history_assembly_tests.rs"]
mod tests;
