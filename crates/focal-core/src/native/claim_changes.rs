use super::prepare::*;
use super::*;
use focal_memory::{Change, Entry};
use focal_model::ObjectRevision;

#[derive(Clone, Copy)]
pub(super) struct History {
    binding: Binding,
    status: ClaimStatus,
}

pub(super) fn event_count(rows: &[ClaimState], view: &View<'_>) -> Result<usize, NativeError> {
    let mut count = 0;
    for row in rows {
        match view.claim(ClaimId(row.binding().object.0)) {
            None => {
                count = add(count, 1)?;
                if matches!(row.lineage().cause(), focal_model::Cause::Claim(_)) {
                    count = add(count, 1)?;
                }
            }
            Some(old) if old.status() != row.status() => count = add(count, 1)?,
            Some(_) => {}
        }
    }
    Ok(count)
}

fn event(
    changes: &mut Vec<Change<Key, Row>>,
    outcome: NativeOutcome,
    ordinal: &mut u32,
    kind: NativeEventKind,
    owned_child: Option<Binding>,
    before: Option<Binding>,
    after: History,
) -> Result<(), NativeError> {
    let item = NativeEvent {
        request: outcome.request,
        sequence: outcome.sequence,
        ordinal: *ordinal,
        fact: NativeFact::Claim(NativeClaimEvent {
            kind,
            owned_child,
            before,
            after: after.binding,
            status: after.status,
        }),
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
pub(super) fn changes(
    plan: transactions::Plan,
    mut extras: Extras,
    meta: Meta,
    outcome: NativeOutcome,
    view: &View<'_>,
    limits: NativeLimits,
    allowance: usize,
    scratch: &mut Scratch,
) -> Result<Vec<Change<Key, Row>>, NativeError> {
    let transactions::Plan {
        mut rows,
        mut registry,
        ..
    } = plan;
    let event_charge = event_containers(
        usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
    )?;
    rows.sort_unstable_by_key(|row| row.binding().object);
    let count = add(
        add(
            rows.len(),
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
        )?,
        add(2, extras.rows.len())?,
    )?;
    if count > limits.range.max_batch_entries {
        return Err(NativeError::Capacity(
            "changes including history and outcome",
        ));
    }
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(
        add(
            add(
                array::<Change<Key, Row>>(changes.capacity())?,
                array::<History>(rows.len())?,
            )?,
            add(containers(rows.len())?, event_charge)?,
        )?,
        allowance,
    )?;
    let mut history = Vec::new();
    history
        .try_reserve_exact(rows.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(
        add(
            add(
                array::<Change<Key, Row>>(changes.capacity())?,
                array::<History>(history.capacity())?,
            )?,
            add(containers(rows.len())?, event_charge)?,
        )?,
        allowance,
    )?;
    let mut ordinal = 0;
    // A report publishes its artifact, evaluation and accepted result before the
    // derived claim failure. Existing command history retains its original order.
    if outcome.operation == NativeOperation::ReportAdmission {
        append_extras(&mut changes, &mut extras, outcome, &mut ordinal)?;
    }
    // Match model preparation phases: creation, child registration in child-ID
    // order, then terminal changes. Registrations never precede child creation.
    for row in &rows {
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
            event(
                &mut changes,
                outcome,
                &mut ordinal,
                NativeEventKind::Created,
                None,
                None,
                state,
            )?;
        }
        history.push(state);
    }
    for row in &rows {
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
        let state = history.get_mut(index).ok_or(ContractError::InvalidTarget)?;
        let before = state.binding;
        state.binding = before.next()?;
        event(
            &mut changes,
            outcome,
            &mut ordinal,
            NativeEventKind::ChildRegistered,
            Some(child.binding()),
            Some(before),
            *state,
        )?;
    }
    for (row, mut state) in rows.into_iter().zip(history) {
        if state.status != row.status() {
            let kind = match row.status() {
                ClaimStatus::Cancelled => NativeEventKind::Cancelled,
                ClaimStatus::Superseded => NativeEventKind::Superseded,
                ClaimStatus::Posted => NativeEventKind::Posted,
                ClaimStatus::PostFailed => NativeEventKind::PostFailed,
                _ => return Err(ContractError::InvalidTransition.into()),
            };
            let before = state.binding;
            state.binding = before.next()?;
            state.status = row.status();
            event(
                &mut changes,
                outcome,
                &mut ordinal,
                kind,
                None,
                Some(before),
                state,
            )?;
        }
        state.binding.check(&row.binding())?;
        let id = ClaimId(row.binding().object.0);
        let registrations = if registry.as_ref().is_some_and(|(owner, _)| *owner == id) {
            registry.take().ok_or(ContractError::InvalidTarget)?.1
        } else {
            transactions::copy_registry(view, &row, limits, scratch)?
        };
        registrations.check(&row)?;
        let row = OwnedClaim::new(row, registrations)?;
        let heap_bytes = row.heap_charge()?;
        changes.push(Change::Put(Entry::new(
            Key::Claim(id),
            Row::Claim(row),
            heap_bytes,
        )));
    }
    if registry.is_some() {
        return Err(ContractError::InvalidTarget.into());
    }
    append_extras(&mut changes, &mut extras, outcome, &mut ordinal)?;
    if ordinal != outcome.events {
        return Err(ContractError::InvalidManifest.into());
    }
    changes.push(Change::Put(Entry::new(Key::Meta, Row::Meta(meta), 0)));
    changes.push(Change::Put(Entry::new(
        Key::Outcome(outcome.request),
        Row::Outcome(outcome),
        0,
    )));
    Ok(changes)
}

fn append_extras(
    changes: &mut Vec<Change<Key, Row>>, extras: &mut Extras,
    outcome: NativeOutcome, ordinal: &mut u32,
) -> Result<(), NativeError> {
    for extra in extras.rows.drain(..) {
        let needed = if extra.fact.is_some() { 2 } else { 1 };
        if changes.len().checked_add(needed).is_none_or(|count| count > changes.capacity()) {
            return Err(NativeError::Capacity("extra history preparation"));
        }
        if let Some(fact) = extra.fact {
            if *ordinal >= outcome.events { return Err(NativeError::Capacity("extra history ordinal")); }
            let event = NativeEvent { request: outcome.request, sequence: outcome.sequence, ordinal: *ordinal, fact };
            let stored = OwnedEvent::new(StoredEvent::pack(event)?)?;
            let heap = stored.heap_charge()?;
            changes.push(Change::Put(Entry::new(Key::Event(outcome.sequence, *ordinal), Row::Event(stored), heap)));
            *ordinal = ordinal.checked_add(1).ok_or(NativeError::Capacity("event ordinal"))?;
        }
        changes.push(Change::Put(Entry::new(extra.key, extra.row, extra.heap)));
    }
    Ok(())
}
