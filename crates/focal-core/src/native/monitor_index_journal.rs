//! Exact replay of retained subscription writes from checked scope histories.
use super::*;
pub(in crate::native) fn replay_bytes(index_rows: usize) -> Result<usize, NativeError> {
    crate::native::prepare::array::<Extra>(index_rows)
}
fn index_key(key: Key) -> bool {
    matches!(
        key,
        Key::Monitor(_) | Key::MonitorHead(_) | Key::MonitorLink(..)
    )
}
fn same_index(a: &Row, b: &Row) -> bool {
    match (a, b) {
        (Row::Monitor(a), Row::Monitor(b)) => a == b,
        (Row::MonitorHead(a), Row::MonitorHead(b)) => a == b,
        (Row::MonitorLink(a), Row::MonitorLink(b)) => a == b,
        _ => false,
    }
}
fn model_event(fact: &NativeFact) -> Option<(ClaimId, NativeMonitorEvent)> {
    match fact {
        NativeFact::Claim(event) => match event.kind {
            NativeEventKind::Monitor(monitor) => Some((ClaimId(event.after.object.0), monitor)),
            _ => None,
        },
        _ => None,
    }
}
fn events_for(
    journal: &[NativeFact],
    owner: ClaimId,
    id: MonitorId,
    visits: &mut Visits,
) -> Result<(Option<NativeMonitorEvent>, Option<NativeMonitorEvent>), ContractError> {
    visits.take(journal.len())?;
    let mut found = journal
        .iter()
        .filter_map(model_event)
        .filter(|(claim, event)| *claim == owner && event.id() == id)
        .map(|(_, event)| event);
    let first = found.next();
    let second = found.next();
    if found.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    Ok((first, second))
}
fn checked_scope_history(
    owner: &ClaimState,
    next: &ClaimState,
    scope: &Scope,
    journal: &[NativeFact],
    visits: &mut Visits,
) -> Result<(), ContractError> {
    let id = scope.id();
    let depth = owner
        .scopes()
        .limits()
        .scopes
        .checked_ilog2()
        .map(|n| n.checked_add(1).ok_or(ContractError::Capacity))
        .transpose()?
        .unwrap_or(0);
    visits.take(add(
        usize::try_from(depth).map_err(|_| ContractError::Capacity)?,
        1,
    )?)?;
    let old = owner.scopes().monitor(id);
    let (first, second) = events_for(journal, ClaimId(owner.binding().object.0), id, visits)?;
    let Some(first) = first else {
        visits.take(scope.roots().len())?;
        if old != Some(scope) {
            return Err(ContractError::InvalidManifest);
        }
        return Ok(());
    };
    if let Some(second) = second
        && (!matches!(
            first,
            NativeMonitorEvent::Registered { .. } | NativeMonitorEvent::Rebound { .. }
        ) || !matches!(
            second,
            NativeMonitorEvent::Released { .. } | NativeMonitorEvent::Cancelled { .. }
        ))
    {
        return Err(ContractError::InvalidManifest);
    }
    match first {
        NativeMonitorEvent::Registered { cut, .. } => {
            if old.is_some()
                || scope.registered() != cut.position
                || scope.last_rebinding().is_some()
                || scope.roots().is_empty()
                || scope.deadline().timer.is_zero()
                || scope.deadline().generation == 0
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        NativeMonitorEvent::Rebound { change, .. } => {
            let old = old.ok_or(ContractError::InvalidTarget)?;
            if !old.active()
                || old.registered() != scope.registered()
                || old.deadline() != scope.deadline()
                || scope.last_rebinding() != Some(change)
                || change.predecessor == change.successor
                || !contains(old, change.predecessor, visits)?
            {
                return Err(ContractError::InvalidManifest);
            }
            visits.take(mul(
                mul(add(old.roots().len(), 1)?, add(scope.roots().len(), 1)?)?,
                2,
            )?)?;
            let mapped = |root| match root {
                WaitPredicate::Satisfied(id) if id == change.predecessor => {
                    WaitPredicate::Satisfied(change.successor)
                }
                WaitPredicate::Terminal(id) if id == change.predecessor => {
                    WaitPredicate::Terminal(change.successor)
                }
                WaitPredicate::Released(id) if id == change.predecessor => {
                    WaitPredicate::Released(change.successor)
                }
                root => root,
            };
            if old
                .roots()
                .iter()
                .any(|root| !scope.roots().contains(&mapped(*root)))
                || scope
                    .roots()
                    .iter()
                    .any(|root| !old.roots().iter().any(|prior| mapped(*prior) == *root))
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        NativeMonitorEvent::Released { .. } | NativeMonitorEvent::Cancelled { .. } => {
            let old = old.ok_or(ContractError::InvalidTarget)?;
            visits.take(add(old.roots().len(), scope.roots().len())?)?;
            if !old.active()
                || old.registered() != scope.registered()
                || old.deadline() != scope.deadline()
                || old.roots() != scope.roots()
                || old.last_rebinding() != scope.last_rebinding()
            {
                return Err(ContractError::InvalidManifest);
            }
        }
    }
    match second.unwrap_or(first) {
        NativeMonitorEvent::Registered { .. } | NativeMonitorEvent::Rebound { .. }
            if scope.active() => {}
        NativeMonitorEvent::Released { cut, .. }
            if scope.disposition() == Some(MonitorDisposition::Released(cut)) => {}
        NativeMonitorEvent::Cancelled { cancellation, .. }
            if scope.disposition() == Some(MonitorDisposition::Cancelled(cancellation)) =>
        {
            if !next.is_terminal() {
                return Err(ContractError::InvalidTransition);
            }
        }
        _ => return Err(ContractError::InvalidManifest),
    }
    Ok(())
}
/// Verify all and only index writes by replaying the genuine scope event deltas
/// into one precharged overlay. Original/final scopes remain borrowed; there is
/// no synthetic model transition and no second history-publication pass.
pub(in crate::native) fn check_journal(
    view: &View<'_>,
    rows: &[ClaimState],
    extras: &Extras,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<usize, NativeError> {
    let mut visits = Visits(limits.plan_edges);
    visits.take(extras.rows.len())?;
    let count = extras
        .rows
        .iter()
        .filter(|extra| index_key(extra.key))
        .count();
    let journal = extras.journal.as_deref().unwrap_or(&[]);
    visits.take(journal.len())?;
    let has_events = journal.iter().any(|fact| model_event(fact).is_some());
    if count == 0 && !has_events {
        return Ok(0);
    }
    if count > limits.range.max_batch_entries {
        return Err(ContractError::Capacity.into());
    }
    let mut expected = Extras::new(count, replay_bytes(count)?)?;
    expected.rows = scratch.reserve(count)?;
    // Complete retained membership: removals or changes without their named
    // event are refused, including inactive historical scopes.
    for next in rows {
        visits.take(1)?;
        let Some(owner) = view.claim(ClaimId(next.binding().object.0)) else {
            // Only a private checked control capability may introduce a source
            // row. Creation has no monitor memberships to reconstruct; later
            // graph outcomes may still advance its final claim revision.
            let proof = extras
                .control_graph
                .as_ref()
                .ok_or(ContractError::InvalidTarget)?;
            visits.take(add(proof.originals().len(), proof.prefix().len())?)?;
            let original = proof
                .originals()
                .iter()
                .find(|row| row.binding().object == next.binding().object)
                .ok_or(ContractError::InvalidTarget)?;
            same_owner(next, original.binding(), view.ledger())?;
            if original.created().0
                != view
                    .prefix()
                    .0
                    .checked_add(1)
                    .ok_or(ContractError::InvalidCut)?
                || original.scopes().iter().next().is_some()
                || next.scopes().iter().next().is_some()
                || !proof.prefix().iter().any(|fact| {
                    matches!(fact,
                    NativeFact::Claim(event) if event.kind == NativeEventKind::Created
                    && event.before.is_none() && event.after == Binding {
                        revision: focal_model::ObjectRevision(1), ..original.binding()
                    })
                })
            {
                return Err(ContractError::InvalidManifest.into());
            }
            continue;
        };
        same_owner(next, owner.binding(), view.ledger())?;
        let baseline = if let Some(proof) = &extras.control_graph {
            visits.take(proof.originals().len())?;
            proof
                .originals()
                .iter()
                .find(|row| row.binding().object == next.binding().object)
                .unwrap_or(owner)
        } else {
            owner
        };
        visits.take(add(
            baseline.scopes().children().len(),
            next.scopes().children().len(),
        )?)?;
        if baseline.scopes().limits() != next.scopes().limits()
            || baseline.scopes().children() != next.scopes().children()
        {
            return Err(ContractError::InvalidManifest.into());
        }
        if baseline.scopes().release_cut() != next.scopes().release_cut() {
            visits.take(journal.len())?;
            let cut = next
                .scopes()
                .release_cut()
                .ok_or(ContractError::InvalidCut)?;
            let mut releases = journal.iter().filter_map(|fact| match fact {
                NativeFact::Claim(event)
                    if event.kind == NativeEventKind::OwnerReleased
                        && event.after.object == next.binding().object =>
                {
                    Some(event)
                }
                _ => None,
            });
            let released = releases.next().ok_or(ContractError::InvalidManifest)?;
            if baseline.scopes().released()
                || !baseline.is_terminal()
                || released.before != Some(baseline.binding())
                || released.after != next.binding()
                || released.after != baseline.binding().next()?
                || releases.next().is_some()
                || cut.position.0
                    != view
                        .prefix()
                        .0
                        .checked_add(1)
                        .ok_or(ContractError::InvalidCut)?
                || cut.cause == ContentHash([0; 32])
            {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        for old in owner.scopes().iter() {
            visits.take(1)?;
            monitor(next, old.id(), &mut visits)?;
        }
        for scope in next.scopes().iter() {
            visits.take(1)?;
            checked_scope_history(owner, next, scope, journal, &mut visits)?;
        }
    }
    let overlay = limits.range.max_batch_entries;
    for fact in journal {
        visits.take(1)?;
        let Some((owner_id, event)) = model_event(fact) else {
            continue;
        };
        visits.take(add(rows.len(), 1)?)?;
        let owner = view.claim(owner_id).ok_or(ContractError::InvalidTarget)?;
        let next = rows
            .iter()
            .find(|row| row.binding().object.0 == owner_id.0)
            .ok_or(ContractError::InvalidTarget)?;
        let scope = monitor(next, event.id(), &mut visits)?;
        let cut = event.cut();
        if cut.cause == ContentHash([0; 32])
            || cut.position.0
                != view
                    .prefix()
                    .0
                    .checked_add(1)
                    .ok_or(ContractError::InvalidCut)?
        {
            return Err(ContractError::InvalidCut.into());
        }
        // Before any replay mutation, validate each original chain reached by
        // this event. Source validation is independent of untrusted Extras.
        let roots = match event {
            NativeMonitorEvent::Rebound { .. } => owner
                .scopes()
                .monitor(event.id())
                .ok_or(ContractError::InvalidTarget)?
                .roots(),
            _ => scope.roots(),
        };
        for (index, root) in roots.iter().enumerate() {
            if unique(roots, index, &mut visits)? {
                chain(
                    &Read {
                        view,
                        extras: &[],
                        owner: None,
                        owners: &[],
                        overlay: 0,
                    },
                    target(*root),
                    limits,
                    &mut visits,
                    false,
                    None,
                )?;
            }
        }
        match event {
            NativeMonitorEvent::Registered { .. } => {
                if read(view, &expected, owner, limits)
                    .get(Key::Monitor(event.id()), &mut visits)?
                    .is_some()
                {
                    return Err(ContractError::InvalidTarget.into());
                }
                let NativeFact::Claim(claim_event) = fact else {
                    return Err(ContractError::InvalidManifest.into());
                };
                let binding = claim_event.before.ok_or(ContractError::InvalidTarget)?;
                same_owner(owner, binding, view.ledger()).or_else(|error| {
                    // Earlier genuine same-candidate claim revisions may precede
                    // registration; immutable identity is checked independently.
                    if binding.ledger == owner.binding().ledger
                        && binding.object == owner.binding().object
                        && binding.content == owner.binding().content
                        && binding.revision >= owner.binding().revision
                    {
                        Ok(())
                    } else {
                        Err(error)
                    }
                })?;
                put(
                    &mut expected,
                    Key::Monitor(event.id()),
                    Row::Monitor(MonitorAllocation {
                        owner: binding,
                        registered: scope.registered(),
                        deadline: scope.deadline(),
                    }),
                    &mut visits,
                    overlay,
                )?;
                for (index, root) in scope.roots().iter().enumerate() {
                    if unique(scope.roots(), index, &mut visits)? {
                        insert(
                            view,
                            owner,
                            scope,
                            target(*root),
                            limits,
                            &mut expected,
                            &mut visits,
                        )?;
                    }
                }
            }
            NativeMonitorEvent::Rebound { .. } => {
                let old = owner
                    .scopes()
                    .monitor(event.id())
                    .ok_or(ContractError::InvalidTarget)?;
                for (index, root) in old.roots().iter().enumerate() {
                    if !unique(old.roots(), index, &mut visits)? {
                        continue;
                    }
                    let target = target(*root);
                    if !contains(scope, target, &mut visits)? {
                        unlink(
                            view,
                            owner,
                            target,
                            event.id(),
                            limits,
                            &mut expected,
                            &mut visits,
                        )?;
                    } else {
                        let link = read(view, &expected, owner, limits).link(
                            target,
                            event.id(),
                            &mut visits,
                        )?;
                        put(
                            &mut expected,
                            Key::MonitorLink(target, event.id()),
                            Row::MonitorLink(Some(MonitorLink {
                                stamp: stamp(scope),
                                ..link
                            })),
                            &mut visits,
                            overlay,
                        )?;
                    }
                }
                for (index, root) in scope.roots().iter().enumerate() {
                    if unique(scope.roots(), index, &mut visits)?
                        && !contains(old, target(*root), &mut visits)?
                    {
                        chain(
                            &Read {
                                view,
                                extras: &[],
                                owner: None,
                                owners: &[],
                                overlay: 0,
                            },
                            target(*root),
                            limits,
                            &mut visits,
                            false,
                            None,
                        )?;
                        insert(
                            view,
                            owner,
                            scope,
                            target(*root),
                            limits,
                            &mut expected,
                            &mut visits,
                        )?;
                    }
                }
            }
            NativeMonitorEvent::Released { .. } | NativeMonitorEvent::Cancelled { .. } => {
                for (index, root) in scope.roots().iter().enumerate() {
                    if unique(scope.roots(), index, &mut visits)? {
                        unlink(
                            view,
                            owner,
                            target(*root),
                            event.id(),
                            limits,
                            &mut expected,
                            &mut visits,
                        )?;
                    }
                }
            }
        }
    }
    if expected.rows.len() != count {
        return Err(ContractError::InvalidManifest.into());
    }
    for extra in &extras.rows {
        visits.take(1)?;
        if !index_key(extra.key) {
            continue;
        }
        visits.take(expected.rows.len())?;
        let expected = expected
            .rows
            .iter()
            .find(|row| row.key == extra.key)
            .ok_or(ContractError::InvalidManifest)?;
        if extra.heap != 0 || extra.fact.is_some() || !same_index(&extra.row, &expected.row) {
            return Err(ContractError::InvalidManifest.into());
        }
    }
    for extra in &expected.rows {
        visits.take(1)?;
        if let Key::MonitorHead(target) = extra.key {
            chain(
                &Read {
                    view,
                    extras: &expected.rows,
                    owner: None,
                    owners: rows,
                    overlay,
                },
                target,
                limits,
                &mut visits,
                false,
                None,
            )?;
        }
    }
    Ok(count)
}
