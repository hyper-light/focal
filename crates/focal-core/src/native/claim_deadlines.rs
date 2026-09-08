//! Trusted claim timers select a canonical deadlock victim before ordinary
//! expiry. Graph evidence and all mutations read one effective owner prefix.
//! Only expiry revokes evaluation authority; graph failures preserve begun work.
use super::prepare::{Extras, Scratch, heap, within};
use super::*;
use focal_model::lifecycle::{
    claim::{ClaimCut, ClaimTerminalCut},
    graph,
};

/// Cheap, allocation-free admission facts. Graph discovery deliberately waits
/// until the common control construction reservation has been acquired.
#[derive(Debug, Clone, Copy)]
pub(super) struct Resolved {
    binding: Binding,
    terminal: bool,
}

pub(super) fn resolve(
    view: &View<'_>,
    input: NativeClaimDeadlineInput,
    logical_time: u64,
) -> Result<Resolved, NativeError> {
    let claim = view
        .claim(input.claim)
        .ok_or(ContractError::InvalidTarget)?;
    if claim.binding().ledger != view.ledger() {
        return Err(ContractError::WrongLedger.into());
    }
    if claim.binding().object.0 != input.claim.0
        || input.claim.is_zero()
        || claim.binding().revision.0 == 0
    {
        return Err(ContractError::InvalidTarget.into());
    }
    if claim.created().0 == 0
        || claim.created() > view.prefix()
        || claim.deadline() != Some(input.deadline)
        || input.deadline.timer.is_zero()
        || input.deadline.generation == 0
        || logical_time < input.deadline.at
    {
        return Err(ContractError::InvalidCut.into());
    }
    Ok(Resolved {
        binding: claim.binding(),
        terminal: claim.is_terminal(),
    })
}

pub(super) fn copy(source: &ClaimState, scratch: &mut Scratch) -> Result<ClaimState, NativeError> {
    let charge = heap(source)?;
    scratch.charge(charge)?;
    let copied = source.try_copy(source.retained_bytes()?)?;
    within(heap(&copied)?, charge)?;
    Ok(copied)
}

pub(super) fn record(
    extras: &mut Extras,
    source: &ClaimState,
    next: &ClaimState,
    kind: NativeEventKind,
    graph: NativeGraphCapture,
) -> Result<(), NativeError> {
    source.binding().next()?.check(&next.binding())?;
    extras.record(NativeFact::Claim(NativeClaimEvent {
        graph: Some(graph),
        kind,
        owned_child: None,
        before: Some(source.binding()),
        after: next.binding(),
        status: next.status(),
    }))
}

fn registry<'a>(
    view: &'a View<'_>,
    claim: &ClaimState,
    limits: NativeLimits,
) -> Result<&'a RegistrationSet, NativeError> {
    let registry = view
        .owned_claim(ClaimId(claim.binding().object.0))?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    registry.check(claim)?;
    within(registry.rows().len(), limits.evaluations_per_claim)?;
    within(registry.rows().len(), limits.plan_edges)?;
    Ok(registry)
}

pub(super) fn fence_expired(
    view: &View<'_>,
    expired: &ClaimState,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
    visits: &mut graph::VisitBudget,
) -> Result<(), NativeError> {
    let id = ClaimId(expired.binding().object.0);
    for registered in registry(view, expired, limits)?.rows() {
        visits.charge(1)?;
        let key = transactions::key_for_registered(id, *registered);
        let declaration = view.definition(key.validation)?;
        let previous = *view.evaluation(key)?;
        registered.check_state(previous, declaration)?;
        let next = previous.expire_claim(declaration, expired)?;
        if next != previous {
            extras.evaluation(id, declaration, Some(previous.binding()), next, scratch)?;
        }
    }
    Ok(())
}

/// The immutable topology comes from the complete owner closure. Terminal
/// replacements are merged in key order; earlier staged cuts participate in
/// every subsequent witness without becoming externally observable prefixes.
pub(super) fn freeze<'a>(
    original: &[&'a ClaimState],
    changed: &'a [ClaimState],
    limits: NativeLimits,
    scratch: &mut Scratch,
    visits: &mut graph::VisitBudget,
) -> Result<(Vec<&'a ClaimState>, graph::Snapshot), NativeError> {
    visits.charge(original.len())?;
    let mut current = scratch.reserve::<&ClaimState>(original.len())?;
    let mut replacements = changed.iter().peekable();
    for source in original {
        let row = if let Some(next) = replacements.peek() {
            if next.binding().object < source.binding().object {
                return Err(ContractError::InvalidManifest.into());
            }
            if next.binding().object == source.binding().object {
                replacements.next().ok_or(ContractError::InvalidManifest)?
            } else {
                *source
            }
        } else {
            *source
        };
        if current.len() == current.capacity() {
            return Err(ContractError::Capacity.into());
        }
        current.push(row);
    }
    if replacements.next().is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    let plan = graph::Snapshot::prepare_capture_with_visits(
        &current,
        graph::Limits {
            nodes: limits.plan_nodes,
            edges: limits.plan_edges,
            visits: limits.plan_edges,
        },
        scratch.remaining()?,
        visits,
    )?;
    let charge = plan.construction_charge();
    scratch.charge(charge)?;
    let snapshot = plan.build_with_visits(visits)?;
    within(snapshot.retained_charge()?, charge)?;
    Ok((current, snapshot))
}

pub(super) fn peers<'a>(
    current: &[&'a ClaimState],
    selected: Binding,
    peers: &mut Vec<&'a ClaimState>,
    visits: &mut graph::VisitBudget,
) -> Result<(), NativeError> {
    // One pass constructs peers, one is the model witness's exact binding check,
    // and release may hash every captured binding. Those helpers allocate none.
    visits.charge(
        current
            .len()
            .checked_mul(3)
            .ok_or(ContractError::Capacity)?,
    )?;
    peers.clear();
    for row in current {
        if row.binding().object != selected.object {
            if peers.len() == peers.capacity() {
                return Err(ContractError::Capacity.into());
            }
            peers.push(*row);
        }
    }
    Ok(())
}

fn install(
    changed: &mut Vec<ClaimState>,
    row: ClaimState,
    visits: &mut graph::VisitBudget,
) -> Result<(), NativeError> {
    if !row.is_terminal() {
        return Err(ContractError::InvalidTransition.into());
    }
    visits.charge(
        changed
            .len()
            .checked_add(1)
            .ok_or(ContractError::Capacity)?,
    )?;
    match changed.binary_search_by_key(&row.binding().object, |row| row.binding().object) {
        Ok(position) => {
            // An earlier real monitor release may already have advanced this
            // object in the same candidate. Consume that exact open revision.
            let previous = changed
                .get_mut(position)
                .ok_or(ContractError::InvalidTarget)?;
            if previous.is_terminal() || previous.binding().next()? != row.binding() {
                return Err(ContractError::InvalidTransition.into());
            }
            *previous = row;
        }
        Err(position) => {
            if changed.len() == changed.capacity() {
                return Err(ContractError::Capacity.into());
            }
            changed.insert(position, row);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One frozen graph propagation stage.
fn propagate(
    original: &[&ClaimState],
    changed: &mut Vec<ClaimState>,
    pending: &mut Vec<ClaimState>,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
    visits: &mut graph::VisitBudget,
) -> Result<(), NativeError> {
    let captured = super::graph_effects::capture(extras)?;
    let (current, snapshot) = freeze(original, changed, limits, scratch, visits)?;
    snapshot.check_cut(cut.position)?;
    let failure_charge = snapshot.dependency_failure_charge()?;
    // Failure witness buffers coexist only for one target at a time. They all
    // spend the same held allowance, while visits remain cumulative per query.
    scratch.charge(failure_charge)?;
    let count = current
        .len()
        .checked_sub(1)
        .ok_or(ContractError::InvalidManifest)?;
    let mut peer_rows = scratch.reserve::<&ClaimState>(count)?;
    for source in &current {
        visits.charge(1)?;
        if source.is_terminal() {
            continue;
        }
        let id = ClaimId(source.binding().object.0);
        peers(&current, source.binding(), &mut peer_rows, visits)?;
        let next = match snapshot.dependency_failure_with_visits(id, failure_charge, visits) {
            Ok(failure) => {
                let mut next = copy(source, scratch)?;
                next.dependency_failed(&source.binding(), &failure, &peer_rows, cut.position)?;
                record(
                    extras,
                    source,
                    &next,
                    NativeEventKind::DependencyFailed,
                    captured,
                )?;
                Some(next)
            }
            Err(ContractError::InvalidTransition) => {
                if source.status() == ClaimStatus::Validating
                    && source.local_complete()
                    && snapshot.satisfied(id)?
                {
                    let release = snapshot.release(id)?;
                    let mut next = copy(source, scratch)?;
                    next.graph_release(&source.binding(), &release, &peer_rows, cut.position)?;
                    record(extras, source, &next, NativeEventKind::Satisfied, captured)?;
                    Some(next)
                } else {
                    None
                }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(next) = next {
            within(heap(&next)?, heap(source)?)?;
            if pending.len() == pending.capacity() {
                return Err(ContractError::Capacity.into());
            }
            pending.push(next);
        }
    }
    // All consequences above used exactly the same pre-propagation source.
    // Only now can their terminal rows become inputs to a subsequent SCC query.
    drop(peer_rows);
    drop(current);
    for row in pending.drain(..) {
        install(changed, row, visits)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One pre-admitted trusted transaction context.
pub(super) fn prepare(
    view: &View<'_>,
    resolved: Resolved,
    input: NativeClaimDeadlineInput,
    logical_time: u64,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let source = view
        .claim(input.claim)
        .ok_or(ContractError::InvalidTarget)?;
    resolved.binding.check(&source.binding())?;
    if resolved.terminal != source.is_terminal() {
        return Err(ContractError::InvalidTransition.into());
    }
    extras.begin_journal(
        if resolved.terminal {
            0
        } else {
            limits.range.max_batch_entries
        },
        scratch,
    )?;
    if resolved.terminal {
        return Ok(transactions::Plan {
            rows: Vec::new(),
            registry: transactions::RegistryOverrides::new(),
            created: 0,
        });
    }
    let (original, discovery_visits) =
        super::graph_effects::deadline_closure(view, source, limits, scratch)?;
    let mut visits = graph::VisitBudget::new(limits.plan_edges);
    visits.charge(discovery_visits)?;
    let mut active_monitors = false;
    for claim in &original {
        visits.charge(1)?;
        for monitor in claim.scopes().iter() {
            visits.charge(1)?;
            active_monitors |= monitor.active();
        }
    }
    let mut changed = scratch.reserve::<ClaimState>(original.len())?;
    let mut pending = scratch.reserve::<ClaimState>(original.len())?;
    let mut rounds = original.len();
    while changed
        .binary_search_by_key(&source.binding().object, |row| row.binding().object)
        .ok()
        .and_then(|index| changed.get(index))
        .is_none_or(|claim| !claim.is_terminal())
    {
        rounds = rounds.checked_sub(1).ok_or(ContractError::Capacity)?;
        visits.charge(1)?;
        let prior = extras.events();
        let captured = super::graph_effects::capture(extras)?;
        let next = {
            let (current, snapshot) = freeze(&original, &changed, limits, scratch, &mut visits)?;
            let trigger = current
                .binary_search_by_key(&source.binding().object, |row| row.binding().object)
                .ok()
                .and_then(|index| current.get(index))
                .copied()
                .ok_or(ContractError::InvalidTarget)?;
            let charge = snapshot.deadlock_charge()?;
            scratch.charge(charge)?;
            let source_cut = if changed.is_empty() {
                view.prefix()
            } else {
                cut.position
            };
            let witness = snapshot.deadlock_query_with_visits(
                input.claim,
                input.deadline,
                logical_time,
                source_cut,
                charge,
                &mut visits,
            )?;
            let mut peer_rows = scratch.reserve::<&ClaimState>(
                current
                    .len()
                    .checked_sub(1)
                    .ok_or(ContractError::InvalidManifest)?,
            )?;
            match witness {
                Some(witness) => {
                    let binding = witness.victim()?;
                    let index = current
                        .binary_search_by_key(&binding.object, |row| row.binding().object)
                        .map_err(|_| ContractError::InvalidTarget)?;
                    let actual = *current.get(index).ok_or(ContractError::InvalidTarget)?;
                    binding.check(&actual.binding())?;
                    peers(&current, binding, &mut peer_rows, &mut visits)?;
                    let mut next = copy(actual, scratch)?;
                    next.break_deadlock(&binding, &witness, &peer_rows, cut.position)?;
                    record(extras, actual, &next, NativeEventKind::Deadlocked, captured)?;
                    next
                }
                None => {
                    // A prior justified local success wins over expiry. All
                    // other still-open negative SCC outcomes expire the trigger.
                    let mut next = copy(trigger, scratch)?;
                    if snapshot.satisfied(input.claim)? {
                        peers(&current, trigger.binding(), &mut peer_rows, &mut visits)?;
                        next.graph_release(
                            &trigger.binding(),
                            &snapshot.release(input.claim)?,
                            &peer_rows,
                            cut.position,
                        )?;
                        record(extras, trigger, &next, NativeEventKind::Satisfied, captured)?;
                    } else {
                        next.expire(&trigger.binding(), input.deadline, logical_time, cut)?;
                        record(extras, trigger, &next, NativeEventKind::Expired, captured)?;
                        fence_expired(view, &next, limits, extras, scratch, &mut visits)?;
                    }
                    next
                }
            }
        };
        install(&mut changed, next, &mut visits)?;
        if active_monitors {
            // Scope releases add real revisions and reverse-index changes.
            // Recapture after every transition before any subsequent SCC query.
            super::graph_effects::settle_scopes(
                view,
                &original,
                &mut changed,
                cut,
                limits,
                extras,
                scratch,
                &mut visits,
            )?;
        } else {
            // Newly terminal dependencies can settle Awaits edges that the prior
            // frozen snapshot could not release. Finish that monotone propagation
            // even when the timer's own trigger is already terminal.
            loop {
                let before = changed.len();
                propagate(
                    &original,
                    &mut changed,
                    &mut pending,
                    cut,
                    limits,
                    extras,
                    scratch,
                    &mut visits,
                )?;
                if changed.len() == before {
                    break;
                }
                if changed.len() > original.len() {
                    return Err(ContractError::InvalidTransition.into());
                }
            }
        }
        if extras.events() <= prior {
            return Err(ContractError::InvalidTransition.into());
        }
    }
    Ok(transactions::Plan {
        rows: changed,
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}

fn claim_row(rows: &[ClaimState], binding: Binding) -> Result<&ClaimState, NativeError> {
    let row = rows
        .iter()
        .find(|row| row.binding().object == binding.object)
        .ok_or(ContractError::InvalidTarget)?;
    Binding {
        revision: binding.revision,
        ..row.binding()
    }
    .check(&binding)?;
    if row.binding().revision < binding.revision {
        return Err(ContractError::StaleRevision.into());
    }
    Ok(row)
}

/// Claim history has already passed the common complete revision-chain check.
/// This timer-specific check proves the initiating cut and the exact expiry
/// fence cohort; it cannot admit WholeWork entry or any participant evidence.
pub(super) fn check_journal(
    rows: &[ClaimState],
    extras: &mut Extras,
    view: &View<'_>,
    outcome: NativeOutcome,
    limits: NativeLimits,
    index_rows: usize,
) -> Result<(), NativeError> {
    let NativeInvocation::ClaimDeadline(key) = outcome.invocation else {
        return Err(ContractError::InvalidTransition.into());
    };
    let source = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    let input = NativeClaimDeadlineInput {
        claim: key.claim,
        deadline: source.deadline().ok_or(ContractError::InvalidCut)?,
    };
    let resolved = resolve(view, input, outcome.logical_time)?;
    if input.key() != key
        || intent::claim_deadline_fingerprint(view.ledger(), input)? != outcome.intent
        || outcome.operation != NativeOperation::ClaimDeadline
        || outcome.sequence.0
            != view
                .prefix()
                .0
                .checked_add(1)
                .ok_or(ContractError::Capacity)?
    {
        return Err(ContractError::InvalidCut.into());
    }
    let events = extras
        .journal
        .as_deref()
        .ok_or(ContractError::InvalidTransition)?;
    within(events.len(), limits.plan_edges)?;
    within(rows.len(), limits.plan_nodes)?;
    if resolved.terminal {
        return if rows.is_empty() && extras.rows.is_empty() && events.is_empty() && index_rows == 0
        {
            Ok(())
        } else {
            Err(ContractError::InvalidTransition.into())
        };
    }
    let trigger = rows
        .iter()
        .find(|row| row.binding().object.0 == input.claim.0)
        .ok_or(ContractError::InvalidTransition)?;
    if !trigger.is_terminal() {
        return Err(ContractError::InvalidTransition.into());
    }
    // Monitor releases can add intermediate or terminal-owner revisions. The
    // common checker independently verifies the complete rows/history bijection.
    extras.rows.sort_unstable_by_key(|extra| extra.key);
    if extras
        .rows
        .windows(2)
        .any(|pair| pair.first().map(|row| row.key) == pair.get(1).map(|row| row.key))
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut remaining = events.iter();
    let mut changed = 0usize;
    let mut started = false;
    let mut trigger_done = false;
    let mut expired = false;
    let mut visits = limits.plan_edges;
    while let Some(fact) = remaining.next() {
        visits = visits
            .checked_sub(rows.len())
            .ok_or(NativeError::Capacity("deadline journal visits"))?;
        let NativeFact::Claim(event) = fact else {
            return Err(ContractError::InvalidTransition.into());
        };
        let ordinal = events
            .len()
            .checked_sub(remaining.len())
            .and_then(|n| n.checked_sub(1))
            .ok_or(ContractError::Capacity)?;
        let prefix = events
            .get(..ordinal)
            .ok_or(ContractError::InvalidManifest)?;
        let row = claim_row(rows, event.after)?;
        let previous = view
            .claim(ClaimId(event.after.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        let (prior_binding, prior_status) =
            super::monitor_deadlines::prior_binding(prefix, previous, &mut visits)?;
        if event.before != Some(prior_binding)
            || event.after != prior_binding.next()?
            || event.owned_child.is_some()
        {
            return Err(ContractError::InvalidTransition.into());
        }
        if let NativeEventKind::Monitor(NativeMonitorEvent::Released { id, cut }) = event.kind {
            let original = previous
                .scopes()
                .monitor(id)
                .ok_or(ContractError::InvalidTarget)?;
            let released = row
                .scopes()
                .monitor(id)
                .ok_or(ContractError::InvalidTarget)?;
            visits = visits
                .checked_sub(prefix.len())
                .ok_or(NativeError::Capacity("deadline monitor history visits"))?;
            if !started
                || !original.active()
                || released.release_cut() != Some(cut)
                || cut.position != outcome.sequence
                || cut.cause != outcome.intent
                || original.deadline() != released.deadline()
                || original.registered() != released.registered()
                || original.roots() != released.roots()
                || original.last_rebinding() != released.last_rebinding()
                || event.status != prior_status
                || prefix.iter().any(|fact| matches!(fact,
                    NativeFact::Claim(prior) if prior.after.object == event.after.object
                        && matches!(prior.kind, NativeEventKind::Monitor(change) if change.id() == id)))
            {
                return Err(ContractError::InvalidCut.into());
            }
            continue;
        }
        if prior_status.is_terminal() || !row.is_terminal() || event.status != row.status() {
            return Err(ContractError::InvalidTransition.into());
        }
        match (event.kind, row.terminal_cut()) {
            (NativeEventKind::Deadlocked, Some(ClaimTerminalCut::Graph(cut))) => {
                let (trigger_binding, trigger_status) =
                    super::monitor_deadlines::prior_binding(prefix, source, &mut visits)?;
                if trigger_done
                    || trigger_status.is_terminal()
                    || row.status() != ClaimStatus::Deadlocked
                    || cut.kind() != graph::FailureKind::Deadlocked
                    || cut.origin().binding() != trigger_binding
                    || cut.origin().created() != source.created()
                    || cut.origin().terminal() != outcome.sequence
                    || cut.sequence() != outcome.sequence
                    || cut.deadline() != Some(input.deadline)
                    || cut.fired_at() != Some(outcome.logical_time)
                    || cut.fingerprint() == ContentHash([0; 32])
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            (NativeEventKind::Expired, Some(ClaimTerminalCut::Explicit(cut))) => {
                if trigger_done
                    || expired
                    || row.binding().object.0 != key.claim.0
                    || row.status() != ClaimStatus::Expired
                    || cut.position != outcome.sequence
                    || cut.cause != outcome.intent
                {
                    return Err(ContractError::InvalidCut.into());
                }
                expired = true;
                changed = check_expiry_fences(view, row, limits, &extras.rows, &mut remaining)?;
            }
            (NativeEventKind::Satisfied, Some(ClaimTerminalCut::Explicit(cut))) => {
                if (!started && row.binding().object.0 != key.claim.0)
                    || prior_status != ClaimStatus::Validating
                    || !previous.local_complete()
                    || row.status() != ClaimStatus::Satisfied
                    || cut.position != outcome.sequence
                    || cut.cause == ContentHash([0; 32])
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            (NativeEventKind::DependencyFailed, Some(ClaimTerminalCut::Graph(cut))) => {
                if !started
                    || row.status() != ClaimStatus::DependencyFailed
                    || cut.kind() != graph::FailureKind::DependencyFailed
                    || cut.sequence() != outcome.sequence
                    || cut.fingerprint() == ContentHash([0; 32])
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            _ => return Err(ContractError::InvalidTransition.into()),
        }
        started = true;
        trigger_done |= row.binding().object.0 == key.claim.0;
    }
    if !started
        || !trigger_done
        || extras.rows.len()
            != index_rows
                .checked_add(changed)
                .ok_or(ContractError::Capacity)?
    {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(())
}

pub(super) fn check_expiry_fences(
    view: &View<'_>,
    expired: &ClaimState,
    limits: NativeLimits,
    extras: &[super::prepare::Extra],
    remaining: &mut std::slice::Iter<'_, NativeFact>,
) -> Result<usize, NativeError> {
    let id = ClaimId(expired.binding().object.0);
    let mut changed = 0usize;
    for registered in registry(view, expired, limits)?.rows() {
        let key = transactions::key_for_registered(id, *registered);
        let definition = view.definition(key.validation)?;
        let previous = *view.evaluation(key)?;
        registered.check_state(previous, definition)?;
        let next = previous.expire_claim(definition, expired)?;
        let found = extras.binary_search_by_key(&Key::Evaluation(key), |row| row.key);
        if next == previous {
            if found.is_ok() {
                return Err(ContractError::InvalidTransition.into());
            }
            continue;
        }
        let extra = extras
            .get(found.map_err(|_| ContractError::InvalidManifest)?)
            .ok_or(ContractError::InvalidManifest)?;
        let Row::Evaluation(stored) = &extra.row else {
            return Err(ContractError::InvalidTarget.into());
        };
        if stored.get() != Some(&next)
            || extra.fact.is_some()
            || extra.heap != stored.heap_charge()?
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let expected = NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::AuthorityFenced,
            key,
            before: Some(previous.binding()),
            after: next.binding(),
            state: next.state(),
            phase: next.phase(),
            attempt: if next.has_begun() {
                Some(next.bind(definition)?.current_attempt()?)
            } else {
                None
            },
            fence: next.fence(),
        };
        if remaining.next() != Some(&expected) {
            return Err(ContractError::InvalidTransition.into());
        }
        changed = changed.checked_add(1).ok_or(ContractError::Capacity)?;
    }
    Ok(changed)
}
