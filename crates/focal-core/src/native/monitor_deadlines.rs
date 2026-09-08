//! Trusted monitor timers retain the authored monitor and deadline identity.
//! A complete negative SCC query is the only authority for monitor expiry;
//! settled waits use real release transitions and never synthesize testimony.
use super::claim_deadlines::{copy, fence_expired, freeze, peers, record};
use super::prepare::{Extras, Scratch, within};
use super::*;
use focal_model::lifecycle::{claim::ClaimCut, graph, scope};

#[derive(Debug, Clone, Copy)]
pub(super) struct Resolved {
    binding: Binding,
    inactive: bool,
}

fn monitor(source: &ClaimState, id: focal_model::MonitorId) -> Result<&scope::Scope, NativeError> {
    source
        .scopes()
        .monitor(id)
        .ok_or(ContractError::InvalidTarget.into())
}

/// Source checks allocate nothing. A disjoint invocation key is checked for an
/// exact retained retry before this fresh timer admission is attempted.
pub(super) fn resolve(
    view: &View<'_>,
    input: NativeMonitorDeadlineInput,
    logical_time: u64,
) -> Result<Resolved, NativeError> {
    let source = view
        .claim(input.claim)
        .ok_or(ContractError::InvalidTarget)?;
    if source.binding().ledger != view.ledger() {
        return Err(ContractError::WrongLedger.into());
    }
    if input.claim.is_zero()
        || input.monitor.is_zero()
        || input.deadline.timer.is_zero()
        || input.deadline.generation == 0
        || source.binding().object.0 != input.claim.0
        || source.binding().revision.0 == 0
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let scope = monitor(source, input.monitor)?;
    let Some(Row::Monitor(allocation)) = view.get(Key::Monitor(input.monitor)) else {
        return Err(ContractError::MissingEvidence.into());
    };
    if allocation.owner.ledger != source.binding().ledger
        || allocation.owner.object != source.binding().object
        || allocation.owner.content != source.binding().content
        || allocation.owner.revision.0 == 0
        || allocation.owner.revision >= source.binding().revision
        || allocation.registered != scope.registered()
        || allocation.deadline != input.deadline
    {
        return Err(ContractError::InvalidManifest.into());
    }
    if source.created().0 == 0
        || source.created() > view.prefix()
        || scope.registered() < source.created()
        || scope.registered() > view.prefix()
        || scope.deadline() != input.deadline
        || logical_time < input.deadline.at
        || scope.last_rebinding().is_some_and(|change| {
            change.cut.position < scope.registered() || change.cut.position > view.prefix()
        })
        || scope
            .release_cut()
            .is_some_and(|cut| cut.position < scope.registered() || cut.position > view.prefix())
        || scope.cancellation().is_some_and(|cancel| {
            cancel.cut.position < scope.registered()
                || cancel.cut.position > view.prefix()
                || cancel.terminal > cancel.cut.position
        })
    {
        return Err(ContractError::InvalidCut.into());
    }
    Ok(Resolved {
        binding: source.binding(),
        inactive: source.is_terminal() || source.scopes().released() || !scope.active(),
    })
}

fn current<'a>(rows: &'a [&ClaimState], id: ClaimId) -> Result<&'a ClaimState, NativeError> {
    rows.binary_search_by_key(&id.0, |row| row.binding().object.0)
        .ok()
        .and_then(|index| rows.get(index).copied())
        .ok_or(ContractError::InvalidTarget.into())
}

/// A row can have acquired real monitor revisions in earlier rounds. Installing
/// its terminal transition replaces that exact predecessor, never a stale copy.
fn install(
    original: &[&ClaimState],
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
            .checked_add(original.len())
            .ok_or(ContractError::Capacity)?,
    )?;
    match changed.binary_search_by_key(&row.binding().object, |value| value.binding().object) {
        Ok(index) => {
            let previous = changed.get_mut(index).ok_or(ContractError::InvalidTarget)?;
            if previous.is_terminal() || previous.binding().next()? != row.binding() {
                return Err(ContractError::StaleRevision.into());
            }
            *previous = row;
        }
        Err(index) => {
            let previous = current(original, ClaimId(row.binding().object.0))?;
            if previous.is_terminal() || previous.binding().next()? != row.binding() {
                return Err(ContractError::StaleRevision.into());
            }
            if changed.len() == changed.capacity() {
                return Err(ContractError::Capacity.into());
            }
            changed.insert(index, row);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One admitted trusted timer transaction.
pub(super) fn prepare(
    view: &View<'_>,
    resolved: Resolved,
    input: NativeMonitorDeadlineInput,
    logical_time: u64,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let source = view
        .claim(input.claim)
        .ok_or(ContractError::InvalidTarget)?;
    let checked = resolve(view, input, logical_time)?;
    resolved.binding.check(&checked.binding)?;
    if resolved.inactive != checked.inactive {
        return Err(ContractError::InvalidTransition.into());
    }
    if view.prefix().0.checked_add(1) != Some(cut.position.0)
        || cut.cause != intent::monitor_deadline_fingerprint(view.ledger(), input)?
    {
        return Err(ContractError::InvalidCut.into());
    }
    extras.begin_journal(
        if resolved.inactive {
            0
        } else {
            limits.range.max_batch_entries
        },
        scratch,
    )?;
    if resolved.inactive {
        return Ok(transactions::Plan {
            rows: Vec::new(),
            registry: transactions::RegistryOverrides::new(),
            created: 0,
        });
    }
    let (original, discovery_visits) =
        graph_effects::deadline_closure(view, source, limits, scratch)?;
    let mut visits = graph::VisitBudget::new(limits.plan_edges);
    visits.charge(discovery_visits)?;
    let mut changed = scratch.reserve::<ClaimState>(original.len())?;
    // Each iteration either resolves the trigger or terminalizes a previously
    // open component member. Releases between iterations only decrease blockers.
    let mut rounds = original
        .len()
        .checked_add(1)
        .ok_or(ContractError::Capacity)?;
    loop {
        rounds = rounds.checked_sub(1).ok_or(ContractError::Capacity)?;
        visits.charge(1)?;
        let before = extras.events();
        let captured = graph_effects::capture(extras)?;
        let (next, settled) = {
            let (current_rows, snapshot) =
                freeze(&original, &changed, limits, scratch, &mut visits)?;
            let actual = current(&current_rows, input.claim)?;
            let mut peer_rows = scratch.reserve::<&ClaimState>(
                current_rows
                    .len()
                    .checked_sub(1)
                    .ok_or(ContractError::InvalidManifest)?,
            )?;
            peers(&current_rows, actual.binding(), &mut peer_rows, &mut visits)?;
            let plan = scope::Registry::prepare_monitor_deadline_with_visits(
                actual,
                scope::MonitorDeadlineRequest {
                    id: input.monitor,
                    deadline: input.deadline,
                    fired_at: logical_time,
                },
                &snapshot,
                &peer_rows,
                cut,
                scope::BuildLimits {
                    bytes: scratch.remaining()?,
                    visits: limits.plan_edges,
                },
                &mut visits,
            )?;
            scratch.charge(plan.construction_charge())?;
            match plan.resolve_with_visits(&mut visits)? {
                scope::MonitorDeadlineDecision::Inactive => break,
                scope::MonitorDeadlineDecision::Settled => (None, true),
                scope::MonitorDeadlineDecision::Deadlock(witness) => {
                    let binding = witness.victim()?;
                    let victim = current(&current_rows, ClaimId(binding.object.0))?;
                    binding.check(&victim.binding())?;
                    let mut victim_peers = scratch.reserve::<&ClaimState>(peer_rows.capacity())?;
                    peers(&current_rows, binding, &mut victim_peers, &mut visits)?;
                    let mut next = copy(victim, scratch)?;
                    next.break_deadlock(&binding, &witness, &victim_peers, cut.position)?;
                    record(extras, victim, &next, NativeEventKind::Deadlocked, captured)?;
                    (Some(next), false)
                }
                scope::MonitorDeadlineDecision::Expire(expiry) => {
                    let mut next = copy(actual, scratch)?;
                    next.expire_monitor(&actual.binding(), &expiry, &peer_rows)?;
                    record(extras, actual, &next, NativeEventKind::Expired, captured)?;
                    fence_expired(view, &next, limits, extras, scratch, &mut visits)?;
                    (Some(next), false)
                }
            }
        };
        if let Some(next) = next {
            install(&original, &mut changed, next, &mut visits)?;
        }
        graph_effects::settle_scopes(
            view,
            &original,
            &mut changed,
            cut,
            limits,
            extras,
            scratch,
            &mut visits,
        )?;
        if extras.events() <= before {
            return Err(ContractError::InvalidTransition.into());
        }
        let trigger = changed
            .binary_search_by_key(&input.claim.0, |row| row.binding().object.0)
            .ok()
            .and_then(|index| changed.get(index))
            .unwrap_or(source);
        let active = monitor(trigger, input.monitor)?.active();
        if settled && active {
            return Err(ContractError::InvalidTransition.into());
        }
        if trigger.is_terminal() || !active {
            break;
        }
    }
    Ok(transactions::Plan {
        rows: changed,
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}

pub(super) fn prior_binding(
    events: &[NativeFact],
    source: &ClaimState,
    visits: &mut usize,
) -> Result<(Binding, ClaimStatus), NativeError> {
    *visits = visits
        .checked_sub(events.len())
        .ok_or(NativeError::Capacity("monitor timer history visits"))?;
    let mut binding = source.binding();
    let mut status = source.status();
    for fact in events {
        if let NativeFact::Claim(event) = fact
            && event.after.object == source.binding().object
        {
            if event.before != Some(binding) || event.after != binding.next()? {
                return Err(ContractError::StaleRevision.into());
            }
            binding = event.after;
            status = event.status;
        }
    }
    Ok((binding, status))
}

/// Common history validation proves full claim revision chains. The separate
/// monitor-index checker proves all subscription writes. This check admits only
/// exact due timer provenance, real monitor releases, graph consequences and the
/// complete expiry-fence cohort; no actor report or testimony can impersonate it.
pub(super) fn check_journal(
    rows: &[ClaimState],
    extras: &mut Extras,
    view: &View<'_>,
    outcome: NativeOutcome,
    limits: NativeLimits,
    index_rows: usize,
) -> Result<(), NativeError> {
    use focal_model::lifecycle::claim::ClaimTerminalCut;
    let NativeInvocation::MonitorDeadline(key) = outcome.invocation else {
        return Err(ContractError::InvalidTransition.into());
    };
    let source = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    let input = NativeMonitorDeadlineInput {
        claim: key.claim,
        monitor: key.monitor,
        deadline: monitor(source, key.monitor)?.deadline(),
    };
    let resolved = resolve(view, input, outcome.logical_time)?;
    if input.key() != key
        || intent::monitor_deadline_fingerprint(view.ledger(), input)? != outcome.intent
        || outcome.operation != NativeOperation::MonitorDeadline
        || view.prefix().0.checked_add(1) != Some(outcome.sequence.0)
    {
        return Err(ContractError::InvalidCut.into());
    }
    let events = extras
        .journal
        .as_deref()
        .ok_or(ContractError::InvalidTransition)?;
    within(events.len(), limits.plan_edges)?;
    within(rows.len(), limits.plan_nodes)?;
    if resolved.inactive {
        return if rows.is_empty() && events.is_empty() && extras.rows.is_empty() && index_rows == 0
        {
            Ok(())
        } else {
            Err(ContractError::InvalidTransition.into())
        };
    }
    extras.rows.sort_unstable_by_key(|extra| extra.key);
    if extras
        .rows
        .windows(2)
        .any(|pair| pair.first().map(|row| row.key) == pair.get(1).map(|row| row.key))
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut visits = limits.plan_edges;
    let mut remaining = events.iter();
    let mut expired = false;
    let mut fenced = 0usize;
    let mut started = false;
    while let Some(fact) = remaining.next() {
        let ordinal = events
            .len()
            .checked_sub(remaining.len())
            .and_then(|n| n.checked_sub(1))
            .ok_or(ContractError::Capacity)?;
        let NativeFact::Claim(event) = fact else {
            return Err(ContractError::InvalidTransition.into());
        };
        visits = visits
            .checked_sub(rows.len())
            .ok_or(NativeError::Capacity("monitor timer history visits"))?;
        let row = rows
            .iter()
            .find(|row| row.binding().object == event.after.object)
            .ok_or(ContractError::InvalidTarget)?;
        let previous = view
            .claim(ClaimId(event.after.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        let prefix = events
            .get(..ordinal)
            .ok_or(ContractError::InvalidManifest)?;
        let (binding, status) = prior_binding(prefix, previous, &mut visits)?;
        if event.before != Some(binding)
            || event.after != binding.next()?
            || event.after.revision > row.binding().revision
            || event.after.content != row.binding().content
            || event.after.ledger != row.binding().ledger
            || event.owned_child.is_some()
        {
            return Err(ContractError::StaleRevision.into());
        }
        match event.kind {
            NativeEventKind::Monitor(NativeMonitorEvent::Released { id, cut }) => {
                let original = monitor(previous, id)?;
                let released = monitor(row, id)?;
                visits = visits
                    .checked_sub(prefix.len())
                    .ok_or(NativeError::Capacity("monitor timer history visits"))?;
                if !original.active()
                    || released.release_cut() != Some(cut)
                    || cut.position != outcome.sequence
                    || cut.cause != outcome.intent
                    || original.deadline() != released.deadline()
                    || original.registered() != released.registered()
                    || original.roots() != released.roots()
                    || original.last_rebinding() != released.last_rebinding()
                    || event.status != status
                    || prefix.iter().any(|fact| matches!(fact,
                        NativeFact::Claim(prior) if prior.after.object == event.after.object
                            && matches!(prior.kind, NativeEventKind::Monitor(change) if change.id() == id)))
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            NativeEventKind::Deadlocked => {
                let Some(ClaimTerminalCut::Graph(cut)) = row.terminal_cut() else {
                    return Err(ContractError::InvalidCut.into());
                };
                let (trigger_binding, trigger_status) = prior_binding(prefix, source, &mut visits)?;
                if status.is_terminal()
                    || trigger_status.is_terminal()
                    || event.status != ClaimStatus::Deadlocked
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
            NativeEventKind::Expired => {
                let Some(ClaimTerminalCut::Explicit(cut)) = row.terminal_cut() else {
                    return Err(ContractError::InvalidCut.into());
                };
                if expired
                    || status.is_terminal()
                    || row.binding().object.0 != key.claim.0
                    || event.status != ClaimStatus::Expired
                    || row.status() != ClaimStatus::Expired
                    || cut.position != outcome.sequence
                    || cut.cause != outcome.intent
                {
                    return Err(ContractError::InvalidCut.into());
                }
                expired = true;
                fenced = super::claim_deadlines::check_expiry_fences(
                    view,
                    row,
                    limits,
                    &extras.rows,
                    &mut remaining,
                )?;
            }
            NativeEventKind::Satisfied => {
                let Some(ClaimTerminalCut::Explicit(cut)) = row.terminal_cut() else {
                    return Err(ContractError::InvalidCut.into());
                };
                if !started
                    || status != ClaimStatus::Validating
                    || !previous.local_complete()
                    || event.status != ClaimStatus::Satisfied
                    || row.status() != ClaimStatus::Satisfied
                    || cut.position != outcome.sequence
                    || cut.cause == ContentHash([0; 32])
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            NativeEventKind::DependencyFailed => {
                let Some(ClaimTerminalCut::Graph(cut)) = row.terminal_cut() else {
                    return Err(ContractError::InvalidCut.into());
                };
                if !started
                    || status.is_terminal()
                    || event.status != ClaimStatus::DependencyFailed
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
    }
    let trigger = rows
        .iter()
        .find(|row| row.binding().object.0 == key.claim.0)
        .ok_or(ContractError::InvalidTransition)?;
    if !started
        || (!trigger.is_terminal() && monitor(trigger, key.monitor)?.active())
        || extras.rows.len()
            != index_rows
                .checked_add(fenced)
                .ok_or(ContractError::Capacity)?
    {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(())
}
