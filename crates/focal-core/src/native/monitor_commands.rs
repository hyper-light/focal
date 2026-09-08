//! Actor-authored monitor changes publish their retained subscriptions and real
//! graph consequences with the same request outcome and claim revision chain.
use super::prepare::{Extras, Scratch, add, heap, within};
use super::*;
use focal_model::lifecycle::claim::ClaimCut;

fn target(root: WaitPredicate) -> ClaimId {
    match root {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}

#[allow(clippy::too_many_arguments)] // Complete already-reserved native transaction context.
pub(super) fn prepare(
    command: NativeCommand,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let (expected, receipt) = match &command {
        NativeCommand::RegisterMonitor {
            expected, receipt, ..
        }
        | NativeCommand::RebindMonitor {
            expected, receipt, ..
        }
        | NativeCommand::CancelMonitor {
            expected, receipt, ..
        } => (*expected, *receipt),
        _ => return Err(ContractError::InvalidTransition.into()),
    };
    let source = view
        .claim(ClaimId(expected.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    source.binding().check(&expected)?;
    context.principal.require_actor(source.issuer())?;
    if receipt != source.receipt().map(|receipt| receipt.fence) {
        return Err(ContractError::StaleReceipt.into());
    }
    let mut additional = match &command {
        NativeCommand::RegisterMonitor { roots, .. } => {
            within(roots.len(), limits.plan_edges)?;
            let mut targets = scratch.reserve(roots.len())?;
            for root in roots {
                targets.push(target(*root));
            }
            targets
        }
        NativeCommand::RebindMonitor {
            predecessor,
            successor,
            ..
        } => {
            let mut targets = scratch.reserve(2)?;
            for binding in [predecessor, successor] {
                let id = ClaimId(binding.object.0);
                view.claim(id)
                    .ok_or(ContractError::InvalidTarget)?
                    .binding()
                    .check(binding)?;
                targets.push(id);
            }
            targets
        }
        _ => Vec::new(),
    };
    additional.sort_unstable();
    additional.dedup();
    let graph = graph_effects::monitor(view, source, &additional, cut, limits, scratch)?;
    let authority = scope::Authority {
        principal: context.principal,
        expected,
        receipt,
        cut,
        now: context.logical_time,
    };
    let build = scope::BuildLimits {
        bytes: scratch.remaining()?,
        visits: limits.plan_edges,
    };
    let transition = match &command {
        NativeCommand::RegisterMonitor {
            id,
            roots,
            deadline,
            ..
        } => scope::Registry::prepare_register_bounded(
            source,
            authority,
            scope::Registration {
                id: *id,
                roots,
                deadline: *deadline,
            },
            graph.snapshot(),
            graph.peers(),
            build,
        )?,
        NativeCommand::RebindMonitor {
            id,
            predecessor,
            successor,
            ..
        } => scope::Registry::prepare_rebind_bounded(
            source,
            authority,
            scope::RebindRequest {
                id: *id,
                predecessor: view
                    .claim(ClaimId(predecessor.object.0))
                    .ok_or(ContractError::InvalidTarget)?,
                successor: view
                    .claim(ClaimId(successor.object.0))
                    .ok_or(ContractError::InvalidTarget)?,
            },
            graph.snapshot(),
            graph.peers(),
            build,
        )?,
        NativeCommand::CancelMonitor { id, .. } => scope::Registry::prepare_cancel_monitor_bounded(
            source,
            authority,
            *id,
            graph.snapshot(),
            graph.peers(),
            build,
        )?,
        _ => return Err(ContractError::InvalidTransition.into()),
    };
    let charge = transition.construction_charge();
    scratch.charge(charge)?;
    let transition = transition.build()?;
    within(transition.construction_charge()?, charge)?;
    let event = transition.event();
    let copy = heap(source)?;
    scratch.charge(copy)?;
    let root = source.try_copy(source.retained_bytes()?)?;
    within(heap(&root)?, copy)?;
    let changed = graph.apply(root, transition)?;
    extras.begin_journal(limits.range.max_batch_entries, scratch)?;
    extras.record(NativeFact::Claim(NativeClaimEvent {
        graph: None,
        kind: NativeEventKind::Monitor(NativeMonitorEvent::from_scope(event)?),
        before: Some(source.binding()),
        after: changed.claim().binding(),
        owned_child: None,
        status: changed.claim().status(),
    }))?;
    let additions = monitor_index::stage(
        view,
        source,
        changed.claim(),
        event,
        limits,
        extras,
        scratch,
    )?;
    meta.monitors = add(meta.monitors, additions.monitors)?;
    meta.monitor_links = add(meta.monitor_links, additions.links)?;
    within(meta.monitors, limits.monitors)?;
    within(meta.monitor_links, limits.monitor_links)?;
    let rows = graph_effects::prepare_monitor(changed, limits, extras, scratch)?;
    Ok(transactions::Plan {
        rows,
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}

/// The claim journal separately checks every intermediate revision/status.
/// Match monitor evidence to its exact retained final field without allowing
/// cancellation to pass as successful predicate release.
pub(super) fn check_event(row: &ClaimState, event: NativeMonitorEvent) -> Result<(), NativeError> {
    let monitor = row
        .scopes()
        .monitor(event.id())
        .ok_or(ContractError::InvalidTarget)?;
    if event.cut().position.0 == 0 || event.cut().cause == ContentHash([0; 32]) {
        return Err(ContractError::InvalidCut.into());
    }
    let matches = match event {
        NativeMonitorEvent::Registered { cut, .. } => monitor.registered() == cut.position,
        NativeMonitorEvent::Rebound { change, .. } => monitor.last_rebinding() == Some(change),
        NativeMonitorEvent::Released { cut, .. } => monitor.release_cut() == Some(cut),
        NativeMonitorEvent::Cancelled { cancellation, .. } => {
            monitor.cancellation() == Some(cancellation)
        }
    };
    if !matches {
        return Err(ContractError::InvalidTransition.into());
    }
    Ok(())
}

pub(super) fn check_journal(
    rows: &[ClaimState],
    extras: &Extras,
    view: &View<'_>,
    outcome: NativeOutcome,
    limits: NativeLimits,
    index_rows: usize,
) -> Result<(), NativeError> {
    let NativeInvocation::Request(request) = outcome.invocation else {
        return Err(ContractError::InvalidTransition.into());
    };
    let journal = extras
        .journal
        .as_deref()
        .ok_or(ContractError::InvalidManifest)?;
    let Some(NativeFact::Claim(first)) = journal.first() else {
        return Err(ContractError::InvalidManifest.into());
    };
    let NativeEventKind::Monitor(change) = first.kind else {
        return Err(ContractError::InvalidTransition.into());
    };
    let source = view
        .claim(ClaimId(first.after.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    if request.principal != source.issuer()
        || first.before != Some(source.binding())
        || change.cut().position != outcome.sequence
        || change.cut().cause != outcome.intent
        || index_rows != extras.rows.len()
        || outcome.sequence.0
            != view
                .prefix()
                .0
                .checked_add(1)
                .ok_or(ContractError::Capacity)?
    {
        return Err(ContractError::InvalidCut.into());
    }
    match (outcome.operation, change) {
        (NativeOperation::RegisterMonitor, NativeMonitorEvent::Registered { .. })
        | (NativeOperation::RebindMonitor, NativeMonitorEvent::Rebound { .. })
        | (NativeOperation::CancelMonitor, NativeMonitorEvent::Cancelled { .. }) => {}
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    within(journal.len(), limits.events)?;
    let mut remaining = limits.plan_edges;
    for (ordinal, fact) in journal.iter().enumerate() {
        remaining = remaining
            .checked_sub(rows.len())
            .ok_or(NativeError::Capacity("monitor history visits"))?;
        let NativeFact::Claim(event) = fact else {
            return Err(ContractError::InvalidTransition.into());
        };
        let row = rows
            .iter()
            .find(|row| row.binding().object == event.after.object)
            .ok_or(ContractError::InvalidTarget)?;
        match event.kind {
            NativeEventKind::Monitor(change) => {
                if ordinal != 0 && !matches!(change, NativeMonitorEvent::Released { .. }) {
                    return Err(ContractError::InvalidTransition.into());
                }
                check_event(row, change)?;
                if change.cut().position != outcome.sequence {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            NativeEventKind::Satisfied | NativeEventKind::DependencyFailed if ordinal != 0 => {
                use focal_model::lifecycle::{claim::ClaimTerminalCut, graph};
                let valid = match (event.kind, row.terminal_cut()) {
                    (NativeEventKind::Satisfied, Some(ClaimTerminalCut::Explicit(cut))) => {
                        event.status == ClaimStatus::Satisfied
                            && row.local_complete()
                            && cut.position == outcome.sequence
                            && cut.cause != ContentHash([0; 32])
                    }
                    (NativeEventKind::DependencyFailed, Some(ClaimTerminalCut::Graph(cut))) => {
                        event.status == ClaimStatus::DependencyFailed
                            && cut.kind() == graph::FailureKind::DependencyFailed
                            && cut.sequence() == outcome.sequence
                            && cut.fingerprint() != ContentHash([0; 32])
                    }
                    _ => false,
                };
                if !valid {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            _ => return Err(ContractError::InvalidTransition.into()),
        }
    }
    Ok(())
}
