//! Explicit release of terminal owned work. The owner gathers complete source
//! history and applies a model release capability before deriving graph effects.
//! Release never stands in for cancellation, testimony or an evaluator report.
use super::prepare::{Extras, Scratch, heap, within};
use super::*;
use focal_model::lifecycle::{
    claim::{ClaimCut, ClaimTerminalCut},
    graph, scope,
};

pub(super) fn prepare(
    expected: Binding,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let source = view
        .claim(ClaimId(expected.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    source.binding().check(&expected)?;
    context.principal.require_actor(source.issuer())?;
    if !source.is_terminal() || source.scopes().released() {
        return Err(ContractError::InvalidTransition.into());
    }
    let plan = graph_effects::owner_release(view, source, cut, limits, scratch)?;
    let release = scope::Registry::prepare_release_owner_bounded(
        source,
        plan.snapshot(),
        plan.peers(),
        cut,
        scratch.remaining()?,
        limits.plan_edges,
    )?;
    let charge = release.construction_charge();
    scratch.charge(charge)?;
    let transition = release.build()?;
    within(transition.construction_charge()?, charge)?;
    let copied = heap(source)?;
    scratch.charge(copied)?;
    let root = source.try_copy(source.retained_bytes()?)?;
    within(heap(&root)?, copied)?;
    let released = plan.apply(root, transition)?;
    extras.begin_journal(limits.range.max_batch_entries, scratch)?;
    extras.record(NativeFact::Claim(NativeClaimEvent {
        kind: NativeEventKind::OwnerReleased,
        owned_child: None,
        before: Some(source.binding()),
        after: released.claim().binding(),
        status: released.claim().status(),
    }))?;
    let rows = graph_effects::prepare_released(released, limits, extras, scratch)?;
    Ok(transactions::Plan {
        rows,
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}

/// Validate the complete original claim-only journal before the independently
/// checked cohort suffix. The initiating release must remain first and unique;
/// any subsequent terminal consequence retains its own checked graph cut.
#[cfg(test)]
pub(super) fn check_journal(
    rows: &[ClaimState],
    extras: &Extras,
    view: &View<'_>,
    outcome: NativeOutcome,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    check_journal_with_monitors(rows, extras, view, outcome, limits, 0)
}

pub(super) fn check_journal_with_monitors(
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
        .ok_or(ContractError::InvalidTransition)?;
    within(rows.len(), limits.plan_nodes)?;
    within(journal.len(), limits.events)?;
    if outcome.operation != NativeOperation::ReleaseScope
        || rows.is_empty()
        || extras.rows.len() != index_rows
        || journal.len() < rows.len()
        || outcome.sequence.0
            != view
                .prefix()
                .0
                .checked_add(1)
                .ok_or(ContractError::Capacity)?
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut remaining = limits.plan_edges;
    for (ordinal, fact) in journal.iter().enumerate() {
        let visits = rows
            .len()
            .checked_add(ordinal)
            .ok_or(NativeError::Capacity("scope release history visits"))?;
        remaining = remaining
            .checked_sub(visits)
            .ok_or(NativeError::Capacity("scope release history visits"))?;
        let NativeFact::Claim(event) = fact else {
            return Err(ContractError::InvalidTransition.into());
        };
        let monitor_release = matches!(
            event.kind,
            NativeEventKind::Monitor(NativeMonitorEvent::Released { .. })
        );
        if !monitor_release
            && journal.iter().take(ordinal).any(|earlier| {
                matches!(earlier, NativeFact::Claim(previous)
                if previous.after.object == event.after.object
                && !matches!(previous.kind, NativeEventKind::Monitor(_)))
            })
        {
            return Err(ContractError::InvalidManifest.into());
        }
        // The common claim journal proves every per-object revision chain and
        // its final row. This checker verifies the initiating authority and each
        // subsequent terminal cut; index replay proves scope dispositions.
        let mut row = None;
        for candidate in rows {
            if candidate.binding().object == event.after.object && row.replace(candidate).is_some()
            {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        let row = row.ok_or(ContractError::InvalidTarget)?;
        let previous = view
            .claim(ClaimId(event.after.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        remaining = remaining
            .checked_sub(ordinal)
            .ok_or(NativeError::Capacity("scope release history visits"))?;
        let preceding = journal
            .iter()
            .take(ordinal)
            .rev()
            .find_map(|fact| match fact {
                NativeFact::Claim(prior) if prior.after.object == event.after.object => Some(prior),
                _ => None,
            });
        let before = preceding.map_or(previous.binding(), |event| event.after);
        let status = preceding.map_or(previous.status(), |event| event.status);
        if event.before != Some(before)
            || event.after != before.next()?
            || (monitor_release && event.status != status)
            || (!monitor_release && event.status != row.status())
            || event.owned_child.is_some()
        {
            return Err(ContractError::StaleRevision.into());
        }
        if ordinal == 0 {
            let cut = ClaimCut {
                position: outcome.sequence,
                cause: outcome.intent,
            };
            let input = NativeInput {
                request,
                command: NativeCommand::ReleaseScope {
                    expected: previous.binding(),
                },
            };
            if event.kind != NativeEventKind::OwnerReleased
                || event.before != Some(previous.binding())
                || event.after != previous.binding().next()?
                || event.after != row.binding()
                || !previous.is_terminal()
                || previous.scopes().released()
                || row.scopes().release_cut() != Some(cut)
                || previous.status() != row.status()
                || previous.terminal_cut() != row.terminal_cut()
                || previous.local_sealed_at() != row.local_sealed_at()
                || previous.local_complete() != row.local_complete()
                || previous.receipt() != row.receipt()
                || previous.latest_response() != row.latest_response()
                || previous.response_count() != row.response_count()
                || request.principal != previous.issuer()
                || intent::fingerprint(view.ledger(), &input)? != outcome.intent
            {
                return Err(ContractError::InvalidTransition.into());
            }
        } else if let NativeEventKind::Monitor(change @ NativeMonitorEvent::Released { .. }) =
            event.kind
        {
            super::monitor_commands::check_event(row, change)?;
            if change.cut().position != outcome.sequence {
                return Err(ContractError::InvalidCut.into());
            }
        } else {
            if previous.is_terminal() || !row.is_terminal() {
                return Err(ContractError::InvalidTransition.into());
            }
            match (event.kind, row.terminal_cut()) {
                (NativeEventKind::DependencyFailed, Some(ClaimTerminalCut::Graph(cut)))
                    if row.status() == ClaimStatus::DependencyFailed
                        && cut.kind() == graph::FailureKind::DependencyFailed
                        && cut.sequence() == outcome.sequence
                        && cut.fingerprint() != ContentHash([0; 32]) => {}
                (NativeEventKind::Satisfied, Some(ClaimTerminalCut::Explicit(cut)))
                    if previous.status() == ClaimStatus::Validating
                        && previous.local_complete()
                        && row.status() == ClaimStatus::Satisfied
                        && cut.position == outcome.sequence
                        && cut.cause != ContentHash([0; 32]) => {}
                _ => return Err(ContractError::InvalidTransition.into()),
            }
        }
    }
    Ok(())
}
