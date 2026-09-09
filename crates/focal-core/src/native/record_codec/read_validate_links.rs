//! Complete direct index chains, traversed once per owning head.
use super::super::read_validate_evidence::HistoryIndex;
use super::*;
use focal_model::lifecycle::graph::{Kind, Obligation};

pub(super) fn target(value: WaitPredicate) -> ClaimId {
    match value {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
pub(super) fn incoming(
    target: ClaimId,
    head: incoming_graph::IncomingHead,
    read: &ValidationRead<'_, '_>,
) -> Result<usize, NativeError> {
    read.claim(target)?;
    if head.count > read.limits.claims {
        return Err(ContractError::Capacity.into());
    }
    read.charge(sum(head.count, 1)?)?;
    let mut next = head.head;
    for _ in 0..head.count {
        let dependent = next.ok_or_else(invalid)?;
        declares(read.claim(dependent)?, target, read)?;
        let Row::IncomingLink(link) = read.require(Key::IncomingLink(target, dependent))? else {
            return Err(invalid());
        };
        next = link.next;
    }
    if next.is_some() {
        return Err(invalid());
    }
    Ok(head.count)
}
fn declares(
    claim: &ClaimState,
    target: ClaimId,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    read.charge(const { (usize::BITS as usize + 1) * 4 })?;
    let declarations = claim.graph().obligations();
    if declarations
        .binary_search(&Obligation {
            kind: Kind::DependsOn,
            target,
        })
        .is_err()
        && declarations
            .binary_search(&Obligation {
                kind: Kind::Awaits,
                target,
            })
            .is_err()
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn monitors(
    target: ClaimId,
    head: monitor_index::MonitorHead,
    read: &ValidationRead<'_, '_>,
) -> Result<usize, NativeError> {
    read.claim(target)?;
    if head.count > read.limits.monitors || head.count > read.limits.monitor_links {
        return Err(ContractError::Capacity.into());
    }
    read.charge(sum(head.count, 1)?)?;
    let (mut next, mut previous) = (head.head, None);
    for _ in 0..head.count {
        let id = next.ok_or_else(invalid)?;
        let Row::MonitorLink(Some(link)) = read.require(Key::MonitorLink(target, id))? else {
            return Err(invalid());
        };
        if link.previous != previous {
            return Err(invalid());
        }
        monitor_link(target, id, *link, read)?;
        previous = Some(id);
        next = link.next;
    }
    if next.is_some() {
        return Err(invalid());
    }
    Ok(head.count)
}
fn monitor_link(
    target: ClaimId,
    id: MonitorId,
    link: monitor_index::MonitorLink,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let Row::Monitor(allocation) = read.require(Key::Monitor(id))? else {
        return Err(invalid());
    };
    let owner = read.claim(link.owner)?;
    read.charge(const { (usize::BITS as usize + 1) * 4 })?;
    let scope = owner.scopes().monitor(id).ok_or_else(invalid)?;
    if !scope.active()
        || allocation.owner.object.0 != link.owner.0
        || allocation.owner.ledger != read.ledger
        || allocation.owner.content != owner.binding().content
        || allocation.owner.revision > owner.binding().revision
        || allocation.registered != link.registered
        || link.registered != scope.registered()
        || link.stamp
            != scope
                .last_rebinding()
                .map_or(scope.registered(), |change| change.cut.position)
        || link.stamp > read.prefix
        || allocation.deadline != scope.deadline()
    {
        return Err(invalid());
    }
    read.charge(sum(scope.roots().len(), 1)?)?;
    if !scope
        .roots()
        .iter()
        .any(|root| self::target(*root) == target)
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn retired(
    claim_id: ClaimId,
    head: RetiredCycleHead,
    read: &ValidationRead<'_, '_>,
) -> Result<usize, NativeError> {
    let claim = read.claim(claim_id)?;
    let receipt = claim.receipt().ok_or_else(invalid)?;
    if head.count > read.limits.receipts {
        return Err(ContractError::Capacity.into());
    }
    read.charge(sum(head.count, 1)?)?;
    let (mut next, mut epoch, mut cycle, mut works) = (
        head.head,
        receipt.fence.epoch,
        claim.max_responses(),
        0usize,
    );
    for _ in 0..head.count {
        let key = next.ok_or_else(invalid)?;
        if key.claim != claim_id || key.epoch >= epoch || key.cycle > cycle {
            return Err(invalid());
        }
        let Row::RetiredCycle(link) = read.require(Key::RetiredCycle(key))? else {
            return Err(invalid());
        };
        let Row::Cycle(value) = read.require(Key::Cycle(key))? else {
            return Err(invalid());
        };
        if value.response.is_some() || value.work_count == 0 && value.diagnostic_count == 0 {
            return Err(invalid());
        }
        works = sum(works, value.work_count)?;
        next = link.next;
        epoch = key.epoch;
        cycle = key.cycle;
    }
    if next.is_some() || works != head.work_count {
        return Err(invalid());
    }
    Ok(head.count)
}
pub(super) fn cycle(
    key: NativeCycleKey,
    value: NativeCycle,
    read: &ValidationRead<'_, '_>,
) -> Result<(usize, usize), NativeError> {
    let owner = read.claim(key.claim)?;
    let Row::Receipt(receipt) = read.require(Key::Receipt(key.receipt))? else {
        return Err(invalid());
    };
    if receipt.claim != key.claim
        || receipt.fence.epoch != key.epoch
        || key.cycle > owner.max_responses()
        || value.work_count > read.limits.work_artifacts_per_cycle
        || value.diagnostic_count > read.limits.diagnostics_per_cycle
    {
        return Err(invalid());
    }
    if let Some(id) = value.response {
        let Row::Response(response) = read.require(Key::Response(id))? else {
            return Err(invalid());
        };
        let identity = response.get().ok_or_else(invalid)?.identity();
        if identity.claim != key.claim
            || identity.receipt != receipt.fence
            || identity.cycle != key.cycle
        {
            return Err(invalid());
        }
    } else {
        let current = owner.receipt().ok_or_else(invalid)?;
        if receipt.fence == current.fence {
            if u32::try_from(owner.response_count())
                .map_err(|_| ContractError::Capacity)?
                .checked_add(1)
                != Some(key.cycle)
                || current.holder != receipt.holder
            {
                return Err(invalid());
            }
        } else if !matches!(read.require(Key::RetiredCycle(key))?, Row::RetiredCycle(_)) {
            return Err(invalid());
        }
    }
    read.charge(sum(sum(value.work_count, value.diagnostic_count)?, 2)?)?;
    let mut next = value.work_head;
    for _ in 0..value.work_count {
        let id = next.ok_or_else(invalid)?;
        let Row::Work(work) = read.require(Key::Work(id))? else {
            return Err(invalid());
        };
        let work = work.get().ok_or_else(invalid)?;
        let state = &work.state;
        if state.binding().object.0 != id.0
            || state.claim() != key.claim
            || state.receipt() != receipt.fence
            || state.cycle() != key.cycle
            || state.producer() != receipt.holder
        {
            return Err(invalid());
        }
        next = work.next;
    }
    if next.is_some() {
        return Err(invalid());
    }
    next = value.diagnostic_head;
    for _ in 0..value.diagnostic_count {
        let id = next.ok_or_else(invalid)?;
        let Row::Diagnostic(diagnostic) = read.require(Key::Diagnostic(id))? else {
            return Err(invalid());
        };
        let diagnostic = diagnostic.get().ok_or_else(invalid)?;
        let state = diagnostic.diagnostic;
        if state.artifact().id != id
            || state.claim() != key.claim
            || state.receipt() != receipt.fence
            || state.cycle() != key.cycle
            || state.producer() != receipt.holder
        {
            return Err(invalid());
        }
        next = diagnostic.next;
    }
    if next.is_some() {
        return Err(invalid());
    }
    Ok((value.work_count, value.diagnostic_count))
}
pub(super) fn row(
    key: Key,
    row: &Row,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    match (key, row) {
        (Key::IncomingLink(target, dependent), Row::IncomingLink(_)) => {
            read.claim(target)?;
            declares(read.claim(dependent)?, target, read)?;
        }
        (Key::Monitor(id), Row::Monitor(value)) => {
            let owner = read.claim(ClaimId(value.owner.object.0))?;
            read.charge(const { (usize::BITS as usize + 1) * 4 })?;
            let scope = owner.scopes().monitor(id).ok_or_else(invalid)?;
            if value.owner.ledger != read.ledger
                || value.owner.content != owner.binding().content
                || value.owner.revision > owner.binding().revision
                || value.registered != scope.registered()
                || value.deadline != scope.deadline()
                || value.registered > read.prefix
            {
                return Err(invalid());
            }
            monitor_history(owner, scope, history, read)?;
        }
        (Key::MonitorLink(target, id), Row::MonitorLink(value)) => {
            read.claim(target)?;
            if let Some(link) = value {
                monitor_link(target, id, *link, read)?;
            } else if !matches!(read.require(Key::Monitor(id))?, Row::Monitor(_)) {
                return Err(invalid());
            }
        }
        (Key::Receipt(id), Row::Receipt(value)) => {
            let claim = read.claim(value.claim)?;
            if value.acquired > read.prefix
                || value.acquired < claim.created()
                || claim
                    .receipt()
                    .is_none_or(|current| current.fence.epoch < value.fence.epoch)
            {
                return Err(invalid());
            }
            let mut found = false;
            history.events(Key::Receipt(id), read, |event| {
                read.charge(64)?;
                if found || event.sequence != value.acquired {
                    return Err(invalid());
                }
                let (binding, fence, holder) = match event.fact {
                    NativeFact::Receipt {
                        claim,
                        fence,
                        holder,
                    } => {
                        if fence.epoch != 1 && event.invocation != NativeInvocation::Import {
                            return Err(invalid());
                        }
                        (claim, fence, holder)
                    }
                    NativeFact::ReceiptAdopted {
                        claim,
                        previous,
                        replacement,
                        ..
                    } => {
                        let Row::Receipt(old) =
                            read.require(Key::Receipt(previous.fence.receipt))?
                        else {
                            return Err(invalid());
                        };
                        if old.claim != value.claim
                            || old.fence != previous.fence
                            || old.holder != previous.holder
                            || old.acquired >= value.acquired
                            || old.fence.epoch.checked_add(1) != Some(replacement.fence.epoch)
                        {
                            return Err(invalid());
                        }
                        (claim, replacement.fence, replacement.holder)
                    }
                    _ => return Err(invalid()),
                };
                if binding.object.0 != value.claim.0
                    || binding.content != claim.binding().content
                    || fence != value.fence
                    || holder != value.holder
                {
                    return Err(invalid());
                }
                let recorded = history
                    .revision(Key::Claim(value.claim), binding.revision.0, read)?
                    .ok_or_else(invalid)?;
                let NativeFact::Claim(recorded_claim) = recorded.fact else {
                    return Err(invalid());
                };
                if recorded.sequence != event.sequence
                    || recorded_claim.after != binding
                    || !(matches!(
                        recorded_claim.kind,
                        NativeEventKind::Received | NativeEventKind::ReceiptAdopted
                    ) || event.invocation == NativeInvocation::Import
                        && matches!(recorded_claim.kind, NativeEventKind::Imported(_)))
                {
                    return Err(invalid());
                }
                found = true;
                Ok(())
            })?;
            if !found {
                return Err(invalid());
            }
        }
        (Key::RetiredCycle(key), Row::RetiredCycle(link)) => {
            let Row::Receipt(receipt) = read.require(Key::Receipt(key.receipt))? else {
                return Err(invalid());
            };
            let Row::Cycle(cycle) = read.require(Key::Cycle(key))? else {
                return Err(invalid());
            };
            if receipt.claim != key.claim
                || receipt.fence.epoch != key.epoch
                || receipt.holder != link.holder
                || cycle.response.is_some()
                || cycle.work_count == 0 && cycle.diagnostic_count == 0
            {
                return Err(invalid());
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

/// Retained facts prove the observable independent scope history. Historical
/// root sets and prior SCC graphs are not present in a full-root checkpoint;
/// their original authorization/fingerprints require the enclosing trusted
/// checkpoint provenance, not a newly inferred graph or a checksum assertion.
fn monitor_history(
    owner: &ClaimState,
    scope: &scope::Scope,
    history: &HistoryIndex,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let (mut registered, mut rebound, mut disposition, mut position) = (false, None, None, None);
    history.monitors(scope.id(), read, |event| {
        read.charge(128)?;
        let NativeFact::Claim(claim) = event.fact else {
            return Err(invalid());
        };
        let NativeEventKind::Monitor(monitor) = claim.kind else {
            return Err(invalid());
        };
        let cut = monitor.cut();
        let at = (event.sequence, event.ordinal);
        if claim.after.object != owner.binding().object
            || monitor.id() != scope.id()
            || cut.position != event.sequence
            || cut.cause.0 == [0; 32]
            || position.is_some_and(|prior| prior >= at)
            || disposition.is_some()
        {
            return Err(invalid());
        }
        match monitor {
            NativeMonitorEvent::Registered { .. } => {
                if registered || scope.registered() != event.sequence {
                    return Err(invalid());
                }
                registered = true;
            }
            NativeMonitorEvent::Rebound { change, .. } => {
                if !registered || change.predecessor == change.successor {
                    return Err(invalid());
                }
                read.claim(change.predecessor)?;
                let successor = read.claim(change.successor)?;
                read.charge(sum(successor.lineage().corrections().len(), 1)?)?;
                if !successor.lineage().corrections().iter().any(|correction| {
                    correction.kind
                        == focal_model::lifecycle::succession::CorrectionKind::Supersedes
                        && correction.predecessor.ledger == read.ledger
                        && correction.predecessor.id.0 == change.predecessor.0
                }) {
                    return Err(invalid());
                }
                rebound = Some(change);
            }
            NativeMonitorEvent::Released { cut, .. } => {
                if !registered {
                    return Err(invalid());
                }
                disposition = Some(scope::MonitorDisposition::Released(cut));
            }
            NativeMonitorEvent::Cancelled { cancellation, .. } => {
                if !registered
                    || cancellation.terminal.0 == 0
                    || cancellation.terminal > event.sequence
                {
                    return Err(invalid());
                }
                let terminal = history
                    .at_or_before(
                        Key::Claim(ClaimId(owner.binding().object.0)),
                        cancellation.terminal,
                        read,
                    )?
                    .ok_or_else(invalid)?;
                let NativeFact::Claim(terminal) = terminal.fact else {
                    return Err(invalid());
                };
                if !terminal.status.is_terminal() {
                    return Err(invalid());
                }
                disposition = Some(scope::MonitorDisposition::Cancelled(cancellation));
            }
        }
        position = Some(at);
        Ok(())
    })?;
    if !registered || rebound != scope.last_rebinding() || disposition != scope.disposition() {
        return Err(invalid());
    }
    if let Some(cut) = scope.release_cut() {
        read.charge(sum(scope.roots().len(), 1)?)?;
        for root in scope.roots() {
            let id = target(*root);
            let actual = read.claim(id)?;
            let event = history
                .at_or_before(Key::Claim(id), cut.position, read)?
                .ok_or_else(invalid)?;
            let NativeFact::Claim(claim) = event.fact else {
                return Err(invalid());
            };
            let satisfied = match root {
                WaitPredicate::Satisfied(_) => claim.status == ClaimStatus::Satisfied,
                WaitPredicate::Terminal(_) => claim.status.is_terminal(),
                WaitPredicate::Released(_) => actual
                    .scopes()
                    .release_cut()
                    .is_some_and(|released| released.position <= cut.position),
            };
            if !satisfied {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
