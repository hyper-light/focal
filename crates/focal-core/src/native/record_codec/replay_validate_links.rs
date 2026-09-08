use super::*;
use focal_model::lifecycle::graph::{Kind, Obligation};

#[derive(Default)]
pub(super) struct Counts {
    incoming: usize,
    declared: usize,
    work: usize,
    linked_work: usize,
    diagnostic: usize,
    linked_diagnostic: usize,
    retired: usize,
    linked_retired: usize,
}
fn count(value: &mut usize, amount: usize) -> Result<(), NativeError> {
    *value = add(*value, amount)?;
    Ok(())
}
fn target(value: WaitPredicate) -> ClaimId {
    match value {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
fn declares<O: Overlay>(
    claim: &ClaimState,
    target: ClaimId,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    read.charge(const { (usize::BITS as usize + 1) * 8 })?;
    let values = claim.graph().obligations();
    require(
        values
            .binary_search(&Obligation {
                kind: Kind::DependsOn,
                target,
            })
            .is_ok()
            || values
                .binary_search(&Obligation {
                    kind: Kind::Awaits,
                    target,
                })
                .is_ok(),
    )
}
impl Counts {
    pub(super) fn finish(self) -> Result<(), NativeError> {
        require(
            self.incoming == self.declared
                && self.work == self.linked_work
                && self.diagnostic == self.linked_diagnostic
                && self.retired == self.linked_retired,
        )
    }
}
pub(super) fn check<O: Overlay>(
    key: Key,
    value: &Row,
    read: &ReplayRead<'_, '_, O>,
    counts: &mut Counts,
) -> Result<(), NativeError> {
    read.charge(256)?;
    match (key, value) {
        (Key::IncomingHead(target), Row::IncomingHead(head)) => {
            let previous = match read.before(key)? {
                Some(Row::IncomingHead(head)) => *head,
                None => incoming_graph::IncomingHead::default(),
                _ => return Err(invalid()),
            };
            read.claim(target)?;
            let added = head.count.checked_sub(previous.count).ok_or_else(invalid)?;
            read.charge(add(added, 1)?)?;
            let mut next = head.head;
            for _ in 0..added {
                let id = next.ok_or_else(invalid)?;
                let key = Key::IncomingLink(target, id);
                require(read.before(key)?.is_none())?;
                let Row::IncomingLink(link) = read.require(key)? else {
                    return Err(invalid());
                };
                declares(read.claim(id)?, target, read)?;
                next = link.next;
            }
            require(next == previous.head)?;
            count(&mut counts.declared, added)?;
        }
        (Key::IncomingLink(target, id), Row::IncomingLink(_)) => {
            require(
                read.before(Key::Claim(id))?.is_none()
                    && read.changed(Key::IncomingHead(target))?,
            )?;
            declares(read.claim(id)?, target, read)?;
            count(&mut counts.incoming, 1)?;
        }
        (Key::Cycle(key), Row::Cycle(cycle)) => {
            let previous = match read.before(Key::Cycle(key))? {
                Some(Row::Cycle(value)) => *value,
                None => NativeCycle::default(),
                _ => return Err(invalid()),
            };
            let claim = read.claim(key.claim)?;
            let Row::Receipt(receipt) = read.require(Key::Receipt(key.receipt))? else {
                return Err(invalid());
            };
            require(
                receipt.claim == key.claim
                    && receipt.fence.epoch == key.epoch
                    && key.cycle <= claim.max_responses(),
            )?;
            let work = cycle
                .work_count
                .checked_sub(previous.work_count)
                .ok_or_else(invalid)?;
            let diagnostic = cycle
                .diagnostic_count
                .checked_sub(previous.diagnostic_count)
                .ok_or_else(invalid)?;
            if cycle.work_count > read.limits.work_artifacts_per_cycle
                || cycle.diagnostic_count > read.limits.diagnostics_per_cycle
            {
                return Err(ContractError::Capacity.into());
            }
            read.charge(add(add(work, diagnostic)?, 1)?)?;
            let mut next = cycle.work_head;
            for _ in 0..work {
                let id = next.ok_or_else(invalid)?;
                require(read.before(Key::Work(id))?.is_none())?;
                let value = as_work(Some(read.require(Key::Work(id))?)).ok_or_else(invalid)?;
                require(
                    value.state.claim() == key.claim
                        && value.state.receipt().receipt == key.receipt
                        && value.state.receipt().epoch == key.epoch
                        && value.state.cycle() == key.cycle,
                )?;
                next = value.next;
            }
            require(next == previous.work_head)?;
            let mut next = cycle.diagnostic_head;
            for _ in 0..diagnostic {
                let id = next.ok_or_else(invalid)?;
                require(read.before(Key::Diagnostic(id))?.is_none())?;
                let Row::Diagnostic(value) = read.require(Key::Diagnostic(id))? else {
                    return Err(invalid());
                };
                let value = value.get().ok_or_else(invalid)?;
                require(
                    value.diagnostic.claim() == key.claim
                        && value.diagnostic.receipt().receipt == key.receipt
                        && value.diagnostic.receipt().epoch == key.epoch
                        && value.diagnostic.cycle() == key.cycle,
                )?;
                next = value.next;
            }
            require(next == previous.diagnostic_head)?;
            if let Some(response) = previous.response {
                require(cycle.response == Some(response) && work == 0 && diagnostic == 0)?;
            }
            if let Some(response) = cycle.response {
                let value = response_reads::as_response_record(Some(
                    read.require(Key::Response(response))?,
                ))
                .ok_or_else(invalid)?
                .response();
                let identity = value.identity();
                require(
                    identity.claim == key.claim
                        && identity.receipt == receipt.fence
                        && identity.cycle == key.cycle,
                )?;
            }
            count(&mut counts.linked_work, work)?;
            count(&mut counts.linked_diagnostic, diagnostic)?;
        }
        (Key::Work(id), Row::Work(_)) if read.before(Key::Work(id))?.is_none() => {
            count(&mut counts.work, 1)?
        }
        (Key::Diagnostic(_), Row::Diagnostic(_)) => count(&mut counts.diagnostic, 1)?,
        (Key::RetiredCycleHead(claim), Row::RetiredCycleHead(head)) => {
            let previous = match read.before(key)? {
                Some(Row::RetiredCycleHead(value)) => *value,
                None => RetiredCycleHead {
                    head: None,
                    count: 0,
                    work_count: 0,
                },
                _ => return Err(invalid()),
            };
            let added = head.count.checked_sub(previous.count).ok_or_else(invalid)?;
            read.charge(add(added, 1)?)?;
            let mut next = head.head;
            let mut work = previous.work_count;
            for _ in 0..added {
                let key = next.ok_or_else(invalid)?;
                require(key.claim == claim && read.before(Key::RetiredCycle(key))?.is_none())?;
                let Row::RetiredCycle(value) = read.require(Key::RetiredCycle(key))? else {
                    return Err(invalid());
                };
                let Row::Cycle(cycle) = read.require(Key::Cycle(key))? else {
                    return Err(invalid());
                };
                require(
                    cycle.response.is_none()
                        && (cycle.work_count != 0 || cycle.diagnostic_count != 0),
                )?;
                work = add(work, cycle.work_count)?;
                next = value.next;
            }
            require(next == previous.head && work == head.work_count)?;
            count(&mut counts.linked_retired, added)?;
        }
        (Key::RetiredCycle(key), Row::RetiredCycle(value)) => {
            require(read.changed(Key::RetiredCycleHead(key.claim))?)?;
            let Row::Receipt(receipt) = read.require(Key::Receipt(key.receipt))? else {
                return Err(invalid());
            };
            let current = read.claim(key.claim)?.receipt().ok_or_else(invalid)?;
            require(
                receipt.claim == key.claim
                    && receipt.fence.epoch == key.epoch
                    && receipt.holder == value.holder
                    && current.fence.epoch > key.epoch,
            )?;
            count(&mut counts.retired, 1)?;
        }
        (Key::Monitor(id), Row::Monitor(value)) => {
            let claim = read.claim(ClaimId(value.owner.object.0))?;
            read.charge(const { (usize::BITS as usize + 1) * 8 })?;
            let scope = claim.scopes().monitor(id).ok_or_else(invalid)?;
            require(
                value.registered == read.outcome.sequence
                    && scope.registered() == value.registered
                    && scope.deadline() == value.deadline
                    && value.owner.content == claim.binding().content,
            )?;
            let mut found = false;
            read.index.monitors(read.encoded, id, read.parsing, read.meter, |event| {
                if matches!(event.fact, NativeFact::Claim(NativeClaimEvent { kind: NativeEventKind::Monitor(NativeMonitorEvent::Registered { id: actual, .. }), .. }) if actual == id) {
                    require(!found)?; found = true;
                }
                Ok(())
            })?;
            require(found)?;
        }
        (Key::MonitorHead(target), Row::MonitorHead(head)) => monitor_head(target, *head, read)?,
        (Key::MonitorLink(target, id), Row::MonitorLink(link)) => {
            monitor_link(target, id, *link, read)?
        }
        _ => (),
    }
    Ok(())
}
fn monitor_head<O: Overlay>(
    target: ClaimId,
    head: monitor_index::MonitorHead,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let old = match read.before(Key::MonitorHead(target))? {
        Some(Row::MonitorHead(value)) => *value,
        None => monitor_index::MonitorHead::default(),
        _ => return Err(invalid()),
    };
    let mut count = old.count;
    read.charge(const { (usize::BITS as usize + 1) * 64 })?;
    let mut changes = read
        .overlay
        .changes_from(Key::MonitorLink(target, MonitorId([0; 16])));
    loop {
        read.charge(128)?;
        let Some((key @ Key::MonitorLink(actual, _), Some(Row::MonitorLink(next)))) =
            changes.next()
        else {
            break;
        };
        if actual != target {
            break;
        }
        let before = match read.before(key)? {
            Some(Row::MonitorLink(value)) => value.is_some(),
            None => false,
            _ => return Err(invalid()),
        };
        match (before, next.is_some()) {
            (false, true) => count = add(count, 1)?,
            (true, false) => count = count.checked_sub(1).ok_or_else(invalid)?,
            _ => (),
        }
    }
    require(count == head.count && (count == 0) == head.head.is_none())?;
    if let Some(id) = head.head {
        require(
            matches!(read.require(Key::MonitorLink(target, id))?, Row::MonitorLink(Some(link)) if link.previous.is_none()),
        )?;
    }
    Ok(())
}
fn monitor_link<O: Overlay>(
    target_id: ClaimId,
    id: MonitorId,
    link: Option<monitor_index::MonitorLink>,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let old = match read.before(Key::MonitorLink(target_id, id))? {
        Some(Row::MonitorLink(value)) => *value,
        None => None,
        _ => return Err(invalid()),
    };
    if old.is_some() != link.is_some() {
        require(read.changed(Key::MonitorHead(target_id))?)?;
    }
    let Row::Monitor(allocation) = read.require(Key::Monitor(id))? else {
        return Err(invalid());
    };
    let owner = read.claim(ClaimId(allocation.owner.object.0))?;
    read.charge(const { (usize::BITS as usize + 1) * 8 })?;
    let scope = owner.scopes().monitor(id).ok_or_else(invalid)?;
    if let Some(link) = link {
        read.charge(add(scope.roots().len(), 1)?)?;
        require(
            scope.active()
                && scope.roots().iter().any(|root| target(*root) == target_id)
                && link.owner.0 == allocation.owner.object.0
                && link.registered == allocation.registered
                && link.stamp
                    == scope
                        .last_rebinding()
                        .map_or(scope.registered(), |change| change.cut.position),
        )?;
        if let Some(previous) = link.previous {
            require(
                previous != id
                    && matches!(read.require(Key::MonitorLink(target_id, previous))?, Row::MonitorLink(Some(value)) if value.next == Some(id)),
            )?;
        } else {
            require(
                matches!(read.require(Key::MonitorHead(target_id))?, Row::MonitorHead(head) if head.head == Some(id)),
            )?;
        }
        if let Some(next) = link.next {
            require(
                next != id
                    && matches!(read.require(Key::MonitorLink(target_id, next))?, Row::MonitorLink(Some(value)) if value.previous == Some(id)),
            )?;
        }
    } else if let Some(old) = old {
        read.charge(add(scope.roots().len(), 1)?)?;
        require(!scope.active() || !scope.roots().iter().any(|root| target(*root) == target_id))?;
        if let Some(previous) = old.previous {
            require(
                !matches!(read.get(Key::MonitorLink(target_id, previous))?, Some(Row::MonitorLink(Some(value))) if value.next == Some(id)),
            )?;
        }
        if let Some(next) = old.next {
            require(
                !matches!(read.get(Key::MonitorLink(target_id, next))?, Some(Row::MonitorLink(Some(value))) if value.previous == Some(id)),
            )?;
        }
    }
    Ok(())
}
