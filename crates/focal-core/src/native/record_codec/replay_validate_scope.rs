//! Original scope membership and record-local monitor transitions. Immutable
//! base rows provide the predecessor; no historical registry is reconstructed.
use super::*;
use focal_model::lifecycle::scope::{self, RegistrySnapshotSource};
use focal_model::lifecycle::{claim::ClaimTerminalCut, succession::CorrectionKind};
use focal_model::{Cause, ObjectRef};

fn target(value: WaitPredicate) -> ClaimId {
    match value {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
fn replace(value: WaitPredicate, before: ClaimId, after: ClaimId) -> WaitPredicate {
    match value {
        WaitPredicate::Satisfied(id) if id == before => WaitPredicate::Satisfied(after),
        WaitPredicate::Terminal(id) if id == before => WaitPredicate::Terminal(after),
        WaitPredicate::Released(id) if id == before => WaitPredicate::Released(after),
        _ => value,
    }
}
fn settled<O: Overlay>(
    value: WaitPredicate,
    read: &ReplayRead<'_, '_, O>,
) -> Result<bool, NativeError> {
    let claim = read.claim(target(value))?;
    Ok(match value {
        WaitPredicate::Satisfied(_) => claim.status() == ClaimStatus::Satisfied,
        WaitPredicate::Terminal(_) => claim.is_terminal(),
        WaitPredicate::Released(_) => claim.scopes().released(),
    })
}
fn cut(
    value: focal_model::lifecycle::claim::ClaimCut,
    sequence: SessionSeq,
) -> Result<(), NativeError> {
    require(value.position == sequence && value.cause.0 != [0; 32])
}

pub(super) fn validate<O: Overlay>(
    old: Option<&ClaimState>,
    next: &ClaimState,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let id = ClaimId(next.binding().object.0);
    let registry = next.scopes();
    let fields = registry.snapshot_v1().fields();
    let previous = old.map(|old| old.scopes().snapshot_v1().fields());
    let mut last_cut = previous.map_or(SessionSeq(0), |fields| fields.last_cut);
    if let Some(previous) = previous {
        require(
            fields.owner == previous.owner
                && fields.limits == previous.limits
                && fields.scopes >= previous.scopes
                && fields.children >= previous.children,
        )?;
        if let Some(released) = previous.released {
            require(fields.released == Some(released))?;
        }
    } else {
        require(fields.owner == next.lineage().binding())?;
    }
    read.charge(mul(add(fields.children, 1)?, 128)?)?;
    for child in registry.children() {
        let prior = old.and_then(|old| {
            old.scopes()
                .children()
                .binary_search_by_key(&child.id(), |row| row.id())
                .ok()
                .and_then(|index| old.scopes().children().get(index))
        });
        read.charge(const { (usize::BITS as usize + 1) * 16 })?;
        if let Some(prior) = prior {
            require(*prior == *child)?;
        } else {
            let event = read
                .index
                .child(read.encoded, child.id(), read.parsing, read.meter)?
                .ok_or_else(invalid)?;
            require(
                matches!(event.fact, NativeFact::Claim(NativeClaimEvent { kind: NativeEventKind::ChildRegistered,
                after, owned_child: Some(binding), .. }) if after.object.0 == id.0 && binding == child.binding()),
            )?;
            require(
                child.registered() == read.outcome.sequence
                    && read.before(Key::Claim(child.id()))?.is_none(),
            )?;
            let actual = read.claim(child.id())?;
            require(
                actual.lineage().cause() == &Cause::Claim(id)
                    && actual.created() == child.registered()
                    && actual.binding().content == child.binding().content,
            )?;
            last_cut = last_cut.max(child.registered());
        }
    }
    if let Some(old) = old {
        read.charge(mul(
            add(old.scopes().children().len(), 1)?,
            const { (usize::BITS as usize + 1) * 16 },
        )?)?;
        for child in old.scopes().children() {
            require(
                registry
                    .children()
                    .binary_search_by_key(&child.id(), |row| row.id())
                    .is_ok(),
            )?;
        }
        read.charge(mul(
            add(previous.ok_or_else(invalid)?.scopes, 1)?,
            const { (usize::BITS as usize + 1) * 16 },
        )?)?;
        for monitor in old.scopes().iter() {
            require(registry.monitor(monitor.id()).is_some())?;
        }
    }
    read.charge(add(fields.scopes, 1)?)?;
    for monitor in registry.iter() {
        read.charge(const { (usize::BITS as usize + 1) * 16 })?;
        let source = old.and_then(|old| old.scopes().monitor(monitor.id()));
        let at = monitor_step(id, source, monitor, read)?;
        if let Some(at) = at {
            last_cut = last_cut.max(at);
        }
    }
    let mut release = previous.and_then(|fields| fields.released);
    read.events(Key::Claim(id), |event| {
        read.charge(128)?;
        if let NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::OwnerReleased,
            ..
        }) = event.fact
        {
            require(release.is_none())?;
            let value = fields.released.ok_or_else(invalid)?;
            cut(value, event.sequence)?;
            require(next.is_terminal())?;
            read.charge(add(add(fields.children, fields.scopes)?, 1)?)?;
            for child in registry.children() {
                require(read.claim(child.id())?.scopes().released())?;
            }
            for scope in registry.iter() {
                require(!scope.active())?;
            }
            release = Some(value);
            last_cut = last_cut.max(value.position);
        }
        Ok(())
    })?;
    require(fields.released == release && fields.last_cut == last_cut)
}

fn monitor_step<O: Overlay>(
    owner: ClaimId,
    old: Option<&scope::Scope>,
    next: &scope::Scope,
    read: &ReplayRead<'_, '_, O>,
) -> Result<Option<SessionSeq>, NativeError> {
    if let Some(old) = old {
        require(old.registered() == next.registered() && old.deadline() == next.deadline())?;
    }
    let mut registered = old.is_some();
    let mut disposition = old.and_then(scope::Scope::disposition);
    let mut rebound = old.and_then(scope::Scope::last_rebinding);
    let mut replacement = None;
    let mut last = None;
    let mut last_ordinal = None;
    read.index
        .monitors(read.encoded, next.id(), read.parsing, read.meter, |event| {
            read.charge(256)?;
            let NativeFact::Claim(NativeClaimEvent {
                kind: NativeEventKind::Monitor(value),
                after,
                ..
            }) = event.fact
            else {
                return Err(invalid());
            };
            require(
                after.object.0 == owner.0
                    && last_ordinal.is_none_or(|ordinal| ordinal < event.ordinal),
            )?;
            last_ordinal = Some(event.ordinal);
            require(value.id() == next.id())?;
            cut(value.cut(), event.sequence)?;
            match value {
                NativeMonitorEvent::Registered { .. } => {
                    require(!registered && next.registered() == event.sequence)?;
                    registered = true;
                }
                NativeMonitorEvent::Rebound { change, .. } => {
                    require(registered && disposition.is_none() && replacement.is_none())?;
                    let previous = read.claim(change.predecessor)?;
                    let successor = read.claim(change.successor)?;
                    read.charge(add(successor.lineage().corrections().len(), 1)?)?;
                    require(
                        previous.issuer() == successor.issuer()
                            && previous.subject() == successor.subject()
                            && successor.lineage().corrections().iter().any(|correction| {
                                correction.kind == CorrectionKind::Supersedes
                                    && correction.predecessor
                                        == ObjectRef::claim(read.ledger, change.predecessor)
                            }),
                    )?;
                    replacement = Some(change);
                    rebound = Some(change);
                }
                NativeMonitorEvent::Released { cut, .. } => {
                    require(registered && disposition.is_none())?;
                    disposition = Some(scope::MonitorDisposition::Released(cut));
                }
                NativeMonitorEvent::Cancelled { cancellation, .. } => {
                    require(registered && disposition.is_none())?;
                    let claim = read.claim(owner)?;
                    require(
                        claim.terminal_cut().is_some_and(|terminal| {
                            let sequence = match terminal {
                                ClaimTerminalCut::Explicit(cut) => cut.position,
                                ClaimTerminalCut::Required(cut) => cut.sequence(),
                                ClaimTerminalCut::Graph(cut) => cut.sequence(),
                            };
                            sequence == cancellation.terminal
                        }) && cancellation.terminal <= event.sequence,
                    )?;
                    disposition = Some(scope::MonitorDisposition::Cancelled(cancellation));
                }
            }
            last = Some(event.sequence);
            Ok(())
        })?;
    require(registered && disposition == next.disposition() && rebound == next.last_rebinding())?;
    if let Some(old) = old {
        read.charge(mul(
            add(add(old.roots().len(), next.roots().len())?, 1)?,
            const { (usize::BITS as usize + 1) * 16 },
        )?)?;
        if let Some(change) = replacement {
            require(
                old.roots()
                    .iter()
                    .any(|root| target(*root) == change.predecessor),
            )?;
            for root in old.roots() {
                let mapped = replace(*root, change.predecessor, change.successor);
                require(next.roots().binary_search(&mapped).is_ok())?;
            }
            for root in next.roots() {
                let unchanged =
                    old.roots().binary_search(root).is_ok() && target(*root) != change.predecessor;
                let renamed = target(*root) == change.successor
                    && old
                        .roots()
                        .binary_search(&replace(*root, change.successor, change.predecessor))
                        .is_ok();
                require(unchanged || renamed)?;
            }
        } else {
            require(old.roots() == next.roots())?;
        }
    } else {
        require(replacement.is_none())?;
    }
    read.charge(add(next.roots().len(), 1)?)?;
    let mut all = true;
    for root in next.roots() {
        all &= settled(*root, read)?;
        if next.active() {
            require(
                matches!(read.require(Key::MonitorLink(target(*root), next.id()))?, Row::MonitorLink(Some(link)) if link.owner == owner),
            )?;
        }
    }
    // A new release requires its exact predicates; old releases remain frozen.
    // Affected active scopes must be quiescent after the captured fixed point.
    if matches!(
        next.disposition(),
        Some(scope::MonitorDisposition::Released(_))
    ) && old.is_none_or(|old| old.active())
    {
        require(all)?;
    }
    if next.active() {
        require(!all)?;
    }
    Ok(last)
}
