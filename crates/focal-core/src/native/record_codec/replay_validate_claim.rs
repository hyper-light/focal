//! Claim successor proof over an already validated predecessor. Immutable
//! authoring and prior histories are compared once; only this mutation's new
//! events may explain appended observations or advancing lifecycle facts.
use super::*;
use focal_model::lifecycle::claim::ClaimTerminalCut;
use focal_model::lifecycle::graph::{FailureKind, Kind};

fn same_identity(before: Binding, after: Binding) -> Result<(), NativeError> {
    require(
        before.ledger == after.ledger
            && before.object == after.object
            && before.content == after.content
            && before.revision <= after.revision,
    )
}
fn model(error: NativeError) -> ContractError {
    match error {
        NativeError::Contract(error) => error,
        NativeError::Memory(_) | NativeError::Capacity(_) => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}

fn immutable<O: Overlay>(
    old: &ClaimState,
    next: &ClaimState,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let before = old.snapshot_v1();
    let after = next.snapshot_v1();
    same_identity(before.binding, after.binding)?;
    require(
        before.issuer == after.issuer
            && before.subject == after.subject
            && before.created == after.created
            && before.max_responses == after.max_responses
            && before.deadline == after.deadline
            && (!before.local_complete || after.local_complete)
            && before
                .local_sealed_at
                .is_none_or(|cut| after.local_sealed_at == Some(cut))
            && before
                .terminal_cut
                .is_none_or(|cut| after.terminal_cut == Some(cut)),
    )?;
    let graph = old.graph().obligations();
    let corrections = old.lineage().corrections();
    read.charge(mul(add(add(graph.len(), corrections.len())?, 1)?, 256)?)?;
    require(
        graph == next.graph().obligations()
            && old.lineage().binding() == next.lineage().binding()
            && old.lineage().cause() == next.lineage().cause()
            && corrections == next.lineage().corrections(),
    )?;
    // The complete immutable acceptance fingerprint includes ordered slots,
    // checks and actual declaration semantic stamps. Price both full bodies.
    for policy in [old.acceptance(), next.acceptance()] {
        let mut units = add(policy.declarations().len(), 1)?;
        let mut slots = policy.slots();
        loop {
            read.charge(1)?;
            let Some(slot) = slots.next() else {
                break;
            };
            units = add(units, add(slot.checks.len(), 1)?)?;
        }
        read.charge(mul(units, 512)?)?;
    }
    require(old.acceptance().intent_fingerprint() == next.acceptance().intent_fingerprint())
}

fn expected_status(kind: NativeEventKind, prior: Option<ClaimStatus>, next: ClaimStatus) -> bool {
    match kind {
        NativeEventKind::Created => prior.is_none() && next == ClaimStatus::Generated,
        NativeEventKind::Posted => {
            prior == Some(ClaimStatus::Generated) && next == ClaimStatus::Posted
        }
        NativeEventKind::Received => {
            prior == Some(ClaimStatus::Posted) && next == ClaimStatus::Received
        }
        NativeEventKind::TestamentGenerated => {
            matches!(prior, Some(ClaimStatus::Received | ClaimStatus::Progressed))
                && next == ClaimStatus::TestamentGenerated
        }
        NativeEventKind::TestamentAcknowledged => {
            prior == Some(ClaimStatus::TestamentGenerated)
                && next == ClaimStatus::TestamentAcknowledged
        }
        NativeEventKind::Validating => {
            prior == Some(ClaimStatus::TestamentAcknowledged) && next == ClaimStatus::Validating
        }
        NativeEventKind::LocallyComplete => {
            prior == Some(ClaimStatus::Validating) && next == ClaimStatus::Validating
        }
        NativeEventKind::Satisfied => {
            prior == Some(ClaimStatus::Validating) && next == ClaimStatus::Satisfied
        }
        NativeEventKind::PostFailed => {
            prior == Some(ClaimStatus::Posted) && next == ClaimStatus::PostFailed
        }
        NativeEventKind::ValidationIncomplete => {
            prior == Some(ClaimStatus::Validating) && next == ClaimStatus::ValidationIncomplete
        }
        NativeEventKind::ValidationFailed => {
            prior == Some(ClaimStatus::Validating) && next == ClaimStatus::ValidationFailed
        }
        NativeEventKind::ValidationErrored => {
            prior == Some(ClaimStatus::Validating) && next == ClaimStatus::ValidationErrored
        }
        NativeEventKind::DependencyFailed => {
            prior.is_some_and(|status| !status.is_terminal())
                && next == ClaimStatus::DependencyFailed
        }
        NativeEventKind::Deadlocked => {
            prior.is_some_and(|status| !status.is_terminal()) && next == ClaimStatus::Deadlocked
        }
        NativeEventKind::Expired => {
            prior.is_some_and(|status| !status.is_terminal()) && next == ClaimStatus::Expired
        }
        NativeEventKind::Cancelled => {
            prior.is_some_and(|status| !status.is_terminal()) && next == ClaimStatus::Cancelled
        }
        NativeEventKind::Superseded => {
            prior.is_some_and(|status| !status.is_terminal()) && next == ClaimStatus::Superseded
        }
        NativeEventKind::ReceiptAdopted
        | NativeEventKind::ChildRegistered
        | NativeEventKind::OwnerReleased
        | NativeEventKind::Monitor(_)
        | NativeEventKind::ResponseObserved => prior == Some(next),
    }
}

fn cut_at(cut: ClaimTerminalCut) -> SessionSeq {
    match cut {
        ClaimTerminalCut::Explicit(value) => value.position,
        ClaimTerminalCut::Required(value) => value.sequence(),
        ClaimTerminalCut::Graph(value) => value.sequence(),
    }
}

#[derive(Default)]
struct Events {
    received: usize,
    adopted: usize,
    response: usize,
    local: bool,
    terminal: bool,
}

fn history<O: Overlay>(
    id: ClaimId,
    old: Option<&ClaimState>,
    next: &ClaimState,
    read: &ReplayRead<'_, '_, O>,
) -> Result<Events, NativeError> {
    let mut binding = old.map(ClaimState::binding);
    let mut status = old.map(ClaimState::status);
    let mut ordinal = None;
    let mut facts = Events::default();
    read.events(Key::Claim(id), |event| {
        read.charge(512)?;
        let NativeFact::Claim(value) = event.fact else {
            return Err(invalid());
        };
        require(
            value.after.ledger == read.ledger
                && value.after.object.0 == id.0
                && value.after.content == next.binding().content
                && expected_status(value.kind, status, value.status)
                && ordinal.is_none_or(|prior| prior < event.ordinal),
        )?;
        match binding {
            None => require(
                value.before.is_none()
                    && value.after.revision.0 == 1
                    && value.kind == NativeEventKind::Created
                    && next.created() == event.sequence,
            )?,
            Some(previous) => {
                require(value.before == Some(previous) && value.after == previous.next()?)?
            }
        }
        if value.kind == NativeEventKind::ChildRegistered {
            require(value.owned_child.is_some())?;
        } else {
            require(value.owned_child.is_none())?;
        }
        match value.kind {
            NativeEventKind::Received => facts.received = add(facts.received, 1)?,
            NativeEventKind::ReceiptAdopted => facts.adopted = add(facts.adopted, 1)?,
            NativeEventKind::TestamentGenerated
            | NativeEventKind::TestamentAcknowledged
            | NativeEventKind::ResponseObserved => facts.response = add(facts.response, 1)?,
            NativeEventKind::LocallyComplete => {
                require(!facts.local && old.is_none_or(|old| !old.local_complete()))?;
                facts.local = true;
            }
            _ => (),
        }
        if value.status.is_terminal() && status.is_none_or(|previous| !previous.is_terminal()) {
            require(
                !facts.terminal
                    && next
                        .terminal_cut()
                        .is_some_and(|cut| cut_at(cut) == event.sequence),
            )?;
            facts.terminal = true;
        }
        binding = Some(value.after);
        status = Some(value.status);
        ordinal = Some(event.ordinal);
        Ok(())
    })?;
    require(binding == Some(next.binding()) && status == Some(next.status()))?;
    if old.is_none() {
        require(ordinal.is_some())?;
    }
    let old_local = old.is_some_and(ClaimState::local_complete);
    require((!old_local && next.local_complete()) == facts.local)?;
    let old_seal = old.and_then(ClaimState::local_sealed_at);
    if old_seal.is_none() && (facts.local || facts.terminal) {
        require(next.local_sealed_at() == Some(read.outcome.sequence))?;
    } else {
        require(next.local_sealed_at() == old_seal)?;
    }
    require(
        (old.is_none_or(|old| old.terminal_cut().is_none()) && next.terminal_cut().is_some())
            == facts.terminal,
    )?;
    Ok(facts)
}

fn receipt<O: Overlay>(
    id: ClaimId,
    old: Option<&ClaimState>,
    next: &ClaimState,
    facts: &Events,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let before = old.and_then(ClaimState::receipt);
    let after = next.receipt();
    if before == after {
        return require(facts.received == 0 && facts.adopted == 0);
    }
    let actual = after.ok_or_else(invalid)?;
    let key = Key::Receipt(actual.fence.receipt);
    require(read.before(key)?.is_none())?;
    let Row::Receipt(row) = read.require(key)? else {
        return Err(invalid());
    };
    require(
        row.claim == id
            && row.fence == actual.fence
            && row.holder == actual.holder
            && row.acquired == read.outcome.sequence,
    )?;
    match (before, read.only(key)?.fact) {
        (
            None,
            NativeFact::Receipt {
                claim,
                fence,
                holder,
            },
        ) => {
            require(
                facts.received == 1
                    && facts.adopted == 0
                    && fence == actual.fence
                    && holder == actual.holder
                    && fence.epoch == 1,
            )?;
            same_identity(claim, next.binding())?;
        }
        (
            Some(before),
            NativeFact::ReceiptAdopted {
                claim,
                previous,
                replacement,
                ..
            },
        ) => {
            require(
                facts.received == 0
                    && facts.adopted == 1
                    && previous == before
                    && replacement == actual
                    && previous.fence.epoch.checked_add(1) == Some(actual.fence.epoch),
            )?;
            same_identity(claim, next.binding())?;
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

fn responses<O: Overlay>(
    id: ClaimId,
    old: Option<&ClaimState>,
    next: &ClaimState,
    facts: &Events,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let mut previous = old.map(ClaimState::response_snapshots_v1);
    let mut observed = false;
    let mut appended = 0usize;
    let mut values = next.response_snapshots_v1();
    if values.len() > read.limits.responses {
        return Err(ContractError::Capacity.into());
    }
    loop {
        read.charge(256)?;
        let Some(value) = values.next() else {
            break;
        };
        let before = previous.as_mut().and_then(Iterator::next);
        let record = response_reads::as_response_record(Some(
            read.require(Key::Response(value.link.testament))?,
        ))
        .ok_or_else(invalid)?;
        let response = record.response();
        require(
            response.identity().claim == id
                && next.recorded_response(response)? == (value.posted, value.received),
        )?;
        let mut generated = false;
        let mut posted = false;
        let mut received = false;
        if before != Some(value) {
            observed = true;
            read.events(Key::Response(value.link.testament), |event| {
                read.charge(128)?;
                let NativeFact::Response {
                    claim,
                    before,
                    after,
                    state,
                } = event.fact
                else {
                    return Err(invalid());
                };
                require(claim == id && after.object.0 == value.link.testament.0)?;
                match state {
                    ResponseState::Generated => {
                        require(!generated && before.is_none())?;
                        generated = true;
                    }
                    ResponseState::Posted => {
                        require(!posted)?;
                        posted = true;
                    }
                    ResponseState::Received => {
                        require(!received)?;
                        received = true;
                    }
                    _ => (),
                }
                Ok(())
            })?;
        }
        match before {
            Some(before) => {
                require(
                    before.link == value.link
                        && (!before.posted || value.posted)
                        && (!before.received || value.received)
                        && !generated
                        && (!before.posted && value.posted) == posted
                        && (!before.received && value.received) == received,
                )?;
            }
            None => {
                appended = add(appended, 1)?;
                require(
                    generated
                        && !value.posted
                        && !value.received
                        && !posted
                        && !received
                        && read.before(Key::Response(value.link.testament))?.is_none(),
                )?;
            }
        }
    }
    read.charge(1)?;
    require(
        previous.as_mut().and_then(Iterator::next).is_none()
            && appended <= 1
            && observed == (facts.response != 0),
    )?;
    Ok(())
}

fn registrations<O: Overlay>(
    id: ClaimId,
    old: Option<&OwnedClaim>,
    next: &OwnedClaim,
    adopted: bool,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let current = next.registrations().ok_or_else(invalid)?;
    let before = old
        .map(|old| old.registrations().ok_or_else(invalid))
        .transpose()?;
    let old_count = before.map_or(0, |old| old.rows().len());
    let rows = current.rows();
    require(rows.len() >= old_count && rows.len() <= read.limits.evaluations_per_claim)?;
    read.charge(mul(add(old_count, 1)?, 512)?)?;
    if let Some(before) = before {
        require(
            rows.get(..old_count) == Some(before.rows())
                && before.max_rows() == current.max_rows()
                && (!before.is_sealed() || current.is_sealed())
                && before
                    .snapshot_v1()
                    .sealed_at
                    .is_none_or(|at| current.snapshot_v1().sealed_at == Some(at)),
        )?;
        if adopted {
            require(current.snapshot_v1().claim == next.claim().ok_or_else(invalid)?.binding())?;
        } else {
            require(before.snapshot_v1().claim == current.snapshot_v1().claim)?;
        }
        if before.increment_targets_sealed() && !current.increment_targets_sealed() {
            require(adopted && !before.is_sealed())?;
        }
    }
    let appended = rows.get(old_count..).ok_or_else(invalid)?;
    read.charge(mul(add(appended.len(), 1)?, 512)?)?;
    for member in appended {
        let key = transactions::key_for_registered(id, *member);
        require(read.before(Key::Evaluation(key))?.is_none())?;
        let value = as_evaluation(Some(read.require(Key::Evaluation(key))?)).ok_or_else(invalid)?;
        require(
            value.target() == member.target()
                && value.receipt() == member.receipt()
                && value.generation() == member.generation(),
        )?;
        same_identity(member.binding(), value.binding())?;
        let first = read
            .index
            .first(read.encoded, Key::Evaluation(key), read.parsing, read.meter)?
            .ok_or_else(invalid)?;
        require(
            matches!(first.fact, NativeFact::Evaluation { kind: NativeEvaluationEventKind::Materialized, key: found,
            before: None, after, .. } if found == key && after == member.binding()),
        )?;
    }
    let mut registered = 0usize;
    read.index
        .registrations(read.encoded, id, read.parsing, read.meter, |event| {
            read.charge(256)?;
            let NativeFact::Registrations { claim } = event.fact else {
                return Err(invalid());
            };
            same_identity(claim, next.claim().ok_or_else(invalid)?.binding())?;
            registered = add(registered, 1)?;
            Ok(())
        })?;
    let changed = before.is_some_and(|old| old != current);
    // Materialization itself proves each appended membership; explicit target
    // seals carry Registrations, while receipt adoption has its own exact fact.
    let new_seal = current.is_sealed() && before.is_none_or(|old| !old.is_sealed())
        || current.increment_targets_sealed()
            && before.is_none_or(|old| !old.increment_targets_sealed());
    if new_seal {
        require(registered != 0)?;
    }
    if registered != 0 {
        require(changed || !appended.is_empty() || before.is_none() && new_seal)?;
    }
    Ok(())
}

fn timer<O: Overlay>(deadline: Deadline, read: &ReplayRead<'_, '_, O>) -> Result<(), NativeError> {
    let actual = match read.outcome.invocation {
        NativeInvocation::ClaimDeadline(key) => {
            require(key.timer == deadline.timer && key.generation == deadline.generation)?;
            read.claim(key.claim)?.deadline().ok_or_else(invalid)?
        }
        NativeInvocation::MonitorDeadline(key) => {
            require(key.timer == deadline.timer && key.generation == deadline.generation)?;
            let Row::Monitor(monitor) = read.require(Key::Monitor(key.monitor))? else {
                return Err(invalid());
            };
            require(monitor.owner.object.0 == key.claim.0)?;
            monitor.deadline
        }
        _ => return Err(invalid()),
    };
    require(actual == deadline && read.outcome.logical_time >= deadline.at)
}

fn terminal<O: Overlay>(
    old: Option<&ClaimState>,
    next: &ClaimState,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    if old.is_some_and(|old| old.terminal_cut().is_some()) {
        return Ok(());
    }
    let Some(cut) = next.terminal_cut() else {
        return Ok(());
    };
    require(cut_at(cut) == read.outcome.sequence)?;
    match cut {
        ClaimTerminalCut::Explicit(value) => {
            if next.status() != ClaimStatus::Satisfied {
                require(value.cause == read.outcome.intent)?;
            }
            match next.status() {
                ClaimStatus::Cancelled => {
                    require(read.outcome.operation == NativeOperation::Cancel)?
                }
                ClaimStatus::Superseded => {
                    require(read.outcome.operation == NativeOperation::Create)?
                }
                ClaimStatus::Satisfied => require(next.local_complete())?,
                ClaimStatus::Expired => {
                    let deadline = match read.outcome.invocation {
                        NativeInvocation::ClaimDeadline(key) => {
                            require(key.claim.0 == next.binding().object.0)?;
                            next.deadline().ok_or_else(invalid)?
                        }
                        NativeInvocation::MonitorDeadline(key) => {
                            require(key.claim.0 == next.binding().object.0)?;
                            let Row::Monitor(value) = read.require(Key::Monitor(key.monitor))?
                            else {
                                return Err(invalid());
                            };
                            value.deadline
                        }
                        _ => return Err(invalid()),
                    };
                    timer(deadline, read)?;
                }
                _ => return Err(invalid()),
            }
        }
        // Admission and whole-work projectors compare the complete canonical
        // Required decision, including exact original result publication cuts.
        ClaimTerminalCut::Required(_) => (),
        ClaimTerminalCut::Graph(value) => {
            let origin = value.origin();
            let id = ClaimId(origin.binding().object.0);
            let source = read.claim(id)?;
            same_identity(origin.binding(), source.binding())?;
            require(source.created() == origin.created())?;
            match value.kind() {
                FailureKind::DependencyFailed => {
                    require(
                        next.status() == ClaimStatus::DependencyFailed
                            && source.is_terminal()
                            && source.status() != ClaimStatus::Satisfied
                            && origin.terminal() <= value.sequence(),
                    )?;
                }
                FailureKind::Deadlocked => {
                    require(
                        next.status() == ClaimStatus::Deadlocked
                            && origin.terminal() == value.sequence()
                            && value.fired_at() == Some(read.outcome.logical_time),
                    )?;
                    timer(value.deadline().ok_or_else(invalid)?, read)?;
                }
            }
            // A direct source binding must be present in the validated base or
            // in the actual current event chain. A propagated origin may be
            // inherited unchanged from an already validated dependency cut.
            let previous = as_claim(read.before(Key::Claim(id))?);
            let mut witnessed = previous.is_some_and(|source| source.binding() == origin.binding());
            read.events(Key::Claim(id), |event| {
                read.charge(128)?;
                let NativeFact::Claim(event) = event.fact else {
                    return Err(invalid());
                };
                witnessed |=
                    event.after == origin.binding() || event.before == Some(origin.binding());
                Ok(())
            })?;
            if !witnessed {
                let mut obligations = next.graph().obligations().iter();
                loop {
                    read.charge(256)?;
                    let Some(edge) = obligations.next() else {
                        break;
                    };
                    if edge.kind != Kind::DependsOn {
                        continue;
                    }
                    for peer in [
                        as_claim(read.before(Key::Claim(edge.target))?),
                        Some(read.claim(edge.target)?),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if let Some(ClaimTerminalCut::Graph(cut)) = peer.terminal_cut() {
                            witnessed |= cut.kind() == FailureKind::DependencyFailed
                                && cut.origin() == origin;
                        }
                    }
                }
            }
            require(witnessed)?;
        }
    }
    Ok(())
}

pub(super) fn validate<O: Overlay>(
    id: ClaimId,
    owned: &OwnedClaim,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    read.charge(512)?;
    let next = owned.claim().ok_or_else(invalid)?;
    let old = match read.before(Key::Claim(id))? {
        Some(Row::Claim(value)) => Some(value),
        None => None,
        _ => return Err(invalid()),
    };
    let source = old.map(|old| old.claim().ok_or_else(invalid)).transpose()?;
    require(
        next.binding().ledger == read.ledger
            && next.binding().object.0 == id.0
            && next.created() <= read.outcome.sequence,
    )?;
    if read.profile == NativeContentProfile::AuthoredV1 {
        let Row::ClaimContent(content) = read.require(Key::ClaimContent(id))? else {
            return Err(invalid());
        };
        let body = content.get().ok_or_else(invalid)?;
        let profile = content.profile().ok_or_else(invalid)?;
        // This checker has no lookup callbacks. The exclusive model loan
        // therefore cannot overlap the source/lookup meter's allowance.
        read.meter.budget(|visits| {
            authored::check_recorded_claim(body, profile, next, visits).map_err(model)
        })?;
    }
    if let Some(source) = source {
        immutable(source, next, read)?;
    }
    let events = history(id, source, next, read)?;
    receipt(id, source, next, &events, read)?;
    responses(id, source, next, &events, read)?;
    registrations(id, old, owned, events.adopted != 0, read)?;
    scopes::validate(source, next, read)?;
    terminal(source, next, read)?;
    super::super::replay_projection::validate(read, owned)
}
