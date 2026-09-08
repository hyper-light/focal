use super::super::read_validate_attempts::AttemptCursor;
use super::*;
use focal_model::lifecycle::aggregation::PublicationPosition;
use focal_model::lifecycle::audit::ResultTestamentState;

fn step(
    previous: Option<Binding>,
    before: Option<Binding>,
    after: Binding,
) -> Result<(), NativeError> {
    match previous {
        Some(previous) => require(before == Some(previous) && previous.next()? == after),
        None => require(before.is_none() && after.revision.0 != 0),
    }
}
fn position(event: NativeEvent) -> PublicationPosition {
    PublicationPosition {
        sequence: event.sequence,
        ordinal: event.ordinal,
    }
}
fn advance(previous: &mut Option<u32>, event: NativeEvent) -> Result<(), NativeError> {
    require(previous.is_none_or(|old| old < event.ordinal))?;
    *previous = Some(event.ordinal);
    Ok(())
}

pub(super) fn work<O: Overlay>(
    id: ArtifactId,
    next: &NativeWork,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let old = as_work(read.before(Key::Work(id))?);
    let value = &next.state;
    let Row::Receipt(receipt) = read.require(Key::Receipt(value.receipt().receipt))? else {
        return Err(invalid());
    };
    require(
        receipt.claim == value.claim()
            && receipt.fence == value.receipt()
            && receipt.holder == value.producer(),
    )?;
    if let Some(old) = old {
        read.charge(256)?;
        let source = &old.state;
        require(
            old.next == next.next
                && source.claim() == value.claim()
                && source.receipt() == value.receipt()
                && source.cycle() == value.cycle()
                && source.slot() == value.slot()
                && source.producer() == value.producer()
                && source.reference() == value.reference(),
        )?;
        if let Some(attachment) = source.attachment() {
            require(value.attachment() == Some(attachment))?;
        }
        if let Some(diagnostic) = source.diagnostic() {
            require(value.diagnostic() == Some(diagnostic))?;
        }
        if let Some(terminal) = source.terminal() {
            require(value.terminal() == Some(terminal))?;
        }
    }
    let mut binding = old.map(|row| row.state.binding());
    let mut state = old.map(|row| row.state.state());
    let mut ordinal = None;
    let mut terminal = old.and_then(|row| row.state.terminal().map(|(at, _)| at));
    read.events(Key::Work(id), |event| {
        read.charge(256)?;
        let NativeFact::Work { claim, before, after, state: updated } = event.fact else { return Err(invalid()); };
        require(claim == value.claim() && after.object.0 == id.0 && after.content == value.binding().content)?;
        step(binding, before, after)?; advance(&mut ordinal, event)?;
        require(matches!((state, updated),
            (None, WorkArtifactState::Generated | WorkArtifactState::GenerationFailed)
            | (Some(WorkArtifactState::Generated), WorkArtifactState::Received)
            | (Some(WorkArtifactState::Generated | WorkArtifactState::Received), WorkArtifactState::ReceiptFailed | WorkArtifactState::Attached)
            | (Some(WorkArtifactState::Attached), WorkArtifactState::Validating)
            | (Some(WorkArtifactState::Validating), WorkArtifactState::Validated | WorkArtifactState::ValidationFailed)))?;
        if updated == WorkArtifactState::Attached {
            let response_id = value.attachment().ok_or_else(invalid)?;
            let response = response_reads::as_response_record(Some(read.require(Key::Response(response_id))?)).ok_or_else(invalid)?;
            require(read.before(Key::Response(response_id))?.is_none()
                && response.response().identity().claim == claim)?;
            let generated = read.only(Key::Response(response_id))?;
            require(matches!(generated.fact, NativeFact::Response { before: None, after, state: ResponseState::Generated, .. } if after == response.generated()))?;
        }
        if matches!(updated, WorkArtifactState::Validated | WorkArtifactState::ValidationFailed) {
            terminal = Some(event.sequence);
        }
        binding = Some(after); state = Some(updated); Ok(())
    })?;
    require(
        ordinal.is_some()
            && binding == Some(value.binding())
            && state == Some(value.state())
            && terminal == value.terminal().map(|(at, _)| at),
    )?;
    let cycle = NativeCycleKey {
        claim: value.claim(),
        receipt: value.receipt().receipt,
        epoch: value.receipt().epoch,
        cycle: value.cycle(),
    };
    require(
        matches!(read.require(Key::WorkSlot(cycle, value.slot()))?, Row::WorkSlot(found) if *found == id),
    )?;
    read.require(Key::Cycle(cycle))?;
    Ok(())
}

pub(super) fn response<O: Overlay>(
    id: TestamentId,
    next: &NativeResponseRecord,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let old = response_reads::as_response_record(read.before(Key::Response(id))?);
    let value = next.response();
    if let Some(old) = old {
        let source = old.response();
        let fields = add(
            add(source.manifest().len(), source.failed_work().len())?,
            source.diagnostics().len(),
        )?;
        read.charge(add(512, add(mul(fields, 512)?, source.summary().len())?)?)?;
        let mut identity = value.identity();
        identity.binding = source.identity().binding;
        require(
            identity == source.identity()
                && source.manifest() == value.manifest()
                && source.failed_work() == value.failed_work()
                && source.summary() == value.summary()
                && source.confidence() == value.confidence()
                && source.reported_outcome() == value.reported_outcome()
                && source.diagnostics() == value.diagnostics()
                && source.respondent() == value.respondent()
                && old.generated() == next.generated(),
        )?;
        if let Some(terminal) = source.terminal() {
            require(value.terminal() == Some(terminal))?;
        }
    }
    let mut binding = old.map(|row| row.response().identity().binding);
    let mut state = old.map(|row| row.response().state());
    let mut received = old.and_then(NativeResponseRecord::received);
    let mut entered = old.and_then(NativeResponseRecord::entered);
    let mut ordinal = None;
    read.events(Key::Response(id), |event| {
        read.charge(256)?;
        let NativeFact::Response {
            claim,
            before,
            after,
            state: updated,
        } = event.fact
        else {
            return Err(invalid());
        };
        require(
            claim == value.identity().claim
                && after.object.0 == id.0
                && after.content == value.identity().binding.content,
        )?;
        step(binding, before, after)?;
        advance(&mut ordinal, event)?;
        require(matches!(
            (state, updated),
            (None, ResponseState::Generated)
                | (Some(ResponseState::Generated), ResponseState::Posted)
                | (Some(ResponseState::Posted), ResponseState::Received)
                | (Some(ResponseState::Received), ResponseState::Validating)
                | (
                    Some(ResponseState::Validating),
                    ResponseState::Validated
                        | ResponseState::ValidationIncomplete
                        | ResponseState::ValidationFailed
                        | ResponseState::ValidationErrored
                )
        ))?;
        match updated {
            ResponseState::Generated => require(after == next.generated())?,
            ResponseState::Received => received = Some(position(event)),
            ResponseState::Validating => entered = Some(position(event)),
            _ => (),
        }
        binding = Some(after);
        state = Some(updated);
        Ok(())
    })?;
    require(
        ordinal.is_some()
            && binding == Some(value.identity().binding)
            && state == Some(value.state())
            && received == next.received()
            && entered == next.entered(),
    )?;
    let identity = value.identity();
    let cycle = NativeCycleKey {
        claim: identity.claim,
        receipt: identity.receipt.receipt,
        epoch: identity.receipt.epoch,
        cycle: identity.cycle,
    };
    require(
        matches!(read.require(Key::Cycle(cycle))?, Row::Cycle(cycle) if cycle.response == Some(id)),
    )?;
    Ok(())
}

fn result<O: Overlay>(
    key: NativeResultKey,
    read: &ReplayRead<'_, '_, O>,
) -> Result<Option<validation::AcceptedResult>, NativeError> {
    let mut found = None;
    for key in [
        Key::Accepted(key),
        Key::DeliveryResult(key),
        Key::MissingResult(key),
    ] {
        let next = match read.get(key)? {
            Some(Row::Accepted(row)) => Some(row.get().ok_or_else(invalid)?.result()),
            Some(Row::DeliveryResult(row)) => Some(row.get().ok_or_else(invalid)?.result()),
            Some(Row::MissingResult(row)) => Some(row.get().ok_or_else(invalid)?.result()),
            None => None,
            _ => return Err(invalid()),
        };
        if let Some(next) = next {
            require(found.replace(next).is_none())?;
        }
    }
    Ok(found)
}

pub(super) fn evaluation<O: Overlay>(
    key: EvaluationKey,
    next: &validation::EvaluationState,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let old = as_evaluation(read.before(Key::Evaluation(key))?);
    let declaration = read.definition(key.validation)?;
    require(EvaluationKey::of(key.claim, next) == key && declaration.claim() == key.claim)?;
    let mut cursor = if let Some(old) = old {
        read.charge(256)?;
        require(
            old.target() == next.target()
                && old.generation() == next.generation()
                && old.receipt() == next.receipt(),
        )?;
        AttemptCursor::resume(declaration, old, read)?
    } else {
        new_target(key, next, read)?;
        AttemptCursor::new(declaration)
    };
    let mut binding = old.map(validation::EvaluationState::binding);
    let mut ordinal = None;
    read.events(Key::Evaluation(key), |event| {
        read.charge(256)?;
        advance(&mut ordinal, event)?;
        match event.fact {
            NativeFact::Evaluation {
                kind,
                key: actual,
                before,
                after,
                state,
                phase,
                attempt,
                fence,
            } => {
                require(actual == key)?;
                step(binding, before, after)?;
                if binding.is_none() {
                    require(
                        kind == NativeEvaluationEventKind::Materialized
                            && after == declaration.binding(),
                    )?;
                }
                let result = if matches!(
                    kind,
                    NativeEvaluationEventKind::Reported | NativeEvaluationEventKind::MissingTarget
                ) {
                    result(
                        NativeResultKey {
                            evaluation: key,
                            revision: after.revision,
                        },
                        read,
                    )?
                } else {
                    None
                };
                if kind == NativeEvaluationEventKind::Sealed {
                    require(read.claim(key.claim)?.local_sealed_at() == Some(event.sequence))?;
                }
                cursor.event(kind, attempt, state, phase, fence, result, read)?;
                binding = Some(after);
            }
            NativeFact::Delivery { key: actual } => {
                require(actual.evaluation == key)?;
                let result = result(actual, read)?.ok_or_else(invalid)?;
                let old = binding.ok_or_else(invalid)?;
                require(result.binding() == old.next()?)?;
                cursor.delivery(result, read)?;
                binding = Some(result.binding());
            }
            _ => return Err(invalid()),
        }
        Ok(())
    })?;
    require(ordinal.is_some() && binding == Some(next.binding()))?;
    cursor.finish(next, read)
}

pub(super) fn audit<O: Overlay>(
    id: TestamentId,
    next: &NativeResultTestament,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let old = match read.before(Key::ResultTestament(id))? {
        Some(Row::ResultTestament(row)) => Some(row.get().ok_or_else(invalid)?),
        None => None,
        _ => return Err(invalid()),
    };
    let value = next.testament();
    if let Some(old) = old {
        let source = old.testament();
        read.charge(mul(
            add(add(source.members().len(), source.results().len())?, 1)?,
            1024,
        )?)?;
        require(
            source.cohort().snapshot_v1()? == value.cohort().snapshot_v1()?
                && source.results() == value.results()
                && old.publications() == next.publications()
                && old.captured_at() == next.captured_at()
                && old.generated_at() == next.generated_at()
                && old.generated_binding() == next.generated_binding()
                && source.members().len() == value.members().len(),
        )?;
        for (previous, next) in source.members().iter().zip(value.members()) {
            require(previous.snapshot_v1() == next.snapshot_v1())?;
        }
    } else {
        require(next.captured_at() == read.base)?;
        super::audit::generated(id, next, read)?;
    }
    let mut binding = old.map(|old| old.testament().binding());
    let mut state = old.map(|old| old.testament().state());
    let mut ordinal = None;
    let mut posted = old.and_then(NativeResultTestament::posted_at);
    read.events(Key::ResultTestament(id), |event| {
        read.charge(256)?;
        let NativeFact::ResultTestament {
            claim,
            before,
            after,
            state: updated,
        } = event.fact
        else {
            return Err(invalid());
        };
        require(claim == value.claim())?;
        step(binding, before, after)?;
        advance(&mut ordinal, event)?;
        match (state, updated) {
            (None, ResultTestamentState::Generated) => require(
                after == next.generated_binding() && position(event) == next.generated_at(),
            )?,
            (Some(ResultTestamentState::Generated), ResultTestamentState::Posted) => {
                posted = Some(position(event))
            }
            _ => return Err(invalid()),
        }
        binding = Some(after);
        state = Some(updated);
        Ok(())
    })?;
    require(
        ordinal.is_some()
            && binding == Some(value.binding())
            && state == Some(value.state())
            && posted == next.posted_at(),
    )?;
    require(
        matches!(read.require(Key::ClaimResultTestament(value.claim()))?, Row::ClaimResultTestament(found) if *found == id),
    )
}

fn row_binding(row: &Row) -> Option<Binding> {
    match row {
        Row::Claim(row) => row.claim().map(ClaimState::binding),
        Row::Work(row) => row.get().map(|row| row.state.binding()),
        Row::Response(row) => row.get().map(|row| row.identity().binding),
        Row::Artifact(row) => row.get().map(|row| row.descriptor().binding()),
        _ => None,
    }
}
fn witness<O: Overlay>(
    key: Key,
    expected: Binding,
    cut: u32,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let current = row_binding(read.require(key)?).ok_or_else(invalid)?;
    require(
        expected.ledger == current.ledger
            && expected.object == current.object
            && expected.content == current.content
            && expected.revision <= current.revision,
    )?;
    if read.before(key)?.and_then(row_binding) == Some(expected) {
        return Ok(());
    }
    let mut found = false;
    read.events(key, |event| {
        read.charge(128)?;
        let binding = match event.fact {
            NativeFact::Claim(value) => value.after,
            NativeFact::Work { after, .. } | NativeFact::Response { after, .. } => after,
            NativeFact::Artifact { binding } => binding,
            _ => return Err(invalid()),
        };
        if binding == expected && event.ordinal < cut {
            found = true;
        }
        Ok(())
    })?;
    require(found)
}
fn new_target<O: Overlay>(
    key: EvaluationKey,
    value: &validation::EvaluationState,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let event = read
        .index
        .first(read.encoded, Key::Evaluation(key), read.parsing, read.meter)?
        .ok_or_else(invalid)?;
    let cut = event.ordinal;
    match value.target() {
        validation::Target::Admission { claim } => {
            require(claim.object.0 == key.claim.0)?;
            witness(Key::Claim(key.claim), claim, cut, read)?;
        }
        validation::Target::Increment { claim, artifact } => {
            require(claim.object.0 == key.claim.0)?;
            witness(Key::Claim(key.claim), claim, cut, read)?;
            witness(
                Key::Work(ArtifactId(artifact.object.0)),
                artifact,
                cut,
                read,
            )?;
        }
        validation::Target::Artifact {
            response,
            artifact,
            slot,
        } => {
            let response_id = TestamentId(response.object.0);
            witness(Key::Response(response_id), response, cut, read)?;
            witness(
                Key::Work(ArtifactId(artifact.object.0)),
                artifact,
                cut,
                read,
            )?;
            let actual =
                response_reads::as_response_record(Some(read.require(Key::Response(response_id))?))
                    .ok_or_else(invalid)?
                    .response();
            let work = as_work(Some(
                read.require(Key::Work(ArtifactId(artifact.object.0)))?,
            ))
            .ok_or_else(invalid)?
            .state;
            read.charge(const { (usize::BITS as usize + 1) * 16 })?;
            require(
                actual
                    .manifest()
                    .binary_search_by_key(&slot, |entry| entry.slot)
                    .ok()
                    .and_then(|index| actual.manifest().get(index))
                    .is_some_and(|entry| entry.artifact == work.reference()),
            )?;
            require(
                actual.identity().claim == key.claim
                    && work.claim() == key.claim
                    && work.slot() == slot
                    && work.attachment() == Some(response_id)
                    && value.receipt() == Some(actual.identity().receipt)
                    && value.generation() == u64::from(actual.identity().cycle),
            )?;
        }
        validation::Target::MissingSlot { response, .. }
        | validation::Target::Delivery { response } => {
            let response_id = TestamentId(response.object.0);
            witness(Key::Response(response_id), response, cut, read)?;
            let actual =
                response_reads::as_response_record(Some(read.require(Key::Response(response_id))?))
                    .ok_or_else(invalid)?
                    .response();
            require(
                actual.identity().claim == key.claim
                    && value.receipt() == Some(actual.identity().receipt)
                    && value.generation() == u64::from(actual.identity().cycle),
            )?;
            if let validation::Target::MissingSlot { slot, .. } = value.target() {
                read.charge(const { (usize::BITS as usize + 1) * 16 })?;
                require(
                    actual
                        .manifest()
                        .binary_search_by_key(&slot, |entry| entry.slot)
                        .is_err(),
                )?;
            }
        }
    }
    Ok(())
}
