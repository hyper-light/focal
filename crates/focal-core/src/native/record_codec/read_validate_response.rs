use super::*;
use focal_model::lifecycle::evidence::ResponseSnapshotSource;

pub(super) fn record<'a>(
    read: &'a ValidationRead<'_, '_>,
    id: TestamentId,
) -> Result<&'a NativeResponseRecord, NativeError> {
    let Row::Response(row) = read.require(Key::Response(id))? else {
        return Err(invalid());
    };
    row.record().ok_or_else(invalid)
}
pub(super) fn validate(
    id: TestamentId,
    value: &NativeResponseRecord,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let response = value.response();
    let identity = response.identity();
    let claim = read.claim(identity.claim)?;
    require(identity.binding.ledger == read.ledger && identity.binding.object.0 == id.0)?;
    receipt(
        read,
        identity.claim,
        identity.receipt,
        response.respondent(),
    )?;
    let cycle_key = NativeCycleKey {
        claim: identity.claim,
        receipt: identity.receipt.receipt,
        epoch: identity.receipt.epoch,
        cycle: identity.cycle,
    };
    let Row::Cycle(cycle) = read.require(Key::Cycle(cycle_key))? else {
        return Err(invalid());
    };
    require(cycle.response == Some(id))?;
    let (posted, received) = claim.recorded_response(response)?;
    require(
        posted == (response.state() != ResponseState::Generated)
            && (!received || value.received().is_some()),
    )?;
    let visits = response.snapshot_visits()?;
    read.charge(visits)?;
    let snapshot = response.snapshot_v1(value.generated(), visits)?;
    let fields = snapshot.fields()?;
    if let Some(prior) = identity.prior {
        let prior = record(read, prior)?.response().identity();
        require(
            prior.claim == identity.claim && prior.cycle.checked_add(1) == Some(identity.cycle),
        )?;
    } else {
        require(identity.cycle == 1)?;
    }
    let mut previous = None;
    let mut previous_state = None;
    let mut previous_position = None;
    let mut observed = None;
    let mut entered = None;
    let mut terminal = None;
    history.events(Key::Response(id), read, |event| {
        read.charge(128)?;
        let NativeFact::Response {
            claim: owner,
            before,
            after,
            state,
        } = event.fact
        else {
            return Err(invalid());
        };
        require(
            owner == identity.claim
                && after.ledger == read.ledger
                && after.object.0 == id.0
                && after.content == identity.binding.content,
        )?;
        binding_step(previous, before, after)?;
        if let Some(prior) = previous_position {
            require(later(position(event), prior))?;
        }
        match (previous_state, state) {
            (None, ResponseState::Generated) => {
                require(after == value.generated())?;
                actor(event, response.respondent())?;
            }
            (Some(ResponseState::Generated), ResponseState::Posted) => {
                actor(event, response.respondent())?;
            }
            (Some(ResponseState::Posted), ResponseState::Received) => {
                actor(event, claim.issuer())?;
                observed = Some(position(event));
            }
            (Some(ResponseState::Received), ResponseState::Validating) => {
                entry_actor(event, claim.issuer(), id, read)?;
                entered = Some(position(event));
            }
            (
                Some(ResponseState::Validating),
                ResponseState::Validated
                | ResponseState::ValidationIncomplete
                | ResponseState::ValidationFailed
                | ResponseState::ValidationErrored,
            ) => {
                terminal = Some(event.sequence);
            }
            _ => return Err(invalid()),
        }
        previous = Some(after);
        previous_state = Some(state);
        previous_position = Some(position(event));
        Ok(())
    })?;
    require(
        previous == Some(identity.binding)
            && previous_state == Some(response.state())
            && observed == value.received()
            && entered == value.entered(),
    )?;
    let recorded_terminal = match response.terminal() {
        None | Some(ResponseOutcome::Evaluating) => None,
        Some(ResponseOutcome::Validated { sequence }) => Some(sequence),
        Some(ResponseOutcome::Blocked(cut)) => Some(cut.sequence()),
    };
    require(terminal == recorded_terminal)?;
    let count = fields
        .manifest_count
        .checked_add(fields.failed_work_count)
        .and_then(|n| n.checked_add(fields.diagnostic_count))
        .and_then(|n| n.checked_add(1))
        .ok_or_else(invalid)?;
    read.charge(count.checked_mul(64).ok_or_else(invalid)?)?;
    for slot in response.manifest() {
        let Row::Work(row) = read.require(Key::Work(slot.artifact.id))? else {
            return Err(invalid());
        };
        let state = row.get().ok_or_else(invalid)?.state;
        require(
            state.claim() == identity.claim
                && state.receipt() == identity.receipt
                && state.cycle() == identity.cycle
                && state.slot() == slot.slot
                && state.reference() == slot.artifact
                && state.attachment() == Some(id)
                && state.snapshot_v1()?.attachment == Some(value.generated()),
        )?;
    }
    for failed in response.failed_work() {
        let Row::Work(row) = read.require(Key::Work(ArtifactId(failed.binding().object.0)))? else {
            return Err(invalid());
        };
        let state = row.get().ok_or_else(invalid)?.state;
        require(
            state.claim() == identity.claim
                && state.receipt() == identity.receipt
                && state.cycle() == identity.cycle
                && state.binding() == failed.binding()
                && state.slot() == failed.slot()
                && state.state() == failed.state()
                && state.diagnostic() == Some(failed.diagnostic()),
        )?;
    }
    for diagnostic in response.diagnostics() {
        let Row::Diagnostic(row) = read.require(Key::Diagnostic(diagnostic.artifact().id))? else {
            return Err(invalid());
        };
        require(row.get().ok_or_else(invalid)?.diagnostic == *diagnostic)?;
    }
    require(
        response
            .manifest()
            .len()
            .checked_add(response.failed_work().len())
            == Some(cycle.work_count)
            && response.diagnostics().len() == cycle.diagnostic_count,
    )?;
    if let Some(ResponseOutcome::Blocked(cut)) = response.terminal()
        && let Some(reference) = cut.cause().evidence()
    {
        exact_artifact(read, reference)?;
    }
    Ok(())
}
