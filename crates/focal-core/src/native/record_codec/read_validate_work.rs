use super::*;
use focal_model::lifecycle::evidence::{ResponseDiagnostic, WorkArtifact};

pub(super) fn work(
    id: ArtifactId,
    value: &NativeWork,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let state = value.state;
    require(state.reference().id == id && state.binding().ledger == read.ledger)?;
    let claim = read.claim(state.claim())?;
    receipt(read, state.claim(), state.receipt(), state.producer())?;
    let cycle_key = NativeCycleKey {
        claim: state.claim(),
        receipt: state.receipt().receipt,
        epoch: state.receipt().epoch,
        cycle: state.cycle(),
    };
    let Row::Cycle(cycle) = read.require(Key::Cycle(cycle_key))? else {
        return Err(invalid());
    };
    require(cycle.work_count != 0 && cycle.work_head.is_some())?;
    require(
        matches!(read.require(Key::WorkSlot(cycle_key, state.slot()))?, Row::WorkSlot(found) if *found == id),
    )?;
    let own = exact_artifact(read, state.reference())?;
    let rejection = state
        .diagnostic()
        .filter(|diagnostic| diagnostic.artifact != state.reference())
        .map(|diagnostic| exact_artifact(read, diagnostic.artifact))
        .transpose()?;
    let visits = WorkArtifact::hydration_visits(claim.acceptance())?;
    read.charge(visits)?;
    let restored = WorkArtifact::hydrate_v1(
        claim.acceptance(),
        state.snapshot_v1()?,
        own.descriptor(),
        rejection.map(NativeArtifact::descriptor),
        visits,
    )?;
    require(restored == state)?;
    let mut previous = None;
    let mut previous_state = None;
    let mut previous_position = None;
    let mut terminal = None;
    history.events(Key::Work(id), read, |event| {
        read.charge(128)?;
        let NativeFact::Work {
            claim: owner,
            before,
            after,
            state: next,
        } = event.fact
        else {
            return Err(invalid());
        };
        require(
            owner == state.claim()
                && after.ledger == read.ledger
                && after.object.0 == id.0
                && after.content == state.binding().content,
        )?;
        binding_step(previous, before, after)?;
        if let Some(prior) = previous_position {
            require(later(position(event), prior))?;
        }
        match (previous_state, next) {
            (None, WorkArtifactState::Generated | WorkArtifactState::GenerationFailed) => {
                actor(event, state.producer())?;
            }
            (Some(WorkArtifactState::Generated), WorkArtifactState::Received)
            | (
                Some(WorkArtifactState::Generated | WorkArtifactState::Received),
                WorkArtifactState::ReceiptFailed,
            ) => {
                actor(event, claim.issuer())?;
            }
            (
                Some(WorkArtifactState::Generated | WorkArtifactState::Received),
                WorkArtifactState::Attached,
            ) => {
                actor(event, state.producer())?;
                let response_id = state.attachment().ok_or_else(invalid)?;
                let response = super::response::record(read, response_id)?;
                let generated = history
                    .first(Key::Response(response_id), read)?
                    .ok_or_else(invalid)?;
                require(
                    generated.sequence == event.sequence
                        && generated.invocation == event.invocation
                        && cycle.response == Some(response_id)
                        && response.generated()
                            == state.snapshot_v1()?.attachment.ok_or_else(invalid)?,
                )?;
            }
            (Some(WorkArtifactState::Attached), WorkArtifactState::Validating) => {
                entry_actor(
                    event,
                    claim.issuer(),
                    state.attachment().ok_or_else(invalid)?,
                    read,
                )?;
            }
            (
                Some(WorkArtifactState::Validating),
                WorkArtifactState::Validated | WorkArtifactState::ValidationFailed,
            ) => {
                terminal = Some(event.sequence);
            }
            _ => return Err(invalid()),
        }
        previous = Some(after);
        previous_state = Some(next);
        previous_position = Some(position(event));
        Ok(())
    })?;
    require(
        previous == Some(state.binding())
            && previous_state == Some(state.state())
            && terminal == state.terminal().map(|(sequence, _)| sequence),
    )?;
    if let Some(response_id) = state.attachment() {
        let response = super::response::record(read, response_id)?;
        require(
            response.response().identity().claim == state.claim()
                && response.response().identity().receipt == state.receipt()
                && response.response().identity().cycle == state.cycle(),
        )?;
        read.charge(
            response
                .response()
                .manifest()
                .len()
                .checked_add(1)
                .ok_or_else(invalid)?,
        )?;
        require(
            response
                .response()
                .manifest()
                .iter()
                .any(|slot| slot.slot == state.slot() && slot.artifact == state.reference()),
        )?;
    }
    if let Some((_, focal_model::lifecycle::aggregation::ArtifactOutcome::Blocked(cause))) =
        state.terminal()
        && let Some(reference) = cause.evidence()
    {
        exact_artifact(read, reference)?;
    }
    Ok(())
}
pub(super) fn diagnostic(
    id: ArtifactId,
    value: &NativeDiagnostic,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let snapshot = value.diagnostic.snapshot_v1();
    require(snapshot.ledger == read.ledger && snapshot.diagnostic.artifact.id == id)?;
    read.claim(snapshot.claim)?;
    receipt(read, snapshot.claim, snapshot.receipt, snapshot.producer)?;
    let cycle_key = NativeCycleKey {
        claim: snapshot.claim,
        receipt: snapshot.receipt.receipt,
        epoch: snapshot.receipt.epoch,
        cycle: snapshot.cycle,
    };
    let Row::Cycle(cycle) = read.require(Key::Cycle(cycle_key))? else {
        return Err(invalid());
    };
    require(cycle.diagnostic_count != 0 && cycle.diagnostic_head.is_some())?;
    let descriptor = exact_artifact(read, snapshot.diagnostic.artifact)?.descriptor();
    read.charge(ResponseDiagnostic::HYDRATION_VISITS)?;
    require(
        ResponseDiagnostic::hydrate_v1(snapshot, descriptor, ResponseDiagnostic::HYDRATION_VISITS)?
            == value.diagnostic,
    )?;
    let event = one(history, read, Key::Diagnostic(id))?;
    require(
        event.fact
            == NativeFact::Diagnostic {
                claim: snapshot.claim,
                binding: descriptor.binding(),
                reason: snapshot.diagnostic.reason,
            },
    )?;
    actor(event, snapshot.producer)?;
    let artifact_event = one(history, read, Key::Artifact(id))?;
    require(
        artifact_event.sequence == event.sequence
            && artifact_event.invocation == event.invocation
            && artifact_event.ordinal < event.ordinal,
    )?;
    Ok(())
}
