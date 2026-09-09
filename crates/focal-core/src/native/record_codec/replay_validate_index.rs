//! Secondary index rows in a replayed record are exactly the changes the
//! replayed primary rows imply (doc 22 §7): every index put or delete is
//! rederived from its primary row at this prefix, and every changed primary
//! row must have carried exactly its derived index changes.
use super::super::index_rows::{self, IndexChange, Primary, TimerOutcomes};
use super::replay_validate::{Overlay, ReplayRead, invalid, require};
use super::*;
use focal_model::lifecycle::claim_descriptor::ClaimDescriptor;

/// A timer is consumed once its outcome row exists: before this record for
/// the before side, after it (including this record's own outcome) for the
/// after side.
struct Outcomes<'a, 'b, 'c, O: Overlay>(&'a ReplayRead<'b, 'c, O>);
impl<O: Overlay> TimerOutcomes for Outcomes<'_, '_, '_, O> {
    fn before(&self, invocation: NativeInvocation) -> Result<bool, NativeError> {
        Ok(self.0.before(Key::Outcome(invocation))?.is_some())
    }
    fn after(&self, invocation: NativeInvocation) -> Result<bool, NativeError> {
        Ok(self.0.get(Key::Outcome(invocation))?.is_some())
    }
}
fn evaluation_of(row: Option<&Row>) -> Result<Option<&validation::EvaluationState>, NativeError> {
    match row {
        None => Ok(None),
        Some(Row::Evaluation(owned)) => Ok(Some(owned.get().ok_or_else(invalid)?)),
        Some(_) => Err(invalid()),
    }
}

fn content<'a, O: Overlay>(
    id: ClaimId,
    read: &'a ReplayRead<'_, '_, O>,
) -> Result<Option<&'a ClaimDescriptor>, NativeError> {
    Ok(authored_reads::content(read.get(Key::ClaimContent(id))?))
}

/// Run the derivation of the primary row `key` belongs to, in its after
/// state, and report whether `expected` is among its changes.
fn derives<O: Overlay>(
    key: Key,
    expected: IndexChange,
    read: &ReplayRead<'_, '_, O>,
) -> Result<bool, NativeError> {
    let mut found = false;
    let mut sink = |change: IndexChange| {
        found |= change == expected;
        Ok(())
    };
    read.charge(512)?;
    let outcomes = Outcomes(read);
    match index_rows::primary(key).ok_or_else(invalid)? {
        Primary::Claim(id) => {
            let after = read.claim(id)?;
            let before = as_claim(read.before(Key::Claim(id))?);
            index_rows::claim(before, after, content(id, read)?, &outcomes, &mut sink)?;
        }
        Primary::Evaluation(key) => {
            let after =
                evaluation_of(Some(read.require(Key::Evaluation(key))?))?.ok_or_else(invalid)?;
            let before = evaluation_of(read.before(Key::Evaluation(key))?)?;
            let declaration = read.definition(key.validation)?;
            index_rows::evaluation(key, before, after, declaration, &outcomes, &mut sink)?;
        }
        Primary::Artifact(id) => {
            require(read.before(Key::Artifact(id))?.is_none())?;
            let artifact = read.artifact(id)?;
            index_rows::artifact(artifact.descriptor(), &mut sink)?;
        }
        Primary::Definition(id) => {
            require(read.before(Key::Definition(id))?.is_none())?;
            let declaration = read.definition(id)?;
            index_rows::definition(declaration, &mut sink)?;
        }
        Primary::Result(result) => {
            require(read.before(Key::Accepted(result))?.is_none())?;
            let value =
                as_result(Some(read.require(Key::Accepted(result))?)).ok_or_else(invalid)?;
            index_rows::accepted(result, value.result().verdict(), &mut sink)?;
        }
    }
    Ok(found)
}

/// An index row written by this record is derived from its primary row.
pub(super) fn check_put<O: Overlay>(
    key: Key,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    require(index_rows::is_index(key) && read.before(key)?.is_none())?;
    require(derives(key, IndexChange::Put(key), read)?)
}

/// A deleted index row is the status row a claim transition left behind or
/// a due timer that settled, fenced or was consumed.
pub(super) fn check_delete<O: Overlay>(
    key: Key,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    require(matches!(key, Key::ByStatus(..) | Key::DueTimer(..)) && read.before(key)?.is_some())?;
    require(derives(key, IndexChange::Delete(key), read)?)
}

/// Every index change the primary row `key` implies is carried by the record
/// with the right disposition.
fn complete<O: Overlay>(
    read: &ReplayRead<'_, '_, O>,
    derive: impl FnOnce(
        &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
    ) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let mut sink = |change: IndexChange| {
        let key = change.key();
        read.charge(64)?;
        require(read.changed(key)?)?;
        match change {
            IndexChange::Put(_) => require(matches!(read.get(key)?, Some(Row::Index))),
            IndexChange::Delete(_) => require(read.get(key)?.is_none()),
        }
    };
    derive(&mut sink)
}

pub(super) fn claim_complete<O: Overlay>(
    id: ClaimId,
    row: &OwnedClaim,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let after = row.claim().ok_or_else(invalid)?;
    let before = as_claim(read.before(Key::Claim(id))?);
    let content = content(id, read)?;
    let outcomes = Outcomes(read);
    complete(read, |sink| {
        index_rows::claim(before, after, content, &outcomes, sink)
    })
}
pub(super) fn evaluation_complete<O: Overlay>(
    key: EvaluationKey,
    row: &OwnedEvaluation,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let after = row.get().ok_or_else(invalid)?;
    let before = evaluation_of(read.before(Key::Evaluation(key))?)?;
    let declaration = read.definition(key.validation)?;
    let outcomes = Outcomes(read);
    complete(read, |sink| {
        index_rows::evaluation(key, before, after, declaration, &outcomes, sink)
    })
}
/// A delivered monitor or evaluation timer whose target row this record left
/// unchanged must still carry the consumption of its due-timer row.
pub(super) fn consumption_complete<O: Overlay>(
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    match read.outcome.invocation {
        NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey { claim: id, .. })
        | NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey { claim: id, .. })
            if !read.changed(Key::Claim(id))? =>
        {
            let claim = read.claim(id)?;
            let content = content(id, read)?;
            let outcomes = Outcomes(read);
            complete(read, |sink| {
                index_rows::claim(Some(claim), claim, content, &outcomes, sink)
            })
        }
        NativeInvocation::EvaluationDeadline(key)
            if !read.changed(Key::Evaluation(key.evaluation))? =>
        {
            let state = evaluation_of(Some(read.require(Key::Evaluation(key.evaluation))?))?
                .ok_or_else(invalid)?;
            let declaration = read.definition(key.evaluation.validation)?;
            let outcomes = Outcomes(read);
            complete(read, |sink| {
                index_rows::evaluation(
                    key.evaluation,
                    Some(state),
                    state,
                    declaration,
                    &outcomes,
                    sink,
                )
            })
        }
        _ => Ok(()),
    }
}
pub(super) fn artifact_complete<O: Overlay>(
    id: ArtifactId,
    row: &OwnedArtifact,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    if read.before(Key::Artifact(id))?.is_some() {
        return Ok(());
    }
    let value = row.get().ok_or_else(invalid)?;
    complete(read, |sink| index_rows::artifact(value.descriptor(), sink))
}
pub(super) fn definition_complete<O: Overlay>(
    id: ValidationId,
    row: &OwnedDeclaration,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    if read.before(Key::Definition(id))?.is_some() {
        return Ok(());
    }
    let value = row.get().ok_or_else(invalid)?;
    complete(read, |sink| index_rows::definition(value, sink))
}
pub(super) fn accepted_complete<O: Overlay>(
    key: NativeResultKey,
    row: &OwnedAccepted,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    if read.before(Key::Accepted(key))?.is_some() {
        return Ok(());
    }
    let value = row.get().ok_or_else(invalid)?;
    complete(read, |sink| {
        index_rows::accepted(key, value.result().verdict(), sink)
    })
}
