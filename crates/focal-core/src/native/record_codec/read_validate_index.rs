//! Secondary index rows in a checkpoint are exactly the rows its primary rows
//! imply (doc 22 §7): each index row rederives from the primary row it names
//! at the checkpoint's prefix, and each claim, declaration, artifact and
//! accepted result is covered by every row its derivation yields.
use super::super::index_rows::{self, IndexChange, Primary, TimerOutcomes};
use super::read_validate::{ValidationRead, invalid};
use super::*;
use focal_model::lifecycle::artifact_descriptor::ArtifactDescriptor;

/// A checkpoint has no before side; a timer is consumed once its outcome
/// row is retained.
struct Outcomes<'a, 'b, 'c>(&'a ValidationRead<'b, 'c>);
impl TimerOutcomes for Outcomes<'_, '_, '_> {
    fn before(&self, _: NativeInvocation) -> Result<bool, NativeError> {
        Ok(false)
    }
    fn after(&self, invocation: NativeInvocation) -> Result<bool, NativeError> {
        Ok(self.0.get(Key::Outcome(invocation))?.is_some())
    }
}

fn require(condition: bool) -> Result<(), NativeError> {
    if condition { Ok(()) } else { Err(invalid()) }
}

fn derives(key: Key, read: &ValidationRead<'_, '_>) -> Result<bool, NativeError> {
    let mut found = false;
    let mut sink = |change: IndexChange| {
        found |= change == IndexChange::Put(key);
        Ok(())
    };
    read.charge(512)?;
    let outcomes = Outcomes(read);
    match index_rows::primary(key).ok_or_else(invalid)? {
        Primary::Claim(id) => {
            let claim = read.claim(id)?;
            let content = authored_reads::content(read.get(Key::ClaimContent(id))?);
            index_rows::claim(None, claim, content, &outcomes, &mut sink)?;
        }
        Primary::Evaluation(key) => {
            let Row::Evaluation(owned) = read.require(Key::Evaluation(key))? else {
                return Err(invalid());
            };
            let state = owned.get().ok_or_else(invalid)?;
            let declaration = read.definition(key.validation)?;
            index_rows::evaluation(key, None, state, declaration, &outcomes, &mut sink)?;
        }
        Primary::Artifact(id) => {
            let artifact = read.artifact(id)?;
            index_rows::artifact(artifact.descriptor(), &mut sink)?;
        }
        Primary::Definition(id) => {
            let declaration = read.definition(id)?;
            index_rows::definition(declaration, &mut sink)?;
        }
        Primary::Result(result) => {
            let value =
                as_result(Some(read.require(Key::Accepted(result))?)).ok_or_else(invalid)?;
            index_rows::accepted(result, value.result().verdict(), &mut sink)?;
        }
    }
    Ok(found)
}

/// One retained index row is derived from the primary row it names.
pub(super) fn check_row(
    key: Key,
    row: &Row,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    require(matches!(row, Row::Index) && index_rows::is_index(key))?;
    super::read_rows::check_fixed(key, row, read.ledger)?;
    require(derives(key, read)?)
}

fn complete(
    read: &ValidationRead<'_, '_>,
    derive: impl FnOnce(
        &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
    ) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let mut sink = |change: IndexChange| match change {
        IndexChange::Put(key) => require(matches!(read.get(key)?, Some(Row::Index))),
        IndexChange::Delete(_) => Err(invalid()),
    };
    derive(&mut sink)
}

pub(super) fn require_claim(
    id: ClaimId,
    claim: &ClaimState,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let content = authored_reads::content(read.get(Key::ClaimContent(id))?);
    let outcomes = Outcomes(read);
    complete(read, |sink| {
        index_rows::claim(None, claim, content, &outcomes, sink)
    })
}
pub(super) fn require_evaluation(
    key: EvaluationKey,
    state: &validation::EvaluationState,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let declaration = read.definition(key.validation)?;
    let outcomes = Outcomes(read);
    complete(read, |sink| {
        index_rows::evaluation(key, None, state, declaration, &outcomes, sink)
    })
}
pub(super) fn require_definition(
    declaration: &validation::Declaration,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    complete(read, |sink| index_rows::definition(declaration, sink))
}
pub(super) fn require_artifact(
    descriptor: &ArtifactDescriptor,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    complete(read, |sink| index_rows::artifact(descriptor, sink))
}
pub(super) fn require_accepted(
    key: NativeResultKey,
    verdict: focal_model::VerdictValue,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    complete(read, |sink| index_rows::accepted(key, verdict, sink))
}
