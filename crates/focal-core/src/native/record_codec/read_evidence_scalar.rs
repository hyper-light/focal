use super::*;
use crate::native::{
    delivery_owned::{NativeDeliveryResult, OwnedDeliveryResult},
    missing_owned::{NativeMissingResult, OwnedMissingResult},
    result_owned::{NativeAccepted, OwnedAccepted},
    work_owned::{NativeDiagnostic, NativeWork, OwnedDiagnostic, OwnedWork},
};
use focal_model::lifecycle::{
    aggregation::PublicationPosition,
    audit::{ResultArtifact, ResultArtifactSnapshotV1},
    evidence::{
        self, Diagnostic, ResponseDiagnostic, ResponseDiagnosticSnapshotV1, WorkArtifact,
        WorkArtifactSnapshotV1,
    },
    validation::{AcceptedResult, AcceptedResultSnapshotV1, Attempt},
};
use focal_model::{ArtifactId, ParticipantId};

#[derive(Debug, Clone, Copy)]
pub(in crate::native::record_codec) enum ScalarInput {
    Work(WorkArtifactSnapshotV1, Option<ArtifactId>),
    Diagnostic(ResponseDiagnosticSnapshotV1, Option<ArtifactId>),
    Accepted(
        AcceptedResultSnapshotV1,
        Attempt,
        ArtifactRef,
        ParticipantId,
        PublicationPosition,
    ),
    Delivery(AcceptedResultSnapshotV1, PublicationPosition),
    Missing(AcceptedResultSnapshotV1, PublicationPosition),
}
enum Value {
    Work(NativeWork),
    Diagnostic(NativeDiagnostic),
    Accepted(NativeAccepted),
    Delivery(NativeDeliveryResult),
    Missing(NativeMissingResult),
}
pub(in crate::native::record_codec) struct ScalarPlan {
    value: Value,
    quote: Quote,
}
fn diagnostic_ref(c: &mut Cursor<'_>) -> Result<Diagnostic, Error> {
    Ok(Diagnostic {
        reason: fields::failure(c)?,
        artifact: fields::artifact_ref(c)?,
    })
}
pub(in crate::native::record_codec) fn diagnostic_value(
    c: &mut Cursor<'_>,
) -> Result<ResponseDiagnosticSnapshotV1, Error> {
    Ok(ResponseDiagnosticSnapshotV1 {
        ledger: fields::ledger(c)?,
        claim: ClaimId(c.fixed()?),
        receipt: fields::receipt(c)?,
        cycle: c.u32()?,
        producer: fields::participant(c)?,
        diagnostic: diagnostic_ref(c)?,
    })
}
pub(in crate::native::record_codec) fn failed_value(
    c: &mut Cursor<'_>,
) -> Result<evidence::FailedWorkSnapshotV1, Error> {
    Ok(evidence::FailedWorkSnapshotV1 {
        binding: fields::binding(c)?,
        slot: c.u32()?,
        state: fields::work_state(c)?,
        diagnostic: diagnostic_ref(c)?,
    })
}
pub(in crate::native::record_codec) fn work(
    c: &mut Cursor<'_>,
) -> Result<ScalarInput, NativeError> {
    let read = |c: &mut Cursor<'_>| -> Result<ScalarInput, Error> {
        let value = WorkArtifactSnapshotV1 {
            binding: fields::binding(c)?,
            claim: ClaimId(c.fixed()?),
            slot: c.u32()?,
            cycle: c.u32()?,
            producer: fields::participant(c)?,
            receipt: fields::receipt(c)?,
            state: fields::work_state(c)?,
            attachment: fields::optional_binding(c)?,
            diagnostic: fields::optional(c, diagnostic_ref)?,
            terminal: fields::optional(c, fields::work_terminal)?,
        };
        Ok(ScalarInput::Work(
            value,
            fields::optional(c, |c| Ok(ArtifactId(c.fixed()?)))?,
        ))
    };
    read(c).map_err(codec)
}
pub(in crate::native::record_codec) fn diagnostic(
    c: &mut Cursor<'_>,
) -> Result<ScalarInput, NativeError> {
    let value = diagnostic_value(c).map_err(codec)?;
    let next = fields::optional(c, |c| Ok(ArtifactId(c.fixed()?))).map_err(codec)?;
    Ok(ScalarInput::Diagnostic(value, next))
}
pub(in crate::native::record_codec) fn accepted(
    c: &mut Cursor<'_>,
) -> Result<ScalarInput, NativeError> {
    Ok(ScalarInput::Accepted(
        fields::accepted_result(c).map_err(codec)?,
        fields::attempt(c).map_err(codec)?,
        fields::artifact_ref(c).map_err(codec)?,
        fields::participant(c).map_err(codec)?,
        fields::position(c).map_err(codec)?,
    ))
}
pub(in crate::native::record_codec) fn delivery(
    c: &mut Cursor<'_>,
) -> Result<ScalarInput, NativeError> {
    Ok(ScalarInput::Delivery(
        fields::accepted_result(c).map_err(codec)?,
        fields::position(c).map_err(codec)?,
    ))
}
pub(in crate::native::record_codec) fn missing(
    c: &mut Cursor<'_>,
) -> Result<ScalarInput, NativeError> {
    Ok(ScalarInput::Missing(
        fields::accepted_result(c).map_err(codec)?,
        fields::position(c).map_err(codec)?,
    ))
}
impl ScalarInput {
    pub(in crate::native::record_codec) fn prepare(
        self,
        deps: &impl Dependencies,
        max_visits: usize,
    ) -> Result<ScalarPlan, NativeError> {
        // Scalar native linkage checks plus model restoration, debited together
        // before any policy scan or allocation. Dependency lookup is importer work.
        let (value, heap, visits) = match self {
            Self::Work(snapshot, next) => {
                let policy = deps.policy(snapshot.claim)?;
                let model = WorkArtifact::hydration_visits(policy)?;
                let visits = add(model, 32)?;
                fits(visits, max_visits)?;
                let reference = ArtifactRef {
                    id: ArtifactId(snapshot.binding.object.0),
                    hash: snapshot.binding.content,
                };
                let descriptor = exact_artifact(deps, reference)?;
                let rejection = snapshot
                    .diagnostic
                    .filter(|d| d.artifact != reference)
                    .map(|d| exact_artifact(deps, d.artifact))
                    .transpose()?;
                let state =
                    WorkArtifact::hydrate_v1(policy, snapshot, descriptor, rejection, model)?;
                if next.is_some_and(|id| id.is_zero() || id == reference.id) {
                    return Err(ContractError::InvalidManifest.into());
                }
                (
                    Value::Work(NativeWork { state, next }),
                    OwnedWork::container_charge(),
                    visits,
                )
            }
            Self::Diagnostic(snapshot, next) => {
                let visits = add(ResponseDiagnostic::HYDRATION_VISITS, 16)?;
                fits(visits, max_visits)?;
                let descriptor = exact_artifact(deps, snapshot.diagnostic.artifact)?;
                let diagnostic = ResponseDiagnostic::hydrate_v1(
                    snapshot,
                    descriptor,
                    ResponseDiagnostic::HYDRATION_VISITS,
                )?;
                if next.is_some_and(|id| id.is_zero() || id == snapshot.diagnostic.artifact.id) {
                    return Err(ContractError::InvalidManifest.into());
                }
                (
                    Value::Diagnostic(NativeDiagnostic { diagnostic, next }),
                    OwnedDiagnostic::container_charge(),
                    visits,
                )
            }
            Self::Accepted(snapshot, attempt, artifact, producer, position) => {
                let declaration = deps.declaration(snapshot.validation)?;
                let model = ResultArtifact::hydration_visits(declaration)?;
                let visits = add(model, 48)?;
                fits(visits, max_visits)?;
                let descriptor = exact_artifact(deps, artifact)?;
                if descriptor.ledger() != snapshot.ledger
                    || descriptor.producer() != producer
                    || descriptor.receipt() != snapshot.receipt
                    || descriptor.result_provenance()
                        != Some(
                            focal_model::lifecycle::artifact_descriptor::ResultProvenance {
                                claim: snapshot.claim,
                                validation: snapshot.validation,
                                target: snapshot.target,
                                generation: snapshot.generation,
                                attempt,
                                value: snapshot.verdict,
                            },
                        )
                {
                    return Err(ContractError::MissingEvidence.into());
                }
                let result = ResultArtifact::hydrate_v1(
                    declaration,
                    ResultArtifactSnapshotV1 {
                        result: snapshot,
                        artifact,
                        producer,
                    },
                    model,
                )?;
                let value = NativeAccepted::new(
                    result.result(),
                    attempt,
                    result,
                    position.sequence,
                    position.ordinal,
                )?;
                (
                    Value::Accepted(value),
                    OwnedAccepted::container_charge(),
                    visits,
                )
            }
            Self::Delivery(snapshot, position) | Self::Missing(snapshot, position) => {
                let declaration = deps.declaration(snapshot.validation)?;
                let model = AcceptedResult::hydration_visits(declaration)?;
                let visits = add(model, 48)?;
                fits(visits, max_visits)?;
                let result = AcceptedResult::hydrate_v1(declaration, snapshot, model)?;
                match self {
                    Self::Delivery(..) => (
                        Value::Delivery(NativeDeliveryResult::new(
                            result,
                            position.sequence,
                            position.ordinal,
                        )?),
                        OwnedDeliveryResult::container_charge(),
                        visits,
                    ),
                    _ => (
                        Value::Missing(NativeMissingResult::new(
                            result,
                            position.sequence,
                            position.ordinal,
                        )?),
                        OwnedMissingResult::container_charge(),
                        visits,
                    ),
                }
            }
        };
        Ok(ScalarPlan {
            value,
            quote: Quote {
                heap_bytes: heap,
                allocations: 1,
                model_inspection_visits: visits,
                model_build_visits: 1,
                source_inspection_visits: 0,
                source_build_visits: 0,
            },
        })
    }
}
impl ScalarPlan {
    pub(in crate::native::record_codec) fn quote(&self) -> Quote {
        self.quote
    }
    pub(in crate::native::record_codec) fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<Row, NativeError> {
        fits(self.quote.heap_bytes, max_bytes)?;
        fits(self.quote.model_build_visits, max_visits)?;
        let (row, actual) = match self.value {
            Value::Work(value) => {
                let row = OwnedWork::new(value)?;
                let heap = row.heap_charge()?;
                (Row::Work(row), heap)
            }
            Value::Diagnostic(value) => {
                let row = OwnedDiagnostic::new(value)?;
                let heap = row.heap_charge()?;
                (Row::Diagnostic(row), heap)
            }
            Value::Accepted(value) => {
                let row = OwnedAccepted::new(value)?;
                let heap = row.heap_charge()?;
                (Row::Accepted(row), heap)
            }
            Value::Delivery(value) => {
                let row = OwnedDeliveryResult::new(value)?;
                let heap = row.heap_charge()?;
                (Row::DeliveryResult(row), heap)
            }
            Value::Missing(value) => {
                let row = OwnedMissingResult::new(value)?;
                let heap = row.heap_charge()?;
                (Row::MissingResult(row), heap)
            }
        };
        finish(row, actual, self.quote, max_bytes)
    }
}
