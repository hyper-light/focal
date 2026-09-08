//! Retained evidence cross-links and exact independent revision histories.
//! No current receipt/claim authorization is replayed. The parent validates
//! complete cycle chains and object counts; this pass binds their actual rows.
pub(super) use super::read_history::HistoryIndex;
use super::read_validate::{ValidationRead, invalid};
pub(super) use super::read_validate_aggregate::validate_claim;
use super::*;
use focal_model::lifecycle::{
    aggregation::{PublicationPosition, ResponseOutcome},
    artifact_descriptor::{ResultProvenance, WorkRole},
    evidence,
};
use focal_model::{ObjectKind, ObjectRevision, VerdictValue};

#[path = "read_validate_response.rs"]
mod response;
#[path = "read_validate_work.rs"]
mod work;

fn require(value: bool) -> Result<(), NativeError> {
    if value { Ok(()) } else { Err(invalid()) }
}
fn position(event: NativeEvent) -> PublicationPosition {
    PublicationPosition {
        sequence: event.sequence,
        ordinal: event.ordinal,
    }
}
fn actor(event: NativeEvent, expected: ParticipantId) -> Result<RequestKey, NativeError> {
    match event.invocation {
        NativeInvocation::Request(key) if key.principal == expected => Ok(key),
        _ => Err(invalid()),
    }
}
fn receipt(
    read: &ValidationRead<'_, '_>,
    claim: ClaimId,
    fence: ReceiptFence,
    holder: ParticipantId,
) -> Result<NativeReceipt, NativeError> {
    let Row::Receipt(value) = read.require(Key::Receipt(fence.receipt))? else {
        return Err(invalid());
    };
    require(value.claim == claim && value.fence == fence && value.holder == holder)?;
    Ok(*value)
}
fn one(
    index: &HistoryIndex,
    read: &ValidationRead<'_, '_>,
    object: Key,
) -> Result<NativeEvent, NativeError> {
    let mut found = None;
    index.events(object, read, |event| {
        if found.replace(event).is_some() {
            return Err(invalid());
        }
        Ok(())
    })?;
    found.ok_or_else(invalid)
}
fn exact_artifact<'a>(
    read: &'a ValidationRead<'_, '_>,
    reference: focal_model::ArtifactRef,
) -> Result<&'a NativeArtifact, NativeError> {
    let value = read.artifact(reference.id)?;
    require(
        value.descriptor().id() == reference.id
            && value.descriptor().content_hash() == reference.hash
            && value.descriptor().ledger() == read.ledger,
    )?;
    Ok(value)
}
fn later(next: PublicationPosition, before: PublicationPosition) -> bool {
    (next.sequence, next.ordinal) > (before.sequence, before.ordinal)
}
fn binding_step(
    previous: Option<Binding>,
    before: Option<Binding>,
    after: Binding,
) -> Result<(), NativeError> {
    if let Some(previous) = previous {
        require(before == Some(previous) && after == previous.next()?)
    } else {
        require(before.is_none() && after.revision == ObjectRevision(1))
    }
}

pub(super) fn validate(
    key: Key,
    row: &Row,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    read.charge(128)?;
    match (key, row) {
        (Key::ArtifactIdentity(hash), Row::ArtifactIdentity(id)) => {
            let artifact = read.artifact(*id)?.descriptor();
            require(
                artifact.id() == *id
                    && artifact.ledger() == read.ledger
                    && artifact.content_hash() == hash,
            )
        }
        (Key::WorkSlot(cycle, slot), Row::WorkSlot(id)) => {
            let Row::Work(work) = read.require(Key::Work(*id))? else {
                return Err(invalid());
            };
            let work = work.get().ok_or_else(invalid)?.state;
            require(
                work.reference().id == *id
                    && work.slot() == slot
                    && work.claim() == cycle.claim
                    && work.receipt().receipt == cycle.receipt
                    && work.receipt().epoch == cycle.epoch
                    && work.cycle() == cycle.cycle,
            )
        }
        (Key::Artifact(id), Row::Artifact(row)) => {
            artifact(id, row.get().ok_or_else(invalid)?, read, history)
        }
        (Key::Work(id), Row::Work(row)) => {
            work::work(id, row.get().ok_or_else(invalid)?, read, history)
        }
        (Key::Diagnostic(id), Row::Diagnostic(row)) => {
            work::diagnostic(id, row.get().ok_or_else(invalid)?, read, history)
        }
        (Key::Response(id), Row::Response(row)) => {
            response::validate(id, row.record().ok_or_else(invalid)?, read, history)
        }
        (Key::Accepted(key), Row::Accepted(row)) => {
            accepted(key, row.get().ok_or_else(invalid)?, read, history)
        }
        (Key::DeliveryResult(key), Row::DeliveryResult(row)) => {
            let row = row.get().ok_or_else(invalid)?;
            pure(
                key,
                row.result(),
                PublicationPosition {
                    sequence: row.sequence(),
                    ordinal: row.ordinal(),
                },
                true,
                read,
                history,
            )
        }
        (Key::MissingResult(key), Row::MissingResult(row)) => {
            let row = row.get().ok_or_else(invalid)?;
            pure(
                key,
                row.result(),
                PublicationPosition {
                    sequence: row.sequence(),
                    ordinal: row.ordinal(),
                },
                false,
                read,
                history,
            )
        }
        _ => Ok(()),
    }
}
fn artifact(
    id: ArtifactId,
    value: &NativeArtifact,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let descriptor = value.descriptor();
    require(descriptor.id() == id && descriptor.ledger() == read.ledger)?;
    require(
        matches!(read.require(Key::ArtifactIdentity(descriptor.content_hash()))?, Row::ArtifactIdentity(found) if *found == id),
    )?;
    let event = one(history, read, Key::Artifact(id))?;
    require(
        event.fact
            == NativeFact::Artifact {
                binding: descriptor.binding(),
            },
    )?;
    let request = actor(event, descriptor.producer())?;
    value.custody().check(request, descriptor)?;
    require(value.custody().local_revision() == 1)?;
    let provenance = match (descriptor.work_provenance(), descriptor.result_provenance()) {
        (Some(work), None) => {
            let claim = read.claim(work.claim)?;
            let fence = descriptor.receipt().ok_or_else(invalid)?;
            match work.role {
                WorkRole::Output { slot } => {
                    let Row::Work(row) = read.require(Key::Work(id))? else {
                        return Err(invalid());
                    };
                    let state = row.get().ok_or_else(invalid)?.state;
                    require(
                        state.reference()
                            == (focal_model::ArtifactRef {
                                id: descriptor.id(),
                                hash: descriptor.content_hash(),
                            })
                            && state.claim() == work.claim
                            && state.slot() == slot
                            && state.cycle() == work.cycle,
                    )?;
                    receipt(read, work.claim, fence, descriptor.producer())?;
                }
                WorkRole::Diagnostic { reason } => {
                    let Row::Diagnostic(row) = read.require(Key::Diagnostic(id))? else {
                        return Err(invalid());
                    };
                    let diagnostic = row.get().ok_or_else(invalid)?.diagnostic.snapshot_v1();
                    require(
                        diagnostic.claim == work.claim
                            && diagnostic.cycle == work.cycle
                            && diagnostic.diagnostic.reason == reason
                            && diagnostic.diagnostic.artifact
                                == (focal_model::ArtifactRef {
                                    id: descriptor.id(),
                                    hash: descriptor.content_hash(),
                                }),
                    )?;
                    receipt(read, work.claim, fence, descriptor.producer())?;
                }
                WorkRole::ReceiptRejection { artifact, reason } => {
                    let Row::Work(row) = read.require(Key::Work(artifact.id))? else {
                        return Err(invalid());
                    };
                    let work = row.get().ok_or_else(invalid)?.state;
                    require(
                        descriptor.producer() == claim.issuer()
                            && work.reference() == artifact
                            && work.receipt() == fence
                            && work.diagnostic()
                                == Some(evidence::Diagnostic {
                                    reason,
                                    artifact: focal_model::ArtifactRef {
                                        id: descriptor.id(),
                                        hash: descriptor.content_hash(),
                                    },
                                }),
                    )?;
                }
            }
            true
        }
        (None, Some(result)) => {
            let event = read.event(event.sequence, 2)?;
            let NativeFact::Accepted { key } = event.fact else {
                return Err(invalid());
            };
            let Row::Accepted(row) = read.require(Key::Accepted(key))? else {
                return Err(invalid());
            };
            let accepted = row.get().ok_or_else(invalid)?;
            require(
                accepted.artifact().reference()
                    == (focal_model::ArtifactRef {
                        id: descriptor.id(),
                        hash: descriptor.content_hash(),
                    })
                    && accepted.attempt() == result.attempt,
            )?;
            true
        }
        _ => false,
    };
    require(provenance)?;
    read.charge(
        descriptor
            .inputs()
            .len()
            .checked_add(1)
            .ok_or_else(invalid)?,
    )?;
    for input in descriptor.inputs() {
        require(input.ledger == read.ledger)?;
        let object = match input.kind {
            ObjectKind::Claim => Key::Claim(ClaimId(input.id.0)),
            ObjectKind::Validation => Key::Definition(ValidationId(input.id.0)),
            ObjectKind::Artifact => Key::Artifact(ArtifactId(input.id.0)),
            ObjectKind::Testament => Key::Response(TestamentId(input.id.0)),
        };
        read.require(object)?;
        let original = history.first(object, read)?.ok_or_else(invalid)?;
        require(original.sequence < event.sequence)?;
        if let ObjectKind::Artifact = input.kind {
            let source = read.artifact(ArtifactId(input.id.0))?.descriptor();
            let mut actual = descriptor.visibility();
            for required in source.visibility() {
                read.charge(1)?;
                loop {
                    read.charge(
                        required
                            .len()
                            .checked_mul(2)
                            .and_then(|n| n.checked_add(4))
                            .ok_or_else(invalid)?,
                    )?;
                    match actual.next() {
                        Some(label) if label < required => continue,
                        Some(label) if label == required => break,
                        _ => return Err(invalid()),
                    }
                }
            }
            read.charge(1)?;
        }
    }
    Ok(())
}
fn accepted(
    key: NativeResultKey,
    value: &NativeAccepted,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let result = value.result();
    require(key == NativeResultKey::of(result))?;
    let published = PublicationPosition {
        sequence: value.sequence(),
        ordinal: value.ordinal(),
    };
    let event = one(history, read, Key::Accepted(key))?;
    require(
        position(event) == published
            && event.fact == NativeFact::Accepted { key }
            && event.ordinal == 2,
    )?;
    let artifact = exact_artifact(read, value.artifact().reference())?;
    let descriptor = artifact.descriptor();
    actor(event, value.attempt().evaluator)?;
    require(
        descriptor.result_provenance()
            == Some(ResultProvenance {
                claim: result.claim(),
                validation: result.validation(),
                target: result.target(),
                generation: result.generation(),
                attempt: value.attempt(),
                value: result.verdict(),
            })
            && descriptor.receipt() == result.receipt(),
    )?;
    let authored = one(history, read, Key::Artifact(descriptor.id()))?;
    require(
        authored.sequence == event.sequence
            && authored.ordinal == 0
            && authored.invocation == event.invocation,
    )?;
    let reported = read.event(event.sequence, 1)?;
    require(reported.invocation == event.invocation)?;
    match reported.fact {
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Reported,
            key: reported_key,
            after,
            state,
            phase,
            attempt,
            ..
        } => {
            require(
                reported_key == key.evaluation
                    && after == result.binding()
                    && state == result.resulting_state()
                    && (phase == result.phase()
                        || (result.phase() == validation::Phase::Programmatic
                            && phase == validation::Phase::Quality
                            && result.verdict() == VerdictValue::Pass))
                    && attempt == Some(value.attempt()),
            )?;
        }
        _ => return Err(invalid()),
    }
    target(result, published, read, history)
}
fn pure(
    key: NativeResultKey,
    result: validation::AcceptedResult,
    published: PublicationPosition,
    delivery: bool,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    require(key == NativeResultKey::of(result))?;
    let object = if delivery {
        Key::DeliveryResult(key)
    } else {
        Key::MissingResult(key)
    };
    let event = one(history, read, object)?;
    let expected = if delivery {
        NativeFact::Delivery { key }
    } else {
        NativeFact::Missing { key }
    };
    require(position(event) == published && event.fact == expected)?;
    target(result, published, read, history)
}
fn target(
    result: validation::AcceptedResult,
    published: PublicationPosition,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let claim = read.claim(result.claim())?;
    let declaration = read.definition(result.validation())?;
    require(
        result.ledger() == read.ledger
            && declaration.claim() == result.claim()
            && result.declaration_index() == declaration.declaration_index(),
    )?;
    let evaluation_key = NativeResultKey::of(result).evaluation;
    let Row::Evaluation(current) = read.require(Key::Evaluation(evaluation_key))? else {
        return Err(invalid());
    };
    let current = current.get().ok_or_else(invalid)?;
    require(
        current.target() == result.target()
            && current.receipt() == result.receipt()
            && current.binding().revision >= result.binding().revision,
    )?;
    if result.phase() == validation::Phase::Delivery {
        let ready = history
            .first(Key::Evaluation(evaluation_key), read)?
            .ok_or_else(invalid)?;
        let NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Materialized,
            before: None,
            after,
            state: validation::State::Ready,
            ..
        } = ready.fact
        else {
            return Err(invalid());
        };
        require(
            after.next()? == result.binding()
                && ready.sequence == published.sequence
                && ready.ordinal < published.ordinal,
        )?;
    } else {
        let event = history
            .revision(
                Key::Evaluation(evaluation_key),
                result.binding().revision.0,
                read,
            )?
            .ok_or_else(invalid)?;
        let NativeFact::Evaluation { after, state, .. } = event.fact else {
            return Err(invalid());
        };
        require(
            after == result.binding()
                && state == result.resulting_state()
                && event.sequence == published.sequence
                && event.ordinal < published.ordinal,
        )?;
    }
    match result.target() {
        validation::Target::Artifact {
            response,
            slot,
            artifact,
        } => {
            let record = response::record(read, TestamentId(response.object.0))?;
            response_frame(record, result)?;
            historical_response_binding(record, response, published, read, history)?;
            require(record.response().identity().claim == result.claim())?;
            let reference = focal_model::ArtifactRef {
                id: ArtifactId(artifact.object.0),
                hash: artifact.content,
            };
            require(
                record
                    .response()
                    .manifest()
                    .binary_search_by_key(&slot, |item| item.slot)
                    .ok()
                    .and_then(|index| record.response().manifest().get(index))
                    .is_some_and(|item| item.artifact == reference),
            )?;
            require(record.received().is_some_and(|at| !later(at, published)))?;
            let event = history
                .revision(Key::Work(reference.id), artifact.revision.0, read)?
                .ok_or_else(invalid)?;
            require(
                matches!(event.fact, NativeFact::Work { after, state: WorkArtifactState::Attached | WorkArtifactState::Validating, .. } if after == artifact)
                    && !later(position(event), published),
            )?;
            receipt(
                read,
                result.claim(),
                result.receipt().ok_or_else(invalid)?,
                record.response().respondent(),
            )?;
        }
        validation::Target::MissingSlot { response, slot } => {
            let record = response::record(read, TestamentId(response.object.0))?;
            response_frame(record, result)?;
            historical_response_binding(record, response, published, read, history)?;
            require(
                record.response().identity().claim == result.claim()
                    && record
                        .response()
                        .manifest()
                        .binary_search_by_key(&slot, |item| item.slot)
                        .is_err()
                    && record.received().is_some_and(|at| !later(at, published)),
            )?;
        }
        validation::Target::Delivery { response } => {
            let record = response::record(read, TestamentId(response.object.0))?;
            response_frame(record, result)?;
            historical_response_binding(record, response, published, read, history)?;
            require(
                record.response().identity().claim == result.claim()
                    && record.received().is_some_and(|at| {
                        at.sequence == published.sequence && at.ordinal < published.ordinal
                    }),
            )?;
        }
        validation::Target::Admission { claim: bound } => {
            require(
                bound.ledger == read.ledger
                    && bound.object == claim.binding().object
                    && bound.content == claim.binding().content,
            )?;
            historical_claim_binding(bound, published, read, history)?;
        }
        validation::Target::Increment {
            claim: bound,
            artifact,
        } => {
            require(
                bound.ledger == read.ledger
                    && bound.object == claim.binding().object
                    && bound.content == claim.binding().content,
            )?;
            historical_claim_binding(bound, published, read, history)?;
            let Row::Work(row) = read.require(Key::Work(ArtifactId(artifact.object.0)))? else {
                return Err(invalid());
            };
            let work = row.get().ok_or_else(invalid)?.state;
            require(
                work.claim() == result.claim()
                    && work.binding().content == artifact.content
                    && work.receipt() == result.receipt().ok_or_else(invalid)?,
            )?;
            let original = history
                .revision(
                    Key::Work(ArtifactId(artifact.object.0)),
                    artifact.revision.0,
                    read,
                )?
                .ok_or_else(invalid)?;
            require(
                matches!(original.fact, NativeFact::Work { after, .. } if after == artifact)
                    && !later(position(original), published),
            )?;
        }
    }
    Ok(())
}
fn response_frame(
    record: &NativeResponseRecord,
    result: validation::AcceptedResult,
) -> Result<(), NativeError> {
    require(
        Some(record.response().identity().receipt) == result.receipt()
            && u64::from(record.response().identity().cycle) == result.generation(),
    )
}
fn historical_claim_binding(
    binding: Binding,
    published: PublicationPosition,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let original = history
        .revision(
            Key::Claim(ClaimId(binding.object.0)),
            binding.revision.0,
            read,
        )?
        .ok_or_else(invalid)?;
    require(
        matches!(original.fact, NativeFact::Claim(claim) if claim.after == binding)
            && !later(position(original), published),
    )
}
fn historical_response_binding(
    record: &NativeResponseRecord,
    binding: Binding,
    published: PublicationPosition,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let event = history
        .revision(
            Key::Response(TestamentId(record.generated().object.0)),
            binding.revision.0,
            read,
        )?
        .ok_or_else(invalid)?;
    require(
        matches!(event.fact, NativeFact::Response { after, state: ResponseState::Received | ResponseState::Validating, .. } if after == binding)
            && !later(position(event), published),
    )
}
/// A designated evaluator's real Begin may observe response/work entry in the
/// same transaction. This is retained provenance, not claimant impersonation.
fn entry_actor(
    event: NativeEvent,
    issuer: ParticipantId,
    response: TestamentId,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let NativeInvocation::Request(request) = event.invocation else {
        return Err(invalid());
    };
    if request.principal == issuer {
        return Ok(());
    }
    let begin = read.event(event.sequence, 0)?;
    require(begin.invocation == event.invocation && begin.ordinal < event.ordinal)?;
    match begin.fact {
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Begun,
            key,
            attempt: Some(attempt),
            ..
        } => require(
            attempt.evaluator == request.principal
                && matches!(key.target, EvaluationTarget::Work { response: id, .. } if id == response),
        ),
        _ => Err(invalid()),
    }
}
