use super::*;
use focal_model::lifecycle::aggregation::PublicationPosition;
use focal_model::{ObjectKind, RelationTarget};

pub(super) fn check<O: Overlay>(
    key: Key,
    value: &Row,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    read.charge(256)?;
    match (key, value) {
        (Key::Definition(id), Row::Definition(row)) => {
            let definition = row.get().ok_or_else(invalid)?;
            let claim = read.claim(definition.claim())?;
            read.charge(const { (usize::BITS as usize + 1) * 16 })?;
            claim.acceptance().check_declaration(definition)?;
            require(
                definition.binding().object.0 == id.0 && claim.created() == read.outcome.sequence,
            )?;
            let event = read.only(key)?;
            require(
                event.fact
                    == (NativeFact::Definition {
                        binding: definition.binding(),
                        claim: definition.claim(),
                        index: definition.declaration_index(),
                        intent: definition.intent_fingerprint(),
                    }),
            )?;
            match (read.profile, row.descriptor()) {
                (NativeContentProfile::ProjectionOnly, None) => (),
                (NativeContentProfile::AuthoredV1, Some(descriptor)) => {
                    require(
                        matches!(read.require(Key::DefinitionIdentity(descriptor.schema(), descriptor.content_hash()))?,
                        Row::DefinitionIdentity(found) if *found == id),
                    )?;
                    let Row::ClaimContent(body) =
                        read.require(Key::ClaimContent(definition.claim()))?
                    else {
                        return Err(invalid());
                    };
                    let body = body.get().ok_or_else(invalid)?;
                    let failure = std::cell::Cell::new(None);
                    let checked = read.meter.budget(|visits| {
                        authored::check_recorded_declaration(body, claim, descriptor, visits)
                            .map_err(|error| {
                                failure.set(Some(error));
                                ContractError::InvalidPolicy
                            })
                    });
                    if let Some(error) = failure.take() {
                        return Err(error);
                    }
                    checked?;
                }
                _ => return Err(invalid()),
            }
        }
        (Key::Artifact(id), Row::Artifact(row)) => {
            let descriptor = row.get().ok_or_else(invalid)?.descriptor();
            require(
                descriptor.id() == id
                    && read.only(key)?.fact
                        == (NativeFact::Artifact {
                            binding: descriptor.binding(),
                        }),
            )?;
            require(
                matches!(read.require(Key::ArtifactIdentity(descriptor.content_hash()))?, Row::ArtifactIdentity(found) if *found == id),
            )?;
        }
        (Key::ArtifactIdentity(hash), Row::ArtifactIdentity(id)) => {
            require(read.artifact(*id)?.descriptor().content_hash() == hash)?;
        }
        (Key::Receipt(id), Row::Receipt(row)) => {
            let event = read.only(key)?;
            let claim = read.claim(row.claim)?;
            require(row.fence.receipt == id && row.acquired == event.sequence)?;
            match event.fact {
                NativeFact::Receipt {
                    claim: binding,
                    fence,
                    holder,
                } => {
                    require(
                        binding.object.0 == row.claim.0
                            && fence == row.fence
                            && holder == row.holder
                            && fence.epoch == 1,
                    )?;
                    let old = as_claim(read.before(Key::Claim(row.claim))?).ok_or_else(invalid)?;
                    require(old.receipt().is_none())?;
                }
                NativeFact::ReceiptAdopted {
                    claim: binding,
                    previous,
                    replacement,
                    ..
                } => {
                    require(
                        binding.object.0 == row.claim.0
                            && replacement.fence == row.fence
                            && replacement.holder == row.holder
                            && previous.fence.epoch.checked_add(1) == Some(row.fence.epoch),
                    )?;
                    let old = as_claim(read.before(Key::Claim(row.claim))?).ok_or_else(invalid)?;
                    require(old.receipt() == Some(previous))?;
                    let Row::Receipt(receipt) =
                        read.require(Key::Receipt(previous.fence.receipt))?
                    else {
                        return Err(invalid());
                    };
                    require(
                        receipt.claim == row.claim
                            && receipt.fence == previous.fence
                            && receipt.holder == previous.holder,
                    )?;
                }
                _ => return Err(invalid()),
            }
            require(
                claim.receipt()
                    == Some(ReceiptEntitlement {
                        fence: row.fence,
                        holder: row.holder,
                    }),
            )?;
        }
        (Key::Diagnostic(id), Row::Diagnostic(row)) => {
            let value = row.get().ok_or_else(invalid)?;
            let diagnostic = value.diagnostic;
            let event = read.only(key)?;
            let descriptor = read.artifact(id)?.descriptor();
            require(
                matches!(event.fact, NativeFact::Diagnostic { claim, binding, reason }
                if claim == diagnostic.claim() && binding == descriptor.binding() && reason == diagnostic.diagnostic().reason),
            )?;
            require(diagnostic.diagnostic().artifact.id == id)?;
            let Row::Receipt(owner) = read.require(Key::Receipt(diagnostic.receipt().receipt))?
            else {
                return Err(invalid());
            };
            require(
                owner.claim == diagnostic.claim()
                    && owner.fence == diagnostic.receipt()
                    && owner.holder == diagnostic.producer(),
            )?;
            let receipt = diagnostic.receipt();
            let cycle = NativeCycleKey {
                claim: diagnostic.claim(),
                receipt: receipt.receipt,
                epoch: receipt.epoch,
                cycle: diagnostic.cycle(),
            };
            read.require(Key::Cycle(cycle))?;
        }
        (Key::Accepted(key), Row::Accepted(row)) => {
            let value = row.get().ok_or_else(invalid)?;
            published(
                Key::Accepted(key),
                key,
                value.result(),
                value.sequence(),
                value.ordinal(),
                read,
            )?;
            require(read.only(Key::Accepted(key))?.fact == (NativeFact::Accepted { key }))?;
            let previous = value.ordinal().checked_sub(1).ok_or_else(invalid)?;
            require(
                matches!(read.event(previous)?.fact, NativeFact::Evaluation { kind: NativeEvaluationEventKind::Reported,
                key: actual, after, attempt: Some(attempt), .. }
                if actual == key.evaluation && after == value.result().binding() && attempt == value.attempt()),
            )?;
        }
        (Key::DeliveryResult(key), Row::DeliveryResult(row)) => {
            let value = row.get().ok_or_else(invalid)?;
            published(
                Key::DeliveryResult(key),
                key,
                value.result(),
                value.sequence(),
                value.ordinal(),
                read,
            )?;
            require(read.only(Key::DeliveryResult(key))?.fact == (NativeFact::Delivery { key }))?;
        }
        (Key::MissingResult(key), Row::MissingResult(row)) => {
            let value = row.get().ok_or_else(invalid)?;
            published(
                Key::MissingResult(key),
                key,
                value.result(),
                value.sequence(),
                value.ordinal(),
                read,
            )?;
            require(read.only(Key::MissingResult(key))?.fact == (NativeFact::Missing { key }))?;
            let previous = value.ordinal().checked_sub(1).ok_or_else(invalid)?;
            require(
                matches!(read.event(previous)?.fact, NativeFact::Evaluation { kind: NativeEvaluationEventKind::MissingTarget,
                key: actual, after, .. } if actual == key.evaluation && after == value.result().binding()),
            )?;
        }
        (Key::WorkSlot(cycle, slot), Row::WorkSlot(id)) => {
            let work = as_work(Some(read.require(Key::Work(*id))?))
                .ok_or_else(invalid)?
                .state;
            require(
                work.reference().id == *id
                    && work.slot() == slot
                    && work.claim() == cycle.claim
                    && work.receipt().receipt == cycle.receipt
                    && work.receipt().epoch == cycle.epoch
                    && work.cycle() == cycle.cycle,
            )?;
        }
        (Key::ClaimResultTestament(claim), Row::ClaimResultTestament(id)) => {
            let Row::ResultTestament(value) = read.require(Key::ResultTestament(*id))? else {
                return Err(invalid());
            };
            require(value.get().ok_or_else(invalid)?.testament().claim() == claim)?;
        }
        (Key::ClaimContent(_), Row::ClaimContent(_))
        | (Key::ClaimIdentity(..), Row::ClaimIdentity(_))
        | (Key::DefinitionIdentity(..), Row::DefinitionIdentity(_))
        | (Key::CreationResult(_), Row::CreationResult(_)) => authored(key, value, read)?,
        _ => (),
    }
    Ok(())
}
fn published<O: Overlay>(
    row: Key,
    key: NativeResultKey,
    result: validation::AcceptedResult,
    sequence: SessionSeq,
    ordinal: u32,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    require(NativeResultKey::of(result) == key && sequence == read.outcome.sequence)?;
    let event = read.only(row)?;
    require(event.ordinal == ordinal)?;
    let evaluation =
        as_evaluation(Some(read.require(Key::Evaluation(key.evaluation))?)).ok_or_else(invalid)?;
    require(evaluation.binding().revision >= result.binding().revision)?;
    let publication = PublicationPosition { sequence, ordinal };
    match result.target() {
        validation::Target::Artifact { response, .. }
        | validation::Target::MissingSlot { response, .. } => {
            let response = response_reads::as_response_record(Some(
                read.require(Key::Response(TestamentId(response.object.0)))?,
            ))
            .ok_or_else(invalid)?;
            require(
                response
                    .entered()
                    .is_some_and(|entered| entered < publication),
            )?;
        }
        validation::Target::Delivery { response } => {
            let response = response_reads::as_response_record(Some(
                read.require(Key::Response(TestamentId(response.object.0)))?,
            ))
            .ok_or_else(invalid)?;
            require(
                response
                    .received()
                    .is_some_and(|received| received < publication),
            )?;
        }
        _ => (),
    }
    Ok(())
}
fn authored<O: Overlay>(
    key: Key,
    row: &Row,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    require(read.profile == NativeContentProfile::AuthoredV1)?;
    match (key, row) {
        (Key::ClaimContent(id), Row::ClaimContent(value)) => {
            let body = value.get().ok_or_else(invalid)?;
            let binding = read.claim(id)?.binding();
            require(
                body.id() == id
                    && body.ledger() == read.ledger
                    && body.binding().content == binding.content,
            )?;
            require(
                matches!(read.require(Key::ClaimIdentity(body.schema(), body.content_hash()))?, Row::ClaimIdentity(found) if *found == id),
            )?;
            read.charge(add(
                add(body.requirements().len(), body.relations().len())?,
                1,
            )?)?;
            for requirement in body.requirements() {
                read.definition(requirement.id)?;
            }
            for relation in body.relations() {
                if let RelationTarget::Object(target) = relation.target {
                    require(target.ledger == read.ledger && target.kind == ObjectKind::Claim)?;
                    read.claim(ClaimId(target.id.0))?;
                }
            }
        }
        (Key::ClaimIdentity(schema, hash), Row::ClaimIdentity(id)) => {
            let Row::ClaimContent(value) = read.require(Key::ClaimContent(*id))? else {
                return Err(invalid());
            };
            let body = value.get().ok_or_else(invalid)?;
            require(body.schema() == schema && body.content_hash() == hash)?;
        }
        (Key::DefinitionIdentity(schema, hash), Row::DefinitionIdentity(id)) => {
            let Row::Definition(value) = read.require(Key::Definition(*id))? else {
                return Err(invalid());
            };
            let body = value.descriptor().ok_or_else(invalid)?;
            require(body.schema() == schema && body.content_hash() == hash)?;
        }
        (Key::CreationResult(invocation), Row::CreationResult(value)) => {
            require(
                invocation == read.outcome.invocation
                    && read.outcome.operation == NativeOperation::Create,
            )?;
            read.charge(add(value.get().entries().len(), 1)?)?;
            for object in value.get().entries() {
                let actual = match object.family {
                    NativeCreatedFamily::Claim => {
                        match read.require(Key::ClaimIdentity(object.schema, object.content))? {
                            Row::ClaimIdentity(id) => id.0,
                            _ => return Err(invalid()),
                        }
                    }
                    NativeCreatedFamily::Validation => match read
                        .require(Key::DefinitionIdentity(object.schema, object.content))?
                    {
                        Row::DefinitionIdentity(id) => id.0,
                        _ => return Err(invalid()),
                    },
                };
                require(
                    actual == object.resolved.0
                        && (read.outcome.created == 0 || object.requested == object.resolved),
                )?;
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}
