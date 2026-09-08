//! New result testaments freeze the complete already validated predecessor.
//! Registry membership gives exact evaluation keys and original revisions;
//! bounded revision probes cover every retained result without scanning a root.
use super::*;
use focal_model::ObjectRevision;
use focal_model::lifecycle::{
    aggregation::{CauseTarget, PublicationPosition},
    audit::{AuditMember, AuditMemberSnapshotV1},
    validation::{AcceptedResult, Phase, Target},
};

#[cfg(test)]
#[path = "replay_validate_audit_tests.rs"]
mod tests;

type MemberOrder = (CauseTarget, u32, u64, ValidationId);
fn order(target: Target, index: u32, generation: u64, validation: ValidationId) -> MemberOrder {
    let target = match target {
        Target::Artifact { response, .. }
        | Target::MissingSlot { response, .. }
        | Target::Delivery { response } => CauseTarget::Response(TestamentId(response.object.0)),
        Target::Admission { .. } => CauseTarget::Admission,
        Target::Increment { artifact, .. } => CauseTarget::Increment {
            artifact: ArtifactId(artifact.object.0),
            content: artifact.content,
        },
    };
    (target, index, generation, validation)
}
fn member_order(member: &AuditMember) -> MemberOrder {
    let member = member.snapshot_v1();
    order(
        member.key.target,
        member.declaration_index,
        member.key.generation,
        member.key.validation,
    )
}
fn result_order(result: &AcceptedResult) -> (MemberOrder, Option<u32>, u8) {
    let phase = match result.phase() {
        Phase::Programmatic => 0,
        Phase::Quality => 1,
        Phase::Delivery => 2,
        Phase::MissingTarget => 3,
    };
    (
        order(
            result.target(),
            result.declaration_index(),
            result.generation(),
            result.validation(),
        ),
        result.attempt(),
        phase,
    )
}
fn search_work() -> Result<usize, NativeError> {
    mul(
        add(
            usize::try_from(usize::BITS).map_err(|_| ContractError::Capacity)?,
            1,
        )?,
        1024,
    )
}

pub(super) fn generated<O: Overlay>(
    id: TestamentId,
    value: &NativeResultTestament,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    read.charge(1024)?;
    let testament = value.testament();
    let cohort = testament.cohort();
    let claim_id = testament.claim();
    require(
        value.captured_at() == read.base
            && value.generated_at().sequence == read.outcome.sequence
            && testament.binding().ledger == read.ledger
            && testament.binding().object.0 == id.0
            && read.before(Key::ResultTestament(id))?.is_none()
            && read.before(Key::ClaimResultTestament(claim_id))?.is_none()
            && read.get(Key::Response(id))?.is_none(),
    )?;
    let Some(Row::Claim(owner)) = read.before(Key::Claim(claim_id))? else {
        return Err(invalid());
    };
    let claim = owner.claim().ok_or_else(invalid)?;
    let registry = owner.registrations().ok_or_else(invalid)?;
    let header = registry.snapshot_v1();
    require(
        cohort.claim_binding() == claim.binding()
            && cohort.issuer() == claim.issuer()
            && header.sealed
            && header.increments_sealed
            && header.sealed_at == Some(cohort.sealed_at())
            && claim.local_sealed_at() == Some(cohort.sealed_at())
            && (claim.local_complete() || claim.status().is_terminal())
            && registry.rows().len() == cohort.members().len()
            && cohort.results().len() == value.publications().len()
            && cohort.result_capacity() == cohort.results().len(),
    )?;
    if registry.rows().len() > read.limits.evaluations_per_claim
        || cohort.results().len() > read.limits.results
    {
        return Err(ContractError::Capacity.into());
    }
    // Intrinsic hydration has proved strict canonical member/result ordering.
    // Resolve every original registered member into that unique collection;
    // equal cardinality closes both omissions and additions without a set.
    read.charge(add(registry.rows().len(), 1)?)?;
    let mut results = 0usize;
    for registered in registry.rows() {
        read.charge(1024)?;
        let key = transactions::key_for_registered(claim_id, *registered);
        let Some(Row::Evaluation(owned)) = read.before(Key::Evaluation(key))? else {
            return Err(invalid());
        };
        let state = owned.get().ok_or_else(invalid)?;
        let Some(Row::Definition(owned)) = read.before(Key::Definition(key.validation))? else {
            return Err(invalid());
        };
        let declaration = owned.get().ok_or_else(invalid)?;
        require(declaration.claim() == claim_id && declaration.binding().ledger == read.ledger)?;
        registered.check_state(*state, declaration)?;
        let snapshot = state.snapshot_v1()?;
        let expected = AuditMemberSnapshotV1 {
            key: focal_model::lifecycle::audit::EvaluationKey {
                validation: key.validation,
                target: snapshot.target,
                generation: snapshot.generation,
            },
            declaration_index: declaration.declaration_index(),
            binding: snapshot.binding,
            receipt: snapshot.receipt,
            begun: snapshot.begun,
            state: snapshot.state,
            suppression: snapshot.suppression,
            fence: snapshot.fence,
            last_result: snapshot.last_result,
            sealed: snapshot.sealed,
        };
        read.charge(search_work()?)?;
        let expected_order = order(
            snapshot.target,
            declaration.declaration_index(),
            snapshot.generation,
            key.validation,
        );
        let position = cohort
            .members()
            .binary_search_by_key(&expected_order, member_order)
            .map_err(|_| invalid())?;
        let member = cohort.members().get(position).ok_or_else(invalid)?;
        require(member.complete() && member.snapshot_v1() == expected)?;
        results = add(
            results,
            coverage(
                value,
                key,
                registered.binding().revision,
                snapshot.binding.revision,
                declaration.attempt_bound(),
                read,
            )?,
        )?;
        if results > read.limits.results {
            return Err(ContractError::Capacity.into());
        }
    }
    require(results == cohort.results().len() && results == value.publications().len())
}

fn coverage<O: Overlay>(
    value: &NativeResultTestament,
    evaluation: EvaluationKey,
    original: ObjectRevision,
    current: ObjectRevision,
    attempts: u32,
    read: &ReplayRead<'_, '_, O>,
) -> Result<usize, NativeError> {
    read.charge(256)?;
    let span = current
        .0
        .checked_sub(original.0)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(invalid)?;
    // A chain can begin once, report at most its admitted attempts, and acquire
    // at most one cohort seal and one authority fence. Include fixed slack for
    // pure Delivery/MissingTarget and the initial materialization revision.
    let ceiling = u64::from(attempts.max(1))
        .checked_mul(2)
        .and_then(|value| value.checked_add(4))
        .ok_or(ContractError::Capacity)?;
    require(span <= ceiling)?;
    let span = usize::try_from(span).map_err(|_| ContractError::Capacity)?;
    read.charge(add(span, 1)?)?;
    let mut found = 0usize;
    let mut last_position = None;
    for offset in 0..span {
        read.charge(512)?;
        let revision = original
            .0
            .checked_add(u64::try_from(offset).map_err(|_| ContractError::Capacity)?)
            .ok_or(ContractError::Capacity)?;
        let key = NativeResultKey {
            evaluation,
            revision: ObjectRevision(revision),
        };
        let keys = [
            Key::Accepted(key),
            Key::DeliveryResult(key),
            Key::MissingResult(key),
        ];
        read.charge(add(keys.len(), 1)?)?;
        let mut at_revision = false;
        for address in keys {
            let Some(row) = read.before(address)? else {
                continue;
            };
            let (result, position) = match (address, row) {
                (Key::Accepted(_), Row::Accepted(owned)) => {
                    let row = owned.get().ok_or_else(invalid)?;
                    require(!matches!(
                        evaluation.target,
                        EvaluationTarget::Delivery { .. } | EvaluationTarget::MissingSlot { .. }
                    ))?;
                    (
                        row.result(),
                        PublicationPosition {
                            sequence: row.sequence(),
                            ordinal: row.ordinal(),
                        },
                    )
                }
                (Key::DeliveryResult(_), Row::DeliveryResult(owned)) => {
                    let row = owned.get().ok_or_else(invalid)?;
                    require(matches!(
                        evaluation.target,
                        EvaluationTarget::Delivery { .. }
                    ))?;
                    (
                        row.result(),
                        PublicationPosition {
                            sequence: row.sequence(),
                            ordinal: row.ordinal(),
                        },
                    )
                }
                (Key::MissingResult(_), Row::MissingResult(owned)) => {
                    let row = owned.get().ok_or_else(invalid)?;
                    require(matches!(
                        evaluation.target,
                        EvaluationTarget::MissingSlot { .. }
                    ))?;
                    (
                        row.result(),
                        PublicationPosition {
                            sequence: row.sequence(),
                            ordinal: row.ordinal(),
                        },
                    )
                }
                _ => return Err(invalid()),
            };
            require(
                !at_revision
                    && NativeResultKey::of(result) == key
                    && result.ledger() == read.ledger
                    && position.sequence.0 != 0
                    && position.sequence <= read.base
                    && last_position.is_none_or(|previous| previous < position),
            )?;
            at_revision = true;
            last_position = Some(position);
            // Compare the complete retained result, including semantic stamps,
            // then its original publication. A new same-key surrogate cannot
            // replace a result frozen in the predecessor.
            read.charge(mul(search_work()?, 2)?)?;
            let at = value
                .testament()
                .results()
                .binary_search_by_key(&result_order(&result), result_order)
                .map_err(|_| invalid())?;
            require(
                value.testament().results().get(at) == Some(&result)
                    && value.publication(result) == Some(position),
            )?;
            found = add(found, 1)?;
            if found > usize::try_from(attempts.max(1)).map_err(|_| ContractError::Capacity)? {
                return Err(invalid());
            }
        }
    }
    Ok(found)
}
