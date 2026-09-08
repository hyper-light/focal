use super::super::report_tests::{
    Custody, EVALUATOR, ISSUER, artifact_spec, begin, binding, context, copy_report, core,
    creation, descriptor, key, post, report_for, request, running, verified,
};
use super::*;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use focal_model::lifecycle::{creation::Owner, succession::Lineage};
use focal_model::{
    Cause, ClaimId, ClaimStatus, ObjectRevision, SessionSeq, ValidationMode, VerdictValue,
};

fn stage(
    owner: &mut NativeOwner,
    time: u64,
    input: NativeInput,
) -> (NativeCandidate, NativeOutcome) {
    match owner
        .prepare(context(input.request.principal, time), input, None)
        .unwrap()
    {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        NativeStaging::Existing { .. } => panic!("expected a new owner candidate"),
    }
}

fn cancel(id: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Cancel { expected },
    }
}

fn child(id: u128, claim: u128, parent: Binding) -> NativeInput {
    let mut input = creation(id, claim, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!()
    };
    let row = claims.first_mut().unwrap();
    row.definition.lineage = Lineage::new(
        row.definition.binding,
        Cause::Claim(ClaimId(parent.object.0)),
        &[],
        1,
    )
    .unwrap();
    row.owner = Some(Owner {
        expected: parent,
        receipt: None,
    });
    input
}

fn pressured(budget: &MemoryBudget) -> focal_memory::Reservation {
    let stats = budget.stats();
    budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            stats.limit - stats.used,
        )
        .unwrap()
}

#[test]
fn one_owner_chain_exposes_distinct_candidate_and_committed_facts_until_ordered_publication() {
    let mut core = core();
    let pinned = core.pin_native(0, 1000).unwrap();
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (created, created_outcome) = stage(
        &mut owner,
        10,
        creation(1, 1, &[(ValidationMode::Required, false)], None),
    );
    let (posted, posted_outcome) = stage(&mut owner, 20, post(2, binding(1)));
    let claim = owner.effective().claim(key(1).claim).unwrap().binding();
    let evaluation = owner.effective().evaluation(key(1)).unwrap().binding();
    let (begun, begun_outcome) = stage(&mut owner, 30, begin(3, claim, 1, evaluation));
    assert_eq!(owner.pending_len(), 3);
    assert_eq!(owner.oldest(), Some(created));
    assert_eq!(owner.committed().sequence(), SessionSeq(0));
    assert!(owner.committed().claim(key(1).claim).is_none());
    assert_eq!(
        owner
            .candidate(created)
            .unwrap()
            .claim(key(1).claim)
            .unwrap()
            .status(),
        ClaimStatus::Generated
    );
    assert!(
        owner
            .candidate(created)
            .unwrap()
            .evaluation(key(1))
            .is_none()
    );
    assert_eq!(
        owner
            .candidate(posted)
            .unwrap()
            .evaluation(key(1))
            .unwrap()
            .state(),
        validation::State::Ready
    );
    assert_eq!(
        owner.effective().evaluation(key(1)).unwrap().state(),
        validation::State::Validating
    );
    assert_eq!(
        owner.effective().recorded(request(ISSUER, 1)),
        Some(created_outcome)
    );
    let before = budget.stats();
    assert!(matches!(
        owner.publish_after_durable(begun),
        Err(NativeOwnerError::OutOfOrder)
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.pending_len(), 3);
    assert_eq!(owner.oldest(), Some(created));
    assert_eq!(
        owner.publish_after_durable(created).unwrap(),
        created_outcome
    );
    assert_eq!(owner.oldest(), Some(posted));
    assert_eq!(
        owner.committed().claim(key(1).claim).unwrap().status(),
        ClaimStatus::Generated
    );
    assert_eq!(owner.effective().sequence(), begun_outcome.sequence);
    assert_eq!(owner.publish_after_durable(posted).unwrap(), posted_outcome);
    assert_eq!(owner.publish_after_durable(begun).unwrap(), begun_outcome);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(owner.oldest(), None);
    assert_eq!(owner.committed().sequence(), begun_outcome.sequence);
    assert_eq!(
        pinned
            .with_claim(key(1).claim, 1, |claim| claim.status())
            .unwrap(),
        None
    );
    for outcome in [created_outcome, posted_outcome, begun_outcome] {
        for ordinal in 0..outcome.events {
            let event = owner.committed().event(outcome.sequence, ordinal).unwrap();
            assert_eq!(event.invocation, outcome.invocation);
            assert_eq!(event.ordinal, ordinal);
        }
    }
}

#[test]
fn discarded_suffix_rewinds_owned_registry_and_reused_request_prefix_cannot_revive_old_tickets() {
    let core = core();
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (root, _) = stage(&mut owner, 10, creation(1, 1, &[], None));
    let survivor = budget.stats();
    let (old_child, child_outcome) = stage(&mut owner, 20, child(2, 2, binding(1)));
    let parent = owner
        .effective()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let (old_cancel, _) = stage(&mut owner, 30, cancel(3, parent));
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(2))
            .unwrap()
            .status(),
        ClaimStatus::Cancelled
    );
    assert_eq!(owner.discard_from(old_child).unwrap(), 2);
    assert_eq!(budget.stats(), survivor);
    assert_eq!(owner.oldest(), Some(root));
    assert_eq!(owner.pending_len(), 1);
    let view = owner.effective();
    let current = view.claim(ClaimId::from_u128(1)).unwrap();
    assert_eq!(current.binding(), binding(1));
    assert!(current.scopes().children().is_empty());
    assert!(owner.effective().claim(ClaimId::from_u128(2)).is_none());
    assert!(owner.effective().recorded(request(ISSUER, 2)).is_none());
    assert!(owner.effective().recorded(request(ISSUER, 3)).is_none());
    // The discarded clock 30 also disappears. Reusing the exact request at 20
    // recreates identical logical history but must never recreate its capability.
    let (new_child, replay) = stage(&mut owner, 20, child(2, 2, binding(1)));
    assert_eq!(replay, child_outcome);
    assert_ne!(new_child, old_child);
    let before = budget.stats();
    for stale in [old_child, old_cancel] {
        assert!(matches!(
            owner.candidate(stale),
            Err(NativeOwnerError::UnknownCandidate)
        ));
        assert!(matches!(
            owner.publish_after_durable(stale),
            Err(NativeOwnerError::UnknownCandidate)
        ));
        assert!(matches!(
            owner.discard_from(stale),
            Err(NativeOwnerError::UnknownCandidate)
        ));
    }
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.pending_len(), 2);
    owner.publish_after_durable(root).unwrap();
    owner.publish_after_durable(new_child).unwrap();
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .binding()
            .revision,
        ObjectRevision(2)
    );
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(2))
            .unwrap()
            .status(),
        ClaimStatus::Generated
    );
}

#[test]
fn identical_ledgers_ranges_requests_and_sequences_do_not_share_owner_tickets() {
    let mut left = NativeOwner::new(core()).unwrap();
    let mut right = NativeOwner::new(core()).unwrap();
    let (a, first) = stage(&mut left, 10, creation(1, 1, &[], None));
    let (b, second) = stage(&mut right, 10, creation(1, 1, &[], None));
    assert_eq!(first, second);
    assert_ne!(a, b);
    assert!(matches!(
        left.candidate(b),
        Err(NativeOwnerError::WrongOwner)
    ));
    assert!(matches!(
        left.publish_after_durable(b),
        Err(NativeOwnerError::WrongOwner)
    ));
    assert!(matches!(
        right.discard_from(a),
        Err(NativeOwnerError::WrongOwner)
    ));
    assert_eq!((left.oldest(), right.oldest()), (Some(a), Some(b)));
    left.publish_after_durable(a).unwrap();
    assert!(matches!(
        left.publish_after_durable(a),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    assert!(matches!(
        left.discard_from(a),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    right.publish_after_durable(b).unwrap();
    assert_eq!(left.committed().recorded(first.invocation), Some(first));
    assert_eq!(right.committed().recorded(second.invocation), Some(second));
    drop(left);
    let mut replacement = NativeOwner::new(core()).unwrap();
    let (c, _) = stage(&mut replacement, 10, creation(1, 1, &[], None));
    assert_ne!(a, c);
    assert!(matches!(
        replacement.publish_after_durable(a),
        Err(NativeOwnerError::WrongOwner)
    ));
    replacement.publish_after_durable(c).unwrap();
}

#[test]
fn full_queue_and_full_parent_pressure_preserve_exact_pending_and_committed_retries() {
    let mut core = core();
    core.limits.pending = 2;
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (first, original) = stage(&mut owner, 10, creation(1, 1, &[], None));
    let (second, _) = stage(&mut owner, 20, creation(2, 2, &[], None));
    let pressure = pressured(&budget);
    let before = budget.stats();
    assert_eq!(before.used, before.limit);
    assert!(
        matches!(owner.prepare(context(ISSUER, 9999), creation(1, 1, &[], None), None).unwrap(),
        NativeStaging::Existing { outcome, candidate: Some(ticket) } if outcome == original && ticket == first)
    );
    assert_eq!(budget.stats(), before);
    let mut conflicting = creation(1, 1, &[], None);
    let NativeCommand::Create { claims, .. } = &mut conflicting.command else {
        panic!()
    };
    claims[0].definition.max_responses += 1;
    assert!(matches!(
        owner.prepare(context(ISSUER, 21), conflicting, None),
        Err(NativeOwnerError::Native(NativeError::RequestConflict))
    ));
    assert!(matches!(
        owner.prepare(context(ISSUER, 21), creation(3, 3, &[], None), None),
        Err(NativeOwnerError::Native(NativeError::Capacity(_)))
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.pending_len(), 2);
    assert_eq!(owner.publish_after_durable(first).unwrap(), original);
    assert_eq!(owner.oldest(), Some(second));
    let published = budget.stats();
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), creation(1, 1, &[], None), None).unwrap(),
        NativeStaging::Existing { outcome, candidate: None } if outcome == original)
    );
    assert_eq!(budget.stats(), published);
    owner.publish_after_durable(second).unwrap();
    drop(pressure);
}

#[test]
fn failed_domain_admission_preserves_tail_clock_requests_and_ability_to_publish() {
    let core = core();
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (first, original) = stage(&mut owner, 10, creation(1, 1, &[], None));
    let before = budget.stats();
    let stale = Binding {
        revision: ObjectRevision(2),
        ..binding(1)
    };
    assert!(matches!(
        owner.prepare(context(ISSUER, 900), post(2, stale), None),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::StaleRevision
        )))
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.pending_len(), 1);
    assert_eq!(owner.oldest(), Some(first));
    assert_eq!(
        owner
            .candidate(first)
            .unwrap()
            .recorded(original.invocation),
        Some(original)
    );
    assert!(owner.effective().recorded(request(ISSUER, 2)).is_none());
    let (valid, _) = stage(&mut owner, 20, post(2, binding(1)));
    owner.publish_after_durable(first).unwrap();
    owner.publish_after_durable(valid).unwrap();
    assert_eq!(
        owner.committed().claim(key(1).claim).unwrap().status(),
        ClaimStatus::Posted
    );
}

#[test]
fn retained_neighbor_copy_failure_keeps_the_entire_existing_chain_and_refunds_candidate_memory() {
    let mut core = core();
    super::super::report_tests::publish(&mut core, 10, creation(1, 1, &[], None));
    super::super::report_tests::publish(&mut core, 20, creation(2, 2, &[], None));
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (first, first_outcome) = stage(&mut owner, 30, creation(3, 3, &[], None));
    let before = budget.stats();
    let failed = super::super::prepare::fail_copies_after(1, || {
        owner.prepare(context(ISSUER, 900), creation(4, 4, &[], None), None)
    });
    assert!(matches!(
        failed,
        Err(NativeOwnerError::Native(NativeError::Memory(
            MemoryError::AllocationFailed
        )))
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.pending_len(), 1);
    assert_eq!(owner.oldest(), Some(first));
    assert_eq!(owner.effective().sequence(), first_outcome.sequence);
    assert!(owner.effective().claim(ClaimId::from_u128(4)).is_none());
    assert!(owner.effective().recorded(request(ISSUER, 4)).is_none());
    let (retried, _) = stage(&mut owner, 40, creation(4, 4, &[], None));
    owner.publish_after_durable(first).unwrap();
    owner.publish_after_durable(retried).unwrap();
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(4))
            .unwrap()
            .created(),
        SessionSeq(4)
    );
}

#[test]
fn actual_report_custody_retry_and_discard_follow_the_same_owned_chain() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    core.limits.pending = 1;
    let input = report_for(
        &core,
        None,
        40,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(840, EVALUATOR, VerdictValue::Pass)),
    );
    let original = copy_report(&input);
    let artifact_id = match &input.command {
        NativeCommand::ReportAdmission { report, .. } => report.evidence.id,
        _ => panic!(),
    };
    let mut custody = Custody::new();
    let token = verified(&mut custody, &input);
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let before = budget.stats();
    let (candidate, outcome) = match owner
        .prepare(context(EVALUATOR, 100), input, Some(&token))
        .unwrap()
    {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        _ => panic!(),
    };
    assert!(owner.committed().artifact(artifact_id).is_none());
    assert!(owner.effective().artifact(artifact_id).is_some());
    let result = owner
        .effective()
        .evaluation(key(1))
        .unwrap()
        .last_result()
        .unwrap();
    let result_key = NativeResultKey::of(result);
    assert_eq!(
        owner.effective().result(result_key).unwrap().sequence(),
        outcome.sequence
    );
    let pressure = pressured(&budget);
    let occupied = budget.stats();
    assert!(
        matches!(owner.prepare(context(EVALUATOR, 1), copy_report(&original), None).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: Some(ticket) } if actual == outcome && ticket == candidate)
    );
    assert_eq!(budget.stats(), occupied);
    drop(pressure);
    assert_eq!(owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(budget.stats(), before);
    assert!(owner.effective().artifact(artifact_id).is_none());
    assert!(owner.effective().result(result_key).is_none());
    assert!(
        owner
            .effective()
            .evaluation(key(1))
            .unwrap()
            .last_result()
            .is_none()
    );
    assert!(
        owner
            .prepare(context(EVALUATOR, 100), copy_report(&original), None)
            .is_err()
    );
    assert_eq!(budget.stats(), before);
    let (replacement, new_outcome) = match owner
        .prepare(
            context(EVALUATOR, 100),
            copy_report(&original),
            Some(&token),
        )
        .unwrap()
    {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        _ => panic!(),
    };
    assert_ne!(replacement, candidate);
    assert_eq!(new_outcome, outcome);
    assert!(matches!(
        owner.publish_after_durable(candidate),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    drop(token);
    owner.publish_after_durable(replacement).unwrap();
    assert!(
        matches!(owner.prepare(context(EVALUATOR, 9999), original, None).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    assert!(owner.committed().artifact(artifact_id).is_some());
    assert!(owner.committed().result(result_key).is_some());
}

#[test]
fn discard_all_is_idempotent_and_never_erases_committed_requests_or_reuses_tickets() {
    let core = core();
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (committed, original) = stage(&mut owner, 10, creation(1, 1, &[], None));
    owner.publish_after_durable(committed).unwrap();
    let before = budget.stats();
    let (second, _) = stage(&mut owner, 20, creation(2, 2, &[], None));
    let (third, _) = stage(&mut owner, 30, creation(3, 3, &[], None));
    assert_eq!(owner.discard_all(), 2);
    assert_eq!(owner.discard_all(), 0);
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(owner.oldest(), None);
    assert_eq!(owner.effective().sequence(), original.sequence);
    assert_eq!(
        owner.effective().recorded(original.invocation),
        Some(original)
    );
    assert!(owner.effective().recorded(request(ISSUER, 2)).is_none());
    let (replacement, _) = stage(&mut owner, 20, creation(2, 2, &[], None));
    assert_ne!(replacement, second);
    assert_ne!(replacement, third);
    assert!(matches!(
        owner.discard_from(third),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    owner.publish_after_durable(replacement).unwrap();
}

#[test]
fn dropping_owner_releases_queue_backing_and_all_unpublished_candidates() {
    let core = core();
    let budget = core.state.budget.clone();
    let core_only = budget.stats().used;
    let mut owner = NativeOwner::new(core).unwrap();
    assert!(budget.stats().used > core_only);
    stage(&mut owner, 10, creation(1, 1, &[], None));
    stage(&mut owner, 20, child(2, 2, binding(1)));
    assert!(budget.stats().used > core_only);
    drop(owner);
    assert_eq!(budget.stats().used, 0);
    assert_eq!(budget.stats().ordinary_used, 0);
    assert!(budget.stats().by_kind.iter().all(|bytes| *bytes == 0));
}

#[test]
fn owner_construction_refuses_unfunded_queue_storage_and_returns_original_core_for_retry() {
    let mut core = core();
    let outcome = super::super::report_tests::publish(&mut core, 10, creation(1, 1, &[], None));
    let budget = core.state.budget.clone();
    let pressure = pressured(&budget);
    let before = budget.stats();
    let refused = NativeOwner::new(core).unwrap_err();
    assert!(matches!(
        refused.error,
        NativeOwnerError::Native(NativeError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(refused.core.native_sequence(), outcome.sequence);
    assert_eq!(
        refused.core.native_outcome(outcome.invocation),
        Some(outcome)
    );
    assert_eq!(
        refused.core.native_claim(key(1).claim).unwrap().binding(),
        binding(1)
    );
    drop(pressure);
    let owner = NativeOwner::new(refused.core).unwrap();
    assert_eq!(
        owner.committed().recorded(outcome.invocation),
        Some(outcome)
    );
    assert_eq!(owner.pending_len(), 0);
    drop(owner);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn exhausted_candidate_serial_cannot_reuse_a_ticket_or_prevent_existing_retry_and_publication() {
    let core = core();
    let budget = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let (first, original) = stage(&mut owner, 10, creation(1, 1, &[], None));
    owner.next_serial = u64::MAX;
    let before = budget.stats();
    assert!(matches!(
        owner.prepare(context(ISSUER, 900), creation(2, 2, &[], None), None),
        Err(NativeOwnerError::Native(NativeError::Memory(
            MemoryError::CounterExhausted(_)
        )))
    ));
    assert_eq!(owner.next_serial, u64::MAX);
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.oldest(), Some(first));
    assert_eq!(owner.pending_len(), 1);
    assert!(owner.effective().claim(ClaimId::from_u128(2)).is_none());
    assert_eq!(owner.effective().logical_time(), 10);
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), creation(1, 1, &[], None), None).unwrap(),
        NativeStaging::Existing { outcome, candidate: Some(ticket) } if outcome == original && ticket == first)
    );
    assert_eq!(budget.stats(), before);
    owner.publish_after_durable(first).unwrap();
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), creation(1, 1, &[], None), None).unwrap(),
        NativeStaging::Existing { outcome, candidate: None } if outcome == original)
    );
}

#[test]
fn lower_level_publication_refusal_restores_the_exact_head_ticket_and_successors() {
    let original_core = core();
    let budget = original_core.state.budget.clone();
    let mut owner = NativeOwner::new(original_core).unwrap();
    let (first, original) = stage(&mut owner, 10, creation(1, 1, &[], None));
    let (second, later) = stage(&mut owner, 20, child(2, 2, binding(1)));
    // The safe public owner API cannot create this mismatch. Swap only inside
    // this private test to force RangeStore's foreign-root publication refusal.
    let mut saved = core();
    std::mem::swap(&mut owner.core, &mut saved);
    let before = budget.stats();
    assert!(matches!(
        owner.publish_after_durable(first),
        Err(NativeOwnerError::Native(NativeError::Memory(_)))
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(owner.oldest(), Some(first));
    assert_eq!(owner.pending_len(), 2);
    assert_eq!(owner.pending[0].candidate, first);
    assert_eq!(owner.pending[0].prepared.outcome(), original);
    assert_eq!(owner.pending[1].candidate, second);
    assert_eq!(owner.pending[1].prepared.outcome(), later);
    std::mem::swap(&mut owner.core, &mut saved);
    let pressure = pressured(&budget);
    owner.publish_after_durable(first).unwrap();
    owner.publish_after_durable(second).unwrap();
    assert_eq!(
        owner.committed().recorded(original.invocation),
        Some(original)
    );
    assert_eq!(owner.committed().recorded(later.invocation), Some(later));
    drop(pressure);
}

#[test]
fn owner_snapshot_facade_pins_only_committed_facts_and_expires_retained_pages() {
    let mut owner = NativeOwner::new(core()).unwrap();
    let (created, _) = stage(&mut owner, 10, creation(1, 1, &[], None));
    let empty = owner.pin(0, 100).unwrap();
    assert_eq!(empty.sequence(), SessionSeq(0));
    owner.publish_after_durable(created).unwrap();
    let (posted, _) = stage(&mut owner, 20, post(2, binding(1)));
    let generated = owner.pin(0, 100).unwrap();
    assert_eq!(generated.sequence(), SessionSeq(1));
    owner.publish_after_durable(posted).unwrap();
    assert_eq!(
        owner.committed().claim(key(1).claim).unwrap().status(),
        ClaimStatus::Posted
    );
    assert_eq!(
        empty
            .with_claim(key(1).claim, 1, |row| row.status())
            .unwrap(),
        None
    );
    assert_eq!(
        generated
            .with_claim(key(1).claim, 99, |row| row.status())
            .unwrap(),
        Some(ClaimStatus::Generated)
    );
    assert_eq!(generated.recorded(request(ISSUER, 2), 99).unwrap(), None);
    let retained = owner.budget_stats().used;
    assert_eq!(owner.advance_clock(100).unwrap(), 2);
    assert!(owner.budget_stats().used < retained);
    assert_eq!(
        generated.with_claim(key(1).claim, 100, |row| row.status()),
        Err(MemoryError::LeaseExpired)
    );
    assert_eq!(
        empty.recorded(request(ISSUER, 1), 100),
        Err(MemoryError::LeaseExpired)
    );
    assert_eq!(owner.pending_len(), 0);
}
