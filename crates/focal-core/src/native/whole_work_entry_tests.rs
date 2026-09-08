use super::*;

#[path = "adoption_tests.rs"]
mod adoption_tests;
use focal_model::VerdictValue;
#[path = "object_journal_tests.rs"]
mod object_journal_tests;
#[path = "work_completion_hybrid_tests.rs"]
mod work_completion_hybrid_tests;
#[path = "work_completion_tests.rs"]
mod work_completion_tests;

const CLAIM: ClaimId = ClaimId::from_u128(1);
const RESPONSE: TestamentId = TestamentId::from_u128(900);

fn enter(f: &Fixture, id: u128) -> NativeCommand {
    NativeCommand::EnterWholeWork {
        claim: f.claim(),
        expected: f.response(id),
    }
}

fn deliver(f: &mut Fixture, id: u128) {
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    );
}

fn complete_response(f: &mut Fixture, response: u128, first_artifact: u128) -> [SlotBinding; 2] {
    let slots = [f.work(first_artifact, 0), f.work(first_artifact + 1, 1)];
    f.commit(
        SUBJECT,
        f.close(response, OutcomeKind::Complete, slots.to_vec(), vec![]),
    );
    deliver(f, response);
    slots
}

fn assert_refused_unchanged(f: &mut Fixture, actor: ParticipantId, command: NativeCommand) {
    let budget = f.owner.budget_stats();
    let range = f.owner.range_stats();
    let prefix = f.owner.effective().sequence();
    let claim = f.claim();
    let pending = f.owner.pending_len();
    assert!(f.stage(actor, command).is_err());
    assert_eq!(f.owner.budget_stats(), budget);
    assert_eq!(f.owner.range_stats(), range);
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert_eq!(f.owner.pending_len(), pending);
    assert_eq!(f.claim(), claim);
}

#[test]
fn explicit_claimant_entry_validates_present_zero_check_slots_and_satisfies_claim() {
    let mut f = Fixture::new();
    let slots = complete_response(&mut f, 900, 801);
    let received = f
        .owner
        .committed()
        .response_record(RESPONSE)
        .unwrap()
        .received();
    let receipt = f.owner.committed().claim(CLAIM).unwrap().receipt();
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Received
    );
    assert!(!f.owner.committed().claim(CLAIM).unwrap().local_complete());
    let pinned = f.owner.pin(0, 100).unwrap();

    let outcome = f.commit(ISSUER, enter(&f, 900));
    assert_eq!(outcome.operation, NativeOperation::EnterWholeWork);
    assert_eq!(
        (outcome.artifacts, outcome.results, outcome.receipts),
        (0, 0, 0)
    );
    let claim = f.owner.committed().claim(CLAIM).unwrap();
    assert_eq!(claim.status(), ClaimStatus::Satisfied);
    assert!(claim.local_complete());
    assert_eq!(claim.local_sealed_at(), Some(outcome.sequence));
    assert_eq!(claim.receipt(), receipt);
    let record = f.owner.committed().response_record(RESPONSE).unwrap();
    assert_eq!(record.response().state(), ResponseState::Validated);
    assert_eq!(record.received(), received);
    assert_eq!(record.entered().unwrap().sequence, outcome.sequence);
    assert!(record.entered() > record.received());
    assert_eq!(record.response().manifest(), &slots);
    assert_eq!(record.response().reported_outcome(), OutcomeKind::Complete);
    for slot in slots {
        let work = f.owner.committed().work(slot.artifact.id).unwrap();
        assert_eq!(work.state.state(), WorkArtifactState::Validated);
        assert_eq!(work.state.binding().content, slot.artifact.hash);
        assert_eq!(
            pinned
                .with_work(slot.artifact.id, 0, |row| row.state.state())
                .unwrap(),
            Some(WorkArtifactState::Attached)
        );
    }
    assert_eq!(
        pinned
            .with_response_record(RESPONSE, 0, |row| (row.response().state(), row.entered()))
            .unwrap(),
        Some((ResponseState::Received, None))
    );
    f.owner.with_effective_acceptance(CLAIM, |projection| {
        assert!(matches!(projection.claim_decision().outcome(), aggregation::AggregateOutcome::LocalComplete { sequence } if sequence == outcome.sequence));
        assert!(projection.response_decision(RESPONSE).is_some());
    }).unwrap();
}

#[test]
fn missing_required_presence_becomes_incomplete_without_fabricating_work_or_evaluation() {
    let mut f = Fixture::new();
    f.commit(SUBJECT, f.close(900, OutcomeKind::Complete, vec![], vec![]));
    deliver(&mut f, 900);
    let registrations = f
        .owner
        .committed()
        .registrations(CLAIM)
        .unwrap()
        .rows()
        .len();
    let outcome = f.commit(ISSUER, enter(&f, 900));
    assert_eq!(
        (outcome.artifacts, outcome.evaluations, outcome.results),
        (0, 0, 0)
    );
    assert_eq!(
        f.owner
            .committed()
            .registrations(CLAIM)
            .unwrap()
            .rows()
            .len(),
        registrations
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::ValidationIncomplete
    );
    assert!(!f.owner.committed().claim(CLAIM).unwrap().local_complete());
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().local_sealed_at(),
        Some(outcome.sequence)
    );
    let response = f.owner.committed().response(RESPONSE).unwrap();
    assert_eq!(response.state(), ResponseState::ValidationIncomplete);
    assert!(response.manifest().is_empty());
    assert_eq!(response.reported_outcome(), OutcomeKind::Complete);
    for id in [801, 802, 900] {
        assert!(
            f.owner
                .committed()
                .work(ArtifactId::from_u128(id))
                .is_none()
        );
        assert!(
            f.owner
                .committed()
                .artifact(ArtifactId::from_u128(id))
                .is_none()
        );
    }
}

// This fixture commits the full declaration and slot policy before taking the
// receipt. It never installs an evaluation or accepted result through a test seam.
fn checked_slot_fixture() -> Fixture {
    checked_slot_fixture_with_mode(ValidationMode::Required)
}

fn checked_slot_fixture_with_mode(mode: ValidationMode) -> Fixture {
    checked_slot_fixture_with_visits(mode, 2048)
}

fn checked_slot_fixture_with_visits(mode: ValidationMode, visits: usize) -> Fixture {
    checked_slot_fixture_with_definition(
        mode,
        visits,
        work_check_tests::declaration(1, 0, mode, 1000),
    )
}

fn checked_slot_fixture_with_definition(
    mode: ValidationMode,
    visits: usize,
    declaration: validation::Declaration,
) -> Fixture {
    let core = Core::new_native(
        binding(1).ledger,
        RangeId(5781),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: visits,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 32,
            range: RangeConfig {
                max_batch_entries: 128,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let store = ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap();
    let mut f = Fixture {
        owner: NativeOwner::new(core).unwrap(),
        store,
        _directory: directory,
        serial: 10,
    };
    let mut initial = creation(1, 1, &[], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut initial.command
    else {
        panic!("create")
    };
    declarations.push(declaration);
    let check = [aggregation::CheckPolicy {
        declaration_index: 1,
        validation: ValidationId::from_u128(301),
        mode,
    }];
    let policies = [
        aggregation::SlotPolicy {
            checks: &check,
            ..slot_policy(0)
        },
        slot_policy(1),
    ];
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &policies,
        declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 64,
            max_updates: 64,
        },
    )
    .unwrap();
    f.commit(ISSUER, initial.command);
    f.commit(
        ISSUER,
        NativeCommand::Post {
            expected: f.claim(),
        },
    );
    f.commit(
        SUBJECT,
        NativeCommand::AcquireReceipt {
            expected: f.claim(),
            receipt: ReceiptId::from_u128(701),
        },
    );
    f
}

#[test]
fn declared_missing_slot_retains_an_artifact_free_incomplete_result_at_actual_entry() {
    let mut f = checked_slot_fixture();
    f.commit(SUBJECT, f.close(900, OutcomeKind::Complete, vec![], vec![]));
    deliver(&mut f, 900);
    let key = EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::MissingSlot {
            response: RESPONSE,
            slot: 0,
        },
        generation: 1,
    };
    let ready = *f.owner.committed().evaluation(key).unwrap();
    assert_eq!(ready.state(), validation::State::Ready);
    assert!(ready.last_result().is_none());
    let pinned = f.owner.pin(0, 100).unwrap();
    let outcome = f.commit(ISSUER, enter(&f, 900));
    assert_eq!((outcome.artifacts, outcome.results), (0, 1));
    let state = f.owner.committed().evaluation(key).unwrap();
    assert_eq!(state.state(), validation::State::ValidationIncomplete);
    assert_eq!(state.target(), ready.target());
    assert_eq!(state.receipt(), ready.receipt());
    let accepted = state.last_result().unwrap();
    let result_key = NativeResultKey::of(accepted);
    let result = f.owner.committed().missing_result(result_key).unwrap();
    assert_eq!(result.result(), accepted);
    assert_eq!(result.sequence(), outcome.sequence);
    assert_eq!(accepted.phase(), validation::Phase::MissingTarget);
    assert_eq!(accepted.verdict(), VerdictValue::Incomplete);
    assert_eq!(accepted.generation(), 1);
    assert_eq!(accepted.binding(), state.binding());
    assert!(accepted.attempt().is_none());
    assert!(accepted.reporter().is_none());
    assert!(accepted.evidence().is_none());
    assert!(accepted.programmatic_evidence().is_none());
    assert!(f.owner.committed().result(result_key).is_none());
    assert!(f.owner.committed().delivery_result(result_key).is_none());
    assert_eq!(
        pinned
            .with_missing_result(result_key, 0, |row| *row)
            .unwrap(),
        None
    );
    assert!(
        matches!(f.owner.committed().event(result.sequence(), result.ordinal()).unwrap().fact,
        NativeFact::Missing { key: stored } if stored == result_key)
    );
    let entered = f
        .owner
        .committed()
        .response_record(RESPONSE)
        .unwrap()
        .entered()
        .unwrap();
    assert!(result.ordinal() > entered.ordinal);
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::ValidationIncomplete
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::ValidationIncomplete
    );
}

#[test]
fn entry_requires_actual_claimant_exact_parent_and_exact_received_response() {
    let mut f = Fixture::new();
    let slots = [f.work(801, 0), f.work(802, 1)];
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, slots.to_vec(), vec![]),
    );
    let generated = f.response(900);
    let command = enter(&f, 900);
    assert_refused_unchanged(&mut f, ISSUER, command);
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: generated,
        },
    );
    let posted = f.response(900);
    let command = enter(&f, 900);
    assert_refused_unchanged(&mut f, ISSUER, command);
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: posted,
        },
    );
    for actor in [SUBJECT, super::super::report_tests::EVALUATOR] {
        let command = enter(&f, 900);
        assert_refused_unchanged(&mut f, actor, command);
    }
    for expected in [generated, posted, binding(901)] {
        let command = NativeCommand::EnterWholeWork {
            claim: f.claim(),
            expected,
        };
        assert_refused_unchanged(&mut f, ISSUER, command);
    }
    let command = NativeCommand::EnterWholeWork {
        claim: binding(1),
        expected: f.response(900),
    };
    assert_refused_unchanged(&mut f, ISSUER, command);
    assert!(
        f.owner
            .committed()
            .response_record(RESPONSE)
            .unwrap()
            .entered()
            .is_none()
    );
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Received
    );
    f.commit(ISSUER, enter(&f, 900));
}

#[test]
fn pending_entry_is_atomic_discardable_and_exactly_retryable_after_satisfaction() {
    let mut f = Fixture::new();
    let slots = complete_response(&mut f, 900, 801);
    let claim = f.claim();
    let expected = f.response(900);
    let received = f
        .owner
        .committed()
        .response_record(RESPONSE)
        .unwrap()
        .received();
    let pinned = f.owner.pin(0, 100).unwrap();
    let budget = f.owner.budget_stats();
    let range = f.owner.range_stats();
    let input = || NativeInput {
        request: request(ISSUER, 90),
        command: NativeCommand::EnterWholeWork { claim, expected },
    };
    let NativeStaging::Prepared { candidate, outcome } =
        f.owner.prepare(context(ISSUER, 90), input(), None).unwrap()
    else {
        panic!("fresh entry")
    };
    let pending_budget = f.owner.budget_stats();
    assert!(pending_budget.used > budget.used);
    assert_eq!(
        f.owner.effective().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(f.owner.committed().claim(CLAIM).unwrap().binding(), claim);
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Received
    );
    assert_eq!(
        f.owner
            .candidate(candidate)
            .unwrap()
            .response(RESPONSE)
            .unwrap()
            .state(),
        ResponseState::Validated
    );
    for slot in slots {
        assert_eq!(
            f.owner
                .effective()
                .work(slot.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Validated
        );
        assert_eq!(
            f.owner
                .committed()
                .work(slot.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Attached
        );
    }
    assert_eq!(
        f.owner.prepare(context(ISSUER, 0), input(), None).unwrap(),
        NativeStaging::Existing {
            candidate: Some(candidate),
            outcome
        }
    );
    assert_eq!(f.owner.budget_stats(), pending_budget);
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(f.owner.budget_stats(), budget);
    assert_eq!(f.owner.range_stats(), range);
    assert_eq!(f.claim(), claim);
    assert_eq!(f.response(900), expected);
    let record = f.owner.effective().response_record(RESPONSE).unwrap();
    assert_eq!((record.received(), record.entered()), (received, None));
    let NativeStaging::Prepared {
        candidate: replacement,
        outcome: repeated,
    } = f.owner.prepare(context(ISSUER, 90), input(), None).unwrap()
    else {
        panic!("restaged entry")
    };
    assert_ne!(replacement, candidate);
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(replacement).unwrap();
    assert_eq!(
        f.owner.prepare(context(ISSUER, 0), input(), None).unwrap(),
        NativeStaging::Existing {
            candidate: None,
            outcome
        }
    );
    assert_eq!(
        pinned
            .with_response_record(RESPONSE, 0, |row| (row.response().state(), row.entered()))
            .unwrap(),
        Some((ResponseState::Received, None))
    );
    let new_command = enter(&f, 900);
    assert_refused_unchanged(&mut f, ISSUER, new_command);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().local_sealed_at(),
        Some(outcome.sequence)
    );
}

#[test]
fn entering_one_of_two_received_responses_preserves_the_other_response_and_work() {
    let mut f = Fixture::new();
    let first = complete_response(&mut f, 900, 801);
    let second = complete_response(&mut f, 901, 803);
    let other_id = TestamentId::from_u128(901);
    let other_binding = f.response(901);
    let other_received = f
        .owner
        .committed()
        .response_record(other_id)
        .unwrap()
        .received();
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().response_count(),
        2
    );
    let outcome = f.commit(ISSUER, enter(&f, 900));
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().local_sealed_at(),
        Some(outcome.sequence)
    );
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Validated
    );
    let other = f.owner.committed().response_record(other_id).unwrap();
    assert_eq!(other.response().identity().binding, other_binding);
    assert_eq!(other.response().state(), ResponseState::Received);
    assert_eq!((other.received(), other.entered()), (other_received, None));
    for slot in first {
        assert_eq!(
            f.owner
                .committed()
                .work(slot.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Validated
        );
    }
    for slot in second {
        assert_eq!(
            f.owner
                .committed()
                .work(slot.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Attached
        );
    }
    let command = enter(&f, 901);
    assert_refused_unchanged(&mut f, ISSUER, command);
}

#[test]
fn diagnostic_only_failed_testimony_preserves_truthful_error_and_missing_work_outcome() {
    let mut f = Fixture::new();
    let diagnostic = f.diagnostic(850);
    let source = f
        .owner
        .committed()
        .artifact(diagnostic.id)
        .unwrap()
        .descriptor()
        .binding();
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
    deliver(&mut f, 900);
    let outcome = f.commit(ISSUER, enter(&f, 900));
    assert_eq!((outcome.artifacts, outcome.results), (0, 0));
    let response = f.owner.committed().response(RESPONSE).unwrap();
    assert_eq!(response.reported_outcome(), OutcomeKind::Failed);
    assert_eq!(response.state(), ResponseState::ValidationIncomplete);
    assert!(response.manifest().is_empty());
    assert_eq!(response.diagnostics().len(), 1);
    assert_eq!(response.diagnostics()[0].artifact(), diagnostic);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::ValidationIncomplete
    );
    assert!(!f.owner.committed().claim(CLAIM).unwrap().local_complete());
    assert_eq!(
        f.owner
            .committed()
            .artifact(diagnostic.id)
            .unwrap()
            .descriptor()
            .binding(),
        source
    );
    assert!(f.owner.committed().diagnostic(diagnostic.id).is_some());
    assert!(f.owner.committed().work(diagnostic.id).is_none());
}
