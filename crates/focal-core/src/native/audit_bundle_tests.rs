use crate::native::report_tests as f;
#[path = "audit_bundle_graph_tests.rs"]
mod graph_tests;
use crate::native::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    aggregation::PublicationPosition, artifact_descriptor::ResultProvenance,
    audit::ResultTestamentState,
};
use focal_model::{
    ArtifactRef, Confidence, ObjectRevision, OutcomeKind, ValidationMode, VerdictValue,
};

const CLAIM: ClaimId = ClaimId::from_u128(1);
const RESPONSE: TestamentId = TestamentId::from_u128(900);
const BUNDLE: TestamentId = TestamentId::from_u128(950);

fn generate(claim: Binding, id: TestamentId, actor: ParticipantId, request: u128) -> NativeInput {
    NativeInput {
        request: f::request(actor, request),
        command: NativeCommand::GenerateResultTestament { claim, id },
    }
}

fn post(expected: Binding, actor: ParticipantId, request: u128) -> NativeInput {
    NativeInput {
        request: f::request(actor, request),
        command: NativeCommand::PostResultTestament { expected },
    }
}

fn stage(owner: &mut NativeOwner, input: NativeInput) -> (NativeCandidate, NativeOutcome) {
    let context = f::context(
        input.request.principal,
        owner.effective().logical_time().max(100),
    );
    let NativeStaging::Prepared { candidate, outcome } =
        owner.prepare(context, input, None).unwrap()
    else {
        panic!("fresh owner operation")
    };
    (candidate, outcome)
}

fn commit(owner: &mut NativeOwner, input: NativeInput) -> NativeOutcome {
    let (candidate, outcome) = stage(owner, input);
    assert_eq!(owner.publish_after_durable(candidate).unwrap(), outcome);
    outcome
}

fn refused(owner: &mut NativeOwner, input: NativeInput) {
    let budget = owner.budget_stats();
    let range = owner.range_stats();
    let sequence = owner.effective().sequence();
    let pending = owner.pending_len();
    let context = f::context(
        input.request.principal,
        owner.effective().logical_time().max(100),
    );
    assert!(owner.prepare(context, input, None).is_err());
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    assert_eq!(owner.effective().sequence(), sequence);
    assert_eq!(owner.pending_len(), pending);
}

fn retry(
    owner: &mut NativeOwner,
    input: NativeInput,
    expected: NativeOutcome,
    candidate: Option<NativeCandidate>,
) {
    let budget = owner.budget_stats();
    let range = owner.range_stats();
    let context = f::context(
        input.request.principal,
        owner.effective().logical_time().max(100),
    );
    let NativeStaging::Existing {
        outcome,
        candidate: actual,
    } = owner.prepare(context, input, None).unwrap()
    else {
        panic!("exact retained request")
    };
    assert_eq!(outcome, expected);
    assert_eq!(actual, candidate);
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
}

fn posted(requirements: &[(ValidationMode, bool)], begun: &[u32]) -> Core<NativeState> {
    let mut core = f::core();
    core.limits.plan_edges = 65_536;
    f::publish(&mut core, 10, f::creation(1, 1, requirements, None));
    f::publish(&mut core, 20, f::post(2, f::binding(1)));
    for &index in begun {
        let claim = core.native_claim(CLAIM).unwrap().binding();
        let binding = core.native_evaluation(f::key(index)).unwrap().binding();
        f::publish(
            &mut core,
            30,
            f::begin(10 + u128::from(index), claim, index, binding),
        );
    }
    core
}

fn fail_required(core: &mut Core<NativeState>, custody: &mut f::Custody) -> NativeAccepted {
    let input = f::report_for(
        core,
        None,
        500,
        1,
        VerdictValue::Fail,
        f::descriptor(f::artifact_spec(1500, f::EVALUATOR, VerdictValue::Fail)),
    );
    let evidence = f::verified(custody, &input);
    let prepared = f::report(core, input, &[], &evidence);
    let result = prepared
        .evaluation(f::key(1))
        .unwrap()
        .last_result()
        .unwrap();
    let accepted = *prepared.result(NativeResultKey::of(result)).unwrap();
    core.publish_native(prepared).unwrap();
    accepted
}

fn completed_core() -> (Core<NativeState>, f::Custody, NativeAccepted) {
    let mut core = posted(
        &[
            (ValidationMode::Required, false),
            (ValidationMode::Observe, false),
        ],
        &[1],
    );
    let mut custody = f::Custody::new();
    let accepted = fail_required(&mut core, &mut custody);
    (core, custody, accepted)
}

fn unfinished_owner() -> (NativeOwner, f::Custody, NativeAccepted) {
    let mut core = posted(
        &[
            (ValidationMode::Required, false),
            (ValidationMode::Observe, true),
            (ValidationMode::Observe, false),
        ],
        &[1, 2],
    );
    let mut custody = f::Custody::new();
    let accepted = fail_required(&mut core, &mut custody);
    (NativeOwner::new(core).unwrap(), custody, accepted)
}

fn report_observer(
    owner: &mut NativeOwner,
    custody: &mut f::Custody,
    id: u128,
    value: VerdictValue,
) -> NativeAccepted {
    let view = owner.effective();
    let key = f::key(2);
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = f::descriptor(f::artifact_spec(10_000 + id, attempt.evaluator, value))
        .with_result_provenance(ResultProvenance {
            claim: CLAIM,
            validation: key.validation,
            target: state.target(),
            generation: state.generation(),
            attempt,
            value,
        })
        .unwrap();
    let input = NativeInput {
        request: f::request(attempt.evaluator, id),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(CLAIM).unwrap().binding(),
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    };
    let evidence = f::verified(custody, &input);
    let NativeStaging::Prepared { candidate, .. } = owner
        .prepare(f::context(attempt.evaluator, 100), input, Some(&evidence))
        .unwrap()
    else {
        panic!("late report")
    };
    let view = owner.candidate(candidate).unwrap();
    let result = view.evaluation(key).unwrap().last_result().unwrap();
    let accepted = *view.result(NativeResultKey::of(result)).unwrap();
    owner.publish_after_durable(candidate).unwrap();
    accepted
}

fn frozen_claim(owner: &NativeOwner) -> ClaimState {
    let claim = owner.effective().claim(CLAIM).unwrap();
    claim.try_copy(claim.copy_charge().unwrap()).unwrap()
}

fn assert_claim_unchanged(owner: &NativeOwner, expected: &ClaimState) {
    let claim = owner.effective().claim(CLAIM).unwrap();
    assert_eq!(claim.binding(), expected.binding());
    assert_eq!(claim.status(), expected.status());
    assert_eq!(claim.receipt(), expected.receipt());
    assert_eq!(claim.local_complete(), expected.local_complete());
    assert_eq!(claim.local_sealed_at(), expected.local_sealed_at());
    assert_eq!(claim.latest_response(), expected.latest_response());
    assert_eq!(claim.response_count(), expected.response_count());
}

fn assert_bundle_outcome(outcome: NativeOutcome, operation: NativeOperation) {
    assert_eq!(outcome.operation, operation);
    assert_eq!(outcome.result_testaments, 1);
    assert_eq!(
        (
            outcome.changed,
            outcome.responses,
            outcome.artifacts,
            outcome.results,
            outcome.evaluations
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(outcome.events, 1);
}

#[test]
fn claimant_bundle_has_independent_generated_posted_history_and_exact_retries_after_posting() {
    let (core, _custody, accepted) = completed_core();
    let mut owner = NativeOwner::new(core).unwrap();
    let original = frozen_claim(&owner);
    let captured_at = owner.effective().sequence();
    let evaluation = *owner.effective().evaluation(f::key(2)).unwrap();
    let (generated, generation) = stage(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 600),
    );
    assert_bundle_outcome(generation, NativeOperation::GenerateResultTestament);
    assert!(owner.committed().result_testament(BUNDLE).is_none());
    let row = owner.effective().result_testament(BUNDLE).unwrap();
    let binding = row.testament().binding();
    assert_eq!(binding.object.0, BUNDLE.0);
    assert_eq!(binding.revision, ObjectRevision(1));
    assert_ne!(binding.content, ContentHash([0; 32]));
    assert_eq!(row.testament().state(), ResultTestamentState::Generated);
    assert_eq!(row.testament().claim(), CLAIM);
    assert_eq!(row.testament().results(), &[accepted.result()]);
    assert_eq!(
        row.publication(accepted.result()),
        Some(PublicationPosition {
            sequence: accepted.sequence(),
            ordinal: accepted.ordinal()
        })
    );
    assert_eq!(row.captured_at(), captured_at);
    assert_eq!(
        row.generated_at(),
        PublicationPosition {
            sequence: generation.sequence,
            ordinal: 0
        }
    );
    assert_eq!(row.posted_at(), None);
    let publications = row.publications().to_vec();
    assert_eq!(
        owner
            .effective()
            .claim_result_testament(CLAIM)
            .unwrap()
            .testament()
            .binding(),
        binding
    );
    assert!(owner.effective().response(BUNDLE).is_none());
    assert_claim_unchanged(&owner, &original);
    assert_eq!(owner.effective().evaluation(f::key(2)), Some(&evaluation));
    assert!(
        matches!(owner.effective().event(generation.sequence, 0).unwrap().fact,
        NativeFact::ResultTestament { claim: CLAIM, before: None, after, state: ResultTestamentState::Generated } if after == binding)
    );
    retry(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 600),
        generation,
        Some(generated),
    );

    let (posted, posting) = stage(&mut owner, post(binding, f::ISSUER, 601));
    assert_bundle_outcome(posting, NativeOperation::PostResultTestament);
    let row = owner.effective().result_testament(BUNDLE).unwrap();
    assert_eq!(row.testament().state(), ResultTestamentState::Posted);
    assert_eq!(row.testament().binding(), binding.next().unwrap());
    assert_eq!(row.testament().results(), &[accepted.result()]);
    assert_eq!(row.publications(), publications);
    assert_eq!(row.captured_at(), captured_at);
    assert_eq!(
        row.generated_at(),
        PublicationPosition {
            sequence: generation.sequence,
            ordinal: 0
        }
    );
    assert_eq!(
        row.posted_at(),
        Some(PublicationPosition {
            sequence: posting.sequence,
            ordinal: 0
        })
    );
    assert!(
        matches!(owner.effective().event(posting.sequence, 0).unwrap().fact,
        NativeFact::ResultTestament { claim: CLAIM, before: Some(before), after, state: ResultTestamentState::Posted }
            if before == binding && after == binding.next().unwrap())
    );
    assert_claim_unchanged(&owner, &original);
    retry(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 600),
        generation,
        Some(generated),
    );
    retry(
        &mut owner,
        post(binding, f::ISSUER, 601),
        posting,
        Some(posted),
    );
    owner.publish_after_durable(generated).unwrap();
    let generated_read = owner.pin(100, 100).unwrap();
    owner.publish_after_durable(posted).unwrap();
    assert_eq!(
        generated_read
            .with_result_testament(BUNDLE, 101, |row| {
                (
                    row.testament().binding(),
                    row.testament().state(),
                    row.posted_at(),
                )
            })
            .unwrap(),
        Some((binding, ResultTestamentState::Generated, None))
    );
    owner.release(&generated_read).unwrap();
    retry(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 600),
        generation,
        None,
    );
    retry(&mut owner, post(binding, f::ISSUER, 601), posting, None);
    assert_claim_unchanged(&owner, &original);
    assert_eq!(
        owner
            .committed()
            .result_testament(BUNDLE)
            .unwrap()
            .testament()
            .state(),
        ResultTestamentState::Posted
    );
}

#[test]
fn claimant_authority_expected_bindings_and_one_bundle_per_claim_are_checked_at_pending_prefix() {
    let (core, _custody, _) = completed_core();
    let mut owner = NativeOwner::new(core).unwrap();
    let claim = owner.effective().claim(CLAIM).unwrap().binding();
    for (offset, actor) in [f::SUBJECT, f::EVALUATOR, f::QUALITY]
        .into_iter()
        .enumerate()
    {
        refused(
            &mut owner,
            generate(claim, BUNDLE, actor, 700 + offset as u128),
        );
    }
    refused(
        &mut owner,
        generate(claim.next().unwrap(), BUNDLE, f::ISSUER, 710),
    );
    refused(
        &mut owner,
        generate(claim, TestamentId::from_u128(0), f::ISSUER, 711),
    );
    refused(&mut owner, post(f::binding(950), f::ISSUER, 712));
    let before = owner.budget_stats();
    assert!(
        owner
            .prepare(
                NativeContext {
                    principal: Principal::Node(f::ISSUER),
                    logical_time: 100
                },
                generate(claim, BUNDLE, f::ISSUER, 713),
                None
            )
            .is_err()
    );
    assert_eq!(owner.budget_stats(), before);

    let (candidate, _) = stage(&mut owner, generate(claim, BUNDLE, f::ISSUER, 714));
    let binding = owner
        .effective()
        .result_testament(BUNDLE)
        .unwrap()
        .testament()
        .binding();
    refused(&mut owner, generate(claim, BUNDLE, f::ISSUER, 715));
    refused(
        &mut owner,
        generate(claim, TestamentId::from_u128(951), f::ISSUER, 716),
    );
    refused(
        &mut owner,
        generate(claim, TestamentId::from_u128(951), f::ISSUER, 714),
    );
    for (offset, actor) in [f::SUBJECT, f::EVALUATOR, f::QUALITY]
        .into_iter()
        .enumerate()
    {
        refused(&mut owner, post(binding, actor, 720 + offset as u128));
    }
    refused(&mut owner, post(binding.next().unwrap(), f::ISSUER, 724));
    refused(
        &mut owner,
        post(
            Binding {
                content: ContentHash([87; 32]),
                ..binding
            },
            f::ISSUER,
            725,
        ),
    );
    owner.publish_after_durable(candidate).unwrap();
    commit(&mut owner, post(binding, f::ISSUER, 726));
    refused(&mut owner, post(binding, f::ISSUER, 727));
    refused(&mut owner, post(binding.next().unwrap(), f::ISSUER, 728));
}

#[test]
fn discard_restores_bundle_index_and_ordinary_pressure_refuses_fresh_operations_but_not_exact_retries()
 {
    let (core, _custody, _) = completed_core();
    let mut owner = NativeOwner::new(core).unwrap();
    let claim = owner.effective().claim(CLAIM).unwrap().binding();
    let before_budget = owner.budget_stats();
    let before_range = owner.range_stats();
    let (generated, generation) = stage(&mut owner, generate(claim, BUNDLE, f::ISSUER, 800));
    let binding = owner
        .effective()
        .result_testament(BUNDLE)
        .unwrap()
        .testament()
        .binding();
    stage(&mut owner, post(binding, f::ISSUER, 801));
    assert_eq!(owner.discard_from(generated).unwrap(), 2);
    assert!(owner.effective().result_testament(BUNDLE).is_none());
    assert!(owner.effective().claim_result_testament(CLAIM).is_none());
    assert_eq!(owner.budget_stats(), before_budget);
    assert_eq!(owner.range_stats(), before_range);

    let budget = owner.budget_for_test();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    refused(&mut owner, generate(claim, BUNDLE, f::ISSUER, 800));
    drop(pressure);
    let (generated, repeated) = stage(&mut owner, generate(claim, BUNDLE, f::ISSUER, 800));
    assert_eq!(repeated, generation);
    assert_eq!(
        owner
            .effective()
            .result_testament(BUNDLE)
            .unwrap()
            .testament()
            .binding(),
        binding
    );
    owner.publish_after_durable(generated).unwrap();
    let budget = owner.budget_for_test();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    retry(
        &mut owner,
        generate(claim, BUNDLE, f::ISSUER, 800),
        generation,
        None,
    );
    refused(&mut owner, post(binding, f::ISSUER, 801));
    drop(pressure);
    let posting = commit(&mut owner, post(binding, f::ISSUER, 801));
    let budget = owner.budget_for_test();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    retry(&mut owner, post(binding, f::ISSUER, 801), posting, None);
    drop(pressure);
}

#[test]
fn begun_observe_must_finish_all_late_retry_and_quality_results_before_bundle_generation() {
    let (mut owner, mut custody, required) = unfinished_owner();
    let claim = frozen_claim(&owner);
    let sealed_at = claim.local_sealed_at().unwrap();
    refused(
        &mut owner,
        generate(claim.binding(), BUNDLE, f::ISSUER, 900),
    );
    let error = report_observer(&mut owner, &mut custody, 901, VerdictValue::Error);
    refused(
        &mut owner,
        generate(claim.binding(), BUNDLE, f::ISSUER, 900),
    );
    let pass = report_observer(&mut owner, &mut custody, 902, VerdictValue::Pass);
    refused(
        &mut owner,
        generate(claim.binding(), BUNDLE, f::ISSUER, 900),
    );
    let quality = report_observer(&mut owner, &mut custody, 903, VerdictValue::Pass);
    let captured = owner.effective().sequence();
    commit(
        &mut owner,
        generate(claim.binding(), BUNDLE, f::ISSUER, 900),
    );
    let row = owner.effective().result_testament(BUNDLE).unwrap();
    assert_eq!(
        row.testament().results(),
        &[
            required.result(),
            error.result(),
            pass.result(),
            quality.result()
        ]
    );
    assert_eq!(row.testament().sealed_at(), sealed_at);
    assert_eq!(row.captured_at(), captured);
    assert_eq!(error.result().binding().revision, ObjectRevision(4));
    for result in [required, error, pass, quality] {
        assert_eq!(
            row.publication(result.result()),
            Some(PublicationPosition {
                sequence: result.sequence(),
                ordinal: result.ordinal()
            })
        );
    }
    assert!(error.sequence() > sealed_at);
    assert_eq!(quality.attempt().evaluator, f::QUALITY);
    let ready = row
        .testament()
        .members()
        .iter()
        .find(|member| member.key().validation == f::key(3).validation)
        .unwrap();
    assert_eq!(ready.state(), validation::State::Ready);
    assert!(ready.suppression().is_some());
    assert_claim_unchanged(&owner, &claim);
    let expected = owner.effective().evaluation(f::key(3)).unwrap().binding();
    refused(&mut owner, f::begin(904, claim.binding(), 3, expected));
}

#[test]
fn explicit_deadline_fence_permits_bundle_closure_without_inventing_a_lost_observer_result() {
    let (mut owner, _custody, required) = unfinished_owner();
    let claim = frozen_claim(&owner);
    refused(
        &mut owner,
        generate(claim.binding(), BUNDLE, f::ISSUER, 1000),
    );
    let deadline = NativeDeadlineInput {
        evaluation: f::key(2),
        deadline: owner
            .effective()
            .evaluation(f::key(2))
            .unwrap()
            .bind(owner.effective().definition(f::key(2).validation).unwrap())
            .unwrap()
            .deadline(),
    };
    let NativeStaging::Prepared {
        candidate: fenced, ..
    } = owner
        .prepare_evaluation_deadline(deadline, deadline.deadline.at)
        .unwrap()
    else {
        panic!("deadline fence")
    };
    assert_eq!(
        owner
            .effective()
            .evaluation(f::key(2))
            .unwrap()
            .last_result(),
        None
    );
    let (generated, _) = stage(
        &mut owner,
        generate(claim.binding(), BUNDLE, f::ISSUER, 1000),
    );
    let row = owner.effective().result_testament(BUNDLE).unwrap();
    assert_eq!(row.testament().results(), &[required.result()]);
    let observer = row
        .testament()
        .members()
        .iter()
        .find(|member| member.key().validation == f::key(2).validation)
        .unwrap();
    assert_eq!(observer.state(), validation::State::Validating);
    assert_eq!(
        observer.fence().unwrap().reason,
        validation::FenceReason::Deadline(deadline.deadline)
    );
    assert_eq!(row.publications().len(), 1);
    let binding = row.testament().binding();
    owner.publish_after_durable(fenced).unwrap();
    owner.publish_after_durable(generated).unwrap();
    commit(&mut owner, post(binding, f::ISSUER, 1001));
    assert_claim_unchanged(&owner, &claim);
}

fn close(claim: Binding, id: u128, request: u128) -> NativeInput {
    NativeInput {
        request: f::request(f::SUBJECT, request),
        command: NativeCommand::CloseResponse {
            claim,
            response: f::binding(id),
            report: NativeResponseInput {
                summary: "No work artifacts were required.".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                manifest: Vec::new(),
                diagnostics: Vec::new(),
            },
        },
    }
}

#[test]
fn structural_bundle_preserves_respondent_response_and_testament_ids_cannot_cross_roles() {
    let core = work_authority::history_fixture(false, true);
    let mut owner = NativeOwner::new(core).unwrap();
    let claim = owner.effective().claim(CLAIM).unwrap().binding();
    let expected = owner
        .effective()
        .response(RESPONSE)
        .unwrap()
        .identity()
        .binding;
    commit(
        &mut owner,
        NativeInput {
            request: f::request(f::ISSUER, 1100),
            command: NativeCommand::EnterWholeWork { claim, expected },
        },
    );
    let original = frozen_claim(&owner);
    let source = owner.effective().response_record(RESPONSE).unwrap();
    let response_facts = (
        source.response().identity(),
        source.response().state(),
        source.response().reported_outcome(),
        source.received(),
        source.entered(),
    );
    refused(
        &mut owner,
        generate(original.binding(), RESPONSE, f::ISSUER, 1101),
    );
    let (generated, generation) = stage(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 1102),
    );
    assert_bundle_outcome(generation, NativeOperation::GenerateResultTestament);
    let row = owner.effective().result_testament(BUNDLE).unwrap();
    assert_eq!(row.testament().results().len(), 2);
    for result in row.testament().results() {
        assert!(matches!(
            result.phase(),
            validation::Phase::Delivery | validation::Phase::MissingTarget
        ));
        assert_eq!(result.evidence(), None);
        assert_eq!(result.attempt(), None);
        assert_eq!(result.reporter(), None);
        assert!(row.publication(*result).unwrap().sequence < generation.sequence);
    }
    assert!(owner.effective().response(BUNDLE).is_none());
    assert!(owner.effective().result_testament(RESPONSE).is_none());
    assert_claim_unchanged(&owner, &original);
    let source = owner.effective().response_record(RESPONSE).unwrap();
    assert_eq!(
        (
            source.response().identity(),
            source.response().state(),
            source.response().reported_outcome(),
            source.received(),
            source.entered()
        ),
        response_facts
    );

    // The result ID is already allocated in the pending prefix. An unrelated
    // respondent cannot reuse it for an ordinary authored response.
    let (created2, _) = stage(&mut owner, f::creation(1110, 2, &[], None));
    let claim2 = ClaimId::from_u128(2);
    let binding = owner.effective().claim(claim2).unwrap().binding();
    let (posted2, _) = stage(&mut owner, f::post(1111, binding));
    let binding = owner.effective().claim(claim2).unwrap().binding();
    let (received2, _) = stage(
        &mut owner,
        NativeInput {
            request: f::request(f::SUBJECT, 1112),
            command: NativeCommand::AcquireReceipt {
                expected: binding,
                receipt: ReceiptId::from_u128(702),
            },
        },
    );
    let binding = owner.effective().claim(claim2).unwrap().binding();
    refused(&mut owner, close(binding, 950, 1113));
    let (closed2, _) = stage(&mut owner, close(binding, 951, 1114));
    assert!(
        owner
            .effective()
            .response(TestamentId::from_u128(951))
            .is_some()
    );
    assert!(owner.effective().result_testament(BUNDLE).is_some());
    assert!(owner.effective().claim_result_testament(claim2).is_none());
    assert!(owner.committed().result_testament(BUNDLE).is_none());
    assert!(
        owner
            .committed()
            .response(TestamentId::from_u128(951))
            .is_none()
    );
    for candidate in [generated, created2, posted2, received2, closed2] {
        owner.publish_after_durable(candidate).unwrap();
    }
    assert!(owner.committed().result_testament(BUNDLE).is_some());
    assert!(
        owner
            .committed()
            .response(TestamentId::from_u128(951))
            .is_some()
    );
}

#[test]
fn insufficient_batch_capacity_and_copy_failure_leave_no_partial_bundle_or_consumed_request() {
    let (mut core, _custody, _) = completed_core();
    let claim = core.native_claim(CLAIM).unwrap().binding();
    let before_budget = core.native_budget();
    let before_sequence = core.native_sequence();
    core.limits.range.max_batch_entries = 4;
    assert!(
        core.prepare_native(
            f::context(f::ISSUER, 100),
            generate(claim, BUNDLE, f::ISSUER, 1300),
            &[]
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), before_budget);
    assert_eq!(core.native_sequence(), before_sequence);
    assert!(core.native_result_testament(BUNDLE).is_none());
    assert!(core.native_claim_result_testament(CLAIM).is_none());
    assert!(core.native_outcome(f::request(f::ISSUER, 1300)).is_none());

    core.limits.range.max_batch_entries = 5;
    let failed = crate::native::prepare::fail_copies_after(0, || {
        core.prepare_native(
            f::context(f::ISSUER, 100),
            generate(claim, BUNDLE, f::ISSUER, 1300),
            &[],
        )
    });
    assert!(
        matches!(
            failed,
            Err(NativeError::Memory(MemoryError::AllocationFailed))
        ),
        "{failed:?}"
    );
    assert_eq!(core.native_budget(), before_budget);
    assert_eq!(core.native_sequence(), before_sequence);
    assert!(core.native_result_testament(BUNDLE).is_none());
    assert!(core.native_claim_result_testament(CLAIM).is_none());
    assert!(core.native_outcome(f::request(f::ISSUER, 1300)).is_none());

    f::publish(&mut core, 100, generate(claim, BUNDLE, f::ISSUER, 1300));
    let binding = core
        .native_result_testament(BUNDLE)
        .unwrap()
        .testament()
        .binding();
    let generated_budget = core.native_budget();
    let generated_sequence = core.native_sequence();
    core.limits.range.max_batch_entries = 3;
    assert!(
        core.prepare_native(
            f::context(f::ISSUER, 100),
            post(binding, f::ISSUER, 1301),
            &[]
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), generated_budget);
    assert_eq!(core.native_sequence(), generated_sequence);
    assert_eq!(
        core.native_result_testament(BUNDLE)
            .unwrap()
            .testament()
            .state(),
        ResultTestamentState::Generated
    );
    assert!(core.native_outcome(f::request(f::ISSUER, 1301)).is_none());
    core.limits.range.max_batch_entries = 4;
    f::publish(&mut core, 100, post(binding, f::ISSUER, 1301));
    assert_eq!(
        core.native_result_testament(BUNDLE)
            .unwrap()
            .testament()
            .state(),
        ResultTestamentState::Posted
    );
}

#[test]
fn retained_native_bundle_survives_ram_owner_reconstruction_then_posts_and_retries() {
    let (mut core, _custody, accepted) = completed_core();
    let claim = core.native_claim(CLAIM).unwrap().binding();
    let captured = core.native_sequence();
    let input = generate(claim, BUNDLE, f::ISSUER, 1200);
    let prepared = f::prepared(core.prepare_native(f::context(f::ISSUER, 100), input, &[]));
    let binding = prepared
        .result_testament(BUNDLE)
        .unwrap()
        .testament()
        .binding();
    assert_eq!(
        prepared
            .claim_result_testament(CLAIM)
            .unwrap()
            .testament()
            .binding(),
        binding
    );
    let generation = core.publish_native(prepared).unwrap();
    assert_eq!(
        core.native_result_testament(BUNDLE)
            .unwrap()
            .testament()
            .state(),
        ResultTestamentState::Generated
    );
    assert_eq!(
        core.native_claim_result_testament(CLAIM)
            .unwrap()
            .testament()
            .binding(),
        binding
    );
    let mut owner = NativeOwner::new(core).unwrap();
    let row = owner.committed().result_testament(BUNDLE).unwrap();
    assert_eq!(row.captured_at(), captured);
    assert_eq!(row.testament().results(), &[accepted.result()]);
    assert_eq!(
        row.generated_at(),
        PublicationPosition {
            sequence: generation.sequence,
            ordinal: 0
        }
    );
    retry(
        &mut owner,
        generate(claim, BUNDLE, f::ISSUER, 1200),
        generation,
        None,
    );
    let posting = commit(&mut owner, post(binding, f::ISSUER, 1201));
    retry(&mut owner, post(binding, f::ISSUER, 1201), posting, None);
    assert_eq!(
        owner
            .committed()
            .result_testament(BUNDLE)
            .unwrap()
            .testament()
            .state(),
        ResultTestamentState::Posted
    );
    assert_eq!(owner.committed().claim(CLAIM).unwrap().binding(), claim);
}
