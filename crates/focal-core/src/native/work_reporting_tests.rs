use super::*;

fn begin_input(owner: &NativeOwner, key: EvaluationKey, request: u128) -> NativeInput {
    let view = owner.effective();
    NativeInput {
        request: f::request(EVALUATOR, request),
        command: NativeCommand::BeginWork {
            claim: view.claim(key.claim).unwrap().binding(),
            key,
            expected: view.evaluation(key).unwrap().binding(),
        },
    }
}
fn publish(owner: &mut NativeOwner, staged: NativeStaging) -> NativeOutcome {
    let NativeStaging::Prepared { candidate, .. } = staged else {
        panic!("fresh candidate")
    };
    owner.publish_after_durable(candidate).unwrap()
}
fn report_input(
    owner: &NativeOwner,
    key: EvaluationKey,
    id: u128,
    value: VerdictValue,
) -> NativeInput {
    let view = owner.effective();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let mut spec = f::artifact_spec(id, attempt.evaluator, value);
    spec.receipt = state.receipt();
    spec.visibility = &["internal", "team:qa"];
    spec.result = Some(ResultProvenance {
        claim: key.claim,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value,
    });
    let artifact = f::descriptor(spec);
    NativeInput {
        request: f::request(attempt.evaluator, id),
        command: NativeCommand::ReportWork {
            claim: view.claim(key.claim).unwrap().binding(),
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
    }
}
fn store(directory: &tempfile::TempDir) -> ContentStore {
    ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 1024 * 1024,
            max_staging_bytes: 2 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 65536,
        },
    )
    .unwrap()
}

#[test]
fn managed_first_begin_and_late_sibling_report_keep_independent_original_cuts() {
    let fixture = Fixture::new(true);
    let required = fixture.key(1);
    let observe = fixture.key(2);
    let mut owner = NativeOwner::new(fixture.core).unwrap();
    let input = begin_input(&owner, required, 2000);
    let staged = owner
        .prepare(f::context(EVALUATOR, 100), input, None)
        .unwrap();
    let begun = publish(&mut owner, staged);
    assert!(
        matches!(owner.committed().event(begun.sequence,0).unwrap().fact,
        NativeFact::Evaluation {kind:NativeEvaluationEventKind::Begun,key,..} if key == required)
    );
    assert_eq!(
        owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Validating
    );
    let input = begin_input(&owner, observe, 2001);
    let staged = owner
        .prepare(f::context(EVALUATOR, 100), input, None)
        .unwrap();
    assert_eq!(publish(&mut owner, staged).events, 1);
    let directory = tempfile::tempdir().unwrap();
    let mut store = store(&directory);
    let input = report_input(&owner, required, 2002, VerdictValue::Fail);
    let staged = owner
        .prepare_with_custody(
            f::context(EVALUATOR, 100),
            input,
            &mut store,
            ContentDomainId::from_u128(93),
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let failed = publish(&mut owner, staged);
    let claim_cut = owner
        .committed()
        .claim(required.claim)
        .unwrap()
        .terminal_cut();
    let response_cut = owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap()
        .terminal();
    let work_cut = owner
        .committed()
        .work(ArtifactId::from_u128(800))
        .unwrap()
        .state
        .terminal();
    assert_eq!(
        owner.committed().claim(required.claim).unwrap().status(),
        ClaimStatus::ValidationFailed
    );
    assert!(owner.committed().evaluation(observe).unwrap().has_begun());
    let input = report_input(&owner, observe, 2003, VerdictValue::Pass);
    let staged = owner
        .prepare_with_custody(
            f::context(EVALUATOR, 100),
            input,
            &mut store,
            ContentDomainId::from_u128(93),
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let late = publish(&mut owner, staged);
    assert_eq!((late.events, late.changed, late.responses), (3, 0, 0));
    assert!(late.sequence > failed.sequence);
    assert_eq!(
        owner
            .committed()
            .claim(required.claim)
            .unwrap()
            .terminal_cut(),
        claim_cut
    );
    assert_eq!(
        owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .terminal(),
        response_cut
    );
    assert_eq!(
        owner
            .committed()
            .work(ArtifactId::from_u128(800))
            .unwrap()
            .state
            .terminal(),
        work_cut
    );
    assert!(matches!(
        owner.committed().event(late.sequence, 2).unwrap().fact,
        NativeFact::Accepted { .. }
    ));
}

#[test]
fn first_evaluator_entry_refuses_wrong_actor_and_rolls_back_as_one_candidate() {
    let fixture = Fixture::new(true);
    let key = fixture.key(1);
    let mut owner = NativeOwner::new(fixture.core).unwrap();
    let original_claim = owner.effective().claim(key.claim).unwrap().binding();
    let original_response = owner
        .effective()
        .response_record(TestamentId::from_u128(900))
        .unwrap()
        .received();
    let before = owner.budget_stats();
    let mut input = begin_input(&owner, key, 2100);
    input.request = f::request(ISSUER, 2100);
    assert!(owner.prepare(f::context(ISSUER, 100), input, None).is_err());
    assert_eq!(owner.budget_stats(), before);
    let input = begin_input(&owner, key, 2101);
    let staged = owner
        .prepare(f::context(EVALUATOR, 100), input, None)
        .unwrap();
    let NativeStaging::Prepared { candidate, .. } = staged else {
        panic!("candidate")
    };
    assert_eq!(
        owner
            .effective()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Validating
    );
    assert_eq!(owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(
        owner.effective().claim(key.claim).unwrap().binding(),
        original_claim
    );
    let response = owner
        .effective()
        .response_record(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(response.response().state(), ResponseState::Received);
    assert_eq!(response.received(), original_response);
    assert_eq!(response.entered(), None);
    assert_eq!(
        owner
            .effective()
            .work(ArtifactId::from_u128(800))
            .unwrap()
            .state
            .state(),
        WorkArtifactState::Attached
    );
    assert_eq!(
        owner.effective().evaluation(key).unwrap().state(),
        validation::State::Ready
    );
    assert_eq!(owner.budget_stats(), before);
}

#[test]
fn direct_core_cannot_accept_unfunded_work_responsibility() {
    let fixture = Fixture::new(true);
    let key = fixture.key(1);
    let before = fixture.core.native_budget();
    let input = NativeInput {
        request: f::request(EVALUATOR, 2200),
        command: NativeCommand::BeginWork {
            claim: fixture.claim(),
            key,
            expected: fixture.core.native_evaluation(key).unwrap().binding(),
        },
    };
    assert!(matches!(
        fixture
            .core
            .prepare_native(f::context(EVALUATOR, 100), input, &[]),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    assert_eq!(fixture.core.native_budget(), before);
    assert_eq!(fixture.response().state(), ResponseState::Received);
    assert!(!fixture.core.native_evaluation(key).unwrap().has_begun());
}

#[test]
fn reconstructed_work_owner_funds_remaining_attempt_after_recorded_retryable_error() {
    use crate::native::completion_envelope::{
        CompletionEnvelope, EvidenceBounds, descriptor_limits,
    };
    use crate::native::completion_schemas::SchemaSet;
    use crate::native::prepare::Checked;
    use focal_memory::BudgetKind;

    let mut fixture = Fixture::new(true);
    let key = fixture.key(1);
    let source = fixture.core.state.budget.clone();
    let envelope = {
        let view = fixture.view();
        let registered =
            crate::native::admission_authority::registered_any(&view, fixture.claim(), key)
                .unwrap();
        let pins = SchemaSet::new(
            registered.definition,
            &source,
            &BuiltinNativeSchemas,
            fixture.core.limits.plan_edges,
        )
        .unwrap();
        let (envelope, members) = CompletionEnvelope::derive_work(
            &view,
            fixture.core.limits,
            &registered,
            descriptor_limits(fixture.core.limits, registered.parent, registered.registry).unwrap(),
            EvidenceBounds {
                workspace_bytes: pins.workspace_bytes() - pins.custody_bytes(),
                retained_bytes: pins.custody_bytes(),
            },
        )
        .unwrap();
        drop(members);
        envelope
    };
    // Match the existing native recovery harness: publish actual checked
    // transactions into a retained Core, then construct its exclusive owner.
    // This exercises owner reconstruction, not an unimplemented disk codec.
    let input = NativeInput {
        request: f::request(EVALUATOR, 2300),
        command: NativeCommand::BeginWork {
            claim: fixture.claim(),
            key,
            expected: fixture.core.native_evaluation(key).unwrap().binding(),
        },
    };
    let Checked::Fresh(fresh) = fixture
        .core
        .check_native_chain(f::context(EVALUATOR, 100), input, std::iter::empty())
        .unwrap()
    else {
        panic!("fresh recorded Begin")
    };
    let begin = fresh
        .build_with_completion(&source, None, Some(&envelope))
        .unwrap();
    fixture.core.publish_native(begin).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let mut store = store(&directory);
    let (report, descriptor) = fixture.report(1, VerdictValue::Error, &["internal", "team:qa"]);
    let request = f::request(EVALUATOR, 2301);
    let verified = store
        .verify_native_artifact(
            request,
            &descriptor,
            ContentDomainId::from_u128(93),
            &source,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let input = NativeInput {
        request,
        command: NativeCommand::ReportWork {
            claim: fixture.claim(),
            key,
            expected: fixture.core.native_evaluation(key).unwrap().binding(),
            report,
            artifact: NativeArtifactInput::new(descriptor).unwrap(),
        },
    };
    let Checked::Fresh(fresh) = fixture
        .core
        .check_native_chain(f::context(EVALUATOR, 100), input, std::iter::empty())
        .unwrap()
    else {
        panic!("fresh recorded Error")
    };
    let retry = fresh
        .build_with_completion(&source, Some(&verified), Some(&envelope))
        .unwrap();
    let retry = fixture.core.publish_native(retry).unwrap();
    drop(verified);
    let state = *fixture.core.native_evaluation(key).unwrap();
    assert!(state.has_begun());
    assert!(!state.state().is_terminal());
    let attempt = state
        .bind(fixture.core.native_definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    assert_eq!(attempt.index, report.attempt.index + 1);
    let original_result = NativeResultKey::of(state.last_result().unwrap());
    let original_response = fixture.response().identity();

    let mut owner = NativeOwner::new(fixture.core).unwrap();
    assert_eq!(*owner.committed().evaluation(key).unwrap(), state);
    assert_eq!(
        owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .identity(),
        original_response
    );
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    assert_eq!(source.stats().used, source.limit());
    let input = report_input(&owner, key, 2302, VerdictValue::Pass);
    let staged = owner
        .prepare_with_custody(
            f::context(EVALUATOR, 100),
            input,
            &mut store,
            ContentDomainId::from_u128(93),
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let done = publish(&mut owner, staged);
    assert!(done.sequence > retry.sequence);
    assert_eq!(
        owner.committed().evaluation(key).unwrap().state(),
        validation::State::Validated
    );
    assert_eq!(
        owner.committed().claim(key.claim).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Validated
    );
    let original = owner.committed().result(original_result).unwrap();
    assert_eq!(original.result().verdict(), VerdictValue::Error);
    assert_eq!(
        (original.sequence(), original.ordinal()),
        (retry.sequence, 2)
    );
    drop(pressure);
    drop(owner);
    assert_eq!(source.stats().used, 0);
}
