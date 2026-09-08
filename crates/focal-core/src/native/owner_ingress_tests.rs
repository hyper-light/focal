use super::*;
use crate::native::input_codec::*;
use crate::native::report_tests::{
    Custody, EVALUATOR, ISSUER, artifact_spec, binding, context, core, creation, descriptor, key,
    post, report_for, running, verified,
};
use focal_model::lifecycle::artifact_descriptor::Limits as ArtifactLimits;
use focal_model::{ValidationMode, VerdictValue};

fn encoded(input: &NativeInput, profile: NativeContentProfile) -> Vec<u8> {
    let plan = EncodingPlan::prepare(
        InputFrame::Request {
            ledger: binding(1).ledger,
            profile,
            input,
        },
        EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 22,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn inspected(bytes: &[u8]) -> StructuralInput<'_> {
    StructuralInput::inspect(
        bytes,
        InspectionLimits {
            bytes: 1 << 20,
            visits: 1 << 22,
            items: 4096,
            text_bytes: 1 << 20,
            blob_bytes: 1 << 20,
        },
    )
    .unwrap()
}
fn artifact_limits() -> ArtifactLimits {
    ArtifactLimits {
        kind_bytes: 128,
        metadata_bytes: 4096,
        inline_bytes: 4096,
        inputs: 16,
        visibility_labels: 16,
        visibility_label_bytes: 128,
        construction_bytes: 65536,
    }
}
fn stage_report(
    owner: &mut NativeOwner,
    bytes: &[u8],
    actor: ParticipantId,
    evidence: Option<&VerifiedNativeArtifact>,
    work: usize,
) -> Result<NativeStaging, NativeOwnerError> {
    let frame = inspected(bytes);
    let mut body = frame.artifact_input(1 << 22).unwrap().unwrap();
    let plan = body
        .prepare(
            owner.core.limits,
            artifact_limits(),
            1 << 22,
            1 << 22,
            1 << 22,
        )
        .unwrap();
    owner.prepare_decoded(context(actor, 100), plan.into(), work, evidence)
}
fn report_fixture(id: u128) -> (NativeOwner, NativeInput, MemoryBudget) {
    let core = running(&[(ValidationMode::Required, false)]);
    let report = report_for(
        &core,
        None,
        id,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(40000 + id, EVALUATOR, VerdictValue::Pass)),
    );
    let budget = core.state.budget.clone();
    (NativeOwner::new(core).unwrap(), report, budget)
}
fn exhaust(budget: &MemoryBudget) -> Allocation {
    budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit()
}
fn ticket(staging: NativeStaging) -> NativeCandidate {
    let NativeStaging::Prepared { candidate, .. } = staging else {
        panic!("fresh candidate")
    };
    candidate
}

#[test]
fn decoded_admission_uses_held_input_capacity_with_all_parent_memory_exhausted() {
    let (mut owner, input, budget) = report_fixture(501);
    let bytes = encoded(&input, NativeContentProfile::ProjectionOnly);
    let mut custody = Custody::new();
    let proof = verified(&mut custody, &input);
    drop(input);
    let _pressure = exhaust(&budget);
    let pool_before = owner.book.source().stats();
    let candidate =
        ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
    let pool_pending = owner.book.source().stats();
    assert!(pool_pending.used > pool_before.used);
    // No second construction allowance, current-state report authority or
    // queue admission is needed for the exact retained request.
    assert!(
        matches!(stage_report(&mut owner, &bytes, EVALUATOR, None, 0).unwrap(),
        NativeStaging::Existing { candidate: Some(found), .. } if found == candidate)
    );
    assert_eq!(owner.book.source().stats(), pool_pending);
    owner.publish_after_durable(candidate).unwrap();
    assert!(matches!(
        stage_report(&mut owner, &bytes, EVALUATOR, None, 0).unwrap(),
        NativeStaging::Existing {
            candidate: None,
            ..
        }
    ));
    assert_eq!(
        owner.committed().evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    assert_eq!(
        owner
            .committed()
            .claim(key(1).claim)
            .unwrap()
            .response_count(),
        0
    );
}

#[test]
fn decoded_report_refusals_release_ingress_capacity_and_preserve_the_promise() {
    let (mut owner, input, budget) = report_fixture(502);
    let bytes = encoded(&input, NativeContentProfile::ProjectionOnly);
    let mut custody = Custody::new();
    let proof = verified(&mut custody, &input);
    let _pressure = exhaust(&budget);
    let before = owner.book.source().stats();
    for (actor, evidence, work) in [
        (ISSUER, Some(&proof), 1 << 24),
        (EVALUATOR, None, 1 << 24),
        (EVALUATOR, Some(&proof), 0),
    ] {
        assert!(stage_report(&mut owner, &bytes, actor, evidence, work).is_err());
        assert_eq!(owner.pending_len(), 0);
        assert_eq!(owner.book.source().stats(), before);
    }
    let candidate =
        ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
    owner.discard_from(candidate).unwrap();
    assert_eq!(owner.book.source().stats(), before);
    ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
}

#[test]
fn decoded_identity_conflict_and_wrong_actor_do_not_replay_a_pending_success() {
    let (mut owner, input, _) = report_fixture(503);
    let bytes = encoded(&input, NativeContentProfile::ProjectionOnly);
    let mut custody = Custody::new();
    let proof = verified(&mut custody, &input);
    ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
    assert!(matches!(
        stage_report(&mut owner, &bytes, ISSUER, None, 0),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::WrongActor
        )))
    ));
    let changed = report_for(
        &owner.core,
        None,
        503,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(90909, EVALUATOR, VerdictValue::Pass)),
    );
    let changed = encoded(&changed, NativeContentProfile::ProjectionOnly);
    assert!(matches!(
        stage_report(&mut owner, &changed, EVALUATOR, None, 0),
        Err(NativeOwnerError::Native(NativeError::RequestConflict))
    ));
    assert_eq!(owner.pending_len(), 1);
}

#[test]
fn decoded_projection_and_fixed_commands_follow_one_owner_pending_chain() {
    let mut owner = NativeOwner::new(core()).unwrap();
    let bytes = encoded(
        &creation(601, 1, &[], None),
        NativeContentProfile::ProjectionOnly,
    );
    let frame = inspected(&bytes);
    let limits = LegacyCreationLimits {
        declaration: validation::Limits {
            handlers: 8,
            attempts: 16,
            slot_bytes: 128,
        },
        acceptance: focal_model::lifecycle::aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 32,
            max_updates: 32,
        },
        bytes: 1 << 20,
        work: LegacyCreationWork {
            parsing: 1 << 24,
            source: 1 << 24,
            declarations: 1 << 24,
            acceptance: 1 << 24,
            structure: 1 << 24,
        },
    };
    let plan = frame
        .prepare_legacy_creation(owner.core.limits, limits)
        .unwrap()
        .unwrap();
    let visits = DecodedRequest::from(plan).construction_visits().unwrap();
    let before = owner.core.state.budget.stats();
    let plan = frame
        .prepare_legacy_creation(owner.core.limits, limits)
        .unwrap()
        .unwrap();
    assert!(
        owner
            .prepare_decoded(context(ISSUER, 10), plan.into(), visits - 1, None)
            .is_err()
    );
    assert_eq!(owner.core.state.budget.stats(), before);
    let plan = frame
        .prepare_legacy_creation(owner.core.limits, limits)
        .unwrap()
        .unwrap();
    let created = ticket(
        owner
            .prepare_decoded(context(ISSUER, 10), plan.into(), visits, None)
            .unwrap(),
    );
    let bytes = encoded(&post(602, binding(1)), NativeContentProfile::ProjectionOnly);
    let plan = inspected(&bytes).decode_fixed(1 << 20).unwrap().unwrap();
    let posted = ticket(
        owner
            .prepare_decoded(context(ISSUER, 20), plan.try_into().unwrap(), 0, None)
            .unwrap(),
    );
    assert_eq!(owner.pending_len(), 2);
    assert_eq!(owner.committed().sequence(), SessionSeq(0));
    assert_eq!(
        owner
            .effective()
            .claim(key(1).claim)
            .unwrap()
            .response_count(),
        0
    );
    owner.publish_after_durable(created).unwrap();
    owner.publish_after_durable(posted).unwrap();
}

#[test]
fn decoded_headers_and_timer_namespace_cannot_substitute_owner_authority() {
    let mut owner = NativeOwner::new(core()).unwrap();
    for wrong_profile in [false, true] {
        let mut ledger = binding(1).ledger;
        if !wrong_profile {
            ledger.session = focal_model::SessionId::from_u128(99999);
        }
        let frame = FixedFrame::Request {
            ledger,
            profile: if wrong_profile {
                NativeContentProfile::AuthoredV1
            } else {
                NativeContentProfile::ProjectionOnly
            },
            input: post(701, binding(1)),
        };
        assert!(
            owner
                .prepare_decoded(context(ISSUER, 20), frame.try_into().unwrap(), 0, None)
                .is_err()
        );
    }
    let timer = FixedFrame::ClaimDeadline {
        ledger: binding(1).ledger,
        profile: NativeContentProfile::ProjectionOnly,
        input: NativeClaimDeadlineInput {
            claim: key(1).claim,
            deadline: Deadline {
                timer: focal_model::TimerId::from_u128(2),
                generation: 1,
                at: 20,
            },
        },
    };
    assert!(matches!(
        DecodedRequest::try_from(timer),
        Err(DecodeError::Native(NativeError::Contract(
            ContractError::WrongActor
        )))
    ));
    assert_eq!(owner.pending_len(), 0);
}

#[test]
fn borrowed_authority_cannot_consume_the_final_artifact_construction_pass() {
    let core = running(&[(ValidationMode::Required, false)]);
    let labels = ["restricted"];
    let inputs = [focal_model::ObjectRef::claim(
        binding(1).ledger,
        key(1).claim,
    )];
    let mut spec = artifact_spec(80808, EVALUATOR, VerdictValue::Pass);
    spec.visibility = &labels;
    spec.inputs = &inputs;
    let input = report_for(&core, None, 808, 1, VerdictValue::Pass, descriptor(spec));
    let mut custody = Custody::new();
    let proof = verified(&mut custody, &input);
    let bytes = encoded(&input, NativeContentProfile::ProjectionOnly);
    let frame = inspected(&bytes);
    let mut owner = NativeOwner::new(core).unwrap();
    let mut body = frame.artifact_input(1 << 22).unwrap().unwrap();
    let quote = body
        .prepare(
            owner.core.limits,
            artifact_limits(),
            1 << 22,
            1 << 22,
            1 << 22,
        )
        .unwrap()
        .quote();
    assert!(quote.source_build_visits > 0);
    // Enough for initial source inspection plus report authority/envelope,
    // with nothing left for the source pass that constructs final buffers.
    let source = quote.source_inspection_visits * 2;
    let plan = body
        .prepare(
            owner.core.limits,
            artifact_limits(),
            1 << 22,
            source,
            1 << 22,
        )
        .unwrap();
    let before = owner.book.source().stats();
    assert!(
        owner
            .prepare_decoded(context(EVALUATOR, 100), plan.into(), 1 << 24, Some(&proof))
            .is_err()
    );
    assert_eq!(owner.book.source().stats(), before);
    assert_eq!(owner.pending_len(), 0);
    ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
}

#[test]
fn raw_owner_ingress_preserves_held_reporting_and_exact_retry_without_build_headroom() {
    let (mut owner, input, budget) = report_fixture(901);
    let bytes = encoded(&input, NativeContentProfile::ProjectionOnly);
    let mut custody = Custody::new();
    let proof = verified(&mut custody, &input);
    drop(input);
    let limits = NativeDecodeLimits::for_native(
        owner.core.limits,
        1 << 20,
        DecodeWork {
            parse: 1 << 24,
            source: 1 << 24,
            model: 1 << 24,
            acceptance: 1 << 24,
            native: 1 << 24,
        },
    )
    .unwrap();
    let quote = limits
        .with_request(owner.core.limits, &bytes, |_, quote| quote)
        .unwrap();
    let inspect_only = NativeDecodeLimits {
        work: quote.preparation,
        ..limits
    };
    let _pressure = exhaust(&budget);
    let before = owner.book.source().stats();
    assert!(
        owner
            .prepare_frame(context(EVALUATOR, 100), &bytes, inspect_only, Some(&proof))
            .is_err()
    );
    assert_eq!(owner.book.source().stats(), before);
    let candidate = ticket(
        owner
            .prepare_frame(context(EVALUATOR, 100), &bytes, limits, Some(&proof))
            .unwrap(),
    );
    assert!(
        matches!(owner.prepare_frame(context(EVALUATOR, 0), &bytes, inspect_only, None).unwrap(), NativeStaging::Existing { candidate: Some(found), .. } if found == candidate)
    );
    owner.publish_after_durable(candidate).unwrap();
    assert!(matches!(
        owner
            .prepare_frame(context(EVALUATOR, 0), &bytes, inspect_only, None)
            .unwrap(),
        NativeStaging::Existing {
            candidate: None,
            ..
        }
    ));
}

#[test]
fn raw_actor_header_is_checked_before_scanning_an_untrusted_body() {
    let (mut owner, input, _) = report_fixture(902);
    let bytes = encoded(&input, NativeContentProfile::ProjectionOnly);
    let limits = NativeDecodeLimits::for_native(
        owner.core.limits,
        1 << 20,
        DecodeWork {
            parse: 1 << 24,
            source: 1 << 24,
            model: 1 << 24,
            acceptance: 1 << 24,
            native: 1 << 24,
        },
    )
    .unwrap();
    let header = &bytes[..85];
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 100), header, limits, None),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::WrongActor
        )))
    ));
    assert!(matches!(
        owner.prepare_frame(context(EVALUATOR, 100), header, limits, None),
        Err(NativeOwnerError::Input(CodecError::Truncated))
    ));
    assert_eq!(owner.pending_len(), 0);
}
