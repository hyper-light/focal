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
            BudgetLane::Ordinary,
            budget.headroom(BudgetLane::Ordinary),
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
    let pool_before = owner.core.state.budget.stats();
    let candidate =
        ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
    let pool_pending = owner.core.state.budget.stats();
    assert!(pool_pending.used > pool_before.used);
    // No second construction allowance, current-state report authority or
    // queue admission is needed for the exact retained request.
    assert!(
        matches!(stage_report(&mut owner, &bytes, EVALUATOR, None, 0).unwrap(),
        NativeStaging::Existing { candidate: Some(found), .. } if found == candidate)
    );
    assert_eq!(owner.core.state.budget.stats(), pool_pending);
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
    let before = owner.core.state.budget.stats();
    for (actor, evidence, work) in [
        (ISSUER, Some(&proof), 1 << 24),
        (EVALUATOR, None, 1 << 24),
        (EVALUATOR, Some(&proof), 0),
    ] {
        assert!(stage_report(&mut owner, &bytes, actor, evidence, work).is_err());
        assert_eq!(owner.pending_len(), 0);
        assert_eq!(owner.core.state.budget.stats(), before);
    }
    let candidate =
        ticket(stage_report(&mut owner, &bytes, EVALUATOR, Some(&proof), 1 << 24).unwrap());
    owner.discard_from(candidate).unwrap();
    assert_eq!(owner.core.state.budget.stats(), before);
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
    let before = owner.core.state.budget.stats();
    assert!(
        owner
            .prepare_decoded(context(EVALUATOR, 100), plan.into(), 1 << 24, Some(&proof))
            .is_err()
    );
    assert_eq!(owner.core.state.budget.stats(), before);
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
    let before = owner.core.state.budget.stats();
    assert!(
        owner
            .prepare_frame(context(EVALUATOR, 100), &bytes, inspect_only, Some(&proof))
            .is_err()
    );
    assert_eq!(owner.core.state.budget.stats(), before);
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

/// An owner holding as many candidates as its memory funds refuses a fresh
/// request from its frame's header alone, before reading the rest: a header
/// with nothing after it is refused for the full queue where an owner with room
/// reads on and refuses it as truncated. Its queue's places filled and the
/// memory that would grow it taken, it is full. Its exact retry of a pending
/// request is still answered, and a header naming another actor is still
/// refused as such.
#[test]
fn a_full_owner_refuses_a_fresh_request_from_its_header() {
    let mut owner = NativeOwner::new(core()).unwrap();
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
    let full = owner.core.limits.pending;
    let mut first = None;
    for n in 0..full {
        let id = 800 + u128::try_from(n).unwrap();
        let bytes = encoded(
            &creation(id, id, &[], None),
            NativeContentProfile::ProjectionOnly,
        );
        let staged = owner
            .prepare_frame(context(ISSUER, 10), &bytes, limits, None)
            .unwrap();
        if first.is_none() {
            first = Some((bytes, ticket(staged)));
        }
    }
    assert_eq!(owner.pending_len(), full);
    // The queue's places are filled; with the memory that would double them taken, nothing more
    // is funded.
    let budget = owner.core.state.budget.clone();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            budget.headroom(BudgetLane::Ordinary),
        )
        .unwrap();
    let fresh = encoded(
        &creation(900, 900, &[], None),
        NativeContentProfile::ProjectionOnly,
    );
    let header = &fresh[..85];
    for frame in [header, &fresh[..]] {
        assert!(matches!(
            owner.prepare_frame(context(ISSUER, 10), frame, limits, None),
            Err(NativeOwnerError::Native(NativeError::Capacity(
                "pending candidates"
            )))
        ));
    }
    let (bytes, candidate) = first.unwrap();
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 10), &bytes, limits, None),
        Ok(NativeStaging::Existing { candidate: Some(retried), .. }) if retried == candidate
    ));
    assert!(matches!(
        owner.prepare_frame(context(EVALUATOR, 10), header, limits, None),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::WrongActor
        )))
    ));
    drop(pressure);
    owner.publish_after_durable(candidate).unwrap();
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 10), header, limits, None),
        Err(NativeOwnerError::Input(CodecError::Truncated))
    ));
    assert_eq!(owner.pending_len(), full - 1);
}

/// A fresh request the owner's own memory refused leaves its shortfall:
/// another of its command, its frame as large, is refused from its header,
/// no reservation tried (the budget meets no further refusal), while the
/// headroom stays short of what the refused one needed; every
/// `SHORTFALL_PROBE`th is prepared after all, and refused on its merits; an
/// exact retry is still answered; a smaller frame and another command are
/// prepared as before; and memory given back ends the shortfall. A capacity
/// the budget did not refuse leaves none.
#[test]
fn a_request_the_memory_refused_refuses_its_like_from_its_header() {
    let mut owner = NativeOwner::new(core()).unwrap();
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
    let frame = |id: u128, requirements: &[(ValidationMode, bool)]| {
        encoded(
            &creation(id, id, requirements, None),
            NativeContentProfile::ProjectionOnly,
        )
    };
    let large = |id: u128| frame(id, &[(ValidationMode::Required, false)]);
    let retried = large(700);
    let candidate = ticket(
        owner
            .prepare_frame(context(ISSUER, 10), &retried, limits, None)
            .unwrap(),
    );
    // The ordinary lane squeezed to less than any creation or post holds.
    let budget = owner.core.state.budget.clone();
    let left = 8 * 1024;
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            budget.headroom(BudgetLane::Ordinary) - left,
        )
        .unwrap();
    let refusals = budget.refusals().count;
    let first = owner.prepare_frame(context(ISSUER, 10), &large(701), limits, None);
    assert!(
        matches!(
            first,
            Err(NativeOwnerError::Native(NativeError::Memory(
                MemoryError::Capacity { .. }
            )))
        ),
        "{first:?}"
    );
    assert_eq!(budget.refusals().count, refusals + 1);
    let Need::Memory { bytes: need, .. } = owner.shortfalls[0].unwrap().need else {
        panic!("a memory refusal left another shortfall");
    };
    assert!(need > left, "{need}");
    for n in 1..SHORTFALL_PROBE {
        let refused = owner.prepare_frame(
            context(ISSUER, 10),
            &large(701 + u128::from(n)),
            limits,
            None,
        );
        assert!(
            matches!(
                refused,
                Err(NativeOwnerError::Native(NativeError::Memory(
                    MemoryError::Capacity { requested, available }
                ))) if requested == need && available == left
            ),
            "{n}: {refused:?}"
        );
        assert_eq!(budget.refusals().count, refusals + 1, "{n}");
    }
    let probe = owner.prepare_frame(context(ISSUER, 10), &large(800), limits, None);
    assert!(
        matches!(
            probe,
            Err(NativeOwnerError::Native(NativeError::Memory(
                MemoryError::Capacity { .. }
            )))
        ),
        "{probe:?}"
    );
    assert_eq!(budget.refusals().count, refusals + 2);
    // Refused again, the next probe waits twice as long.
    for n in 1..2 * SHORTFALL_PROBE {
        let id = 2000 + u128::from(n);
        assert!(
            owner
                .prepare_frame(context(ISSUER, 10), &large(id), limits, None)
                .is_err()
        );
        assert_eq!(budget.refusals().count, refusals + 2, "{n}");
    }
    assert!(
        owner
            .prepare_frame(context(ISSUER, 10), &large(3000), limits, None)
            .is_err()
    );
    assert_eq!(budget.refusals().count, refusals + 3);
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 10), &retried, limits, None),
        Ok(NativeStaging::Existing { candidate: Some(again), .. }) if again == candidate
    ));
    // A smaller frame of the command is prepared, and refused on its merits.
    let smaller = owner.prepare_frame(context(ISSUER, 10), &frame(801, &[]), limits, None);
    assert!(
        matches!(
            smaller,
            Err(NativeOwnerError::Native(NativeError::Memory(
                MemoryError::Capacity { .. }
            )))
        ),
        "{smaller:?}"
    );
    assert_eq!(budget.refusals().count, refusals + 4);
    // Another command is decoded and prepared, not refused from the
    // creation's header: charged only what it holds, it reaches its own
    // check (its target is absent) and leaves no shortfall.
    let other = encoded(
        &post(802, binding(802)),
        NativeContentProfile::ProjectionOnly,
    );
    let posted = owner.prepare_frame(context(ISSUER, 10), &other, limits, None);
    assert!(
        matches!(
            posted,
            Err(NativeOwnerError::Native(NativeError::Contract(
                ContractError::InvalidTarget
            )))
        ),
        "{posted:?}"
    );
    assert_eq!(budget.refusals().count, refusals + 4);
    assert_eq!(owner.shortfalls.iter().flatten().count(), 1);
    let refused = owner.prepare_frame(context(ISSUER, 10), &large(805), limits, None);
    assert!(
        matches!(
            refused,
            Err(NativeOwnerError::Native(NativeError::Memory(
                MemoryError::Capacity { .. }
            )))
        ),
        "{refused:?}"
    );
    assert_eq!(budget.refusals().count, refusals + 4);
    // A capacity the budget did not refuse leaves no shortfall.
    let held = owner.shortfalls;
    owner.shortfalls = [None; SHORTFALLS];
    let before = Before {
        refusals: budget.refusals().count,
        ordinary: budget.headroom(BudgetLane::Ordinary),
        completion: budget.headroom(BudgetLane::Completion),
    };
    owner.note_shortfall(
        &large(803),
        before,
        &NativeOwnerError::Native(NativeError::Memory(MemoryError::Capacity {
            requested: 300,
            available: 256,
        })),
    );
    assert!(owner.shortfalls.iter().all(Option::is_none));
    owner.shortfalls = held;
    // Memory given back ends it.
    drop(pressure);
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 10), &large(804), limits, None),
        Ok(NativeStaging::Prepared { .. })
    ));
    assert!(
        owner
            .shortfalls
            .iter()
            .flatten()
            .all(|shortfall| shortfall.command != command(&large(806)).unwrap())
    );
}

/// The owner's preparation of a fresh claim creation, and then of 5,000 more
/// once its memory refuses them: what each refusal costs and how many reach
/// the budget (the shortfall's probes). Run with `--ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_refusal_at_the_memory_bound() {
    let mut owner = NativeOwner::new(core()).unwrap();
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
    let frame = |id: u128| {
        encoded(
            &creation(id, id, &[], None),
            NativeContentProfile::ProjectionOnly,
        )
    };
    let mut id = 10_000u128;
    let mut prepared_ns = 0u128;
    let mut prepared = 0u128;
    loop {
        id += 1;
        let bytes = frame(id);
        let started = std::time::Instant::now();
        let staged = owner.prepare_frame(context(ISSUER, 10), &bytes, limits, None);
        let took = started.elapsed().as_nanos();
        match staged {
            Ok(NativeStaging::Prepared { candidate, .. }) => {
                prepared_ns += took;
                prepared += 1;
                owner.publish_after_durable(candidate).unwrap();
            }
            Ok(other) => panic!("{other:?}"),
            Err(error) => {
                println!("first refusal after {prepared} claims: {error}");
                break;
            }
        }
    }
    let budget = owner.core.state.budget.clone();
    let frames: Vec<_> = (0..5000u128).map(|n| frame(id + 1 + n)).collect();
    let before = budget.refusals().count;
    let started = std::time::Instant::now();
    let mut refused = 0usize;
    for bytes in &frames {
        if owner
            .prepare_frame(context(ISSUER, 10), bytes, limits, None)
            .is_err()
        {
            refused += 1;
        }
    }
    let each = started.elapsed().as_nanos() / 5000;
    println!(
        "prepared {:.1} µs each; then {refused} of 5000 refused at {:.2} µs each, {} reaching the budget",
        prepared_ns as f64 / prepared as f64 / 1000.0,
        each as f64 / 1000.0,
        budget.refusals().count - before
    );
}

/// A bound on `owner`'s checkpoint that leaves fresh work no room past the root as it stands,
/// pending candidates published: the least whose ordinary share reaches the projection.
fn bound_at_projection(owner: &mut NativeOwner) {
    let projection = owner.checkpoint_projection().unwrap();
    let least = |at: usize| {
        let mut whole = at;
        while whole - whole / 8 < at {
            whole += 1;
        }
        whole
    };
    owner
        .core
        .bound_checkpoint(Some(crate::native::CheckpointBound {
            rows: least(projection.rows),
            bytes: least(projection.bytes),
        }));
}

/// A request the checkpoint's bound refused leaves its shortfall as one the memory refused does:
/// another of its command, its frame as large, is refused from its header while the root has no
/// room for what the first grew it by, every `SHORTFALL_PROBE`th prepared after all and, refused
/// again, the next probe twice as far; an exact retry is answered; and room made (the pending
/// candidate discarded) ends it.
#[test]
fn a_request_the_checkpoint_refused_refuses_its_like_from_its_header() {
    let mut owner = NativeOwner::new(core()).unwrap();
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
    let frame = |id: u128| {
        encoded(
            &creation(id, id, &[(ValidationMode::Required, false)], None),
            NativeContentProfile::ProjectionOnly,
        )
    };
    let retried = frame(700);
    let pending = ticket(
        owner
            .prepare_frame(context(ISSUER, 10), &retried, limits, None)
            .unwrap(),
    );
    bound_at_projection(&mut owner);
    let refused = |result: &Result<NativeStaging, NativeOwnerError>| {
        matches!(
            result,
            Err(NativeOwnerError::Native(NativeError::Capacity(
                CHECKPOINT_REFUSAL
            )))
        )
    };
    let first = owner.prepare_frame(context(ISSUER, 10), &frame(701), limits, None);
    assert!(refused(&first), "{first:?}");
    let shortfall = owner.shortfalls.iter().flatten().next().copied().unwrap();
    let Need::Checkpoint { lane, rows, bytes } = shortfall.need else {
        panic!("{shortfall:?}");
    };
    assert_eq!(lane, BudgetLane::Ordinary);
    assert!(rows > 0 && bytes > 0, "{shortfall:?}");
    for n in 1..SHORTFALL_PROBE {
        let header = owner.prepare_frame(
            context(ISSUER, 10),
            &frame(701 + u128::from(n)),
            limits,
            None,
        );
        assert!(refused(&header), "{n}: {header:?}");
        let now = owner.shortfalls.iter().flatten().next().copied().unwrap();
        assert_eq!(now.refused, n, "{n}: refused from its header");
    }
    // The probe is prepared, refused on its merits, and the next waits twice as long.
    let probe = owner.prepare_frame(context(ISSUER, 10), &frame(800), limits, None);
    assert!(refused(&probe), "{probe:?}");
    let now = owner.shortfalls.iter().flatten().next().copied().unwrap();
    assert_eq!((now.refused, now.probe), (0, 2 * SHORTFALL_PROBE));
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 10), &retried, limits, None),
        Ok(NativeStaging::Existing { candidate: Some(again), .. }) if again == pending
    ));
    // Room made ends it: the next like request is prepared and admitted.
    owner.discard_from(pending).unwrap();
    assert!(matches!(
        owner.prepare_frame(context(ISSUER, 10), &frame(900), limits, None),
        Ok(NativeStaging::Prepared { .. })
    ));
    assert!(owner.shortfalls.iter().all(Option::is_none));
}

/// The owner's preparation of fresh claim creations to the checkpoint's bound, then of 5,000
/// more it refuses there: what each refusal costs and how many are prepared (the shortfall's
/// probes). Run with `--ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_refusal_at_the_checkpoint_bound() {
    let mut owner = NativeOwner::new(core()).unwrap();
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
    let frame = |id: u128| {
        encoded(
            &creation(id, id, &[], None),
            NativeContentProfile::ProjectionOnly,
        )
    };
    let mut id = 10_000u128;
    let mut prepared_ns = 0u128;
    for _ in 0..200 {
        id += 1;
        let bytes = frame(id);
        let started = std::time::Instant::now();
        let staged = owner.prepare_frame(context(ISSUER, 10), &bytes, limits, None);
        prepared_ns += started.elapsed().as_nanos();
        let Ok(NativeStaging::Prepared { candidate, .. }) = staged else {
            panic!("{staged:?}");
        };
        owner.publish_after_durable(candidate).unwrap();
    }
    bound_at_projection(&mut owner);
    let frames: Vec<_> = (0..5000u128).map(|n| frame(id + 1 + n)).collect();
    let started = std::time::Instant::now();
    let mut refused = 0usize;
    for bytes in &frames {
        if owner
            .prepare_frame(context(ISSUER, 10), bytes, limits, None)
            .is_err()
        {
            refused += 1;
        }
    }
    let each = started.elapsed().as_nanos() / 5000;
    let probes = owner
        .shortfalls
        .iter()
        .flatten()
        .map(|shortfall| shortfall.probe)
        .max()
        .unwrap_or(0);
    println!(
        "prepared {:.1} µs each; then {refused} of 5000 refused at {:.2} µs each (probe every {probes})",
        prepared_ns as f64 / 200.0 / 1000.0,
        each as f64 / 1000.0,
    );
}
