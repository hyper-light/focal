use super::*;

fn deadline_input(owner: &NativeOwner, index: u32) -> NativeDeadlineInput {
    let evaluation = key(index);
    let view = owner.effective();
    let deadline = view
        .evaluation(evaluation)
        .unwrap()
        .bind(view.definition(evaluation.validation).unwrap())
        .unwrap()
        .deadline();
    NativeDeadlineInput {
        evaluation,
        deadline,
    }
}

fn stage_deadline(
    owner: &mut NativeOwner,
    input: NativeDeadlineInput,
    time: u64,
) -> (NativeCandidate, NativeOutcome) {
    match owner.prepare_evaluation_deadline(input, time).unwrap() {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        existing => panic!("expected fresh deadline, got {existing:?}"),
    }
}

fn assert_fenced_without_result(
    old: validation::EvaluationState,
    next: validation::EvaluationState,
    deadline: Deadline,
) {
    assert_eq!(next.binding(), old.binding().next().unwrap());
    assert_eq!(next.state(), old.state());
    assert_eq!(next.target(), old.target());
    assert_eq!(next.receipt(), old.receipt());
    assert_eq!(next.generation(), old.generation());
    assert_eq!(next.phase(), old.phase());
    assert_eq!(next.has_begun(), old.has_begun());
    assert_eq!(next.sealed(), old.sealed());
    assert_eq!(next.last_result(), old.last_result());
    let fence = next.fence().unwrap();
    assert_eq!(fence.reason, validation::FenceReason::Deadline(deadline));
    assert_ne!(fence.cause, ContentHash([0; 32]));
}

#[test]
fn ready_deadline_publishes_only_a_real_authority_fence_and_preserves_claim() {
    let core = posted(&[(ValidationMode::Required, true)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let input = deadline_input(&owner, 1);
    let original = *owner.committed().evaluation(key(1)).unwrap();
    let claim = owner.committed().claim(key(1).claim).unwrap().binding();
    let before = parent.stats();
    assert!(
        owner
            .prepare_evaluation_deadline(input, input.deadline.at - 1)
            .is_err()
    );
    assert_eq!(parent.stats(), before);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), original);
    assert_eq!(owner.book.remaining_reports(key(1)), None);

    let (candidate, outcome) = stage_deadline(&mut owner, input, input.deadline.at);
    assert_eq!((outcome.artifacts, outcome.results), (0, 0));
    assert_eq!(*owner.committed().evaluation(key(1)).unwrap(), original);
    let changed = *owner.effective().evaluation(key(1)).unwrap();
    assert_fenced_without_result(original, changed, input.deadline);
    assert_eq!(changed.state(), validation::State::Ready);
    assert!(!changed.has_begun());
    assert!(changed.last_result().is_none());
    let events: Vec<_> = (0..outcome.events)
        .map(|ordinal| {
            owner
                .effective()
                .event(outcome.sequence, ordinal)
                .unwrap()
                .fact
        })
        .collect();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], NativeFact::Evaluation {
        kind: NativeEvaluationEventKind::AuthorityFenced,
        key: recorded, before: Some(before), after, state: validation::State::Ready,
        attempt: None, fence: Some(fence), ..
    } if recorded == key(1) && before == original.binding() && after == changed.binding()
        && fence.reason == validation::FenceReason::Deadline(input.deadline)));
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(*owner.committed().evaluation(key(1)).unwrap(), changed);
    assert_eq!(
        owner.committed().claim(key(1).claim).unwrap().binding(),
        claim
    );
    assert_eq!(
        owner.committed().claim(key(1).claim).unwrap().status(),
        focal_model::ClaimStatus::Posted
    );
    assert_eq!(owner.book.remaining_reports(key(1)), None);
}

#[test]
fn retry_and_quality_deadlines_spend_held_credit_with_ancestor_full_and_old_pages_pinned() {
    for preceding in [VerdictValue::Error, VerdictValue::Pass] {
        let core = posted(&[(ValidationMode::Required, true)]);
        let parent = core.state.budget.clone();
        let mut owner = NativeOwner::new(core).unwrap();
        let begun = start(&mut owner, 1, 3);
        owner.publish_after_durable(begun).unwrap();
        let original_pin = owner.pin(0, 5000).unwrap();
        let begun_state = *owner.committed().evaluation(key(1)).unwrap();
        let mut store = Store::new();
        let report = report_input(&owner, 1, 110, preceding);
        let reported = report_stage(&mut owner, &mut store, report);
        owner.publish_after_durable(reported).unwrap();
        let report_pin = owner.pin(0, 5000).unwrap();
        let original = *owner.committed().evaluation(key(1)).unwrap();
        assert_eq!(
            original.state(),
            if preceding == VerdictValue::Pass {
                validation::State::ValidatingQualityBar
            } else {
                validation::State::Validating
            }
        );
        let accepted = original.last_result().unwrap();
        let accepted_key = NativeResultKey::of(accepted);
        let retained = *owner.committed().result(accepted_key).unwrap();
        let evidence = accepted.evidence().unwrap();
        let descriptor = owner
            .committed()
            .artifact(evidence.id)
            .unwrap()
            .descriptor()
            .binding();
        let input = deadline_input(&owner, 1);
        let credit = owner.book.remaining_reports(key(1)).unwrap();
        assert!(credit > 0);
        let pressure = exhaust(&parent);
        let full = parent.stats();
        assert_eq!(full.used, full.limit);
        let (candidate, outcome) = stage_deadline(&mut owner, input, input.deadline.at);
        assert_eq!(parent.stats().used, full.used);
        assert_eq!(parent.stats().ordinary_used, full.ordinary_used);
        assert_eq!((outcome.artifacts, outcome.results), (0, 0));
        assert_eq!(owner.book.remaining_reports(key(1)), Some(0));
        let changed = *owner.effective().evaluation(key(1)).unwrap();
        assert_fenced_without_result(original, changed, input.deadline);
        assert_eq!(*owner.effective().result(accepted_key).unwrap(), retained);
        assert_eq!(
            owner
                .effective()
                .artifact(evidence.id)
                .unwrap()
                .descriptor()
                .binding(),
            descriptor
        );
        owner.publish_after_durable(candidate).unwrap();
        assert_eq!(owner.book.remaining_reports(key(1)), None);
        assert_eq!(
            original_pin
                .with_evaluation(key(1), 0, |state| *state)
                .unwrap(),
            Some(begun_state)
        );
        assert_eq!(
            report_pin
                .with_evaluation(key(1), 0, |state| *state)
                .unwrap(),
            Some(original)
        );
        assert_eq!(
            report_pin.with_result(accepted_key, 0, |row| *row).unwrap(),
            Some(retained)
        );
        owner.release(&original_pin).unwrap();
        owner.release(&report_pin).unwrap();
        drop(original_pin);
        drop(report_pin);
        drop(pressure);
        drop(owner);
        assert_eq!(parent.stats().used, 0);
    }
}

#[test]
fn pending_deadline_blocks_a_report_and_discard_restores_exact_attempt_credit_and_time() {
    let core = posted(&[(ValidationMode::Required, true)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    let report = report_input(&owner, 1, 210, VerdictValue::Error);
    let retry = copy_report(&report);
    let input = deadline_input(&owner, 1);
    let original = *owner.effective().evaluation(key(1)).unwrap();
    let credit = owner.book.remaining_reports(key(1));
    let pressure = exhaust(&parent);
    let before = parent.stats();
    let source = owner.book.source().stats();
    let (deadline, _) = stage_deadline(&mut owner, input, input.deadline.at);
    assert_eq!(owner.book.remaining_reports(key(1)), Some(0));
    let after_deadline = parent.stats();
    let after_source = owner.book.source().stats();
    let mut store = Store::new();
    assert!(
        owner
            .prepare_with_custody(
                context(report.request.principal, input.deadline.at),
                report,
                &mut store.content,
                DOMAIN,
                &BuiltinNativeSchemas,
            )
            .is_err()
    );
    assert_eq!(owner.pending_len(), 1);
    assert_eq!(parent.stats(), after_deadline);
    assert_eq!(owner.book.source().stats(), after_source);
    assert_eq!(owner.discard_from(deadline).unwrap(), 1);
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), original);
    assert_eq!(owner.book.remaining_reports(key(1)), credit);
    assert_eq!(owner.book.source().stats(), source);
    assert_eq!(parent.stats(), before);
    assert_eq!(owner.effective().logical_time(), 30);
    let reported = report_stage(&mut owner, &mut store, retry);
    assert!(
        owner
            .effective()
            .evaluation(key(1))
            .unwrap()
            .fence()
            .is_none()
    );
    owner.publish_after_durable(reported).unwrap();
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn deadline_after_pending_error_fences_the_new_revision_and_suffix_discard_restores_every_credit() {
    let core = posted(&[(ValidationMode::Required, true)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    let original = *owner.effective().evaluation(key(1)).unwrap();
    let input = deadline_input(&owner, 1);
    let report = report_input(&owner, 1, 310, VerdictValue::Error);
    let retry = copy_report(&report);
    let original_credit = owner.book.remaining_reports(key(1));
    let pressure = exhaust(&parent);
    let parent_before = parent.stats();
    let source_before = owner.book.source().stats();
    let mut store = Store::new();
    let reported = report_stage(&mut owner, &mut store, report);
    let reported_state = *owner.effective().evaluation(key(1)).unwrap();
    let result = reported_state.last_result().unwrap();
    let result_key = NativeResultKey::of(result);
    let accepted = *owner.effective().result(result_key).unwrap();
    let report_credit = owner.book.remaining_reports(key(1));
    let source_reported = owner.book.source().stats();
    let (deadline, _) = stage_deadline(&mut owner, input, input.deadline.at);
    assert_fenced_without_result(
        reported_state,
        *owner.effective().evaluation(key(1)).unwrap(),
        input.deadline,
    );
    assert_eq!(*owner.effective().result(result_key).unwrap(), accepted);
    assert_eq!(owner.discard_from(deadline).unwrap(), 1);
    assert_eq!(
        *owner.effective().evaluation(key(1)).unwrap(),
        reported_state
    );
    assert_eq!(owner.book.remaining_reports(key(1)), report_credit);
    assert_eq!(owner.book.source().stats(), source_reported);
    assert_eq!(owner.oldest(), Some(reported));
    let (deadline_again, _) = stage_deadline(&mut owner, input, input.deadline.at);
    assert_ne!(deadline_again, deadline);
    assert_eq!(owner.discard_from(reported).unwrap(), 2);
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), original);
    assert!(owner.effective().result(result_key).is_none());
    assert!(
        owner
            .effective()
            .artifact(result.evidence().unwrap().id)
            .is_none()
    );
    assert_eq!(owner.book.remaining_reports(key(1)), original_credit);
    assert_eq!(owner.book.source().stats(), source_before);
    assert_eq!(parent.stats(), parent_before);
    let replacement = report_stage(&mut owner, &mut store, retry);
    owner.publish_after_durable(replacement).unwrap();
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn deadline_consumes_terminal_evaluation_timer_without_rewriting_accepted_evidence() {
    let core = posted(&[(ValidationMode::Required, false)]);
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    let mut store = Store::new();
    let report = report_input(&owner, 1, 410, VerdictValue::Pass);
    let reported = report_stage(&mut owner, &mut store, report);
    // The deadline resolves the terminal evaluation from the pending prefix.
    let original = *owner.effective().evaluation(key(1)).unwrap();
    let result = original.last_result().unwrap();
    let result_key = NativeResultKey::of(result);
    let accepted = *owner.effective().result(result_key).unwrap();
    let input = deadline_input(&owner, 1);
    let (deadline, outcome) = stage_deadline(&mut owner, input, input.deadline.at);
    assert_eq!(
        (outcome.events, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), original);
    assert!(original.fence().is_none());
    assert_eq!(*owner.effective().result(result_key).unwrap(), accepted);
    owner.publish_after_durable(reported).unwrap();
    owner.publish_after_durable(deadline).unwrap();
    assert_eq!(*owner.committed().evaluation(key(1)).unwrap(), original);
    assert_eq!(*owner.committed().result(result_key).unwrap(), accepted);
    assert_eq!(owner.book.remaining_reports(key(1)), None);
}

#[test]
fn typed_timer_retry_precedes_backwards_clock_checks_and_changed_authored_deadline_refuses() {
    let mut core = posted(&[(ValidationMode::Required, false)]);
    core.limits.pending = 1;
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let input = deadline_input(&owner, 1);
    let (candidate, outcome) = stage_deadline(&mut owner, input, input.deadline.at);
    let pressure = exhaust(&parent);
    let before = parent.stats();
    assert!(
        matches!(owner.prepare_evaluation_deadline(input, 0).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: Some(ticket) }
            if actual == outcome && ticket == candidate)
    );
    assert_eq!(parent.stats(), before);
    owner.publish_after_durable(candidate).unwrap();
    assert!(
        matches!(owner.prepare_evaluation_deadline(input, 0).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    drop(pressure);
    let later = stage(&mut owner, creation(510, 2, &[], None), 2000);
    owner.publish_after_durable(later).unwrap();
    assert!(owner.effective().sequence() > outcome.sequence);
    let before = parent.stats();
    assert!(
        matches!(owner.prepare_evaluation_deadline(input, 1).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    assert_eq!(parent.stats(), before);
    let conflicting = NativeDeadlineInput {
        deadline: Deadline {
            at: input.deadline.at + 1,
            ..input.deadline
        },
        ..input
    };
    assert!(matches!(
        owner.prepare_evaluation_deadline(conflicting, 2001),
        Err(NativeOwnerError::Native(NativeError::RequestConflict))
    ));
    for changed in [
        Deadline {
            generation: input.deadline.generation + 1,
            ..input.deadline
        },
        Deadline {
            timer: TimerId::from_u128(9999),
            ..input.deadline
        },
    ] {
        assert!(
            owner
                .prepare_evaluation_deadline(
                    NativeDeadlineInput {
                        deadline: changed,
                        ..input
                    },
                    2001
                )
                .is_err()
        );
    }
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(parent.stats(), before);
}

fn work_deadline_owner() -> (NativeOwner, Store, MemoryBudget, EvaluationKey) {
    use focal_model::lifecycle::artifact_descriptor::{WorkProvenance, WorkRole};
    use focal_model::{Confidence, OutcomeKind, ValidationKind, ValidationPhase};
    let parent = MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let mut core = Core::new_native(
        binding(1).ledger,
        RangeId(6901),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 65_536,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 32,
            range: RangeConfig {
                page_entries: 4,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        parent.clone(),
    )
    .unwrap();
    let mut input = creation(1, 1, &[], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("create");
    };
    let handler = HandlerRef {
        id: ValidatorId::from_u128(77),
        version: ContentHash([77; 32]),
        agentic: false,
    };
    let handlers = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    declarations.push(
        validation::Declaration::new(
            Principal::Actor(ISSUER),
            validation::DeclarationSpec {
                binding: binding(301),
                claim: ClaimId::from_u128(1),
                issuer: ISSUER,
                declaration_index: 1,
                kind: ValidationKind::Test,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                target: validation::TargetDeclaration::WholeWorkSlot {
                    index: 0,
                    name: "output",
                },
                program: validation::Program::Programmatic {
                    check: validation::PhasePolicy {
                        evaluator: EVALUATOR,
                        definition: ContentHash([78; 32]),
                        handlers: &handlers,
                        required_policy: None,
                    },
                    quality: None,
                },
                deadline: Deadline {
                    timer: TimerId::from_u128(301),
                    generation: 1,
                    at: 1000,
                },
            },
            validation::Limits {
                handlers: 4,
                attempts: 8,
                slot_bytes: 64,
            },
        )
        .unwrap(),
    );
    let checks = [aggregation::CheckPolicy {
        declaration_index: 1,
        validation: ValidationId::from_u128(301),
        mode: ValidationMode::Required,
    }];
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &[aggregation::SlotPolicy {
            slot: 0,
            missing_declaration_index: 20,
            mode: ValidationMode::Required,
            checks: &checks,
        }],
        declarations,
        aggregation::Limits {
            max_slots: 2,
            max_checks: 8,
            max_results: 32,
            max_updates: 16,
        },
    )
    .unwrap();
    publish(&mut core, 10, input);
    publish(&mut core, 20, post(2, binding(1)));
    let mut owner = NativeOwner::new(core).unwrap();
    let claim_id = ClaimId::from_u128(1);
    let claim = owner.effective().claim(claim_id).unwrap().binding();
    let receipt = stage(
        &mut owner,
        NativeInput {
            request: request(SUBJECT, 3),
            command: NativeCommand::AcquireReceipt {
                expected: claim,
                receipt: ReceiptId::from_u128(701),
            },
        },
        30,
    );
    owner.publish_after_durable(receipt).unwrap();
    let entitlement =
        evidence::Parent::from_claim(owner.effective().claim(claim_id).unwrap()).unwrap();
    let mut spec = artifact_spec(800, SUBJECT, VerdictValue::Pass);
    spec.receipt = Some(entitlement.receipt);
    spec.visibility = &[];
    spec.work = Some(WorkProvenance {
        claim: claim_id,
        cycle: entitlement.next_cycle,
        role: WorkRole::Output { slot: 0 },
    });
    let artifact = descriptor(spec);
    let reference = ArtifactRef {
        id: artifact.id(),
        hash: artifact.content_hash(),
    };
    let mut store = Store::new();
    let input = NativeInput {
        request: request(SUBJECT, 4),
        command: NativeCommand::SubmitWork {
            claim: owner.effective().claim(claim_id).unwrap().binding(),
            slot: 0,
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    };
    let NativeStaging::Prepared { candidate, .. } = owner
        .prepare_with_custody(
            context(SUBJECT, 40),
            input,
            &mut store.content,
            DOMAIN,
            &BuiltinNativeSchemas,
        )
        .unwrap()
    else {
        panic!("work");
    };
    owner.publish_after_durable(candidate).unwrap();
    let input = NativeInput {
        request: request(SUBJECT, 5),
        command: NativeCommand::CloseResponse {
            claim: owner.effective().claim(claim_id).unwrap().binding(),
            response: binding(900),
            report: NativeResponseInput {
                summary: "Respondent-authored output for the deadline test.".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                manifest: vec![evidence::SlotBinding {
                    slot: 0,
                    artifact: reference,
                }],
                diagnostics: vec![],
            },
        },
    };
    let response = stage(&mut owner, input, 50);
    owner.publish_after_durable(response).unwrap();
    let response_id = TestamentId::from_u128(900);
    let input = NativeInput {
        request: request(SUBJECT, 6),
        command: NativeCommand::PostResponse {
            claim: owner.effective().claim(claim_id).unwrap().binding(),
            expected: owner
                .effective()
                .response(response_id)
                .unwrap()
                .identity()
                .binding,
        },
    };
    let posted = stage(&mut owner, input, 60);
    owner.publish_after_durable(posted).unwrap();
    let input = NativeInput {
        request: request(ISSUER, 7),
        command: NativeCommand::ReceiveResponse {
            claim: owner.effective().claim(claim_id).unwrap().binding(),
            expected: owner
                .effective()
                .response(response_id)
                .unwrap()
                .identity()
                .binding,
        },
    };
    let received = stage(&mut owner, input, 70);
    owner.publish_after_durable(received).unwrap();
    let evaluation = EvaluationKey {
        claim: claim_id,
        validation: ValidationId::from_u128(301),
        generation: 1,
        target: EvaluationTarget::Work {
            response: response_id,
            slot: 0,
            artifact: reference.id,
        },
    };
    let input = NativeInput {
        request: request(EVALUATOR, 8),
        command: NativeCommand::BeginWork {
            claim: owner.effective().claim(claim_id).unwrap().binding(),
            key: evaluation,
            expected: owner.effective().evaluation(evaluation).unwrap().binding(),
        },
    };
    let begun = stage(&mut owner, input, 80);
    owner.publish_after_durable(begun).unwrap();
    (owner, store, parent, evaluation)
}

fn incoming_to_work() -> NativeInput {
    let mut input = creation(610, 2, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("create");
    };
    claims[0].definition.graph = focal_model::lifecycle::graph::Declaration::new(
        &[focal_model::lifecycle::graph::Obligation {
            kind: focal_model::lifecycle::graph::Kind::DependsOn,
            target: ClaimId::from_u128(1),
        }],
        8,
    )
    .unwrap();
    input
}

#[test]
fn work_deadline_retirement_and_discard_restore_the_actual_reverse_protections() {
    let (mut owner, _store, parent, evaluation) = work_deadline_owner();
    let old = *owner.effective().evaluation(evaluation).unwrap();
    let claim = owner.effective().claim(evaluation.claim).unwrap().binding();
    let input = NativeDeadlineInput {
        evaluation,
        deadline: old
            .bind(owner.effective().definition(evaluation.validation).unwrap())
            .unwrap()
            .deadline(),
    };
    let credit = owner.book.remaining_reports(evaluation);
    assert!(credit.unwrap() > 0);
    assert!(
        owner
            .prepare(context(ISSUER, 90), incoming_to_work(), None)
            .is_err()
    );
    assert!(owner.effective().claim(ClaimId::from_u128(2)).is_none());
    let pinned = owner.pin(0, 5000).unwrap();
    let pressure = exhaust(&parent);
    let before = parent.stats();
    let source_before = owner.book.source().stats();
    let (deadline, _) = stage_deadline(&mut owner, input, input.deadline.at);
    assert_eq!(parent.stats().used, before.used);
    assert_eq!(owner.book.remaining_reports(evaluation), Some(0));
    assert_fenced_without_result(
        old,
        *owner.effective().evaluation(evaluation).unwrap(),
        input.deadline,
    );
    assert_eq!(owner.discard_from(deadline).unwrap(), 1);
    assert_eq!(owner.book.remaining_reports(evaluation), credit);
    assert_eq!(owner.book.source().stats(), source_before);
    assert_eq!(parent.stats(), before);
    drop(pressure);
    assert!(
        owner
            .prepare(context(ISSUER, 90), incoming_to_work(), None)
            .is_err()
    );

    // Retiring the real Work grant permits its graph closure to grow. Discard
    // rewinds both that graph change and the fence, restoring the original promise.
    let (deadline, _) = stage_deadline(&mut owner, input, input.deadline.at);
    let incoming = stage(&mut owner, incoming_to_work(), 1001);
    assert!(
        owner
            .candidate(incoming)
            .unwrap()
            .claim(ClaimId::from_u128(2))
            .is_some()
    );
    assert_eq!(owner.discard_from(deadline).unwrap(), 2);
    assert_eq!(owner.book.remaining_reports(evaluation), credit);
    assert_eq!(*owner.effective().evaluation(evaluation).unwrap(), old);
    assert_eq!(
        owner.effective().claim(evaluation.claim).unwrap().binding(),
        claim
    );
    assert!(owner.effective().claim(ClaimId::from_u128(2)).is_none());
    assert!(
        owner
            .prepare(context(ISSUER, 90), incoming_to_work(), None)
            .is_err()
    );
    assert_eq!(
        pinned
            .with_evaluation(evaluation, 0, |state| *state)
            .unwrap(),
        Some(old)
    );
    let (deadline, _) = stage_deadline(&mut owner, input, input.deadline.at);
    owner.publish_after_durable(deadline).unwrap();
    assert_eq!(owner.book.remaining_reports(evaluation), None);
    let incoming = stage(&mut owner, incoming_to_work(), 1001);
    owner.publish_after_durable(incoming).unwrap();
    owner.release(&pinned).unwrap();
    drop(pinned);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}
