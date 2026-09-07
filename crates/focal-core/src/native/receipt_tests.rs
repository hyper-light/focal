use super::report_tests::{
    Custody, EVALUATOR, ISSUER, SUBJECT, artifact_spec, begin, binding, context, core, creation,
    descriptor, events, key, post, prepared, publish, report, report_for, request, running,
    verified,
};
use super::*;
use focal_model::lifecycle::{creation::Owner, graph, succession::Lineage};
use focal_model::{
    Cause, Deadline, ObjectRevision, ReceiptFence, ReceiptId, TimerId, ValidationMode, VerdictValue,
};

fn acquire(id: u128, expected: Binding, receipt: u128) -> NativeInput {
    NativeInput {
        request: request(SUBJECT, id),
        command: NativeCommand::AcquireReceipt {
            expected,
            receipt: ReceiptId::from_u128(receipt),
        },
    }
}
fn copy_acquisition(input: &NativeInput) -> NativeInput {
    let NativeCommand::AcquireReceipt { expected, receipt } = &input.command else {
        panic!("expected receipt request")
    };
    NativeInput {
        request: input.request,
        command: NativeCommand::AcquireReceipt {
            expected: *expected,
            receipt: *receipt,
        },
    }
}
fn cancel(id: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Cancel { expected },
    }
}
fn joined(id: u128, inputs: Vec<NativeInput>) -> NativeInput {
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for input in inputs {
        let NativeCommand::Create {
            claims: mut next,
            declarations: mut definitions,
        } = input.command
        else {
            panic!("expected creation")
        };
        claims.append(&mut next);
        declarations.append(&mut definitions);
    }
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Create {
            claims,
            declarations,
        },
    }
}
fn claim_id(id: u128) -> ClaimId {
    ClaimId::from_u128(id)
}
fn current(core: &Core<NativeState>, id: u128) -> Binding {
    core.native_claim(claim_id(id)).unwrap().binding()
}

fn check_received(core: &Core<NativeState>, outcome: NativeOutcome, original: Binding, id: u128) {
    let claim = core.native_claim(ClaimId(original.object.0)).unwrap();
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(id),
        epoch: 1,
    };
    assert_eq!(claim.status(), ClaimStatus::Received);
    assert_eq!(claim.binding(), original.next().unwrap());
    assert_eq!(claim.receipt().unwrap().holder, SUBJECT);
    assert_eq!(claim.receipt().unwrap().fence, fence);
    assert_eq!(
        core.native_receipt(fence.receipt),
        Some(NativeReceipt {
            claim: ClaimId(original.object.0),
            fence,
            holder: SUBJECT,
            acquired: outcome.sequence,
        })
    );
    assert_eq!(outcome.receipts, 1);
    assert_eq!(claim.response_count(), 0);
    assert_eq!(claim.terminal_cut(), None);
    assert_eq!(claim.local_sealed_at(), None);
    assert!(!claim.local_complete());
    assert_eq!(
        (
            outcome.created,
            outcome.changed,
            outcome.definitions,
            outcome.evaluations,
            outcome.artifacts,
            outcome.results,
            outcome.events
        ),
        (0, 1, 0, 0, 0, 0, 2)
    );
    assert_eq!(
        events(core, outcome),
        vec![
            NativeFact::Claim(NativeClaimEvent {
                kind: NativeEventKind::Received,
                owned_child: None,
                before: Some(original),
                after: claim.binding(),
                status: ClaimStatus::Received
            }),
            NativeFact::Receipt {
                claim: claim.binding(),
                fence,
                holder: SUBJECT
            },
        ]
    );
}

#[test]
fn pending_empty_admission_create_post_receive_publishes_entitlement_without_testimony() {
    let mut core = core();
    let create = prepared(core.prepare_native(context(ISSUER, 10), creation(1, 1, &[], None), &[]));
    let post = prepared(core.prepare_native(context(ISSUER, 20), post(2, binding(1)), &[&create]));
    let expected = post.claim(claim_id(1)).unwrap().binding();
    let received = prepared(core.prepare_native(
        context(SUBJECT, 30),
        acquire(3, expected, 601),
        &[&create, &post],
    ));
    let next = received.claim(claim_id(1)).unwrap();
    assert_eq!(next.status(), ClaimStatus::Received);
    assert_eq!(next.receipt().unwrap().fence.epoch, 1);
    assert_eq!(next.response_count(), 0);
    assert_eq!(
        received
            .receipt(ReceiptId::from_u128(601))
            .unwrap()
            .acquired,
        received.outcome().sequence
    );
    assert!(received.evaluation(key(1)).is_none());
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim(claim_id(1)).is_none());
    let pinned = core.pin_native(0, 100).unwrap();
    let rejected = core.publish_native(received).unwrap_err();
    assert_eq!(core.native_sequence(), SessionSeq(0));
    core.publish_native(create).unwrap();
    core.publish_native(post).unwrap();
    let outcome = core.publish_native(rejected.prepared).unwrap();
    check_received(&core, outcome, expected, 601);
    assert_eq!(
        pinned
            .with_claim(claim_id(1), 1, |row| row.receipt())
            .unwrap(),
        None
    );
    assert_eq!(pinned.receipt(ReceiptId::from_u128(601), 1).unwrap(), None);
    core.release_native(&pinned).unwrap();
}

#[test]
fn receipt_checks_actual_subject_actor_current_revision_valid_id_and_posted_state() {
    let mut core = running(&[]);
    let expected = current(&core, 1);
    let before = core.native_budget();
    let prefix = core.native_sequence();
    for case in 0..5 {
        let mut input = acquire(11, expected, 611);
        let mut owner = context(SUBJECT, 30);
        let NativeCommand::AcquireReceipt {
            expected: altered,
            receipt,
        } = &mut input.command
        else {
            panic!()
        };
        match case {
            0 => {
                input.request.principal = ISSUER;
                owner.principal = Principal::Actor(ISSUER);
            }
            1 => owner.principal = Principal::Node(SUBJECT),
            2 => altered.revision = ObjectRevision(1),
            3 => *receipt = ReceiptId::from_u128(0),
            _ => altered.content = ContentHash([99; 32]),
        }
        assert!(
            core.prepare_native(owner, input, &[]).is_err(),
            "case {case}"
        );
        assert_eq!(core.native_budget(), before);
        assert_eq!(core.native_sequence(), prefix);
        assert_eq!(current(&core, 1), expected);
        assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
    }
    let outcome = publish(&mut core, 30, acquire(11, expected, 611));
    check_received(&core, outcome, expected, 611);
    let received = current(&core, 1);
    assert!(
        core.prepare_native(context(SUBJECT, 40), acquire(12, received, 612), &[])
            .is_err()
    );
    assert_eq!(
        core.native_claim(claim_id(1))
            .unwrap()
            .receipt()
            .unwrap()
            .fence
            .receipt,
        ReceiptId::from_u128(611)
    );
    for cancelled in [false, true] {
        let mut core = super::report_tests::core();
        publish(&mut core, 10, creation(1, 1, &[], None));
        if cancelled {
            publish(&mut core, 20, cancel(2, binding(1)));
        }
        let original = current(&core, 1);
        assert!(
            core.prepare_native(context(SUBJECT, 30), acquire(13, original, 613), &[])
                .is_err()
        );
        assert_eq!(current(&core, 1), original);
        assert!(core.native_outcome(request(SUBJECT, 13)).is_none());
    }
}

#[test]
fn receipt_ids_are_ledger_unique_across_pending_claims_and_survive_cancellation() {
    let mut core = core();
    publish(
        &mut core,
        10,
        joined(
            1,
            vec![creation(1, 1, &[], None), creation(2, 2, &[], None)],
        ),
    );
    publish(&mut core, 20, post(2, binding(1)));
    publish(&mut core, 20, post(3, binding(2)));
    let first_expected = current(&core, 1);
    let second_expected = current(&core, 2);
    let first =
        prepared(core.prepare_native(context(SUBJECT, 30), acquire(21, first_expected, 621), &[]));
    let before = core.native_budget();
    assert!(
        core.prepare_native(
            context(SUBJECT, 30),
            acquire(22, second_expected, 621),
            &[&first]
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(first.claim(claim_id(2)).unwrap().receipt(), None);
    let first_outcome = core.publish_native(first).unwrap();
    check_received(&core, first_outcome, first_expected, 621);
    let current_first = current(&core, 1);
    publish(&mut core, 40, cancel(23, current_first));
    assert!(
        core.prepare_native(context(SUBJECT, 50), acquire(22, second_expected, 621), &[])
            .is_err()
    );
    let outcome = publish(&mut core, 50, acquire(22, second_expected, 622));
    check_received(&core, outcome, second_expected, 622);
    assert_eq!(
        core.native_claim(claim_id(1))
            .unwrap()
            .receipt()
            .unwrap()
            .fence
            .receipt,
        ReceiptId::from_u128(621)
    );
}

#[test]
fn required_admission_results_are_resolved_from_pending_rows_before_receipt() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let expected = current(&core, 1);
    let before = core.native_budget();
    assert!(
        core.prepare_native(context(SUBJECT, 90), acquire(31, expected, 631), &[])
            .is_err()
    );
    assert_eq!(core.native_budget(), before);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        32,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(632, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let passed = report(&core, input, &[], &token);
    let result = passed.evaluation(key(1)).unwrap().last_result().unwrap();
    let receipt = prepared(core.prepare_native(
        context(SUBJECT, 110),
        acquire(31, expected, 631),
        &[&passed],
    ));
    assert_eq!(
        receipt.evaluation(key(1)).unwrap().last_result(),
        Some(result)
    );
    assert_eq!(
        receipt
            .result(NativeResultKey::of(result))
            .unwrap()
            .result(),
        result
    );
    assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
    core.publish_native(passed).unwrap();
    let outcome = core.publish_native(receipt).unwrap();
    check_received(&core, outcome, expected, 631);
}

#[test]
fn required_failure_is_not_bypassed_by_fresh_receipt_or_observe_pass() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        41,
        1,
        VerdictValue::Fail,
        descriptor(artifact_spec(641, EVALUATOR, VerdictValue::Fail)),
    );
    let token = verified(&mut custody, &input);
    let failed = report(&core, input, &[], &token);
    let expected = failed.claim(claim_id(1)).unwrap().binding();
    let cut = failed.claim(claim_id(1)).unwrap().terminal_cut();
    assert!(
        core.prepare_native(
            context(SUBJECT, 110),
            acquire(42, expected, 642),
            &[&failed]
        )
        .is_err()
    );
    let input = report_for(
        &core,
        Some(&failed),
        43,
        2,
        VerdictValue::Pass,
        descriptor(artifact_spec(643, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let observe = report(&core, input, &[&failed], &token);
    assert!(
        core.prepare_native(
            context(SUBJECT, 110),
            acquire(42, expected, 642),
            &[&failed, &observe]
        )
        .is_err()
    );
    core.publish_native(failed).unwrap();
    core.publish_native(observe).unwrap();
    assert_eq!(core.native_claim(claim_id(1)).unwrap().terminal_cut(), cut);
    assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
    assert_eq!(core.native_claim(claim_id(1)).unwrap().response_count(), 0);
}

#[test]
fn begun_observe_report_after_receipt_keeps_entitlement_and_never_authors_testimony() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        51,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(651, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let passed = report(&core, input, &[], &token);
    let expected = passed.claim(claim_id(1)).unwrap().binding();
    let receipt = prepared(core.prepare_native(
        context(SUBJECT, 110),
        acquire(52, expected, 652),
        &[&passed],
    ));
    let acquired = receipt.claim(claim_id(1)).unwrap().receipt().unwrap();
    let before = *receipt.evaluation(key(2)).unwrap();
    assert_eq!(before.receipt(), None);
    assert_eq!(before.fence(), None);
    let input = report_for(
        &core,
        Some(&receipt),
        53,
        2,
        VerdictValue::Fail,
        descriptor(artifact_spec(653, EVALUATOR, VerdictValue::Fail)),
    );
    let token = verified(&mut custody, &input);
    let late = prepared(core.prepare_native_evidenced(
        context(EVALUATOR, 120),
        input,
        &[&passed, &receipt],
        Some(&token),
    ));
    let result = late.evaluation(key(2)).unwrap().last_result().unwrap();
    assert_eq!(
        late.evaluation(key(2)).unwrap().state(),
        validation::State::ValidationFailedNotRequired
    );
    assert_eq!(late.claim(claim_id(1)).unwrap().receipt(), Some(acquired));
    assert_eq!(
        late.claim(claim_id(1)).unwrap().status(),
        ClaimStatus::Received
    );
    assert_eq!(late.claim(claim_id(1)).unwrap().response_count(), 0);
    core.publish_native(passed).unwrap();
    let receipt_outcome = core.publish_native(receipt).unwrap();
    check_received(&core, receipt_outcome, expected, 652);
    let late_outcome = core.publish_native(late).unwrap();
    assert_eq!(
        core.native_result(NativeResultKey::of(result))
            .unwrap()
            .sequence(),
        late_outcome.sequence
    );
    assert!(
        !events(&core, late_outcome)
            .iter()
            .any(|fact| matches!(fact, NativeFact::Claim(_) | NativeFact::Receipt { .. }))
    );
    assert_eq!(
        core.native_claim(claim_id(1)).unwrap().receipt(),
        Some(acquired)
    );
}

#[test]
fn receipt_does_not_start_ready_observe_checks_and_later_begin_is_refused() {
    let mut core = core();
    publish(
        &mut core,
        10,
        creation(1, 1, &[(ValidationMode::Observe, false)], None),
    );
    publish(&mut core, 20, post(2, binding(1)));
    let expected = current(&core, 1);
    let old = *core.native_evaluation(key(1)).unwrap();
    let outcome = publish(&mut core, 30, acquire(61, expected, 661));
    check_received(&core, outcome, expected, 661);
    assert_eq!(core.native_evaluation(key(1)), Some(&old));
    assert_eq!(old.state(), validation::State::Ready);
    assert!(!old.has_begun());
    let received = current(&core, 1);
    assert!(
        core.prepare_native(
            context(EVALUATOR, 40),
            begin(62, received, 1, old.binding()),
            &[]
        )
        .is_err()
    );
    assert_eq!(core.native_evaluation(key(1)), Some(&old));
}

#[test]
fn exact_receipt_retry_returns_original_pending_or_committed_outcome_after_control() {
    let mut core = running(&[]);
    let input = acquire(71, current(&core, 1), 671);
    let pending_retry = copy_acquisition(&input);
    let committed_retry = copy_acquisition(&input);
    let mut conflict = copy_acquisition(&input);
    let NativeCommand::AcquireReceipt { receipt, .. } = &mut conflict.command else {
        panic!()
    };
    *receipt = ReceiptId::from_u128(672);
    let receipt = prepared(core.prepare_native(context(SUBJECT, 30), input, &[]));
    let expected = receipt.outcome();
    assert!(
        matches!(core.prepare_native(context(SUBJECT, 1), pending_retry, &[&receipt]).unwrap(), NativePreparation::Existing { outcome, committed: false } if outcome == expected)
    );
    assert!(matches!(
        core.prepare_native(context(SUBJECT, 40), conflict, &[&receipt]),
        Err(NativeError::RequestConflict)
    ));
    core.publish_native(receipt).unwrap();
    let received = current(&core, 1);
    publish(&mut core, 40, cancel(72, received));
    let before = core.native_budget();
    assert!(
        matches!(core.prepare_native(context(SUBJECT, 9999), committed_retry, &[]).unwrap(), NativePreparation::Existing { outcome, committed: true } if outcome == expected)
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(
        core.native_claim(claim_id(1)).unwrap().status(),
        ClaimStatus::Cancelled
    );
    assert_eq!(core.native_outcome(request(SUBJECT, 71)), Some(expected));
}

#[test]
fn dependency_and_await_receipt_gates_use_actual_pending_target_settlement() {
    for kind in [graph::Kind::Awaits, graph::Kind::DependsOn] {
        let mut core = core();
        let mut input = joined(
            1,
            vec![creation(1, 1, &[], None), creation(2, 2, &[], None)],
        );
        let NativeCommand::Create { claims, .. } = &mut input.command else {
            panic!()
        };
        claims[0].definition.graph = graph::Declaration::new(
            &[graph::Obligation {
                kind,
                target: claim_id(2),
            }],
            1,
        )
        .unwrap();
        publish(&mut core, 10, input);
        publish(&mut core, 20, post(2, binding(1)));
        let expected = current(&core, 1);
        assert!(
            core.prepare_native(context(SUBJECT, 30), acquire(81, expected, 681), &[])
                .is_err()
        );
        let target =
            prepared(core.prepare_native(context(ISSUER, 30), cancel(82, binding(2)), &[]));
        let before = core.native_budget();
        let candidate =
            core.prepare_native(context(SUBJECT, 40), acquire(81, expected, 681), &[&target]);
        if kind == graph::Kind::Awaits {
            let receipt = prepared(candidate);
            assert_eq!(
                receipt.claim(claim_id(2)).unwrap().status(),
                ClaimStatus::Cancelled
            );
            core.publish_native(target).unwrap();
            let outcome = core.publish_native(receipt).unwrap();
            check_received(&core, outcome, expected, 681);
        } else {
            assert!(candidate.is_err());
            assert_eq!(core.native_budget(), before);
            assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
            core.publish_native(target).unwrap();
            assert!(
                core.prepare_native(context(SUBJECT, 40), acquire(81, expected, 681), &[])
                    .is_err()
            );
        }
    }
}

#[test]
fn receipt_graph_gathers_complete_owned_descendants_without_caller_selected_peers() {
    let mut core = core();
    let mut input = joined(
        1,
        vec![
            creation(1, 1, &[], None),
            creation(2, 2, &[], None),
            creation(3, 3, &[], None),
        ],
    );
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!()
    };
    for (index, parent) in [(1, 1), (2, 2)] {
        let definition = &mut claims[index].definition;
        definition.lineage =
            Lineage::new(definition.binding, Cause::Claim(claim_id(parent)), &[], 0).unwrap();
        claims[index].owner = Some(Owner {
            expected: binding(parent),
            receipt: None,
        });
    }
    let created = prepared(core.prepare_native(context(ISSUER, 10), input, &[]));
    let generated = created.claim(claim_id(1)).unwrap().binding();
    let posted =
        prepared(core.prepare_native(context(ISSUER, 20), post(2, generated), &[&created]));
    let expected = posted.claim(claim_id(1)).unwrap().binding();
    let received = prepared(core.prepare_native(
        context(SUBJECT, 30),
        acquire(91, expected, 691),
        &[&created, &posted],
    ));
    for id in [2, 3] {
        assert_eq!(
            received.claim(claim_id(id)).unwrap().binding(),
            created.claim(claim_id(id)).unwrap().binding()
        );
        assert_eq!(
            received.claim(claim_id(id)).unwrap().status(),
            ClaimStatus::Generated
        );
        assert_eq!(received.claim(claim_id(id)).unwrap().receipt(), None);
    }
    core.publish_native(created).unwrap();
    core.publish_native(posted).unwrap();
    let outcome = core.publish_native(received).unwrap();
    check_received(&core, outcome, expected, 691);
}

#[test]
fn receipt_candidate_copy_failure_rolls_back_entitlement_history_and_unique_id_reservation() {
    let mut core = running(&[]);
    let expected = current(&core, 1);
    let before = core.native_budget();
    let prefix = core.native_sequence();
    let failed = super::prepare::fail_copies_after(0, || {
        core.prepare_native(context(SUBJECT, 30), acquire(101, expected, 701), &[])
    });
    assert!(
        matches!(
            failed,
            Err(NativeError::Memory(MemoryError::AllocationFailed))
        ),
        "{failed:?}"
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), prefix);
    assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
    assert!(core.native_outcome(request(SUBJECT, 101)).is_none());
    assert!(core.native_event(SessionSeq(prefix.0 + 1), 0).is_none());
    let outcome = publish(&mut core, 30, acquire(101, expected, 701));
    check_received(&core, outcome, expected, 701);
}

#[test]
fn receipt_capacity_refusal_preserves_target_and_does_not_allocate_a_receipt_identity() {
    let original = core();
    let mut limits = original.limits;
    limits.receipts = 1;
    let mut core = Core::new_native(
        binding(1).ledger,
        RangeId(82),
        limits,
        MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    publish(
        &mut core,
        10,
        joined(
            1,
            vec![creation(1, 1, &[], None), creation(2, 2, &[], None)],
        ),
    );
    publish(&mut core, 20, post(2, binding(1)));
    publish(&mut core, 20, post(3, binding(2)));
    let first = current(&core, 1);
    let second = current(&core, 2);
    publish(&mut core, 30, acquire(111, first, 711));
    let before = core.native_budget();
    let sequence = core.native_sequence();
    assert!(matches!(
        core.prepare_native(context(SUBJECT, 999), acquire(112, second, 712), &[]),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), sequence);
    assert_eq!(current(&core, 2), second);
    assert_eq!(core.native_claim(claim_id(2)).unwrap().receipt(), None);
    assert_eq!(core.native_receipt(ReceiptId::from_u128(712)), None);
    assert!(core.native_outcome(request(SUBJECT, 112)).is_none());
    // Failure cannot advance the trusted clock past a subsequent valid control.
    publish(&mut core, 31, cancel(113, second));
    assert_eq!(
        core.native_claim(claim_id(2)).unwrap().status(),
        ClaimStatus::Cancelled
    );
}

#[test]
fn unrelated_claim_count_does_not_consume_the_receipt_graph_closure_limit() {
    let mut core = core();
    let count = core.limits.plan_nodes + 2;
    for index in 1..=count {
        let id = u128::try_from(index).unwrap();
        publish(&mut core, 10, creation(200 + id, id, &[], None));
    }
    publish(&mut core, 20, post(300, binding(1)));
    let expected = current(&core, 1);
    let outcome = publish(&mut core, 30, acquire(301, expected, 721));
    check_received(&core, outcome, expected, 721);
    for index in 2..=count {
        let row = core
            .native_claim(claim_id(u128::try_from(index).unwrap()))
            .unwrap();
        assert_eq!(row.status(), ClaimStatus::Generated);
        assert_eq!(row.receipt(), None);
    }
}

#[test]
fn required_programmatic_pass_waits_for_authorized_quality_pass_before_receipt() {
    let mut core = running(&[(ValidationMode::Required, true)]);
    let mut custody = Custody::new();
    let expected = current(&core, 1);
    let input = report_for(
        &core,
        None,
        401,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(801, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let program = report(&core, input, &[], &token);
    let state = program.evaluation(key(1)).unwrap();
    assert_eq!(state.state(), validation::State::ValidatingQualityBar);
    let program_result = state.last_result().unwrap();
    assert_eq!(program_result.verdict(), VerdictValue::Pass);
    assert!(!program_result.is_terminal());
    let quality_attempt = state
        .bind(program.definition(key(1).validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    assert_eq!(quality_attempt.phase, validation::Phase::Quality);
    assert_ne!(quality_attempt.evaluator, EVALUATOR);
    let before = core.native_budget();
    assert!(
        core.prepare_native(
            context(SUBJECT, 110),
            acquire(402, expected, 802),
            &[&program]
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(program.claim(claim_id(1)).unwrap().receipt(), None);
    assert!(program.recorded(request(SUBJECT, 402)).is_none());
    let input = report_for(
        &core,
        Some(&program),
        403,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(
            803,
            quality_attempt.evaluator,
            VerdictValue::Pass,
        )),
    );
    let token = verified(&mut custody, &input);
    let quality = prepared(core.prepare_native_evidenced(
        context(quality_attempt.evaluator, 110),
        input,
        &[&program],
        Some(&token),
    ));
    let quality_result = quality.evaluation(key(1)).unwrap().last_result().unwrap();
    assert_eq!(quality_result.phase(), validation::Phase::Quality);
    assert_eq!(quality_result.reporter(), Some(quality_attempt.evaluator));
    assert!(quality_result.is_terminal());
    let receipt = prepared(core.prepare_native(
        context(SUBJECT, 120),
        acquire(402, expected, 802),
        &[&program, &quality],
    ));
    assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
    let program_outcome = core.publish_native(program).unwrap();
    let quality_outcome = core.publish_native(quality).unwrap();
    let receipt_outcome = core.publish_native(receipt).unwrap();
    check_received(&core, receipt_outcome, expected, 802);
    for (result, outcome) in [
        (program_result, program_outcome),
        (quality_result, quality_outcome),
    ] {
        let retained = core.native_result(NativeResultKey::of(result)).unwrap();
        assert_eq!(retained.result(), result);
        assert_eq!(retained.sequence(), outcome.sequence);
        assert_eq!(retained.ordinal(), 2);
    }
}

#[test]
fn authored_deadline_blocks_new_receipt_without_expiring_claim_and_preserves_exact_retries() {
    let mut core = core();
    let mut input = joined(
        1,
        vec![creation(1, 1, &[], None), creation(2, 2, &[], None)],
    );
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!()
    };
    for claim in claims {
        claim.definition.deadline = Some(Deadline {
            timer: TimerId(claim.definition.binding.object.0),
            generation: 1,
            at: 50,
        });
    }
    publish(&mut core, 10, input);
    publish(&mut core, 20, post(2, binding(1)));
    publish(&mut core, 20, post(3, binding(2)));
    let first = current(&core, 1);
    let second = current(&core, 2);
    let input = acquire(411, first, 811);
    let pending_retry = copy_acquisition(&input);
    let committed_retry = copy_acquisition(&input);
    let received = prepared(core.prepare_native(context(SUBJECT, 49), input, &[]));
    let original = received.outcome();
    let before = core.native_budget();
    let prefix = core.native_sequence();
    assert!(
        core.prepare_native(
            context(SUBJECT, 50),
            acquire(412, second, 812),
            &[&received]
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), prefix);
    let untouched = received.claim(claim_id(2)).unwrap();
    assert_eq!(untouched.binding(), second);
    assert_eq!(untouched.status(), ClaimStatus::Posted);
    assert_eq!(untouched.receipt(), None);
    assert_eq!(untouched.terminal_cut(), None);
    assert_eq!(untouched.local_sealed_at(), None);
    assert!(received.receipt(ReceiptId::from_u128(812)).is_none());
    assert!(received.recorded(request(SUBJECT, 412)).is_none());
    assert!(
        matches!(core.prepare_native(context(SUBJECT, 50), pending_retry, &[&received]).unwrap(),
        NativePreparation::Existing { outcome, committed: false } if outcome == original)
    );
    let outcome = core.publish_native(received).unwrap();
    check_received(&core, outcome, first, 811);
    let before = core.native_budget();
    assert!(
        core.prepare_native(context(SUBJECT, 5000), acquire(412, second, 812), &[])
            .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert!(
        matches!(core.prepare_native(context(SUBJECT, 5000), committed_retry, &[]).unwrap(),
        NativePreparation::Existing { outcome, committed: true } if outcome == original)
    );
    assert_eq!(core.native_sequence(), original.sequence);
    assert_eq!(
        core.native_claim(claim_id(2)).unwrap().status(),
        ClaimStatus::Posted
    );
    assert!(
        core.native_event(SessionSeq(original.sequence.0 + 1), 0)
            .is_none()
    );
    assert!(core.native_receipt(ReceiptId::from_u128(812)).is_none());
}

#[test]
fn begun_observe_error_retry_and_quality_chain_after_receipt_retains_every_attempt() {
    let mut core = running(&[(ValidationMode::Observe, true)]);
    let mut custody = Custody::new();
    let expected = current(&core, 1);
    let received =
        prepared(core.prepare_native(context(SUBJECT, 40), acquire(421, expected, 821), &[]));
    let original = received.claim(claim_id(1)).unwrap().receipt().unwrap();
    let receipt_binding = received.claim(claim_id(1)).unwrap().binding();
    let receipt_outcome = received.outcome();
    let mut candidates = vec![received];
    let mut accepted = Vec::new();
    for (index, value, phase, resulting) in [
        (
            0,
            VerdictValue::Error,
            validation::Phase::Programmatic,
            validation::State::Validating,
        ),
        (
            1,
            VerdictValue::Pass,
            validation::Phase::Programmatic,
            validation::State::ValidatingQualityBar,
        ),
        (
            2,
            VerdictValue::Pass,
            validation::Phase::Quality,
            validation::State::Validated,
        ),
    ] {
        let tail = candidates.last().unwrap();
        let attempt = tail
            .evaluation(key(1))
            .unwrap()
            .bind(tail.definition(key(1).validation).unwrap())
            .unwrap()
            .current_attempt()
            .unwrap();
        assert_eq!((attempt.index, attempt.phase), (index, phase));
        let input = report_for(
            &core,
            Some(tail),
            422 + u128::from(index),
            1,
            value,
            descriptor(artifact_spec(
                822 + u128::from(index),
                attempt.evaluator,
                value,
            )),
        );
        let token = verified(&mut custody, &input);
        let pending = candidates.iter().collect::<Vec<_>>();
        let next = prepared(core.prepare_native_evidenced(
            context(attempt.evaluator, 50 + u64::from(index) * 10),
            input,
            &pending,
            Some(&token),
        ));
        let state = next.evaluation(key(1)).unwrap();
        let result = state.last_result().unwrap();
        assert_eq!(state.state(), resulting);
        assert_eq!(state.receipt(), None);
        assert_eq!(state.fence(), None);
        assert_eq!(
            (result.attempt(), result.phase(), result.reporter()),
            (Some(index), phase, Some(attempt.evaluator))
        );
        assert_eq!(result.is_terminal(), index == 2);
        assert_eq!(next.claim(claim_id(1)).unwrap().binding(), receipt_binding);
        assert_eq!(next.claim(claim_id(1)).unwrap().receipt(), Some(original));
        assert_eq!(
            next.claim(claim_id(1)).unwrap().status(),
            ClaimStatus::Received
        );
        assert_eq!(next.claim(claim_id(1)).unwrap().response_count(), 0);
        assert_eq!(
            next.receipt(original.fence.receipt).unwrap().acquired,
            receipt_outcome.sequence
        );
        accepted.push((result, attempt, next.outcome()));
        candidates.push(next);
    }
    assert_eq!(core.native_claim(claim_id(1)).unwrap().receipt(), None);
    for candidate in candidates {
        core.publish_native(candidate).unwrap();
    }
    check_received(&core, receipt_outcome, expected, 821);
    for (result, attempt, outcome) in accepted {
        let stored = core.native_result(NativeResultKey::of(result)).unwrap();
        assert_eq!(stored.result(), result);
        assert_eq!(stored.attempt(), attempt);
        assert_eq!((stored.sequence(), stored.ordinal()), (outcome.sequence, 2));
        assert_eq!(stored.artifact().result(), result);
        let artifact = core
            .native_artifact(stored.artifact().reference().id)
            .unwrap();
        assert_eq!(artifact.facts().unwrap().attempt, attempt);
        assert_eq!(artifact.facts().unwrap().value, result.verdict());
        assert_eq!(artifact.descriptor().receipt(), None);
        assert!(
            !events(&core, outcome)
                .iter()
                .any(|fact| matches!(fact, NativeFact::Claim(_) | NativeFact::Receipt { .. }))
        );
    }
    assert_eq!(
        core.native_claim(claim_id(1)).unwrap().receipt(),
        Some(original)
    );
    assert_eq!(
        core.native_receipt(original.fence.receipt)
            .unwrap()
            .acquired,
        receipt_outcome.sequence
    );
}
