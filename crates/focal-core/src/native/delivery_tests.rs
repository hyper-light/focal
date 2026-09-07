use super::*;
use focal_model::{ObjectRevision, VerdictValue};

fn delivery_key(response: u128, cycle: u64) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(1),
        validation: ValidationId::from_u128(100),
        target: EvaluationTarget::Delivery {
            response: TestamentId::from_u128(response),
        },
        generation: cycle,
    }
}
fn posted(f: &mut Fixture, id: u128) {
    let diagnostic = f.diagnostic(id + 100);
    f.commit(
        SUBJECT,
        f.close(id, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    );
}

#[test]
fn receipt_records_delivery_of_failed_work_without_fabricating_quality_or_artifacts() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    let read = f.owner.pin(0, 100).unwrap();
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    assert_eq!(
        (
            outcome.evaluations,
            outcome.results,
            outcome.artifacts,
            outcome.events
        ),
        (1, 1, 0, 4)
    );
    let key = delivery_key(900, 1);
    let view = f.owner.committed();
    let state = view.evaluation(key).unwrap();
    assert_eq!(state.state(), validation::State::Validated);
    assert_eq!(state.binding().revision, ObjectRevision(2));
    assert!(!state.has_begun());
    let result_key = NativeResultKey {
        evaluation: key,
        revision: state.binding().revision,
    };
    let result = view.delivery_result(result_key).unwrap();
    assert_eq!(result.sequence(), outcome.sequence);
    assert_eq!(result.ordinal(), 2);
    assert_eq!(
        view.event(result.sequence(), result.ordinal())
            .unwrap()
            .fact,
        NativeFact::Delivery { key: result_key },
    );
    assert_eq!(result.result().verdict(), VerdictValue::Pass);
    assert_eq!(result.result().phase(), validation::Phase::Delivery);
    assert_eq!(result.result().attempt(), None);
    assert_eq!(result.result().reporter(), None);
    assert_eq!(result.result().evidence(), None);
    assert!(view.result(result_key).is_none());
    assert_eq!(
        view.response(TestamentId::from_u128(900))
            .unwrap()
            .reported_outcome(),
        OutcomeKind::Failed
    );
    assert!(!view.claim(ClaimId::from_u128(1)).unwrap().local_complete());
    let facts: Vec<_> = (0..outcome.events)
        .map(|ordinal| view.event(outcome.sequence, ordinal).unwrap().fact)
        .collect();
    assert!(matches!(
        facts[0],
        NativeFact::Response {
            state: ResponseState::Received,
            ..
        }
    ));
    assert!(matches!(
        facts[1],
        NativeFact::Evaluation {
            state: validation::State::Ready,
            before: None,
            ..
        }
    ));
    assert_eq!(facts[2], NativeFact::Delivery { key: result_key });
    assert!(matches!(
        facts[3],
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::TestamentAcknowledged,
            ..
        })
    ));
    assert!(
        read.with_evaluation(key, 0, |row| row.state())
            .unwrap()
            .is_none()
    );
    assert!(
        read.with_delivery_result(result_key, 0, |row| row.result())
            .unwrap()
            .is_none()
    );
}

#[test]
fn late_delivery_is_observed_with_ready_expired_check_and_no_pass() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    f.serial = 1000;
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let view = f.owner.committed();
    assert_eq!((outcome.evaluations, outcome.results), (1, 0));
    let state = view.evaluation(delivery_key(900, 1)).unwrap();
    assert_eq!(state.state(), validation::State::Ready);
    assert_eq!(state.last_result(), None);
    assert_eq!(
        view.response(TestamentId::from_u128(900)).unwrap().state(),
        ResponseState::Received
    );
    assert_eq!(
        view.claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::TestamentAcknowledged
    );
}

#[test]
fn pending_receipt_and_delivery_cohort_retry_or_discard_as_one_unit() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    let claim = f.claim();
    let response = f.response(900);
    let input = f.input(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim,
            expected: response,
        },
    );
    let request = input.request;
    let NativeStaging::Prepared { candidate, outcome } =
        f.owner.prepare(context(ISSUER, 90), input, None).unwrap()
    else {
        panic!("prepared")
    };
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Posted
    );
    assert!(
        f.owner
            .committed()
            .evaluation(delivery_key(900, 1))
            .is_none()
    );
    assert_eq!(
        f.owner
            .effective()
            .evaluation(delivery_key(900, 1))
            .unwrap()
            .state(),
        validation::State::Validated
    );
    let retry = || NativeInput {
        request,
        command: NativeCommand::ReceiveResponse {
            claim,
            expected: response,
        },
    };
    assert_eq!(
        f.owner.prepare(context(ISSUER, 0), retry(), None).unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: Some(candidate)
        }
    );
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert!(
        f.owner
            .effective()
            .evaluation(delivery_key(900, 1))
            .is_none()
    );
    assert_eq!(f.claim(), claim);
    let NativeStaging::Prepared { candidate, .. } =
        f.owner.prepare(context(ISSUER, 90), retry(), None).unwrap()
    else {
        panic!("prepared")
    };
    f.owner.publish_after_durable(candidate).unwrap();
    assert!(matches!(
        f.owner.prepare(context(ISSUER, 0), retry(), None).unwrap(),
        NativeStaging::Existing {
            candidate: None,
            ..
        }
    ));
}

#[test]
fn distinct_responses_keep_distinct_receipt_results_when_received_out_of_order() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    posted(&mut f, 901);
    for id in [901, 900] {
        f.commit(
            ISSUER,
            NativeCommand::ReceiveResponse {
                claim: f.claim(),
                expected: f.response(id),
            },
        );
    }
    let view = f.owner.committed();
    for (id, cycle) in [(900, 1), (901, 2)] {
        let key = delivery_key(id, cycle);
        let state = view.evaluation(key).unwrap();
        let result = view
            .delivery_result(NativeResultKey {
                evaluation: key,
                revision: state.binding().revision,
            })
            .unwrap()
            .result();
        assert_eq!(result.generation(), cycle);
        assert!(
            matches!(result.target(),validation::Target::Delivery {response} if response.object.0 == TestamentId::from_u128(id).0)
        );
    }
    assert_eq!(
        view.claim(ClaimId::from_u128(1)).unwrap().response_count(),
        2
    );
}

#[test]
fn terminal_claimant_receipt_is_audit_only_and_creates_no_acceptance_cohort() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    f.commit(
        ISSUER,
        NativeCommand::Cancel {
            expected: f.claim(),
        },
    );
    let claim = f.claim();
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim,
            expected: f.response(900),
        },
    );
    assert_eq!(
        (
            outcome.changed,
            outcome.evaluations,
            outcome.results,
            outcome.events
        ),
        (0, 0, 0, 1)
    );
    assert_eq!(f.claim(), claim);
    assert!(
        f.owner
            .committed()
            .evaluation(delivery_key(900, 1))
            .is_none()
    );
}

#[test]
fn responsibility_refuses_an_authored_response_count_that_cannot_fit_delivery_membership() {
    for (cap, allowed) in [(1, false), (4, true)] {
        let mut core = Core::new_native(
            binding(1).ledger,
            RangeId(791),
            NativeLimits {
                evaluations_per_claim: cap,
                plan_nodes: 16,
                plan_edges: 256,
                preparation_bytes: 1024 * 1024,
                range: RangeConfig {
                    max_batch_entries: 128,
                    ..RangeConfig::default()
                },
                ..NativeLimits::default()
            },
            MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        super::super::report_tests::publish(&mut core, 10, creation(1, 1, &[], None));
        super::super::report_tests::publish(
            &mut core,
            20,
            super::super::report_tests::post(2, binding(1)),
        );
        let expected = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
        let result = core.prepare_native(
            context(SUBJECT, 30),
            NativeInput {
                request: request(SUBJECT, 3),
                command: NativeCommand::AcquireReceipt {
                    expected,
                    receipt: ReceiptId::from_u128(799),
                },
            },
            &[],
        );
        assert_eq!(result.is_ok(), allowed);
        assert_eq!(
            core.native_claim(ClaimId::from_u128(1)).unwrap().binding(),
            expected
        );
        assert!(core.native_receipt(ReceiptId::from_u128(799)).is_none());
    }
}
