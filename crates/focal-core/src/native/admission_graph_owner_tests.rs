//! Admission completion promises cover actual incoming graph and monitor facts.
use super::*;
use focal_model::{MonitorId, WaitPredicate};

const MONITOR: MonitorId = MonitorId::from_u128(90_001);
const PARENT_MONITOR: MonitorId = MonitorId::from_u128(90_004);

fn limits(core: &mut Core<NativeState>) {
    // Full graph/index replay has a separate finite bound from the small report
    // fixture. These are node limits established before owner responsibilities.
    core.limits.plan_edges = 2 * 1024 * 1024;
    core.limits.preparation_bytes = 16 * 1024 * 1024;
}
fn register(
    owner: &mut NativeOwner,
    request_id: u128,
    holder: u128,
    target: u128,
    id: MonitorId,
    time: u64,
) -> NativeInput {
    let source = owner.effective().claim(ClaimId::from_u128(holder)).unwrap();
    NativeInput {
        request: request(ISSUER, request_id),
        command: NativeCommand::RegisterMonitor {
            expected: source.binding(),
            receipt: source.receipt().map(|row| row.fence),
            id,
            roots: vec![WaitPredicate::Terminal(ClaimId::from_u128(target))],
            deadline: Deadline {
                timer: TimerId::from_u128(request_id),
                generation: 1,
                at: time + 400,
            },
        },
    }
}
#[track_caller]
fn report_at(
    owner: &mut NativeOwner,
    store: &mut Store,
    input: NativeInput,
    time: u64,
) -> NativeCandidate {
    let NativeCommand::ReportAdmission {
        claim,
        key,
        expected,
        ..
    } = &input.command
    else {
        panic!("Admission report")
    };
    assert_eq!(
        owner.effective().claim(key.claim).unwrap().binding(),
        *claim
    );
    assert_eq!(
        owner.effective().evaluation(*key).unwrap().binding(),
        *expected
    );
    let invocation = input.request;
    match owner.prepare_with_custody(
        context(invocation.principal, time),
        input,
        &mut store.content,
        DOMAIN,
        &BuiltinNativeSchemas,
    ) {
        Ok(NativeStaging::Prepared { candidate, .. }) => candidate,
        result => panic!("Admission report {invocation:?} at time {time}: {result:?}"),
    }
}
fn source() -> NativeOwner {
    let mut core = authored_posted(&[
        (
            1,
            900,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, false),
            ],
            &[],
        ),
        (2, 900, &[], &[(graph::Kind::DependsOn, 1)]),
        (3, 900, &[], &[]),
    ]);
    limits(&mut core);
    let mut owner = NativeOwner::new(core).unwrap();
    let input = register(&mut owner, 90_001, 3, 1, MONITOR, 25);
    let staged = stage(&mut owner, input, 25);
    owner.publish_after_durable(staged).unwrap();
    // This real runtime wait forms a finite cycle with dependent2's DependsOn1.
    // A failed Admission result resolves it through the dependency's terminal
    // consequence before releasing the report parent's own monitor.
    let input = register(&mut owner, 90_004, 1, 2, PARENT_MONITOR, 26);
    let staged = stage(&mut owner, input, 26);
    owner.publish_after_durable(staged).unwrap();
    begin_claim(&mut owner, 1, 2, 90_002);
    begin_claim(&mut owner, 1, 1, 90_003);
    owner
}
fn claims(owner: &NativeOwner) -> Vec<ClaimState> {
    (1..=3)
        .map(|id| {
            let row = owner.effective().claim(ClaimId::from_u128(id)).unwrap();
            row.try_copy(row.retained_bytes().unwrap()).unwrap()
        })
        .collect()
}
fn assert_claims(owner: &NativeOwner, expected: &[ClaimState]) {
    for row in expected {
        assert_eq!(
            owner
                .effective()
                .claim(ClaimId(row.binding().object.0))
                .unwrap(),
            row
        );
    }
}
fn assert_publication(
    owner: &NativeOwner,
    key: EvaluationKey,
    outcome: NativeOutcome,
) -> NativeAccepted {
    let view = owner.effective();
    let result = view.evaluation(key).unwrap().last_result().unwrap();
    let key = NativeResultKey::of(result);
    let accepted = *view.result(key).unwrap();
    assert_eq!(accepted.sequence(), outcome.sequence);
    assert_eq!(accepted.ordinal(), 2);
    let evidence = result.evidence().unwrap();
    let artifact = view.artifact(evidence.id).unwrap();
    assert_eq!(artifact.descriptor().id(), evidence.id);
    assert_eq!(artifact.descriptor().content_hash(), evidence.hash);
    assert!(matches!(view.event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Artifact { binding } if binding == artifact.descriptor().binding()));
    assert!(matches!(
        view.event(outcome.sequence, 1).unwrap().fact,
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Reported,
            ..
        }
    ));
    assert_eq!(
        view.event(outcome.sequence, 2).unwrap().fact,
        NativeFact::Accepted { key }
    );
    accepted
}

#[test]
fn funded_admission_failure_publishes_complete_graph_and_monitor_effects_and_discard_restores_them()
{
    let mut owner = source();
    let parent = owner.core.state.budget.clone();
    let mut store = Store::new();
    // Keep an actual diagnostic from the previous permitted attempt as well as
    // the final failing proof. Neither graph propagation nor seals erase it.
    let error = report_claim(&owner, 1, 1, 90_010, VerdictValue::Error);
    let pressure = exhaust(&parent);
    let error_candidate = report_at(&mut owner, &mut store, error, 40);
    let error_outcome = owner
        .candidate(error_candidate)
        .unwrap()
        .recorded(request(EVALUATOR, 90_010))
        .unwrap();
    let error = assert_publication(&owner, claim_key(1, 1), error_outcome);
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Posted
    );
    owner.publish_after_durable(error_candidate).unwrap();
    drop(pressure);
    let original = claims(&owner);
    let previous = *owner.effective().evaluation(claim_key(1, 1)).unwrap();
    let observe = *owner.effective().evaluation(claim_key(1, 2)).unwrap();
    let credit = owner.book.remaining_reports(claim_key(1, 1));
    assert_eq!(credit, Some(1));
    let failed = report_claim(&owner, 1, 1, 90_011, VerdictValue::Fail);
    let retry = copy_report(&failed);
    let pressure = exhaust(&parent);
    assert_eq!(parent.stats().used, parent.limit());
    let failure_candidate = report_at(&mut owner, &mut store, failed, 41);
    let outcome = owner
        .candidate(failure_candidate)
        .unwrap()
        .recorded(request(EVALUATOR, 90_011))
        .unwrap();
    let accepted = assert_publication(&owner, claim_key(1, 1), outcome);
    assert_eq!(accepted.result().verdict(), VerdictValue::Fail);
    let NativeFact::Claim(post_failed) = owner.effective().event(outcome.sequence, 3).unwrap().fact
    else {
        panic!("original Admission failure")
    };
    assert_eq!(post_failed.kind, NativeEventKind::PostFailed);
    assert_eq!(post_failed.after.object.0, ClaimId::from_u128(1).0);
    let failed = owner.effective().claim(ClaimId::from_u128(1)).unwrap();
    assert_eq!(failed.status(), ClaimStatus::PostFailed);
    assert_eq!(failed.binding(), post_failed.after.next().unwrap());
    let ClaimTerminalCut::Required(required_cut) = failed.terminal_cut().unwrap() else {
        panic!("original required failure cut")
    };
    assert_eq!(required_cut.sequence(), outcome.sequence);
    assert_eq!(
        failed
            .scopes()
            .monitor(PARENT_MONITOR)
            .unwrap()
            .release_cut()
            .unwrap()
            .position,
        outcome.sequence
    );
    assert!(!failed.released());
    let dependent = owner.effective().claim(ClaimId::from_u128(2)).unwrap();
    assert_eq!(dependent.status(), ClaimStatus::DependencyFailed);
    let ClaimTerminalCut::Graph(cut) = dependent.terminal_cut().unwrap() else {
        panic!("dependency cut")
    };
    assert_eq!(cut.origin().binding(), post_failed.after);
    assert_eq!(cut.origin().terminal(), outcome.sequence);
    let monitor_owner = owner.effective().claim(ClaimId::from_u128(3)).unwrap();
    assert_eq!(monitor_owner.status(), ClaimStatus::Posted);
    assert_eq!(
        monitor_owner
            .scopes()
            .monitor(MONITOR)
            .unwrap()
            .release_cut()
            .unwrap()
            .position,
        outcome.sequence
    );
    assert!(!monitor_owner.released());
    let monitor_releases = (0..outcome.events).filter(|ordinal| matches!(
        owner.effective().event(outcome.sequence, *ordinal).unwrap().fact,
        NativeFact::Claim(event) if matches!(event.kind, NativeEventKind::Monitor(NativeMonitorEvent::Released { .. }))
    )).count();
    assert_eq!(monitor_releases, 2);
    assert_eq!(
        owner
            .effective()
            .evaluation(claim_key(1, 2))
            .unwrap()
            .fence(),
        observe.fence()
    );
    assert_eq!(
        owner
            .effective()
            .result(NativeResultKey::of(error.result())),
        Some(&error)
    );
    assert!(
        owner
            .effective()
            .artifact(error.result().evidence().unwrap().id)
            .is_some()
    );
    assert_eq!((outcome.responses, outcome.result_testaments), (0, 0));
    for row in &original {
        assert_eq!(
            owner
                .committed()
                .claim(ClaimId(row.binding().object.0))
                .unwrap(),
            row
        );
    }
    assert_eq!(owner.discard_from(failure_candidate).unwrap(), 1);
    assert_claims(&owner, &original);
    assert_eq!(
        owner.effective().evaluation(claim_key(1, 1)).unwrap(),
        &previous
    );
    assert_eq!(
        owner.effective().evaluation(claim_key(1, 2)).unwrap(),
        &observe
    );
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), credit);
    assert!(
        owner
            .effective()
            .result(NativeResultKey::of(accepted.result()))
            .is_none()
    );
    assert!(
        owner
            .effective()
            .recorded(request(EVALUATOR, 90_011))
            .is_none()
    );
    let failure_candidate = report_at(&mut owner, &mut store, retry, 41);
    assert_eq!(
        owner
            .candidate(failure_candidate)
            .unwrap()
            .recorded(request(EVALUATOR, 90_011))
            .unwrap(),
        outcome
    );
    owner.publish_after_durable(failure_candidate).unwrap();
    drop(pressure);
    let terminal = claims(&owner);
    let pressure = exhaust(&parent);
    let late = report_claim(&owner, 1, 2, 90_012, VerdictValue::Pass);
    let late = report_at(&mut owner, &mut store, late, 42);
    let late_outcome = owner
        .candidate(late)
        .unwrap()
        .recorded(request(EVALUATOR, 90_012))
        .unwrap();
    assert_publication(&owner, claim_key(1, 2), late_outcome);
    assert_eq!(late_outcome.events, 3);
    assert_eq!(late_outcome.changed, 0);
    assert_claims(&owner, &terminal);
    owner.publish_after_durable(late).unwrap();
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), None);
    assert_eq!(owner.book.remaining_reports(claim_key(1, 2)), None);
    drop(pressure);
}

#[test]
fn begun_posted_admission_refuses_or_funds_new_incoming_dependency_and_monitor_before_accepting_them()
 {
    for monitor_growth in [false, true] {
        let mut core = authored_posted(&[
            (1, 900, &[(ValidationMode::Required, false)], &[]),
            (3, 900, &[], &[]),
        ]);
        limits(&mut core);
        let parent = core.state.budget.clone();
        let mut owner = NativeOwner::new(core).unwrap();
        begin_claim(&mut owner, 1, 1, 91_001);
        let before = owner.effective().sequence();
        let credit = owner.book.remaining_reports(claim_key(1, 1));
        let input = if monitor_growth {
            register(&mut owner, 91_002, 3, 1, MONITOR, 35)
        } else {
            let mut input = creation(91_002, 2, &[], None);
            let NativeCommand::Create { claims, .. } = &mut input.command else {
                panic!("creation")
            };
            claims[0].definition.graph = graph::Declaration::new(
                &[graph::Obligation {
                    kind: graph::Kind::DependsOn,
                    target: ClaimId::from_u128(1),
                }],
                16,
            )
            .unwrap();
            input
        };
        let accepted_growth = match owner.prepare(context(ISSUER, 35), input, None) {
            Ok(NativeStaging::Prepared { candidate, .. }) => {
                owner.publish_after_durable(candidate).unwrap();
                true
            }
            Err(error) => {
                assert!(
                    matches!(
                        error,
                        NativeOwnerError::Native(
                            NativeError::Capacity(_)
                                | NativeError::Memory(_)
                                | NativeError::Contract(
                                    ContractError::Capacity | ContractError::InvalidPolicy
                                )
                        )
                    ),
                    "growth must be refused by its capacity contract: {error:?}"
                );
                assert_eq!(owner.effective().sequence(), before);
                assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), credit);
                assert_eq!(owner.pending_len(), 0);
                false
            }
            other => panic!("fresh growth: {other:?}"),
        };
        let mut store = Store::new();
        let failed = report_claim(&owner, 1, 1, 91_003, VerdictValue::Fail);
        let pressure = exhaust(&parent);
        let candidate = report_at(&mut owner, &mut store, failed, 40);
        let outcome = owner
            .candidate(candidate)
            .unwrap()
            .recorded(request(EVALUATOR, 91_003))
            .unwrap();
        assert_publication(&owner, claim_key(1, 1), outcome);
        assert_eq!(
            owner
                .effective()
                .claim(ClaimId::from_u128(1))
                .unwrap()
                .status(),
            ClaimStatus::PostFailed
        );
        if monitor_growth {
            let row = owner.effective().claim(ClaimId::from_u128(3)).unwrap();
            if accepted_growth {
                assert!(
                    row.scopes()
                        .monitor(MONITOR)
                        .unwrap()
                        .release_cut()
                        .is_some()
                );
            } else {
                assert!(row.scopes().monitor(MONITOR).is_none());
            }
        } else if accepted_growth {
            assert_eq!(
                owner
                    .effective()
                    .claim(ClaimId::from_u128(2))
                    .unwrap()
                    .status(),
                ClaimStatus::DependencyFailed
            );
        } else {
            assert!(owner.effective().claim(ClaimId::from_u128(2)).is_none());
        }
        owner.publish_after_durable(candidate).unwrap();
        assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), None);
        drop(pressure);
    }
}

#[test]
fn reconstructing_actual_begun_admission_restores_its_graph_promise_before_accepting_reports() {
    let mut core = authored_posted(&[
        (1, 900, &[(ValidationMode::Required, false)], &[]),
        (2, 900, &[], &[(graph::Kind::DependsOn, 1)]),
        (3, 900, &[], &[]),
    ]);
    limits(&mut core);
    let expected = core.native_claim(ClaimId::from_u128(3)).unwrap().binding();
    publish(
        &mut core,
        25,
        NativeInput {
            request: request(ISSUER, 92_001),
            command: NativeCommand::RegisterMonitor {
                expected,
                receipt: None,
                id: MONITOR,
                roots: vec![WaitPredicate::Terminal(ClaimId::from_u128(1))],
                deadline: Deadline {
                    timer: TimerId::from_u128(92_001),
                    generation: 1,
                    at: 500,
                },
            },
        },
    );
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let evaluation = core.native_evaluation(claim_key(1, 1)).unwrap().binding();
    publish(&mut core, 30, begin(92_002, claim, 1, evaluation));
    let source_prefix = core.native_sequence();
    let parent = core.state.budget.clone();
    let pressure = exhaust(&parent);
    let refusal = NativeOwner::new(core).unwrap_err();
    assert_eq!(refusal.core.native_sequence(), source_prefix);
    assert!(
        refusal
            .core
            .native_evaluation(claim_key(1, 1))
            .unwrap()
            .has_begun()
    );
    assert!(
        refusal
            .core
            .native_claim(ClaimId::from_u128(3))
            .unwrap()
            .scopes()
            .monitor(MONITOR)
            .unwrap()
            .active()
    );
    drop(pressure);
    // This reconstructs the managed RAM owner from actual retained Core rows;
    // it is deliberately not a claim of codec or crash-recovery qualification.
    let mut owner = NativeOwner::new(refusal.core).unwrap();
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), Some(2));
    let failed = report_claim(&owner, 1, 1, 92_003, VerdictValue::Fail);
    let mut store = Store::new();
    let pressure = exhaust(&parent);
    let candidate = report_at(&mut owner, &mut store, failed, 40);
    let outcome = owner
        .candidate(candidate)
        .unwrap()
        .recorded(request(EVALUATOR, 92_003))
        .unwrap();
    assert_publication(&owner, claim_key(1, 1), outcome);
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::PostFailed
    );
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(2))
            .unwrap()
            .status(),
        ClaimStatus::DependencyFailed
    );
    assert!(
        owner
            .effective()
            .claim(ClaimId::from_u128(3))
            .unwrap()
            .scopes()
            .monitor(MONITOR)
            .unwrap()
            .release_cut()
            .is_some()
    );
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), None);
    drop(pressure);
}
