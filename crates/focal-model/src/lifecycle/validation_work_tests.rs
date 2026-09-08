use super::super::tests as fixtures;
use super::*;
use crate::lifecycle::{aggregation as a, claim, evidence as e, graph};
use crate::{Confidence, EvidenceAttestation, OutcomeKind, ReceiptId, SessionSeq};

#[path = "evidence_entry_tests.rs"]
mod entry_tests;

fn limits() -> a::Limits {
    a::Limits {
        max_slots: 2,
        max_checks: 8,
        max_results: 32,
        max_updates: 8,
    }
}

fn definitions(
    mode: ValidationMode,
    program: Program<'_>,
    increment: bool,
    sibling: bool,
) -> Vec<Declaration> {
    let spec = DeclarationSpec {
        binding: fixtures::binding(202),
        claim: ClaimId::from_u128(9),
        declaration_index: 1,
        deadline: Deadline {
            at: 1000,
            ..fixtures::specification(mode, program).deadline
        },
        ..fixtures::specification(mode, program)
    };
    let mut definitions = vec![
        Declaration::new(
            Principal::Actor(spec.issuer),
            DeclarationSpec {
                binding: fixtures::binding(201),
                declaration_index: 0,
                kind: ValidationKind::Receipt,
                target: TargetDeclaration::Delivery,
                mode: ValidationMode::Required,
                program: Program::Delivery,
                ..spec
            },
            fixtures::limits(),
        )
        .unwrap(),
        Declaration::new(Principal::Actor(spec.issuer), spec, fixtures::limits()).unwrap(),
    ];
    if sibling {
        definitions.push(
            Declaration::new(
                Principal::Actor(spec.issuer),
                DeclarationSpec {
                    binding: fixtures::binding(203),
                    declaration_index: 2,
                    mode: ValidationMode::Observe,
                    program: fixtures::programmatic(true),
                    ..spec
                },
                fixtures::limits(),
            )
            .unwrap(),
        );
    }
    if increment {
        definitions.push(
            Declaration::new(
                Principal::Actor(spec.issuer),
                DeclarationSpec {
                    binding: fixtures::binding(204),
                    declaration_index: 3,
                    target: TargetDeclaration::Increment,
                    phase: ValidationPhase::Increment,
                    mode: ValidationMode::Required,
                    program: fixtures::programmatic(false),
                    ..spec
                },
                fixtures::limits(),
            )
            .unwrap(),
        );
    }
    definitions
}

fn setup(
    definitions: &[Declaration],
    present: bool,
) -> (ClaimState, Response, Option<WorkArtifact>) {
    let mut definition = claim::tests::definition(2);
    let checks: Vec<_> = definitions
        .iter()
        .filter_map(|declaration| {
            matches!(
                declaration.target(),
                TargetDeclaration::WholeWorkSlot { .. }
            )
            .then_some(a::CheckPolicy {
                declaration_index: declaration.declaration_index(),
                validation: ValidationId(declaration.binding().object.0),
                mode: declaration.mode(),
            })
        })
        .collect();
    definition.acceptance = a::AcceptancePolicy::new(
        definition.binding,
        definition.issuer,
        &[a::SlotPolicy {
            slot: 0,
            missing_declaration_index: 4,
            mode: ValidationMode::Required,
            checks: &checks,
        }],
        definitions,
        limits(),
    )
    .unwrap();
    let mut claim = ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    claim
        .post_owned(Principal::Actor(claim.issuer()), claim.binding())
        .unwrap();
    let graph = graph::Snapshot::capture(
        &[&claim],
        graph::Limits {
            nodes: 2,
            edges: 2,
            visits: 64,
        },
    )
    .unwrap();
    let start = graph.start(ClaimId(claim.binding().object.0)).unwrap();
    let aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    claim
        .acquire_receipt(
            &claim.binding(),
            Principal::Actor(claim.subject()),
            ReceiptFence {
                receipt: ReceiptId::from_u128(100),
                epoch: 1,
            },
            &aggregate.admission(),
            &start,
            &[],
        )
        .unwrap();
    let parent = e::Parent::from_claim(&claim).unwrap();
    let generated = present.then(|| {
        WorkArtifact::generate(
            fixtures::binding(300),
            &parent,
            Principal::Actor(parent.holder),
            0,
            parent.receipt,
            &EvidenceAttestation {
                descriptor_hash: fixtures::binding(300).content,
                custody_revision: 1,
                durable: true,
                schema_valid: true,
            },
        )
        .unwrap()
    });
    let current: Vec<_> = generated.into_iter().collect();
    let manifest: Vec<_> = current
        .iter()
        .map(|work| e::SlotBinding {
            slot: work.slot(),
            artifact: work.reference(),
        })
        .collect();
    let plan = Response::close(e::ResponseIdentity {
        binding: fixtures::binding(400), claim: parent.claim, receipt: parent.receipt,
        cycle: parent.next_cycle, prior: parent.latest_response,
    }, &parent, Principal::Actor(parent.holder), &current, &manifest, e::CloseReport {
        summary: "The respondent reports completion; acceptance is still independently checked.",
        confidence: Confidence::Committed, outcome: OutcomeKind::Complete, diagnostics: &[],
        limits: e::ResponseLimits { artifacts: 1, diagnostics: 0, summary_bytes: 256, construction_bytes: 4096 },
    }).unwrap();
    claim
        .observe_response(
            &claim.binding(),
            Principal::Actor(parent.holder),
            &plan.response,
        )
        .unwrap();
    (claim, plan.response, plan.attachments.first().copied())
}

fn post(claim: &mut ClaimState, response: &mut Response) {
    let parent = e::Parent::from_claim(claim).unwrap();
    response
        .apply(
            response
                .plan_post(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(parent.holder),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.holder), response)
        .unwrap();
}

fn receive(claim: &mut ClaimState, response: &mut Response) {
    let parent = e::Parent::from_claim(claim).unwrap();
    response
        .apply(
            response
                .plan_receive(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(parent.issuer),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.issuer), response)
        .unwrap();
}

fn received(
    definitions: &[Declaration],
    present: bool,
) -> (ClaimState, Response, Option<WorkArtifact>) {
    let (mut claim, mut response, work) = setup(definitions, present);
    post(&mut claim, &mut response);
    receive(&mut claim, &mut response);
    (claim, response, work)
}

fn ready<'a>(
    declaration: &'a Declaration,
    claim: &ClaimState,
    response: &Response,
    work: Option<&WorkArtifact>,
) -> Evaluation<'a> {
    Evaluation::materialize_work(
        Principal::Actor(claim.issuer()),
        declaration,
        claim,
        response,
        work,
    )
    .unwrap()
}

fn begin<'a>(
    evaluation: Evaluation<'a>,
    claim: &ClaimState,
    response: &Response,
    work: Option<&WorkArtifact>,
    aggregate: &a::ClaimAggregation,
) -> Evaluation<'a> {
    let owner = evaluation
        .work_owner(claim, response, work, &aggregate.decision(), 10)
        .unwrap();
    evaluation
        .begin(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &owner,
        )
        .unwrap()
        .next
}

fn enter(
    claim: &mut ClaimState,
    response: &mut Response,
    work: WorkArtifact,
    evaluation: &Evaluation<'_>,
    aggregate: &mut a::ClaimAggregation,
) -> WorkArtifact {
    let parent = e::Parent::from_claim(claim).unwrap();
    response
        .apply(
            response
                .plan_evaluation(
                    &response.identity().binding,
                    claim,
                    evaluation,
                    &aggregate.decision(),
                )
                .unwrap(),
        )
        .unwrap();
    let work = work
        .observe_evaluation(&work.binding(), &parent, response, evaluation)
        .unwrap();
    claim
        .observe_evaluation(&claim.binding(), evaluation, &aggregate.decision())
        .unwrap();
    aggregate.rebind(claim).unwrap();
    work
}

fn report<'a>(
    evaluation: Evaluation<'a>,
    claim: &ClaimState,
    response: &Response,
    work: &WorkArtifact,
    verdict: VerdictValue,
) -> Transition<'a> {
    let owner = evaluation
        .work_report_owner(claim, response, Some(work), 20)
        .unwrap();
    let (report, evidence) = fixtures::report_parts(&evaluation, verdict);
    evaluation
        .report(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &owner,
            report,
            &evidence,
        )
        .unwrap()
}

fn delivery(declaration: &Declaration, claim: &ClaimState, response: &Response) -> AcceptedResult {
    let ready = Evaluation::materialize_delivery(
        Principal::Actor(claim.issuer()),
        declaration,
        claim,
        response,
    )
    .unwrap();
    ready
        .receive_delivery(
            Principal::Actor(claim.issuer()),
            &ready.binding(),
            &ready.delivery_owner(claim, response, 10).unwrap(),
        )
        .unwrap()
        .result
        .unwrap()
}

#[test]
fn only_actual_received_manifest_and_attached_source_can_materialize() {
    let definitions = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let (mut claim, mut response, work) = setup(&definitions, true);
    let work = work.unwrap();
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &definitions[1],
            &claim,
            &response,
            Some(&work)
        )
        .is_err()
    );
    post(&mut claim, &mut response);
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &definitions[1],
            &claim,
            &response,
            Some(&work)
        )
        .is_err()
    );
    let parent = e::Parent::from_claim(&claim).unwrap();
    response
        .apply(
            response
                .plan_receive(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(parent.issuer),
                )
                .unwrap(),
        )
        .unwrap();
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &definitions[1],
            &claim,
            &response,
            Some(&work)
        )
        .is_err()
    );
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.issuer), &response)
        .unwrap();
    let ready = ready(&definitions[1], &claim, &response, Some(&work));
    assert_eq!(
        ready.target(),
        Target::Artifact {
            response: response.identity().binding,
            slot: 0,
            artifact: work.binding()
        }
    );
    assert_eq!(ready.generation(), 1);
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &definitions[1],
            &claim,
            &response,
            None
        )
        .is_err()
    );
    for actor in [
        Principal::Node(claim.issuer()),
        Principal::Actor(claim.subject()),
    ] {
        assert_eq!(
            Evaluation::materialize_work(actor, &definitions[1], &claim, &response, Some(&work))
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    let other = WorkArtifact::generate(
        fixtures::binding(301),
        &e::Parent::from_claim(&claim).unwrap(),
        Principal::Actor(parent.holder),
        0,
        parent.receipt,
        &EvidenceAttestation {
            descriptor_hash: fixtures::binding(301).content,
            custody_revision: 1,
            durable: true,
            schema_valid: true,
        },
    )
    .unwrap();
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &definitions[1],
            &claim,
            &response,
            Some(&other)
        )
        .is_err()
    );
    let altered = self::definitions(
        ValidationMode::Observe,
        fixtures::programmatic(false),
        false,
        false,
    );
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &altered[1],
            &claim,
            &response,
            Some(&work)
        )
        .is_err()
    );
}

#[test]
fn absent_slot_has_an_artifact_free_required_outcome_or_observe_suppression() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        let definitions = definitions(mode, fixtures::programmatic(false), false, false);
        let (claim, response, work) = received(&definitions, false);
        assert!(work.is_none());
        let ready = ready(&definitions[1], &claim, &response, None);
        assert_eq!(
            ready.target(),
            Target::MissingSlot {
                response: response.identity().binding,
                slot: 0
            }
        );
        let aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
        let owner = ready
            .work_owner(&claim, &response, None, &aggregate.decision(), 10)
            .unwrap();
        let transition = ready
            .begin(Principal::Actor(claim.issuer()), &ready.binding(), &owner)
            .unwrap();
        if mode == ValidationMode::Required {
            let result = transition.result.unwrap();
            assert_eq!(transition.next.state(), State::ValidationIncomplete);
            assert_eq!(result.phase(), Phase::MissingTarget);
            assert_eq!(result.verdict(), VerdictValue::Incomplete);
            assert!(result.evidence().is_none());
            assert!(result.attempt().is_none());
        } else {
            assert_eq!(transition.next.state(), State::Ready);
            assert_eq!(
                transition.next.suppression,
                Some(Suppression::MissingTarget)
            );
            assert!(transition.result.is_none());
        }
        assert!(!transition.next.has_begun());
        assert_eq!(response.state(), ResponseState::Received);
        assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
    }
}

#[test]
fn complete_increment_outcomes_and_explicit_target_seal_are_required_for_begin() {
    let definitions = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        true,
        false,
    );
    let (claim, response, work) = received(&definitions, true);
    let work = work.unwrap();
    let ready = ready(&definitions[1], &claim, &response, Some(&work));
    let increment = Evaluation::materialize_increment(
        Principal::Actor(claim.issuer()),
        &definitions[2],
        &claim,
        &work,
    )
    .unwrap();
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    aggregate.register(&ready).unwrap();
    aggregate.register(&increment).unwrap();
    assert!(
        ready
            .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 10)
            .is_err()
    );
    let owner = increment.increment_owner(&claim, &work, 10).unwrap();
    let increment = increment
        .begin(
            Principal::Actor(increment.evaluator().unwrap()),
            &increment.binding(),
            &owner,
        )
        .unwrap()
        .next;
    let (input, evidence) = fixtures::report_parts(&increment, VerdictValue::Pass);
    let result = increment
        .report(
            Principal::Actor(increment.evaluator().unwrap()),
            &increment.binding(),
            &increment.increment_report_owner(&claim, &work, 11).unwrap(),
            input,
            &evidence,
        )
        .unwrap()
        .result
        .unwrap();
    aggregate
        .apply_acceptance(SessionSeq(8), &[], &[result])
        .unwrap();
    assert!(
        ready
            .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 12)
            .is_err()
    );
    aggregate.seal_increment_targets().unwrap();
    assert!(
        ready
            .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 12)
            .is_ok()
    );
    let mut changed = claim.clone();
    changed
        .request_evaluation(
            &changed.binding(),
            Principal::Actor(changed.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    assert_eq!(
        ready
            .work_owner(&changed, &response, Some(&work), &aggregate.decision(), 12)
            .unwrap_err(),
        ContractError::StaleRevision
    );
}

#[test]
fn actual_success_preserves_begun_observe_quality_reports_after_local_completion() {
    let definitions = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        true,
    );
    let (mut claim, mut response, work) = received(&definitions, true);
    let work = work.unwrap();
    let required = ready(&definitions[1], &claim, &response, Some(&work));
    let observe = ready(&definitions[2], &claim, &response, Some(&work));
    let delivery = delivery(&definitions[0], &claim, &response);
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    aggregate.register(&required).unwrap();
    aggregate.register(&observe).unwrap();
    let required = begin(required, &claim, &response, Some(&work), &aggregate);
    let observe = begin(observe, &claim, &response, Some(&work), &aggregate);
    let work = enter(&mut claim, &mut response, work, &required, &mut aggregate);
    let mut response_aggregate = a::ResponseAggregation::new(
        &claim,
        response.evaluation().unwrap(),
        &[delivery],
        limits(),
    )
    .unwrap();
    let result = report(required, &claim, &response, &work, VerdictValue::Pass)
        .result
        .unwrap();
    let update = response_aggregate.apply(SessionSeq(10), &[result]).unwrap();
    let work = work.apply_aggregate(&work.binding(), &update).unwrap();
    response
        .apply(
            response
                .plan_aggregate(&response.identity().binding, &update)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    aggregate.apply(SessionSeq(10), &[update]).unwrap();
    claim
        .apply_aggregate(&claim.binding(), &aggregate.decision())
        .unwrap();
    assert!(claim.local_complete());
    assert_eq!(work.state(), WorkArtifactState::Validated);
    assert_eq!(response.state(), ResponseState::Validated);
    let claim_before = claim.clone();
    let response_before = response.clone();
    let attempt = observe.current_attempt().unwrap();
    let program = report(observe, &claim, &response, &work, VerdictValue::Pass);
    assert_eq!(program.result.unwrap().attempt(), Some(attempt.index));
    assert_eq!(program.next.current_phase(), Phase::Quality);
    let quality = report(program.next, &claim, &response, &work, VerdictValue::Pass);
    assert_eq!(quality.next.state(), State::Validated);
    assert_eq!(
        quality.result.unwrap().programmatic_evidence(),
        program.result.unwrap().evidence()
    );
    assert_eq!(claim, claim_before);
    assert_eq!(response, response_before);
    assert!(
        observe
            .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 20)
            .is_err()
    );
    assert!(
        Evaluation::materialize_work(
            Principal::Actor(claim.issuer()),
            &definitions[2],
            &claim,
            &response,
            Some(&work)
        )
        .is_err()
    );
}

#[test]
fn failed_artifact_suppresses_ready_sibling_and_begun_report_keeps_original_cuts() {
    let definitions = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        true,
    );
    let (mut claim, mut response, work) = received(&definitions, true);
    let work = work.unwrap();
    let required = ready(&definitions[1], &claim, &response, Some(&work));
    let sibling = ready(&definitions[2], &claim, &response, Some(&work));
    let delivery = delivery(&definitions[0], &claim, &response);
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    aggregate.register(&required).unwrap();
    aggregate.register(&sibling).unwrap();
    let required = begin(required, &claim, &response, Some(&work), &aggregate);
    let begun = begin(sibling, &claim, &response, Some(&work), &aggregate);
    let work = enter(&mut claim, &mut response, work, &required, &mut aggregate);
    let mut response_aggregate = a::ResponseAggregation::new(
        &claim,
        response.evaluation().unwrap(),
        &[delivery],
        limits(),
    )
    .unwrap();
    let result = report(required, &claim, &response, &work, VerdictValue::Fail)
        .result
        .unwrap();
    let update = response_aggregate.apply(SessionSeq(10), &[result]).unwrap();
    let work = work.apply_aggregate(&work.binding(), &update).unwrap();
    response
        .apply(
            response
                .plan_aggregate(&response.identity().binding, &update)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    // The checked candidate's artifact consequence suppresses only siblings on
    // that source. Parent aggregation is the following atomic owner reduction.
    let owner = sibling
        .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 20)
        .unwrap();
    let suppressed = sibling
        .begin(
            Principal::Actor(sibling.evaluator().unwrap()),
            &sibling.binding(),
            &owner,
        )
        .unwrap();
    assert_eq!(suppressed.next.state(), State::Ready);
    assert!(matches!(
        suppressed.next.suppression,
        Some(Suppression::ArtifactFailure(_))
    ));
    assert!(suppressed.result.is_none());
    aggregate.apply(SessionSeq(10), &[update]).unwrap();
    claim
        .apply_aggregate(&claim.binding(), &aggregate.decision())
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::ValidationFailed);
    let cut = claim.terminal_cut();
    let work_cut = work.terminal();
    let response_cut = response.terminal();
    let error = report(begun, &claim, &response, &work, VerdictValue::Error);
    let pass = report(error.next, &claim, &response, &work, VerdictValue::Pass);
    let _ = report(pass.next, &claim, &response, &work, VerdictValue::Fail);
    assert_eq!(claim.terminal_cut(), cut);
    assert_eq!(work.terminal(), work_cut);
    assert_eq!(response.terminal(), response_cut);
    assert!(
        sibling
            .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 20)
            .is_err()
    );
}

#[test]
fn controls_adoption_deadline_policy_and_stale_source_cannot_authorize_whole_work() {
    let definitions = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let (mut claim, mut response, work) = received(&definitions, true);
    let work = work.unwrap();
    let stale_response = response.clone();
    let ready = ready(&definitions[1], &claim, &response, Some(&work));
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    aggregate.register(&ready).unwrap();
    let owner = ready
        .work_owner(&claim, &response, Some(&work), &aggregate.decision(), 10)
        .unwrap();
    assert!(
        ready
            .begin(Principal::Actor(claim.issuer()), &ready.binding(), &owner)
            .is_err()
    );
    assert!(
        ready
            .begin(
                Principal::Node(ready.evaluator().unwrap()),
                &ready.binding(),
                &owner
            )
            .is_err()
    );
    let begun = begin(ready, &claim, &response, Some(&work), &aggregate);
    let work = enter(&mut claim, &mut response, work, &begun, &mut aggregate);
    assert!(
        begun
            .work_report_owner(&claim, &stale_response, Some(&work), 20)
            .is_err()
    );
    assert!(
        begun
            .work_report_owner(&claim, &response, None, 20)
            .is_err()
    );
    assert_eq!(
        begun
            .work_report_owner(&claim, &response, Some(&work), begun.deadline().at)
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    let cut = claim::ClaimCut {
        position: SessionSeq(10),
        cause: ContentHash([81; 32]),
    };
    for intent in [
        claim::ClaimIntent::Cancel { cut },
        claim::ClaimIntent::Revoke { cut },
    ] {
        let mut controlled = claim.clone();
        controlled
            .apply(
                &controlled.binding(),
                Principal::Actor(controlled.issuer()),
                intent,
            )
            .unwrap();
        assert_eq!(
            begun
                .work_report_owner(&controlled, &response, Some(&work), 20)
                .unwrap_err(),
            ContractError::StaleEvaluation
        );
    }
    let mut adopted = claim.clone();
    adopted
        .apply(
            &adopted.binding(),
            Principal::Actor(adopted.issuer()),
            claim::ClaimIntent::AdoptReceipt {
                previous: work.receipt(),
                replacement: claim::ReceiptEntitlement {
                    holder: ParticipantId::from_u128(99),
                    fence: ReceiptFence {
                        receipt: ReceiptId::from_u128(101),
                        epoch: 2,
                    },
                },
            },
        )
        .unwrap();
    assert_eq!(
        begun
            .work_report_owner(&adopted, &response, Some(&work), 20)
            .unwrap_err(),
        ContractError::StaleReceipt
    );
    let Program::Programmatic { check, quality } = fixtures::programmatic(true) else {
        panic!("fixture")
    };
    let mut quality = quality.unwrap();
    quality.required_policy = Some(ContentHash([82; 32]));
    let declarations = self::definitions(
        ValidationMode::Required,
        Program::Programmatic {
            check,
            quality: Some(quality),
        },
        false,
        false,
    );
    let (claim, response, work) = received(&declarations, true);
    let ready = self::ready(&declarations[1], &claim, &response, work.as_ref());
    let aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    assert_eq!(
        ready
            .work_owner(&claim, &response, work.as_ref(), &aggregate.decision(), 10)
            .unwrap_err(),
        ContractError::InvalidPolicy
    );
}

#[test]
fn structural_absence_needs_no_handler_deadline_or_policy_grant() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for required_policy in [None, Some(ContentHash([92; 32]))] {
            let Program::Programmatic { mut check, quality } = fixtures::programmatic(true) else {
                panic!("programmatic fixture")
            };
            check.required_policy = required_policy;
            let declarations =
                definitions(mode, Program::Programmatic { check, quality }, false, false);
            let (claim, response, _) = received(&declarations, false);
            let evaluation = ready(&declarations[1], &claim, &response, None);
            let aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
            // Generic execution still refuses the absent grant or expired
            // handler. Structural settlement never opens an external attempt.
            assert!(
                evaluation
                    .work_owner(
                        &claim,
                        &response,
                        None,
                        &aggregate.decision(),
                        evaluation.deadline().at
                    )
                    .is_err()
            );
            let transition = evaluation
                .settle_missing(
                    Principal::Actor(claim.issuer()),
                    &evaluation.binding(),
                    &claim,
                    &response,
                    &aggregate.decision(),
                )
                .unwrap();
            assert!(!transition.next.has_begun());
            assert_eq!(
                transition.next.binding().revision.0,
                evaluation.binding().revision.0 + 1
            );
            match mode {
                ValidationMode::Required => {
                    let result = transition.result.unwrap();
                    assert_eq!(result.phase(), Phase::MissingTarget);
                    assert_eq!(result.verdict(), VerdictValue::Incomplete);
                    assert_eq!(
                        (result.attempt(), result.reporter(), result.evidence()),
                        (None, None, None)
                    );
                    assert!(result.programmatic_evidence().is_none());
                    assert_eq!(transition.next.state(), State::ValidationIncomplete);
                }
                ValidationMode::Observe => {
                    assert!(transition.result.is_none());
                    assert_eq!(
                        transition.next.suppression(),
                        Some(Suppression::MissingTarget)
                    );
                    assert_eq!(transition.next.state(), State::Ready);
                }
            }
            assert!(
                transition
                    .next
                    .settle_missing(
                        Principal::Actor(claim.issuer()),
                        &transition.next.binding(),
                        &claim,
                        &response,
                        &aggregate.decision()
                    )
                    .is_err()
            );
            assert!(evaluation.last_result().is_none());
            assert_eq!(response.state(), ResponseState::Received);
        }
    }
}

#[test]
fn structural_absence_preserves_explicit_fences_and_sealed_suppression() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        let declarations = definitions(mode, fixtures::programmatic(false), false, false);
        let (claim, response, _) = received(&declarations, false);
        let evaluation = ready(&declarations[1], &claim, &response, None);
        let aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
        let owner = evaluation
            .work_owner(&claim, &response, None, &aggregate.decision(), 10)
            .unwrap();
        let mut fenced_owner = owner;
        fenced_owner.logical_time = evaluation.deadline().at;
        fenced_owner.authority.state = AuthorityState::Fenced(AuthorityFence {
            reason: FenceReason::Deadline(evaluation.deadline()),
            cause: ContentHash([93; 32]),
        });
        let fenced = evaluation
            .record_fence(&evaluation.binding(), &fenced_owner)
            .unwrap();
        let mut sealed_owner = owner;
        sealed_owner.cohort = Cohort::Sealed {
            cause: ContentHash([94; 32]),
        };
        let sealed = evaluation
            .record_seal(&evaluation.binding(), &sealed_owner)
            .unwrap();
        for retained in [fenced, sealed] {
            let transition = retained
                .settle_missing(
                    Principal::Actor(claim.issuer()),
                    &retained.binding(),
                    &claim,
                    &response,
                    &aggregate.decision(),
                )
                .unwrap();
            assert_eq!(transition.next.into_state(), retained.into_state());
            assert!(transition.result.is_none());
            assert!(!transition.next.has_begun());
        }
    }
}

#[test]
fn structural_absence_rejects_wrong_actor_report_receipt_and_parent_control() {
    let declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        false,
        false,
    );
    let (claim, response, _) = received(&declarations, false);
    let evaluation = ready(&declarations[1], &claim, &response, None);
    let aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    for actor in [
        Principal::Actor(claim.subject()),
        Principal::Node(claim.issuer()),
        Principal::Actor(evaluation.evaluator().unwrap()),
    ] {
        assert_eq!(
            evaluation
                .settle_missing(
                    actor,
                    &evaluation.binding(),
                    &claim,
                    &response,
                    &aggregate.decision()
                )
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    let (_, changed_report, _) = received(&declarations, true);
    assert!(
        evaluation
            .settle_missing(
                Principal::Actor(claim.issuer()),
                &evaluation.binding(),
                &claim,
                &changed_report,
                &aggregate.decision()
            )
            .is_err()
    );
    let mut stale = evaluation.binding();
    stale.revision.0 += 1;
    assert_eq!(
        evaluation
            .settle_missing(
                Principal::Actor(claim.issuer()),
                &stale,
                &claim,
                &response,
                &aggregate.decision()
            )
            .unwrap_err(),
        ContractError::StaleRevision
    );
    let cut = claim::ClaimCut {
        position: SessionSeq(10),
        cause: ContentHash([95; 32]),
    };
    for intent in [
        claim::ClaimIntent::Cancel { cut },
        claim::ClaimIntent::Revoke { cut },
        claim::ClaimIntent::AdoptReceipt {
            previous: response.identity().receipt,
            replacement: claim::ReceiptEntitlement {
                holder: ParticipantId::from_u128(99),
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(101),
                    epoch: 2,
                },
            },
        },
    ] {
        let mut controlled = claim.clone();
        controlled
            .apply(
                &controlled.binding(),
                Principal::Actor(controlled.issuer()),
                intent,
            )
            .unwrap();
        let changed = a::ClaimAggregation::new(&controlled, limits()).unwrap();
        assert!(
            evaluation
                .settle_missing(
                    Principal::Actor(controlled.issuer()),
                    &evaluation.binding(),
                    &controlled,
                    &response,
                    &changed.decision()
                )
                .is_err()
        );
    }
    assert_eq!(evaluation.state(), State::Ready);
    assert!(!evaluation.has_begun());
}

#[test]
fn structural_absence_requires_the_actual_increment_target_set_to_be_sealed() {
    let declarations = definitions(
        ValidationMode::Required,
        fixtures::programmatic(false),
        true,
        false,
    );
    let (claim, response, _) = received(&declarations, false);
    let evaluation = ready(&declarations[1], &claim, &response, None);
    let mut aggregate = a::ClaimAggregation::new(&claim, limits()).unwrap();
    assert!(
        evaluation
            .settle_missing(
                Principal::Actor(claim.issuer()),
                &evaluation.binding(),
                &claim,
                &response,
                &aggregate.decision()
            )
            .is_err()
    );
    aggregate.seal_increment_targets().unwrap();
    assert!(
        evaluation
            .settle_missing(
                Principal::Actor(claim.issuer()),
                &evaluation.binding(),
                &claim,
                &response,
                &aggregate.decision()
            )
            .unwrap()
            .result
            .is_some()
    );
}
