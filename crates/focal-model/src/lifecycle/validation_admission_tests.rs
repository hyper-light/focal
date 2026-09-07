use super::super::tests as fixtures;
use super::*;
use crate::lifecycle::{aggregation, claim, graph, scope, succession};
use crate::{ObjectRevision, ReceiptId, RootCommandId, SessionSeq};
use fixtures::{EVALUATOR, ISSUER, binding, programmatic};

const SUBJECT: ParticipantId = ParticipantId::from_u128(9);

fn limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 4,
        max_checks: 8,
        max_results: 16,
        max_updates: 8,
    }
}
fn definitions(mode: ValidationMode, quality: bool, blocker: bool) -> Vec<Declaration> {
    let mut spec = fixtures::specification(mode, programmatic(quality));
    spec.binding = binding(202);
    spec.declaration_index = 1;
    spec.target = TargetDeclaration::Admission;
    spec.phase = ValidationPhase::Admission;
    let check = Declaration::new(Principal::Actor(ISSUER), spec, fixtures::limits()).unwrap();
    let delivery = Declaration::new(
        Principal::Actor(ISSUER),
        DeclarationSpec {
            binding: binding(201),
            declaration_index: 0,
            kind: ValidationKind::Receipt,
            target: TargetDeclaration::Delivery,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            program: Program::Delivery,
            ..spec
        },
        fixtures::limits(),
    )
    .unwrap();
    let mut definitions = vec![delivery, check];
    if blocker {
        definitions.push(
            Declaration::new(
                Principal::Actor(ISSUER),
                DeclarationSpec {
                    binding: binding(203),
                    declaration_index: 2,
                    mode: ValidationMode::Required,
                    program: programmatic(false),
                    ..spec
                },
                fixtures::limits(),
            )
            .unwrap(),
        );
    }
    definitions
}
fn generated(definitions: &[Declaration]) -> ClaimState {
    ClaimState::generate(
        Principal::Actor(ISSUER),
        claim::ClaimDefinition {
            binding: binding(200),
            issuer: ISSUER,
            subject: SUBJECT,
            deadline: Some(Deadline {
                timer: crate::TimerId::from_u128(500),
                generation: 1,
                at: 200,
            }),
            max_responses: 4,
            created: SessionSeq(1),
            graph: graph::Declaration::empty(),
            lineage: succession::Lineage::root(binding(200), RootCommandId::from_u128(1)).unwrap(),
            acceptance: aggregation::AcceptancePolicy::new(
                binding(200),
                ISSUER,
                &[],
                definitions,
                limits(),
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 2,
                roots: 4,
                children: 4,
            },
        },
    )
    .unwrap()
}
fn posted(definitions: &[Declaration]) -> ClaimState {
    let mut claim = generated(definitions);
    claim
        .post_owned(Principal::Actor(ISSUER), claim.binding())
        .unwrap();
    claim
}
fn ready<'a>(claim: &ClaimState, declaration: &'a Declaration) -> Evaluation<'a> {
    Evaluation::materialize_admission(Principal::Actor(ISSUER), declaration, claim, 1).unwrap()
}
fn begin<'a>(claim: &ClaimState, ready: Evaluation<'a>) -> Evaluation<'a> {
    let owner = ready.admission_owner(claim, 1).unwrap();
    ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next
}
fn report<'a>(
    claim: &ClaimState,
    evaluation: Evaluation<'a>,
    value: VerdictValue,
) -> Transition<'a> {
    let owner = evaluation.admission_report_owner(claim, 2).unwrap();
    let (report, evidence) = fixtures::report_parts(&evaluation, value);
    evaluation
        .report(
            Principal::Actor(report.attempt.evaluator),
            &evaluation.binding(),
            &owner,
            report,
            &evidence,
        )
        .unwrap()
}
fn graph(claim: &ClaimState) -> graph::Snapshot {
    graph::Snapshot::capture(
        &[claim],
        graph::Limits {
            nodes: 8,
            edges: 16,
            visits: 64,
        },
    )
    .unwrap()
}
fn receipt(claim: &mut ClaimState, aggregate: &aggregation::ClaimAggregation) -> ReceiptFence {
    let graph = graph(claim);
    let start = graph.start(ClaimId(claim.binding().object.0)).unwrap();
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(10),
        epoch: 1,
    };
    claim
        .acquire_receipt(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            fence,
            &aggregate.admission(),
            &start,
            &[],
        )
        .unwrap();
    fence
}

#[test]
fn begun_observe_reports_after_receipt_without_granting_a_new_begin() {
    let definitions = definitions(ValidationMode::Observe, false, false);
    let mut claim = posted(&definitions);
    let ready = ready(&claim, &definitions[1]);
    assert!(matches!(
        ready.admission_report_owner(&claim, 1),
        Err(ContractError::InvalidTransition)
    ));
    let mut aggregate = aggregation::ClaimAggregation::new(&claim, limits()).unwrap();
    aggregate.register(&ready).unwrap();
    let evaluation = begin(&claim, ready);
    receipt(&mut claim, &aggregate);
    assert_eq!(claim.status(), ClaimStatus::Received);
    assert!(ready.admission_owner(&claim, 2).is_err());
    assert!(
        Evaluation::materialize_admission(Principal::Actor(ISSUER), &definitions[1], &claim, 2)
            .is_err()
    );
    let expected = claim.binding();
    let owner = evaluation.admission_report_owner(&claim, 2).unwrap();
    assert_eq!(owner.parent, ParentState::Open);
    assert_eq!(owner.authority.receipt, None);
    let (reported, evidence) = fixtures::report_parts(&evaluation, VerdictValue::Fail);
    for principal in [
        Principal::Actor(ISSUER),
        Principal::Actor(SUBJECT),
        Principal::Node(EVALUATOR),
    ] {
        assert_eq!(
            evaluation
                .report(
                    principal,
                    &evaluation.binding(),
                    &owner,
                    reported,
                    &evidence
                )
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    let transition = evaluation
        .report(
            Principal::Actor(EVALUATOR),
            &evaluation.binding(),
            &owner,
            reported,
            &evidence,
        )
        .unwrap();
    assert_eq!(transition.next.state(), State::ValidationFailedNotRequired);
    assert_eq!(
        transition.result.unwrap().attempt(),
        Some(reported.attempt.index)
    );
    assert_eq!(
        transition.result.unwrap().evidence(),
        Some(reported.evidence)
    );
    assert_eq!(claim.binding(), expected);
    assert_eq!(claim.status(), ClaimStatus::Received);
    assert!(claim.terminal_cut().is_none());
}

#[test]
fn begun_required_and_observe_chains_finish_after_sibling_failure_and_scope_release() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        let definitions = definitions(mode, true, true);
        let mut claim = posted(&definitions);
        let tested = ready(&claim, &definitions[1]);
        let blocker = ready(&claim, &definitions[2]);
        let mut aggregate = aggregation::ClaimAggregation::new(&claim, limits()).unwrap();
        aggregate.register(&tested).unwrap();
        aggregate.register(&blocker).unwrap();
        let tested = begin(&claim, tested);
        let blocker = report(&claim, begin(&claim, blocker), VerdictValue::Fail);
        aggregate
            .apply_acceptance(SessionSeq(3), &[], &[blocker.result.unwrap()])
            .unwrap();
        claim
            .apply_admission(&claim.binding(), &aggregate.admission())
            .unwrap();
        assert_eq!(claim.status(), ClaimStatus::PostFailed);
        let original_cut = claim.terminal_cut();
        let original_seal = claim.local_sealed_at();
        let owner = tested.admission_report_owner(&claim, 2).unwrap();
        assert!(matches!(owner.parent, ParentState::Failed { .. }));

        let snapshot = graph(&claim);
        let release = scope::Registry::prepare_release_owner(
            &claim,
            &snapshot,
            &[],
            claim::ClaimCut {
                position: SessionSeq(4),
                cause: ContentHash([55; 32]),
            },
        )
        .unwrap();
        claim.apply_scope(&claim.binding(), release, &[]).unwrap();
        assert!(claim.released());
        assert_eq!(
            tested.admission_report_owner(&claim, 2).unwrap().parent,
            owner.parent
        );

        let mut seal = tested.admission_report_owner(&claim, 2).unwrap();
        seal.cohort = Cohort::Sealed {
            cause: ContentHash([56; 32]),
        };
        let tested = tested.record_seal(&tested.binding(), &seal).unwrap();
        let error_attempt = tested.current_attempt().unwrap();
        let error = report(&claim, tested, VerdictValue::Error);
        assert_eq!(error.result.unwrap().attempt(), Some(error_attempt.index));
        assert!(!error.result.unwrap().is_terminal());
        assert_ne!(error.next.current_attempt().unwrap(), error_attempt);
        let reporting_attempt = error.next.current_attempt().unwrap();
        let quality = report(&claim, error.next, VerdictValue::Pass);
        let result = quality.result.unwrap();
        assert_eq!(result.phase(), reporting_attempt.phase);
        assert_eq!(result.attempt(), Some(reporting_attempt.index));
        assert_eq!(quality.next.current_phase(), Phase::Quality);
        assert_ne!(quality.next.current_attempt().unwrap(), reporting_attempt);
        let reporting_attempt = quality.next.current_attempt().unwrap();
        let terminal = report(&claim, quality.next, VerdictValue::Pass);
        let result = terminal.result.unwrap();
        assert_eq!(result.phase(), reporting_attempt.phase);
        assert_eq!(result.attempt(), Some(reporting_attempt.index));
        assert_eq!(result.reporter(), Some(reporting_attempt.evaluator));
        assert!(result.is_terminal());
        assert!(terminal.next.current_attempt().is_err());
        assert!(terminal.next.admission_report_owner(&claim, 2).is_err());
        assert_eq!(claim.terminal_cut(), original_cut);
        assert_eq!(claim.local_sealed_at(), original_seal);
    }
}

#[test]
fn explicit_claim_controls_reject_even_before_a_matching_evaluation_fence_is_loaded() {
    for status in [
        ClaimStatus::Cancelled,
        ClaimStatus::Revoked,
        ClaimStatus::Expired,
        ClaimStatus::Superseded,
    ] {
        let definitions = definitions(ValidationMode::Observe, false, false);
        let mut claim = posted(&definitions);
        let evaluation = begin(&claim, ready(&claim, &definitions[1]));
        let cut = claim::ClaimCut {
            position: SessionSeq(3),
            cause: ContentHash([57; 32]),
        };
        match status {
            ClaimStatus::Cancelled => claim
                .apply(
                    &claim.binding(),
                    Principal::Actor(ISSUER),
                    claim::ClaimIntent::Cancel { cut },
                )
                .unwrap(),
            ClaimStatus::Revoked => claim
                .apply(
                    &claim.binding(),
                    Principal::Actor(ISSUER),
                    claim::ClaimIntent::Revoke { cut },
                )
                .unwrap(),
            ClaimStatus::Expired => claim
                .expire(&claim.binding(), claim.deadline().unwrap(), 200, cut)
                .unwrap(),
            ClaimStatus::Superseded => claim.supersede_verified(&claim.binding(), cut).unwrap(),
            _ => panic!("unlisted control"),
        }
        assert_eq!(claim.status(), status);
        assert_eq!(
            evaluation.admission_report_owner(&claim, 2).unwrap_err(),
            ContractError::StaleEvaluation
        );
        assert_eq!(evaluation.state(), State::Validating);
        assert!(evaluation.last_result().is_none());
    }
}

#[test]
fn every_retained_authority_fence_and_exact_deadline_reject_reporting() {
    let definitions = definitions(ValidationMode::Observe, false, false);
    let claim = posted(&definitions);
    let evaluation = begin(&claim, ready(&claim, &definitions[1]));
    let owner = evaluation.admission_report_owner(&claim, 99).unwrap();
    assert_eq!(
        evaluation.admission_report_owner(&claim, 100).unwrap_err(),
        ContractError::StaleEvaluation
    );
    for reason in [
        FenceReason::Cancellation,
        FenceReason::Revocation,
        FenceReason::Supersession,
        FenceReason::Expiry,
        FenceReason::ReceiptAdoption,
        FenceReason::Evaluation,
        FenceReason::Deadline(evaluation.deadline()),
    ] {
        let mut fenced_owner = owner;
        fenced_owner.logical_time = 100;
        fenced_owner.authority.state = AuthorityState::Fenced(AuthorityFence {
            reason,
            cause: ContentHash([58; 32]),
        });
        let fenced = evaluation
            .record_fence(&evaluation.binding(), &fenced_owner)
            .unwrap();
        assert_eq!(
            fenced.admission_report_owner(&claim, 2).unwrap_err(),
            ContractError::StaleEvaluation
        );
        assert_eq!(fenced.state(), evaluation.state());
        assert_eq!(fenced.last_result(), None);
    }
}

#[test]
fn adoption_fence_invalidates_begun_admission_without_binding_the_first_receipt() {
    let definitions = definitions(ValidationMode::Observe, false, false);
    let mut claim = posted(&definitions);
    let ready = ready(&claim, &definitions[1]);
    let mut aggregate = aggregation::ClaimAggregation::new(&claim, limits()).unwrap();
    aggregate.register(&ready).unwrap();
    let evaluation = begin(&claim, ready);
    let first = receipt(&mut claim, &aggregate);
    let mut owner = evaluation.admission_report_owner(&claim, 2).unwrap();
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::AdoptReceipt {
                previous: first,
                replacement: claim::ReceiptEntitlement {
                    holder: SUBJECT,
                    fence: ReceiptFence {
                        receipt: ReceiptId::from_u128(11),
                        epoch: 2,
                    },
                },
            },
        )
        .unwrap();
    owner.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::ReceiptAdoption,
        cause: ContentHash([59; 32]),
    });
    let fenced = evaluation
        .record_fence(&evaluation.binding(), &owner)
        .unwrap();
    assert_eq!(
        fenced.admission_report_owner(&claim, 2).unwrap_err(),
        ContractError::StaleEvaluation
    );
    assert_eq!(fenced.receipt(), None);
    assert_eq!(fenced.fence().unwrap().reason, FenceReason::ReceiptAdoption);
}

#[test]
fn reporting_pins_target_content_revision_and_absent_admission_receipt() {
    let definitions = definitions(ValidationMode::Observe, false, false);
    let claim = posted(&definitions);
    let evaluation = begin(&claim, ready(&claim, &definitions[1]));
    let mut altered = evaluation;
    altered.stored.target = Target::Admission {
        claim: Binding {
            content: ContentHash([66; 32]),
            ..claim.binding()
        },
    };
    assert_eq!(
        altered.admission_report_owner(&claim, 2).unwrap_err(),
        ContractError::ContentConflict
    );
    let mut altered = evaluation;
    altered.stored.target = Target::Admission {
        claim: Binding {
            revision: ObjectRevision(claim.binding().revision.0 + 1),
            ..claim.binding()
        },
    };
    assert_eq!(
        altered.admission_report_owner(&claim, 2).unwrap_err(),
        ContractError::StaleRevision
    );
    let mut altered = evaluation;
    altered.stored.target = Target::Admission {
        claim: binding(999),
    };
    assert_eq!(
        altered.admission_report_owner(&claim, 2).unwrap_err(),
        ContractError::WrongObject
    );
    let mut altered = evaluation;
    altered.stored.receipt = Some(ReceiptFence {
        receipt: ReceiptId::from_u128(12),
        epoch: 1,
    });
    assert_eq!(
        altered.admission_report_owner(&claim, 2).unwrap_err(),
        ContractError::StaleReceipt
    );
}

#[test]
fn native_report_frame_does_not_invent_an_additional_policy_grant() {
    let mut definitions = definitions(ValidationMode::Observe, false, false);
    let Program::Programmatic { mut check, quality } = programmatic(false) else {
        panic!("programmatic fixture")
    };
    let grant = ContentHash([67; 32]);
    check.required_policy = Some(grant);
    let mut spec = fixtures::specification(
        ValidationMode::Observe,
        Program::Programmatic { check, quality },
    );
    spec.binding = binding(202);
    spec.declaration_index = 1;
    spec.phase = ValidationPhase::Admission;
    spec.target = TargetDeclaration::Admission;
    definitions[1] = Declaration::new(Principal::Actor(ISSUER), spec, fixtures::limits()).unwrap();
    let claim = posted(&definitions);
    let ready = ready(&claim, &definitions[1]);
    assert_eq!(
        ready.admission_owner(&claim, 1).unwrap_err(),
        ContractError::InvalidPolicy
    );
    // A different trusted owner could previously have installed this grant.
    // Native reporting still cannot manufacture that absent committed fact.
    let mut original_owner = fixtures::owner_for(&ready);
    original_owner.authority.policy_evidence = Some(PolicyEvidence {
        evaluator: check.evaluator,
        definition: check.definition,
        policy: grant,
    });
    let evaluation = ready
        .begin(
            Principal::Actor(EVALUATOR),
            &ready.binding(),
            &original_owner,
        )
        .unwrap()
        .next;
    assert_eq!(
        evaluation.admission_report_owner(&claim, 2).unwrap_err(),
        ContractError::InvalidPolicy
    );
    assert!(evaluation.last_result().is_none());
}

#[test]
fn native_begin_checks_future_quality_and_agentic_policy_without_changing_explicit_owners() {
    let Program::Programmatic {
        check,
        quality: Some(mut quality),
    } = programmatic(true)
    else {
        panic!("quality fixture");
    };
    quality.required_policy = Some(ContentHash([68; 32]));
    for program in [
        Program::Programmatic {
            check,
            quality: Some(quality),
        },
        Program::Agentic { check: quality },
    ] {
        let mut definitions = definitions(ValidationMode::Observe, false, false);
        let mut spec = fixtures::specification(ValidationMode::Observe, program);
        spec.binding = binding(202);
        spec.declaration_index = 1;
        spec.phase = ValidationPhase::Admission;
        spec.target = TargetDeclaration::Admission;
        definitions[1] =
            Declaration::new(Principal::Actor(ISSUER), spec, fixtures::limits()).unwrap();
        let claim = posted(&definitions);
        let ready = ready(&claim, &definitions[1]);
        assert_eq!(
            ready.admission_owner(&claim, 1).unwrap_err(),
            ContractError::InvalidPolicy
        );
        assert!(!ready.has_begun());
        assert!(ready.last_result().is_none());

        // Explicit checked owner frames remain able to supply actual installed
        // grants. Native frame helpers never synthesize those facts themselves.
        let owner = fixtures::owner_for(&ready);
        let mut evaluation = ready
            .begin(
                Principal::Actor(owner.authority.evaluator),
                &ready.binding(),
                &owner,
            )
            .unwrap()
            .next;
        if matches!(program, Program::Programmatic { .. }) {
            assert!(evaluation.admission_report_owner(&claim, 2).is_ok());
            evaluation = report(&claim, evaluation, VerdictValue::Pass).next;
            assert_eq!(evaluation.current_phase(), Phase::Quality);
        }
        assert_eq!(
            evaluation.admission_report_owner(&claim, 2).unwrap_err(),
            ContractError::InvalidPolicy
        );
        let owner = fixtures::owner_for(&evaluation);
        let (report, evidence) = fixtures::report_parts(&evaluation, VerdictValue::Pass);
        let terminal = evaluation
            .report(
                Principal::Actor(report.attempt.evaluator),
                &evaluation.binding(),
                &owner,
                report,
                &evidence,
            )
            .unwrap();
        assert_eq!(terminal.next.state(), State::Validated);
        assert!(terminal.result.unwrap().is_terminal());
    }
}
