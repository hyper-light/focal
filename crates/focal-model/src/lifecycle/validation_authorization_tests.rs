use super::tests::{
    EVALUATOR, ISSUER, binding, declaration, limits, owner_for, programmatic, ready, report_parts,
    report_value, specification,
};
use super::*;
use crate::ObjectRevision;

fn begun(declaration: &Declaration) -> Evaluation<'_> {
    let ready = ready(declaration);
    ready
        .begin(
            Principal::Actor(ready.evaluator().unwrap()),
            &ready.binding(),
            &owner_for(&ready),
        )
        .unwrap()
        .next
}

fn complete_definition(duplicate_quality_schemas: bool) -> Declaration {
    let Program::Programmatic {
        check,
        quality: Some(quality),
    } = programmatic(true)
    else {
        panic!("fixture must include quality");
    };
    let fallback = HandlerRef {
        id: ValidatorId::from_u128(91),
        version: ContentHash([91; 32]),
        agentic: true,
    };
    let first = quality.handlers[0];
    let steps = [
        first,
        HandlerPolicy {
            handler: &fallback,
            attempts: 1,
            proof_schema: if duplicate_quality_schemas {
                first.proof_schema
            } else {
                ContentHash([27; 32])
            },
            diagnostic_schema: if duplicate_quality_schemas {
                first.diagnostic_schema
            } else {
                ContentHash([28; 32])
            },
        },
    ];
    Declaration::new(
        Principal::Actor(ISSUER),
        specification(
            ValidationMode::Required,
            Program::Programmatic {
                check,
                quality: Some(PhasePolicy {
                    handlers: &steps,
                    ..quality
                }),
            },
        ),
        limits(),
    )
    .unwrap()
}

#[test]
fn evidence_schema_iterator_covers_every_fallback_and_quality_without_deduplicating() {
    let full = complete_definition(false);
    assert_eq!(
        full.evidence_schemas().collect::<Vec<_>>(),
        (21..=28)
            .map(|byte| ContentHash([byte; 32]))
            .collect::<Vec<_>>()
    );
    assert_eq!(full.evidence_schemas().count(), 8);
    let repeated = complete_definition(true);
    assert_eq!(
        repeated.evidence_schemas().collect::<Vec<_>>(),
        [21, 22, 23, 24, 25, 26, 25, 26].map(|byte| ContentHash([byte; 32]))
    );
    let Program::Programmatic {
        quality: Some(quality),
        ..
    } = programmatic(true)
    else {
        panic!("fixture must include quality");
    };
    let agentic = declaration(ValidationMode::Observe, Program::Agentic { check: quality });
    assert_eq!(
        agentic.evidence_schemas().collect::<Vec<_>>(),
        [ContentHash([25; 32]), ContentHash([26; 32])]
    );
    let delivery = Declaration::new(
        Principal::Actor(ISSUER),
        DeclarationSpec {
            kind: ValidationKind::Receipt,
            target: TargetDeclaration::Delivery,
            ..specification(ValidationMode::Required, Program::Delivery)
        },
        limits(),
    )
    .unwrap();
    assert_eq!(delivery.evidence_schemas().next(), None);
}

#[test]
fn report_preflight_selects_exact_schema_through_retry_fallback_and_quality() {
    let definition = complete_definition(false);
    let mut active = begun(&definition);
    for (index, phase, proof, diagnostic) in [
        (0, Phase::Programmatic, 21, 22),
        (1, Phase::Programmatic, 21, 22),
        (2, Phase::Programmatic, 23, 24),
        (3, Phase::Quality, 25, 26),
        (4, Phase::Quality, 25, 26),
        (5, Phase::Quality, 27, 28),
    ] {
        let before = active.into_state();
        for (value, schema) in [
            (VerdictValue::Pass, proof),
            (VerdictValue::Fail, proof),
            (VerdictValue::Incomplete, diagnostic),
            (VerdictValue::Error, diagnostic),
        ] {
            let (report, _) = report_parts(&active, value);
            let authorization = active
                .authorize_report(
                    Principal::Actor(report.attempt.evaluator),
                    &active.binding(),
                    &owner_for(&active),
                    report,
                )
                .unwrap();
            assert_eq!(authorization.attempt(), active.current_attempt().unwrap());
            assert_eq!(authorization.attempt().index, index);
            assert_eq!(authorization.attempt().phase, phase);
            assert_eq!(authorization.schema(), ContentHash([schema; 32]));
            assert_eq!(active.into_state(), before);
        }
        active = report_value(
            &active,
            if index == 2 || index == 5 {
                VerdictValue::Pass
            } else {
                VerdictValue::Error
            },
        )
        .next;
    }
    assert_eq!(active.state(), State::Validated);
    let Program::Programmatic {
        quality: Some(quality),
        ..
    } = programmatic(true)
    else {
        panic!("fixture must include quality");
    };
    let agentic = declaration(
        ValidationMode::Required,
        Program::Agentic { check: quality },
    );
    let active = begun(&agentic);
    let (report, _) = report_parts(&active, VerdictValue::Error);
    let authorization = active
        .authorize_report(
            Principal::Actor(report.attempt.evaluator),
            &active.binding(),
            &owner_for(&active),
            report,
        )
        .unwrap();
    assert_eq!(authorization.attempt().phase, Phase::Quality);
    assert_eq!(authorization.schema(), ContentHash([26; 32]));
}

fn same_refusal(
    evaluation: &Evaluation<'_>,
    principal: Principal,
    expected: Binding,
    owner: OwnerState,
    report: Report,
    mut evidence: EvidenceFacts,
    error: ContractError,
) {
    // Evidence would fail later too. Authority failures must retain their
    // original precedence and never depend on custody being available first.
    evidence.custody_revision = None;
    let before = evaluation.into_state();
    assert_eq!(
        evaluation.authorize_report(principal, &expected, &owner, report),
        Err(error)
    );
    assert_eq!(
        evaluation
            .report(principal, &expected, &owner, report, &evidence)
            .err(),
        Some(error)
    );
    assert_eq!(evaluation.into_state(), before);
}

#[test]
fn authority_preflight_and_report_keep_exact_frame_actor_attempt_and_state_error_order() {
    let definition = declaration(ValidationMode::Required, programmatic(false));
    let active = begun(&definition);
    let (report, evidence) = report_parts(&active, VerdictValue::Pass);
    type Change = fn(&mut OwnerState);
    let cases: [(Change, ContractError); 9] = [
        (
            |o: &mut OwnerState| o.evaluation.revision = ObjectRevision(99),
            ContractError::StaleRevision,
        ),
        (
            |o: &mut OwnerState| o.evaluation.content = ContentHash([99; 32]),
            ContractError::ContentConflict,
        ),
        (
            |o: &mut OwnerState| {
                o.target = Target::Admission {
                    claim: binding(200),
                }
            },
            ContractError::InvalidTarget,
        ),
        (
            |o: &mut OwnerState| o.authority.receipt = None,
            ContractError::StaleReceipt,
        ),
        (
            |o: &mut OwnerState| o.authority.generation += 1,
            ContractError::StaleEvaluation,
        ),
        (
            |o: &mut OwnerState| o.authority.evaluator = ISSUER,
            ContractError::StaleEvaluation,
        ),
        (
            |o: &mut OwnerState| o.authority.definition = ContentHash([99; 32]),
            ContractError::StaleEvaluation,
        ),
        (
            |o: &mut OwnerState| o.logical_time = o.authority.deadline.at,
            ContractError::StaleEvaluation,
        ),
        (
            |o: &mut OwnerState| o.parent = ParentState::Cancelled,
            ContractError::StaleEvaluation,
        ),
    ];
    for (change, error) in cases {
        let mut owner = owner_for(&active);
        change(&mut owner);
        same_refusal(
            &active,
            Principal::Actor(ISSUER),
            active.binding(),
            owner,
            report,
            evidence,
            error,
        );
    }
    same_refusal(
        &active,
        Principal::Actor(ISSUER),
        active.binding(),
        owner_for(&active),
        report,
        evidence,
        ContractError::WrongActor,
    );
    same_refusal(
        &active,
        Principal::Node(EVALUATOR),
        active.binding(),
        owner_for(&active),
        report,
        evidence,
        ContractError::WrongActor,
    );
    same_refusal(
        &active,
        Principal::Actor(EVALUATOR),
        active.binding().next().unwrap(),
        owner_for(&active),
        report,
        evidence,
        ContractError::StaleRevision,
    );
    let mut wrong = report;
    wrong.generation += 1;
    same_refusal(
        &active,
        Principal::Actor(EVALUATOR),
        active.binding(),
        owner_for(&active),
        wrong,
        evidence,
        ContractError::StaleEvaluation,
    );
    type AttemptChange = fn(&mut Attempt);
    let changes: [AttemptChange; 6] = [
        |a| a.phase = Phase::Quality,
        |a| a.index += 1,
        |a| a.handler = ValidatorId::from_u128(999),
        |a| a.version = ContentHash([99; 32]),
        |a| a.evaluator = ISSUER,
        |a| a.definition = ContentHash([99; 32]),
    ];
    for change in changes {
        let mut wrong = report;
        change(&mut wrong.attempt);
        same_refusal(
            &active,
            Principal::Actor(EVALUATOR),
            active.binding(),
            owner_for(&active),
            wrong,
            evidence,
            ContractError::StaleEvaluation,
        );
    }
    let ready = ready(&definition);
    same_refusal(
        &ready,
        Principal::Actor(ISSUER),
        ready.binding(),
        owner_for(&ready),
        report,
        evidence,
        ContractError::InvalidTransition,
    );
    let terminal = report_value(&active, VerdictValue::Fail).next;
    same_refusal(
        &terminal,
        Principal::Actor(ISSUER),
        terminal.binding(),
        owner_for(&terminal),
        report,
        evidence,
        ContractError::InvalidTransition,
    );
}

#[test]
fn authorization_is_not_custody_and_later_authority_or_attempt_changes_are_rechecked() {
    let definition = declaration(ValidationMode::Required, programmatic(false));
    let active = begun(&definition);
    let (report, mut evidence) = report_parts(&active, VerdictValue::Error);
    let mut owner = owner_for(&active);
    owner.parent = ParentState::Failed {
        cause: ContentHash([66; 32]),
    };
    owner.cohort = Cohort::Sealed {
        cause: ContentHash([67; 32]),
    };
    let principal = Principal::Actor(EVALUATOR);
    let authorization = active
        .authorize_report(principal, &active.binding(), &owner, report)
        .unwrap();
    assert_eq!(authorization.schema(), evidence.schema);
    evidence.custody_revision = None;
    assert_eq!(
        active
            .report(principal, &active.binding(), &owner, report, &evidence)
            .err(),
        Some(ContractError::MissingEvidence)
    );
    evidence.custody_revision = Some(1);
    evidence.schema = ContentHash([99; 32]);
    assert_eq!(
        active
            .report(principal, &active.binding(), &owner, report, &evidence)
            .err(),
        Some(ContractError::MissingEvidence)
    );
    evidence.schema = authorization.schema();
    owner.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::Evaluation,
        cause: ContentHash([68; 32]),
    });
    same_refusal(
        &active,
        principal,
        active.binding(),
        owner,
        report,
        evidence,
        ContractError::StaleEvaluation,
    );
    owner.authority.state = AuthorityState::Live;
    let next = active
        .report(principal, &active.binding(), &owner, report, &evidence)
        .unwrap()
        .next;
    same_refusal(
        &next,
        principal,
        next.binding(),
        owner_for(&next),
        report,
        evidence,
        ContractError::StaleEvaluation,
    );
}

#[test]
fn preflight_requires_the_exact_installed_policy_grant_before_custody() {
    let Program::Programmatic {
        quality: Some(quality),
        ..
    } = programmatic(true)
    else {
        panic!("fixture must include quality");
    };
    let definition = declaration(
        ValidationMode::Required,
        Program::Agentic {
            check: PhasePolicy {
                required_policy: Some(ContentHash([71; 32])),
                ..quality
            },
        },
    );
    let active = begun(&definition);
    let (report, evidence) = report_parts(&active, VerdictValue::Pass);
    let mut owner = owner_for(&active);
    owner.authority.policy_evidence = None;
    same_refusal(
        &active,
        Principal::Actor(ISSUER),
        active.binding(),
        owner,
        report,
        evidence,
        ContractError::InvalidPolicy,
    );
    let owner = owner_for(&active);
    let authorization = active
        .authorize_report(
            Principal::Actor(report.attempt.evaluator),
            &active.binding(),
            &owner,
            report,
        )
        .unwrap();
    assert_eq!(authorization.schema(), evidence.schema);
}
