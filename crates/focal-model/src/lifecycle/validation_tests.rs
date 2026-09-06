use super::*;
use crate::{ObjectId, ObjectRevision, SessionId, TenantId, TimerId};

pub(crate) const ISSUER: ParticipantId = ParticipantId::from_u128(1);
pub(crate) const EVALUATOR: ParticipantId = ParticipantId::from_u128(2);
pub(crate) const QUALITY_EVALUATOR: ParticipantId = ParticipantId::from_u128(3);
const PROGRAM_HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId::from_u128(10),
    version: ContentHash([10; 32]),
    agentic: false,
};
const FALLBACK_HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId::from_u128(11),
    version: ContentHash([11; 32]),
    agentic: false,
};
const AGENTIC_HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId::from_u128(12),
    version: ContentHash([12; 32]),
    agentic: true,
};
const PROGRAM_STEPS: [HandlerPolicy<'static>; 2] = [
    HandlerPolicy {
        handler: &PROGRAM_HANDLER,
        attempts: 2,
        proof_schema: ContentHash([21; 32]),
        diagnostic_schema: ContentHash([22; 32]),
    },
    HandlerPolicy {
        handler: &FALLBACK_HANDLER,
        attempts: 1,
        proof_schema: ContentHash([23; 32]),
        diagnostic_schema: ContentHash([24; 32]),
    },
];
const AGENTIC_STEPS: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &AGENTIC_HANDLER,
    attempts: 2,
    proof_schema: ContentHash([25; 32]),
    diagnostic_schema: ContentHash([26; 32]),
}];

pub(crate) fn binding(id: u128) -> Binding {
    Binding {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        object: ObjectId::from_u128(id),
        content: ContentHash([31; 32]),
        revision: ObjectRevision(1),
    }
}

fn quality_policy() -> PhasePolicy<'static> {
    PhasePolicy {
        evaluator: QUALITY_EVALUATOR,
        definition: ContentHash([41; 32]),
        handlers: &AGENTIC_STEPS,
        required_policy: None,
    }
}

pub(crate) fn programmatic(quality: bool) -> Program<'static> {
    Program::Programmatic {
        check: PhasePolicy {
            evaluator: EVALUATOR,
            definition: ContentHash([40; 32]),
            handlers: &PROGRAM_STEPS,
            required_policy: None,
        },
        quality: quality.then_some(quality_policy()),
    }
}

pub(crate) fn specification(mode: ValidationMode, program: Program<'_>) -> DeclarationSpec<'_> {
    DeclarationSpec {
        binding: binding(100),
        claim: ClaimId::from_u128(200),
        issuer: ISSUER,
        declaration_index: 4,
        kind: ValidationKind::Inspection,
        phase: ValidationPhase::WholeWork,
        mode,
        target: TargetDeclaration::WholeWorkSlot {
            index: 0,
            name: "output",
        },
        program,
        deadline: Deadline {
            timer: TimerId::from_u128(20),
            generation: 1,
            at: 100,
        },
    }
}

pub(crate) fn limits() -> Limits {
    Limits {
        handlers: 4,
        attempts: 8,
        slot_bytes: 64,
    }
}

pub(crate) fn declaration(mode: ValidationMode, program: Program<'static>) -> Declaration {
    Declaration::new(
        Principal::Actor(ISSUER),
        specification(mode, program),
        limits(),
    )
    .unwrap()
}

pub(crate) fn owner_for(evaluation: &Evaluation<'_>) -> OwnerState {
    let authority = if evaluation.current_phase() == Phase::Delivery {
        Authority {
            evaluator: evaluation.issuer(),
            definition: evaluation.declaration.binding().content,
            generation: evaluation.generation(),
            receipt: evaluation.receipt(),
            deadline: evaluation.deadline(),
            policy_evidence: None,
            state: AuthorityState::Live,
        }
    } else {
        let policy = evaluation
            .declaration
            .policy(evaluation.current_phase())
            .unwrap();
        Authority {
            evaluator: policy.evaluator,
            definition: policy.definition,
            generation: evaluation.generation(),
            receipt: evaluation.receipt(),
            deadline: evaluation.deadline(),
            policy_evidence: policy.required_policy.map(|policy_hash| PolicyEvidence {
                evaluator: policy.evaluator,
                definition: policy.definition,
                policy: policy_hash,
            }),
            state: AuthorityState::Live,
        }
    };
    let readiness = match evaluation.target() {
        Target::Admission { .. } => Readiness::AdmissionPosted,
        Target::Increment { .. } => Readiness::IncrementEligible,
        target => Readiness::ResponseReceived(ResponseReadiness {
            target,
            claim: evaluation.claim(),
            receipt: evaluation.receipt().unwrap(),
        }),
    };
    OwnerState {
        evaluation: evaluation.binding(),
        target: evaluation.target(),
        parent: ParentState::Open,
        readiness,
        cohort: Cohort::Open,
        authority,
        logical_time: 1,
    }
}

pub(crate) fn ready<'a>(declaration: &'a Declaration) -> Evaluation<'a> {
    let target = match declaration.target() {
        TargetDeclaration::WholeWorkSlot { index, .. } => Target::Artifact {
            response: binding(300),
            slot: index,
            artifact: binding(400),
        },
        TargetDeclaration::Delivery => Target::Delivery {
            response: binding(300),
        },
        TargetDeclaration::Admission => Target::Admission {
            claim: binding(200),
        },
        TargetDeclaration::Increment => Target::Increment {
            claim: binding(200),
            artifact: binding(400),
        },
    };
    Evaluation::materialize(
        Principal::Actor(ISSUER),
        declaration,
        Materialization {
            binding: declaration.binding(),
            target,
            slot_name: match declaration.target() {
                TargetDeclaration::WholeWorkSlot { name, .. } => Some(name),
                _ => None,
            },
            generation: 7,
            receipt: if declaration.target() == TargetDeclaration::Admission {
                None
            } else {
                Some(ReceiptFence {
                    receipt: crate::ReceiptId::from_u128(10),
                    epoch: 3,
                })
            },
        },
    )
    .unwrap()
}

pub(crate) fn report_parts(
    evaluation: &Evaluation<'_>,
    value: VerdictValue,
) -> (Report, EvidenceFacts) {
    let attempt = evaluation.current_attempt().unwrap();
    let step = &evaluation
        .declaration
        .policy(attempt.phase)
        .unwrap()
        .handlers[evaluation.handler];
    let evidence_binding = binding(1000 + u128::from(attempt.index));
    let reference = ArtifactRef {
        id: ArtifactId(evidence_binding.object.0),
        hash: evidence_binding.content,
    };
    let report = Report {
        generation: evaluation.generation(),
        attempt,
        value,
        evidence: reference,
    };
    let (kind, schema) = match value {
        VerdictValue::Pass | VerdictValue::Fail => (EvidenceKind::Proof, step.proof_schema),
        VerdictValue::Incomplete | VerdictValue::Error => {
            (EvidenceKind::Diagnostic, step.diagnostic_schema)
        }
    };
    let facts = EvidenceFacts {
        binding: evidence_binding,
        claim: evaluation.claim(),
        validation: evaluation.validation(),
        target: evaluation.target(),
        generation: evaluation.generation(),
        attempt,
        producer: attempt.evaluator,
        value,
        kind,
        schema,
        custody_revision: Some(1),
    };
    (report, facts)
}

fn begun<'a>(declaration: &'a Declaration) -> Evaluation<'a> {
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

pub(crate) fn report_value<'a>(evaluation: &Evaluation<'a>, value: VerdictValue) -> Transition<'a> {
    let (report, evidence) = report_parts(evaluation, value);
    evaluation
        .report(
            Principal::Actor(report.attempt.evaluator),
            &evaluation.binding(),
            &owner_for(evaluation),
            report,
            &evidence,
        )
        .unwrap()
}

/// Test-only fact construction for aggregation permutations. Production callers
/// can obtain the private capability only through checked validation transitions.
pub(in crate::lifecycle) struct AcceptedFixture {
    pub definition: DefinitionStamp,
    pub binding: Binding,
    pub claim: ClaimId,
    pub target: Target,
    pub index: u32,
    pub mode: ValidationMode,
    pub verdict: VerdictValue,
    pub phase: Phase,
    pub generation: u64,
    pub receipt: Option<ReceiptFence>,
    pub attempt: Option<u32>,
    pub evidence: Option<ArtifactRef>,
    pub programmatic: Option<ArtifactRef>,
    pub terminal: bool,
}

pub(in crate::lifecycle) fn accepted_fixture(input: AcceptedFixture) -> AcceptedResult {
    let resulting_state = if !input.terminal {
        if input.phase == Phase::Quality {
            State::ValidatingQualityBar
        } else {
            State::Validating
        }
    } else {
        match (input.verdict, input.phase, input.mode) {
            (VerdictValue::Pass, _, _) => State::Validated,
            (VerdictValue::Incomplete, _, _) => State::ValidationIncomplete,
            (VerdictValue::Error, _, ValidationMode::Required) => State::Errored,
            (VerdictValue::Error, _, ValidationMode::Observe) => State::ErroredNotRequired,
            (VerdictValue::Fail, Phase::Quality, ValidationMode::Required) => {
                State::QualityBarValidationFailed
            }
            (VerdictValue::Fail, Phase::Quality, ValidationMode::Observe) => {
                State::QualityBarValidationFailedNotRequired
            }
            (VerdictValue::Fail, _, ValidationMode::Required) => State::ValidationFailed,
            (VerdictValue::Fail, _, ValidationMode::Observe) => State::ValidationFailedNotRequired,
        }
    };
    AcceptedResult {
        definition: input.definition,
        binding: input.binding,
        ledger: input.binding.ledger,
        claim: input.claim,
        target: input.target,
        validation: ValidationId(input.binding.object.0),
        declaration_index: input.index,
        mode: input.mode,
        verdict: input.verdict,
        phase: input.phase,
        attempt: input.attempt,
        generation: input.generation,
        receipt: input.receipt,
        evidence: input.evidence,
        programmatic_evidence: input.programmatic,
        reporter: match input.phase {
            Phase::Programmatic => Some(EVALUATOR),
            Phase::Quality => Some(QUALITY_EVALUATOR),
            Phase::Delivery | Phase::MissingTarget => None,
        },
        resulting_state,
    }
}

#[test]
fn declaration_rejects_role_target_execution_and_resource_mismatches() {
    let original = specification(ValidationMode::Required, programmatic(true));
    for principal in [Principal::Actor(EVALUATOR), Principal::Node(ISSUER)] {
        assert_eq!(
            Declaration::new(principal, original, limits()).unwrap_err(),
            ContractError::WrongActor
        );
    }
    for (changed, error) in [
        (
            DeclarationSpec {
                phase: ValidationPhase::Increment,
                ..original
            },
            ContractError::InvalidTarget,
        ),
        (
            DeclarationSpec {
                target: TargetDeclaration::WholeWorkSlot { index: 0, name: "" },
                ..original
            },
            ContractError::InvalidTarget,
        ),
        (
            DeclarationSpec {
                kind: ValidationKind::Receipt,
                ..original
            },
            ContractError::InvalidPolicy,
        ),
        (
            DeclarationSpec {
                program: Program::Delivery,
                ..original
            },
            ContractError::InvalidPolicy,
        ),
        (
            DeclarationSpec {
                program: Program::Agentic {
                    check: PhasePolicy {
                        evaluator: EVALUATOR,
                        definition: ContentHash([0; 32]),
                        handlers: &PROGRAM_STEPS,
                        required_policy: None,
                    },
                },
                ..original
            },
            ContractError::InvalidPolicy,
        ),
    ] {
        assert_eq!(
            Declaration::new(Principal::Actor(ISSUER), changed, limits()).unwrap_err(),
            error
        );
    }
    assert_eq!(
        Declaration::new(
            Principal::Actor(ISSUER),
            original,
            Limits {
                attempts: 4,
                ..limits()
            }
        )
        .unwrap_err(),
        ContractError::Capacity
    );
    assert_eq!(
        Declaration::new(
            Principal::Actor(ISSUER),
            original,
            Limits {
                handlers: 1,
                ..limits()
            }
        )
        .unwrap_err(),
        ContractError::Capacity
    );
    let no_attempts = [HandlerPolicy {
        attempts: 0,
        ..PROGRAM_STEPS[0]
    }];
    let policy = PhasePolicy {
        handlers: &no_attempts,
        evaluator: EVALUATOR,
        definition: ContentHash([1; 32]),
        required_policy: None,
    };
    let spec = DeclarationSpec {
        program: Program::Programmatic {
            check: policy,
            quality: None,
        },
        ..original
    };
    assert_eq!(
        Declaration::new(Principal::Actor(ISSUER), spec, limits()).unwrap_err(),
        ContractError::InvalidPolicy
    );
}

#[test]
fn materialization_keeps_exact_slot_artifact_and_generation_bindings() {
    let declaration = declaration(ValidationMode::Required, programmatic(false));
    let original = Materialization {
        binding: declaration.binding(),
        target: Target::Artifact {
            response: binding(300),
            slot: 0,
            artifact: binding(400),
        },
        slot_name: Some("output"),
        generation: 1,
        receipt: Some(ReceiptFence {
            receipt: crate::ReceiptId::from_u128(10),
            epoch: 1,
        }),
    };
    for (changed, error) in [
        (
            Materialization {
                slot_name: Some("same-schema-other-slot"),
                ..original
            },
            ContractError::InvalidTarget,
        ),
        (
            Materialization {
                target: Target::Artifact {
                    response: binding(300),
                    slot: 1,
                    artifact: binding(400),
                },
                ..original
            },
            ContractError::InvalidTarget,
        ),
        (
            Materialization {
                target: Target::Admission {
                    claim: binding(200),
                },
                ..original
            },
            ContractError::InvalidTarget,
        ),
        (
            Materialization {
                generation: 0,
                ..original
            },
            ContractError::StaleEvaluation,
        ),
        (
            Materialization {
                receipt: None,
                ..original
            },
            ContractError::StaleReceipt,
        ),
    ] {
        assert_eq!(
            Evaluation::materialize(Principal::Actor(ISSUER), &declaration, changed).unwrap_err(),
            error
        );
    }
    assert_eq!(
        Evaluation::materialize(Principal::Node(ISSUER), &declaration, original).unwrap_err(),
        ContractError::WrongActor
    );
}

#[test]
fn begin_and_report_reject_forbidden_writers_without_mutation() {
    for program in [
        programmatic(false),
        Program::Agentic {
            check: quality_policy(),
        },
    ] {
        let declaration = declaration(ValidationMode::Required, program);
        let ready = ready(&declaration);
        let evaluator = ready.evaluator().unwrap();
        for principal in [
            Principal::Actor(ISSUER),
            Principal::Actor(ParticipantId::from_u128(99)),
            Principal::Node(evaluator),
        ] {
            assert_eq!(
                ready
                    .begin(principal, &ready.binding(), &owner_for(&ready))
                    .unwrap_err(),
                ContractError::WrongActor
            );
        }
        assert_eq!(ready.state(), State::Ready);
        let active = ready
            .begin(
                Principal::Actor(evaluator),
                &ready.binding(),
                &owner_for(&ready),
            )
            .unwrap()
            .next;
        let (report, evidence) = report_parts(&active, VerdictValue::Pass);
        for principal in [
            Principal::Actor(ISSUER),
            Principal::Actor(ParticipantId::from_u128(99)),
            Principal::Node(evaluator),
        ] {
            assert_eq!(
                active
                    .report(
                        principal,
                        &active.binding(),
                        &owner_for(&active),
                        report,
                        &evidence
                    )
                    .unwrap_err(),
                ContractError::WrongActor
            );
        }
        assert!(active.last_result().is_none());
    }
}

#[test]
fn direct_agentic_and_programmatic_quality_paths_preserve_real_proof() {
    let direct = declaration(
        ValidationMode::Required,
        Program::Agentic {
            check: quality_policy(),
        },
    );
    let active = begun(&direct);
    assert_eq!(active.state(), State::ValidatingQualityBar);
    let passed = report_value(&active, VerdictValue::Pass);
    let result = passed.result.unwrap();
    assert_eq!(result.programmatic_evidence(), None);
    assert_eq!(result.reporter(), Some(QUALITY_EVALUATOR));
    assert!(result.is_terminal());
    let both = declaration(ValidationMode::Required, programmatic(true));
    let active = begun(&both);
    assert_eq!(active.state(), State::Validating);
    let first = report_value(&active, VerdictValue::Pass);
    assert!(!first.result.unwrap().is_terminal());
    assert_eq!(first.next.state(), State::ValidatingQualityBar);
    assert_eq!(
        first.next.current_attempt().unwrap().evaluator,
        QUALITY_EVALUATOR
    );
    let final_result = report_value(&first.next, VerdictValue::Pass)
        .result
        .unwrap();
    assert_eq!(
        final_result.programmatic_evidence(),
        first.result.unwrap().evidence()
    );
    assert_ne!(
        final_result.programmatic_evidence(),
        final_result.evidence()
    );
    assert!(final_result.is_terminal());
}

#[test]
fn conclusive_fail_incomplete_and_exhausted_error_cover_all_terminal_states() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for (program, phase) in [
            (programmatic(false), Phase::Programmatic),
            (
                Program::Agentic {
                    check: quality_policy(),
                },
                Phase::Quality,
            ),
        ] {
            let declaration = declaration(mode, program);
            let active = begun(&declaration);
            let failed = report_value(&active, VerdictValue::Fail);
            let expected = match (phase, mode) {
                (Phase::Programmatic, ValidationMode::Required) => State::ValidationFailed,
                (Phase::Programmatic, ValidationMode::Observe) => {
                    State::ValidationFailedNotRequired
                }
                (Phase::Quality, ValidationMode::Required) => State::QualityBarValidationFailed,
                (Phase::Quality, ValidationMode::Observe) => {
                    State::QualityBarValidationFailedNotRequired
                }
                _ => unreachable!(),
            };
            assert_eq!(failed.next.state(), expected);
            assert!(failed.result.unwrap().is_terminal());
            assert_eq!(failed.result.unwrap().attempt(), Some(0));
            let incomplete = report_value(&active, VerdictValue::Incomplete);
            assert_eq!(incomplete.next.state(), State::ValidationIncomplete);
            assert_eq!(incomplete.result.unwrap().mode(), mode);
            let mut errors = active;
            let mut count = 0;
            while !errors.state().is_terminal() {
                let transition = report_value(&errors, VerdictValue::Error);
                errors = transition.next;
                count += 1;
                assert!(count <= declaration.attempt_bound());
            }
            assert_eq!(
                errors.state(),
                if mode == ValidationMode::Required {
                    State::Errored
                } else {
                    State::ErroredNotRequired
                }
            );
            assert_eq!(count, declaration.attempt_bound());
        }
    }
}

#[test]
fn only_error_advances_bounded_attempts_and_fallback_handlers() {
    let declaration = declaration(ValidationMode::Required, programmatic(true));
    let first = begun(&declaration);
    let second = report_value(&first, VerdictValue::Error).next;
    assert_eq!(
        second.current_attempt().unwrap().handler,
        PROGRAM_HANDLER.id
    );
    assert_eq!(second.current_attempt().unwrap().index, 1);
    let fallback = report_value(&second, VerdictValue::Error).next;
    assert_eq!(
        fallback.current_attempt().unwrap().handler,
        FALLBACK_HANDLER.id
    );
    assert_eq!(fallback.current_attempt().unwrap().index, 2);
    let quality = report_value(&fallback, VerdictValue::Pass).next;
    assert_eq!(quality.current_attempt().unwrap().phase, Phase::Quality);
    assert_eq!(quality.current_attempt().unwrap().index, 3);
    let final_attempt = report_value(&quality, VerdictValue::Error).next;
    assert_eq!(final_attempt.current_attempt().unwrap().index, 4);
    assert_eq!(
        report_value(&final_attempt, VerdictValue::Error)
            .next
            .state(),
        State::Errored
    );
    let failed = report_value(&first, VerdictValue::Fail).next;
    assert_eq!(failed.state(), State::ValidationFailed);
    assert_eq!(
        failed.current_attempt().unwrap_err(),
        ContractError::InvalidTransition
    );
}

#[test]
fn terminal_replacements_rebegins_and_stale_attempts_are_rejected() {
    let declaration = declaration(ValidationMode::Required, programmatic(false));
    let active = begun(&declaration);
    let (original, evidence) = report_parts(&active, VerdictValue::Pass);
    let passed = report_value(&active, VerdictValue::Pass).next;
    assert_eq!(
        passed
            .report(
                Principal::Actor(EVALUATOR),
                &passed.binding(),
                &owner_for(&passed),
                original,
                &evidence
            )
            .unwrap_err(),
        ContractError::InvalidTransition
    );
    assert_eq!(
        passed
            .begin(
                Principal::Actor(EVALUATOR),
                &passed.binding(),
                &owner_for(&passed)
            )
            .unwrap_err(),
        ContractError::InvalidTransition
    );
    let retry = report_value(&active, VerdictValue::Error).next;
    assert_eq!(
        retry
            .report(
                Principal::Actor(EVALUATOR),
                &retry.binding(),
                &owner_for(&retry),
                original,
                &evidence
            )
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    assert_eq!(
        active
            .begin(
                Principal::Actor(EVALUATOR),
                &active.binding(),
                &owner_for(&active)
            )
            .unwrap_err(),
        ContractError::InvalidTransition
    );
}

#[test]
fn every_owner_fence_is_checked_for_begin_and_report() {
    type Change = fn(&mut OwnerState);
    let cases: [(Change, ContractError); 9] = [
        (
            |owner| owner.evaluation.revision = ObjectRevision(99),
            ContractError::StaleRevision,
        ),
        (
            |owner| owner.evaluation.content = ContentHash([99; 32]),
            ContractError::ContentConflict,
        ),
        (
            |owner| {
                owner.target = Target::Artifact {
                    response: binding(300),
                    slot: 0,
                    artifact: binding(401),
                }
            },
            ContractError::InvalidTarget,
        ),
        (
            |owner| {
                owner.authority.receipt = Some(ReceiptFence {
                    receipt: crate::ReceiptId::from_u128(10),
                    epoch: 4,
                })
            },
            ContractError::StaleReceipt,
        ),
        (
            |owner| owner.authority.generation = 8,
            ContractError::StaleEvaluation,
        ),
        (
            |owner| owner.authority.evaluator = ISSUER,
            ContractError::StaleEvaluation,
        ),
        (
            |owner| owner.authority.definition = ContentHash([99; 32]),
            ContractError::StaleEvaluation,
        ),
        (
            |owner| owner.authority.deadline.generation = 2,
            ContractError::StaleEvaluation,
        ),
        (
            |owner| owner.logical_time = 100,
            ContractError::StaleEvaluation,
        ),
    ];
    let declaration = declaration(ValidationMode::Required, programmatic(false));
    let ready = ready(&declaration);
    let active = begun(&declaration);
    let (report, evidence) = report_parts(&active, VerdictValue::Pass);
    for (change, error) in cases {
        let mut owner = owner_for(&ready);
        change(&mut owner);
        assert_eq!(
            ready
                .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
                .unwrap_err(),
            error
        );
        let mut owner = owner_for(&active);
        change(&mut owner);
        assert_eq!(
            active
                .report(
                    Principal::Actor(EVALUATOR),
                    &active.binding(),
                    &owner,
                    report,
                    &evidence
                )
                .unwrap_err(),
            error
        );
    }
    for parent in [
        ParentState::Cancelled,
        ParentState::Revoked,
        ParentState::Superseded,
        ParentState::Expired,
    ] {
        let mut owner = owner_for(&active);
        owner.parent = parent;
        assert_eq!(
            active
                .report(
                    Principal::Actor(EVALUATOR),
                    &active.binding(),
                    &owner,
                    report,
                    &evidence
                )
                .unwrap_err(),
            ContractError::StaleEvaluation
        );
    }
    assert_eq!(active.state(), State::Validating);
    assert!(active.last_result().is_none());
}

#[test]
fn evidence_must_match_the_actual_attempt_target_actor_schema_and_custody() {
    type Change = fn(&mut EvidenceFacts);
    let cases: [(Change, ContractError); 11] = [
        (|facts| facts.producer = ISSUER, ContractError::WrongActor),
        (
            |facts| facts.binding.object = ObjectId::from_u128(99),
            ContractError::WrongObject,
        ),
        (
            |facts| facts.binding.content = ContentHash([99; 32]),
            ContractError::ContentConflict,
        ),
        (
            |facts| facts.binding.ledger.session = SessionId::from_u128(99),
            ContractError::WrongLedger,
        ),
        (
            |facts| facts.claim = ClaimId::from_u128(99),
            ContractError::InvalidTarget,
        ),
        (
            |facts| facts.validation = ValidationId::from_u128(99),
            ContractError::InvalidTarget,
        ),
        (
            |facts| facts.attempt.version = ContentHash([99; 32]),
            ContractError::StaleEvaluation,
        ),
        (
            |facts| facts.value = VerdictValue::Fail,
            ContractError::ContentConflict,
        ),
        (
            |facts| facts.schema = ContentHash([99; 32]),
            ContractError::MissingEvidence,
        ),
        (
            |facts| facts.custody_revision = None,
            ContractError::MissingEvidence,
        ),
        (
            |facts| facts.kind = EvidenceKind::Diagnostic,
            ContractError::MissingEvidence,
        ),
    ];
    let declaration = declaration(ValidationMode::Required, programmatic(false));
    let active = begun(&declaration);
    let (report, original) = report_parts(&active, VerdictValue::Pass);
    for (change, error) in cases {
        let mut facts = original;
        change(&mut facts);
        assert_eq!(
            active
                .report(
                    Principal::Actor(EVALUATOR),
                    &active.binding(),
                    &owner_for(&active),
                    report,
                    &facts
                )
                .unwrap_err(),
            error
        );
    }
    let (_, error_facts) = report_parts(&active, VerdictValue::Error);
    assert_eq!(
        active
            .report(
                Principal::Actor(EVALUATOR),
                &active.binding(),
                &owner_for(&active),
                report,
                &error_facts
            )
            .unwrap_err(),
        ContractError::ContentConflict
    );
}

#[test]
fn additional_policy_is_checked_only_when_immutably_declared() {
    let policy = PhasePolicy {
        required_policy: Some(ContentHash([90; 32])),
        ..quality_policy()
    };
    let declaration = declaration(ValidationMode::Required, Program::Agentic { check: policy });
    let ready = ready(&declaration);
    let mut owner = owner_for(&ready);
    owner.authority.policy_evidence = None;
    assert_eq!(
        ready
            .begin(
                Principal::Actor(QUALITY_EVALUATOR),
                &ready.binding(),
                &owner
            )
            .unwrap_err(),
        ContractError::InvalidPolicy
    );
    owner.authority.policy_evidence = Some(PolicyEvidence {
        evaluator: ISSUER,
        definition: policy.definition,
        policy: ContentHash([90; 32]),
    });
    assert_eq!(
        ready
            .begin(
                Principal::Actor(QUALITY_EVALUATOR),
                &ready.binding(),
                &owner
            )
            .unwrap_err(),
        ContractError::InvalidPolicy
    );
    let active = begun(&declaration);
    let (report, facts) = report_parts(&active, VerdictValue::Pass);
    let mut owner = owner_for(&active);
    owner.authority.policy_evidence = None;
    assert_eq!(
        active
            .report(
                Principal::Actor(QUALITY_EVALUATOR),
                &active.binding(),
                &owner,
                report,
                &facts
            )
            .unwrap_err(),
        ContractError::InvalidPolicy
    );
    assert_eq!(
        report_value(&active, VerdictValue::Pass).next.state(),
        State::Validated
    );
}

#[test]
fn missing_slots_are_assessed_only_after_response_receipt_without_fake_attempts() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        let declaration = declaration(mode, programmatic(false));
        let ordinary = ready(&declaration);
        let missing = Evaluation::materialize(
            Principal::Actor(ISSUER),
            &declaration,
            Materialization {
                binding: ordinary.binding(),
                target: Target::MissingSlot {
                    response: binding(300),
                    slot: 0,
                },
                slot_name: Some("output"),
                generation: ordinary.generation(),
                receipt: ordinary.receipt(),
            },
        )
        .unwrap();
        for readiness in [
            Readiness::ResponseGenerated,
            Readiness::ResponsePosted,
            Readiness::AdmissionPosted,
            Readiness::IncrementEligible,
        ] {
            let mut owner = owner_for(&missing);
            owner.readiness = readiness;
            assert_eq!(
                missing
                    .begin(Principal::Actor(ISSUER), &missing.binding(), &owner)
                    .unwrap_err(),
                ContractError::InvalidTransition
            );
        }
        let assessed = missing
            .begin(
                Principal::Actor(ISSUER),
                &missing.binding(),
                &owner_for(&missing),
            )
            .unwrap();
        assert!(!assessed.next.has_begun());
        assert_eq!(assessed.next.attempt_index(), None);
        if mode == ValidationMode::Required {
            assert_eq!(assessed.next.state(), State::ValidationIncomplete);
            let fact = assessed.result.unwrap();
            assert_eq!(fact.phase(), Phase::MissingTarget);
            assert_eq!(
                (fact.evidence(), fact.attempt(), fact.reporter()),
                (None, None, None)
            );
        } else {
            assert_eq!(assessed.next.state(), State::Ready);
            assert_eq!(
                assessed.next.suppression(),
                Some(Suppression::MissingTarget)
            );
            assert!(assessed.result.is_none());
        }
        assert_eq!(
            assessed
                .next
                .begin(
                    Principal::Actor(ISSUER),
                    &assessed.next.binding(),
                    &owner_for(&assessed.next)
                )
                .unwrap_err(),
            ContractError::InvalidTransition
        );
    }
}

#[test]
fn suppression_and_cohort_seal_prevent_new_begins_without_fabricated_results() {
    let declaration = declaration(ValidationMode::Observe, programmatic(false));
    let ready = ready(&declaration);
    let cause = ContentHash([70; 32]);
    for (parent, readiness, cohort, expected) in [
        (
            ParentState::Failed { cause },
            owner_for(&ready).readiness,
            Cohort::Open,
            Suppression::ParentFailure(cause),
        ),
        (
            ParentState::Open,
            Readiness::ArtifactFailed {
                artifact: binding(400),
                cause,
            },
            Cohort::Open,
            Suppression::ArtifactFailure(cause),
        ),
        (
            ParentState::Open,
            owner_for(&ready).readiness,
            Cohort::Sealed { cause },
            Suppression::CohortSealed(cause),
        ),
    ] {
        let owner = OwnerState {
            parent,
            readiness,
            cohort,
            ..owner_for(&ready)
        };
        let suppressed = ready
            .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
            .unwrap();
        assert_eq!(suppressed.next.state(), State::Ready);
        assert_eq!(suppressed.next.suppression(), Some(expected));
        assert!(suppressed.result.is_none());
        assert!(!suppressed.next.has_begun());
        assert_eq!(
            suppressed
                .next
                .begin(
                    Principal::Actor(EVALUATOR),
                    &suppressed.next.binding(),
                    &owner_for(&suppressed.next)
                )
                .unwrap_err(),
            ContractError::InvalidTransition
        );
    }
    let owner = OwnerState {
        cohort: Cohort::Sealed { cause },
        ..owner_for(&ready)
    };
    let sealed = ready.record_seal(&ready.binding(), &owner).unwrap();
    assert!(sealed.audit_finished());
    assert_eq!(sealed.state(), State::Ready);
    assert!(sealed.last_result().is_none());
    assert_eq!(
        sealed
            .record_seal(
                &sealed.binding(),
                &OwnerState {
                    evaluation: sealed.binding(),
                    ..owner
                }
            )
            .unwrap_err(),
        ContractError::InvalidTransition
    );
}

#[test]
fn begun_error_fallback_and_quality_chain_can_finish_after_business_failure_and_seal() {
    let declaration = declaration(ValidationMode::Required, programmatic(true));
    let active = begun(&declaration);
    let cause = ContentHash([71; 32]);
    let mut owner = OwnerState {
        parent: ParentState::Failed { cause },
        cohort: Cohort::Sealed { cause },
        ..owner_for(&active)
    };
    let sealed = active.record_seal(&active.binding(), &owner).unwrap();
    assert!(!sealed.audit_finished());
    owner.evaluation = sealed.binding();
    let (report, evidence) = report_parts(&sealed, VerdictValue::Error);
    let retry = sealed
        .report(
            Principal::Actor(EVALUATOR),
            &sealed.binding(),
            &owner,
            report,
            &evidence,
        )
        .unwrap()
        .next;
    owner.evaluation = retry.binding();
    let (report, evidence) = report_parts(&retry, VerdictValue::Pass);
    let quality = retry
        .report(
            Principal::Actor(EVALUATOR),
            &retry.binding(),
            &owner,
            report,
            &evidence,
        )
        .unwrap()
        .next;
    let owner = OwnerState {
        parent: ParentState::Failed { cause },
        cohort: Cohort::Sealed { cause },
        ..owner_for(&quality)
    };
    let (report, evidence) = report_parts(&quality, VerdictValue::Pass);
    let final_result = quality
        .report(
            Principal::Actor(QUALITY_EVALUATOR),
            &quality.binding(),
            &owner,
            report,
            &evidence,
        )
        .unwrap();
    assert_eq!(final_result.next.state(), State::Validated);
    assert!(final_result.next.audit_finished());
    assert_eq!(final_result.next.generation(), active.generation());
    assert_eq!(final_result.next.target(), active.target());
    assert_eq!(owner.parent, ParentState::Failed { cause });
}

#[test]
fn explicit_authority_fence_closes_audit_without_inventing_a_verdict() {
    let declaration = declaration(ValidationMode::Required, programmatic(false));
    let active = begun(&declaration);
    let fence = AuthorityFence {
        reason: FenceReason::Evaluation,
        cause: ContentHash([72; 32]),
    };
    let mut owner = owner_for(&active);
    assert_eq!(
        active.record_fence(&active.binding(), &owner).unwrap_err(),
        ContractError::InvalidTransition
    );
    owner.authority.state = AuthorityState::Fenced(fence);
    let fenced = active.record_fence(&active.binding(), &owner).unwrap();
    assert_eq!(fenced.fence(), Some(fence));
    assert_eq!(fenced.state(), State::Validating);
    assert!(fenced.audit_finished());
    assert!(fenced.last_result().is_none());
    let (report, evidence) = report_parts(&fenced, VerdictValue::Pass);
    assert_eq!(
        fenced
            .report(
                Principal::Actor(EVALUATOR),
                &fenced.binding(),
                &owner_for(&fenced),
                report,
                &evidence
            )
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    let mut owner = owner_for(&active);
    owner.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::Deadline(active.deadline()),
        ..fence
    });
    assert_eq!(
        active.record_fence(&active.binding(), &owner).unwrap_err(),
        ContractError::StaleEvaluation
    );
    owner.logical_time = active.deadline().at;
    assert!(
        active
            .record_fence(&active.binding(), &owner)
            .unwrap()
            .audit_finished()
    );
}

#[test]
fn receipt_is_a_distinct_handler_free_issuer_fact_and_cannot_be_an_evidence_bypass() {
    let spec = DeclarationSpec {
        kind: ValidationKind::Receipt,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        ..specification(ValidationMode::Required, Program::Delivery)
    };
    let declaration = Declaration::new(Principal::Actor(ISSUER), spec, limits()).unwrap();
    assert_eq!(declaration.attempt_bound(), 0);
    let ready = ready(&declaration);
    for principal in [Principal::Actor(EVALUATOR), Principal::Node(ISSUER)] {
        assert_eq!(
            ready
                .receive_delivery(principal, &ready.binding(), &owner_for(&ready))
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    let mut owner = owner_for(&ready);
    owner.readiness = Readiness::ResponsePosted;
    assert_eq!(
        ready
            .receive_delivery(Principal::Actor(ISSUER), &ready.binding(), &owner)
            .unwrap_err(),
        ContractError::InvalidTransition
    );
    assert_eq!(
        ready
            .begin(
                Principal::Actor(ISSUER),
                &ready.binding(),
                &owner_for(&ready)
            )
            .unwrap_err(),
        ContractError::InvalidTransition
    );
    let received = ready
        .receive_delivery(
            Principal::Actor(ISSUER),
            &ready.binding(),
            &owner_for(&ready),
        )
        .unwrap();
    let result = received.result.unwrap();
    assert_eq!(result.phase(), Phase::Delivery);
    assert_eq!(result.verdict(), VerdictValue::Pass);
    assert_eq!(
        (result.evidence(), result.reporter(), result.attempt()),
        (None, None, None)
    );
    assert!(!received.next.has_begun());
    assert_eq!(received.next.state(), State::Validated);
    assert_eq!(
        Declaration::new(
            Principal::Actor(ISSUER),
            DeclarationSpec {
                mode: ValidationMode::Observe,
                ..spec
            },
            limits()
        )
        .unwrap_err(),
        ContractError::InvalidPolicy
    );
    assert_eq!(
        Declaration::new(
            Principal::Actor(ISSUER),
            DeclarationSpec {
                kind: ValidationKind::Test,
                ..spec
            },
            limits()
        )
        .unwrap_err(),
        ContractError::InvalidPolicy
    );
}

#[test]
fn admission_and_increment_readiness_cannot_fill_whole_work_targets() {
    for (phase, target) in [
        (ValidationPhase::Admission, TargetDeclaration::Admission),
        (ValidationPhase::Increment, TargetDeclaration::Increment),
    ] {
        let spec = DeclarationSpec {
            phase,
            target,
            ..specification(ValidationMode::Required, programmatic(false))
        };
        let declaration = Declaration::new(Principal::Actor(ISSUER), spec, limits()).unwrap();
        let ready = ready(&declaration);
        let mut owner = owner_for(&ready);
        owner.readiness = if phase == ValidationPhase::Admission {
            Readiness::IncrementEligible
        } else {
            Readiness::AdmissionPosted
        };
        assert_eq!(
            ready
                .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
                .unwrap_err(),
            ContractError::InvalidTransition
        );
        assert_eq!(begun(&declaration).declared_phase(), phase);
    }
    let declaration = declaration(ValidationMode::Required, programmatic(false));
    let ready = ready(&declaration);
    for readiness in [
        Readiness::AdmissionPosted,
        Readiness::IncrementEligible,
        Readiness::ResponseGenerated,
        Readiness::ResponsePosted,
    ] {
        let owner = OwnerState {
            readiness,
            ..owner_for(&ready)
        };
        assert_eq!(
            ready
                .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
                .unwrap_err(),
            ContractError::InvalidTransition
        );
    }
}

#[test]
fn manifest_readiness_selects_exact_slot_id_and_hash_and_proves_missing_absence() {
    let receipt = ReceiptFence {
        receipt: crate::ReceiptId::from_u128(10),
        epoch: 3,
    };
    let artifact = binding(400);
    let reference = ArtifactRef {
        id: ArtifactId(artifact.object.0),
        hash: artifact.content,
    };
    let manifest = [super::super::evidence::SlotBinding {
        slot: 0,
        artifact: reference,
    }];
    let correct = Target::Artifact {
        response: binding(300),
        slot: 0,
        artifact,
    };
    assert!(
        ResponseReadiness::checked(
            binding(300),
            ClaimId::from_u128(200),
            receipt,
            &manifest,
            correct
        )
        .is_ok()
    );
    for target in [
        Target::Artifact {
            response: binding(300),
            slot: 1,
            artifact,
        },
        Target::Artifact {
            response: binding(300),
            slot: 0,
            artifact: binding(401),
        },
        Target::Artifact {
            response: binding(300),
            slot: 0,
            artifact: Binding {
                content: ContentHash([99; 32]),
                ..artifact
            },
        },
        Target::MissingSlot {
            response: binding(300),
            slot: 0,
        },
    ] {
        assert_eq!(
            ResponseReadiness::checked(
                binding(300),
                ClaimId::from_u128(200),
                receipt,
                &manifest,
                target
            )
            .unwrap_err(),
            ContractError::InvalidManifest
        );
    }
    assert!(
        ResponseReadiness::checked(
            binding(300),
            ClaimId::from_u128(200),
            receipt,
            &manifest,
            Target::MissingSlot {
                response: binding(300),
                slot: 1
            }
        )
        .is_ok()
    );
}

#[test]
fn revision_capacity_refusal_leaves_the_ready_evaluation_unstarted() {
    let mut spec = specification(ValidationMode::Required, programmatic(false));
    spec.binding.revision = ObjectRevision(u64::MAX);
    let declaration = Declaration::new(Principal::Actor(ISSUER), spec, limits()).unwrap();
    let ready = ready(&declaration);
    assert_eq!(
        ready
            .begin(
                Principal::Actor(EVALUATOR),
                &ready.binding(),
                &owner_for(&ready)
            )
            .unwrap_err(),
        ContractError::Capacity
    );
    assert_eq!(ready.state(), State::Ready);
    assert!(!ready.has_begun());
    assert!(ready.last_result().is_none());
}
