use super::tests::{limits, owner_for, programmatic, ready, report_value, specification};
use super::*;

fn owned_definition() -> Declaration {
    let Program::Programmatic {
        check,
        quality: Some(quality),
    } = programmatic(true)
    else {
        panic!()
    };
    let handlers: Vec<_> = check
        .handlers
        .iter()
        .map(|step| step.handler.clone())
        .collect();
    let steps: Vec<_> = check
        .handlers
        .iter()
        .zip(&handlers)
        .map(|(step, handler)| HandlerPolicy { handler, ..*step })
        .collect();
    let quality_handlers: Vec<_> = quality
        .handlers
        .iter()
        .map(|step| step.handler.clone())
        .collect();
    let quality_steps: Vec<_> = quality
        .handlers
        .iter()
        .zip(&quality_handlers)
        .map(|(step, handler)| HandlerPolicy { handler, ..*step })
        .collect();
    let slot = String::from("output");
    let mut spec = specification(
        ValidationMode::Required,
        Program::Programmatic {
            check: PhasePolicy {
                handlers: &steps,
                ..check
            },
            quality: Some(PhasePolicy {
                handlers: &quality_steps,
                ..quality
            }),
        },
    );
    spec.target = TargetDeclaration::WholeWorkSlot {
        index: 0,
        name: &slot,
    };
    Declaration::new(Principal::Actor(spec.issuer), spec, limits()).unwrap()
    // Every String, handler and policy construction buffer is dropped here.
}

#[test]
fn detached_row_survives_all_inputs_and_definitions_then_resumes_exact_policy() {
    fn requires_static<T: 'static>() {}
    requires_static::<Declaration>();
    requires_static::<EvaluationState>();
    assert!(!std::mem::needs_drop::<EvaluationState>());
    let stored = {
        let definition = owned_definition();
        let ready = ready(&definition);
        let active = ready
            .begin(
                Principal::Actor(ready.evaluator().unwrap()),
                &ready.binding(),
                &owner_for(&ready),
            )
            .unwrap()
            .next;
        report_value(&active, VerdictValue::Error).next.into_state()
    };
    let stored = {
        let exact = owned_definition();
        let rebound = stored.bind(&exact).unwrap();
        assert_eq!(rebound.current_attempt().unwrap().index, 1);
        let transition = report_value(&rebound, VerdictValue::Pass);
        assert_eq!(transition.next.state(), State::ValidatingQualityBar);
        assert!(!transition.result.unwrap().is_terminal());
        transition.next.into_state()
    };
    let stored = {
        let exact = owned_definition();
        let rebound = stored.bind(&exact).unwrap();
        let prior_proof = rebound.programmatic_evidence;
        assert!(prior_proof.is_some());
        let transition = report_value(&rebound, VerdictValue::Pass);
        let result = transition.result.unwrap();
        assert!(result.is_terminal());
        assert_eq!(result.programmatic_evidence(), prior_proof);
        transition.next.into_state()
    };
    let exact = owned_definition();
    let terminal = stored.bind(&exact).unwrap();
    assert_eq!(terminal.state(), State::Validated);
    assert_eq!(
        terminal.last_result().unwrap().verdict(),
        VerdictValue::Pass
    );
    assert_eq!(terminal.binding(), stored.binding());
}

#[test]
fn semantic_stamp_rejects_every_same_binding_policy_substitution() {
    let original = specification(ValidationMode::Required, programmatic(true));
    let definition =
        Declaration::new(Principal::Actor(original.issuer), original, limits()).unwrap();
    let stored = ready(&definition).into_state();
    let Program::Programmatic {
        check,
        quality: Some(quality),
    } = original.program
    else {
        panic!()
    };
    let mut reordered = check.handlers.to_vec();
    reordered.reverse();
    let mut different_attempts = check.handlers.to_vec();
    different_attempts[0].attempts += 1;
    let mut different_proof = check.handlers.to_vec();
    different_proof[0].proof_schema = ContentHash([201; 32]);
    let mut different_diagnostic = check.handlers.to_vec();
    different_diagnostic[0].diagnostic_schema = ContentHash([202; 32]);
    let mut handler = check.handlers[0].handler.clone();
    handler.version = ContentHash([203; 32]);
    let mut different_handler = check.handlers.to_vec();
    different_handler[0].handler = &handler;
    let mut changed = Vec::new();
    for policy in [
        PhasePolicy {
            handlers: &reordered,
            ..check
        },
        PhasePolicy {
            handlers: &different_attempts,
            ..check
        },
        PhasePolicy {
            handlers: &different_proof,
            ..check
        },
        PhasePolicy {
            handlers: &different_diagnostic,
            ..check
        },
        PhasePolicy {
            handlers: &different_handler,
            ..check
        },
        PhasePolicy {
            evaluator: ParticipantId::from_u128(204),
            ..check
        },
        PhasePolicy {
            definition: ContentHash([205; 32]),
            ..check
        },
        PhasePolicy {
            required_policy: Some(ContentHash([206; 32])),
            ..check
        },
    ] {
        changed.push(DeclarationSpec {
            program: Program::Programmatic {
                check: policy,
                quality: Some(quality),
            },
            ..original
        });
    }
    changed.extend([
        DeclarationSpec {
            program: Program::Programmatic {
                check,
                quality: None,
            },
            ..original
        },
        DeclarationSpec {
            program: Program::Agentic { check: quality },
            ..original
        },
        DeclarationSpec {
            program: Program::Programmatic {
                check,
                quality: Some(PhasePolicy {
                    definition: ContentHash([207; 32]),
                    ..quality
                }),
            },
            ..original
        },
        DeclarationSpec {
            target: TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "different",
            },
            ..original
        },
        DeclarationSpec {
            target: TargetDeclaration::WholeWorkSlot {
                index: 1,
                name: "output",
            },
            ..original
        },
        DeclarationSpec {
            target: TargetDeclaration::Increment,
            phase: ValidationPhase::Increment,
            ..original
        },
        DeclarationSpec {
            mode: ValidationMode::Observe,
            ..original
        },
        DeclarationSpec {
            kind: ValidationKind::Test,
            ..original
        },
        DeclarationSpec {
            declaration_index: 99,
            ..original
        },
        DeclarationSpec {
            claim: ClaimId::from_u128(208),
            ..original
        },
        DeclarationSpec {
            issuer: ParticipantId::from_u128(209),
            ..original
        },
        DeclarationSpec {
            deadline: Deadline {
                timer: crate::TimerId::from_u128(210),
                ..original.deadline
            },
            ..original
        },
        DeclarationSpec {
            deadline: Deadline {
                generation: 2,
                ..original.deadline
            },
            ..original
        },
        DeclarationSpec {
            deadline: Deadline {
                at: 101,
                ..original.deadline
            },
            ..original
        },
    ]);
    for spec in changed {
        let other = Declaration::new(Principal::Actor(spec.issuer), spec, limits()).unwrap();
        assert_eq!(other.binding(), definition.binding());
        assert_eq!(
            stored.bind(&other).unwrap_err(),
            ContractError::ContentConflict
        );
    }
    let exact = Declaration::new(Principal::Actor(original.issuer), original, limits()).unwrap();
    assert!(stored.bind(&exact).is_ok());
}

#[test]
fn preparation_checks_all_capacity_and_policy_bounds_before_owned_construction() {
    let spec = specification(ValidationMode::Required, programmatic(true));
    let plan = Declaration::prepare(Principal::Actor(spec.issuer), spec, limits()).unwrap();
    let expected = std::mem::size_of::<Declaration>()
        + "output".len()
        + 3 * std::mem::size_of::<OwnedHandlerPolicy>();
    assert_eq!(plan.construction_charge(), expected);
    let definition = plan.build().unwrap();
    assert_eq!(definition.retained_bytes().unwrap(), expected);
    let invalid = Limits {
        handlers: 1,
        ..limits()
    };
    assert!(matches!(
        Declaration::prepare(Principal::Actor(spec.issuer), spec, invalid),
        Err(ContractError::Capacity)
    ));
    let invalid = Limits {
        attempts: 1,
        ..limits()
    };
    assert!(matches!(
        Declaration::prepare(Principal::Actor(spec.issuer), spec, invalid),
        Err(ContractError::Capacity)
    ));
    let invalid = Limits {
        slot_bytes: 5,
        ..limits()
    };
    assert!(matches!(
        Declaration::prepare(Principal::Actor(spec.issuer), spec, invalid),
        Err(ContractError::InvalidTarget)
    ));
    let Program::Programmatic {
        check,
        quality: Some(quality),
    } = spec.program
    else {
        panic!()
    };
    let bad_quality = PhasePolicy {
        handlers: &[],
        ..quality
    };
    let invalid = DeclarationSpec {
        program: Program::Programmatic {
            check,
            quality: Some(bad_quality),
        },
        ..spec
    };
    assert!(matches!(
        Declaration::prepare(Principal::Actor(spec.issuer), invalid, limits()),
        Err(ContractError::InvalidPolicy)
    ));
    let huge = [
        HandlerPolicy {
            attempts: u32::MAX,
            ..check.handlers[0]
        },
        check.handlers[1],
    ];
    let invalid = DeclarationSpec {
        program: Program::Programmatic {
            check: PhasePolicy {
                handlers: &huge,
                ..check
            },
            quality: None,
        },
        ..spec
    };
    assert!(matches!(
        Declaration::prepare(
            Principal::Actor(spec.issuer),
            invalid,
            Limits {
                attempts: u32::MAX,
                ..limits()
            }
        ),
        Err(ContractError::Capacity)
    ));
}
