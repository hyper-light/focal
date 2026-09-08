use super::*;
use crate::lifecycle::aggregation::{AcceptancePolicy, CheckPolicy, SlotPolicy};
use crate::lifecycle::validation::tests as fixture;
use crate::{ObjectRevision, TimerId, ValidationId};

fn phase_matches(view: PhasePolicyView<'_>, expected: PhasePolicy<'_>) {
    assert_eq!(view.evaluator(), expected.evaluator);
    assert_eq!(view.definition(), expected.definition);
    assert_eq!(view.required_policy(), expected.required_policy);
    let mut handlers = view.handlers();
    assert_eq!(handlers.len(), expected.handlers.len());
    for expected in expected.handlers {
        let actual = handlers.next().unwrap();
        assert_eq!(actual.handler, expected.handler);
        assert_eq!(actual.attempts, expected.attempts);
        assert_eq!(actual.proof_schema, expected.proof_schema);
        assert_eq!(actual.diagnostic_schema, expected.diagnostic_schema);
    }
    assert_eq!(handlers.len(), 0);
    assert!(handlers.next().is_none());
}

fn program_matches(view: ProgramView<'_>, expected: Program<'_>) {
    match (view, expected) {
        (ProgramView::Delivery, Program::Delivery) => {}
        (
            ProgramView::Programmatic { check, quality },
            Program::Programmatic {
                check: expected_check,
                quality: expected_quality,
            },
        ) => {
            phase_matches(check, expected_check);
            match (quality, expected_quality) {
                (None, None) => {}
                (Some(actual), Some(expected)) => phase_matches(actual, expected),
                _ => panic!("quality presence changed"),
            }
        }
        (ProgramView::Agentic { check }, Program::Agentic { check: expected }) => {
            phase_matches(check, expected);
        }
        _ => panic!("program kind changed"),
    }
}

fn build_from_views(original: &Declaration, program: Program<'_>) -> Declaration {
    let spec = DeclarationSpec {
        binding: original.binding(),
        claim: original.claim(),
        issuer: original.issuer(),
        declaration_index: original.declaration_index(),
        kind: original.kind(),
        phase: original.declared_phase(),
        mode: original.mode(),
        target: original.target(),
        program,
        deadline: original.deadline(),
    };
    let plan =
        Declaration::prepare(Principal::Actor(original.issuer()), spec, fixture::limits()).unwrap();
    assert_eq!(plan.intent_fingerprint(), original.intent_fingerprint());
    plan.build().unwrap()
}

fn rebuild(original: &Declaration) -> Declaration {
    // Only reconstruction needs these temporary input arrays. Reading the
    // original policy itself borrows each retained handler directly.
    match original.program() {
        ProgramView::Delivery => build_from_views(original, Program::Delivery),
        ProgramView::Programmatic { check, quality } => {
            let handlers: Vec<_> = check.handlers().collect();
            let quality_handlers = quality.map(|phase| phase.handlers().collect::<Vec<_>>());
            build_from_views(
                original,
                Program::Programmatic {
                    check: PhasePolicy {
                        evaluator: check.evaluator(),
                        definition: check.definition(),
                        handlers: &handlers,
                        required_policy: check.required_policy(),
                    },
                    quality: quality
                        .zip(quality_handlers.as_deref())
                        .map(|(phase, handlers)| PhasePolicy {
                            evaluator: phase.evaluator(),
                            definition: phase.definition(),
                            handlers,
                            required_policy: phase.required_policy(),
                        }),
                },
            )
        }
        ProgramView::Agentic { check } => {
            let handlers: Vec<_> = check.handlers().collect();
            build_from_views(
                original,
                Program::Agentic {
                    check: PhasePolicy {
                        evaluator: check.evaluator(),
                        definition: check.definition(),
                        handlers: &handlers,
                        required_policy: check.required_policy(),
                    },
                },
            )
        }
    }
}

fn round_trip(spec: DeclarationSpec<'_>) {
    let plan =
        Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits()).unwrap();
    let fingerprint = plan.intent_fingerprint();
    let charge = plan.construction_charge();
    let original = plan.build().unwrap();
    assert_eq!(original.intent_fingerprint(), fingerprint);
    assert!(original.retained_bytes().unwrap() >= charge);
    assert_eq!(original.binding(), spec.binding);
    assert_eq!(original.claim(), spec.claim);
    assert_eq!(original.issuer(), spec.issuer);
    assert_eq!(original.declaration_index(), spec.declaration_index);
    assert_eq!(original.kind(), spec.kind);
    assert_eq!(original.declared_phase(), spec.phase);
    assert_eq!(original.mode(), spec.mode);
    assert_eq!(original.target(), spec.target);
    assert_eq!(original.deadline(), spec.deadline);
    program_matches(original.program(), spec.program);

    let rebuilt = rebuild(&original);
    assert_eq!(rebuilt.intent_fingerprint(), fingerprint);
    assert_eq!(rebuilt.definition_stamp(), original.definition_stamp());
    assert_eq!(rebuilt.attempt_bound(), original.attempt_bound());
    program_matches(rebuilt.program(), spec.program);
}

#[test]
fn declaration_views_reconstruct_every_program_target_kind_and_mode() {
    let Program::Programmatic {
        check,
        quality: Some(quality),
    } = fixture::programmatic(true)
    else {
        panic!("two-phase fixture")
    };
    let programs = [
        Program::Programmatic {
            check,
            quality: None,
        },
        Program::Programmatic {
            check: PhasePolicy {
                required_policy: Some(ContentHash([71; 32])),
                ..check
            },
            quality: Some(PhasePolicy {
                required_policy: Some(ContentHash([72; 32])),
                ..quality
            }),
        },
        Program::Agentic { check: quality },
    ];
    let base = DeclarationSpec {
        binding: Binding {
            content: ContentHash([73; 32]),
            revision: ObjectRevision(19),
            ..fixture::binding(987)
        },
        declaration_index: 876,
        deadline: Deadline {
            timer: TimerId::from_u128(765),
            generation: 654,
            at: 543,
        },
        ..fixture::specification(ValidationMode::Required, programs[0])
    };
    for kind in [
        ValidationKind::Test,
        ValidationKind::Inspection,
        ValidationKind::Integration,
        ValidationKind::Contract,
        ValidationKind::Design,
        ValidationKind::Regression,
    ] {
        for mode in [ValidationMode::Required, ValidationMode::Observe] {
            for (target, phase) in [
                (TargetDeclaration::Admission, ValidationPhase::Admission),
                (TargetDeclaration::Increment, ValidationPhase::Increment),
                (
                    TargetDeclaration::WholeWorkSlot {
                        index: 67,
                        name: "résultat final",
                    },
                    ValidationPhase::WholeWork,
                ),
            ] {
                for program in programs {
                    round_trip(DeclarationSpec {
                        kind,
                        mode,
                        target,
                        phase,
                        program,
                        ..base
                    });
                }
            }
        }
    }
    round_trip(DeclarationSpec {
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        ..base
    });
}

#[test]
fn phase_views_borrow_original_handlers_in_fallback_order() {
    let spec = fixture::specification(ValidationMode::Required, fixture::programmatic(true));
    let original = Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits())
        .unwrap()
        .build()
        .unwrap();
    let ProgramView::Programmatic {
        check,
        quality: Some(quality),
    } = original.program()
    else {
        panic!("two-phase view")
    };
    let OwnedProgram::Programmatic {
        check: owned_check,
        quality: Some(owned_quality),
    } = &original.spec.program
    else {
        panic!("two-phase source")
    };
    for (view, owned) in [(check, owned_check), (quality, owned_quality)] {
        assert_eq!(view.handlers().len(), owned.handlers.len());
        for (borrowed, retained) in view.handlers().rev().zip(owned.handlers.iter().rev()) {
            assert!(std::ptr::eq(borrowed.handler, &retained.handler));
        }
    }
}

#[test]
fn acceptance_slot_views_preserve_presence_indexes_modes_and_empty_check_sets() {
    let specs = [
        DeclarationSpec {
            binding: fixture::binding(900),
            declaration_index: 1,
            kind: ValidationKind::Receipt,
            target: TargetDeclaration::Delivery,
            program: Program::Delivery,
            ..fixture::specification(ValidationMode::Required, Program::Delivery)
        },
        DeclarationSpec {
            binding: fixture::binding(901),
            declaration_index: 11,
            target: TargetDeclaration::WholeWorkSlot {
                index: 2,
                name: "primary",
            },
            ..fixture::specification(ValidationMode::Required, fixture::programmatic(false))
        },
        DeclarationSpec {
            binding: fixture::binding(902),
            declaration_index: 12,
            target: TargetDeclaration::WholeWorkSlot {
                index: 2,
                name: "primary",
            },
            ..fixture::specification(ValidationMode::Observe, fixture::programmatic(true))
        },
        DeclarationSpec {
            binding: fixture::binding(903),
            declaration_index: 13,
            target: TargetDeclaration::WholeWorkSlot {
                index: 19,
                name: "appendix",
            },
            ..fixture::specification(ValidationMode::Required, fixture::programmatic(false))
        },
    ];
    let declarations: Vec<_> = specs
        .into_iter()
        .map(|spec| {
            Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits())
                .unwrap()
                .build()
                .unwrap()
        })
        .collect();
    let primary = [
        CheckPolicy {
            declaration_index: 11,
            validation: ValidationId::from_u128(901),
            mode: ValidationMode::Required,
        },
        CheckPolicy {
            declaration_index: 12,
            validation: ValidationId::from_u128(902),
            mode: ValidationMode::Observe,
        },
    ];
    let appendix = [CheckPolicy {
        declaration_index: 13,
        validation: ValidationId::from_u128(903),
        mode: ValidationMode::Required,
    }];
    let slots = [
        SlotPolicy {
            slot: 2,
            missing_declaration_index: 81,
            mode: ValidationMode::Required,
            checks: &primary,
        },
        SlotPolicy {
            slot: 7,
            missing_declaration_index: 83,
            mode: ValidationMode::Required,
            checks: &[],
        },
        SlotPolicy {
            slot: 19,
            missing_declaration_index: 89,
            mode: ValidationMode::Observe,
            checks: &appendix,
        },
    ];
    let limits = crate::lifecycle::aggregation::Limits {
        max_slots: 3,
        max_checks: 4,
        max_results: 16,
        max_updates: 16,
    };
    let original = AcceptancePolicy::new(
        fixture::binding(200),
        fixture::ISSUER,
        &slots,
        &declarations,
        limits,
    )
    .unwrap();
    assert_eq!(original.slots().len(), 3);
    for (actual, expected) in original.slots().zip(slots) {
        assert_eq!(actual.slot, expected.slot);
        assert_eq!(
            actual.missing_declaration_index,
            expected.missing_declaration_index
        );
        assert_eq!(actual.mode, expected.mode);
        assert_eq!(actual.checks, expected.checks);
        let reread = original
            .slots()
            .find(|slot| slot.slot == actual.slot)
            .unwrap();
        assert!(std::ptr::eq(actual.checks, reread.checks));
    }
    assert_eq!(
        original
            .slots()
            .rev()
            .map(|slot| slot.slot)
            .collect::<Vec<_>>(),
        [19, 7, 2]
    );
    assert!(original.has_slot(7));
    assert!(
        !original
            .declarations()
            .iter()
            .any(|declaration| declaration.target()
                == crate::lifecycle::aggregation::ObligationTarget::Slot(7))
    );
    let borrowed: Vec<_> = original.slots().collect();
    let rebuilt = AcceptancePolicy::new(
        original.claim(),
        original.issuer(),
        &borrowed,
        &declarations,
        limits,
    )
    .unwrap();
    assert_eq!(rebuilt, original);
    assert_eq!(rebuilt.intent_fingerprint(), original.intent_fingerprint());
}
