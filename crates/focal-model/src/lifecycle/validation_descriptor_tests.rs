use super::*;
use crate::lifecycle::memory as allocation;
use crate::{HandlerRef, SessionId, TenantId, TimerId, ValidatorId};
use validation::{
    HandlerPolicy, PhasePolicy, PhasePolicyView, Program, ProgramView, TargetDeclaration,
};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const CONTRIBUTORS: [ParticipantId; 2] = [ISSUER, ParticipantId([2; 16])];
const CHECK: HandlerRef = HandlerRef {
    id: ValidatorId([10; 16]),
    version: ContentHash([11; 32]),
    agentic: false,
};
const FALLBACK: HandlerRef = HandlerRef {
    id: ValidatorId([12; 16]),
    version: ContentHash([13; 32]),
    agentic: false,
};
const AGENT: HandlerRef = HandlerRef {
    id: ValidatorId([14; 16]),
    version: ContentHash([15; 32]),
    agentic: true,
};
const CHECKS: [HandlerPolicy<'static>; 2] = [
    HandlerPolicy {
        handler: &CHECK,
        attempts: 2,
        proof_schema: ContentHash([16; 32]),
        diagnostic_schema: ContentHash([17; 32]),
    },
    HandlerPolicy {
        handler: &FALLBACK,
        attempts: 1,
        proof_schema: ContentHash([18; 32]),
        diagnostic_schema: ContentHash([19; 32]),
    },
];
const AGENTS: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &AGENT,
    attempts: 2,
    proof_schema: ContentHash([20; 32]),
    diagnostic_schema: ContentHash([21; 32]),
}];

fn check() -> PhasePolicy<'static> {
    PhasePolicy {
        evaluator: ParticipantId([3; 16]),
        definition: ContentHash([22; 32]),
        handlers: &CHECKS,
        required_policy: Some(ContentHash([23; 32])),
    }
}
fn quality() -> PhasePolicy<'static> {
    PhasePolicy {
        evaluator: ParticipantId([4; 16]),
        definition: ContentHash([24; 32]),
        handlers: &AGENTS,
        required_policy: Some(ContentHash([25; 32])),
    }
}
fn spec() -> ValidationSpec<'static> {
    ValidationSpec {
        ledger: LedgerId {
            tenant: TenantId([5; 16]),
            session: SessionId([6; 16]),
        },
        id: ValidationId([7; 16]),
        schema: 1,
        claim: ClaimId([8; 16]),
        issuer: ISSUER,
        declaration_index: 9,
        kind: ValidationKind::Inspection,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        target: TargetDeclaration::WholeWorkSlot {
            index: 1,
            name: "résultat",
        },
        program: Program::Programmatic {
            check: check(),
            quality: Some(quality()),
        },
        deadline: Deadline {
            timer: TimerId([26; 16]),
            generation: 2,
            at: 100,
        },
        description: "Inspect the exact evidence; retain errors. é",
        quality_bar: Some("Explain the result with cited proof. 🦀"),
        contributed_by: &CONTRIBUTORS,
        policy_revision: 3,
    }
}
fn limits() -> Limits {
    Limits {
        declaration: validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
        description_bytes: 256,
        quality_bar_bytes: 256,
        contributors: 4,
        construction_bytes: 4096,
    }
}
fn plan(spec: ValidationSpec<'_>) -> ValidationPlan<'_> {
    ValidationDescriptor::prepare(Principal::Actor(spec.issuer), spec, limits()).unwrap()
}
fn built(spec: ValidationSpec<'_>) -> ValidationDescriptor {
    let plan = plan(spec);
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}
fn content(spec: ValidationSpec<'_>) -> ContentHash {
    plan(spec).content_hash()
}
fn phase_matches(actual: PhasePolicyView<'_>, expected: PhasePolicy<'_>) {
    assert_eq!(actual.evaluator(), expected.evaluator);
    assert_eq!(actual.definition(), expected.definition);
    assert_eq!(actual.required_policy(), expected.required_policy);
    assert_eq!(actual.handlers().len(), expected.handlers.len());
    for (actual, expected) in actual.handlers().zip(expected.handlers) {
        assert_eq!(actual.handler, expected.handler);
        assert_eq!(actual.attempts, expected.attempts);
        assert_eq!(actual.proof_schema, expected.proof_schema);
        assert_eq!(actual.diagnostic_schema, expected.diagnostic_schema);
    }
}
fn program_matches(actual: ProgramView<'_>, expected: Program<'_>) {
    match (actual, expected) {
        (ProgramView::Delivery, Program::Delivery) => {}
        (ProgramView::Agentic { check }, Program::Agentic { check: expected }) => {
            phase_matches(check, expected);
        }
        (
            ProgramView::Programmatic { check, quality },
            Program::Programmatic {
                check: expected,
                quality: expected_quality,
            },
        ) => {
            phase_matches(check, expected);
            match (quality, expected_quality) {
                (None, None) => {}
                (Some(actual), Some(expected)) => phase_matches(actual, expected),
                _ => panic!("quality program changed"),
            }
        }
        _ => panic!("program changed"),
    }
}
fn descriptor_matches(actual: &ValidationDescriptor, expected: ValidationSpec<'_>) {
    let declaration = actual.declaration();
    assert_eq!(actual.schema(), expected.schema);
    assert_eq!(actual.binding().ledger, expected.ledger);
    assert_eq!(actual.binding().object, ObjectId(expected.id.0));
    assert_eq!(actual.binding().revision, ObjectRevision(1));
    assert_eq!(actual.content_hash(), content(expected));
    assert_eq!(
        actual.specification_hash(),
        plan(expected).specification_hash()
    );
    assert_eq!(actual.description(), expected.description);
    assert_eq!(actual.quality_bar(), expected.quality_bar);
    assert_eq!(actual.contributed_by(), expected.contributed_by);
    assert_eq!(actual.policy_revision(), expected.policy_revision);
    assert_eq!(declaration.claim(), expected.claim);
    assert_eq!(declaration.issuer(), expected.issuer);
    assert_eq!(declaration.declaration_index(), expected.declaration_index);
    assert_eq!(declaration.kind(), expected.kind);
    assert_eq!(declaration.declared_phase(), expected.phase);
    assert_eq!(declaration.mode(), expected.mode);
    assert_eq!(declaration.target(), expected.target);
    assert_eq!(declaration.deadline(), expected.deadline);
    program_matches(declaration.program(), expected.program);
}

#[test]
fn full_authored_bodies_round_trip_to_native_declarations_for_every_program_and_target() {
    for (target, phase) in [
        (spec().target, ValidationPhase::WholeWork),
        (TargetDeclaration::Admission, ValidationPhase::Admission),
        (TargetDeclaration::Increment, ValidationPhase::Increment),
    ] {
        for program in [
            Program::Programmatic {
                check: check(),
                quality: None,
            },
            Program::Programmatic {
                check: check(),
                quality: Some(quality()),
            },
            Program::Agentic { check: quality() },
        ] {
            for mode in [ValidationMode::Required, ValidationMode::Observe] {
                for kind in [
                    ValidationKind::Test,
                    ValidationKind::Inspection,
                    ValidationKind::Integration,
                    ValidationKind::Contract,
                    ValidationKind::Design,
                    ValidationKind::Regression,
                ] {
                    let spec = ValidationSpec {
                        target,
                        phase,
                        program,
                        mode,
                        kind,
                        ..spec()
                    };
                    let prepared = plan(spec);
                    let hash = prepared.content_hash();
                    let specification = prepared.specification_hash();
                    let fingerprint = prepared.intent_fingerprint();
                    assert_eq!(
                        prepared.spec().description.as_ptr(),
                        spec.description.as_ptr()
                    );
                    let charge = prepared.construction_charge();
                    let heap = prepared.construction_heap_bytes();
                    let allocations = prepared.construction_heap_allocations();
                    let descriptor = prepared.build(charge).unwrap();
                    descriptor_matches(&descriptor, spec);
                    assert_eq!(descriptor.intent_fingerprint(), fingerprint);
                    assert_eq!(descriptor.content_hash(), hash);
                    assert_eq!(descriptor.specification_hash(), specification);
                    assert_eq!(descriptor.retained_bytes().unwrap(), charge);
                    assert_eq!(descriptor.retained_heap_bytes().unwrap(), heap);
                    assert_eq!(descriptor.heap_allocations().unwrap(), allocations);
                    // The complete descriptor digest is the declaration binding,
                    // while every external phase standard remains unchanged.
                    let reconstructed = validation::Declaration::new(
                        Principal::Actor(spec.issuer),
                        spec.declaration(hash),
                        limits().declaration,
                    )
                    .unwrap();
                    assert_eq!(
                        descriptor.declaration().intent_fingerprint(),
                        reconstructed.intent_fingerprint()
                    );
                    assert_eq!(
                        descriptor.declaration().attempt_bound(),
                        reconstructed.attempt_bound()
                    );
                    program_matches(reconstructed.program(), spec.program);
                }
            }
        }
    }
    let receipt = ValidationSpec {
        kind: ValidationKind::Receipt,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        quality_bar: None,
        ..spec()
    };
    let descriptor = built(receipt);
    descriptor_matches(&descriptor, receipt);
    assert_eq!(descriptor.declaration().attempt_bound(), 0);
    assert_eq!(descriptor.declaration().evidence_schemas().count(), 0);
}

#[test]
fn program_semantics_are_not_inferred_from_rubric_text_and_delivery_refuses_quality() {
    for quality_bar in [None, Some("An authored rubric does not schedule a phase")] {
        for (program, expected_phase) in [
            (
                Program::Agentic { check: quality() },
                validation::Phase::Quality,
            ),
            (
                Program::Programmatic {
                    check: check(),
                    quality: None,
                },
                validation::Phase::Programmatic,
            ),
            (
                Program::Programmatic {
                    check: check(),
                    quality: Some(quality()),
                },
                validation::Phase::Programmatic,
            ),
        ] {
            let spec = ValidationSpec {
                target: TargetDeclaration::Admission,
                phase: ValidationPhase::Admission,
                program,
                quality_bar,
                ..spec()
            };
            let descriptor = built(spec);
            let evaluation = validation::Evaluation::materialize(
                Principal::Actor(spec.issuer),
                descriptor.declaration(),
                validation::Materialization {
                    binding: descriptor.binding(),
                    target: validation::Target::Admission {
                        claim: Binding {
                            object: ObjectId(spec.claim.0),
                            content: ContentHash([90; 32]),
                            ..descriptor.binding()
                        },
                    },
                    slot_name: None,
                    generation: 1,
                    receipt: None,
                },
            )
            .unwrap();
            assert_eq!(evaluation.current_phase(), expected_phase);
            assert_eq!(evaluation.state(), validation::State::Ready);
            assert_eq!(descriptor.quality_bar(), quality_bar);
            program_matches(descriptor.declaration().program(), program);
        }
    }
    let receipt = ValidationSpec {
        kind: ValidationKind::Receipt,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        ..spec()
    };
    assert!(matches!(
        ValidationDescriptor::prepare(Principal::Actor(ISSUER), receipt, limits()),
        Err(ContractError::InvalidPolicy)
    ));
}

#[test]
fn content_excludes_own_id_and_commits_all_referenced_identity_and_authored_fields() {
    let original = spec();
    let hash = content(original);
    let specification = plan(original).specification_hash();
    assert_ne!(
        specification, hash,
        "specification and bound content have separate domains"
    );
    let rebound = ValidationSpec {
        id: ValidationId([91; 16]),
        ..original
    };
    assert_eq!(content(rebound), hash);
    assert_eq!(plan(rebound).specification_hash(), specification);
    assert_eq!(built(rebound).specification_hash(), specification);
    assert_ne!(
        plan(rebound).intent_fingerprint(),
        plan(original).intent_fingerprint()
    );
    assert_ne!(
        built(rebound).declaration().intent_fingerprint(),
        built(original).declaration().intent_fingerprint()
    );
    // A requirement can pin its specification before a parent claim is
    // allocated. Attaching that specification still binds exact full content.
    let reparented = ValidationSpec {
        claim: ClaimId([92; 16]),
        ..original
    };
    assert_ne!(content(reparented), hash);
    assert_eq!(plan(reparented).specification_hash(), specification);
    assert_eq!(built(reparented).specification_hash(), specification);
    for changed in [
        ValidationSpec {
            ledger: LedgerId {
                tenant: TenantId([92; 16]),
                ..original.ledger
            },
            ..original
        },
        ValidationSpec {
            ledger: LedgerId {
                session: SessionId([92; 16]),
                ..original.ledger
            },
            ..original
        },
        ValidationSpec {
            issuer: ParticipantId([92; 16]),
            ..original
        },
        ValidationSpec {
            declaration_index: 10,
            ..original
        },
        ValidationSpec {
            kind: ValidationKind::Test,
            ..original
        },
        ValidationSpec {
            mode: ValidationMode::Observe,
            ..original
        },
        ValidationSpec {
            target: TargetDeclaration::WholeWorkSlot {
                index: 2,
                name: "résultat",
            },
            ..original
        },
        ValidationSpec {
            target: TargetDeclaration::WholeWorkSlot {
                index: 1,
                name: "result",
            },
            ..original
        },
        ValidationSpec {
            target: TargetDeclaration::Admission,
            phase: ValidationPhase::Admission,
            ..original
        },
        ValidationSpec {
            target: TargetDeclaration::Increment,
            phase: ValidationPhase::Increment,
            ..original
        },
        ValidationSpec {
            deadline: Deadline {
                timer: TimerId([92; 16]),
                ..original.deadline
            },
            ..original
        },
        ValidationSpec {
            deadline: Deadline {
                generation: 3,
                ..original.deadline
            },
            ..original
        },
        ValidationSpec {
            deadline: Deadline {
                at: 101,
                ..original.deadline
            },
            ..original
        },
        ValidationSpec {
            description: " Inspect the exact evidence; retain errors. é",
            ..original
        },
        ValidationSpec {
            quality_bar: Some("Different rubric"),
            ..original
        },
        ValidationSpec {
            quality_bar: None,
            ..original
        },
        ValidationSpec {
            contributed_by: &CONTRIBUTORS[..1],
            ..original
        },
        ValidationSpec {
            contributed_by: &[ParticipantId([92; 16])],
            ..original
        },
        ValidationSpec {
            policy_revision: 4,
            ..original
        },
        ValidationSpec {
            program: Program::Programmatic {
                check: check(),
                quality: None,
            },
            ..original
        },
        ValidationSpec {
            program: Program::Agentic { check: quality() },
            ..original
        },
    ] {
        assert_ne!(
            content(changed),
            hash,
            "uncommitted authored change: {changed:?}"
        );
        assert_ne!(
            plan(changed).specification_hash(),
            specification,
            "uncommitted specification change: {changed:?}"
        );
    }
}

#[test]
fn every_phase_and_ordered_handler_field_changes_content_without_replacing_external_standards() {
    let original = spec();
    let expected = content(original);
    let specification = plan(original).specification_hash();
    for is_quality in [false, true] {
        let base = if is_quality { quality() } else { check() };
        let changed_handler_id = HandlerRef {
            id: ValidatorId([94; 16]),
            ..(*base.handlers[0].handler).clone()
        };
        let changed_handler_version = HandlerRef {
            version: ContentHash([94; 32]),
            ..(*base.handlers[0].handler).clone()
        };
        for changed in [
            PhasePolicy {
                evaluator: ParticipantId([94; 16]),
                ..base
            },
            PhasePolicy {
                definition: ContentHash([94; 32]),
                ..base
            },
            PhasePolicy {
                required_policy: None,
                ..base
            },
            PhasePolicy {
                required_policy: Some(ContentHash([94; 32])),
                ..base
            },
        ] {
            let program = if is_quality {
                Program::Programmatic {
                    check: check(),
                    quality: Some(changed),
                }
            } else {
                Program::Programmatic {
                    check: changed,
                    quality: Some(quality()),
                }
            };
            assert_ne!(
                content(ValidationSpec {
                    program,
                    ..original
                }),
                expected
            );
            assert_ne!(
                plan(ValidationSpec {
                    program,
                    ..original
                })
                .specification_hash(),
                specification
            );
        }
        for changed in [
            HandlerPolicy {
                handler: &changed_handler_id,
                ..base.handlers[0]
            },
            HandlerPolicy {
                handler: &changed_handler_version,
                ..base.handlers[0]
            },
            HandlerPolicy {
                attempts: 3,
                ..base.handlers[0]
            },
            HandlerPolicy {
                proof_schema: ContentHash([94; 32]),
                ..base.handlers[0]
            },
            HandlerPolicy {
                diagnostic_schema: ContentHash([94; 32]),
                ..base.handlers[0]
            },
        ] {
            let mut handlers = base.handlers.to_vec();
            handlers[0] = changed;
            let changed = PhasePolicy {
                handlers: &handlers,
                ..base
            };
            let program = if is_quality {
                Program::Programmatic {
                    check: check(),
                    quality: Some(changed),
                }
            } else {
                Program::Programmatic {
                    check: changed,
                    quality: Some(quality()),
                }
            };
            let descriptor = built(ValidationSpec {
                program,
                ..original
            });
            assert_ne!(descriptor.content_hash(), expected);
            assert_ne!(descriptor.specification_hash(), specification);
            program_matches(descriptor.declaration().program(), program);
        }
    }
    let reversed = [CHECKS[1], CHECKS[0]];
    let program = Program::Programmatic {
        check: PhasePolicy {
            handlers: &reversed,
            ..check()
        },
        quality: Some(quality()),
    };
    let descriptor = built(ValidationSpec {
        program,
        ..original
    });
    assert_ne!(descriptor.content_hash(), expected);
    assert_ne!(descriptor.specification_hash(), specification);
    program_matches(descriptor.declaration().program(), program);
}

#[test]
fn fixed_delivery_vector_uses_independently_encoded_full_content_bytes() {
    let contributors = [ParticipantId([0x17; 16]), ParticipantId([0x18; 16])];
    let spec = ValidationSpec {
        ledger: LedgerId {
            tenant: TenantId([0x11; 16]),
            session: SessionId([0x12; 16]),
        },
        id: ValidationId([0x13; 16]),
        schema: 1,
        claim: ClaimId([0x14; 16]),
        issuer: ParticipantId([0x15; 16]),
        declaration_index: 0x01020304,
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        deadline: Deadline {
            timer: TimerId([0x16; 16]),
            generation: 0x0102030405060708,
            at: 0x1112131415161718,
        },
        description: "Inspect é",
        quality_bar: None,
        contributed_by: &contributors,
        policy_revision: 0x2122232425262728,
    };
    // Literal wire-independent preimage: schema1, ledger, Validation kind3,
    // referenced claim/issuer, index, Receipt1/WholeWork3/Required2, delivery
    // target1/program0, timer, UTF-8 byte length10, no quality, two contributors.
    // No production encoding or enum-code helper participates in this vector.
    let fixed = concat!(
        "666f63616c2f6e61746976652f76616c69646174696f6e2d636f6e74656e742f31",
        "0001",
        "11111111111111111111111111111111",
        "12121212121212121212121212121212",
        "0003",
        "14141414141414141414141414141414",
        "15151515151515151515151515151515",
        "01020304",
        "000100030002",
        "0100",
        "16161616161616161616161616161616",
        "0102030405060708",
        "1112131415161718",
        "000000000000000a",
        "496e737065637420c3a9",
        "00",
        "0000000000000002",
        "17171717171717171717171717171717",
        "18181818181818181818181818181818",
        "2122232425262728"
    );
    let preimage: Vec<_> = fixed
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let expected = ContentHash(*blake3::hash(&preimage).as_bytes());
    assert_eq!(plan(spec).content_hash(), expected);
    assert_eq!(built(spec).content_hash(), expected);
}

#[test]
fn malformed_authored_identity_references_and_cardinalities_refuse_before_allocation() {
    let base = spec();
    for changed in [
        ValidationSpec { schema: 0, ..base },
        ValidationSpec { schema: 2, ..base },
        ValidationSpec {
            policy_revision: 0,
            ..base
        },
        ValidationSpec {
            id: ValidationId([0; 16]),
            ..base
        },
        ValidationSpec {
            claim: ClaimId([0; 16]),
            ..base
        },
        ValidationSpec {
            issuer: ParticipantId([0; 16]),
            ..base
        },
        ValidationSpec {
            ledger: LedgerId {
                tenant: TenantId([0; 16]),
                ..base.ledger
            },
            ..base
        },
        ValidationSpec {
            ledger: LedgerId {
                session: SessionId([0; 16]),
                ..base.ledger
            },
            ..base
        },
        ValidationSpec {
            deadline: Deadline {
                timer: TimerId([0; 16]),
                ..base.deadline
            },
            ..base
        },
        ValidationSpec {
            deadline: Deadline {
                generation: 0,
                ..base.deadline
            },
            ..base
        },
        ValidationSpec {
            description: " \t\n",
            ..base
        },
        ValidationSpec {
            description: "bad\0text",
            ..base
        },
        ValidationSpec {
            quality_bar: Some(""),
            ..base
        },
        ValidationSpec {
            quality_bar: Some("bad\0rubric"),
            ..base
        },
        ValidationSpec {
            contributed_by: &[ParticipantId([0; 16])],
            ..base
        },
        ValidationSpec {
            contributed_by: &[ISSUER, ISSUER],
            ..base
        },
        ValidationSpec {
            contributed_by: &[CONTRIBUTORS[1], CONTRIBUTORS[0]],
            ..base
        },
        ValidationSpec {
            target: TargetDeclaration::WholeWorkSlot { index: 1, name: "" },
            ..base
        },
        ValidationSpec {
            target: TargetDeclaration::Admission,
            ..base
        },
    ] {
        allocation::fail_after(0, || {
            assert!(
                ValidationDescriptor::prepare(Principal::Actor(changed.issuer), changed, limits())
                    .is_err(),
                "accepted {changed:?}"
            );
            assert_eq!(allocation::remaining_allocations(), Some(0));
        });
    }
    for principal in [
        Principal::Actor(ParticipantId([99; 16])),
        Principal::Node(ISSUER),
    ] {
        assert!(matches!(
            ValidationDescriptor::prepare(principal, base, limits()),
            Err(ContractError::WrongActor)
        ));
    }
    for phase in [
        PhasePolicy {
            evaluator: ParticipantId([0; 16]),
            ..check()
        },
        PhasePolicy {
            definition: ContentHash([0; 32]),
            ..check()
        },
        PhasePolicy {
            required_policy: Some(ContentHash([0; 32])),
            ..check()
        },
        PhasePolicy {
            handlers: &[],
            ..check()
        },
    ] {
        let spec = ValidationSpec {
            program: Program::Programmatic {
                check: phase,
                quality: None,
            },
            ..base
        };
        assert!(ValidationDescriptor::prepare(Principal::Actor(ISSUER), spec, limits()).is_err());
    }
    for program in [
        Program::Programmatic {
            check: check(),
            quality: Some(PhasePolicy {
                definition: ContentHash([0; 32]),
                ..quality()
            }),
        },
        Program::Agentic {
            check: PhasePolicy {
                definition: ContentHash([0; 32]),
                ..quality()
            },
        },
    ] {
        assert!(
            ValidationDescriptor::prepare(
                Principal::Actor(ISSUER),
                ValidationSpec { program, ..base },
                limits()
            )
            .is_err()
        );
    }
    let zero_id = HandlerRef {
        id: ValidatorId([0; 16]),
        ..CHECK
    };
    let zero_version = HandlerRef {
        version: ContentHash([0; 32]),
        ..CHECK
    };
    for handler in [
        HandlerPolicy {
            handler: &zero_id,
            ..CHECKS[0]
        },
        HandlerPolicy {
            handler: &zero_version,
            ..CHECKS[0]
        },
        HandlerPolicy {
            handler: &AGENT,
            ..CHECKS[0]
        },
        HandlerPolicy {
            attempts: 0,
            ..CHECKS[0]
        },
        HandlerPolicy {
            attempts: u32::MAX,
            ..CHECKS[0]
        },
        HandlerPolicy {
            proof_schema: ContentHash([0; 32]),
            ..CHECKS[0]
        },
        HandlerPolicy {
            diagnostic_schema: ContentHash([0; 32]),
            ..CHECKS[0]
        },
    ] {
        let handlers = [handler];
        let spec = ValidationSpec {
            program: Program::Programmatic {
                check: PhasePolicy {
                    handlers: &handlers,
                    ..check()
                },
                quality: None,
            },
            ..base
        };
        assert!(ValidationDescriptor::prepare(Principal::Actor(ISSUER), spec, limits()).is_err());
    }
}

#[test]
fn utf8_byte_counts_and_exact_preflight_bounds_are_enforced() {
    let spec = ValidationSpec {
        description: "é",
        quality_bar: Some("🦀"),
        ..spec()
    };
    let exact = Limits {
        description_bytes: 2,
        quality_bar_bytes: 4,
        contributors: 2,
        declaration: validation::Limits {
            slot_bytes: "résultat".len(),
            ..limits().declaration
        },
        ..limits()
    };
    let plan = ValidationDescriptor::prepare(Principal::Actor(ISSUER), spec, exact).unwrap();
    let charge = plan.construction_charge();
    for limited in [
        Limits {
            description_bytes: 1,
            ..exact
        },
        Limits {
            quality_bar_bytes: 3,
            ..exact
        },
        Limits {
            contributors: 1,
            ..exact
        },
        Limits {
            construction_bytes: charge - 1,
            ..exact
        },
        Limits {
            declaration: validation::Limits {
                handlers: 1,
                ..exact.declaration
            },
            ..exact
        },
        Limits {
            declaration: validation::Limits {
                attempts: 4,
                ..exact.declaration
            },
            ..exact
        },
    ] {
        allocation::fail_after(0, || {
            assert!(matches!(
                ValidationDescriptor::prepare(Principal::Actor(ISSUER), spec, limited),
                Err(ContractError::Capacity)
            ));
            assert_eq!(allocation::remaining_allocations(), Some(0));
        });
    }
    assert!(matches!(
        ValidationDescriptor::prepare(
            Principal::Actor(ISSUER),
            spec,
            Limits {
                declaration: validation::Limits {
                    slot_bytes: exact.declaration.slot_bytes - 1,
                    ..exact.declaration
                },
                ..exact
            }
        ),
        Err(ContractError::InvalidTarget)
    ));
    allocation::fail_after(0, || {
        assert!(matches!(
            plan.build(charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(allocation::remaining_allocations(), Some(0));
    });
    let descriptor = ValidationDescriptor::prepare(
        Principal::Actor(ISSUER),
        spec,
        Limits {
            construction_bytes: charge,
            ..exact
        },
    )
    .unwrap()
    .build(charge)
    .unwrap();
    assert_eq!(descriptor.description(), "é");
    assert_eq!(descriptor.quality_bar(), Some("🦀"));
    assert!(memory::authored_heap(usize::MAX, 1, 0).is_err());
    assert!(memory::authored_heap(0, 0, usize::MAX).is_err());
    let overflowing = [
        HandlerPolicy {
            attempts: u32::MAX,
            ..CHECKS[0]
        },
        HandlerPolicy {
            attempts: 1,
            ..CHECKS[1]
        },
    ];
    let overflow_spec = ValidationSpec {
        program: Program::Programmatic {
            check: PhasePolicy {
                handlers: &overflowing,
                ..check()
            },
            quality: None,
        },
        ..spec
    };
    allocation::fail_after(0, || {
        assert!(matches!(
            ValidationDescriptor::prepare(
                Principal::Actor(ISSUER),
                overflow_spec,
                Limits {
                    declaration: validation::Limits {
                        attempts: u32::MAX,
                        ..limits().declaration
                    },
                    ..limits()
                },
            ),
            Err(ContractError::Capacity)
        ));
        assert_eq!(allocation::remaining_allocations(), Some(0));
    });
}

#[test]
fn every_actual_build_and_copy_allocation_can_fail_without_changing_retry_identity() {
    let spec = spec();
    let prepared = allocation::fail_after(0, || {
        let prepared = plan(spec);
        assert_eq!(allocation::remaining_allocations(), Some(0));
        prepared
    });
    let content = prepared.content_hash();
    let intent = prepared.intent_fingerprint();
    let charge = prepared.construction_charge();
    // Named target, check/fallback buffer, quality buffer, description, rubric,
    // contributor set: all six actual owned buffers share the failure injector.
    assert_eq!(prepared.construction_heap_allocations(), 6);
    let original = prepared.build(charge).unwrap();
    for allowed in 0..6 {
        allocation::fail_after(allowed, || {
            assert!(matches!(
                plan(spec).build(charge),
                Err(ContractError::Capacity)
            ));
            assert_eq!(allocation::remaining_allocations(), Some(0));
        });
        let retry = allocation::fail_after(6, || {
            let retry = plan(spec).build(charge).unwrap();
            assert_eq!(allocation::remaining_allocations(), Some(0));
            retry
        });
        descriptor_matches(&retry, spec);
        assert_eq!(retry.content_hash(), content);
        assert_eq!(retry.intent_fingerprint(), intent);
        assert_eq!(retry.retained_bytes().unwrap(), charge);
        allocation::fail_after(allowed, || {
            assert!(matches!(
                original.try_copy(charge),
                Err(ContractError::Capacity)
            ));
            assert_eq!(allocation::remaining_allocations(), Some(0));
        });
        let copied = allocation::fail_after(6, || {
            let copied = original.try_copy(charge).unwrap();
            assert_eq!(allocation::remaining_allocations(), Some(0));
            copied
        });
        descriptor_matches(&copied, spec);
        assert_eq!(copied.intent_fingerprint(), intent);
        assert_eq!(copied.copy_charge().unwrap(), charge);
        assert_eq!(copied.copy_heap_allocations().unwrap(), 6);
        assert_ne!(
            copied.description().as_ptr(),
            original.description().as_ptr()
        );
        assert_ne!(
            copied.quality_bar().unwrap().as_ptr(),
            original.quality_bar().unwrap().as_ptr()
        );
        assert_ne!(
            copied.contributed_by().as_ptr(),
            original.contributed_by().as_ptr()
        );
        descriptor_matches(&original, spec);
    }
    allocation::fail_after(0, || {
        assert!(matches!(
            original.try_copy(charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(allocation::remaining_allocations(), Some(0));
    });
}
