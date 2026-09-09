use super::*;
use crate::lifecycle::{
    claim_descriptor::{self, ClaimSpec, ScopeSpec},
    validation::{self, HandlerPolicy, PhasePolicy, Program, ProgramView, TargetDeclaration},
    validation_descriptor::{self, ValidationSpec},
};
use crate::{
    ActionType, ContentHash, Deadline, HandlerRef, LedgerId, ObjectId, ObjectKind, ObjectRef,
    OccurrenceId, ParticipantId, Relation, RequirementRef, RootCommandId, ScopeKind, SessionId,
    TenantId, TimerId, ValidationId, ValidationKind, ValidationMode, ValidationPhase, ValidatorId,
};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const SUBJECT: ParticipantId = ParticipantId([2; 16]);
const CLAIM: ClaimId = ClaimId::from_u128(200);
const HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId([3; 16]),
    version: ContentHash([4; 32]),
    agentic: false,
};
const HANDLERS: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &HANDLER,
    attempts: 2,
    proof_schema: ContentHash([5; 32]),
    diagnostic_schema: ContentHash([6; 32]),
}];
const CHECK: aggregation::CheckPolicy = aggregation::CheckPolicy {
    declaration_index: 7,
    validation: ValidationId::from_u128(203),
    mode: ValidationMode::Required,
};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId([7; 16]),
        session: SessionId([8; 16]),
    }
}
fn profile() -> Profile {
    Profile {
        max_responses: 4,
        scope_limits: ScopeLimits {
            scopes: 0,
            roots: 0,
            children: 0,
        },
        created: SessionSeq(1),
    }
}
fn limits() -> Limits {
    Limits {
        relations: 32,
        requirements: 16,
        slots: 16,
        checks: 16,
        visits: 4096,
        bytes: 65536,
    }
}
fn specification(
    id: u128,
    index: u32,
    target: TargetDeclaration<'static>,
) -> ValidationSpec<'static> {
    let delivery = target == TargetDeclaration::Delivery;
    ValidationSpec {
        ledger: ledger(),
        id: ValidationId::from_u128(id),
        schema: 1,
        claim: CLAIM,
        issuer: ISSUER,
        declaration_index: index,
        kind: if delivery {
            ValidationKind::Receipt
        } else {
            ValidationKind::Inspection
        },
        phase: match target {
            TargetDeclaration::Admission => ValidationPhase::Admission,
            TargetDeclaration::Increment => ValidationPhase::Increment,
            _ => ValidationPhase::WholeWork,
        },
        mode: ValidationMode::Required,
        target,
        program: if delivery {
            Program::Delivery
        } else {
            Program::Programmatic {
                check: PhasePolicy {
                    evaluator: ParticipantId([9; 16]),
                    definition: ContentHash([10; 32]),
                    handlers: &HANDLERS,
                    required_policy: Some(ContentHash([11; 32])),
                },
                quality: None,
            }
        },
        deadline: Deadline {
            timer: TimerId::from_u128(id),
            generation: 1,
            at: 100,
        },
        description: "Inspect the actual immutable requirement.",
        quality_bar: None,
        contributed_by: &[ISSUER],
        policy_revision: 1,
    }
}
fn validation(spec: ValidationSpec<'_>) -> ValidationDescriptor {
    let plan = ValidationDescriptor::prepare(
        Principal::Actor(spec.issuer),
        spec,
        validation_descriptor::Limits {
            declaration: validation::Limits {
                handlers: 4,
                attempts: 8,
                slot_bytes: 64,
            },
            description_bytes: 256,
            quality_bar_bytes: 256,
            contributors: 4,
            construction_bytes: 4096,
        },
    )
    .unwrap();
    let bytes = plan.construction_charge();
    plan.build(bytes).unwrap()
}
fn descriptors() -> Vec<ValidationDescriptor> {
    vec![
        validation(specification(
            203,
            7,
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output",
            },
        )),
        validation(specification(201, 2, TargetDeclaration::Delivery)),
        validation(specification(202, 5, TargetDeclaration::Admission)),
        validation(specification(204, 9, TargetDeclaration::Increment)),
    ]
}
fn pins(descriptors: &[ValidationDescriptor]) -> Vec<RequirementRef> {
    descriptors
        .iter()
        .map(|descriptor| RequirementRef {
            id: ValidationId(descriptor.binding().object.0),
            specification: descriptor.specification_hash(),
        })
        .collect()
}
fn slots(checks: &[aggregation::CheckPolicy]) -> [aggregation::SlotPolicy<'_>; 2] {
    [
        aggregation::SlotPolicy {
            slot: 4,
            missing_declaration_index: 6,
            mode: ValidationMode::Required,
            checks,
        },
        aggregation::SlotPolicy {
            slot: 99,
            missing_declaration_index: 1,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ]
}
fn relations() -> Vec<Relation> {
    let mut rows = vec![
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ISSUER),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(SUBJECT),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Work),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId([12; 16])),
        },
    ];
    for (kind, id) in [
        (RelationKind::Supersedes, 80),
        (RelationKind::Amends, 81),
        (RelationKind::DependsOn, 90),
        (RelationKind::Awaits, 91),
        (RelationKind::Refines, 92),
    ] {
        rows.push(Relation {
            kind,
            target: RelationTarget::Object(ObjectRef {
                ledger: ledger(),
                kind: ObjectKind::Claim,
                id: ObjectId::from_u128(id),
            }),
        });
    }
    rows.sort();
    rows
}
fn claim(
    requirements: &[RequirementRef],
    slots: &[aggregation::SlotPolicy<'_>],
    relations: &[Relation],
) -> ClaimDescriptor {
    let plan = ClaimDescriptor::prepare(
        ClaimSpec {
            ledger: ledger(),
            id: CLAIM,
            schema: 1,
            occurrence: OccurrenceId([13; 16]),
            description: "Produce the authored output and preserve its complete evidence.",
            relations,
            scopes: &[ScopeSpec {
                kind: ScopeKind::File,
                key: "src",
            }],
            requirements,
            slots,
            deadline: Some(Deadline {
                timer: TimerId([14; 16]),
                generation: 1,
                at: 200,
            }),
            policy: None,
        },
        claim_descriptor::Limits {
            description_bytes: 1024,
            relations: 32,
            scopes: 4,
            scope_key_bytes: 128,
            requirements: 16,
            slots: 16,
            checks: 16,
            construction_bytes: 32768,
        },
    )
    .unwrap();
    let bytes = plan.construction_charge();
    plan.build(bytes).unwrap()
}
fn fixture(descriptors: &[ValidationDescriptor]) -> ClaimDescriptor {
    claim(&pins(descriptors), &slots(&[CHECK]), &relations())
}
fn prepare<'a>(
    claim: &'a ClaimDescriptor,
    descriptors: &'a [ValidationDescriptor],
) -> AuthoredCreationPlan<'a> {
    AuthoredCreationPlan::prepare(
        Principal::Actor(ISSUER),
        claim,
        descriptors,
        profile(),
        limits(),
    )
    .unwrap()
}
fn refuse(claim: &ClaimDescriptor, descriptors: &[ValidationDescriptor]) {
    bytes::fail_after(0, || {
        assert!(
            AuthoredCreationPlan::prepare(
                Principal::Actor(ISSUER),
                claim,
                descriptors,
                profile(),
                limits()
            )
            .is_err()
        );
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
}
fn first_handler(descriptor: &ValidationDescriptor) -> &HandlerRef {
    let ProgramView::Programmatic { check, .. } = descriptor.declaration().program() else {
        panic!("external requirement")
    };
    check.handlers().next().unwrap().handler
}

#[test]
fn full_projection_matches_existing_acceptance_and_borrows_original_handlers() {
    let mut descriptors = descriptors();
    let claim = fixture(&descriptors);
    // A separate checked legacy projection supplies the semantic/identity
    // reference. These test-only copies are absent from authored assembly.
    let declarations: Vec<_> = descriptors
        .iter()
        .map(|descriptor| {
            let declaration = descriptor.declaration();
            declaration
                .try_copy(declaration.copy_charge().unwrap())
                .unwrap()
        })
        .collect();
    let authored_slots: Vec<_> = claim.slots().collect();
    let expected = aggregation::AcceptancePolicy::new(
        claim.binding(),
        ISSUER,
        &authored_slots,
        &declarations,
        limits().acceptance(),
    )
    .unwrap();
    for _ in 0..descriptors.len() {
        let handler = first_handler(
            descriptors
                .iter()
                .find(|row| row.declaration().declaration_index() == 7)
                .unwrap(),
        ) as *const HandlerRef;
        let plan = bytes::fail_after(0, || prepare(&claim, &descriptors));
        let quoted_visits = plan.visits().unwrap();
        let projection = plan.build(65536).unwrap();
        assert_eq!(projection.visits(), quoted_visits);
        assert_eq!(projection.definition().acceptance, expected);
        assert!(std::ptr::eq(projection.claim_descriptor(), &claim));
        for (actual, source) in projection.declarations().zip(&descriptors) {
            assert!(std::ptr::eq(actual, source.declaration()));
        }
        let external = projection
            .descriptors()
            .iter()
            .find(|row| row.declaration().declaration_index() == 7)
            .unwrap();
        assert_eq!(first_handler(external) as *const HandlerRef, handler);
        let definition = projection.into_definition();
        assert_eq!(definition.binding, claim.binding());
        assert_eq!(definition.issuer, ISSUER);
        assert_eq!(definition.subject, SUBJECT);
        assert_eq!(definition.deadline, claim.deadline());
        assert_eq!(definition.max_responses, 4);
        assert_eq!(definition.created, SessionSeq(1));
        assert_eq!(definition.scope_limits, profile().scope_limits);
        assert_eq!(
            definition.graph.obligations(),
            &[
                graph::Obligation {
                    kind: graph::Kind::DependsOn,
                    target: ClaimId::from_u128(90)
                },
                graph::Obligation {
                    kind: graph::Kind::Awaits,
                    target: ClaimId::from_u128(91)
                },
            ]
        );
        assert_eq!(definition.lineage.corrections().len(), 2);
        assert_eq!(
            definition.lineage.corrections()[0].kind,
            CorrectionKind::Supersedes
        );
        assert_eq!(
            definition.lineage.corrections()[1].kind,
            CorrectionKind::Amends
        );
        assert_eq!(definition.lineage.cause(), claim.cause());
        assert_eq!(definition.acceptance.slots().last().unwrap().checks, &[]);
        assert_eq!(
            definition.acceptance.slots().last().unwrap().mode,
            ValidationMode::Observe
        );
        assert!(
            claim
                .relations()
                .iter()
                .any(|row| row.kind == RelationKind::Refines)
        );
        descriptors.rotate_left(1);
    }
    let definition = prepare(&claim, &descriptors)
        .build(65536)
        .unwrap()
        .into_definition();
    // Consuming the projection ends its borrows; original ownership can move
    // into the future immutable store without any declaration clone.
    let originals = (claim, descriptors);
    assert_eq!(definition.binding, originals.0.binding());
}

#[test]
fn exact_specification_pins_and_complete_source_family_are_mandatory() {
    let descriptors = descriptors();
    let claim = fixture(&descriptors);
    refuse(&claim, &descriptors[1..]);
    let mut sources = self::descriptors();
    sources.push(validation(specification(
        205,
        12,
        TargetDeclaration::Admission,
    )));
    refuse(&claim, &sources);
    let mut sources = self::descriptors();
    sources[2] = validation(specification(201, 2, TargetDeclaration::Delivery));
    refuse(&claim, &sources); // duplicate object ID, even with identical content
    let mut wrong_pin = pins(&descriptors);
    wrong_pin[0].specification = descriptors[0].content_hash();
    assert_ne!(
        wrong_pin[0].specification,
        descriptors[0].specification_hash()
    );
    refuse(
        &self::claim(&wrong_pin, &slots(&[CHECK]), &relations()),
        &descriptors,
    );

    let mut rebound = specification(
        203,
        7,
        TargetDeclaration::WholeWorkSlot {
            index: 4,
            name: "output",
        },
    );
    rebound.claim = ClaimId::from_u128(300);
    let rebound = validation(rebound);
    assert_eq!(
        rebound.specification_hash(),
        descriptors[0].specification_hash()
    );
    assert_ne!(rebound.content_hash(), descriptors[0].content_hash());
    let mut sources = self::descriptors();
    sources[0] = rebound;
    refuse(&claim, &sources);

    let mut wrong_issuer = specification(202, 5, TargetDeclaration::Admission);
    wrong_issuer.issuer = SUBJECT;
    let mut sources = self::descriptors();
    sources[2] = validation(wrong_issuer);
    refuse(&fixture(&sources), &sources);
    let mut foreign = specification(202, 5, TargetDeclaration::Admission);
    foreign.ledger.session = SessionId([55; 16]);
    sources[2] = validation(foreign);
    refuse(&fixture(&sources), &sources);
}

#[test]
fn delivery_and_slot_correspondence_include_zero_check_presence_and_distinct_indices() {
    let descriptors = descriptors();
    let no_delivery: Vec<_> = self::descriptors()
        .into_iter()
        .filter(|row| row.declaration().target() != TargetDeclaration::Delivery)
        .collect();
    refuse(&fixture(&no_delivery), &no_delivery);
    refuse(&claim(&pins(&descriptors), &[], &relations()), &descriptors);
    let mismatched = [aggregation::CheckPolicy {
        mode: ValidationMode::Observe,
        ..CHECK
    }];
    refuse(
        &claim(&pins(&descriptors), &slots(&mismatched), &relations()),
        &descriptors,
    );
    let mut collided = slots(&[CHECK]);
    collided[1].missing_declaration_index = 5; // actual Admission declaration
    refuse(
        &claim(&pins(&descriptors), &collided, &relations()),
        &descriptors,
    );
    let mut duplicate_index = self::descriptors();
    duplicate_index[2] = validation(specification(202, 2, TargetDeclaration::Admission));
    refuse(&fixture(&duplicate_index), &duplicate_index);
}

#[test]
fn typed_claim_and_validation_ids_can_share_bytes() {
    let descriptors = [validation(specification(
        200,
        2,
        TargetDeclaration::Delivery,
    ))];
    let claim = claim(&pins(&descriptors), &[], &relations());
    assert_eq!(claim.id().0, descriptors[0].binding().object.0);
    let projection = prepare(&claim, &descriptors).build(65536).unwrap();
    assert_eq!(projection.definition().acceptance.declarations().len(), 1);
    assert_eq!(
        projection.declarations().next().unwrap().claim(),
        claim.id()
    );
}

#[test]
fn exact_byte_and_visit_quotes_cover_build_and_refuse_before_allocation() {
    let descriptors = descriptors();
    let claim = fixture(&descriptors);
    let plan = prepare(&claim, &descriptors);
    let count = plan.visits().unwrap();
    let charge = plan.construction_bytes();
    assert!(
        count < 1024,
        "ordinary four-requirement assembly should fit a small traversal allowance"
    );
    bytes::fail_after(0, || {
        assert!(matches!(
            AuthoredCreationPlan::prepare(
                Principal::Actor(ISSUER),
                &claim,
                &descriptors,
                profile(),
                Limits {
                    visits: count - 1,
                    ..limits()
                }
            ),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            AuthoredCreationPlan::prepare(
                Principal::Actor(ISSUER),
                &claim,
                &descriptors,
                profile(),
                Limits {
                    bytes: charge - 1,
                    ..limits()
                }
            ),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            prepare(&claim, &descriptors).build(charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    let plan = bytes::fail_after(0, || {
        AuthoredCreationPlan::prepare(
            Principal::Actor(ISSUER),
            &claim,
            &descriptors,
            profile(),
            Limits {
                visits: count,
                bytes: charge,
                ..limits()
            },
        )
        .unwrap()
    });
    let built = plan.build(charge).unwrap();
    assert_eq!(built.visits(), count);
    assert_eq!(built.definition().scope_limits, profile().scope_limits); // all zero accepted
}

#[test]
fn independent_dimensions_and_owner_profile_refuse_in_borrowed_preparation() {
    let descriptors = descriptors();
    let claim = fixture(&descriptors);
    for limits in [
        Limits {
            relations: claim.relations().len() - 1,
            ..limits()
        },
        Limits {
            requirements: descriptors.len() - 1,
            ..limits()
        },
        Limits {
            slots: 1,
            ..limits()
        },
        Limits {
            checks: 0,
            ..limits()
        },
        Limits {
            visits: 0,
            ..limits()
        },
    ] {
        bytes::fail_after(0, || {
            assert!(matches!(
                AuthoredCreationPlan::prepare(
                    Principal::Actor(ISSUER),
                    &claim,
                    &descriptors,
                    profile(),
                    limits
                ),
                Err(ContractError::Capacity)
            ))
        });
    }
    for profile in [
        Profile {
            max_responses: 0,
            ..profile()
        },
        Profile {
            created: SessionSeq(0),
            ..profile()
        },
    ] {
        bytes::fail_after(0, || {
            assert!(
                AuthoredCreationPlan::prepare(
                    Principal::Actor(ISSUER),
                    &claim,
                    &descriptors,
                    profile,
                    limits()
                )
                .is_err()
            )
        });
    }
    bytes::fail_after(0, || {
        assert!(matches!(
            AuthoredCreationPlan::prepare(
                Principal::Actor(SUBJECT),
                &claim,
                &descriptors,
                profile(),
                limits()
            ),
            Err(ContractError::WrongActor)
        ))
    });
}

#[test]
fn every_owned_allocation_refusal_preserves_the_actual_inputs_for_retry() {
    let descriptors = descriptors();
    let claim = fixture(&descriptors);
    let plan = prepare(&claim, &descriptors);
    let allocations = plan.construction_allocations();
    let charge = plan.construction_bytes();
    let original = (
        claim.content_hash(),
        descriptors[0].content_hash(),
        first_handler(&descriptors[0]) as *const HandlerRef,
    );
    for failure in 0..allocations {
        let result = bytes::fail_after(failure, || prepare(&claim, &descriptors).build(charge));
        assert!(
            matches!(result, Err(ContractError::Capacity)),
            "allocation {failure}"
        );
        assert_eq!(
            (
                claim.content_hash(),
                descriptors[0].content_hash(),
                first_handler(&descriptors[0]) as *const HandlerRef
            ),
            original
        );
    }
    let built = bytes::fail_after(allocations, || {
        let projection = prepare(&claim, &descriptors).build(charge).unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        projection
    });
    assert_eq!(
        built.visits(),
        prepare(&claim, &descriptors).visits().unwrap()
    );
}
