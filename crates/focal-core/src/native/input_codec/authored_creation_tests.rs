use super::*;
use focal_model::lifecycle::aggregation::{CheckPolicy, SlotPolicy};
use focal_model::{
    ActionType, HandlerRef, OccurrenceId, Relation, RelationKind, RelationTarget, RequestEpoch,
    RequestId, RequirementRef, RootCommandId, SessionId, TenantId, ValidationKind, ValidationMode,
    ValidationPhase, ValidatorId,
};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId([3; 16]),
    version: ContentHash([4; 32]),
    agentic: false,
};
const QUALITY: HandlerRef = HandlerRef {
    id: ValidatorId([5; 16]),
    version: ContentHash([6; 32]),
    agentic: true,
};
const CHECK: [validation::HandlerPolicy<'static>; 1] = [validation::HandlerPolicy {
    handler: &HANDLER,
    attempts: 2,
    proof_schema: ContentHash([7; 32]),
    diagnostic_schema: ContentHash([8; 32]),
}];
const QUALITY_CHECK: [validation::HandlerPolicy<'static>; 1] = [validation::HandlerPolicy {
    handler: &QUALITY,
    attempts: 1,
    proof_schema: ContentHash([9; 32]),
    diagnostic_schema: ContentHash([10; 32]),
}];
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId([1; 16]),
        session: SessionId([2; 16]),
    }
}
fn work() -> AuthoredCreationWork {
    AuthoredCreationWork {
        parse: 100_000_000,
        source: 100_000_000,
        descriptor: 100_000_000,
        acceptance: 100_000_000,
        native: 100_000_000,
    }
}
fn native() -> NativeLimits {
    NativeLimits {
        preparation_bytes: 16 * 1024 * 1024,
        definitions: 64,
        plan_nodes: 16,
        ..NativeLimits::default()
    }
}
fn limits() -> AuthoredCreationLimits {
    AuthoredCreationLimits {
        claim: claim::Limits {
            description_bytes: 256,
            relations: 16,
            scopes: 8,
            scope_key_bytes: 64,
            requirements: 16,
            slots: 8,
            checks: 16,
            construction_bytes: 65_536,
        },
        validation: descriptor::Limits {
            declaration: validation::Limits {
                handlers: 8,
                attempts: 16,
                slot_bytes: 64,
            },
            description_bytes: 256,
            quality_bar_bytes: 256,
            contributors: 8,
            construction_bytes: 65_536,
        },
        acceptance: aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 16,
            max_updates: 16,
        },
        work: work(),
    }
}
fn deadline() -> Deadline {
    Deadline {
        timer: TimerId([11; 16]),
        generation: 1,
        at: 9999,
    }
}
fn definition(id: u128, parent: u128, delivery: bool) -> descriptor::ValidationDescriptor {
    let phase = |agentic| validation::PhasePolicy {
        evaluator: ISSUER,
        definition: ContentHash([12; 32]),
        handlers: if agentic { &QUALITY_CHECK } else { &CHECK },
        required_policy: None,
    };
    let plan = descriptor::ValidationDescriptor::prepare(
        Principal::Actor(ISSUER),
        descriptor::ValidationSpec {
            ledger: ledger(),
            id: ValidationId::from_u128(id),
            schema: 1,
            claim: ClaimId::from_u128(parent),
            issuer: ISSUER,
            declaration_index: if delivery { 1 } else { 2 },
            kind: if delivery {
                ValidationKind::Receipt
            } else {
                ValidationKind::Inspection
            },
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: if delivery {
                validation::TargetDeclaration::Delivery
            } else {
                validation::TargetDeclaration::WholeWorkSlot {
                    index: 1,
                    name: "résultat",
                }
            },
            program: if delivery {
                validation::Program::Delivery
            } else {
                validation::Program::Programmatic {
                    check: phase(false),
                    quality: Some(phase(true)),
                }
            },
            deadline: deadline(),
            description: "Assess exact evidence, including failures.",
            quality_bar: if delivery {
                None
            } else {
                Some("Cite the errors and test results.")
            },
            contributed_by: &[ISSUER],
            policy_revision: 3,
        },
        limits().validation,
    )
    .unwrap();
    let bytes = plan.construction_charge();
    plan.build(bytes).unwrap()
}
#[derive(Clone, Copy)]
enum Change {
    None,
    MissingDelivery,
    WrongPin,
    WrongParent,
}
fn proposal(id: u128, change: Change) -> NativeAuthoredProposal {
    let delivery = definition(id * 10, id, true);
    let checking = definition(
        id * 10 + 1,
        if matches!(change, Change::WrongParent) {
            id + 100
        } else {
            id
        },
        false,
    );
    let mut requirements = Vec::new();
    if !matches!(change, Change::MissingDelivery) {
        requirements.push(RequirementRef {
            id: ValidationId(delivery.binding().object.0),
            specification: delivery.specification_hash(),
        });
    }
    requirements.push(RequirementRef {
        id: ValidationId(checking.binding().object.0),
        specification: if matches!(change, Change::WrongPin) {
            ContentHash([99; 32])
        } else {
            checking.specification_hash()
        },
    });
    let relations = [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ISSUER),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(ParticipantId([2; 16])),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Challenge),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId([13; 16])),
        },
    ];
    let checks = [CheckPolicy {
        declaration_index: 2,
        validation: ValidationId(checking.binding().object.0),
        mode: ValidationMode::Required,
    }];
    let slots = [
        SlotPolicy {
            slot: 1,
            missing_declaration_index: 100,
            mode: ValidationMode::Required,
            checks: &checks,
        },
        SlotPolicy {
            slot: 2,
            missing_declaration_index: 101,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ];
    let plan = claim::ClaimDescriptor::prepare(
        claim::ClaimSpec {
            ledger: ledger(),
            id: ClaimId::from_u128(id),
            schema: 1,
            occurrence: OccurrenceId::from_u128(id),
            description: "Fix the regression; report failures as evidence.",
            relations: &relations,
            scopes: &[],
            requirements: &requirements,
            slots: &slots,
            deadline: Some(deadline()),
            policy: None,
        },
        limits().claim,
    )
    .unwrap();
    let bytes = plan.construction_charge();
    let content = plan.build(bytes).unwrap();
    let declarations = if matches!(change, Change::MissingDelivery) {
        vec![checking]
    } else {
        vec![checking, delivery]
    };
    NativeAuthoredProposal {
        content,
        declarations,
        max_responses: 3,
        scope_limits: scope::ScopeLimits {
            scopes: 0,
            roots: 0,
            children: 0,
        },
        owner: None,
    }
}
fn input(claims: Vec<NativeAuthoredProposal>) -> NativeInput {
    NativeInput {
        request: RequestKey {
            principal: ISSUER,
            epoch: RequestEpoch(1),
            id: RequestId([14; 16]),
        },
        command: NativeCommand::CreateAuthored { claims },
    }
}
fn encoded(input: &NativeInput) -> Vec<u8> {
    let plan = EncodingPlan::prepare(
        InputFrame::Request {
            ledger: ledger(),
            profile: NativeContentProfile::AuthoredV1,
            input,
        },
        EncodingLimits {
            bytes: usize::MAX,
            visits: usize::MAX,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn inspect(bytes: &[u8]) -> StructuralInput<'_> {
    StructuralInput::inspect(
        bytes,
        InspectionLimits {
            bytes: bytes.len(),
            visits: usize::MAX,
            items: usize::MAX,
            text_bytes: usize::MAX,
            blob_bytes: usize::MAX,
        },
    )
    .unwrap()
}

#[test]
fn complete_authored_batch_preserves_nested_bodies_order_and_full_intent_at_exact_limits() {
    let expected = input(vec![proposal(1, Change::None), proposal(2, Change::None)]);
    let bytes = encoded(&expected);
    let frame = inspect(&bytes);
    let plan = frame
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap();
    let quote = plan.quote();
    assert_eq!(quote.claims, 2);
    assert_eq!(quote.declarations, 4);
    assert_eq!(
        plan.intent_fingerprint(),
        super::super::super::intent::fingerprint(ledger(), &expected).unwrap()
    );
    let built = plan.build(quote.bytes, quote.construction).unwrap();
    assert_eq!(encoded(&built), bytes);
    let plan = frame
        .prepare_authored_creation(
            native(),
            AuthoredCreationLimits {
                work: quote.preparation,
                ..limits()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(plan.quote(), quote);
    let built = plan.build(quote.bytes, quote.construction).unwrap();
    assert_eq!(encoded(&built), bytes);
}

#[test]
fn every_work_domain_and_final_bytes_refuse_one_below_the_quote() {
    let bytes = encoded(&input(vec![proposal(1, Change::None)]));
    let frame = inspect(&bytes);
    let quote = frame
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap()
        .quote();
    for domain in 0..5 {
        let mut work = quote.preparation;
        let field = match domain {
            0 => &mut work.parse,
            1 => &mut work.source,
            2 => &mut work.descriptor,
            3 => &mut work.acceptance,
            _ => &mut work.native,
        };
        assert!(*field > 0);
        *field -= 1;
        assert!(
            frame
                .prepare_authored_creation(native(), AuthoredCreationLimits { work, ..limits() })
                .is_err(),
            "prepare domain {domain}"
        );
    }
    for domain in [0, 1, 2, 4] {
        let mut work = quote.construction;
        let field = match domain {
            0 => &mut work.parse,
            1 => &mut work.source,
            2 => &mut work.descriptor,
            _ => &mut work.native,
        };
        assert!(*field > 0);
        *field -= 1;
        let plan = frame
            .prepare_authored_creation(native(), limits())
            .unwrap()
            .unwrap();
        assert!(plan.build(quote.bytes, work).is_err());
    }
    let plan = frame
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap();
    assert!(plan.build(quote.bytes - 1, quote.construction).is_err());
    let plan = frame
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap();
    assert_eq!(
        encoded(&plan.build(quote.bytes, quote.construction).unwrap()),
        bytes
    );
}

#[test]
fn complete_local_correspondence_rejects_missing_delivery_wrong_pin_and_parent() {
    for change in [
        Change::MissingDelivery,
        Change::WrongPin,
        Change::WrongParent,
    ] {
        let bytes = encoded(&input(vec![proposal(1, change)]));
        assert!(
            inspect(&bytes)
                .prepare_authored_creation(native(), limits())
                .is_err()
        );
    }
    let bytes = encoded(&input(vec![
        proposal(1, Change::None),
        proposal(1, Change::None),
    ]));
    assert!(
        inspect(&bytes)
            .prepare_authored_creation(native(), limits())
            .is_err()
    );
}

#[test]
fn full_frame_resource_and_request_principal_limits_are_enforced() {
    let bytes = encoded(&input(vec![proposal(1, Change::None)]));
    let frame = inspect(&bytes);
    let quote = frame
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap()
        .quote();
    assert!(
        frame
            .prepare_authored_creation(
                NativeLimits {
                    preparation_bytes: quote.bytes - 1,
                    ..native()
                },
                limits()
            )
            .is_err()
    );
    assert!(
        frame
            .prepare_authored_creation(
                NativeLimits {
                    definitions: 1,
                    ..native()
                },
                limits()
            )
            .is_err()
    );
    let mut wrong = bytes;
    wrong[44..60].copy_from_slice(&[99; 16]);
    assert!(
        inspect(&wrong)
            .prepare_authored_creation(native(), limits())
            .is_err()
    );
}

#[test]
fn decoded_authored_creation_enters_the_real_ram_owner_without_extra_testimony() {
    let bytes = encoded(&input(vec![proposal(1, Change::None)]));
    let plan = inspect(&bytes)
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap();
    let quote = plan.quote();
    let input = plan.build(quote.bytes, quote.construction).unwrap();
    let mut core = Core::new_native_authored(
        ledger(),
        RangeId(905),
        native(),
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let prepared = core
        .prepare_native(
            NativeContext {
                principal: Principal::Actor(ISSUER),
                logical_time: 0,
            },
            input,
            &[],
        )
        .unwrap();
    let NativePreparation::Prepared(prepared) = prepared else {
        panic!("fresh request returned existing");
    };
    core.publish_native(prepared).unwrap();
    assert_eq!(
        core.native_claim_content(ClaimId::from_u128(1))
            .unwrap()
            .description(),
        "Fix the regression; report failures as evidence."
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0
    );
}

#[test]
fn raw_authored_creation_reaches_managed_owner_and_preserves_exact_retries() {
    let bytes = encoded(&input(vec![proposal(1, Change::None)]));
    let limits = NativeDecodeLimits::for_native(native(), bytes.len(), work().into()).unwrap();
    let preparation = limits
        .with_request(native(), &bytes, |_, quote| quote.preparation)
        .unwrap();
    let retry_limits = NativeDecodeLimits {
        work: preparation,
        ..limits
    };
    let core = Core::new_native_authored(
        ledger(),
        RangeId(906),
        native(),
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let mut owner = NativeOwner::new(core).unwrap();
    let context = NativeContext {
        principal: Principal::Actor(ISSUER),
        logical_time: 0,
    };
    let NativeStaging::Prepared { candidate, outcome } =
        owner.prepare_frame(context, &bytes, limits, None).unwrap()
    else {
        panic!("fresh request returned existing");
    };
    assert!(owner.committed().claim(ClaimId::from_u128(1)).is_none());
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0
    );
    assert!(matches!(
        owner.prepare_frame(context, &bytes, retry_limits, None).unwrap(),
        NativeStaging::Existing { candidate: Some(found), outcome: result }
            if found == candidate && result == outcome
    ));
    // Exercise the publication boundary only; this RAM test does not write a WAL.
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        0
    );
    assert!(matches!(
        owner.prepare_frame(context, &bytes, retry_limits, None).unwrap(),
        NativeStaging::Existing { candidate: None, outcome: result } if result == outcome
    ));
}

#[test]
fn final_native_capacity_inspection_prices_each_additional_scope_and_slot() {
    let original = encoded(&input(vec![proposal(1, Change::None)]));
    let original_quote = inspect(&original)
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap()
        .quote();
    let mut expanded = proposal(1, Change::None);
    let mut slots: Vec<_> = expanded.content.slots().collect();
    slots.push(SlotPolicy {
        slot: 3,
        missing_declaration_index: 102,
        mode: ValidationMode::Observe,
        checks: &[],
    });
    let scopes = [claim::ScopeSpec {
        kind: focal_model::ScopeKind::File,
        key: "src/lib.rs",
    }];
    let plan = claim::ClaimDescriptor::prepare(
        claim::ClaimSpec {
            ledger: expanded.content.ledger(),
            id: expanded.content.id(),
            schema: expanded.content.schema(),
            occurrence: expanded.content.occurrence(),
            description: expanded.content.description(),
            relations: expanded.content.relations(),
            scopes: &scopes,
            requirements: expanded.content.requirements(),
            slots: &slots,
            deadline: expanded.content.deadline(),
            policy: None,
        },
        limits().claim,
    )
    .unwrap();
    let charge = plan.construction_charge();
    expanded.content = plan.build(charge).unwrap();
    let bytes = encoded(&input(vec![expanded]));
    let frame = inspect(&bytes);
    let plan = frame
        .prepare_authored_creation(native(), limits())
        .unwrap()
        .unwrap();
    let quote = plan.quote();
    assert_eq!(
        quote.construction.native,
        original_quote.construction.native + 4
    );
    assert_eq!(
        encoded(&plan.build(quote.bytes, quote.construction).unwrap()),
        bytes
    );
}
