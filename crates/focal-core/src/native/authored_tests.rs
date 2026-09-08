use super::*;
use focal_model::lifecycle::{
    claim_descriptor::{self, ClaimSpec, ScopeSpec},
    validation_descriptor::{self, ValidationSpec},
};
use focal_model::{
    ActionType, Deadline, ObjectKind, ObjectRef, OccurrenceId, Relation, RelationKind,
    RequestEpoch, RequestId, RootCommandId, ScopeKind, SessionId, TenantId, TimerId,
    ValidationKind, ValidationMode, ValidationPhase,
};

#[path = "authored_integrity_tests.rs"]
mod integrity_tests;
#[path = "authored_lifecycle_tests.rs"]
mod lifecycle_tests;

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const SUBJECT: ParticipantId = ParticipantId::from_u128(2);

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(1),
    }
}

fn limits() -> NativeLimits {
    NativeLimits {
        range: RangeConfig {
            page_entries: 8,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 32,
        plan_edges: 4096,
        preparation_bytes: 1024 * 1024,
        ..NativeLimits::default()
    }
}

fn core() -> Core<NativeState> {
    Core::new_native_authored(
        ledger(),
        RangeId(405),
        limits(),
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn declaration(id: u128, claim: u128, issuer: ParticipantId) -> ValidationDescriptor {
    let contributors = [issuer];
    let plan = ValidationDescriptor::prepare(
        Principal::Actor(issuer),
        ValidationSpec {
            ledger: ledger(),
            id: ValidationId::from_u128(id),
            schema: 1,
            claim: ClaimId::from_u128(claim),
            issuer,
            declaration_index: 0,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(id),
                generation: 1,
                at: 1000,
            },
            description: "Record delivery of the actual respondent testimony.",
            quality_bar: None,
            contributed_by: &contributors,
            policy_revision: 1,
        },
        validation_descriptor::Limits {
            declaration: validation::Limits {
                handlers: 4,
                attempts: 8,
                slot_bytes: 64,
            },
            description_bytes: 256,
            quality_bar_bytes: 256,
            contributors: 4,
            construction_bytes: 8192,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}

fn claim_content(
    id: u128,
    subject: ParticipantId,
    action: ActionType,
    pins: &[focal_model::RequirementRef],
    extras: &[Relation],
) -> ClaimDescriptor {
    let mut relations = vec![
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ISSUER),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(subject),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(action),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(900)),
        },
    ];
    relations.extend_from_slice(extras);
    relations.sort();
    let plan = ClaimDescriptor::prepare(
        ClaimSpec {
            ledger: ledger(),
            id: ClaimId::from_u128(id),
            schema: 1,
            occurrence: OccurrenceId::from_u128(id),
            description: "Inspect the supplied source and retain the findings.",
            relations: &relations,
            scopes: &[ScopeSpec {
                kind: ScopeKind::File,
                key: "src",
            }],
            requirements: pins,
            slots: &[],
            deadline: None,
        },
        claim_descriptor::Limits {
            description_bytes: 256,
            relations: 32,
            scopes: 4,
            scope_key_bytes: 64,
            requirements: 8,
            slots: 8,
            checks: 8,
            construction_bytes: 16384,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}

fn proposal_with(
    id: u128,
    validation_id: u128,
    subject: ParticipantId,
    action: ActionType,
    extras: &[Relation],
) -> NativeAuthoredProposal {
    let declaration = declaration(validation_id, id, ISSUER);
    let pins = [focal_model::RequirementRef {
        id: ValidationId::from_u128(validation_id),
        specification: declaration.specification_hash(),
    }];
    NativeAuthoredProposal {
        content: claim_content(id, subject, action, &pins, extras),
        declarations: vec![declaration],
        max_responses: 4,
        scope_limits: scope::ScopeLimits {
            scopes: 0,
            roots: 0,
            children: 0,
        },
        owner: None,
    }
}

fn proposal(id: u128, validation_id: u128) -> NativeAuthoredProposal {
    proposal_with(id, validation_id, SUBJECT, ActionType::Work, &[])
}

fn relation(kind: RelationKind, id: u128) -> Relation {
    Relation {
        kind,
        target: RelationTarget::Object(ObjectRef {
            ledger: ledger(),
            kind: ObjectKind::Claim,
            id: ObjectId::from_u128(id),
        }),
    }
}

fn key(id: u128) -> RequestKey {
    RequestKey {
        principal: ISSUER,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(id),
    }
}

fn context(actor: ParticipantId) -> NativeContext {
    NativeContext {
        principal: Principal::Actor(actor),
        logical_time: 0,
    }
}

fn input(request: u128, command: NativeCommand) -> NativeInput {
    NativeInput {
        request: key(request),
        command,
    }
}

fn create(request: u128, claims: Vec<NativeAuthoredProposal>) -> NativeInput {
    input(request, NativeCommand::CreateAuthored { claims })
}

fn prepare(
    core: &Core<NativeState>,
    input: NativeInput,
    pending: &[&NativePrepared],
) -> NativePrepared {
    match core
        .prepare_native(context(ISSUER), input, pending)
        .unwrap()
    {
        NativePreparation::Prepared(prepared) => prepared,
        NativePreparation::Existing { .. } => panic!("unexpected exact request retry"),
    }
}

fn publish(core: &mut Core<NativeState>, input: NativeInput) -> NativeOutcome {
    let prepared = prepare(core, input, &[]);
    core.publish_native(prepared).unwrap()
}

#[test]
fn authored_bodies_policy_and_resolution_move_into_one_atomic_prefix() {
    let mut core = core();
    let authored = proposal(1, 1); // Numeric IDs intentionally repeat across families.
    let claim_pointer = authored.content.description().as_ptr();
    let definition_pointer = authored.declarations[0].description().as_ptr();
    let binding = authored.content.binding();
    let definition_binding = authored.declarations[0].binding();
    let prepared = prepare(&core, create(1, vec![authored]), &[]);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim_content(ClaimId::from_u128(1)).is_none());
    assert!(core.native_creation_result(key(1)).is_none());
    let body = prepared.claim_content(ClaimId::from_u128(1)).unwrap();
    let descriptor = prepared
        .validation_descriptor(ValidationId::from_u128(1))
        .unwrap();
    assert_eq!(body.description().as_ptr(), claim_pointer);
    assert_eq!(descriptor.description().as_ptr(), definition_pointer);
    let state = prepared.claim(ClaimId::from_u128(1)).unwrap();
    assert_eq!(state.binding(), binding);
    state
        .acceptance()
        .check_declaration(descriptor.declaration())
        .unwrap();
    assert!(std::ptr::eq(
        prepared.definition(ValidationId::from_u128(1)).unwrap(),
        descriptor.declaration(),
    ));
    let mapping = prepared.creation_result(key(1)).unwrap().entries();
    assert_eq!(mapping.len(), 2);
    assert_eq!(mapping[0].family, NativeCreatedFamily::Claim);
    assert_eq!(mapping[1].family, NativeCreatedFamily::Validation);
    assert_eq!(mapping[0].content, binding.content);
    assert_eq!(mapping[1].content, definition_binding.content);
    for object in mapping {
        assert_eq!(object.requested, object.resolved);
    }
    let outcome = core.publish_native(prepared).unwrap();
    assert_eq!(
        (outcome.created, outcome.definitions, outcome.events),
        (1, 1, 2)
    );
    assert_eq!(core.native_sequence(), outcome.sequence);
    assert_eq!(
        core.native_claim_content(ClaimId::from_u128(1))
            .unwrap()
            .description()
            .as_ptr(),
        claim_pointer
    );
    assert_eq!(
        core.native_validation_descriptor(ValidationId::from_u128(1))
            .unwrap()
            .description()
            .as_ptr(),
        definition_pointer
    );
    assert_eq!(
        core.native_creation_result(key(1)).unwrap().entries().len(),
        2
    );
    assert!(matches!(
        core.native_event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Created,
            ..
        })
    ));
    assert!(
        matches!(core.native_event(outcome.sequence, 1).unwrap().fact,
        NativeFact::Definition { binding, .. } if binding == definition_binding)
    );
}

#[test]
fn pending_authored_identity_retry_and_discard_preserve_atomicity() {
    let mut core = core();
    let baseline = core.native_budget();
    let prepared = prepare(&core, create(10, vec![proposal(1, 101)]), &[]);
    assert!(matches!(
        core.prepare_native(
            context(ISSUER),
            create(10, vec![proposal(1, 101)]),
            &[&prepared]
        )
        .unwrap(),
        NativePreparation::Existing {
            committed: false,
            ..
        }
    ));
    let mut changed = proposal(1, 101);
    changed.max_responses = 5;
    assert!(matches!(
        core.prepare_native(context(ISSUER), create(10, vec![changed]), &[&prepared]),
        Err(NativeError::RequestConflict)
    ));
    let duplicate = prepare(&core, create(11, vec![proposal(1, 101)]), &[&prepared]);
    assert_eq!(duplicate.outcome().created, 0);
    assert_eq!(
        duplicate.creation_result(key(11)).unwrap().entries(),
        prepared.creation_result(key(10)).unwrap().entries()
    );
    drop(duplicate);
    drop(prepared);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
    assert!(
        core.native_validation_descriptor(ValidationId::from_u128(101))
            .is_none()
    );
    assert!(core.native_creation_result(key(10)).is_none());
    assert_eq!(core.native_budget().used, baseline.used);
    let accepted = publish(&mut core, create(10, vec![proposal(1, 101)]));
    assert!(matches!(
        core.prepare_native(context(ISSUER), create(10, vec![proposal(1, 101)]), &[]).unwrap(),
        NativePreparation::Existing { outcome, committed: true } if outcome == accepted
    ));
}

#[test]
fn all_existing_results_stay_frozen_and_partial_batches_refuse() {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1, 101)]));
    let binding = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(
        &mut core,
        input(2, NativeCommand::Post { expected: binding }),
    );
    let posted = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let old = core
        .native_creation_result(key(1))
        .unwrap()
        .entries()
        .to_vec();
    let duplicate = publish(&mut core, create(3, vec![proposal(1, 101)]));
    assert_eq!(
        (
            duplicate.created,
            duplicate.changed,
            duplicate.definitions,
            duplicate.events
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().binding(),
        posted
    );
    assert_eq!(core.native_creation_result(key(3)).unwrap().entries(), old);
    let before = core.native_budget().used;
    assert!(matches!(
        core.prepare_native(
            context(ISSUER),
            create(4, vec![proposal(1, 101), proposal(2, 102)]),
            &[]
        ),
        Err(NativeError::Contract(ContractError::ContentConflict))
    ));
    assert_eq!(core.native_sequence(), duplicate.sequence);
    assert_eq!(core.native_budget().used, before);
    assert!(core.native_claim(ClaimId::from_u128(2)).is_none());
    assert!(core.native_creation_result(key(4)).is_none());
    publish(
        &mut core,
        input(5, NativeCommand::Cancel { expected: posted }),
    );
    assert_eq!(core.native_creation_result(key(1)).unwrap().entries(), old);
    assert_eq!(core.native_creation_result(key(3)).unwrap().entries(), old);
}

#[test]
fn contextual_references_use_the_complete_batch_and_pending_prefix() {
    let mut core = core();
    let extra = [relation(RelationKind::Reviews, 2)];
    let missing = proposal_with(1, 101, SUBJECT, ActionType::Consultation, &extra);
    assert!(matches!(
        core.prepare_native(context(ISSUER), create(1, vec![missing]), &[]),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim_content(ClaimId::from_u128(1)).is_none());
    let pending = prepare(&core, create(2, vec![proposal(2, 102)]), &[]);
    let reviews = prepare(
        &core,
        create(
            3,
            vec![proposal_with(
                1,
                101,
                SUBJECT,
                ActionType::Consultation,
                &extra,
            )],
        ),
        &[&pending],
    );
    assert!(reviews.claim_content(ClaimId::from_u128(2)).is_some());
    core.publish_native(pending).unwrap();
    core.publish_native(reviews).unwrap();
    let cyclic = [
        proposal_with(
            3,
            103,
            SUBJECT,
            ActionType::Work,
            &[relation(RelationKind::DependsOn, 4)],
        ),
        proposal_with(
            4,
            104,
            SUBJECT,
            ActionType::Work,
            &[relation(RelationKind::Awaits, 3)],
        ),
    ];
    publish(&mut core, create(4, cyclic.into_iter().collect()));
    assert_eq!(
        core.native_claim(ClaimId::from_u128(3))
            .unwrap()
            .graph()
            .obligations()[0]
            .target,
        ClaimId::from_u128(4)
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(4))
            .unwrap()
            .graph()
            .obligations()[0]
            .target,
        ClaimId::from_u128(3)
    );
}

#[test]
fn malformed_pins_parent_and_issuer_refuse_without_retained_objects() {
    let core = core();
    let baseline = core.native_budget().used;
    let mut wrong_pin = proposal(1, 101);
    wrong_pin.content = claim_content(
        1,
        SUBJECT,
        ActionType::Work,
        &[focal_model::RequirementRef {
            id: ValidationId::from_u128(101),
            specification: ContentHash([99; 32]),
        }],
        &[],
    );
    let mut wrong_parent = proposal(1, 101);
    wrong_parent.declarations = vec![declaration(101, 2, ISSUER)];
    let mut wrong_issuer = proposal(1, 101);
    wrong_issuer.declarations = vec![declaration(101, 1, SUBJECT)];
    let mut omitted = proposal(1, 101);
    omitted.declarations.clear();
    for (index, (proposal, expected)) in [
        (wrong_pin, ContractError::ContentConflict),
        (wrong_parent, ContractError::InvalidTarget),
        (wrong_issuer, ContractError::InvalidTarget),
        (omitted, ContractError::InvalidPolicy),
    ]
    .into_iter()
    .enumerate()
    {
        match core.prepare_native(
            context(ISSUER),
            create(10 + index as u128, vec![proposal]),
            &[],
        ) {
            Err(NativeError::Contract(actual)) => assert_eq!(actual, expected),
            other => panic!("expected {expected:?}, got {other:?}"),
        }
        assert_eq!(core.native_sequence(), SessionSeq(0));
        assert_eq!(core.native_budget().used, baseline);
        assert!(core.native_claim_content(ClaimId::from_u128(1)).is_none());
        assert!(
            core.native_validation_descriptor(ValidationId::from_u128(101))
                .is_none()
        );
    }
    let mut spoofed = create(20, vec![proposal(1, 101)]);
    spoofed.request.principal = SUBJECT;
    assert!(matches!(
        core.prepare_native(context(SUBJECT), spoofed, &[]),
        Err(NativeError::Contract(ContractError::WrongActor))
    ));
    assert_eq!(core.native_budget().used, baseline);
}

#[test]
fn authored_and_projection_profiles_refuse_the_other_creation_input() {
    let authored = core();
    let source = proposal(1, 101);
    let plan = AuthoredCreationPlan::prepare(
        Principal::Actor(ISSUER),
        &source.content,
        &source.declarations,
        authored_creation::Profile {
            max_responses: source.max_responses,
            scope_limits: source.scope_limits,
            created: SessionSeq(1),
        },
        super::limits(limits(), 4096, 65536),
    )
    .unwrap();
    let charge = plan.construction_bytes();
    let definition = plan.build(charge).unwrap().into_definition();
    let declarations = source
        .declarations
        .iter()
        .map(|descriptor| {
            let declaration = descriptor.declaration();
            declaration
                .try_copy(declaration.retained_bytes().unwrap())
                .unwrap()
        })
        .collect();
    let legacy = input(
        1,
        NativeCommand::Create {
            claims: vec![creation::Proposal {
                definition,
                owner: None,
            }],
            declarations,
        },
    );
    assert!(matches!(
        authored.prepare_native(context(ISSUER), legacy, &[]),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    let projection = Core::new_native(
        ledger(),
        RangeId(406),
        limits(),
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        projection.prepare_native(context(ISSUER), create(1, vec![proposal(1, 101)]), &[]),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    assert_eq!(
        authored.native_content_profile(),
        NativeContentProfile::AuthoredV1
    );
    assert_eq!(
        projection.native_content_profile(),
        NativeContentProfile::ProjectionOnly
    );
    assert_eq!(authored.native_sequence(), SessionSeq(0));
    assert_eq!(projection.native_sequence(), SessionSeq(0));
}

/// Real authored publication history shared with detached recovery regression
/// tests. Includes family-scoped IDs, a progressed claim, an exact-content reuse
/// under a different request, and a contextual reference to an existing claim.
pub(in crate::native) fn recovery_fixture() -> Core<NativeState> {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1, 1)]));
    let expected = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(&mut core, input(2, NativeCommand::Post { expected }));
    let reused = publish(&mut core, create(3, vec![proposal(1, 1)]));
    assert_eq!(
        (
            reused.created,
            reused.changed,
            reused.definitions,
            reused.events
        ),
        (0, 0, 0, 0)
    );
    publish(
        &mut core,
        create(
            4,
            vec![proposal_with(
                2,
                2,
                SUBJECT,
                ActionType::Consultation,
                &[relation(RelationKind::Reviews, 1)],
            )],
        ),
    );
    core
}

/// Exact authored inputs for the recorded-successor integration fixture. No
/// synthetic lifecycle rows or guessed content hashes cross this test seam.
pub(in crate::native) fn replay_fixture() -> (Core<NativeState>, [NativeInput; 3]) {
    let first = proposal(1, 1);
    let expected = first.content.binding();
    (
        core(),
        [
            create(1, vec![first]),
            input(2, NativeCommand::Post { expected }),
            create(3, vec![proposal(1, 1)]),
        ],
    )
}
