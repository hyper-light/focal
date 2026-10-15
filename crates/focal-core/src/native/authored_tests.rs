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

pub(in crate::native) const ISSUER: ParticipantId = ParticipantId::from_u128(1);
pub(in crate::native) const SUBJECT: ParticipantId = ParticipantId::from_u128(2);

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(1),
    }
}

pub(in crate::native) fn limits() -> NativeLimits {
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

pub(in crate::native) fn core() -> Core<NativeState> {
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
            policy: None,
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

pub(in crate::native) fn proposal(id: u128, validation_id: u128) -> NativeAuthoredProposal {
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

pub(in crate::native) fn key(id: u128) -> RequestKey {
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

pub(in crate::native) fn input(request: u128, command: NativeCommand) -> NativeInput {
    NativeInput {
        request: key(request),
        command,
    }
}

pub(in crate::native) fn create(request: u128, claims: Vec<NativeAuthoredProposal>) -> NativeInput {
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

pub(in crate::native) fn publish(
    core: &mut Core<NativeState>,
    input: NativeInput,
) -> NativeOutcome {
    let prepared = prepare(core, input, &[]);
    core.publish_native(prepared).unwrap()
}

#[test]
fn authored_bodies_policy_and_resolution_move_into_one_atomic_prefix() {
    let mut core = core();
    // What moves without a copy is what a held row shows: rows spilled to a log (doc 31 M3)
    // are read back, decoded where they are read.
    drop(core.detach_payload_log());
    let authored = proposal(1, 1); // Numeric IDs intentionally repeat across families.
    // A claim's content is held as its encoding (doc 31 M3): read back, it is the same
    // descriptor, decoded; a declaration's body still moves in without a copy.
    let claim_description = authored.content.description().to_string();
    let claim_hash = authored.content.content_hash();
    let definition_pointer = authored.declarations[0].description().as_ptr();
    let binding = authored.content.binding();
    let definition_binding = authored.declarations[0].binding();
    let prepared = prepare(&core, create(1, vec![authored]), &[]);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(
        core.native_claim_content(ClaimId::from_u128(1))
            .unwrap()
            .is_none()
    );
    assert!(core.native_creation_result(key(1)).unwrap().is_none());
    let body = core
        .native_chain(&[&prepared])
        .unwrap()
        .claim_content(ClaimId::from_u128(1))
        .unwrap()
        .unwrap();
    let descriptor = core
        .native_chain(&[&prepared])
        .unwrap()
        .validation_descriptor(ValidationId::from_u128(1))
        .unwrap();
    assert_eq!(body.description(), claim_description);
    assert_eq!(body.content_hash(), claim_hash);
    assert_eq!(descriptor.description().as_ptr(), definition_pointer);
    let state = core
        .native_chain(&[&prepared])
        .unwrap()
        .claim(ClaimId::from_u128(1))
        .unwrap();
    assert_eq!(state.binding(), binding);
    state
        .acceptance()
        .check_declaration(descriptor.declaration())
        .unwrap();
    assert!(std::ptr::eq(
        core.native_chain(&[&prepared])
            .unwrap()
            .definition(ValidationId::from_u128(1))
            .unwrap(),
        descriptor.declaration(),
    ));
    let chain = core.native_chain(&[&prepared]).unwrap();
    let read = chain.creation_result(key(1)).unwrap().unwrap();
    let mapping = read.entries();
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
            .unwrap()
            .description(),
        claim_description
    );
    assert_eq!(
        core.native_validation_descriptor(ValidationId::from_u128(1))
            .unwrap()
            .description()
            .as_ptr(),
        definition_pointer
    );
    assert_eq!(
        core.native_creation_result(key(1))
            .unwrap()
            .unwrap()
            .entries()
            .len(),
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
        core.native_chain(&[&prepared, &duplicate])
            .unwrap()
            .creation_result(key(11))
            .unwrap()
            .unwrap()
            .entries(),
        core.native_chain(&[&prepared])
            .unwrap()
            .creation_result(key(10))
            .unwrap()
            .unwrap()
            .entries()
    );
    drop(duplicate);
    drop(prepared);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
    assert!(
        core.native_validation_descriptor(ValidationId::from_u128(101))
            .is_none()
    );
    assert!(core.native_creation_result(key(10)).unwrap().is_none());
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
    assert_eq!(
        core.native_creation_result(key(3))
            .unwrap()
            .unwrap()
            .entries(),
        old
    );
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
    assert!(core.native_creation_result(key(4)).unwrap().is_none());
    publish(
        &mut core,
        input(5, NativeCommand::Cancel { expected: posted }),
    );
    assert_eq!(
        core.native_creation_result(key(1))
            .unwrap()
            .unwrap()
            .entries(),
        old
    );
    assert_eq!(
        core.native_creation_result(key(3))
            .unwrap()
            .unwrap()
            .entries(),
        old
    );
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
    assert!(
        core.native_claim_content(ClaimId::from_u128(1))
            .unwrap()
            .is_none()
    );
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
    assert!(
        core.native_chain(&[&pending, &reviews])
            .unwrap()
            .claim_content(ClaimId::from_u128(2))
            .unwrap()
            .is_some()
    );
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
        assert!(
            core.native_claim_content(ClaimId::from_u128(1))
                .unwrap()
                .is_none()
        );
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

/// The core a measurement grows: the tests' small leaves, or with
/// `FOCAL_MEASURE_PAGE_BYTES` set, a node's (focal-ledger's session limits:
/// 128 entries a leaf, at most that many bytes, 256 changes a batch).
fn measured_core() -> Core<NativeState> {
    let Some(page_bytes) = std::env::var("FOCAL_MEASURE_PAGE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
    else {
        return core();
    };
    let limits = NativeLimits {
        range: RangeConfig {
            page_entries: 128,
            page_bytes,
            max_batch_entries: 256,
            ..RangeConfig::default()
        },
        plan_nodes: 64,
        plan_edges: 65_536,
        preparation_bytes: 1 << 20,
        evaluations_per_claim: 64,
        ..NativeLimits::default()
    };
    Core::new_native_authored(
        ledger(),
        RangeId(405),
        limits,
        MemoryBudget::new(512 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

/// Measurement, not a check: the owner's CPU per authored claim creation,
/// prepare (decode-free: the input is built) and publish apart, over a core
/// that grows to `CLAIMS`. Run with `--release --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_time_per_authored_claim() {
    let claims: u128 = std::env::var("FOCAL_MEASURE_CLAIMS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000);
    // Rounds over fresh cores, for a profiler to sample long enough.
    let rounds: u32 = std::env::var("FOCAL_MEASURE_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    for _ in 1..rounds {
        let mut core = measured_core();
        for id in 1..=claims {
            let prepared = prepare(&core, create(id, vec![proposal(id, 10_000 + id)]), &[]);
            core.publish_native(prepared).unwrap();
        }
    }
    let mut core = measured_core();
    let (mut prepare_ns, mut publish_ns) = (0u128, 0u128);
    let mut window = (0u128, 0u128);
    // The candidate's log record, measured and written as an owner funds and
    // encodes it, timed apart (FOCAL_MEASURE_RECORD set).
    let record = std::env::var("FOCAL_MEASURE_RECORD").is_ok();
    let mut record_ns = 0u128;
    // A follower's apply of that record, timed apart (FOCAL_MEASURE_REPLAY
    // set): inspected, replayed against a second core at the leader's
    // prefix, and published there, as a follower applies a committed record.
    let replay = std::env::var("FOCAL_MEASURE_REPLAY").is_ok();
    let mut replay_ns = 0u128;
    let replay_dir = tempfile::tempdir().unwrap();
    let store = crate::native::record_codec::recovery::tests::store(replay_dir.path());
    let mut follower = measured_core();
    let limits = record_codec::EncodingLimits {
        bytes: 1 << 24,
        visits: usize::MAX,
        rows: 1 << 16,
    };
    for id in 1..=claims {
        let input = create(id, vec![proposal(id, 10_000 + id)]);
        let started = std::time::Instant::now();
        let prepared = prepare(&core, input, &[]);
        let prepared_at = std::time::Instant::now();
        if record {
            let plan = record_codec::EncodingPlan::prepare(&prepared, limits).unwrap();
            let mut bytes = vec![0; plan.quote().bytes];
            std::hint::black_box(plan.write_into(&mut bytes).unwrap());
            record_ns += prepared_at.elapsed().as_nanos();
        }
        if replay {
            let plan = record_codec::EncodingPlan::prepare(&prepared, limits).unwrap();
            let mut bytes = vec![0; plan.quote().bytes];
            plan.write_into(&mut bytes).unwrap();
            let started = std::time::Instant::now();
            let structural = record_codec::StructuralRecord::inspect(
                &bytes,
                record_codec::InspectionLimits {
                    bytes: bytes.len(),
                    visits: usize::MAX,
                    rows: 1 << 16,
                    row_bytes: 32 << 20,
                },
            )
            .unwrap();
            let replayed = record_codec::replay::prepare(
                &follower,
                &structural,
                core.state.rows.id(),
                crate::native::record_codec::recovery::tests::limits(follower.limits),
                &store,
                &focal_evidence::BuiltinNativeSchemas,
            )
            .unwrap();
            follower.publish_native(replayed).unwrap();
            replay_ns += started.elapsed().as_nanos();
        }
        let publish_at = std::time::Instant::now();
        core.publish_native(prepared).unwrap();
        let p = prepared_at.duration_since(started).as_nanos();
        let q = publish_at.elapsed().as_nanos();
        prepare_ns += p;
        publish_ns += q;
        if id > claims - 1000 {
            window.0 += p;
            window.1 += q;
        }
    }
    if record {
        println!(
            "record {:.1} µs per claim",
            record_ns as f64 / claims as f64 / 1e3
        );
    }
    if replay {
        println!(
            "replay {:.1} µs per claim",
            replay_ns as f64 / claims as f64 / 1e3
        );
    }
    println!(
        "{claims} claims: prepare {:.1} µs, publish {:.1} µs per claim; last 1000: prepare {:.1} µs, publish {:.1} µs",
        prepare_ns as f64 / claims as f64 / 1e3,
        publish_ns as f64 / claims as f64 / 1e3,
        window.0 as f64 / 1000.0 / 1e3,
        window.1 as f64 / 1000.0 / 1e3,
    );
}

/// What one authored claim (one declaration) costs the session's memory, by row family:
/// rows, inline entry bytes and owned heap bytes. A measurement, not an assertion.
#[test]
#[ignore = "measurement: run with --ignored --nocapture"]
fn measure_memory_per_authored_claim_by_family() {
    // The range configuration a node runs with (focal-ledger's native session): leaves of 128
    // entries within a 4 KiB memory page, Linux's; the fixtures' 8-entry leaves would measure
    // their own page overhead.
    let mut core = Core::new_native_authored(
        ledger(),
        RangeId(405),
        NativeLimits {
            range: RangeConfig {
                page_entries: 128,
                page_bytes: 4096,
                max_batch_entries: 256,
                ..RangeConfig::default()
            },
            ..limits()
        },
        MemoryBudget::new(512 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    // With `FOCAL_MEASURE_SPILL` set, the replica spills its payloads to a log (doc 31 M3).
    if std::env::var("FOCAL_MEASURE_SPILL").is_ok() {
        core.attach_payload_log(Box::new(crate::native::payloads::MemoryPayloads::default()));
    }
    const CLAIMS: u128 = 2_000;
    let before = core.state.budget.stats().used;
    for id in 1..=CLAIMS {
        let prepared = prepare(&core, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        core.publish_native(prepared).unwrap();
    }
    // What the native engine's budget holds a claim, every structure included: the row
    // stores' pages and directories, the key sets' blocks, the rows' heap.
    let charged = core.state.budget.stats().used.saturating_sub(before);
    let by_kind = core.state.budget.stats().by_kind;
    let kinds = [
        "arena",
        "payload",
        "index",
        "pages",
        "roots",
        "read pins",
        "pending",
        "query",
        "monitor",
        "dedup",
        "timer",
        "recovery",
        "control",
        "reserved",
    ];
    let mut kinds_line = String::new();
    for (name, bytes) in kinds.iter().zip(by_kind.iter()) {
        if *bytes != 0 {
            kinds_line.push_str(&format!(" {name} {:.0}", *bytes as f64 / CLAIMS as f64));
        }
    }
    println!("charged by kind, a claim:{kinds_line}");
    let stats = core.state.rows.stats();
    println!(
        "row store: {} entries (index keys among them) in {} pages: {:.1} entries a page",
        stats.entries,
        stats.pages,
        stats.entries as f64 / stats.pages.max(1) as f64
    );
    println!(
        "charged {:.0} B a claim ({} claims, the native budget's use)",
        charged as f64 / CLAIMS as f64,
        CLAIMS
    );
    let mut families: std::collections::BTreeMap<String, (usize, usize, usize, usize)> =
        Default::default();
    let entry = std::mem::size_of::<focal_memory::Entry<crate::native::layout::PackedKey, Row>>();
    let ledger = core.state.ledger;
    for item in core.state.rows.entries() {
        // A key set's key takes its share of its blocks (doc 31 M3), not an entry.
        let in_set = crate::native::ranges::indexed(
            &crate::native::layout::PackedKey::pack(&item.key).unwrap(),
        );
        let name = format!("{}{:?}", if in_set { "~" } else { "" }, item.key);
        let family = name
            .split(['(', ' ', '{'])
            .next()
            .unwrap_or("?")
            .to_string();
        let slot_entry = if in_set { 0 } else { entry };
        let encoded = record_codec::checkpoint::row_bytes(
            item.key,
            item.value,
            ledger,
            core.state.payloads(),
        )
        .unwrap();
        let packed = crate::native::layout::PackedKey::pack(&item.key).unwrap();
        let slot = families.entry(family).or_default();
        slot.0 += 1;
        slot.1 += item.heap_bytes + slot_entry;
        slot.2 += encoded;
        slot.3 += packed.as_bytes().len();
    }
    let claims = CLAIMS as f64;
    println!(
        "entry={entry} B (packed key {} B, Row {} B); ~ marks a family a key set holds",
        std::mem::size_of::<crate::native::layout::PackedKey>(),
        std::mem::size_of::<Row>()
    );
    println!(
        "{:<22} {:>9} {:>12} {:>12} {:>10}",
        "family", "rows/claim", "held B/cl", "encoded B/cl", "key B/row"
    );
    let (mut total_rows, mut total_held, mut total_encoded) = (0, 0, 0);
    for (family, (rows, held, encoded, key)) in &families {
        total_rows += rows;
        total_held += held;
        total_encoded += encoded;
        println!(
            "{family:<22} {:>9.2} {:>12.0} {:>12.0} {:>10.1}",
            *rows as f64 / claims,
            *held as f64 / claims,
            *encoded as f64 / claims,
            *key as f64 / *rows as f64
        );
    }
    println!(
        "{:<22} {:>9.2} {:>12.0} {:>12.0}",
        "TOTAL (row store)",
        total_rows as f64 / claims,
        total_held as f64 / claims,
        total_encoded as f64 / claims
    );
    let sets = core
        .state
        .rows
        .indexes()
        .iter()
        .map(|set| set.stats())
        .fold((0, 0), |(keys, bytes), stats| {
            (keys + stats.keys, bytes + stats.block_bytes)
        });
    // What a claim's state holds, by part: where its row's heap goes.
    let state = core.native_claim(ClaimId::from_u128(1)).unwrap();
    println!(
        "claim state: {} B inline; heap: graph {} B, lineage {} B, acceptance {} B, scopes {} B, all {} B ({} allocations)",
        std::mem::size_of::<focal_model::lifecycle::claim::ClaimState>(),
        state.graph().retained_heap_bytes().unwrap(),
        state.lineage().retained_heap_bytes().unwrap(),
        state.acceptance().retained_heap_bytes().unwrap(),
        state.scopes().retained_heap_bytes().unwrap(),
        state.retained_heap_bytes().unwrap(),
        state.heap_allocations().unwrap()
    );
    println!(
        "key sets: {:.2} keys a claim, {:.0} block bytes a claim ({:.1} a key)",
        sets.0 as f64 / claims,
        sets.1 as f64 / claims,
        sets.1 as f64 / sets.0.max(1) as f64
    );
}

#[test]
#[ignore = "measurement"]
fn measure_row_variant_widths() {
    let mut widths: Vec<(&str, usize)> = vec![
        (
            "IncomingHead",
            std::mem::size_of::<incoming_graph::IncomingHead>(),
        ),
        (
            "IncomingLink",
            std::mem::size_of::<incoming_graph::IncomingLink>(),
        ),
        (
            "Monitor",
            std::mem::size_of::<monitor_index::MonitorAllocation>(),
        ),
        (
            "MonitorHead",
            std::mem::size_of::<monitor_index::MonitorHead>(),
        ),
        (
            "MonitorLink",
            std::mem::size_of::<Option<monitor_index::MonitorLink>>(),
        ),
        ("MissingResult", std::mem::size_of::<OwnedMissingResult>()),
        ("Meta", std::mem::size_of::<Meta>()),
        ("Claim", std::mem::size_of::<OwnedClaim>()),
        ("Definition", std::mem::size_of::<OwnedDeclaration>()),
        ("Evaluation", std::mem::size_of::<OwnedEvaluation>()),
        ("Artifact", std::mem::size_of::<OwnedArtifact>()),
        ("ArtifactIdentity", std::mem::size_of::<ArtifactId>()),
        ("Accepted", std::mem::size_of::<OwnedAccepted>()),
        ("DeliveryResult", std::mem::size_of::<OwnedDeliveryResult>()),
        ("Receipt", std::mem::size_of::<NativeReceipt>()),
        ("Cycle", std::mem::size_of::<NativeCycle>()),
        ("RetiredCycleHead", std::mem::size_of::<RetiredCycleHead>()),
        ("RetiredCycle", std::mem::size_of::<RetiredCycle>()),
        ("Work", std::mem::size_of::<OwnedWork>()),
        ("WorkSlot", std::mem::size_of::<ArtifactId>()),
        ("Diagnostic", std::mem::size_of::<OwnedDiagnostic>()),
        ("Response", std::mem::size_of::<OwnedResponse>()),
        (
            "ResultTestament",
            std::mem::size_of::<OwnedResultTestament>(),
        ),
        ("ClaimResultTestament", std::mem::size_of::<TestamentId>()),
        ("Outcome", std::mem::size_of::<OutcomeRow>()),
        ("Event", std::mem::size_of::<OwnedEvent>()),
        ("ClaimContent", std::mem::size_of::<OwnedClaimContent>()),
        ("ClaimIdentity", std::mem::size_of::<ClaimId>()),
        ("DefinitionIdentity", std::mem::size_of::<ValidationId>()),
        ("CreationResult", std::mem::size_of::<OwnedCreationResult>()),
        ("LegacyTestament", std::mem::size_of::<OwnedLegacy>()),
        ("LegacyEvidenceSet", std::mem::size_of::<OwnedLegacy>()),
        ("LegacyRun", std::mem::size_of::<OwnedLegacy>()),
        ("LegacyDefinition", std::mem::size_of::<OwnedLegacy>()),
        ("Retired", std::mem::size_of::<RetiredClaim>()),
        ("Epochs", std::mem::size_of::<EpochWindow>()),
        ("Seal", std::mem::size_of::<SealRow>()),
    ];
    widths.sort_by_key(|(_, w)| std::cmp::Reverse(*w));
    for (name, width) in widths {
        println!("{name:<20} {width}");
    }
}

/// Measurement, not a check: the inline width of each key family's fields,
/// widest first, to see which families set `Key`'s width.
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn measure_key_variant_widths() {
    use std::mem::size_of;
    let mut widths: Vec<(&str, usize)> = vec![
        ("Key", size_of::<Key>()),
        ("ClaimId", size_of::<ClaimId>()),
        ("ContentHash", size_of::<ContentHash>()),
        ("ObjectId", size_of::<focal_model::ObjectId>()),
        ("ParticipantId", size_of::<ParticipantId>()),
        ("NativeInvocation", size_of::<NativeInvocation>()),
        ("NativeResultKey", size_of::<NativeResultKey>()),
        ("EvaluationKey", size_of::<EvaluationKey>()),
        ("EvaluationTarget", size_of::<EvaluationTarget>()),
        ("NativeCycleKey", size_of::<NativeCycleKey>()),
        ("TimerTarget", size_of::<TimerTarget>()),
        ("ByScope", size_of::<(u16, ContentHash, ClaimId)>()),
        (
            "ByCreated",
            size_of::<(u16, SessionSeq, focal_model::ObjectId)>(),
        ),
        ("DueTimer", size_of::<(u64, TimerTarget)>()),
        ("ByVerdict", size_of::<(u16, NativeResultKey)>()),
    ];
    widths.sort_by_key(|(_, w)| std::cmp::Reverse(*w));
    for (name, width) in widths {
        println!("{name:<20} {width}");
    }
}

/// Measurement, not a check: what an authored claim's creation record is made of, by row family —
/// the bytes a leader replicates for it and every member writes. Run with `--ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_record_bytes_by_family() {
    let core = measured_core();
    let prepared = prepare(&core, create(7, vec![proposal(7, 10_007)]), &[]);
    let limits = record_codec::EncodingLimits {
        bytes: 1 << 24,
        visits: usize::MAX,
        rows: 1 << 16,
    };
    let total = record_codec::EncodingPlan::prepare(&prepared, limits)
        .unwrap()
        .quote()
        .bytes;
    let rows = record_codec::row_bytes(&prepared).unwrap();
    let mut by_family: std::collections::BTreeMap<u16, (usize, usize, usize)> =
        std::collections::BTreeMap::new();
    for (family, key, value) in &rows {
        let entry = by_family.entry(*family).or_default();
        entry.0 += 1;
        entry.1 += key;
        entry.2 += value;
    }
    let keys: usize = rows.iter().map(|(_, k, _)| k).sum();
    let values: usize = rows.iter().map(|(_, _, v)| v).sum();
    println!(
        "record {total} bytes: {} rows, keys {keys}, values {values}, framing {}",
        rows.len(),
        total - keys - values
    );
    for (family, (count, key, value)) in by_family {
        println!("family {family:3}: {count:2} rows, keys {key:5}, values {value:6}");
    }
}

/// A replica that spills its events to a payload log (doc 31 M3) holds the rows one that keeps
/// them does: the leader's records and checkpoints are byte-identical with and without a log, a
/// frozen image encodes the same root, a follower that spills replays the leader's records to
/// the same root, every event reads back whole, and the spilling replicas hold no event's bytes.
#[test]
fn spilled_rows_encode_replay_and_checkpoint_as_held_ones() {
    spilled_rows_match_held_ones(None);
}

/// A young generation (doc 31 M3) keeps the newest waiting claims and definitions held: only
/// those newer ones push out spill, and every claim reads as a core that holds them all.
#[test]
fn the_newest_waiting_claims_stay_held_and_only_those_pushed_out_spill() {
    use crate::native::payloads::MemoryPayloads;
    let mut plain = core();
    let mut young = core();
    // A generation of four rows: each claim and its definition take one place apiece.
    young.attach_payload_log_with_young(Box::new(MemoryPayloads::default()), 4);
    assert_eq!(young.state.young.bound(), 4);
    for id in 1..=6 {
        let held = prepare(&plain, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        plain.publish_native(held).unwrap();
        let prepared = prepare(&young, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        young.publish_native(prepared).unwrap();
        assert!(young.state.young.len() <= 4);
    }
    // The four newest rows (claims 5 and 6 and their definitions) are held; the older ones
    // were pushed out and spilled, and every one reads as the held core's.
    for id in 1..=6u128 {
        let claim = ClaimId::from_u128(id);
        let Some(Row::Claim(row)) = young.state.rows.get(&Key::Claim(claim)) else {
            panic!("claim {id} is held or spilled");
        };
        assert_eq!(row.logged().is_some(), id <= 4, "claim {id}");
        let Some(Row::Definition(definition)) = young
            .state
            .rows
            .get(&Key::Definition(ValidationId::from_u128(10_000 + id)))
        else {
            panic!("definition {id} is held or spilled");
        };
        assert_eq!(definition.logged().is_some(), id <= 4, "definition {id}");
        assert_eq!(young.native_claim(claim), plain.native_claim(claim));
    }
}

#[test]
fn a_session_short_of_memory_spills_its_oldest_held_waiting_claims_first() {
    use crate::native::payloads::MemoryPayloads;
    let plain = {
        let mut plain = core();
        for id in 1..=4 {
            let held = prepare(&plain, create(id, vec![proposal(id, 10_000 + id)]), &[]);
            plain.publish_native(held).unwrap();
        }
        plain
    };
    let mut young = core();
    young.attach_payload_log_with_young(Box::new(MemoryPayloads::default()), 1024);
    for id in 1..=3 {
        let prepared = prepare(&young, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        young.publish_native(prepared).unwrap();
    }
    let logged = |core: &Core<NativeState>, id: u128| match core
        .state
        .rows
        .get(&Key::Claim(ClaimId::from_u128(id)))
    {
        Some(Row::Claim(row)) => row.logged().is_some(),
        _ => panic!("claim {id} is held or spilled"),
    };
    // Room to spare: every waiting claim is held.
    assert!((1..=3).all(|id| !logged(&young, id)));
    // The ordinary lane squeezed below the low water: the next publication spills the oldest
    // the generation holds, oldest first, as many as memory wants (64 a publication at most).
    let budget = young.state.budget.clone();
    let squeeze = budget
        .reserve(
            focal_memory::BudgetKind::Payload,
            focal_memory::BudgetLane::Ordinary,
            budget.headroom(focal_memory::BudgetLane::Ordinary) - (1 << 20),
        )
        .unwrap()
        .commit();
    let prepared = prepare(&young, create(4, vec![proposal(4, 10_004)]), &[]);
    young.publish_native(prepared).unwrap();
    assert!((1..=3).all(|id| logged(&young, id)), "the oldest spilled");
    for id in 1..=4u128 {
        assert_eq!(
            young.native_claim(ClaimId::from_u128(id)),
            plain.native_claim(ClaimId::from_u128(id))
        );
    }
    drop(squeeze);
}

/// A payload log that refuses every payload while `refuse` is set: a log out of room or failing,
/// then repaired.
#[derive(Debug)]
struct Switched {
    log: crate::native::payloads::MemoryPayloads,
    refuse: &'static std::sync::atomic::AtomicBool,
}

impl crate::native::payloads::PayloadRead for Switched {
    fn read(
        &self,
        at: crate::native::payloads::PayloadRef,
        into: &mut [u8],
    ) -> Result<(), crate::native::payloads::PayloadError> {
        self.log.read(at, into)
    }
}

impl crate::native::payloads::PayloadLog for Switched {
    fn append(&self, bytes: &[u8]) -> Option<crate::native::payloads::PayloadRef> {
        if self.refuse.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }
        self.log.append(bytes)
    }
    fn hold(&self) -> Option<crate::native::payloads::PayloadHold> {
        self.log.hold()
    }
    fn keep(&self, hold: crate::native::payloads::PayloadHold) {
        self.log.keep(hold);
    }
    fn give_back(&self, hold: crate::native::payloads::PayloadHold) -> bool {
        self.log.give_back(hold)
    }
    fn reader(
        &self,
    ) -> Result<
        Box<dyn crate::native::payloads::PayloadRead + Send>,
        crate::native::payloads::PayloadError,
    > {
        self.log.reader()
    }
}

/// Whether claim `id` is spilled; a test fails where the core holds no such claim.
fn claim_logged(core: &Core<NativeState>, id: u128) -> bool {
    match core.state.rows.get(&Key::Claim(ClaimId::from_u128(id))) {
        Some(Row::Claim(row)) => row.logged().is_some(),
        _ => panic!("claim {id} is held or spilled"),
    }
}

/// The ordinary lane of `core`'s budget squeezed to a mebibyte: below the young generation's low
/// water, so a publication spills the oldest rows for memory.
fn squeeze(core: &Core<NativeState>) -> focal_memory::Allocation {
    let budget = core.state.budget.clone();
    budget
        .reserve(
            focal_memory::BudgetKind::Payload,
            focal_memory::BudgetLane::Ordinary,
            budget.headroom(focal_memory::BudgetLane::Ordinary) - (1 << 20),
        )
        .unwrap()
        .commit()
}

/// A spill the payload log refuses (out of room, failing) is put off, its key kept: the rows stay
/// held while it refuses, and once it takes payloads again an unrelated publication spills them.
/// (Forgotten instead, they were held for good: the review's case of three old rows, still held
/// after the log was repaired.)
#[test]
fn a_spill_the_log_refuses_is_kept_and_done_once_the_log_takes_payloads_again() {
    static REFUSE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let mut plain = core();
    let mut young = core();
    young.attach_payload_log_with_young(
        Box::new(Switched {
            log: crate::native::payloads::MemoryPayloads::default(),
            refuse: &REFUSE,
        }),
        1024,
    );
    for id in 1..=3 {
        let held = prepare(&plain, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        plain.publish_native(held).unwrap();
        let prepared = prepare(&young, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        young.publish_native(prepared).unwrap();
    }
    assert!((1..=3).all(|id| !claim_logged(&young, id)));
    let squeezed = squeeze(&young);
    REFUSE.store(true, std::sync::atomic::Ordering::SeqCst);
    let prepared = prepare(&young, create(4, vec![proposal(4, 10_004)]), &[]);
    young.publish_native(prepared).unwrap();
    // Nothing spilled, nothing forgotten: every row is tracked, its spill put off.
    assert!((1..=4).all(|id| !claim_logged(&young, id)));
    let (tracked, put_off, untracked) = young.native_young();
    assert_eq!(
        (tracked, untracked),
        (8, 0),
        "four claims and their definitions"
    );
    assert!(put_off >= 3, "{put_off}");
    REFUSE.store(false, std::sync::atomic::Ordering::SeqCst);
    let prepared = prepare(&young, create(5, vec![proposal(5, 10_005)]), &[]);
    young.publish_native(prepared).unwrap();
    assert!(
        (1..=5).all(|id| claim_logged(&young, id)),
        "spilled once it may"
    );
    let held = prepare(&plain, create(4, vec![proposal(4, 10_004)]), &[]);
    plain.publish_native(held).unwrap();
    let held = prepare(&plain, create(5, vec![proposal(5, 10_005)]), &[]);
    plain.publish_native(held).unwrap();
    for id in 1..=5u128 {
        let claim = ClaimId::from_u128(id);
        assert_eq!(young.native_claim(claim), plain.native_claim(claim));
    }
    drop(squeezed);
}

/// A spill put off while a frozen image (a checkpoint encoding) shares the row's page keeps its
/// key: the publication under memory pressure spills nothing it may not, and once the image is
/// let go, unrelated publications spill every row it held, within a bounded number of them.
#[test]
fn a_spill_put_off_while_a_frozen_image_shares_its_page_is_done_once_the_image_is_let_go() {
    use crate::native::payloads::MemoryPayloads;
    let mut young = core();
    young.attach_payload_log_with_young(Box::new(MemoryPayloads::default()), 4096);
    // Enough claims that the store has pages a later publication does not touch.
    const CLAIMS: u128 = 300;
    for id in 1..=CLAIMS {
        let prepared = prepare(&young, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        young.publish_native(prepared).unwrap();
    }
    assert!((1..=CLAIMS).all(|id| !claim_logged(&young, id)));
    let frozen = young.freeze_native().unwrap();
    let squeezed = squeeze(&young);
    let far = 1_000_000;
    let prepared = prepare(&young, create(far, vec![proposal(far, far + 10_000)]), &[]);
    young.publish_native(prepared).unwrap();
    let (_, put_off, untracked) = young.native_young();
    assert!(put_off > 0, "the image shares the oldest rows' pages");
    assert_eq!(untracked, 0);
    assert!(!claim_logged(&young, 1), "its page is the image's too");
    drop(frozen);
    let mut publications = 0;
    for request in 0..20u128 {
        if (1..=CLAIMS).all(|id| claim_logged(&young, id)) {
            break;
        }
        let id = 2_000_000 + request;
        let prepared = prepare(&young, create(id, vec![proposal(id, id + 10_000)]), &[]);
        young.publish_native(prepared).unwrap();
        publications += 1;
    }
    assert!(
        (1..=CLAIMS).all(|id| claim_logged(&young, id)),
        "every row the image held spilled, within {publications} publications"
    );
    assert_eq!(young.native_young().2, 0);
    drop(squeezed);
}

/// A row the generation could not keep (it tracks twice its bound already) whose spill the log
/// refused is counted, not forgotten: once the log takes payloads again, a walk of the rows (a
/// bounded step a publication while memory is short) spills every row it lost track of. Each
/// reads as a holding core's.
#[test]
fn a_row_the_generation_could_not_keep_is_found_again_once_it_may_spill() {
    static REFUSE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let mut plain = core();
    let mut young = core();
    // A generation of one row by choice, two tracked at most.
    young.attach_payload_log_with_young(
        Box::new(Switched {
            log: crate::native::payloads::MemoryPayloads::default(),
            refuse: &REFUSE,
        }),
        1,
    );
    let squeezed = squeeze(&young);
    REFUSE.store(true, std::sync::atomic::Ordering::SeqCst);
    for id in 1..=4 {
        let held = prepare(&plain, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        plain.publish_native(held).unwrap();
        let prepared = prepare(&young, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        young.publish_native(prepared).unwrap();
    }
    // Four claims and their definitions: two tracked, six lost, none spilled.
    let (tracked, _, untracked) = young.native_young();
    assert_eq!((tracked, untracked), (2, 6));
    assert!((1..=4).all(|id| !claim_logged(&young, id)));
    REFUSE.store(false, std::sync::atomic::Ordering::SeqCst);
    let mut publications = 0;
    for request in 0..8u128 {
        if (1..=4).all(|id| claim_logged(&young, id)) {
            break;
        }
        let id = 100 + request;
        let prepared = prepare(&young, create(id, vec![proposal(id, id + 10_000)]), &[]);
        young.publish_native(prepared).unwrap();
        publications += 1;
    }
    assert!(
        (1..=4).all(|id| claim_logged(&young, id)),
        "every lost row spilled, within {publications} publications"
    );
    for id in 1..=4u128 {
        let claim = ClaimId::from_u128(id);
        assert_eq!(young.native_claim(claim), plain.native_claim(claim));
    }
    drop(squeezed);
}

/// As above, with each spilling replica's log refusing appends once it holds a few events:
/// the events past its bound stay held, and every encoding is unchanged.
#[test]
fn a_full_payload_log_leaves_rows_held_and_every_encoding_unchanged() {
    spilled_rows_match_held_ones(Some(2_000));
}

/// A leader and a follower that spill their events and contents (doc 31 M3) against one that
/// holds them: every record, checkpoint (inline and frozen) and outcome is the same, and every
/// spilled row reads back as the held one.
fn spilled_rows_match_held_ones(bound: Option<usize>) {
    use crate::native::payloads::MemoryPayloads;
    let log = || Box::new(MemoryPayloads::bounded(bound));
    let mut plain = core();
    // No young generation: each spillable row spills where it is published.
    let mut spilled = core();
    spilled.attach_payload_log_with_young(log(), 0);
    let mut follower = core();
    follower.attach_payload_log_with_young(log(), 0);
    let replay_dir = tempfile::tempdir().unwrap();
    let store = crate::native::record_codec::recovery::tests::store(replay_dir.path());
    let limits = record_codec::EncodingLimits {
        bytes: 1 << 24,
        visits: usize::MAX,
        rows: 1 << 16,
    };
    let record = |prepared: &NativePrepared, core: &Core<NativeState>| {
        let plan =
            record_codec::EncodingPlan::prepare_with(prepared, limits, core.state.payloads())
                .unwrap();
        let mut bytes = vec![0; plan.quote().bytes];
        plan.write_into(&mut bytes).unwrap();
        bytes
    };
    let checkpoint = |core: &Core<NativeState>| {
        let plan = record_codec::checkpoint::EncodingPlan::prepare(core, limits).unwrap();
        let mut bytes = vec![0; plan.quote().bytes];
        plan.write_into(&mut bytes).unwrap();
        bytes
    };
    for id in 1..=12 {
        let held = prepare(&plain, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        let logged = prepare(&spilled, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        let bytes = record(&logged, &spilled);
        assert_eq!(record(&held, &plain), bytes);
        let structural = record_codec::StructuralRecord::inspect(
            &bytes,
            record_codec::InspectionLimits {
                bytes: bytes.len(),
                visits: usize::MAX,
                rows: 1 << 16,
                row_bytes: 32 << 20,
            },
        )
        .unwrap();
        let replayed = record_codec::replay::prepare(
            &follower,
            &structural,
            spilled.state.rows.id(),
            crate::native::record_codec::recovery::tests::limits(follower.limits),
            &store,
            &focal_evidence::BuiltinNativeSchemas,
        )
        .unwrap();
        follower.publish_native(replayed).unwrap();
        let plain_outcome = plain.publish_native(held).unwrap();
        let spilled_outcome = spilled.publish_native(logged).unwrap();
        assert_eq!(plain_outcome, spilled_outcome);
    }
    let root = checkpoint(&plain);
    assert_eq!(checkpoint(&spilled), root);
    assert_eq!(checkpoint(&follower), root);
    // Each claim, waiting to be delivered and tied to nothing, and its definition left memory
    // where its publication left it (doc 31 M3), on the leader and the follower alike (a full
    // log left the rest held), and each reads back as the held one.
    let mut spilled_claims = 0usize;
    for id in 1..=12u128 {
        let claim = ClaimId::from_u128(id);
        let definition = ValidationId::from_u128(10_000 + id);
        for replica in [&spilled, &follower] {
            let Some(Row::Claim(row)) = replica.state.rows.get(&Key::Claim(claim)) else {
                panic!("claim {id} is held or spilled");
            };
            if row.logged().is_some() {
                spilled_claims += 1;
                assert!(matches!(
                    replica.state.rows.get(&Key::Definition(definition)),
                    Some(Row::Definition(row)) if row.logged().is_some()
                ));
            } else {
                assert!(bound.is_some(), "only a full log leaves a claim held");
            }
            assert_eq!(replica.native_claim(claim), plain.native_claim(claim));
            // A declaration is compared by its binding, its claim, its place and the
            // fingerprint of its intent, a hash of all it declares.
            let read = |core: &Core<NativeState>| {
                core.native_definition(definition).map(|read| {
                    (
                        read.binding(),
                        read.claim(),
                        read.declaration_index(),
                        read.intent_fingerprint(),
                    )
                })
            };
            assert_eq!(read(replica), read(&plain));
            assert!(read(&plain).is_some());
        }
    }
    assert!(spilled_claims > 0);
    let frozen = spilled.freeze_native().unwrap();
    let plan = record_codec::checkpoint::EncodingPlan::prepare_frozen(&frozen, limits).unwrap();
    let mut frozen_root = vec![0; plan.quote().bytes];
    plan.write_into(&mut frozen_root).unwrap();
    assert_eq!(frozen_root, root);
    for replica in [&spilled, &follower] {
        let (mut contents, mut spilled_contents) = (0, 0);
        for entry in replica.state.rows.entries() {
            let (Key::ClaimContent(id), Row::ClaimContent(content)) = (entry.key, entry.value)
            else {
                continue;
            };
            match content {
                crate::native::owned::OwnedClaimContent::Logged(_) => {
                    assert_eq!(content.heap_charge().unwrap(), 0);
                    assert_eq!(entry.heap_bytes, 0);
                    spilled_contents += 1;
                }
                crate::native::owned::OwnedClaimContent::Held(_) => {
                    assert!(bound.is_some(), "only a full log leaves one held")
                }
            }
            let mut buffer = crate::native::owned::ContentBuffer::new();
            let read = content.read(replica.state.payloads(), &mut buffer).unwrap();
            let Some(Row::ClaimContent(held)) = plain.state.rows.get(&Key::ClaimContent(id)) else {
                panic!("the holding replica's content");
            };
            assert_eq!(read.encoded(), held.held().unwrap().encoded());
            contents += 1;
        }
        assert_eq!(contents, 12);
        assert!(spilled_contents > 0);
        let mut events = 0;
        let mut spilled_events = 0;
        for entry in replica.state.rows.entries() {
            let Key::Event(sequence, ordinal) = entry.key else {
                continue;
            };
            let Row::Event(event) = entry.value else {
                panic!("event row");
            };
            match event {
                OwnedEvent::Logged(_) => {
                    assert_eq!(event.heap_charge().unwrap(), 0);
                    spilled_events += 1;
                }
                OwnedEvent::Held(_) => assert!(bound.is_some(), "only a full log leaves one held"),
            }
            let read = event
                .get(replica.state.ledger, replica.state.payloads())
                .unwrap();
            assert_eq!((read.sequence, read.ordinal), (sequence, ordinal));
            events += 1;
        }
        assert!(events >= 24);
        assert!(spilled_events > 0);
        if bound.is_some() {
            assert!(spilled_events < events, "the bound left some held");
        }
    }
    // What the rows take, the read-back floor a spilling core holds aside (doc 31 M3).
    assert!(
        spilled
            .state
            .budget
            .stats()
            .used
            .saturating_sub(crate::native::read_back::FLOOR)
            < plain.state.budget.stats().used
    );
}
