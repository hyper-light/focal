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
    assert!(core.native_creation_result(key(1)).is_none());
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
    let mapping = core
        .native_chain(&[&prepared])
        .unwrap()
        .creation_result(key(1))
        .unwrap()
        .entries();
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
        core.native_chain(&[&prepared, &duplicate])
            .unwrap()
            .creation_result(key(11))
            .unwrap()
            .entries(),
        core.native_chain(&[&prepared])
            .unwrap()
            .creation_result(key(10))
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
    let mut core = core();
    const CLAIMS: u128 = 2_000;
    let before = core.state.budget.stats().used;
    for id in 1..=CLAIMS {
        let prepared = prepare(&core, create(id, vec![proposal(id, 10_000 + id)]), &[]);
        core.publish_native(prepared).unwrap();
    }
    // What the native engine's budget holds a claim, every structure included: the row
    // stores' pages and directories, the key sets' blocks, the rows' heap.
    let charged = core.state.budget.stats().used.saturating_sub(before);
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
        let encoded = record_codec::checkpoint::row_bytes(item.key, item.value, ledger).unwrap();
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
