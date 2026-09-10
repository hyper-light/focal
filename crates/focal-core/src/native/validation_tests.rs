use super::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    aggregation,
    claim::ClaimDefinition,
    creation::Owner,
    graph, scope,
    succession::{Correction, CorrectionKind, Lineage},
};
use focal_model::{
    Cause, Deadline, HandlerRef, ObjectId, ObjectRef, ObjectRevision, ParticipantId, RequestEpoch,
    RequestId, RootCommandId, SessionId, TenantId, TimerId, ValidationKind, ValidationMode,
    ValidationPhase, ValidatorId,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(11);
const SUBJECT: ParticipantId = ParticipantId::from_u128(12);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(13);
const DEADLINE: u64 = 1000;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(8),
        session: SessionId::from_u128(9),
    }
}
fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn declaration_id(claim: u128, offset: u128) -> ValidationId {
    ValidationId::from_u128(claim * 100 + offset)
}
fn request(actor: ParticipantId, id: u128) -> RequestKey {
    RequestKey {
        principal: actor,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(id),
    }
}
fn context(actor: ParticipantId, logical_time: u64) -> NativeContext {
    NativeContext {
        principal: Principal::Actor(actor),
        logical_time,
    }
}
fn limits() -> NativeLimits {
    NativeLimits {
        range: RangeConfig {
            page_entries: 4,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 16,
        // Control transactions also close their complete validation cohort.
        plan_edges: 4096,
        preparation_bytes: 512 * 1024,
        evaluations_per_claim: 8,
        ..NativeLimits::default()
    }
}
fn with_budget(limits: NativeLimits, budget: MemoryBudget) -> Core<NativeState> {
    Core::new_native(ledger(), RangeId(31), limits, budget).unwrap()
}
fn core() -> Core<NativeState> {
    with_budget(
        limits(),
        MemoryBudget::new(32 * 1024 * 1024, 4 * 1024 * 1024).unwrap(),
    )
}
fn declaration(
    claim: u128,
    offset: u128,
    agentic: bool,
    policy: bool,
    version: u8,
) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(77),
        version: ContentHash([version; 32]),
        agentic,
    };
    let steps = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: ContentHash([5; 32]),
        diagnostic_schema: ContentHash([6; 32]),
    }];
    let check = validation::PhasePolicy {
        evaluator: EVALUATOR,
        definition: ContentHash([4; 32]),
        handlers: &steps,
        required_policy: policy.then_some(ContentHash([8; 32])),
    };
    let delivery = offset == 1;
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(claim * 100 + offset),
            claim: ClaimId::from_u128(claim),
            issuer: ISSUER,
            declaration_index: u32::try_from(offset - 1).unwrap(),
            kind: if delivery {
                ValidationKind::Receipt
            } else {
                ValidationKind::Test
            },
            phase: if delivery {
                ValidationPhase::WholeWork
            } else {
                ValidationPhase::Admission
            },
            mode: if offset == 3 {
                ValidationMode::Observe
            } else {
                ValidationMode::Required
            },
            target: if delivery {
                validation::TargetDeclaration::Delivery
            } else {
                validation::TargetDeclaration::Admission
            },
            program: if delivery {
                validation::Program::Delivery
            } else if agentic {
                validation::Program::Agentic { check }
            } else {
                validation::Program::Programmatic {
                    check,
                    quality: None,
                }
            },
            deadline: Deadline {
                timer: TimerId::from_u128(claim * 100 + offset),
                generation: 1,
                at: DEADLINE,
            },
        },
        validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}
fn cohort(
    id: u128,
    agentic: bool,
    policy: bool,
    version: u8,
) -> (Proposal, Vec<validation::Declaration>) {
    let definitions = vec![
        declaration(id, 1, false, false, 1),
        declaration(id, 2, agentic, policy, version),
    ];
    (proposal(id, &definitions), definitions)
}
fn proposal(id: u128, declarations: &[validation::Declaration]) -> Proposal {
    Proposal {
        definition: ClaimDefinition {
            binding: binding(id),
            issuer: ISSUER,
            subject: SUBJECT,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding(id), RootCommandId::from_u128(id)).unwrap(),
            acceptance: aggregation::AcceptancePolicy::new(
                binding(id),
                ISSUER,
                &[],
                declarations,
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 16,
                    max_results: 16,
                    max_updates: 8,
                },
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 16,
                children: 8,
            },
        },
        owner: None,
    }
}
fn owned(id: u128, parent: Binding) -> (Proposal, Vec<validation::Declaration>) {
    let (mut claim, definitions) = cohort(id, false, false, 1);
    claim.definition.lineage =
        Lineage::new(binding(id), Cause::Claim(ClaimId(parent.object.0)), &[], 0).unwrap();
    claim.owner = Some(Owner {
        expected: parent,
        receipt: None,
    });
    (claim, definitions)
}
fn successor(
    id: u128,
    predecessor: u128,
    kind: CorrectionKind,
) -> (Proposal, Vec<validation::Declaration>) {
    let (mut claim, definitions) = cohort(id, false, false, 1);
    claim.definition.lineage = Lineage::new(
        binding(id),
        Cause::Root(RootCommandId::from_u128(id)),
        &[Correction {
            kind,
            predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(predecessor)),
        }],
        1,
    )
    .unwrap();
    (claim, definitions)
}
fn create(id: u128, cohorts: Vec<(Proposal, Vec<validation::Declaration>)>) -> NativeInput {
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for (claim, definitions) in cohorts {
        claims.push(claim);
        declarations.extend(definitions);
    }
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Create {
            claims,
            declarations,
        },
    }
}
fn post(id: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Post { expected },
    }
}
fn evaluation_key(claim: u128) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(claim),
        validation: declaration_id(claim, 2),
        target: EvaluationTarget::Admission,
        generation: 1,
    }
}
fn begin(id: u128, claim: Binding, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(EVALUATOR, id),
        command: NativeCommand::BeginAdmission {
            claim,
            key: evaluation_key(u128::from_be_bytes(claim.object.0)),
            expected,
        },
    }
}
fn cancel(id: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Cancel { expected },
    }
}
fn prepare(
    core: &Core<NativeState>,
    actor: ParticipantId,
    time: u64,
    input: NativeInput,
    pending: &[&NativePrepared],
) -> NativePrepared {
    match core
        .prepare_native(context(actor, time), input, pending)
        .unwrap()
    {
        NativePreparation::Prepared(value) => value,
        NativePreparation::Existing { .. } => panic!("new request unexpectedly replayed"),
    }
}
fn publish(
    core: &mut Core<NativeState>,
    actor: ParticipantId,
    time: u64,
    input: NativeInput,
) -> NativeOutcome {
    let prepared = prepare(core, actor, time, input, &[]);
    core.publish_native(prepared).unwrap()
}
fn posted(core: &mut Core<NativeState>, id: u128, agentic: bool, policy: bool) -> Binding {
    publish(
        core,
        ISSUER,
        10,
        create(id * 10, vec![cohort(id, agentic, policy, 1)]),
    );
    publish(core, ISSUER, 20, post(id * 10 + 1, binding(id)));
    core.native_claim(ClaimId::from_u128(id)).unwrap().binding()
}
fn facts(core: &Core<NativeState>, outcome: NativeOutcome) -> Vec<NativeFact> {
    (0..outcome.events)
        .map(|ordinal| {
            let event = core.native_event(outcome.sequence, ordinal).unwrap();
            assert_eq!(event.invocation, outcome.invocation);
            assert_eq!(event.ordinal, ordinal);
            event.fact
        })
        .collect()
}
fn assert_fence_then_seal(
    candidate: &NativePrepared,
    key: EvaluationKey,
    previous: validation::EvaluationState,
    reason: validation::FenceReason,
) {
    let next = candidate.evaluation(key).unwrap();
    let definition = candidate.definition(key.validation).unwrap();
    let outcome = candidate.outcome();
    let fenced_binding = previous.binding().next().unwrap();
    assert_eq!(next.binding(), fenced_binding.next().unwrap());
    assert_eq!(next.state(), previous.state());
    assert_eq!(next.target(), previous.target());
    assert_eq!(next.generation(), previous.generation());
    assert_eq!(next.receipt(), previous.receipt());
    assert_eq!(next.phase(), previous.phase());
    assert_eq!(next.has_begun(), previous.has_begun());
    assert_eq!(next.last_result(), previous.last_result());
    assert_eq!(
        next.fence(),
        Some(validation::AuthorityFence {
            reason,
            cause: outcome.intent,
        })
    );
    assert!(previous.sealed().is_none());
    assert!(
        next.sealed()
            .is_some_and(|cause| cause != ContentHash([0; 32]))
    );
    let attempt = previous.has_begun().then(|| {
        previous
            .bind(definition)
            .unwrap()
            .current_attempt()
            .unwrap()
    });
    assert_eq!(
        next.has_begun()
            .then(|| next.bind(definition).unwrap().current_attempt().unwrap()),
        attempt
    );
    let events: Vec<_> = (0..outcome.events)
        .map(|ordinal| {
            let Some(Row::Event(event)) = candidate
                .fragments
                .get(&Key::Event(outcome.sequence, ordinal))
            else {
                panic!("retained event");
            };
            event.get().unwrap().expand(outcome.ledger).fact
        })
        .filter(|fact| matches!(fact, NativeFact::Evaluation { key: actual, .. } if *actual == key))
        .collect();
    assert_eq!(
        events,
        vec![
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::AuthorityFenced,
                key,
                before: Some(previous.binding()),
                after: fenced_binding,
                state: previous.state(),
                phase: previous.phase(),
                attempt,
                fence: next.fence(),
            },
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Sealed,
                key,
                before: Some(fenced_binding),
                after: next.binding(),
                state: next.state(),
                phase: next.phase(),
                attempt,
                fence: next.fence(),
            },
        ]
    );
}
fn contract(result: Result<NativePreparation, NativeError>, expected: ContractError) {
    assert!(
        matches!(result, Err(NativeError::Contract(actual)) if actual == expected),
        "expected {expected:?}, got {result:?}"
    );
}

#[test]
fn pending_create_post_begin_publish_full_definitions_and_correct_programmatic_or_agentic_phase() {
    for agentic in [false, true] {
        let mut core = core();
        let creation = prepare(
            &core,
            ISSUER,
            10,
            create(1, vec![cohort(1, agentic, false, 1)]),
            &[],
        );
        assert_eq!(creation.outcome().definitions, 2);
        assert!(creation.definition(declaration_id(1, 1)).is_some());
        assert!(creation.definition(declaration_id(1, 2)).is_some());
        let posting = prepare(&core, ISSUER, 20, post(2, binding(1)), &[&creation]);
        let claim = posting.claim(ClaimId::from_u128(1)).unwrap().binding();
        let ready = *posting.evaluation(evaluation_key(1)).unwrap();
        assert_eq!(ready.state(), validation::State::Ready);
        assert_eq!(ready.target(), validation::Target::Admission { claim });
        assert_eq!(ready.receipt(), None);
        assert!(!ready.has_begun());
        assert!(
            posting
                .evaluation(EvaluationKey {
                    validation: declaration_id(1, 1),
                    ..evaluation_key(1)
                })
                .is_none()
        );
        let beginning = prepare(
            &core,
            EVALUATOR,
            30,
            begin(3, claim, ready.binding()),
            &[&creation, &posting],
        );
        let running = beginning.evaluation(evaluation_key(1)).unwrap();
        let expected_state = if agentic {
            validation::State::ValidatingQualityBar
        } else {
            validation::State::Validating
        };
        assert_eq!(running.state(), expected_state);
        assert!(running.has_begun());
        assert!(running.last_result().is_none());
        assert_eq!(
            beginning.claim(ClaimId::from_u128(1)).unwrap().status(),
            ClaimStatus::Posted
        );
        assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
        assert!(core.native_definition(declaration_id(1, 2)).is_none());
        assert!(core.native_evaluation(evaluation_key(1)).is_none());
        let created = core.publish_native(creation).unwrap();
        let posted = core.publish_native(posting).unwrap();
        let begun = core.publish_native(beginning).unwrap();
        assert_eq!(created.events, 3);
        assert_eq!(posted.events, 2);
        assert_eq!(begun.events, 1);
        assert!(matches!(
            facts(&core, posted).as_slice(),
            [
                NativeFact::Claim(NativeClaimEvent {
                    kind: NativeEventKind::Posted,
                    ..
                }),
                NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::Materialized,
                    before: None,
                    state: validation::State::Ready,
                    ..
                }
            ]
        ));
        let begun_facts = facts(&core, begun);
        let [
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Begun,
                state,
                attempt: Some(attempt),
                fence: None,
                before: Some(before),
                after,
                ..
            },
        ] = begun_facts.as_slice()
        else {
            panic!("missing exact begin event")
        };
        assert_eq!(*state, expected_state);
        assert_eq!(attempt.evaluator, EVALUATOR);
        assert_eq!(attempt.index, 0);
        assert_eq!(
            attempt.phase,
            if agentic {
                validation::Phase::Quality
            } else {
                validation::Phase::Programmatic
            }
        );
        assert_eq!(*before, ready.binding());
        assert_eq!(*after, ready.binding().next().unwrap());
    }
}

#[test]
fn admission_post_requires_issuer_and_begin_requires_actual_posted_designated_actor() {
    let mut core = core();
    publish(
        &mut core,
        ISSUER,
        10,
        create(1, vec![cohort(1, false, false, 1)]),
    );
    let budget = core.native_budget();
    assert!(
        core.prepare_native(
            context(EVALUATOR, 11),
            begin(2, binding(1), binding(102)),
            &[]
        )
        .is_err()
    );
    for principal in [Principal::Actor(SUBJECT), Principal::Node(ISSUER)] {
        let input = NativeInput {
            request: request(
                match principal {
                    Principal::Actor(id) | Principal::Node(id) => id,
                },
                3,
            ),
            command: NativeCommand::Post {
                expected: binding(1),
            },
        };
        contract(
            core.prepare_native(
                NativeContext {
                    principal,
                    logical_time: 12,
                },
                input,
                &[],
            ),
            ContractError::WrongActor,
        );
    }
    assert_eq!(core.native_budget(), budget);
    publish(&mut core, ISSUER, 20, post(4, binding(1)));
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    for principal in [
        Principal::Actor(ISSUER),
        Principal::Actor(SUBJECT),
        Principal::Node(EVALUATOR),
    ] {
        let mut input = begin(5, claim, binding(102));
        input.request.principal = match principal {
            Principal::Actor(id) | Principal::Node(id) => id,
        };
        contract(
            core.prepare_native(
                NativeContext {
                    principal,
                    logical_time: 30,
                },
                input,
                &[],
            ),
            ContractError::WrongActor,
        );
    }
    assert_eq!(
        core.native_evaluation(evaluation_key(1)).unwrap().state(),
        validation::State::Ready
    );
    assert!(core.native_outcome(request(EVALUATOR, 5)).is_none());
}

#[test]
fn begin_rejects_stale_or_substituted_claim_evaluation_target_and_generation() {
    let mut core = core();
    let claim = posted(&mut core, 1, false, false);
    let ready = core.native_evaluation(evaluation_key(1)).unwrap().binding();
    let before = core.native_budget();
    for case in 0..6 {
        let mut input = begin(100 + case, claim, ready);
        let NativeCommand::BeginAdmission {
            claim,
            key,
            expected,
        } = &mut input.command
        else {
            panic!()
        };
        match case {
            0 => claim.revision = ObjectRevision(1),
            1 => expected.revision = ObjectRevision(2),
            2 => expected.content = ContentHash([91; 32]),
            3 => key.generation = 2,
            4 => key.claim = ClaimId::from_u128(2),
            _ => {
                key.target = EvaluationTarget::Delivery {
                    response: TestamentId::from_u128(3),
                }
            }
        }
        assert!(
            core.prepare_native(context(EVALUATOR, 30), input, &[])
                .is_err(),
            "case {case}"
        );
        assert_eq!(core.native_budget(), before);
        assert_eq!(
            core.native_evaluation(evaluation_key(1)).unwrap().binding(),
            ready
        );
        assert!(
            core.native_outcome(request(EVALUATOR, 100 + case))
                .is_none()
        );
    }
}

#[test]
fn owner_time_is_monotone_and_deadline_refusal_does_not_advance_it() {
    let mut core = core();
    let claim = posted(&mut core, 1, false, false);
    contract(
        core.prepare_native(context(EVALUATOR, 19), begin(30, claim, binding(102)), &[]),
        ContractError::InvalidCut,
    );
    for time in [DEADLINE, DEADLINE + 1] {
        contract(
            core.prepare_native(
                context(EVALUATOR, time),
                begin(u128::from(time), claim, binding(102)),
                &[],
            ),
            ContractError::StaleEvaluation,
        );
    }
    let outcome = publish(&mut core, EVALUATOR, 30, begin(31, claim, binding(102)));
    assert_eq!(outcome.logical_time, 30);
    assert_eq!(core.native_sequence(), SessionSeq(3));
}

#[test]
fn explicit_additional_policy_never_becomes_a_fabricated_grant() {
    let mut core = core();
    let claim = posted(&mut core, 1, true, true);
    let before = core.native_budget();
    contract(
        core.prepare_native(context(EVALUATOR, 30), begin(20, claim, binding(102)), &[]),
        ContractError::InvalidPolicy,
    );
    assert_eq!(core.native_budget(), before);
    let value = core.native_evaluation(evaluation_key(1)).unwrap();
    assert_eq!(value.state(), validation::State::Ready);
    assert!(!value.has_begun());
    assert!(value.last_result().is_none());
    assert!(value.fence().is_none());
}

#[test]
fn cancellation_fences_pending_begun_evaluation_and_preserves_unrelated_claim() {
    let mut core = core();
    let creation = prepare(
        &core,
        ISSUER,
        10,
        create(
            1,
            vec![cohort(1, false, false, 1), cohort(2, false, false, 1)],
        ),
        &[],
    );
    let posting = prepare(&core, ISSUER, 20, post(2, binding(1)), &[&creation]);
    let claim = posting.claim(ClaimId::from_u128(1)).unwrap().binding();
    let beginning = prepare(
        &core,
        EVALUATOR,
        30,
        begin(3, claim, binding(102)),
        &[&creation, &posting],
    );
    let before = *beginning.evaluation(evaluation_key(1)).unwrap();
    let cancellation = prepare(
        &core,
        ISSUER,
        40,
        cancel(4, claim),
        &[&creation, &posting, &beginning],
    );
    let value = cancellation.evaluation(evaluation_key(1)).unwrap();
    assert_eq!(value.state(), validation::State::Validating);
    assert_fence_then_seal(
        &cancellation,
        evaluation_key(1),
        before,
        validation::FenceReason::Cancellation,
    );
    assert_eq!(
        value.fence().unwrap().reason,
        validation::FenceReason::Cancellation
    );
    assert!(value.last_result().is_none());
    assert_eq!(
        cancellation.claim(ClaimId::from_u128(2)).unwrap().status(),
        ClaimStatus::Generated
    );
    for candidate in [creation, posting, beginning] {
        core.publish_native(candidate).unwrap();
    }
    let outcome = core.publish_native(cancellation).unwrap();
    assert_eq!(outcome.evaluations, 1);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Cancelled
    );
    assert!(facts(&core, outcome).iter().any(|fact| matches!(
        fact,
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::AuthorityFenced,
            fence: Some(validation::AuthorityFence {
                reason: validation::FenceReason::Cancellation,
                ..
            }),
            state: validation::State::Validating,
            ..
        }
    )));
}

#[test]
fn cancel_before_begin_fences_ready_without_fake_result_and_repeated_control_preserves_fence() {
    let mut core = core();
    let claim = posted(&mut core, 1, false, false);
    publish(&mut core, ISSUER, 30, cancel(30, claim));
    let state = *core.native_evaluation(evaluation_key(1)).unwrap();
    assert_eq!(state.state(), validation::State::Ready);
    assert!(!state.has_begun());
    assert!(state.last_result().is_none());
    let terminal = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    assert!(
        core.prepare_native(
            context(EVALUATOR, 31),
            begin(31, terminal, state.binding()),
            &[]
        )
        .is_err()
    );
    publish(&mut core, ISSUER, 32, cancel(32, terminal));
    let after = core.native_evaluation(evaluation_key(1)).unwrap();
    assert_eq!(after.binding(), state.binding());
    assert_eq!(after.fence(), state.fence());
    assert_eq!(after.last_result(), state.last_result());
}

#[test]
fn owned_tree_cancel_preserves_prior_terminal_descendant_and_exact_evaluation_fence() {
    let mut core = core();
    publish(
        &mut core,
        ISSUER,
        10,
        create(1, vec![cohort(1, false, false, 1), owned(2, binding(1))]),
    );
    publish(&mut core, ISSUER, 20, post(2, binding(2)));
    let child = core.native_claim(ClaimId::from_u128(2)).unwrap().binding();
    publish(&mut core, EVALUATOR, 30, begin(3, child, binding(202)));
    publish(&mut core, ISSUER, 40, cancel(4, child));
    let child = core.native_claim(ClaimId::from_u128(2)).unwrap();
    let original = (child.binding(), child.terminal_cut());
    let state = *core.native_evaluation(evaluation_key(2)).unwrap();
    let parent = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(&mut core, ISSUER, 50, cancel(5, parent));
    let child = core.native_claim(ClaimId::from_u128(2)).unwrap();
    assert_eq!((child.binding(), child.terminal_cut()), original);
    let after = core.native_evaluation(evaluation_key(2)).unwrap();
    assert_eq!(after.binding(), state.binding());
    assert_eq!(after.fence(), state.fence());
    assert!(!child.released());
    assert!(!core.native_claim(ClaimId::from_u128(1)).unwrap().released());
}

#[test]
fn pending_supersession_fences_actual_predecessor_but_amends_does_not() {
    for kind in [CorrectionKind::Supersedes, CorrectionKind::Amends] {
        let mut core = core();
        let claim = posted(&mut core, 1, false, false);
        let beginning = prepare(&core, EVALUATOR, 30, begin(3, claim, binding(102)), &[]);
        let before = *beginning.evaluation(evaluation_key(1)).unwrap();
        let correction = prepare(
            &core,
            ISSUER,
            40,
            create(4, vec![successor(2, 1, kind)]),
            &[&beginning],
        );
        let value = correction.evaluation(evaluation_key(1)).unwrap();
        let supersedes = kind == CorrectionKind::Supersedes;
        assert_eq!(
            value.fence().map(|fence| fence.reason),
            supersedes.then_some(validation::FenceReason::Supersession)
        );
        if supersedes {
            assert_fence_then_seal(
                &correction,
                evaluation_key(1),
                before,
                validation::FenceReason::Supersession,
            );
        } else {
            assert_eq!(*value, before);
            assert!(value.sealed().is_none());
        }
        assert_eq!(value.state(), validation::State::Validating);
        assert!(value.last_result().is_none());
        assert_eq!(
            correction.claim(ClaimId::from_u128(1)).unwrap().status(),
            if supersedes {
                ClaimStatus::Superseded
            } else {
                ClaimStatus::Posted
            }
        );
        core.publish_native(beginning).unwrap();
        core.publish_native(correction).unwrap();
        assert!(core.native_definition(declaration_id(2, 2)).is_some());
    }
}

#[test]
fn incomplete_extra_duplicate_and_substituted_definition_cohorts_refuse_atomically() {
    for case in 0..4 {
        let core = core();
        let before = core.native_budget();
        let (claim, mut definitions) = cohort(1, false, false, 1);
        match case {
            0 => {
                definitions.pop();
            }
            1 => definitions.push(declaration(2, 2, false, false, 1)),
            2 => definitions.push(declaration(1, 2, false, false, 1)),
            _ => {
                definitions[1] = declaration(1, 2, false, false, 9);
            }
        }
        let input = create(1, vec![(claim, definitions)]);
        assert!(
            core.prepare_native(context(ISSUER, 10), input, &[])
                .is_err(),
            "case {case}"
        );
        assert_eq!(core.native_budget(), before);
        assert_eq!(core.native_sequence(), SessionSeq(0));
        assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
        assert!(core.native_definition(declaration_id(1, 1)).is_none());
        assert!(core.native_definition(declaration_id(1, 2)).is_none());
        assert!(core.native_outcome(request(ISSUER, 1)).is_none());
        assert!(core.native_event(SessionSeq(1), 0).is_none());
    }
}

#[test]
fn exact_retry_keeps_original_context_and_full_definition_semantics() {
    let mut core = core();
    let input = || create(1, vec![cohort(1, false, false, 1)]);
    let pending = prepare(&core, ISSUER, 10, input(), &[]);
    let outcome = pending.outcome();
    for time in [0, 9999] {
        assert!(
            matches!(core.prepare_native(context(ISSUER, time), input(), &[&pending]).unwrap(), NativePreparation::Existing { outcome: actual, committed: false } if actual == outcome)
        );
    }
    assert!(matches!(
        core.prepare_native(
            context(ISSUER, 20),
            create(1, vec![cohort(1, false, false, 9)]),
            &[&pending]
        ),
        Err(NativeError::RequestConflict)
    ));
    core.publish_native(pending).unwrap();
    assert!(
        matches!(core.prepare_native(context(ISSUER, 0), input(), &[]).unwrap(), NativePreparation::Existing { outcome: actual, committed: true } if actual == outcome)
    );
    assert_eq!(core.native_sequence(), SessionSeq(1));
}

#[test]
fn begin_retry_is_exact_after_state_advanced_and_cancelled() {
    let mut core = core();
    let claim = posted(&mut core, 1, false, false);
    let started = publish(&mut core, EVALUATOR, 30, begin(30, claim, binding(102)));
    publish(&mut core, ISSUER, 40, cancel(40, claim));
    assert!(
        matches!(core.prepare_native(context(EVALUATOR, 10000), begin(30, claim, binding(102)), &[]).unwrap(), NativePreparation::Existing { outcome, committed: true } if outcome == started)
    );
    let changed = begin(30, claim, binding(102).next().unwrap());
    assert!(matches!(
        core.prepare_native(context(EVALUATOR, 10000), changed, &[]),
        Err(NativeError::RequestConflict)
    ));
    assert_eq!(core.native_sequence(), SessionSeq(4));
}

#[test]
fn fixed_prefix_reads_keep_ready_definition_and_outcome_across_begin_and_cancel() {
    let mut core = core();
    let claim = posted(&mut core, 1, false, false);
    let read = core.pin_native(0, 100).unwrap();
    assert_eq!(read.sequence(), SessionSeq(2));
    publish(&mut core, EVALUATOR, 30, begin(30, claim, binding(102)));
    publish(&mut core, ISSUER, 40, cancel(40, claim));
    assert_eq!(
        read.with_evaluation(evaluation_key(1), 1, |row| (row.state(), row.fence()))
            .unwrap(),
        Some((validation::State::Ready, None))
    );
    assert_eq!(
        read.with_claim(ClaimId::from_u128(1), 1, ClaimState::status)
            .unwrap(),
        Some(ClaimStatus::Posted)
    );
    assert_eq!(
        read.with_definition(declaration_id(1, 2), 1, validation::Declaration::binding)
            .unwrap(),
        Some(binding(102))
    );
    assert_eq!(read.recorded(request(EVALUATOR, 30), 1).unwrap(), None);
    let retained = core.native_budget().used;
    core.release_native(&read).unwrap();
    assert!(core.native_budget().used < retained);
    assert!(
        read.with_evaluation(evaluation_key(1), 2, |row| row.state())
            .is_err()
    );
}

#[test]
fn late_admission_registration_capacity_rolls_back_post_and_every_evaluation() {
    let mut configured = limits();
    configured.evaluations_per_claim = 1;
    let mut core = with_budget(
        configured,
        MemoryBudget::new(32 * 1024 * 1024, 4 * 1024 * 1024).unwrap(),
    );
    let definitions = vec![
        declaration(1, 1, false, false, 1),
        declaration(1, 2, false, false, 1),
        declaration(1, 3, true, false, 1),
    ];
    let claim = proposal(1, &definitions);
    publish(&mut core, ISSUER, 10, create(1, vec![(claim, definitions)]));
    let before = core.native_budget();
    assert!(
        core.prepare_native(context(ISSUER, 20), post(2, binding(1)), &[])
            .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().binding(),
        binding(1)
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
    assert!(core.native_evaluation(evaluation_key(1)).is_none());
    assert!(
        core.native_evaluation(EvaluationKey {
            validation: declaration_id(1, 3),
            ..evaluation_key(1)
        })
        .is_none()
    );
    assert!(core.native_outcome(request(ISSUER, 2)).is_none());
    assert!(core.native_event(SessionSeq(2), 0).is_none());
}

#[test]
fn ordinary_pressure_refuses_begin_but_completion_fences_and_prepared_publish_need_no_new_capacity()
{
    let budget = MemoryBudget::new(8 * 1024 * 1024, 2 * 1024 * 1024).unwrap();
    let mut core = with_budget(limits(), budget.clone());
    let claim = posted(&mut core, 1, false, false);
    let stats = budget.stats();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap();
    let before = budget.stats();
    assert!(matches!(
        core.prepare_native(context(EVALUATOR, 30), begin(30, claim, binding(102)), &[]),
        Err(NativeError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(budget.stats(), before);
    let cancellation = prepare(&core, ISSUER, 40, cancel(40, claim), &[]);
    let stats = budget.stats();
    let rest = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            stats.limit - stats.used,
        )
        .unwrap();
    assert_eq!(budget.stats().used, stats.limit);
    core.publish_native(cancellation).unwrap();
    let evaluation = core.native_evaluation(evaluation_key(1)).unwrap();
    assert_eq!(evaluation.state(), validation::State::Ready);
    assert_eq!(
        evaluation.fence().unwrap().reason,
        validation::FenceReason::Cancellation
    );
    assert!(evaluation.last_result().is_none());
    drop(rest);
    drop(pressure);
}

#[test]
fn retained_neighbor_copy_failure_rolls_back_post_definitions_and_ready_registry() {
    let mut core = core();
    publish(
        &mut core,
        ISSUER,
        10,
        create(1, vec![cohort(1, false, false, 1)]),
    );
    let before = core.native_budget();
    let result = super::prepare::fail_copies_after(0, || {
        core.prepare_native(context(ISSUER, 20), post(2, binding(1)), &[])
    });
    assert!(
        matches!(
            result,
            Err(NativeError::Memory(MemoryError::AllocationFailed))
        ),
        "{result:?}"
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
    assert_eq!(
        core.native_definition(declaration_id(1, 2))
            .unwrap()
            .binding(),
        binding(102)
    );
    assert!(core.native_evaluation(evaluation_key(1)).is_none());
    assert!(core.native_outcome(request(ISSUER, 2)).is_none());
    publish(&mut core, ISSUER, 20, post(2, binding(1)));
    assert_eq!(
        core.native_evaluation(evaluation_key(1)).unwrap().state(),
        validation::State::Ready
    );
}

#[test]
fn child_registration_preserves_posted_parent_begun_registry_for_pending_cancellation() {
    let mut core = core();
    let parent = posted(&mut core, 1, false, false);
    publish(&mut core, EVALUATOR, 30, begin(30, parent, binding(102)));
    let before = *core.native_evaluation(evaluation_key(1)).unwrap();
    let child = prepare(&core, ISSUER, 40, create(40, vec![owned(2, parent)]), &[]);
    let changed_parent = child.claim(ClaimId::from_u128(1)).unwrap();
    assert_eq!(changed_parent.status(), ClaimStatus::Posted);
    assert_eq!(changed_parent.binding(), parent.next().unwrap());
    assert_eq!(changed_parent.scopes().children().len(), 1);
    assert_eq!(changed_parent.scopes().children()[0].binding(), binding(2));
    assert_eq!(child.evaluation(evaluation_key(1)), Some(&before));
    let cancellation = prepare(
        &core,
        ISSUER,
        50,
        cancel(50, changed_parent.binding()),
        &[&child],
    );
    let fenced = cancellation.evaluation(evaluation_key(1)).unwrap();
    assert_fence_then_seal(
        &cancellation,
        evaluation_key(1),
        before,
        validation::FenceReason::Cancellation,
    );
    assert_eq!(fenced.state(), validation::State::Validating);
    assert_eq!(
        fenced.fence().unwrap().reason,
        validation::FenceReason::Cancellation
    );
    assert!(fenced.last_result().is_none());
    for id in [1, 2] {
        assert_eq!(
            cancellation.claim(ClaimId::from_u128(id)).unwrap().status(),
            ClaimStatus::Cancelled
        );
    }
    assert_eq!(core.native_evaluation(evaluation_key(1)), Some(&before));
    let child_outcome = core.publish_native(child).unwrap();
    let cancelled = core.publish_native(cancellation).unwrap();
    assert!(facts(&core, child_outcome).iter().any(|fact| matches!(fact,
        NativeFact::Claim(NativeClaimEvent { kind: NativeEventKind::ChildRegistered, owned_child: Some(actual), before: Some(old), after, .. })
        if *actual == binding(2) && *old == parent && *after == parent.next().unwrap()
    )));
    assert_eq!(cancelled.evaluations, 1);
    assert_eq!(
        core.native_evaluation(evaluation_key(1))
            .unwrap()
            .fence()
            .unwrap()
            .cause,
        cancelled.intent
    );
}

#[test]
fn all_required_and_observe_admission_members_survive_pending_begins_and_are_fenced_together() {
    let mut core = core();
    let definitions = vec![
        declaration(1, 1, false, false, 1),
        declaration(1, 2, false, false, 1),
        declaration(1, 3, true, false, 1),
    ];
    let claim = proposal(1, &definitions);
    publish(&mut core, ISSUER, 10, create(1, vec![(claim, definitions)]));
    let posting = prepare(&core, ISSUER, 20, post(2, binding(1)), &[]);
    assert_eq!(posting.outcome().evaluations, 2);
    let parent = posting.claim(ClaimId::from_u128(1)).unwrap().binding();
    let observe = EvaluationKey {
        validation: declaration_id(1, 3),
        ..evaluation_key(1)
    };
    for key in [evaluation_key(1), observe] {
        assert_eq!(
            posting.evaluation(key).unwrap().state(),
            validation::State::Ready
        );
    }
    let first = prepare(
        &core,
        EVALUATOR,
        30,
        begin(3, parent, binding(102)),
        &[&posting],
    );
    let mut second_input = begin(4, parent, binding(103));
    let NativeCommand::BeginAdmission { key, .. } = &mut second_input.command else {
        panic!()
    };
    *key = observe;
    let second = prepare(&core, EVALUATOR, 40, second_input, &[&posting, &first]);
    assert_eq!(
        second.evaluation(evaluation_key(1)).unwrap().state(),
        validation::State::Validating
    );
    assert_eq!(
        second.evaluation(observe).unwrap().state(),
        validation::State::ValidatingQualityBar
    );
    let cancellation = prepare(
        &core,
        ISSUER,
        50,
        cancel(5, parent),
        &[&posting, &first, &second],
    );
    for key in [evaluation_key(1), observe] {
        let prior = second.evaluation(key).unwrap();
        let next = cancellation.evaluation(key).unwrap();
        assert_fence_then_seal(
            &cancellation,
            key,
            *prior,
            validation::FenceReason::Cancellation,
        );
        assert_eq!(next.state(), prior.state());
        assert_eq!(
            next.fence().unwrap().reason,
            validation::FenceReason::Cancellation
        );
        assert!(next.last_result().is_none());
    }
    for candidate in [posting, first, second] {
        core.publish_native(candidate).unwrap();
    }
    let outcome = core.publish_native(cancellation).unwrap();
    assert_eq!(outcome.evaluations, 2);
    let mut keys = facts(&core, outcome)
        .into_iter()
        .filter_map(|fact| match fact {
            NativeFact::Evaluation {
                key,
                kind: NativeEvaluationEventKind::AuthorityFenced,
                fence: Some(fence),
                ..
            } => {
                assert_eq!(fence.cause, outcome.intent);
                Some(key)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, vec![evaluation_key(1), observe]);
}

#[test]
fn global_definition_and_evaluation_limits_roll_back_counts_time_and_request_outcomes() {
    // Refused creation provisionally increments both claim and definition
    // counts. Exact remaining capacity must still admit a smaller valid cohort.
    let mut configured = limits();
    configured.claims = 2;
    configured.definitions = 3;
    configured.outcomes = 2;
    let mut core = with_budget(
        configured,
        MemoryBudget::new(32 * 1024 * 1024, 4 * 1024 * 1024).unwrap(),
    );
    publish(
        &mut core,
        ISSUER,
        10,
        create(1, vec![cohort(1, false, false, 1)]),
    );
    let before = core.native_budget();
    assert!(matches!(
        core.prepare_native(
            context(ISSUER, 50),
            create(50, vec![cohort(2, false, false, 1)]),
            &[]
        ),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(1));
    assert!(core.native_claim(ClaimId::from_u128(2)).is_none());
    assert!(core.native_definition(declaration_id(2, 1)).is_none());
    assert!(core.native_outcome(request(ISSUER, 50)).is_none());
    assert!(core.native_event(SessionSeq(2), 0).is_none());
    let definitions = vec![declaration(2, 1, false, false, 1)];
    let claim = proposal(2, &definitions);
    let outcome = publish(
        &mut core,
        ISSUER,
        11,
        create(50, vec![(claim, definitions)]),
    );
    assert_eq!(
        (outcome.sequence, outcome.logical_time, outcome.definitions),
        (SessionSeq(2), 11, 1)
    );
    assert!(core.native_definition(declaration_id(1, 2)).is_some());

    // One existing evaluation leaves room for one more. A two-check Post must
    // refuse after private registration, then a different valid Post can reuse
    // the unretained request key and consume precisely that last slot.
    let mut configured = limits();
    configured.evaluations = 2;
    configured.outcomes = 3;
    let mut core = with_budget(
        configured,
        MemoryBudget::new(32 * 1024 * 1024, 4 * 1024 * 1024).unwrap(),
    );
    let definitions = vec![
        declaration(2, 1, false, false, 1),
        declaration(2, 2, false, false, 1),
        declaration(2, 3, true, false, 1),
    ];
    let claim = proposal(2, &definitions);
    publish(
        &mut core,
        ISSUER,
        10,
        create(
            1,
            vec![
                cohort(1, false, false, 1),
                (claim, definitions),
                cohort(3, false, false, 1),
            ],
        ),
    );
    publish(&mut core, ISSUER, 20, post(2, binding(1)));
    let before = core.native_budget();
    assert!(matches!(
        core.prepare_native(context(ISSUER, 50), post(50, binding(2)), &[]),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(2));
    assert_eq!(
        core.native_claim(ClaimId::from_u128(2)).unwrap().status(),
        ClaimStatus::Generated
    );
    for offset in [2, 3] {
        assert!(
            core.native_evaluation(EvaluationKey {
                validation: declaration_id(2, offset),
                ..evaluation_key(2)
            })
            .is_none()
        );
    }
    assert!(core.native_outcome(request(ISSUER, 50)).is_none());
    assert!(core.native_event(SessionSeq(3), 0).is_none());
    let outcome = publish(&mut core, ISSUER, 21, post(50, binding(3)));
    assert_eq!(
        (outcome.sequence, outcome.logical_time, outcome.evaluations),
        (SessionSeq(3), 21, 1)
    );
    assert_eq!(
        core.native_evaluation(evaluation_key(1)).unwrap().state(),
        validation::State::Ready
    );
    assert_eq!(
        core.native_evaluation(evaluation_key(3)).unwrap().state(),
        validation::State::Ready
    );
}
