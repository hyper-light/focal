use super::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    Principal, aggregation,
    claim::ClaimDefinition,
    creation::Owner,
    graph, scope,
    succession::{Correction, CorrectionKind, Lineage},
    validation,
};
use focal_model::{
    Cause, Deadline, ObjectId, ObjectRef, ObjectRevision, ParticipantId, RequestEpoch, RequestId,
    RootCommandId, SessionId, TenantId, TimerId, ValidationKind, ValidationMode, ValidationPhase,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const RESPONDENT: ParticipantId = ParticipantId::from_u128(2);
fn context(principal: Principal) -> NativeContext {
    NativeContext {
        principal,
        logical_time: 0,
    }
}
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(1),
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
fn key(id: u128) -> RequestKey {
    RequestKey {
        principal: ISSUER,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(id),
    }
}
fn limits() -> NativeLimits {
    NativeLimits {
        range: RangeConfig {
            page_entries: 4,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 32,
        plan_edges: 128,
        preparation_bytes: 512 * 1024,
        ..NativeLimits::default()
    }
}
fn core() -> Core<NativeState> {
    Core::new_native(
        ledger(),
        RangeId(1),
        limits(),
        MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}
fn definition(binding: Binding) -> validation::Declaration {
    let claim_id = u128::from_be_bytes(binding.object.0);
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(claim_id.checked_add(10_000).unwrap()),
                ..binding
            },
            claim: ClaimId(binding.object.0),
            issuer: ISSUER,
            declaration_index: 900,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 100,
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
fn acceptance(binding: Binding) -> aggregation::AcceptancePolicy {
    aggregation::AcceptancePolicy::new(
        binding,
        ISSUER,
        &[],
        &[definition(binding)],
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 32,
            max_updates: 8,
        },
    )
    .unwrap()
}
fn proposal(id: u128) -> Proposal {
    let binding = binding(id);
    Proposal {
        definition: ClaimDefinition {
            binding,
            issuer: ISSUER,
            subject: RESPONDENT,
            deadline: None,
            max_responses: 4,
            // Deliberately untrusted: only Core may assign a creation position.
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(1)).unwrap(),
            acceptance: acceptance(binding),
            scope_limits: scope::ScopeLimits {
                scopes: 8,
                roots: 32,
                children: 16,
            },
        },
        owner: None,
    }
}
fn child(id: u128, parent: Binding) -> Proposal {
    let mut proposal = proposal(id);
    proposal.definition.lineage = Lineage::new(
        proposal.definition.binding,
        Cause::Claim(ClaimId(parent.object.0)),
        &[],
        8,
    )
    .unwrap();
    proposal.owner = Some(Owner {
        expected: parent,
        receipt: None,
    });
    proposal
}
fn successor(id: u128, predecessor: u128, kind: CorrectionKind) -> Proposal {
    let mut proposal = proposal(id);
    proposal.definition.lineage = Lineage::new(
        proposal.definition.binding,
        Cause::Root(RootCommandId::from_u128(1)),
        &[Correction {
            kind,
            predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(predecessor)),
        }],
        8,
    )
    .unwrap();
    proposal
}
fn create(request: u128, proposals: Vec<Proposal>) -> NativeInput {
    let declarations = proposals
        .iter()
        .map(|proposal| definition(proposal.definition.binding))
        .collect();
    NativeInput {
        request: key(request),
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    }
}
fn cancel(request: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: key(request),
        command: NativeCommand::Cancel { expected },
    }
}
fn prepare(
    core: &Core<NativeState>,
    input: NativeInput,
    pending: &[&NativePrepared],
) -> NativePrepared {
    match core
        .prepare_native(context(Principal::Actor(ISSUER)), input, pending)
        .unwrap()
    {
        NativePreparation::Prepared(prepared) => prepared,
        NativePreparation::Existing { .. } => panic!("unexpected retry"),
    }
}
fn row(core: &Core<NativeState>, id: u128) -> &ClaimState {
    core.native_claim(ClaimId::from_u128(id)).unwrap()
}
fn publish(core: &mut Core<NativeState>, input: NativeInput) -> NativeOutcome {
    let prepared = prepare(core, input, &[]);
    core.publish_native(prepared).unwrap()
}

pub(super) fn owned_claim_fixture() -> ClaimState {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1)]));
    let claim = row(&core, 1);
    claim.try_copy(claim.copy_charge().unwrap()).unwrap()
}

#[test]
fn root_parent_child_history_and_outcome_share_one_publication() {
    let mut core = core();
    let read = core.pin_native(0, 100).unwrap();
    let prepared = prepare(
        &core,
        create(1, vec![child(2, binding(1)), proposal(1)]),
        &[],
    );
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
    assert!(core.native_outcome(key(1)).is_none());
    assert_eq!(
        prepared
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .scopes()
            .children()
            .len(),
        1
    );
    assert_eq!(prepared.outcome().created, 2);
    assert_eq!(prepared.recorded(key(1)), Some(prepared.outcome()));
    let outcome = core.publish_native(prepared).unwrap();
    assert_eq!(outcome.sequence, SessionSeq(1));
    assert_eq!(row(&core, 1).created(), SessionSeq(1));
    assert_eq!(row(&core, 2).created(), SessionSeq(1));
    assert_eq!(row(&core, 1).binding().revision, ObjectRevision(2));
    assert_eq!(row(&core, 2).status(), ClaimStatus::Generated);
    assert_eq!(core.native_outcome(key(1)), Some(outcome));
    assert_eq!(
        core.native_event(SessionSeq(1), 0)
            .unwrap()
            .claim_event()
            .unwrap()
            .after,
        binding(1)
    );
    assert_eq!(
        core.native_event(SessionSeq(1), 1)
            .unwrap()
            .claim_event()
            .unwrap()
            .after,
        row(&core, 2).binding()
    );
    assert_eq!(outcome.events, 5);
    assert_eq!(outcome.definitions, 2);
    for ordinal in 3..5 {
        let NativeFact::Definition {
            binding: declared,
            claim,
            intent,
            ..
        } = core.native_event(SessionSeq(1), ordinal).unwrap().fact
        else {
            panic!("definition history follows claim events")
        };
        let expected = definition(binding(u128::from_be_bytes(claim.0)));
        assert_eq!(declared, expected.binding());
        assert_eq!(intent, expected.intent_fingerprint());
        let id = ValidationId(declared.object.0);
        assert_eq!(
            core.native_definition(id).unwrap().intent_fingerprint(),
            intent
        );
        assert_eq!(
            read.with_definition(id, 1, validation::Declaration::binding)
                .unwrap(),
            None
        );
    }
    let registration = core
        .native_event(SessionSeq(1), 2)
        .unwrap()
        .claim_event()
        .unwrap();
    assert_eq!(registration.kind, NativeEventKind::ChildRegistered);
    assert_eq!(registration.after, row(&core, 1).binding());
    assert_eq!(registration.owned_child, Some(binding(2)));
    assert_eq!(read.recorded(key(1), 1).unwrap(), None);
    assert_eq!(
        read.with_claim(ClaimId::from_u128(1), 1, ClaimState::binding)
            .unwrap(),
        None
    );
}

#[test]
fn pending_child_is_cancelled_with_parent_and_unrelated_peer_survives() {
    let mut core = core();
    let root = prepare(&core, create(1, vec![proposal(1), proposal(10)]), &[]);
    let children = prepare(
        &core,
        create(2, vec![child(2, binding(1)), child(3, binding(2))]),
        &[&root],
    );
    let parent = children.claim(ClaimId::from_u128(1)).unwrap().binding();
    let cancelled = prepare(&core, cancel(3, parent), &[&root, &children]);
    assert_eq!(cancelled.outcome().changed, 3);
    for id in [1, 2, 3] {
        assert_eq!(
            cancelled.claim(ClaimId::from_u128(id)).unwrap().status(),
            ClaimStatus::Cancelled
        );
    }
    core.publish_native(root).unwrap();
    core.publish_native(children).unwrap();
    let old = core.pin_native(0, 100).unwrap();
    core.publish_native(cancelled).unwrap();
    assert_eq!(row(&core, 10).status(), ClaimStatus::Generated);
    assert!(!row(&core, 1).released());
    assert_eq!(
        old.with_claim(ClaimId::from_u128(2), 1, ClaimState::status)
            .unwrap(),
        Some(ClaimStatus::Generated)
    );
    assert_eq!(
        core.native_event(SessionSeq(3), 2)
            .unwrap()
            .claim_event()
            .unwrap()
            .kind,
        NativeEventKind::Cancelled
    );
}

#[test]
fn complete_lineage_and_absence_checks_cover_pending_rows_and_atomic_batch_cycles() {
    let mut core = core();
    let root = prepare(&core, create(1, vec![proposal(1)]), &[]);
    let held = core.native_budget();
    for input in [
        create(2, vec![proposal(1)]),
        create(3, vec![successor(2, 99, CorrectionKind::Amends)]),
        create(
            4,
            vec![
                successor(2, 3, CorrectionKind::Amends),
                successor(3, 2, CorrectionKind::Amends),
            ],
        ),
    ] {
        assert!(
            core.prepare_native(context(Principal::Actor(ISSUER)), input, &[&root])
                .is_err()
        );
        assert_eq!(core.native_budget(), held);
        assert_eq!(core.native_sequence(), SessionSeq(0));
    }
    let next = prepare(
        &core,
        create(5, vec![successor(2, 1, CorrectionKind::Amends)]),
        &[&root],
    );
    core.publish_native(root).unwrap();
    core.publish_native(next).unwrap();
    assert_eq!(row(&core, 1).status(), ClaimStatus::Generated);
    assert_eq!(row(&core, 2).created(), SessionSeq(2));
}

#[test]
fn successor_and_predecessor_replace_atomically_with_exact_outcome() {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1)]));
    let old = core.pin_native(0, 100).unwrap();
    let next = prepare(
        &core,
        create(2, vec![successor(2, 1, CorrectionKind::Supersedes)]),
        &[],
    );
    assert_eq!(
        next.claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Superseded
    );
    assert_eq!(row(&core, 1).status(), ClaimStatus::Generated);
    let outcome = core.publish_native(next).unwrap();
    assert_eq!(outcome.created, 1);
    assert_eq!(outcome.changed, 2);
    assert_eq!(
        core.native_event(SessionSeq(2), 1)
            .unwrap()
            .claim_event()
            .unwrap()
            .kind,
        NativeEventKind::Superseded
    );
    assert_eq!(
        old.with_claim(ClaimId::from_u128(1), 1, ClaimState::status)
            .unwrap(),
        Some(ClaimStatus::Generated)
    );
    assert_eq!(
        old.with_claim(ClaimId::from_u128(2), 1, ClaimState::status)
            .unwrap(),
        None
    );
}

#[test]
fn exact_retry_distinguishes_pending_from_committed_and_rejects_semantic_substitution() {
    let mut core = core();
    let original = prepare(&core, create(1, vec![proposal(1)]), &[]);
    let held = core.native_budget();
    let retry = core
        .prepare_native(
            context(Principal::Actor(ISSUER)),
            create(1, vec![proposal(1)]),
            &[&original],
        )
        .unwrap();
    assert!(matches!(
        retry,
        NativePreparation::Existing {
            committed: false,
            ..
        }
    ));
    assert_eq!(core.native_budget(), held);
    let outcome = core.publish_native(original).unwrap();
    let mut replay = proposal(1);
    replay.definition.created = SessionSeq(0);
    assert!(
        matches!(core.prepare_native(context(Principal::Actor(ISSUER)), create(1, vec![replay]), &[]).unwrap(),
        NativePreparation::Existing { outcome: actual, committed: true } if actual == outcome)
    );
    let mut conflicting = proposal(1);
    conflicting.definition.max_responses = 5;
    assert!(matches!(
        core.prepare_native(
            context(Principal::Actor(ISSUER)),
            create(1, vec![conflicting]),
            &[]
        ),
        Err(NativeError::RequestConflict)
    ));
    assert_eq!(core.native_sequence(), SessionSeq(1));
}

#[test]
fn foreign_forks_stale_parents_and_reordered_publication_return_owned_candidates() {
    let foreign = core();
    let mut core = core();
    let root = prepare(&core, create(1, vec![proposal(1)]), &[]);
    let wrong = prepare(&foreign, create(1, vec![proposal(1)]), &[]);
    assert!(core.validate_native_chain(&[&wrong]).is_err());
    let returned = core.publish_native(wrong).unwrap_err().prepared;
    assert_eq!(returned.outcome().sequence, SessionSeq(1));
    assert_eq!(core.native_sequence(), SessionSeq(0));
    let fork = prepare(&core, create(2, vec![proposal(2)]), &[]);
    let child = prepare(&core, create(3, vec![child(3, binding(1))]), &[&root]);
    let child = core.publish_native(child).unwrap_err().prepared;
    assert!(core.validate_native_chain(&[&fork, &child]).is_err());
    core.publish_native(root).unwrap();
    let _fork = core.publish_native(fork).unwrap_err().prepared;
    core.publish_native(child).unwrap();
    assert!(matches!(
        core.prepare_native(
            context(Principal::Actor(ISSUER)),
            cancel(4, binding(1)),
            &[]
        ),
        Err(NativeError::Contract(ContractError::StaleRevision))
    ));
    assert_eq!(core.native_sequence(), SessionSeq(2));
}

#[test]
fn dropping_suffix_reclaims_charges_and_cannot_publish_orphaned_roots() {
    let core = core();
    let initial = core.native_budget();
    let root = prepare(&core, create(1, vec![proposal(1)]), &[]);
    let base = core.native_budget();
    let child = prepare(&core, create(2, vec![child(2, binding(1))]), &[&root]);
    assert!(core.native_budget().used > base.used);
    drop(child);
    assert_eq!(core.native_budget(), base);
    drop(root);
    assert_eq!(core.native_budget(), initial);
    assert!(core.native_outcome(key(1)).is_none());
}

#[test]
fn terminal_descendant_preserves_original_cut_while_parent_cancels() {
    let mut core = core();
    publish(
        &mut core,
        create(
            1,
            vec![proposal(1), child(2, binding(1)), child(3, binding(2))],
        ),
    );
    let child = row(&core, 2).binding();
    publish(&mut core, cancel(2, child));
    let cut = row(&core, 2).terminal_cut();
    let grandchild_cut = row(&core, 3).terminal_cut();
    let child_binding = row(&core, 2).binding();
    let parent = row(&core, 1).binding();
    let outcome = publish(&mut core, cancel(3, parent));
    assert_eq!(outcome.changed, 1);
    assert_eq!(row(&core, 2).terminal_cut(), cut);
    assert_eq!(row(&core, 2).binding(), child_binding);
    assert_eq!(row(&core, 3).terminal_cut(), grandchild_cut);
}

#[test]
fn memory_pressure_refuses_creation_but_completion_and_prepared_publication_succeed() {
    let budget = MemoryBudget::new(8 * 1024 * 1024, 2 * 1024 * 1024).unwrap();
    let mut core = Core::new_native(ledger(), RangeId(1), limits(), budget.clone()).unwrap();
    publish(
        &mut core,
        create(1, vec![proposal(1), child(2, binding(1))]),
    );
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
        core.prepare_native(
            context(Principal::Actor(ISSUER)),
            create(2, vec![proposal(3)]),
            &[]
        ),
        Err(NativeError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(budget.stats(), before);
    let prepared = prepare(&core, cancel(3, row(&core, 1).binding()), &[]);
    let stats = budget.stats();
    let all_remaining = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            stats.limit - stats.used,
        )
        .unwrap();
    assert_eq!(budget.stats().used, stats.limit);
    core.publish_native(prepared).unwrap();
    assert_eq!(row(&core, 2).status(), ClaimStatus::Cancelled);
    drop(all_remaining);
    drop(pressure);
}

#[test]
fn late_batch_capacity_refusal_rolls_back_all_private_parent_changes() {
    let mut config = limits();
    // One root includes claim + definition + both events + meta/outcome and
    // its eight index rows (the claim's identity, issuer, subject, status and
    // creation, the definition's identity, evaluator and creation). An owned
    // child additionally replaces its parent and records registration.
    config.range.max_batch_entries = 14;
    let mut core = Core::new_native(
        ledger(),
        RangeId(1),
        config,
        MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    publish(&mut core, create(1, vec![proposal(1)]));
    let before = core.native_budget();
    let binding = row(&core, 1).binding();
    assert!(matches!(
        core.prepare_native(
            context(Principal::Actor(ISSUER)),
            create(2, vec![child(2, binding)]),
            &[]
        ),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(row(&core, 1).binding(), binding);
    assert!(row(&core, 1).scopes().children().is_empty());
    assert!(core.native_outcome(key(2)).is_none());
    assert!(core.native_event(SessionSeq(2), 0).is_none());
}

#[test]
fn leases_keep_fixed_prefix_then_release_all_obsolete_pages() {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(2)]));
    let read = core.pin_native(0, 10).unwrap();
    let baseline = core.native_budget().used;
    let expected = row(&core, 2).binding();
    publish(&mut core, cancel(2, expected));
    assert_eq!(
        read.with_claim(ClaimId::from_u128(2), 9, ClaimState::binding)
            .unwrap(),
        Some(expected)
    );
    assert_eq!(
        read.with_claim(ClaimId::from_u128(1), 9, ClaimState::binding)
            .unwrap(),
        None
    );
    let pinned = core.native_budget().used;
    assert!(pinned > baseline);
    assert_eq!(
        read.with_claim(ClaimId::from_u128(2), 10, ClaimState::binding),
        Err(MemoryError::LeaseExpired)
    );
    assert_eq!(core.advance_native_clock(10).unwrap(), 1);
    assert!(core.native_budget().used < pinned);
    assert_eq!(read.recorded(key(1), 0), Err(MemoryError::LeaseExpired));
}

#[test]
fn authentic_actor_and_request_principal_must_match_for_reads_and_control_admission() {
    let core = core();
    for principal in [Principal::Node(ISSUER), Principal::Actor(RESPONDENT)] {
        assert!(matches!(
            core.prepare_native(context(principal), create(1, vec![proposal(1)]), &[]),
            Err(NativeError::Contract(ContractError::WrongActor))
        ));
    }
    assert_eq!(core.native_sequence(), SessionSeq(0));
}

#[test]
fn retained_page_copy_failure_discards_partial_candidate_and_keeps_pinned_facts() {
    let mut core = core();
    publish(&mut core, create(1, vec![proposal(1), proposal(2)]));
    let old = core.pin_native(0, 100).unwrap();
    let before = core.native_budget();
    let failed = prepare::fail_copies_after(1, || {
        core.prepare_native(
            context(Principal::Actor(ISSUER)),
            create(2, vec![proposal(3)]),
            &[],
        )
    });
    assert!(matches!(
        failed,
        Err(NativeError::Memory(MemoryError::AllocationFailed))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(1));
    assert!(core.native_outcome(key(2)).is_none());
    assert!(core.native_event(SessionSeq(2), 0).is_none());
    assert_eq!(
        old.with_claim(ClaimId::from_u128(1), 1, ClaimState::binding)
            .unwrap(),
        Some(binding(1))
    );
    publish(&mut core, create(2, vec![proposal(3)]));
    assert_eq!(core.native_sequence(), SessionSeq(2));
}

#[test]
fn history_preserves_reverse_id_child_registration_and_each_parent_revision() {
    let mut core = core();
    let outcome = publish(
        &mut core,
        create(
            1,
            vec![
                proposal(9),
                child(5, binding(9)),
                child(2, binding(5)),
                child(7, binding(9)),
            ],
        ),
    );
    assert_eq!(outcome.events, 11);
    for ordinal in 0..4 {
        let event = core
            .native_event(SessionSeq(1), ordinal)
            .unwrap()
            .claim_event()
            .unwrap();
        assert_eq!(event.kind, NativeEventKind::Created);
        assert_eq!(event.after.revision, ObjectRevision(1));
    }
    let registrations: Vec<_> = (4..7)
        .map(|ordinal| {
            core.native_event(SessionSeq(1), ordinal)
                .unwrap()
                .claim_event()
                .unwrap()
        })
        .collect();
    assert_eq!(
        registrations
            .iter()
            .map(|event| event.owned_child.unwrap().object)
            .collect::<Vec<_>>(),
        vec![binding(2).object, binding(5).object, binding(7).object]
    );
    assert_eq!(
        registrations[1].owned_child.unwrap().revision,
        ObjectRevision(2)
    );
    assert_eq!(registrations[1].before.unwrap().revision, ObjectRevision(1));
    assert_eq!(registrations[2].before.unwrap().revision, ObjectRevision(2));
    assert_eq!(registrations[2].after, row(&core, 9).binding());
}

#[test]
fn cancellation_reaches_live_grandchild_below_a_superseded_owned_child() {
    let mut core = core();
    publish(
        &mut core,
        create(
            1,
            vec![proposal(1), child(2, binding(1)), child(3, binding(2))],
        ),
    );
    publish(
        &mut core,
        create(2, vec![successor(4, 2, CorrectionKind::Supersedes)]),
    );
    let terminal = row(&core, 2).binding();
    let cut = row(&core, 2).terminal_cut();
    assert_eq!(row(&core, 2).status(), ClaimStatus::Superseded);
    assert_eq!(row(&core, 3).status(), ClaimStatus::Generated);
    let root = row(&core, 1).binding();
    let outcome = publish(&mut core, cancel(3, root));
    assert_eq!(outcome.changed, 2);
    assert_eq!(row(&core, 1).status(), ClaimStatus::Cancelled);
    assert_eq!(row(&core, 2).binding(), terminal);
    assert_eq!(row(&core, 2).terminal_cut(), cut);
    assert_eq!(row(&core, 3).status(), ClaimStatus::Cancelled);
    assert_eq!(row(&core, 4).status(), ClaimStatus::Generated);
    assert!(!row(&core, 1).released());
}
