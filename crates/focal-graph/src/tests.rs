use crate::*;
use focal_core::Core;
use std::collections::BTreeSet;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn input(n: u128, command: Command) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: ledger(),
        principal: ParticipantId::from_u128(3),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(n),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(4)),
            policy_revision: 1,
            logical_time: 0,
            evidence: Vec::new(),
        },
        command,
    }
}
fn claim(n: u128) -> NewClaim {
    let id = ClaimId::from_u128(n);
    let vid = ValidationId::from_u128(n + 10_000);
    let v = ValidationContent {
        ledger: ledger(),
        schema: 1,
        claim: id,
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        description: "receipt".into(),
        quality_bar: None,
        evaluator: ParticipantId::from_u128(5),
        handlers: Vec::new(),
        evidence_schemas: BTreeSet::new(),
        contributed_by: BTreeSet::from([ParticipantId::from_u128(3)]),
        policy_revision: 1,
    };
    NewClaim {
        id,
        content: ClaimContent {
            ledger: ledger(),
            schema: 1,
            occurrence: OccurrenceId::from_u128(n),
            description: format!("claim {n}"),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(ParticipantId::from_u128(3)),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(ParticipantId::from_u128(5)),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(RootCommandId::from_u128(4)),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: vec![RequirementRef {
                id: vid,
                specification: v.specification_hash().unwrap(),
            }],
            deadline: Some(Deadline {
                timer: TimerId::from_u128(n + 20_000),
                generation: 1,
                at: 1000,
            }),
        },
        validations: vec![NewValidation {
            id: vid,
            content: v,
        }],
    }
}
fn advance(core: &mut Core, n: u128, command: Command) {
    let input = input(n, command);
    let p = core.prepare(&input).unwrap();
    core.apply(SessionSeq(core.sequence().0 + 1), p).unwrap();
}
fn setup() -> (Core, GraphStore) {
    let mut core = Core::new(ledger(), Limits::default());
    advance(
        &mut core,
        1,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let graph = GraphStore::from_state(
        core.snapshot(),
        RangeId(1),
        GraphConfig::default(),
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    (core, graph)
}
fn transition(core: &mut Core, graph: &mut GraphStore, n: u128, command: Command) {
    let before = core.snapshot().clone();
    advance(core, n, command);
    let p = graph
        .prepare_transition(&before, core.snapshot(), None, BudgetLane::Ordinary)
        .unwrap();
    graph.publish(p).unwrap();
    graph.audit(core.snapshot()).unwrap();
}

#[test]
fn every_index_changes_atomically_and_matches_recovery_rebuild() {
    let (mut core, mut graph) = setup();
    let c = claim(100);
    let id = c.id;
    transition(
        &mut core,
        &mut graph,
        2,
        Command::GenerateClaim { claim: c },
    );
    let before = core.snapshot().clone();
    let old = graph.snapshot(0, 100).unwrap();
    advance(&mut core, 3, Command::PostClaim { claim: id });
    let prepared = graph
        .prepare_transition(&before, core.snapshot(), None, BudgetLane::Ordinary)
        .unwrap();
    assert_eq!(graph.sequence(), before.sequence);
    assert!(
        matches!(graph.get(ObjectRef::claim(ledger(),id)).unwrap(),Some(GraphObject::Claim(c))if c.lifecycle().status==ClaimStatus::Generated)
    );
    graph.publish(prepared).unwrap();
    graph.audit(core.snapshot()).unwrap();
    let old_status = old
        .scan(
            &GraphScan::Status(ClaimStatus::Generated),
            ReadBudget::default(),
            None,
            1,
        )
        .unwrap();
    assert_eq!(old_status.len(), 1);
    let current = graph.snapshot(1, 100).unwrap();
    assert_eq!(
        current
            .scan(
                &GraphScan::Status(ClaimStatus::Generated),
                ReadBudget::default(),
                None,
                1
            )
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        current
            .scan(
                &GraphScan::Status(ClaimStatus::Posted),
                ReadBudget::default(),
                None,
                1
            )
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        current
            .scan(&GraphScan::ByClaim(id), ReadBudget::default(), None, 1)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        current
            .scan(&GraphScan::Required(id), ReadBudget::default(), None, 1)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        current
            .scan(
                &GraphScan::DeadlinesThrough(1000),
                ReadBudget::default(),
                None,
                1
            )
            .unwrap()
            .len(),
        1
    );
    let recovered = GraphStore::from_state(
        core.snapshot(),
        RangeId(2),
        GraphConfig::default(),
        MemoryBudget::new(128 * 1024 * 1024, 0).unwrap(),
    )
    .unwrap();
    recovered.audit(core.snapshot()).unwrap();
    assert_eq!(recovered.stats().entries, graph.stats().entries);
    transition(
        &mut core,
        &mut graph,
        4,
        Command::CancelClaim {
            claim: id,
            reason: "end".into(),
        },
    );
    let final_snapshot = graph.snapshot(2, 100).unwrap();
    assert_eq!(
        final_snapshot
            .scan(
                &GraphScan::DeadlinesThrough(1000),
                ReadBudget::default(),
                None,
                2
            )
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn pending_pipeline_remains_unpublished_and_dropped_suffix_releases_budget() {
    let (mut core, mut graph) = setup();
    let base = core.snapshot().clone();
    let used = graph.memory_stats().used;
    advance(&mut core, 2, Command::GenerateClaim { claim: claim(100) });
    let first = graph
        .prepare_transition(&base, core.snapshot(), None, BudgetLane::Ordinary)
        .unwrap();
    let middle = core.snapshot().clone();
    advance(
        &mut core,
        3,
        Command::PostClaim {
            claim: ClaimId::from_u128(100),
        },
    );
    let second = graph
        .prepare_transition(&middle, core.snapshot(), Some(&first), BudgetLane::Ordinary)
        .unwrap();
    assert_eq!(graph.sequence(), base.sequence);
    assert!(graph.memory_stats().used > used);
    drop(second);
    graph.publish(first).unwrap();
    graph.audit(&middle).unwrap();
    assert_eq!(graph.sequence(), middle.sequence);
    let prepared = graph
        .prepare_transition(&middle, core.snapshot(), None, BudgetLane::Ordinary)
        .unwrap();
    let before = graph.memory_stats().used;
    drop(prepared);
    assert!(graph.memory_stats().used < before);
}

#[test]
fn snapshot_scan_cursor_cannot_change_prefix_or_query() {
    let (mut core, mut graph) = setup();
    transition(
        &mut core,
        &mut graph,
        2,
        Command::GenerateClaimBatch {
            claims: vec![claim(100), claim(101), claim(102)],
        },
    );
    let snapshot = graph.snapshot(0, 100).unwrap();
    let budget = ReadBudget {
        max_items: 1,
        ..ReadBudget::default()
    };
    let mut first = snapshot
        .scan(
            &GraphScan::Objects(Some(ObjectKind::Claim)),
            budget,
            None,
            0,
        )
        .unwrap();
    assert_eq!(first.len(), 1);
    let cursor = first.continuation.take();
    assert!(matches!(
        snapshot.scan(
            &GraphScan::Objects(Some(ObjectKind::Validation)),
            budget,
            cursor,
            0
        ),
        Err(GraphError::Memory(MemoryError::QueryMismatch))
    ));
    let snapshot2 = graph.snapshot(0, 100).unwrap();
    let mut first = snapshot
        .scan(
            &GraphScan::Objects(Some(ObjectKind::Claim)),
            budget,
            None,
            0,
        )
        .unwrap();
    assert!(matches!(
        snapshot2.scan(
            &GraphScan::Objects(Some(ObjectKind::Claim)),
            budget,
            first.continuation.take(),
            0
        ),
        Err(GraphError::Memory(MemoryError::WrongLease))
    ));
    graph.advance_clock(100).unwrap();
    assert!(matches!(
        snapshot.get(ObjectRef::claim(ledger(), ClaimId::from_u128(100)), 0),
        Err(GraphError::Memory(MemoryError::LeaseExpired))
    ));
}

#[test]
fn traversal_uses_pinned_forward_reverse_indexes_and_exact_continuations() {
    let (mut core, mut graph) = setup();
    let a = claim(100);
    let mut b = claim(101);
    let mut c = claim(102);
    for item in [&mut b, &mut c] {
        item.content.relations.insert(Relation {
            kind: RelationKind::Awaits,
            target: RelationTarget::Object(ObjectRef::claim(ledger(), a.id)),
        });
    }
    transition(
        &mut core,
        &mut graph,
        2,
        Command::GenerateClaimBatch {
            claims: vec![a, b, c],
        },
    );
    let snapshot = graph.snapshot(0, 100).unwrap();
    let query = GraphTraversalQuery {
        root: ObjectRef::claim(ledger(), ClaimId::from_u128(100)),
        direction: Direction::Reverse,
        relations: BTreeSet::from([GraphRelation::Authored(RelationKind::Awaits)]),
        authority_scope: ContentHash([1; 32]),
    };
    let mut continuation = None;
    let mut observed = Vec::new();
    loop {
        let page = snapshot
            .traverse(
                &query,
                TraversalLimits::default(),
                ReadBudget {
                    max_items: 1,
                    max_edge_visits: 1,
                    ..ReadBudget::default()
                },
                continuation,
                0,
            )
            .unwrap();
        for point in &page.objects {
            observed.push(point.items()[0].key.clone())
        }
        assert!(page.edge_visits <= 1);
        continuation = page.continuation;
        if continuation.is_none() {
            assert_eq!(page.stop, TraversalStop::Complete);
            break;
        }
    }
    assert_eq!(
        observed,
        vec![
            GraphKey::Object(ObjectKind::Claim, ObjectId::from_u128(100)),
            GraphKey::Object(ObjectKind::Claim, ObjectId::from_u128(101)),
            GraphKey::Object(ObjectKind::Claim, ObjectId::from_u128(102))
        ]
    );
    let page = snapshot
        .traverse(
            &query,
            TraversalLimits::default(),
            ReadBudget {
                max_items: 1,
                ..ReadBudget::default()
            },
            None,
            0,
        )
        .unwrap();
    let mut different = query;
    different.authority_scope = ContentHash([2; 32]);
    assert!(matches!(
        snapshot.traverse(
            &different,
            TraversalLimits::default(),
            ReadBudget::default(),
            page.continuation,
            0
        ),
        Err(GraphError::Memory(MemoryError::QueryMismatch))
    ));
}

#[test]
fn rejected_graph_preparation_never_changes_prefix_or_budget() {
    let (mut core, mut graph) = setup();
    let before = core.snapshot().clone();
    advance(&mut core, 2, Command::GenerateClaim { claim: claim(100) });
    let used = graph.memory_stats().used;
    let available = graph.memory_stats().limit - used;
    let pressure = graph
        .budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            available - 1024,
        )
        .unwrap();
    let with_pressure = graph.memory_stats().used;
    assert!(matches!(
        graph.prepare_transition(&before, core.snapshot(), None, BudgetLane::Ordinary),
        Err(GraphError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(graph.sequence(), before.sequence);
    assert_eq!(graph.memory_stats().used, with_pressure);
    drop(pressure);
    assert_eq!(graph.memory_stats().used, used);
    let prepared = graph
        .prepare_transition(&before, core.snapshot(), None, BudgetLane::Ordinary)
        .unwrap();
    graph.publish(prepared).unwrap();
    graph.audit(core.snapshot()).unwrap();
}

#[test]
fn typed_object_keyset_resumes_across_families_at_the_same_snapshot() {
    let (mut core, mut graph) = setup();
    transition(
        &mut core,
        &mut graph,
        2,
        Command::GenerateClaimBatch {
            claims: vec![claim(100), claim(101)],
        },
    );
    let snapshot = graph.snapshot(0, 100).unwrap();
    let mut after = None;
    let mut keys = Vec::new();
    loop {
        let page = snapshot
            .objects_after(
                after,
                ReadBudget {
                    max_items: 1,
                    ..ReadBudget::default()
                },
                0,
            )
            .unwrap();
        let Some(entry) = page.items().first() else {
            break;
        };
        let GraphKey::Object(kind, id) = entry.key else {
            panic!("object scan crossed into indexes")
        };
        keys.push((kind, id));
        after = Some((kind, id));
    }
    assert_eq!(
        keys,
        vec![
            (ObjectKind::Claim, ObjectId::from_u128(100)),
            (ObjectKind::Claim, ObjectId::from_u128(101)),
            (ObjectKind::Validation, ObjectId::from_u128(10_100)),
            (ObjectKind::Validation, ObjectId::from_u128(10_101))
        ]
    );
    assert!(
        snapshot
            .objects_after(
                Some((ObjectKind::Artifact, ObjectId::from_u128(u128::MAX))),
                ReadBudget::default(),
                0
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn row_patch_projection_matches_rebuild_and_retains_snapshot_prefix() {
    let mut core = Core::new(ledger(), Limits::default());
    let budget = MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let mut graph = GraphStore::from_state(
        core.snapshot(),
        RangeId(909),
        GraphConfig::default(),
        budget,
    )
    .unwrap();
    let old = graph.snapshot(0, 100).unwrap();
    let mut pending = focal_core::PendingState::new();
    pending.reserve(4).unwrap();
    let requests = [
        input(
            1,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        ),
        input(2, Command::GenerateClaim { claim: claim(100) }),
        input(
            3,
            Command::PostClaim {
                claim: ClaimId::from_u128(100),
            },
        ),
    ];
    let mut prepared = Vec::new();
    let mut roots = Vec::new();
    for request in &requests {
        let staged = core.stage_pending(&pending, request).unwrap();
        let root = graph
            .prepare_patch(
                pending.view(&core).unwrap(),
                staged.patch(),
                roots.last(),
                BudgetLane::Ordinary,
            )
            .unwrap();
        let (intent, _) = pending.accept(&core, staged).unwrap();
        prepared.push(intent);
        roots.push(root);
    }
    graph.validate_publication(roots.iter()).unwrap();
    let output = core
        .plan_epoch(prepared, focal_core::EpochLimits::default())
        .unwrap()
        .execute(&core)
        .unwrap();
    core.publish_epoch(output).unwrap();
    pending.drop_prefix(3, &core).unwrap();
    for root in roots {
        graph.publish(root).unwrap();
    }
    graph.audit(core.snapshot()).unwrap();
    assert_eq!(graph.sequence(), SessionSeq(3));
    assert_eq!(old.sequence(), SessionSeq(0));
    assert!(
        graph
            .get(ObjectRef::claim(ledger(), ClaimId::from_u128(100)))
            .unwrap()
            .is_some()
    );
}
