#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_stream::*;
use std::collections::{BTreeMap, BTreeSet};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn consumer() -> ConsumerId {
    ConsumerId::from_u128(3)
}
fn scope() -> ContentHash {
    ContentHash([4; 32])
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(4 * 1024 * 1024, 256 * 1024).unwrap()
}
fn delta(sequence: u64, ordinal: u32, claim: u128) -> Delta {
    Delta {
        schema: 1,
        id: DeltaId {
            ledger: ledger(),
            sequence: SessionSeq(sequence),
            ordinal,
        },
        action: LifecycleAction::Progressed,
        actor: ParticipantId::from_u128(5),
        claim: Some(ClaimId::from_u128(claim)),
        fact: DeltaFact::Progress(format!("historical {sequence}/{ordinal}")),
    }
}

struct History {
    bounds: ReplayBounds,
    transactions: BTreeMap<u64, Vec<Delta>>,
}
impl History {
    fn new(transactions: Vec<Vec<Delta>>) -> Self {
        Self {
            bounds: ReplayBounds {
                ledger: ledger(),
                floor: SessionSeq(0),
                published: SessionSeq(transactions.len() as u64),
            },
            transactions: transactions
                .into_iter()
                .enumerate()
                .map(|(index, deltas)| (index as u64 + 1, deltas))
                .collect(),
        }
    }
}
impl DeltaSource for History {
    fn bounds(&self) -> ReplayBounds {
        self.bounds
    }
    fn replay(
        &self,
        after: Position,
        limit: ReplayLimit,
        visit: &mut dyn FnMut(&Delta) -> Result<(), StreamError>,
    ) -> Result<Position, StreamError> {
        if after.retention_prefix() < self.bounds.floor {
            return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
        }
        let first = match after.offset {
            PositionOffset::Delta(_) => after.sequence.0,
            PositionOffset::Resolved => after.sequence.0.saturating_add(1),
        };
        let last = self
            .bounds
            .published
            .0
            .min(after.sequence.0.saturating_add(limit.max_sequences));
        let mut position = after;
        let mut count = 0;
        let mut bytes = 0;
        for sequence in first..=last {
            for delta in self
                .transactions
                .get(&sequence)
                .ok_or(StreamError::SourceUnavailable)?
            {
                if Position::after_delta(delta.id) <= after {
                    continue;
                }
                let length = postcard::experimental::serialized_size(delta).unwrap();
                if count == limit.max_items || bytes + length > limit.max_bytes {
                    if count == 0 && position == after && bytes + length > limit.max_bytes {
                        return Err(StreamError::Capacity);
                    }
                    return Ok(position);
                }
                visit(delta)?;
                bytes += length;
                count += 1;
                position = Position::after_delta(delta.id);
            }
            position = Position::resolved(ledger(), SessionSeq(sequence));
        }
        Ok(position)
    }
}

fn command(registry: &CursorRegistry, now: u64, operation: CursorOperation) -> CursorCommand {
    CursorCommand {
        expected_revision: registry.revision(),
        now,
        operation,
    }
}
fn commit(registry: &mut CursorRegistry, now: u64, operation: CursorOperation, published: u64) {
    let command = command(registry, now, operation);
    let prepared = registry.prepare(&command, SessionSeq(published)).unwrap();
    // Fixture's serialized command is the durable log record. Restarts below
    // replay these exact bytes or restore the corresponding checkpoint.
    let encoded = postcard::to_allocvec(&command).unwrap();
    assert_eq!(
        postcard::from_bytes::<CursorCommand>(&encoded).unwrap(),
        command
    );
    registry.publish(prepared).unwrap();
}
fn registered(budget: MemoryBudget, filter: DeltaFilter, expires_at: u64) -> CursorRegistry {
    let mut registry = CursorRegistry::new(ledger(), RegistryConfig::default(), budget).unwrap();
    commit(
        &mut registry,
        0,
        CursorOperation::Register {
            consumer: consumer(),
            scope: scope(),
            filter,
            start: Position::origin(ledger()),
            expires_at,
        },
        100,
    );
    registry
}
fn transport(items: usize) -> TransportConfig {
    TransportConfig {
        max_queue_items: items,
        max_credit_items: items,
        ..TransportConfig::default()
    }
}
fn restored(registry: &CursorRegistry, published: u64, budget: MemoryBudget) -> CursorRegistry {
    let bytes = postcard::to_allocvec(registry.checkpoint()).unwrap();
    CursorRegistry::restore(
        postcard::from_bytes(&bytes).unwrap(),
        SessionSeq(published),
        RegistryConfig::default(),
        budget,
    )
    .unwrap()
}

#[test]
fn crash_before_durable_server_ack_redelivers_exact_ids_and_consumer_deduplicates() {
    let budget = budget();
    let registry = registered(budget.clone(), DeltaFilter::All, 100);
    let source = History::new(vec![
        vec![delta(1, 0, 1), delta(1, 1, 1)],
        vec![],
        vec![delta(3, 0, 1)],
    ]);
    let mut subscription = Subscription::resume(
        registry.get(consumer()).unwrap(),
        0,
        transport(2),
        budget.clone(),
    )
    .unwrap();
    let mut consumer_state =
        ConsumerCheckpoint::new(registry.get(consumer()).unwrap().token).unwrap();
    subscription.grant(2, 10_000).unwrap();
    subscription
        .pump(&source, ReplayLimit::default(), 0)
        .unwrap();
    let mut effects = BTreeSet::new();
    let mut last_token = None;
    for _ in 0..2 {
        let delivery = subscription.next(0).unwrap().unwrap();
        let advance = consumer_state.prepare(delivery.event()).unwrap();
        assert_eq!(advance.decision, ConsumerDecision::ApplyDelta);
        if let StreamEvent::Delta { cursor, delta } = delivery.event() {
            effects.insert(delta.id);
            last_token = Some(*cursor);
        } else {
            panic!("expected delta");
        }
        // The fixture commits the checkpoint with its side effect.
        consumer_state.publish(advance).unwrap();
    }
    let ack = subscription
        .acknowledge_command(last_token.unwrap(), registry.revision(), 0)
        .unwrap();
    let uncommitted = registry.prepare(&ack, source.bounds.published).unwrap();
    drop(uncommitted); // Process lost its proposal before durable commitment.
    let durable_consumer = postcard::to_allocvec(&consumer_state).unwrap();
    let mut registry = restored(&registry, 3, budget.clone());
    drop(subscription);
    let mut consumer_state: ConsumerCheckpoint = postcard::from_bytes(&durable_consumer).unwrap();
    let mut subscription = Subscription::resume(
        registry.get(consumer()).unwrap(),
        1,
        transport(2),
        budget.clone(),
    )
    .unwrap();
    subscription.grant(2, 10_000).unwrap();
    subscription
        .pump(&source, ReplayLimit::default(), 1)
        .unwrap();
    for _ in 0..2 {
        let delivery = subscription.next(1).unwrap().unwrap();
        let advance = consumer_state.prepare(delivery.event()).unwrap();
        assert_eq!(advance.decision, ConsumerDecision::Duplicate);
        consumer_state.publish(advance).unwrap();
    }
    assert_eq!(effects.len(), 2);
    let resolved = subscription.next(1).unwrap().unwrap();
    let StreamEvent::Resolved { cursor } = resolved.event() else {
        panic!("expected complete prefix");
    };
    assert_eq!(cursor.position, Position::resolved(ledger(), SessionSeq(2)));
    let ack = subscription
        .acknowledge_command(*cursor, registry.revision(), 1)
        .unwrap();
    let prepared = registry.prepare(&ack, source.bounds.published).unwrap();
    registry.publish(prepared).unwrap();
    subscription
        .acknowledged_committed(registry.get(consumer()).unwrap())
        .unwrap();
    assert_eq!(registry.retention_limit(SessionSeq(3)), SessionSeq(2));
    let registry = restored(&registry, 3, budget.clone());
    let mut last =
        Subscription::resume(registry.get(consumer()).unwrap(), 2, transport(2), budget).unwrap();
    last.grant(2, 10_000).unwrap();
    last.pump(&source, ReplayLimit::default(), 2).unwrap();
    assert!(matches!(
        last.next(2).unwrap().unwrap().event(),
        StreamEvent::Delta {
            delta: Delta {
                id: DeltaId {
                    sequence: SessionSeq(3),
                    ..
                },
                ..
            },
            ..
        }
    ));
}

#[test]
fn ordinal_cursor_blocks_whole_transaction_retirement_until_resolved() {
    let mut registry = registered(budget(), DeltaFilter::All, 10);
    let mut token = registry.get(consumer()).unwrap().token;
    token.position = Position::after_delta(delta(1, 0, 1).id);
    commit(&mut registry, 1, CursorOperation::Acknowledge { token }, 3);
    assert_eq!(registry.retention_limit(SessionSeq(3)), SessionSeq(0));
    let attempt = command(
        &registry,
        1,
        CursorOperation::AdvanceFloor {
            through: SessionSeq(1),
        },
    );
    assert!(matches!(
        registry.prepare(&attempt, SessionSeq(3)),
        Err(StreamError::RetentionPinned {
            allowed_through: SessionSeq(0)
        })
    ));
    token.position = Position::resolved(ledger(), SessionSeq(1));
    commit(&mut registry, 1, CursorOperation::Acknowledge { token }, 3);
    commit(
        &mut registry,
        1,
        CursorOperation::AdvanceFloor {
            through: SessionSeq(1),
        },
        3,
    );
    assert_eq!(registry.checkpoint().floor, SessionSeq(1));
    // Time is a durable input. Only once the lease expires may retention move.
    commit(
        &mut registry,
        10,
        CursorOperation::AdvanceFloor {
            through: SessionSeq(3),
        },
        3,
    );
    let recovered = restored(&registry, 3, budget());
    assert!(matches!(
        Subscription::resume(
            recovered.get(consumer()).unwrap(),
            10,
            transport(2),
            budget()
        ),
        Err(StreamError::ResyncRequired(ResyncReason::LeaseExpired))
    ));
}

#[test]
fn seed_registration_pins_tail_and_handoff_begins_strictly_after_snapshot() {
    let budget = budget();
    let mut registry = registered(budget.clone(), DeltaFilter::All, 100);
    let old = registry.get(consumer()).unwrap().token;
    commit(
        &mut registry,
        1,
        CursorOperation::BeginSeed {
            consumer: consumer(),
            scope: scope(),
            filter: DeltaFilter::All,
            snapshot: SessionSeq(2),
            expires_at: 100,
        },
        2,
    );
    let seed = registry.get(consumer()).unwrap();
    assert_eq!(seed.token.generation, 2);
    assert_eq!(registry.retention_limit(SessionSeq(4)), SessionSeq(2));
    assert!(matches!(
        Subscription::resume(seed, 1, transport(3), budget.clone()),
        Err(StreamError::SeedNotComplete)
    ));
    let source = History::new(vec![
        vec![delta(1, 0, 1)],
        vec![delta(2, 0, 1)],
        vec![delta(3, 0, 1)],
        vec![],
    ]);
    // Snapshot bytes are installed in the consumer before this durable command.
    commit(
        &mut registry,
        2,
        CursorOperation::CompleteSeed {
            consumer: consumer(),
            generation: 2,
            snapshot: SessionSeq(2),
        },
        4,
    );
    let mut sub =
        Subscription::resume(registry.get(consumer()).unwrap(), 2, transport(3), budget).unwrap();
    sub.grant(3, 10_000).unwrap();
    sub.pump(&source, ReplayLimit::default(), 2).unwrap();
    assert!(matches!(
        sub.next(2).unwrap().unwrap().event(),
        StreamEvent::Delta {
            delta: Delta {
                id: DeltaId {
                    sequence: SessionSeq(3),
                    ..
                },
                ..
            },
            ..
        }
    ));
    assert!(matches!(
        sub.next(2).unwrap().unwrap().event(),
        StreamEvent::Resolved {
            cursor: CursorToken {
                position: Position {
                    sequence: SessionSeq(4),
                    ..
                },
                ..
            }
        }
    ));
    assert_eq!(
        sub.acknowledge_command(old, registry.revision(), 2),
        Err(StreamError::WrongGeneration)
    );
}

#[test]
fn full_data_queue_and_exhausted_memory_do_not_block_final_resync_control() {
    let budget = budget();
    let registry = registered(budget.clone(), DeltaFilter::All, 5);
    let source = History::new(vec![vec![delta(1, 0, 1), delta(1, 1, 1)]]);
    let mut sub = Subscription::resume(
        registry.get(consumer()).unwrap(),
        0,
        transport(2),
        budget.clone(),
    )
    .unwrap();
    sub.pump(&source, ReplayLimit::default(), 0).unwrap();
    assert_eq!(sub.queued_items(), 2);
    assert!(sub.next(0).unwrap().is_none()); // No byte or item credit.
    assert!(
        sub.pump(&source, ReplayLimit::default(), 0)
            .unwrap()
            .backpressured
    );
    let available =
        budget.stats().limit - budget.stats().completion_reserve - budget.stats().ordinary_used;
    let blocker = budget
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, available)
        .unwrap();
    let final_status = sub.next(5).unwrap().unwrap();
    assert!(matches!(
        final_status.event(),
        StreamEvent::Resync {
            reason: ResyncReason::LeaseExpired,
            ..
        }
    ));
    assert_eq!(sub.queued_items(), 0);
    assert_eq!(sub.acknowledged().position, Position::origin(ledger()));
    assert!(sub.next(5).unwrap().is_none());
    drop(blocker);
}

#[test]
fn filtered_and_empty_transactions_resolve_only_after_matching_data_is_dequeued() {
    let budget = budget();
    let registry = registered(
        budget.clone(),
        DeltaFilter::Claims(BTreeSet::from([ClaimId::from_u128(1)])),
        100,
    );
    let source = History::new(vec![vec![delta(1, 0, 2)], vec![], vec![delta(3, 0, 1)]]);
    let mut sub =
        Subscription::resume(registry.get(consumer()).unwrap(), 0, transport(4), budget).unwrap();
    let report = sub.pump(&source, ReplayLimit::default(), 0).unwrap();
    assert_eq!((report.scanned, report.queued), (2, 1));
    assert!(sub.next(0).unwrap().is_none());
    sub.grant(1, 10_000).unwrap();
    let delivery = sub.next(0).unwrap().unwrap();
    assert_eq!(
        delivery.wire_bytes(),
        postcard::experimental::serialized_size(delivery.event()).unwrap()
    );
    assert!(matches!(
        delivery.event(),
        StreamEvent::Delta {
            delta: Delta {
                id: DeltaId {
                    sequence: SessionSeq(3),
                    ..
                },
                ..
            },
            ..
        }
    ));
    assert!(matches!(
        sub.next(0).unwrap().unwrap().event(),
        StreamEvent::Resolved {
            cursor: CursorToken {
                position: Position {
                    sequence: SessionSeq(3),
                    offset: PositionOffset::Resolved,
                    ..
                },
                ..
            }
        }
    ));
    assert_eq!(
        registry.get(consumer()).unwrap().token.position,
        Position::origin(ledger())
    );
}

#[test]
fn source_floor_and_lag_produce_explicit_resync_without_advancing_acknowledgment() {
    for (floor, published, expected) in [
        (1, 1, ResyncReason::HistoryExpired),
        (0, 200, ResyncReason::SlowConsumer),
    ] {
        let budget = budget();
        let registry = registered(budget.clone(), DeltaFilter::All, 100);
        let mut sub = Subscription::resume(
            registry.get(consumer()).unwrap(),
            0,
            TransportConfig {
                max_lag_sequences: 10,
                ..transport(2)
            },
            budget,
        )
        .unwrap();
        sub.reconcile(
            ReplayBounds {
                ledger: ledger(),
                floor: SessionSeq(floor),
                published: SessionSeq(published),
            },
            1,
        )
        .unwrap();
        assert!(
            matches!(sub.next(1).unwrap().unwrap().event(), StreamEvent::Resync { reason, .. } if *reason == expected)
        );
        assert_eq!(sub.acknowledged().position, Position::origin(ledger()));
        assert_eq!(
            sub.acknowledge_command(sub.acknowledged(), registry.revision(), 1),
            Err(StreamError::ResyncRequired(expected))
        );
    }
}

#[test]
fn cursor_scope_ledger_consumer_and_delivery_frontier_are_checked() {
    let budget = budget();
    let mut registry = registered(budget.clone(), DeltaFilter::All, 100);
    let mut sub =
        Subscription::resume(registry.get(consumer()).unwrap(), 0, transport(2), budget).unwrap();
    let original = sub.acknowledged();
    let mut wrong = original;
    wrong.key.ledger.session = SessionId::from_u128(9);
    assert_eq!(
        sub.acknowledge_command(wrong, registry.revision(), 0),
        Err(StreamError::WrongLedger)
    );
    let mut wrong = original;
    wrong.key.consumer = ConsumerId::from_u128(99);
    assert_eq!(
        sub.acknowledge_command(wrong, registry.revision(), 0),
        Err(StreamError::WrongConsumer)
    );
    let mut wrong = original;
    wrong.scope = ContentHash([99; 32]);
    assert_eq!(
        sub.acknowledge_command(wrong, registry.revision(), 0),
        Err(StreamError::WrongScope)
    );
    let mut ahead = original;
    ahead.position = Position::resolved(ledger(), SessionSeq(10));
    assert_eq!(
        sub.acknowledge_command(ahead, registry.revision(), 0),
        Err(StreamError::BeyondDelivered)
    );
    let mut source = History::new(vec![]);
    source.bounds.ledger.tenant = TenantId::from_u128(12);
    assert_eq!(
        sub.pump(&source, ReplayLimit::default(), 0),
        Err(StreamError::WrongLedger)
    );
    commit(
        &mut registry,
        0,
        CursorOperation::RequireResync {
            consumer: consumer(),
            generation: 1,
            reason: ResyncReason::SnapshotExpired,
        },
        0,
    );
    let recovered = restored(&registry, 0, crate::budget());
    assert!(matches!(
        Subscription::resume(
            recovered.get(consumer()).unwrap(),
            0,
            transport(2),
            crate::budget()
        ),
        Err(StreamError::ResyncRequired(ResyncReason::SnapshotExpired))
    ));
}

#[test]
fn malformed_or_failed_replay_rolls_back_queue_position_and_reservations() {
    let budget = budget();
    let registry = registered(budget.clone(), DeltaFilter::All, 100);
    let mut sub = Subscription::resume(
        registry.get(consumer()).unwrap(),
        0,
        transport(4),
        budget.clone(),
    )
    .unwrap();
    let source = History::new(vec![vec![delta(1, 0, 1), delta(1, 2, 1)]]);
    let before = budget.stats();
    assert!(matches!(
        sub.pump(&source, ReplayLimit::default(), 0),
        Err(StreamError::SourceViolation("delta ordinal gap"))
    ));
    assert_eq!(sub.queued_items(), 0);
    assert_eq!(sub.replayed_position(), Position::origin(ledger()));
    assert_eq!(budget.stats(), before);
    struct Failing(Delta);
    impl DeltaSource for Failing {
        fn bounds(&self) -> ReplayBounds {
            ReplayBounds {
                ledger: ledger(),
                floor: SessionSeq(0),
                published: SessionSeq(1),
            }
        }
        fn replay(
            &self,
            _: Position,
            _: ReplayLimit,
            visit: &mut dyn FnMut(&Delta) -> Result<(), StreamError>,
        ) -> Result<Position, StreamError> {
            visit(&self.0)?;
            Err(StreamError::SourceUnavailable)
        }
    }
    assert_eq!(
        sub.pump(&Failing(delta(1, 0, 1)), ReplayLimit::default(), 0),
        Err(StreamError::SourceUnavailable)
    );
    assert_eq!(budget.stats(), before);
    assert_eq!(sub.queued_items(), 0);
}

#[test]
fn periodic_authoritative_replay_recovers_a_lost_final_hint() {
    let budget = budget();
    let registry = registered(budget.clone(), DeltaFilter::All, 100);
    let mut sub =
        Subscription::resume(registry.get(consumer()).unwrap(), 0, transport(2), budget).unwrap();
    let mut source = History::new(vec![]);
    assert_eq!(
        sub.pump(&source, ReplayLimit::default(), 0).unwrap().queued,
        0
    );
    source.transactions.insert(1, vec![delta(1, 0, 1)]);
    source.bounds.published = SessionSeq(1);
    // No transport hint is sent. The runtime's reconciliation poll finds it.
    assert_eq!(
        sub.pump(&source, ReplayLimit::default(), 1).unwrap().queued,
        1
    );
    sub.grant(1, 1000).unwrap();
    assert!(matches!(
        sub.next(1).unwrap().unwrap().event(),
        StreamEvent::Delta { .. }
    ));
}

#[test]
fn prepared_registry_update_rolls_back_and_publishes_without_fresh_allocation() {
    let budget = budget();
    let mut registry = registered(budget.clone(), DeltaFilter::All, 100);
    let before = budget.stats();
    let renew = command(
        &registry,
        1,
        CursorOperation::Renew {
            consumer: consumer(),
            generation: 1,
            expires_at: 200,
        },
    );
    let abandoned = registry.prepare(&renew, SessionSeq(3)).unwrap();
    assert_eq!(registry.get(consumer()).unwrap().expires_at, 100);
    drop(abandoned);
    assert_eq!(budget.stats(), before);
    let one = registry.prepare(&renew, SessionSeq(3)).unwrap();
    let stale = registry.prepare(&renew, SessionSeq(3)).unwrap();
    let available = budget.stats().limit - budget.stats().used;
    let occupied = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, available)
        .unwrap();
    registry.publish(one).unwrap();
    assert_eq!(registry.get(consumer()).unwrap().expires_at, 200);
    assert_eq!(registry.publish(stale), Err(StreamError::StalePreparation));
    drop(occupied);
}

#[test]
fn checkpoint_validation_rejects_cross_tenant_rows_and_invalid_seed_positions() {
    let registry = registered(budget(), DeltaFilter::All, 100);
    let mut checkpoint = registry.checkpoint().clone();
    checkpoint
        .consumers
        .get_mut(&consumer())
        .unwrap()
        .token
        .position
        .ledger
        .tenant = TenantId::from_u128(99);
    assert!(matches!(
        CursorRegistry::restore(
            checkpoint,
            SessionSeq(3),
            RegistryConfig::default(),
            budget()
        ),
        Err(StreamError::WrongLedger)
    ));
    let mut checkpoint = registry.checkpoint().clone();
    checkpoint.consumers.get_mut(&consumer()).unwrap().mode = CursorMode::Seeding {
        snapshot: SessionSeq(2),
    };
    assert!(matches!(
        CursorRegistry::restore(
            checkpoint,
            SessionSeq(3),
            RegistryConfig::default(),
            budget()
        ),
        Err(StreamError::Invalid(_))
    ));
}

#[test]
fn exact_cursor_commands_replay_to_the_same_checkpoint() {
    let mut first = CursorRegistry::new(ledger(), RegistryConfig::default(), budget()).unwrap();
    let mut log = Vec::new();
    let register = command(
        &first,
        1,
        CursorOperation::Register {
            consumer: consumer(),
            scope: scope(),
            filter: DeltaFilter::All,
            start: Position::origin(ledger()),
            expires_at: 100,
        },
    );
    log.push(postcard::to_allocvec(&register).unwrap());
    first.replay_committed(&register, SessionSeq(2)).unwrap();
    let mut token = first.get(consumer()).unwrap().token;
    token.position = Position::after_delta(delta(2, 3, 1).id);
    let ack = command(&first, 2, CursorOperation::Acknowledge { token });
    log.push(postcard::to_allocvec(&ack).unwrap());
    first.replay_committed(&ack, SessionSeq(2)).unwrap();
    let mut recovered = CursorRegistry::new(ledger(), RegistryConfig::default(), budget()).unwrap();
    for encoded in log {
        recovered
            .replay_committed(&postcard::from_bytes(&encoded).unwrap(), SessionSeq(2))
            .unwrap();
    }
    assert_eq!(first.checkpoint(), recovered.checkpoint());
    assert_eq!(
        recovered.get(consumer()).unwrap().token.position.offset,
        PositionOffset::Delta(3)
    );
}

#[test]
fn replay_capacity_failure_at_any_staging_point_preserves_queue_and_releases_bytes() {
    let mut failures = 0;
    let mut successes = 0;
    for headroom in (0..12_000).step_by(127) {
        let allowance = budget();
        let registry = registered(allowance.clone(), DeltaFilter::All, 100);
        let mut sub = Subscription::resume(
            registry.get(consumer()).unwrap(),
            0,
            transport(4),
            allowance.clone(),
        )
        .unwrap();
        let mut source = History::new(vec![vec![delta(1, 0, 1), delta(1, 1, 1), delta(1, 2, 1)]]);
        for delta in source.transactions.get_mut(&1).unwrap() {
            delta.fact = DeltaFact::Progress("x".repeat(1000));
        }
        let available = allowance.stats().limit
            - allowance.stats().completion_reserve
            - allowance.stats().ordinary_used;
        let pressure = allowance
            .reserve(
                BudgetKind::Payload,
                BudgetLane::Ordinary,
                available - headroom,
            )
            .unwrap();
        let before = allowance.stats();
        match sub.pump(&source, ReplayLimit::default(), 0) {
            Ok(report) => {
                successes += 1;
                assert_eq!(report.queued, 3);
            }
            Err(StreamError::Memory(_)) => {
                failures += 1;
                assert_eq!(sub.queued_items(), 0);
                assert_eq!(sub.replayed_position(), Position::origin(ledger()));
                assert_eq!(allowance.stats(), before);
            }
            Err(error) => panic!("unexpected error {error}"),
        }
        drop(pressure);
        drop(sub);
        drop(registry);
        assert_eq!(allowance.stats().used, 0);
    }
    assert!(failures > 10);
    assert!(successes > 10);
}

#[test]
fn malicious_source_cannot_exceed_work_or_claim_unvisited_partial_coverage() {
    struct InvalidSource {
        end: Position,
        deltas: Vec<Delta>,
    }
    impl DeltaSource for InvalidSource {
        fn bounds(&self) -> ReplayBounds {
            ReplayBounds {
                ledger: ledger(),
                floor: SessionSeq(0),
                published: SessionSeq(10),
            }
        }
        fn replay(
            &self,
            _: Position,
            _: ReplayLimit,
            visit: &mut dyn FnMut(&Delta) -> Result<(), StreamError>,
        ) -> Result<Position, StreamError> {
            for delta in &self.deltas {
                visit(delta)?;
            }
            Ok(self.end)
        }
    }
    let allowance = budget();
    let registry = registered(allowance.clone(), DeltaFilter::All, 100);
    let mut sub = Subscription::resume(
        registry.get(consumer()).unwrap(),
        0,
        transport(4),
        allowance.clone(),
    )
    .unwrap();
    let before = allowance.stats();
    let limit = ReplayLimit {
        max_items: 1,
        max_bytes: 1000,
        max_sequences: 1,
    };
    for source in [
        InvalidSource {
            end: Position::resolved(ledger(), SessionSeq(1)),
            deltas: vec![delta(1, 0, 1), delta(1, 1, 1)],
        },
        InvalidSource {
            end: Position::after_delta(delta(1, 9, 1).id),
            deltas: vec![delta(1, 0, 1)],
        },
        InvalidSource {
            end: Position::resolved(ledger(), SessionSeq(10)),
            deltas: vec![],
        },
        InvalidSource {
            end: Position::resolved(ledger(), SessionSeq(1)),
            deltas: vec![Delta {
                id: DeltaId {
                    ledger: LedgerId {
                        tenant: TenantId::from_u128(99),
                        session: ledger().session,
                    },
                    ..delta(1, 0, 1).id
                },
                ..delta(1, 0, 1)
            }],
        },
    ] {
        assert!(sub.pump(&source, limit, 0).is_err());
        assert_eq!(sub.queued_items(), 0);
        assert_eq!(sub.replayed_position(), Position::origin(ledger()));
        assert_eq!(allowance.stats(), before);
    }
}

#[test]
fn item_and_byte_credits_are_independent_and_failed_grants_are_atomic() {
    let allowance = budget();
    let registry = registered(allowance.clone(), DeltaFilter::All, 100);
    let mut sub = Subscription::resume(
        registry.get(consumer()).unwrap(),
        0,
        transport(2),
        allowance,
    )
    .unwrap();
    let source = History::new(vec![vec![delta(1, 0, 1), delta(1, 1, 1)]]);
    sub.pump(&source, ReplayLimit::default(), 0).unwrap();
    sub.grant(2, 0).unwrap();
    assert!(sub.next(0).unwrap().is_none());
    assert_eq!(sub.grant(1, 1000), Err(StreamError::Capacity));
    assert!(sub.next(0).unwrap().is_none()); // Failed grant added no bytes.
    sub.grant(0, 1000).unwrap();
    assert!(matches!(
        sub.next(0).unwrap().unwrap().event(),
        StreamEvent::Delta { .. }
    ));
    assert!(matches!(
        sub.next(0).unwrap().unwrap().event(),
        StreamEvent::Delta { .. }
    ));
    assert!(matches!(
        sub.next(0).unwrap().unwrap().event(),
        StreamEvent::Resolved { .. }
    ));
}

#[test]
fn completion_lane_can_persist_cursor_progress_when_ordinary_admission_is_full() {
    let allowance = budget();
    let mut registry = registered(allowance.clone(), DeltaFilter::All, 100);
    let mut token = registry.get(consumer()).unwrap().token;
    token.position = Position::resolved(ledger(), SessionSeq(1));
    let available = allowance.stats().limit
        - allowance.stats().completion_reserve
        - allowance.stats().ordinary_used;
    let pressure = allowance
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, available)
        .unwrap();
    commit(&mut registry, 1, CursorOperation::Acknowledge { token }, 1);
    assert_eq!(registry.retention_limit(SessionSeq(1)), SessionSeq(1));
    drop(pressure);
}

#[test]
fn protected_obligations_survive_expiry_and_cannot_be_reseeded_or_resynced() {
    let memory = budget();
    let mut registry =
        CursorRegistry::new(ledger(), RegistryConfig::default(), memory.clone()).unwrap();
    commit(
        &mut registry,
        0,
        CursorOperation::RegisterProtected {
            consumer: consumer(),
            scope: scope(),
            filter: DeltaFilter::All,
            start: Position::origin(ledger()),
        },
        2,
    );
    commit(
        &mut registry,
        u64::MAX,
        CursorOperation::AdvanceFloor {
            through: SessionSeq(0),
        },
        2,
    );
    assert_eq!(registry.retention_limit(SessionSeq(2)), SessionSeq(0));
    assert!(matches!(
        registry.prepare(
            &command(
                &registry,
                u64::MAX,
                CursorOperation::AdvanceFloor {
                    through: SessionSeq(1)
                }
            ),
            SessionSeq(2)
        ),
        Err(StreamError::RetentionPinned { .. })
    ));
    for operation in [
        CursorOperation::BeginSeed {
            consumer: consumer(),
            scope: scope(),
            filter: DeltaFilter::All,
            snapshot: SessionSeq(2),
            expires_at: u64::MAX,
        },
        CursorOperation::RequireResync {
            consumer: consumer(),
            generation: 1,
            reason: ResyncReason::ExplicitReset,
        },
        CursorOperation::Renew {
            consumer: consumer(),
            generation: 1,
            expires_at: u64::MAX,
        },
    ] {
        assert!(
            registry
                .prepare(&command(&registry, u64::MAX, operation), SessionSeq(2))
                .is_err()
        );
    }
    let mut registry = restored(&registry, 2, memory.clone());
    assert_eq!(
        registry.get(consumer()).unwrap().mode,
        CursorMode::Protected
    );
    let history = History::new(vec![vec![delta(1, 0, 1)], vec![delta(2, 0, 1)]]);
    let config = TransportConfig {
        max_lag_sequences: 1,
        ..transport(4)
    };
    let mut subscription =
        Subscription::resume(registry.get(consumer()).unwrap(), u64::MAX, config, memory).unwrap();
    subscription.grant(4, 100_000).unwrap();
    subscription
        .pump(&history, ReplayLimit::default(), u64::MAX)
        .unwrap();
    let mut delivered = None;
    while let Some(event) = subscription.next(u64::MAX).unwrap() {
        match event.event() {
            StreamEvent::Delta { cursor, .. } | StreamEvent::Resolved { cursor } => {
                delivered = Some(*cursor)
            }
            StreamEvent::Resync { .. } => panic!("protected obligation silently reset"),
        }
    }
    let command = subscription
        .acknowledge_command(delivered.unwrap(), registry.revision(), u64::MAX)
        .unwrap();
    registry
        .publish(registry.prepare(&command, SessionSeq(2)).unwrap())
        .unwrap();
    subscription
        .acknowledged_committed(registry.get(consumer()).unwrap())
        .unwrap();
    assert_eq!(registry.retention_limit(SessionSeq(2)), SessionSeq(2));
}

#[test]
fn acknowledge_and_renew_is_one_atomic_transition_and_invalid_ack_cannot_extend_lease() {
    let mut registry = registered(budget(), DeltaFilter::All, 100);
    let token = CursorToken {
        position: Position::resolved(ledger(), SessionSeq(2)),
        ..registry.get(consumer()).unwrap().token
    };
    commit(
        &mut registry,
        5,
        CursorOperation::AcknowledgeAndRenew {
            token,
            expires_at: 200,
        },
        3,
    );
    let committed = registry.checkpoint().clone();
    assert_eq!(committed.revision, 2);
    assert_eq!(registry.get(consumer()).unwrap().expires_at, 200);
    for position in [
        Position::origin(ledger()),
        Position::resolved(ledger(), SessionSeq(4)),
    ] {
        let operation = CursorOperation::AcknowledgeAndRenew {
            token: CursorToken { position, ..token },
            expires_at: 300,
        };
        assert!(
            registry
                .prepare(&command(&registry, 10, operation), SessionSeq(3))
                .is_err()
        );
        assert_eq!(registry.checkpoint(), &committed);
    }
    let invalid_lease = CursorOperation::AcknowledgeAndRenew {
        token: CursorToken {
            position: Position::resolved(ledger(), SessionSeq(3)),
            ..token
        },
        expires_at: 10,
    };
    assert!(
        registry
            .prepare(&command(&registry, 10, invalid_lease), SessionSeq(3))
            .is_err()
    );
    assert_eq!(registry.checkpoint(), &committed);
}

#[test]
fn held_control_delivery_bounds_the_reserved_slot_and_preserves_expired_resync() {
    let budget = budget();
    let registry = registered(budget.clone(), DeltaFilter::All, 5);
    let source = History::new(vec![vec![]]);
    let mut sub = Subscription::resume(
        registry.get(consumer()).unwrap(),
        0,
        transport(2),
        budget.clone(),
    )
    .unwrap();
    sub.pump(&source, ReplayLimit::default(), 0).unwrap();
    let held = sub.next(0).unwrap().unwrap();
    assert!(matches!(held.event(), StreamEvent::Resolved { .. }));
    let charged = budget.stats().used;
    for _ in 0..100 {
        assert!(sub.next(5).unwrap().is_none());
        assert_eq!(budget.stats().used, charged);
    }
    // Delivery can cross a worker boundary while the owner keeps running.
    std::thread::spawn(move || drop(held)).join().unwrap();
    let resync = sub.next(5).unwrap().unwrap();
    assert!(matches!(
        resync.event(),
        StreamEvent::Resync {
            reason: ResyncReason::LeaseExpired,
            ..
        }
    ));
    assert_eq!(sub.acknowledged().position, Position::origin(ledger()));
    drop(sub);
    drop(registry);
    assert!(budget.stats().used > 0);
    drop(resync);
    assert_eq!(budget.stats().used, 0);
}
