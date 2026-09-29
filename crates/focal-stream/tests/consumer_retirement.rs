//! Expired and resynced consumers return their admission slots (the audit's
//! F62): a registry that a churn of distinct names passes through stays
//! bounded, a retired name registers again under a generation no earlier
//! token carries, and protected consumers leave only by acknowledgment. A
//! released row stays until a registration needs its slot, so a consumer
//! that comes back still reads why it must reseed.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_memory::MemoryBudget;
use focal_model::*;
use focal_stream::*;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap()
}
fn registry(max_consumers: usize) -> CursorRegistry {
    CursorRegistry::new(
        ledger(),
        RegistryConfig {
            max_consumers,
            ..RegistryConfig::default()
        },
        budget(),
    )
    .unwrap()
}
fn consumer(n: u128) -> ConsumerId {
    ConsumerId::from_u128(n)
}
fn register(consumer: ConsumerId, expires_at: u64) -> CursorOperation {
    CursorOperation::Register {
        consumer,
        scope: ContentHash([4; 32]),
        filter: DeltaFilter::All,
        start: Position::origin(ledger()),
        expires_at,
    }
}
fn protected(consumer: ConsumerId) -> CursorOperation {
    CursorOperation::RegisterProtected {
        consumer,
        scope: ContentHash([4; 32]),
        filter: DeltaFilter::All,
        start: Position::origin(ledger()),
    }
}
/// Prepare and publish one command; the consumers it retired.
fn apply(
    registry: &mut CursorRegistry,
    now: u64,
    operation: CursorOperation,
    published: u64,
) -> Result<Vec<ConsumerId>, StreamError> {
    let command = CursorCommand {
        expected_revision: registry.revision(),
        now,
        operation,
    };
    let prepared = registry.prepare(&command, SessionSeq(published))?;
    let retired = prepared.retired().to_vec();
    registry.publish(prepared)?;
    Ok(retired)
}

#[test]
fn an_expired_consumer_returns_its_slot_and_no_stale_token_reaches_the_name_s_next_incarnation() {
    let mut registry = registry(1);
    assert_eq!(
        apply(&mut registry, 0, register(consumer(1), 1), 0).unwrap(),
        Vec::new()
    );
    let old = registry.get(consumer(1)).unwrap().token;
    // At the bound while the lease lives: refused, nothing retired.
    assert!(matches!(
        apply(&mut registry, 0, register(consumer(2), 5), 0),
        Err(StreamError::Capacity)
    ));
    assert!(registry.get(consumer(1)).is_some());
    // The lease expired: a new consumer takes the slot and the expired row
    // is retired, named.
    assert_eq!(
        apply(&mut registry, 2, register(consumer(2), 5), 0).unwrap(),
        vec![consumer(1)]
    );
    assert!(registry.get(consumer(1)).is_none());
    assert_eq!(registry.checkpoint().consumers.len(), 1);
    // Stale requests of the retired consumer are refused, typed.
    assert!(matches!(
        apply(
            &mut registry,
            2,
            CursorOperation::Renew {
                consumer: consumer(1),
                generation: old.generation,
                expires_at: 9,
            },
            0
        ),
        Err(StreamError::MissingConsumer)
    ));
    assert!(matches!(
        apply(
            &mut registry,
            2,
            CursorOperation::Acknowledge { token: old },
            0
        ),
        Err(StreamError::MissingConsumer)
    ));
    // The name registers again once the slot frees (consumer 2 expires at
    // 5), under a generation no earlier token carries.
    assert_eq!(
        apply(&mut registry, 6, register(consumer(1), 9), 3).unwrap(),
        vec![consumer(2)]
    );
    let new = registry.get(consumer(1)).unwrap().token;
    assert!(new.generation > old.generation, "{new:?} after {old:?}");
    // The old token names the retired incarnation: refused as the wrong
    // generation, and the new cursor does not move.
    let mut stale = old;
    stale.position = Position::resolved(ledger(), SessionSeq(3));
    assert!(matches!(
        apply(
            &mut registry,
            6,
            CursorOperation::Acknowledge { token: stale },
            3
        ),
        Err(StreamError::WrongGeneration)
    ));
    assert_eq!(
        registry.get(consumer(1)).unwrap().token.position,
        Position::origin(ledger())
    );
    // The new token works.
    let mut ahead = new;
    ahead.position = Position::resolved(ledger(), SessionSeq(3));
    assert_eq!(
        apply(
            &mut registry,
            6,
            CursorOperation::Acknowledge { token: ahead },
            3
        )
        .unwrap(),
        Vec::new()
    );
}

#[test]
fn at_the_bound_the_released_rows_leave_together_and_a_protected_one_never() {
    let mut registry = registry(3);
    apply(&mut registry, 0, register(consumer(1), 1), 0).unwrap();
    apply(&mut registry, 0, protected(consumer(2)), 0).unwrap();
    apply(&mut registry, 0, register(consumer(3), 100), 0).unwrap();
    // A consumer sent to resync released its retention too.
    let generation = registry.get(consumer(3)).unwrap().token.generation;
    apply(
        &mut registry,
        0,
        CursorOperation::RequireResync {
            consumer: consumer(3),
            generation,
            reason: ResyncReason::LeaseExpired,
        },
        0,
    )
    .unwrap();
    // The floor moves without touching a row: a consumer that comes back
    // still reads why it must reseed.
    apply(
        &mut registry,
        2,
        CursorOperation::AdvanceFloor {
            through: SessionSeq(0),
        },
        3,
    )
    .unwrap();
    assert_eq!(registry.checkpoint().consumers.len(), 3);
    // A registration at the bound: the expired and the resynced rows leave
    // together, the protected stays.
    let retired = apply(&mut registry, 2, register(consumer(4), 100), 3).unwrap();
    assert_eq!(retired, vec![consumer(1), consumer(3)]);
    assert_eq!(
        registry
            .checkpoint()
            .consumers
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![consumer(2), consumer(4)]
    );
    // Only protected rows at the bound: nothing is retired, the registration
    // is refused.
    let mut registry = registry_of(1);
    apply(&mut registry, 0, protected(consumer(2)), 0).unwrap();
    assert!(matches!(
        apply(
            &mut registry,
            u64::MAX / 2,
            register(consumer(1), u64::MAX / 2 + 1),
            0
        ),
        Err(StreamError::Capacity)
    ));
    assert!(registry.get(consumer(2)).is_some());
}
fn registry_of(max_consumers: usize) -> CursorRegistry {
    registry(max_consumers)
}

#[test]
fn a_churn_of_more_names_than_the_bound_passes_through_a_bounded_registry_that_restores() {
    let config = RegistryConfig::default();
    let mut registry = registry(config.max_consumers);
    let churn = config.max_consumers + 512;
    for n in 0..churn {
        let now = n as u64;
        // Each lease lasts one tick: the previous consumers are released
        // by the time the bound is reached.
        let retired = apply(
            &mut registry,
            now,
            register(consumer(n as u128 + 1), now + 1),
            0,
        )
        .unwrap_or_else(|error| panic!("consumer {n}: {error:?}"));
        assert!(registry.checkpoint().consumers.len() <= config.max_consumers);
        if n == config.max_consumers {
            assert_eq!(
                retired.len(),
                config.max_consumers,
                "every earlier lease ended by now: the released rows leave at once"
            );
        }
    }
    // The registry holds the last, live consumers only, and its checkpoint
    // restores under the same bound.
    let checkpoint = registry.checkpoint().clone();
    assert!(checkpoint.consumers.len() <= config.max_consumers);
    assert!(checkpoint.consumers.contains_key(&consumer(churn as u128)));
    let bytes = postcard::to_allocvec(&checkpoint).unwrap();
    let restored = CursorRegistry::restore(
        postcard::from_bytes(&bytes).unwrap(),
        SessionSeq(0),
        config,
        budget(),
    )
    .unwrap();
    assert_eq!(restored.checkpoint(), &checkpoint);
    // Every generation is the revision that issued it: unique for ever.
    let mut generations: Vec<u64> = checkpoint
        .consumers
        .values()
        .map(|row| row.token.generation)
        .collect();
    generations.sort_unstable();
    generations.dedup();
    assert_eq!(generations.len(), checkpoint.consumers.len());
    assert!(
        generations
            .iter()
            .all(|generation| *generation <= checkpoint.revision)
    );
}
