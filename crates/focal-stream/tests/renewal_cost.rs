//! A renewal, an acknowledgment or any other one-row command copies nothing
//! of the registry (the audit's F61): what a preparation charges does not
//! grow with the consumers registered, a registry restored under exactly the
//! bytes it holds still renews, and the bytes rows hold follow them out.
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
use std::collections::BTreeSet;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn consumer(n: u128) -> ConsumerId {
    ConsumerId::from_u128(n)
}
/// The audit's row: a filter of the maximum 256 claims.
fn wide_filter(seed: u128) -> DeltaFilter {
    DeltaFilter::Claims(
        (0..256u128)
            .map(|n| ClaimId::from_u128(seed * 1000 + n))
            .collect::<BTreeSet<_>>(),
    )
}
fn register(consumer: ConsumerId, filter: DeltaFilter, expires_at: u64) -> CursorOperation {
    CursorOperation::Register {
        consumer,
        scope: ContentHash([4; 32]),
        filter,
        start: Position::origin(ledger()),
        expires_at,
    }
}
fn command(registry: &CursorRegistry, now: u64, operation: CursorOperation) -> CursorCommand {
    CursorCommand {
        expected_revision: registry.revision(),
        now,
        operation,
    }
}
fn apply(registry: &mut CursorRegistry, now: u64, operation: CursorOperation) {
    let prepared = registry
        .prepare(&command(registry, now, operation), SessionSeq(0))
        .unwrap();
    registry.publish(prepared).unwrap();
}
fn renew(registry: &CursorRegistry, consumer: ConsumerId, expires_at: u64) -> CursorOperation {
    CursorOperation::Renew {
        consumer,
        generation: registry.get(consumer).unwrap().token.generation,
        expires_at,
    }
}
/// A registry of `rows` consumers with the audit's filters, in `budget`.
fn populated(rows: u128, budget: MemoryBudget) -> CursorRegistry {
    let mut registry = CursorRegistry::new(ledger(), RegistryConfig::default(), budget).unwrap();
    for n in 1..=rows {
        apply(
            &mut registry,
            10,
            register(consumer(n), wide_filter(n), 1_000_000),
        );
    }
    registry
}

#[test]
fn a_one_row_command_charges_nothing_of_the_registry() {
    let budget = MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let mut registry = populated(64, budget.clone());
    let retained = budget.stats().used;
    assert_eq!(retained, registry.resident_bytes());
    // The audit measured 348,768 bytes requested to prepare one renewal of
    // this shape; a renewal patches one row's expiry and prepares no copy.
    let renewal = registry
        .prepare(
            &command(&registry, 20, renew(&registry, consumer(7), 2_000_000)),
            SessionSeq(0),
        )
        .unwrap();
    assert_eq!(
        budget.stats().used,
        retained,
        "a renewal prepared copies nothing"
    );
    // The row it will leave behind is visible before publication, and the
    // registry is untouched until then.
    assert_eq!(
        registry
            .projected(&renewal, consumer(7))
            .unwrap()
            .expires_at,
        2_000_000
    );
    assert_eq!(registry.get(consumer(7)).unwrap().expires_at, 1_000_000);
    registry.publish(renewal).unwrap();
    assert_eq!(registry.get(consumer(7)).unwrap().expires_at, 2_000_000);
    assert_eq!(
        budget.stats().used,
        retained,
        "a renewal published adds nothing"
    );
    // An acknowledgment moves the row's position: a patch too.
    let mut token = registry.get(consumer(7)).unwrap().token;
    token.position = Position::resolved(ledger(), SessionSeq(0));
    let ack = registry
        .prepare(
            &command(&registry, 30, CursorOperation::Acknowledge { token }),
            SessionSeq(0),
        )
        .unwrap();
    assert_eq!(budget.stats().used, retained);
    registry.publish(ack).unwrap();
    assert_eq!(budget.stats().used, retained);
    // A registration charges its one row, the same in a registry of 64 as
    // in one of none.
    let alone = MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let mut empty =
        CursorRegistry::new(ledger(), RegistryConfig::default(), alone.clone()).unwrap();
    let before = alone.stats().used;
    apply(
        &mut empty,
        10,
        register(consumer(1), wide_filter(1), 1_000_000),
    );
    let one_row = alone.stats().used - before;
    apply(
        &mut registry,
        40,
        register(consumer(65), wide_filter(65), 1_000_000),
    );
    assert_eq!(budget.stats().used - retained, one_row);
    assert_eq!(budget.stats().used, registry.resident_bytes());
}

#[test]
fn a_registry_restored_under_exactly_its_bytes_still_renews() {
    let generous = MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let registry = populated(64, generous.clone());
    let checkpoint = registry.checkpoint().clone();
    let retained = generous.stats().used;
    drop(registry);
    // Room for the registry and nothing else: before the fix a renewal
    // asked for a second copy of it and was refused.
    let exact = MemoryBudget::new(retained, 0).unwrap();
    let mut restored = CursorRegistry::restore(
        checkpoint,
        SessionSeq(0),
        RegistryConfig::default(),
        exact.clone(),
    )
    .unwrap();
    assert_eq!(exact.stats().used, retained);
    let renewal = restored
        .prepare(
            &command(&restored, 20, renew(&restored, consumer(3), 2_000_000)),
            SessionSeq(0),
        )
        .unwrap();
    restored.publish(renewal).unwrap();
    assert_eq!(restored.get(consumer(3)).unwrap().expires_at, 2_000_000);
    assert_eq!(exact.stats().used, retained);
}

#[test]
fn rows_that_leave_return_their_bytes() {
    let budget = MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let config = RegistryConfig {
        max_consumers: 4,
        ..RegistryConfig::default()
    };
    let mut registry = CursorRegistry::new(ledger(), config, budget.clone()).unwrap();
    let empty = budget.stats().used;
    for n in 1..=4u128 {
        apply(
            &mut registry,
            10,
            register(consumer(n), wide_filter(n), 100),
        );
    }
    let full = budget.stats().used;
    assert!(full > empty);
    assert_eq!(full, registry.resident_bytes());
    // Every lease expired: a registration at the bound retires the four
    // released rows, and the registry holds its one new row.
    let arrival = registry
        .prepare(
            &command(
                &registry,
                200,
                register(consumer(5), DeltaFilter::All, 1_000),
            ),
            SessionSeq(0),
        )
        .unwrap();
    assert_eq!(arrival.retired().len(), 4);
    registry.publish(arrival).unwrap();
    assert_eq!(registry.checkpoint().consumers.len(), 1);
    let one_plain_row = budget.stats().used - empty;
    assert!(
        one_plain_row < (full - empty) / 4,
        "a plain row is smaller than a wide one"
    );
    assert_eq!(budget.stats().used, registry.resident_bytes());
    // A seed replacing a consumer's filter leaves the old row's bytes.
    apply(
        &mut registry,
        300,
        CursorOperation::BeginSeed {
            consumer: consumer(5),
            scope: ContentHash([4; 32]),
            filter: wide_filter(5),
            snapshot: SessionSeq(0),
            expires_at: 2_000,
        },
    );
    let wide = budget.stats().used;
    assert!(wide > empty + one_plain_row);
    apply(
        &mut registry,
        400,
        CursorOperation::BeginSeed {
            consumer: consumer(5),
            scope: ContentHash([4; 32]),
            filter: DeltaFilter::All,
            snapshot: SessionSeq(0),
            expires_at: 3_000,
        },
    );
    assert_eq!(budget.stats().used, empty + one_plain_row);
    assert_eq!(budget.stats().used, registry.resident_bytes());
}
