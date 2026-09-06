#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_directory::*;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, SessionId, TenantId};

fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap()
}
fn tenant(id: u128) -> TenantId {
    TenantId::from_u128(id)
}
fn quota(weight: u32) -> TenantQuota {
    TenantQuota {
        weight,
        max_items: 1000,
        reserved_items: 10,
        max_bytes: 16 * 1024 * 1024,
        reserved_bytes: 1024 * 1024,
        session_items: 1000,
        session_reserved_items: 10,
        session_bytes: 16 * 1024 * 1024,
        session_reserved_bytes: 1024 * 1024,
        max_sessions: 8,
    }
}
fn job(t: u128, session: u128, id: u128, class: WorkClass, cost: u64) -> WorkMetadata {
    WorkMetadata {
        key: WorkKey {
            ledger: LedgerId {
                tenant: tenant(t),
                session: SessionId::from_u128(session),
            },
            id: WorkId::from_u128(id),
        },
        class,
        cost,
    }
}
fn config() -> SchedulerConfig {
    SchedulerConfig {
        quantum: 10,
        max_cost: 100,
        max_weight: 8,
        max_visits: 2,
        priority_burst: 3,
        max_tenants: 8,
        max_items: 1000,
        reserved_items: 10,
    }
}
fn next<T>(scheduler: &mut FairScheduler<T>) -> Dispatch<T> {
    for _ in 0..100 {
        match scheduler.schedule().unwrap() {
            ScheduleOutcome::Work(work) => return work,
            ScheduleOutcome::Continue => {}
            ScheduleOutcome::Idle => panic!("expected queued work"),
        }
    }
    panic!("bounded costs must eventually receive service")
}

#[test]
fn one_visit_slice_cannot_hide_ordinary_work_behind_blocked_priority_tenants() {
    let mut configuration = config();
    configuration.max_visits = 1;
    let mut scheduler = FairScheduler::new(configuration, budget()).unwrap();
    for id in 1..=3 {
        scheduler.register_tenant(tenant(id), quota(1)).unwrap();
    }
    for id in 1..=2 {
        scheduler
            .enqueue(job(id, 1, id, WorkClass::Apply, 10), (), 0)
            .unwrap();
    }
    scheduler
        .enqueue(job(3, 1, 3, WorkClass::Query, 10), (), 0)
        .unwrap();
    let ScheduleOutcome::Work(work) = scheduler
        .schedule_when(|ledger| ledger.tenant == tenant(3))
        .unwrap()
    else {
        panic!("blocked priority tenants starved eligible ordinary work");
    };
    assert_eq!(work.metadata.key.ledger.tenant, tenant(3));
    assert_eq!(scheduler.active_items(), 3);
}

#[test]
fn blocked_session_keeps_quotas_without_blocking_same_tenant_or_other_lane() {
    let mut scheduler = FairScheduler::new(config(), budget()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    scheduler.register_tenant(tenant(2), quota(1)).unwrap();
    let blocked = job(1, 1, 1, WorkClass::Apply, 10).key.ledger;
    for item in [
        job(1, 1, 1, WorkClass::Apply, 10),
        job(1, 1, 2, WorkClass::Append, 10),
        job(1, 2, 3, WorkClass::Append, 10),
        job(2, 1, 4, WorkClass::Completion, 10),
    ] {
        scheduler.enqueue(item, item.key.id, 512).unwrap();
    }
    let mut served = Vec::new();
    for _ in 0..10 {
        match scheduler.schedule_when(|ledger| ledger != blocked).unwrap() {
            ScheduleOutcome::Work(work) => served.push(work.metadata.key.id),
            ScheduleOutcome::Continue => {}
            ScheduleOutcome::Idle => break,
        }
    }
    served.sort();
    assert_eq!(served, [WorkId::from_u128(3), WorkId::from_u128(4)]);
    assert_eq!(scheduler.active_items(), 2);
    let used = scheduler.usage(tenant(1)).unwrap();
    for _ in 0..20 {
        assert!(matches!(
            scheduler.schedule_when(|_| false).unwrap(),
            ScheduleOutcome::Idle
        ));
    }
    assert_eq!(scheduler.usage(tenant(1)).unwrap(), used);
    assert_eq!(next(&mut scheduler).metadata.key.id, WorkId::from_u128(1));
    assert_eq!(next(&mut scheduler).metadata.key.id, WorkId::from_u128(2));
    scheduler.reap_finished();
    assert_eq!(scheduler.active_items(), 0);
}

#[test]
fn weighted_deficit_charges_work_cost_and_rotates_classes_without_starvation() {
    let mut scheduler = FairScheduler::new(config(), budget()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    scheduler.register_tenant(tenant(2), quota(3)).unwrap();
    for id in 0..60 {
        scheduler
            .enqueue(job(1, 1, id, WorkClass::Append, 10), (), 0)
            .unwrap();
        scheduler
            .enqueue(job(2, 1, id, WorkClass::Query, 10), (), 0)
            .unwrap();
    }
    let served: Vec<_> = (0..40)
        .map(|_| next(&mut scheduler).metadata.key.ledger.tenant)
        .collect();
    assert_eq!(served.iter().filter(|id| **id == tenant(1)).count(), 10);
    assert_eq!(served.iter().filter(|id| **id == tenant(2)).count(), 30);
    assert!(
        served
            .chunks_exact(4)
            .all(|chunk| chunk[0] == tenant(1) && chunk[1..].iter().all(|id| *id == tenant(2)))
    );

    let mut scheduler = FairScheduler::new(config(), budget()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    for id in 0..3 {
        for class in [WorkClass::Append, WorkClass::Transfer, WorkClass::Query] {
            scheduler
                .enqueue(job(1, 1, id * 3 + class as u128, class, 10), (), 0)
                .unwrap();
        }
    }
    let served: Vec<_> = (0..9)
        .map(|_| next(&mut scheduler).metadata.class)
        .collect();
    assert_eq!(
        served,
        [WorkClass::Append, WorkClass::Transfer, WorkClass::Query].repeat(3)
    );
}

#[test]
fn bounded_visit_slice_retains_deficit_and_makes_progress_without_new_enqueue() {
    let mut scheduler = FairScheduler::new(config(), budget()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    scheduler
        .enqueue(job(1, 1, 1, WorkClass::Query, 100), 42, 0)
        .unwrap();
    for _ in 0..4 {
        assert!(matches!(
            scheduler.schedule().unwrap(),
            ScheduleOutcome::Continue
        ));
    }
    let dispatch = next(&mut scheduler);
    assert_eq!(*dispatch.payload(), 42);
    assert_eq!(scheduler.active_items(), 1);
    assert!(matches!(
        scheduler.schedule().unwrap(),
        ScheduleOutcome::Idle
    ));
    drop(dispatch);
    scheduler.reap_finished();
    assert_eq!(scheduler.active_items(), 0);
}

#[test]
fn ordinary_pressure_preserves_completion_items_and_bounded_priority_preserves_queries() {
    let node = budget();
    let mut cfg = config();
    cfg.max_items = 6;
    cfg.reserved_items = 2;
    let mut scheduler = FairScheduler::new(cfg, node.clone()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    for id in 0..4 {
        scheduler
            .enqueue(job(1, 1, id, WorkClass::Append, 10), (), 0)
            .unwrap();
    }
    let used = node.stats();
    assert!(matches!(
        scheduler.enqueue(job(1, 1, 4, WorkClass::Append, 10), (), 0),
        Err(DirectoryError::Memory(_))
    ));
    assert_eq!(
        node.stats(),
        used,
        "failed item admission rolls back bytes and provisional metadata"
    );
    scheduler
        .enqueue(job(1, 1, 5, WorkClass::Apply, 10), (), 0)
        .unwrap();
    scheduler
        .enqueue(job(1, 1, 6, WorkClass::Completion, 10), (), 0)
        .unwrap();
    assert!(matches!(
        scheduler.enqueue(job(1, 1, 7, WorkClass::Control, 10), (), 0),
        Err(DirectoryError::Memory(_))
    ));
    assert_eq!(next(&mut scheduler).metadata.class, WorkClass::Apply);
    assert_eq!(next(&mut scheduler).metadata.class, WorkClass::Completion);

    let mut scheduler = FairScheduler::new(config(), budget()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    scheduler.register_tenant(tenant(2), quota(1)).unwrap();
    for id in 0..20 {
        scheduler
            .enqueue(job(1, 1, id, WorkClass::Control, 10), (), 0)
            .unwrap();
        scheduler
            .enqueue(job(2, 1, id, WorkClass::Query, 10), (), 0)
            .unwrap();
    }
    for _ in 0..5 {
        for _ in 0..3 {
            assert_eq!(next(&mut scheduler).metadata.class, WorkClass::Control);
        }
        assert_eq!(next(&mut scheduler).metadata.class, WorkClass::Query);
    }
}

#[test]
fn tenant_session_and_node_quota_rejections_are_atomic_and_permits_follow_dispatch() {
    let node = budget();
    let mut scheduler = FairScheduler::new(config(), node.clone()).unwrap();
    let mut limited = quota(1);
    limited.max_items = 3;
    limited.reserved_items = 1;
    limited.session_items = 2;
    limited.session_reserved_items = 1;
    scheduler.register_tenant(tenant(1), limited).unwrap();
    scheduler.register_tenant(tenant(2), quota(1)).unwrap();
    let baseline = node.stats();
    let one = job(1, 1, 1, WorkClass::Append, 10);
    scheduler.enqueue(one, vec![1u8; 100], 100).unwrap();
    let after_one = node.stats();
    assert!(
        scheduler
            .enqueue(job(1, 1, 2, WorkClass::Append, 10), vec![], 0)
            .is_err()
    );
    assert_eq!(node.stats(), after_one);
    scheduler
        .enqueue(job(1, 2, 2, WorkClass::Append, 10), vec![], 0)
        .unwrap();
    let after_two = node.stats();
    assert!(
        scheduler
            .enqueue(job(1, 3, 3, WorkClass::Append, 10), vec![], 0)
            .is_err()
    );
    assert_eq!(node.stats(), after_two);
    assert_eq!(scheduler.usage(tenant(1)).unwrap().sessions, 2);
    // A noisy tenant cannot consume another tenant's quota.
    scheduler
        .enqueue(job(2, 1, 1, WorkClass::Append, 10), vec![], 0)
        .unwrap();
    let dispatch = next(&mut scheduler);
    assert_eq!(dispatch.metadata, one);
    assert_eq!(scheduler.usage(tenant(1)).unwrap().items, 2);
    assert_eq!(scheduler.cancel(one.key), Err(DirectoryError::InFlight));
    assert_eq!(
        scheduler.remove_idle_tenant(tenant(1)),
        Err(DirectoryError::InFlight)
    );
    assert_eq!(
        scheduler.enqueue(one, vec![], 0),
        Err(DirectoryError::Duplicate)
    );
    drop(dispatch);
    scheduler.reap_finished();
    assert_eq!(scheduler.usage(tenant(1)).unwrap().items, 1);
    assert_eq!(scheduler.usage(tenant(1)).unwrap().sessions, 1);
    scheduler
        .cancel(job(1, 2, 2, WorkClass::Append, 10).key)
        .unwrap();
    scheduler
        .cancel(job(2, 1, 1, WorkClass::Append, 10).key)
        .unwrap();
    assert_eq!(scheduler.active_items(), 0);
    assert_eq!(
        node.stats(),
        baseline,
        "all payload, index, queue-buffer, and session metadata charges reclaimed"
    );
    scheduler.enqueue(one, vec![], 0).unwrap();
    drop(scheduler);
    assert_eq!(node.stats().used, 0);
}

#[test]
fn byte_pressure_retains_control_reserve_and_failed_enqueue_does_not_consume_slots() {
    let node = budget();
    let mut scheduler = FairScheduler::new(config(), node.clone()).unwrap();
    scheduler.register_tenant(tenant(1), quota(1)).unwrap();
    let available =
        node.stats().limit - node.stats().completion_reserve - node.stats().ordinary_used;
    let pressure = node
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, available)
        .unwrap()
        .commit();
    let baseline = node.stats();
    assert!(
        scheduler
            .enqueue(job(1, 1, 1, WorkClass::Append, 10), (), 0)
            .is_err()
    );
    assert_eq!(node.stats(), baseline);
    assert_eq!(scheduler.active_items(), 0);
    assert_eq!(scheduler.usage(tenant(1)).unwrap().sessions, 0);
    scheduler
        .enqueue(job(1, 1, 1, WorkClass::Completion, 10), (), 0)
        .unwrap();
    drop(next(&mut scheduler));
    scheduler.reap_finished();
    assert_eq!(node.stats(), baseline);
    drop(pressure);
    let mut limited = quota(1);
    limited.max_bytes = 1;
    limited.reserved_bytes = 0;
    scheduler.register_tenant(tenant(2), limited).unwrap();
    let baseline = node.stats();
    assert!(
        scheduler
            .enqueue(job(2, 1, 1, WorkClass::Control, 10), (), 0)
            .is_err()
    );
    assert_eq!(node.stats(), baseline);
    assert_eq!(scheduler.usage(tenant(2)).unwrap().items, 0);
}

#[test]
fn cancelled_jobs_and_dropped_scheduler_preserve_running_work_charge() {
    let node = budget();
    let mut scheduler = FairScheduler::new(config(), node.clone()).unwrap();
    let mut limited = quota(1);
    limited.max_sessions = 1;
    scheduler.register_tenant(tenant(1), limited).unwrap();
    let key = job(1, 1, 1, WorkClass::Query, 10);
    scheduler.enqueue(key, 5, 0).unwrap();
    assert_eq!(
        scheduler.enqueue(job(1, 2, 2, WorkClass::Query, 10), 6, 0),
        Err(DirectoryError::Capacity)
    );
    let dispatch = next(&mut scheduler);
    drop(scheduler);
    assert!(node.stats().used > 0);
    assert_eq!(*dispatch.payload(), 5);
    drop(dispatch);
    assert_eq!(node.stats().used, 0);
}
