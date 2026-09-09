use crate::{BudgetLane, DiskBudget, DiskBudgetConfig, DiskKind, MemoryError};

const MIB: u64 = 1024 * 1024;

fn budget(headroom: u64, reserve: u64) -> DiskBudget {
    DiskBudget::new(DiskBudgetConfig {
        headroom,
        completion_reserve: reserve,
        sample_interval: 4,
    })
    .unwrap()
}

#[test]
fn unknown_space_refuses_fresh_work_under_a_watermark_and_admits_without_one() {
    let guarded = budget(64 * MIB, 16 * MIB);
    assert!(guarded.sample_due());
    assert_eq!(guarded.available(BudgetLane::Completion), 0);
    assert_eq!(guarded.uncommitted_free(), 0);
    assert!(matches!(
        guarded.reserve(DiskKind::Wal, BudgetLane::Completion, 1),
        Err(MemoryError::DiskCapacity {
            requested: 1,
            available: 0
        })
    ));
    let open = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    // A first sample is always worth taking; none is required to admit.
    assert!(open.sample_due());
    let held = open
        .reserve(DiskKind::Content, BudgetLane::Ordinary, 5 * MIB)
        .unwrap();
    assert_eq!(open.stats().outstanding, 5 * MIB);
    // No estimate is still reported as no free space: an unbounded budget
    // never claims to know the volume.
    assert_eq!(open.uncommitted_free(), 0);
    drop(held);
    assert_eq!(open.stats().outstanding, 0);
    assert!(open.refresh_with(|| Some(3)));
    assert!(!open.sample_due());
    assert_eq!(open.uncommitted_free(), 3);
    assert_eq!(open.available(BudgetLane::Ordinary), 3);
}

#[test]
fn lanes_protect_the_headroom_and_the_completion_reserve() {
    let disk = budget(64 * MIB, 16 * MIB);
    disk.observe(100 * MIB);
    assert_eq!(disk.available(BudgetLane::Ordinary), 20 * MIB);
    assert_eq!(disk.available(BudgetLane::Completion), 36 * MIB);
    assert!(matches!(
        disk.reserve(DiskKind::Wal, BudgetLane::Ordinary, 20 * MIB + 1),
        Err(MemoryError::DiskCapacity { .. })
    ));
    let ordinary = disk
        .reserve(DiskKind::Wal, BudgetLane::Ordinary, 20 * MIB)
        .unwrap();
    assert_eq!(disk.available(BudgetLane::Ordinary), 0);
    assert_eq!(disk.available(BudgetLane::Completion), 16 * MIB);
    let completion = disk
        .reserve(DiskKind::Checkpoint, BudgetLane::Completion, 16 * MIB)
        .unwrap();
    assert!(matches!(
        disk.reserve(DiskKind::Checkpoint, BudgetLane::Completion, 1),
        Err(MemoryError::DiskCapacity {
            requested: 1,
            available: 0
        })
    ));
    let stats = disk.stats();
    assert_eq!(stats.outstanding, 36 * MIB);
    assert_eq!(stats.ordinary_outstanding, 20 * MIB);
    assert_eq!(stats.by_kind[DiskKind::Wal as usize], 20 * MIB);
    assert_eq!(stats.by_kind[DiskKind::Checkpoint as usize], 16 * MIB);
    assert_eq!(disk.uncommitted_free(), 64 * MIB);
    drop(ordinary);
    drop(completion);
    assert_eq!(disk.stats().outstanding, 0);
    assert_eq!(disk.available(BudgetLane::Ordinary), 20 * MIB);
}

#[test]
fn commit_charges_the_estimate_and_drop_returns_the_promise() {
    let disk = budget(10 * MIB, 0);
    disk.observe(50 * MIB);
    let written = disk
        .reserve(DiskKind::Content, BudgetLane::Ordinary, 8 * MIB)
        .unwrap();
    let abandoned = disk
        .reserve(DiskKind::Content, BudgetLane::Ordinary, 4 * MIB)
        .unwrap();
    assert_eq!(disk.uncommitted_free(), 38 * MIB);
    written.commit();
    assert_eq!(disk.stats().free, Some(42 * MIB));
    assert_eq!(disk.uncommitted_free(), 38 * MIB);
    drop(abandoned);
    assert_eq!(disk.stats().free, Some(42 * MIB));
    assert_eq!(disk.uncommitted_free(), 42 * MIB);
    let mut partial = disk
        .reserve(DiskKind::Staging, BudgetLane::Completion, 6 * MIB)
        .unwrap();
    assert!(partial.shrink_to(7 * MIB).is_err());
    partial.shrink_to(MIB).unwrap();
    assert_eq!(disk.stats().outstanding, MIB);
    partial.commit();
    assert_eq!(disk.stats().free, Some(41 * MIB));
    assert_eq!(disk.stats().outstanding, 0);
    // A new observation replaces the estimate entirely.
    disk.observe(41 * MIB + 5);
    assert_eq!(disk.stats().free, Some(41 * MIB + 5));
}

#[test]
fn sampling_is_due_after_a_bounded_run_or_near_the_watermark_and_a_failed_probe_refuses() {
    let disk = budget(10 * MIB, 0);
    assert!(disk.refresh_with(|| Some(100 * MIB)));
    assert!(!disk.sample_due());
    let mut held = Vec::new();
    for _ in 0..4 {
        assert!(!disk.sample_due());
        held.push(
            disk.reserve(DiskKind::Wal, BudgetLane::Completion, MIB)
                .unwrap(),
        );
    }
    assert!(disk.sample_due());
    // A probe that fails forgets the estimate and refuses fresh work.
    assert!(!disk.refresh_with(|| None));
    assert!(matches!(
        disk.reserve(DiskKind::Wal, BudgetLane::Completion, 1),
        Err(MemoryError::DiskCapacity { .. })
    ));
    assert!(disk.refresh_with(|| Some(25 * MIB)));
    // Within twice the headroom every admission samples again.
    assert!(!disk.sample_due());
    let near = disk
        .reserve(DiskKind::Wal, BudgetLane::Completion, 6 * MIB)
        .unwrap();
    assert!(disk.sample_due());
    drop(near);
    drop(held);
    assert!(
        DiskBudget::new(DiskBudgetConfig {
            sample_interval: 0,
            ..DiskBudgetConfig::default()
        })
        .is_err()
    );
}

#[test]
fn reservations_from_independent_owners_share_one_envelope() {
    let disk = budget(0, 0);
    disk.observe(8 * MIB);
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let disk = disk.clone();
            std::thread::spawn(move || {
                let mut admitted = 0u64;
                for _ in 0..64 {
                    if let Ok(reservation) = disk.reserve(DiskKind::Wal, BudgetLane::Ordinary, MIB)
                    {
                        admitted += 1;
                        reservation.commit();
                    }
                }
                admitted
            })
        })
        .collect();
    let admitted: u64 = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(admitted, 8);
    assert_eq!(disk.stats().free, Some(0));
    assert_eq!(disk.stats().outstanding, 0);
}
