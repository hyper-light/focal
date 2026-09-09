use super::*;
use focal_memory::{BudgetKind, BudgetLane, DiskBudget, DiskBudgetConfig};

const MIB: usize = 1024 * 1024;

fn tenant(index: u128) -> TenantId {
    TenantId::from_u128(index)
}

#[test]
fn the_policy_bounds_the_operator_setting() {
    assert_eq!(
        AdmissionPolicy::standard(None).max_tenants,
        AdmissionPolicy::DEFAULT_MAX_TENANTS
    );
    assert_eq!(AdmissionPolicy::standard(Some(0)).max_tenants, 1);
    assert_eq!(AdmissionPolicy::standard(Some(3)).max_tenants, 3);
    assert_eq!(
        AdmissionPolicy::standard(Some(usize::MAX)).max_tenants,
        AdmissionPolicy::MAX_TENANTS
    );
}

#[test]
fn tenants_are_admitted_under_the_node_budget_until_the_bound_and_answered_again_identically() {
    let node = MemoryBudget::new(64 * MIB, 16 * MIB).unwrap();
    let founder = node.child(32 * MIB, 8 * MIB).unwrap();
    let policy = AdmissionPolicy {
        max_tenants: 3,
        tenant_memory: 32 * MIB,
        tenant_completion_reserve: 8 * MIB,
        weight: 2,
    };
    let mut admission = TenantAdmission::new(node.clone(), policy, tenant(1), founder.clone());
    assert!(admission.is_admitted(tenant(1)));
    assert!(!admission.is_admitted(tenant(2)));
    // The founder keeps the budget the fleet was spawned with.
    assert!(admission.budget(tenant(1)).unwrap().is_within(&founder));
    let second = admission.admit(tenant(2), 4 * MIB as u64).unwrap();
    assert_eq!(second.tenant, tenant(2));
    assert_eq!(second.weight, 2);
    assert!(second.budget.is_within(&node));
    assert_eq!(second.budget.limit(), 32 * MIB);
    let again = admission.admit(tenant(2), 4 * MIB as u64).unwrap();
    assert!(again.budget.is_within(&second.budget));
    // More memory than the allowance can ever give is refused before any
    // child exists; more than the node has free right now likewise.
    assert!(matches!(
        admission.admit(tenant(3), 33 * MIB as u64),
        Err(AdmissionRefusal::Memory)
    ));
    let held = node
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 60 * MIB)
        .unwrap()
        .commit();
    assert!(matches!(
        admission.admit(tenant(3), 8 * MIB as u64),
        Err(AdmissionRefusal::Memory)
    ));
    drop(held);
    admission.admit(tenant(3), 8 * MIB as u64).unwrap();
    assert!(matches!(
        admission.admit(tenant(4), 0),
        Err(AdmissionRefusal::Tenants)
    ));
    assert_eq!(admission.tenants().count(), 3);
    assert!(admission.admit(tenant(3), 0).is_ok());
}

#[test]
fn the_report_names_every_tenant_with_its_queue_and_the_volume() {
    let node = MemoryBudget::new(64 * MIB, 16 * MIB).unwrap();
    let founder = node.child(32 * MIB, 8 * MIB).unwrap();
    let mut admission = TenantAdmission::new(
        node.clone(),
        AdmissionPolicy::standard(Some(2)),
        tenant(1),
        founder.clone(),
    );
    let _charge = founder
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, MIB)
        .unwrap()
        .commit();
    admission.admit(tenant(2), 0).unwrap();
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 5,
        completion_reserve: 1,
        sample_interval: 8,
    })
    .unwrap();
    disk.observe(100);
    let _promised = disk
        .reserve(focal_memory::DiskKind::Wal, BudgetLane::Completion, 7)
        .unwrap();
    let mut usage = BTreeMap::new();
    usage.insert(
        tenant(2),
        QueueUsage {
            items: 3,
            bytes: 300,
            queued: 2,
            sessions: 1,
        },
    );
    let report = admission.report(&disk.stats(), &usage);
    assert_eq!(report.max_tenants, 2);
    assert_eq!(report.memory_limit, 64 * MIB as u64);
    assert!(report.memory_used >= MIB as u64);
    assert_eq!(report.memory_completion_reserve, 16 * MIB as u64);
    assert_eq!(report.disk_free, Some(100));
    assert_eq!(report.disk_outstanding, 7);
    assert_eq!(report.disk_headroom, 5);
    assert_eq!(report.tenants.len(), 2);
    let first = &report.tenants[0];
    assert_eq!(first.tenant, tenant(1));
    assert!(first.memory_used >= MIB as u64);
    assert_eq!(
        (first.sessions, first.queued_items, first.queued_bytes),
        (0, 0, 0)
    );
    let second = &report.tenants[1];
    assert_eq!(second.tenant, tenant(2));
    assert_eq!(
        (second.sessions, second.queued_items, second.queued_bytes),
        (1, 2, 300)
    );
}
