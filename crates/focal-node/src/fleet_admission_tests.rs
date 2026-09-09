use super::*;
use focal_consensus::{DurableNode, NodeConfig};
use focal_ledger::SessionLimits;
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};

const CLUSTER: [u8; 16] = [23; 16];
const MIB: usize = 1024 * 1024;

fn replica(wal: &SharedWal, ledger: LedgerId, budget: &MemoryBudget) -> FleetReplica {
    let node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, CLUSTER, ledger.session.0),
        wal.clone(),
        budget,
    )
    .unwrap();
    let mut session =
        Session::from_node_in(ledger, node, SessionLimits::default(), budget).unwrap();
    session.campaign().unwrap();
    for _ in 0..3 {
        session.poll().unwrap();
    }
    let mut config = ReplicaConfig::new(RootCommandId::from_u128(23));
    config.tick = Duration::from_millis(50);
    config.request_timeout = Duration::from_millis(500);
    FleetReplica { session, config }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_admitted_at_runtime_installs_its_sessions_under_its_own_quota() {
    let dir = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(512 * MIB, 128 * MIB).unwrap();
    let founder_tenant = TenantId::from_u128(1);
    let founder = budget.child(128 * MIB, 32 * MIB).unwrap();
    let wal = SharedWal::open_with_budget(
        dir.path().join("wal"),
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 0,
        }),
        WalWriterLimits::default(),
        budget.child(64 * MIB, 16 * MIB).unwrap(),
    )
    .unwrap();
    let (fleet, owner, _outgoing) = ReplicaFleet::spawn_managed(
        1,
        CLUSTER,
        vec![wal.clone()],
        vec![FleetTenant {
            tenant: founder_tenant,
            weight: 1,
            budget: founder.clone(),
        }],
        budget.clone(),
        ReplicaHost::wire_limits(),
        ManagedFleetConfig::default(),
    )
    .unwrap();
    let first = LedgerId {
        tenant: founder_tenant,
        session: SessionId::from_u128(1),
    };
    fleet
        .install(1, replica(&wal, first, &founder))
        .await
        .unwrap();
    // A session of a tenant the fleet does not host is refused before
    // admission, with its candidate handed back.
    let other_tenant = TenantId::from_u128(2);
    let other_budget = budget.child(128 * MIB, 32 * MIB).unwrap();
    let second = LedgerId {
        tenant: other_tenant,
        session: SessionId::from_u128(2),
    };
    let refused = fleet
        .install(2, replica(&wal, second, &other_budget))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.error, FleetError::InvalidSession);
    assert!(refused.replica.is_some());
    drop(refused);
    assert!(!fleet.is_admitted(other_tenant));
    // A tenant budget outside the fleet's own is not a tenant of this fleet.
    let foreign = MemoryBudget::new(MIB, 0).unwrap();
    assert_eq!(
        fleet
            .admit_tenant(FleetTenant {
                tenant: other_tenant,
                weight: 1,
                budget: foreign,
            })
            .await,
        Err(FleetError::InvalidSession)
    );
    fleet
        .admit_tenant(FleetTenant {
            tenant: other_tenant,
            weight: 1,
            budget: other_budget.clone(),
        })
        .await
        .unwrap();
    assert!(fleet.is_admitted(other_tenant));
    // Admission is idempotent, and the tenant's sessions now install.
    fleet
        .admit_tenant(FleetTenant {
            tenant: other_tenant,
            weight: 1,
            budget: other_budget.clone(),
        })
        .await
        .unwrap();
    fleet
        .install(2, replica(&wal, second, &other_budget))
        .await
        .unwrap();
    assert_eq!(fleet.status().installed, 2);
    assert!(fleet.hosts(second));
    let usage = fleet.tenant_usage().await.unwrap();
    assert!(usage.contains_key(&founder_tenant));
    assert!(usage.contains_key(&other_tenant));
    assert_eq!(usage.len(), 2);
    fleet.stop_all().await.unwrap();
    drop(fleet);
    drop(owner);
}
