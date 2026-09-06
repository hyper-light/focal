#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::{DurableNode, NodeConfig};
use focal_evidence::{ContentStore, StoreLimits};
use focal_ledger::{Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::MemoryBudget;
use focal_model::*;
use focal_node::{
    content_host::ContentHost, custody::CustodyConfig, evidence_service::EvidenceCoordinator,
    fleet::*, managed_service::ManagedService,
};
use focal_wire::*;
use std::time::Duration;
const CLUSTER: [u8; 16] = [154; 16];
fn ledger(id: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(id),
        session: SessionId::from_u128(id),
    }
}
fn actor(id: u128) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(154),
        tenants: [ledger(id).tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn request(id: u128) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(id),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    }
}
fn candidate(wal: &SharedWal, tenant: &MemoryBudget, id: u128) -> FleetReplica {
    let node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, CLUSTER, ledger(id).session.0),
        wal.clone(),
        tenant,
    )
    .unwrap();
    let mut session =
        Session::from_node_in(ledger(id), node, SessionLimits::default(), tenant).unwrap();
    session.campaign().unwrap();
    for _ in 0..3 {
        session.poll().unwrap();
    }
    let mut config = ReplicaConfig::new(RootCommandId::from_u128(154));
    config.tick = Duration::from_millis(20);
    config.request_timeout = Duration::from_millis(500);
    FleetReplica { session, config }
}
#[tokio::test]
async fn one_handler_routes_live_installations_without_management_round_trips_and_preserves_scopes()
{
    let data = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024).unwrap();
    let tenants: Vec<_> = (0..2)
        .map(|_| budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap())
        .collect();
    let wal = SharedWal::open_with_budget(
        data.path().join("wal"),
        WalOptions::new(WalIdentity {
            node: 1,
            cluster: CLUSTER,
            stream: 0,
        }),
        WalWriterLimits::default(),
        budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let (manager, owner, outgoing) = ReplicaFleet::spawn_managed(
        1,
        CLUSTER,
        vec![wal.clone()],
        tenants
            .iter()
            .enumerate()
            .map(|(index, budget)| FleetTenant {
                tenant: ledger(index as u128 + 1).tenant,
                weight: 1,
                budget: budget.clone(),
            })
            .collect(),
        budget.clone(),
        ReplicaHost::wire_limits(),
        ManagedFleetConfig {
            max_sessions: 2,
            management_queue: 1,
        },
    )
    .unwrap();
    let (content, content_owner) = ContentHost::spawn(
        ContentStore::open(
            data.path().join("content"),
            StoreLimits {
                max_content_bytes: 1024 * 1024,
                max_staging_bytes: 4 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 4096,
                max_manifest_bytes: 65536,
            },
        )
        .unwrap(),
        CustodyConfig::new(1),
        ReplicaHost::wire_limits(),
        budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let (evidence, driver) = EvidenceCoordinator::channel(
        content.clone(),
        1,
        vec![],
        budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        2,
    )
    .unwrap();
    let service = ManagedService::new(manager.clone(), content.clone(), evidence);
    let empty = dispatch(&service, actor(1), request(1), &ReplicaHost::wire_limits()).await;
    assert_eq!(empty.result, Response::Error(AccessError::Unavailable));
    let foreign = dispatch(&service, actor(2), request(1), &ReplicaHost::wire_limits()).await;
    assert_eq!(foreign.result, Response::Error(AccessError::Unauthorized));
    let installed = manager
        .install(1, candidate(&wal, &tenants[0], 1))
        .await
        .unwrap();
    let original = installed.value().clone();
    // Holding the only management slot would block an inspect round trip. Data
    // uses the same registry through a short borrowed lookup and still commits.
    assert!(matches!(
        manager.inspect(ledger(1)).await,
        Err(FleetError::Capacity)
    ));
    let first = dispatch(&service, actor(1), request(1), &ReplicaHost::wire_limits()).await;
    assert!(
        matches!(
            first.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{first:?}"
    );
    drop(installed);
    let second = {
        let reply = manager
            .install(2, candidate(&wal, &tenants[1], 2))
            .await
            .unwrap();
        reply.value().clone()
    };
    let second_response =
        dispatch(&service, actor(2), request(2), &ReplicaHost::wire_limits()).await;
    assert!(
        matches!(
            second_response.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{second_response:?}"
    );
    assert_eq!(
        dispatch(&service, actor(2), request(1), &ReplicaHost::wire_limits())
            .await
            .result,
        foreign.result
    );
    original.host().stop().await.unwrap();
    original.host().closed().await;
    assert_eq!(
        dispatch(&service, actor(1), request(1), &ReplicaHost::wire_limits())
            .await
            .result,
        empty.result
    );
    manager
        .remove(3, ledger(1), original.incarnation())
        .await
        .unwrap();
    let replacement = {
        let reply = manager
            .install(4, candidate(&wal, &tenants[0], 1))
            .await
            .unwrap();
        reply.value().clone()
    };
    assert_ne!(original.incarnation(), replacement.incarnation());
    assert!(matches!(
        dispatch(
            original.host(),
            actor(1),
            request(1),
            &ReplicaHost::wire_limits()
        )
        .await
        .result,
        Response::Error(_)
    ));
    let retained =
        dispatch_accounted(&service, actor(1), request(1), &ReplicaHost::wire_limits()).await;
    assert_eq!(retained.envelope(), &first);
    manager.shutdown().await.unwrap();
    owner.join().unwrap();
    assert_eq!(
        dispatch(&service, actor(1), request(1), &ReplicaHost::wire_limits())
            .await
            .result,
        empty.result
    );
    content.stop().await.unwrap();
    content_owner.join().unwrap();
    drop(driver);
    drop(service);
    drop(content);
    drop(original);
    drop(second);
    drop(replacement);
    drop(manager);
    drop(outgoing);
    drop(wal);
    assert!(
        budget.stats().used > 0,
        "delivered response lost the tenant/node allowance"
    );
    drop(retained);
    assert_eq!(budget.stats().used, 0);
}
