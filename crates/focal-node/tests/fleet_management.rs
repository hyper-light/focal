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
use focal_ledger::{Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::MemoryBudget;
use focal_model::*;
use focal_node::fleet::*;
use focal_wire::*;
use std::time::Duration;

const CLUSTER: [u8; 16] = [141; 16];
fn ledger(index: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(141),
        session: SessionId::from_u128(index),
    }
}
struct Fixture {
    manager: FleetManager,
    owner: ReplicaOwner,
    outgoing: FleetReplication,
    wal: SharedWal,
    tenant: MemoryBudget,
    budget: MemoryBudget,
}
fn fixture(path: &std::path::Path, max_sessions: usize, management_queue: usize) -> Fixture {
    let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let tenant = budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let wal = SharedWal::open_with_budget(
        path,
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
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
        vec![FleetTenant {
            tenant: ledger(1).tenant,
            weight: 1,
            budget: tenant.clone(),
        }],
        budget.clone(),
        ReplicaHost::wire_limits(),
        ManagedFleetConfig {
            max_sessions,
            management_queue,
        },
    )
    .unwrap();
    Fixture {
        manager,
        owner,
        outgoing,
        wal,
        tenant,
        budget,
    }
}
fn candidate(wal: &SharedWal, tenant: &MemoryBudget, index: u128) -> FleetReplica {
    let node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, CLUSTER, ledger(index).session.0),
        wal.clone(),
        tenant,
    )
    .unwrap();
    let mut session =
        Session::from_node_in(ledger(index), node, SessionLimits::default(), tenant).unwrap();
    session.campaign().unwrap();
    for _ in 0..3 {
        session.poll().unwrap();
    }
    let mut config = ReplicaConfig::new(RootCommandId::from_u128(141));
    config.tick = Duration::from_millis(20);
    config.request_timeout = Duration::from_millis(500);
    FleetReplica { session, config }
}
fn request(index: u128) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(index),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    }
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(141),
        tenants: [ledger(1).tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
async fn epoch(host: &ReplicaHost, index: u128) -> ResponseEnvelope {
    dispatch(host, actor(), request(index), &ReplicaHost::wire_limits()).await
}
async fn stopped(host: &ReplicaHost) {
    host.stop().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), host.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn live_install_remove_reopen_fences_stale_handles_and_preserves_exact_durable_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = fixture(directory.path(), 2, 8);
    assert_eq!(fleet.manager.status().installed, 0);
    let installed = fleet
        .manager
        .install(1, candidate(&fleet.wal, &fleet.tenant, 1))
        .await
        .unwrap();
    let original = installed.value().clone();
    drop(installed);
    let first = epoch(original.host(), 1).await;
    assert!(
        matches!(
            first.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{first:?}"
    );
    assert!(matches!(
        fleet
            .manager
            .remove(2, ledger(1), original.incarnation())
            .await,
        Err(FleetError::Running)
    ));
    let second = fleet
        .manager
        .install(2, candidate(&fleet.wal, &fleet.tenant, 2))
        .await
        .unwrap();
    let second_owned = second.value().clone();
    drop(second);
    let second = second_owned;
    let response = epoch(second.host(), 2).await;
    assert!(
        matches!(
            response.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{response:?}"
    );
    assert!(matches!(
        fleet.manager.retry_install(1).await,
        Err(FleetError::RetryExpired)
    ));
    stopped(original.host()).await;
    fleet
        .manager
        .remove(3, ledger(1), original.incarnation())
        .await
        .unwrap();
    // Exact latest removal is idempotent. Old handles still own the old watch.
    fleet
        .manager
        .remove(3, ledger(1), original.incarnation())
        .await
        .unwrap();
    let replacement = fleet
        .manager
        .install(4, candidate(&fleet.wal, &fleet.tenant, 1))
        .await
        .unwrap();
    let replacement_owned = replacement.value().clone();
    drop(replacement);
    let replacement = replacement_owned;
    assert_ne!(replacement.incarnation(), original.incarnation());
    assert!(original.host().stop().await.is_err());
    let stale = epoch(original.host(), 1).await;
    assert!(matches!(stale.result, Response::Error(_)), "{stale:?}");
    let replay = epoch(replacement.host(), 1).await;
    assert_eq!(replay, first);
    assert!(!replacement.host().progress().stopped);
    assert!(!second.host().progress().stopped);
    fleet.manager.shutdown().await.unwrap();
    fleet.owner.join().unwrap();
    drop(fleet.wal);
    drop(fleet.outgoing);
    drop(fleet.manager);
    drop(second);
    drop(replacement);
    assert!(
        fleet.budget.stats().used > 0,
        "stale host must retain incarnation/channel storage"
    );
    drop(original);
    assert_eq!(fleet.budget.stats().used, 0);
}

#[tokio::test]
async fn cancelled_install_is_inspectable_and_only_latest_sequence_is_retryable() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = fixture(directory.path(), 2, 4);
    // Polling once crosses queue admission; cancel before receiving the answer.
    let mut install = Box::pin(
        fleet
            .manager
            .install(1, candidate(&fleet.wal, &fleet.tenant, 1)),
    );
    std::future::poll_fn(|cx| {
        let _ = install.as_mut().poll(cx);
        std::task::Poll::Ready(())
    })
    .await;
    drop(install);
    let inspection = fleet.manager.inspect(ledger(1)).await.unwrap();
    let host = inspection.value().as_ref().unwrap().host().clone();
    let incarnation = inspection.value().as_ref().unwrap().incarnation();
    drop(inspection);
    let retry = fleet.manager.retry_install(1).await.unwrap();
    assert_eq!(retry.value().incarnation(), incarnation);
    drop(retry);
    assert!(matches!(
        fleet.manager.retry_install(2).await,
        Err(FleetError::OutOfOrder)
    ));
    let failed = fleet
        .manager
        .install(3, candidate(&fleet.wal, &fleet.tenant, 2))
        .await
        .err()
        .unwrap();
    assert_eq!(failed.error, FleetError::OutOfOrder);
    assert_eq!(fleet.manager.status().latest_sequence, 1);
    let response = epoch(&host, 1).await;
    assert!(
        matches!(
            response.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{response:?}"
    );
    fleet.manager.shutdown().await.unwrap();
    fleet.owner.join().unwrap();
}

#[tokio::test]
async fn candidate_validation_and_retained_reply_capacity_bound_admission() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = fixture(directory.path(), 1, 1);
    let mut invalid = candidate(&fleet.wal, &fleet.tenant, 1);
    invalid.config.queue_items = 1;
    let failure = fleet.manager.install(1, invalid).await.err().unwrap();
    assert_eq!(failure.error, FleetError::InvalidSession);
    let mut valid = failure.replica.unwrap();
    valid.config.queue_items = 8;
    let delivered = fleet.manager.install(1, valid).await.unwrap();
    assert!(
        matches!(
            fleet.manager.inspect(ledger(1)).await,
            Err(FleetError::Capacity)
        ),
        "delivered response retains bounded management slot"
    );
    let original = delivered.value().clone();
    drop(delivered);
    let extra = fleet
        .manager
        .install(2, candidate(&fleet.wal, &fleet.tenant, 2))
        .await
        .err()
        .unwrap();
    assert_eq!(extra.error, FleetError::Capacity);
    assert_eq!(fleet.manager.status().installed, 1);
    assert_eq!(fleet.manager.status().latest_sequence, 1);
    let foreign_directory = tempfile::tempdir().unwrap();
    let foreign = SharedWal::open_with_budget(
        foreign_directory.path(),
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 1,
        }),
        WalWriterLimits::default(),
        fleet
            .budget
            .child(128 * 1024 * 1024, 32 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    let failure = fleet
        .manager
        .install(2, candidate(&foreign, &fleet.tenant, 3))
        .await
        .err()
        .unwrap();
    assert_eq!(failure.error, FleetError::InvalidSession);
    assert!(
        failure.replica.is_some(),
        "unretained writer never crosses worker boundary"
    );
    drop(failure);
    stopped(original.host()).await;
    fleet
        .manager
        .remove(2, ledger(1), original.incarnation())
        .await
        .unwrap();
    fleet.manager.shutdown().await.unwrap();
    fleet.owner.join().unwrap();
}

#[tokio::test]
async fn dropping_last_manager_stops_empty_fleet_without_detaching_queue_budget() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = fixture(directory.path(), 1, 1);
    let copy = fleet.manager.clone();
    drop(fleet.manager);
    assert!(!copy.status().stopped);
    drop(copy);
    fleet.owner.join().unwrap();
    drop(fleet.wal);
    assert!(fleet.budget.stats().used > 0);
    drop(fleet.outgoing);
    assert_eq!(fleet.budget.stats().used, 0);
}

fn placement_request(
    kind: focal_ledger::SessionFenceKind,
    expected_index: u64,
    configuration: u64,
    to: u64,
) -> SessionPlacementRequest {
    let members = std::collections::BTreeMap::from([(1, 1)]);
    SessionPlacementRequest {
        expected_index,
        expected_configuration_index: configuration,
        operation: focal_ledger::OperationId::from_u128(to as u128),
        kind,
        from_route: RouteEpoch(to - 1),
        to_route: RouteEpoch(to),
        membership_epoch: 1,
        placement_epoch: to,
        placement: focal_directory::PlacementSpec {
            policy: focal_directory::PlacementPolicy {
                durability: focal_directory::DurabilityIntent {
                    survive: focal_directory::FailureClass::Node,
                    max_failures: 0,
                },
                residency: Default::default(),
                home_regions: Default::default(),
                required_memory: 0,
            },
            placement: focal_directory::Placement {
                preferred_leader: 1,
                voters: members.clone(),
                materializers: members.clone(),
                content_copies: members,
            },
        },
    }
}
#[tokio::test]
async fn committed_placement_witness_waits_for_apply_and_route_changes_close_stale_incarnations() {
    use focal_ledger::SessionFenceKind;
    let directory = tempfile::tempdir().unwrap();
    let fleet = fixture(directory.path(), 1, 8);
    let original = {
        let installed = fleet
            .manager
            .install(1, candidate(&fleet.wal, &fleet.tenant, 1))
            .await
            .unwrap();
        installed.value().clone()
    };
    let first = epoch(original.host(), 1).await;
    assert!(
        matches!(
            first.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{first:?}"
    );
    let configuration = original
        .host()
        .membership()
        .await
        .unwrap()
        .view()
        .configuration_index;
    let created = placement_request(SessionFenceKind::Created, 0, configuration, 1);
    let created_reply = original
        .host()
        .propose_placement(created.clone())
        .await
        .unwrap();
    let created_fence = created_reply.witness().fence().clone();
    assert_eq!(created_fence.kind, SessionFenceKind::Created);
    assert!(created_fence.index.0 > 0);
    assert_eq!(created_reply.witness().cluster(), CLUSTER);
    assert_eq!(created_reply.witness().node(), 1);
    let retry = original.host().propose_placement(created).await.unwrap();
    assert_eq!(retry.witness().fence(), &created_fence);
    drop(retry);
    drop(created_reply);
    let cutover = placement_request(
        SessionFenceKind::Cutover,
        created_fence.index.0,
        configuration,
        2,
    );
    let cutover_reply = original.host().propose_placement(cutover).await.unwrap();
    let cutover_index = cutover_reply.witness().fence().index.0;
    drop(cutover_reply);
    assert!(matches!(
        epoch(original.host(), 1).await.result,
        Response::Error(AccessError::Unavailable)
    ));
    let activated = placement_request(SessionFenceKind::Activated, cutover_index, configuration, 2);
    let activated_reply = original
        .host()
        .propose_placement(activated.clone())
        .await
        .unwrap();
    let witness = activated_reply.into_witness();
    assert_eq!(witness.fence().to_route, RouteEpoch(2));
    assert!(witness.fence().index.0 > cutover_index);
    assert!(matches!(
        epoch(original.host(), 1).await.result,
        Response::Error(AccessError::Unavailable)
    ));
    let mut next = request(1);
    next.route_epoch = RouteEpoch(2);
    assert!(
        matches!(
            dispatch(
                original.host(),
                actor(),
                next.clone(),
                &ReplicaHost::wire_limits()
            )
            .await
            .result,
            Response::Error(AccessError::Unavailable)
        ),
        "old serving metadata remains closed until authorized reinstallation"
    );
    stopped(original.host()).await;
    fleet
        .manager
        .remove(2, ledger(1), original.incarnation())
        .await
        .unwrap();
    let stale = fleet
        .manager
        .install(3, candidate(&fleet.wal, &fleet.tenant, 1))
        .await
        .err()
        .unwrap();
    assert_eq!(stale.error, FleetError::InvalidSession);
    let mut candidate = stale.replica.unwrap();
    candidate.config.route_epoch = RouteEpoch(2);
    let replacement = {
        let installed = fleet.manager.install(3, candidate).await.unwrap();
        installed.value().clone()
    };
    let replay = dispatch(
        replacement.host(),
        actor(),
        next,
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert_eq!(replay.result, first.result);
    assert_eq!(replay.route_epoch, RouteEpoch(2));
    let recovered = replacement
        .host()
        .propose_placement(activated)
        .await
        .unwrap();
    assert_eq!(recovered.witness().fence(), witness.fence());
    fleet.manager.shutdown().await.unwrap();
    fleet.owner.join().unwrap();
    drop(fleet.wal);
    drop(fleet.manager);
    drop(fleet.outgoing);
    drop(replacement);
    drop(original);
    assert!(
        fleet.budget.stats().used > 0,
        "delivered proof witnesses retain their ledger and response budgets"
    );
    drop(recovered);
    drop(witness);
    assert_eq!(fleet.budget.stats().used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_final_session_on_stalled_writer_does_not_block_live_installation() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let tenant = budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let open = |index: u32| {
        SharedWal::open_with_budget(
            directory.path().join(index.to_string()),
            WalOptions::new(WalIdentity {
                cluster: CLUSTER,
                node: 1,
                stream: index,
            }),
            WalWriterLimits::default(),
            budget.child(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        )
        .unwrap()
    };
    let first_wal = open(1);
    let second_wal = open(2);
    let first = candidate(&first_wal, &tenant, 1);
    let second = candidate(&second_wal, &tenant, 2);
    let (manager, owner, outgoing) = ReplicaFleet::spawn_managed(
        1,
        CLUSTER,
        vec![first_wal.clone(), second_wal.clone()],
        vec![FleetTenant {
            tenant: ledger(1).tenant,
            weight: 1,
            budget: tenant.clone(),
        }],
        budget.clone(),
        ReplicaHost::wire_limits(),
        ManagedFleetConfig {
            max_sessions: 1,
            management_queue: 8,
        },
    )
    .unwrap();
    let installed = {
        let reply = manager.install(1, first).await.unwrap();
        reply.value().clone()
    };
    let committed = epoch(installed.host(), 1).await;
    assert!(matches!(
        committed.result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    let paused = first_wal.pause_for_test().unwrap();
    drop(first_wal);
    drop(second_wal); // Pause guard owns no writer handle.
    let stopped = tokio::time::timeout(Duration::from_secs(2), installed.host().stop()).await;
    let closed = tokio::time::timeout(Duration::from_secs(2), installed.host().closed()).await;
    let removed = tokio::time::timeout(
        Duration::from_secs(2),
        manager.remove(2, ledger(1), installed.incarnation()),
    )
    .await;
    let replacement =
        tokio::time::timeout(Duration::from_secs(2), manager.install(3, second)).await;
    let live = match &replacement {
        Ok(Ok(reply)) => {
            Some(tokio::time::timeout(Duration::from_secs(2), epoch(reply.value().host(), 2)).await)
        }
        _ => None,
    };
    // Always release the real disk before assertions/teardown, including a
    // regression failure, so a failing test cannot strand its physical writer.
    paused.resume().unwrap();
    assert!(matches!(stopped, Ok(Ok(()))));
    assert!(closed.is_ok());
    assert!(matches!(removed, Ok(Ok(_))));
    assert!(matches!(
        live,
        Some(Ok(ResponseEnvelope {
            result: Response::Submitted(MutationReply::Committed(_)),
            ..
        }))
    ));
    manager.shutdown().await.unwrap();
    owner.join().unwrap();
    drop(removed);
    drop(replacement);
    drop(installed);
    drop(manager);
    drop(outgoing);
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn stop_all_quiesces_concurrent_installation_and_stops_every_live_incarnation() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = fixture(directory.path(), 3, 8);
    let first = {
        let reply = fleet
            .manager
            .install(1, candidate(&fleet.wal, &fleet.tenant, 1))
            .await
            .unwrap();
        reply.value().clone()
    };
    let second = {
        let reply = fleet
            .manager
            .install(2, candidate(&fleet.wal, &fleet.tenant, 2))
            .await
            .unwrap();
        reply.value().clone()
    };
    let later = candidate(&fleet.wal, &fleet.tenant, 3);
    let mut stopping = Box::pin(fleet.manager.stop_all());
    std::future::poll_fn(|cx| {
        let _ = stopping.as_mut().poll(cx);
        std::task::Poll::Ready(())
    })
    .await;
    let rejected = fleet.manager.install(3, later).await.err().unwrap();
    assert_eq!(rejected.error, FleetError::Unavailable);
    drop(rejected);
    stopping.await.unwrap();
    fleet.owner.join().unwrap();
    assert!(first.host().progress().stopped);
    assert!(second.host().progress().stopped);
    assert!(fleet.manager.status().stopped);
    assert!(matches!(
        fleet.manager.current_host(ledger(1)),
        Err(FleetError::Unavailable)
    ));
    drop(first);
    drop(second);
    drop(fleet.manager);
    drop(fleet.outgoing);
    drop(fleet.wal);
    assert_eq!(fleet.budget.stats().used, 0);
}
