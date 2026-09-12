//! Routing through the directory: a node that does not serve a ledger points
//! the client at the leader, a stale client learns the current epoch, and
//! the partition answers route and route-change reads.
use crate::{
    network_service::tests::{Running, settings},
    placement_agent::tests::{controller_peer, join_peer, partition_host},
};
use focal_control::{ControlRead, ControlReadResult};
use focal_model::{ParticipantId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::{
    AccessError, AuthenticatedPeer, Operation, PROTOCOL_VERSION, PeerGrant, PeerRole,
    RequestEnvelope, RequestHandler, Response, WireLimits, verify_request,
};
use std::{collections::BTreeSet, time::Duration};

pub(crate) fn envelope(
    ledger: focal_model::LedgerId,
    route_epoch: u64,
    id: u128,
) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(route_epoch),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Summary,
    }
}
pub(crate) async fn ask(running: &Running, request: RequestEnvelope) -> Response {
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId([77; 16]),
        tenants: BTreeSet::from([request.ledger.tenant]),
        role: PeerRole::Actor,
    })
    .unwrap();
    let verified = verify_request(peer, request, &WireLimits::default()).unwrap();
    running
        .data
        .handle_accounted(&verified)
        .await
        .into_envelope()
        .result
}
pub(crate) async fn redirect_from(
    running: &Running,
    request: RequestEnvelope,
) -> focal_wire::RouteHint {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match ask(running, request.clone()).await {
                Response::Error(AccessError::RouteChanged(hint)) => return hint,
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("never redirected")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_does_not_serve_a_ledger_redirects_to_its_leader_and_a_stale_client_learns_the_epoch()
 {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let peer_settings = settings(peer_dir.path());
    let founder = Running::start(&founder_settings).await;
    let (peer_a, node_a) = join_peer(&founder, founder_dir.path(), "host-a", &peer_settings).await;
    let ledger = founder.status.ledger;
    let founder_node = founder.status.node;
    let host = partition_host(&founder).await;

    // The partition answers route reads once the founder's session is
    // registered: the founder leads at epoch 1, and the route log names it.
    let route = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(ControlReadResult::Route(Some(route))) = host
                .read(
                    controller_peer(&founder),
                    RequestId::from_u128(501),
                    ControlRead::Route { ledger },
                )
                .await
            {
                return route;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("route never known");
    assert_eq!(route.ledger, ledger);
    assert_eq!(route.leader, founder_node);
    assert_eq!(route.route_epoch, RouteEpoch(1));
    assert_eq!(route.leader_generation, 1);
    let ControlReadResult::RouteChanges(batch) = host
        .read(
            controller_peer(&founder),
            RequestId::from_u128(502),
            ControlRead::RouteChanges { after_revision: 0 },
        )
        .await
        .unwrap()
    else {
        panic!("route changes");
    };
    assert_eq!(batch.after_revision, 0);
    assert!(batch.through_revision >= route.source_revision);
    assert!(
        batch
            .changes
            .iter()
            .any(|change| change.ledger == ledger && change.route_epoch == RouteEpoch(1))
    );
    let ControlReadResult::Route(None) = host
        .read(
            controller_peer(&founder),
            RequestId::from_u128(503),
            ControlRead::Route {
                ledger: focal_model::LedgerId {
                    tenant: ledger.tenant,
                    session: focal_model::SessionId([9; 16]),
                },
            },
        )
        .await
        .unwrap()
    else {
        panic!("unknown ledger has no route");
    };

    // Host a hosts nothing for the founder's ledger: a client asking it is
    // pointed at the founder, at the current epoch.
    assert_ne!(node_a, founder_node);
    let hint = redirect_from(&peer_a, envelope(ledger, 1, 1)).await;
    assert_eq!(hint.epoch, RouteEpoch(1));
    assert_eq!(hint.endpoint, founder.status.advertise.to_string());
    assert!(!hint.server_name.is_empty());
    // The founder serves the same request itself.
    let served = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match ask(&founder, envelope(ledger, 1, 2)).await {
                Response::Error(AccessError::RouteChanged(hint)) => {
                    panic!("the leader redirected to {hint:?}")
                }
                Response::Error(AccessError::Unavailable) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                other => return other,
            }
        }
    })
    .await
    .expect("founder never served");
    assert!(!matches!(served, Response::Error(_)), "{served:?}");
    // An unknown ledger stays unavailable everywhere: no route, no hint.
    let unknown = focal_model::LedgerId {
        tenant: ledger.tenant,
        session: focal_model::SessionId([9; 16]),
    };
    assert!(matches!(
        ask(&peer_a, envelope(unknown, 1, 5)).await,
        Response::Error(AccessError::Unavailable)
    ));
    peer_a.stop().await;
    founder.stop().await;
}
