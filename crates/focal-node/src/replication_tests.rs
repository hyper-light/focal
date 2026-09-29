use super::*;
use focal_memory::MemoryBudget;
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId};
use focal_wire::{
    Operation, PROTOCOL_VERSION, PeerEndpoint, PeerPoolLimits, QuicConnector, RequestEnvelope,
    TlsIdentity, WireLimits, client_tls,
};
use std::collections::BTreeMap;
use std::time::Duration;

fn pool() -> PeerConnectionPool {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = identity.cert.der().to_vec();
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![cert.clone()], identity.signing_key.serialize_der()),
        vec![cert],
        &WireLimits::default(),
    )
    .unwrap();
    let connector =
        QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, WireLimits::default()).unwrap();
    PeerConnectionPool::new(
        connector,
        PeerPoolLimits {
            attempts: 1,
            timeout: Duration::from_millis(500),
            unreachable_cooldown: Duration::ZERO,
            ..PeerPoolLimits::default()
        },
    )
    .unwrap()
}

/// A message to a peer that cannot be reached at all is reported to the
/// frame's owner as lost (27 §3.3), so the core probes the member instead
/// of streaming to it; a lane that was full or a peer that refused would
/// not be.
#[tokio::test]
async fn a_peer_that_cannot_be_reached_is_told_to_the_frames_owner() {
    let pool = pool();
    // A route to a port nothing listens on: the dial fails at its deadline.
    let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: blackhole.local_addr().unwrap(),
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let budget = MemoryBudget::new(1024 * 1024, 0).unwrap();
    let (lost_sender, lost) = std::sync::mpsc::sync_channel(4);
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(99),
        operation: Operation::Raft {
            group: [2; 16],
            message: vec![7, 8, 9],
        },
    };
    let frame = ReplicationFrame::for_test(2, request, lost_sender, &budget).unwrap();
    let (sender, receiver) = mpsc::channel(4);
    sender.send(frame).await.unwrap();
    drop(sender);
    let report = drive_replication(receiver, &pool, 4).await.unwrap();
    assert_eq!((report.attempted, report.lost, report.saturated), (1, 1, 0));
    assert_eq!(lost.try_recv(), Ok(2));
    pool.close();
}
