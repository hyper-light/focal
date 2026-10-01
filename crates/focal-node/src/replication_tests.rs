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
/// of streaming to it.
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

/// Paths that stand for the pool: a peer with a gate carries a frame only
/// when the test lets one through, and every frame carried is noted in the
/// order it was.
struct Paths {
    lane: usize,
    gates: BTreeMap<u64, tokio::sync::Semaphore>,
    refusing: BTreeMap<u64, PeerSendError>,
    carried: std::sync::Mutex<Vec<(u64, u128)>>,
    progress: tokio::sync::watch::Sender<usize>,
}
impl Paths {
    fn new(lane: usize, gated: &[u64], refusing: &[(u64, PeerSendError)]) -> Self {
        Self {
            lane,
            gates: gated
                .iter()
                .map(|peer| (*peer, tokio::sync::Semaphore::new(0)))
                .collect(),
            refusing: refusing.iter().cloned().collect(),
            carried: std::sync::Mutex::new(Vec::new()),
            progress: tokio::sync::watch::channel(0).0,
        }
    }
    fn carried(&self, peer: u64) -> Vec<u128> {
        self.carried
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, _)| *to == peer)
            .map(|(_, id)| *id)
            .collect()
    }
    /// Waits until `count` frames in all have been carried.
    async fn until(&self, count: usize) {
        let mut progress = self.progress.subscribe();
        while *progress.borrow_and_update() < count {
            progress.changed().await.unwrap();
        }
    }
    fn open(&self, peer: u64, frames: usize) {
        self.gates[&peer].add_permits(frames);
    }
}
impl Carrier for Paths {
    fn lane(&self) -> usize {
        self.lane
    }
    async fn carry(&self, target: u64, request: &RequestEnvelope) -> Result<(), PeerSendError> {
        if let Some(gate) = self.gates.get(&target) {
            gate.acquire().await.unwrap().forget();
        }
        if let Some(refusal) = self.refusing.get(&target) {
            return Err(refusal.clone());
        }
        self.carried
            .lock()
            .unwrap()
            .push((target, u128::from_be_bytes(request.request_id.0)));
        self.progress.send_modify(|carried| *carried += 1);
        Ok(())
    }
}
fn frame(
    target: u64,
    id: u128,
    lost: &std::sync::mpsc::SyncSender<u64>,
    budget: &MemoryBudget,
) -> ReplicationFrame {
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Raft {
            group: [2; 16],
            message: vec![7, 8, 9],
        },
    };
    ReplicationFrame::for_test(target, request, lost.clone(), budget).unwrap()
}

/// A peer that answers nothing holds its own lane and what waits for it,
/// and nothing of another's (the audit's F42: it held the whole driver, and
/// the frames of the peers that answer waited in the owner's channel behind
/// it). Twenty frames for a peer that answers nothing fill a driver of
/// eight; five for a peer that answers come after them, and every one is
/// carried while the first peer still answers nothing. What the driver had
/// no room for is told to its owner, as what was lost is: nothing is
/// dropped untold, and every frame's charge is given back.
#[tokio::test]
async fn a_peer_that_answers_nothing_holds_its_own_lane_and_nothing_of_anothers() {
    let paths = Paths::new(2, &[2], &[(2, PeerSendError::Lost)]);
    let budget = MemoryBudget::new(1024 * 1024, 0).unwrap();
    let (lost_sender, lost) = std::sync::mpsc::sync_channel(64);
    let (sender, receiver) = mpsc::channel(32);
    for id in 1..=20 {
        sender
            .send(frame(2, id, &lost_sender, &budget))
            .await
            .unwrap();
    }
    for id in 21..=25 {
        sender
            .send(frame(3, id, &lost_sender, &budget))
            .await
            .unwrap();
    }
    drop(sender);
    let (report, ()) = tokio::join!(drive(Receiver::Single(receiver), &paths, 8), async {
        // The five for the peer that answers are carried, all of them,
        // before the peer that answers nothing is let through.
        paths.until(5).await;
        let mut carried = paths.carried(3);
        carried.sort_unstable();
        assert_eq!(carried, vec![21, 22, 23, 24, 25]);
        assert!(paths.carried(2).is_empty());
        paths.open(2, 20);
    });
    let report = report.unwrap();
    assert_eq!(report.attempted, 25);
    assert_eq!(report.accepted, 5);
    // Each of the twenty was lost on its way or given up for the room, and
    // each was told.
    assert_eq!(report.lost + report.refused, 20);
    assert!(report.refused >= 12, "{report:?}");
    assert_eq!(report.saturated, 0);
    let mut told = 0;
    while let Ok(peer) = lost.try_recv() {
        assert_eq!(peer, 2);
        told += 1;
    }
    assert_eq!(told, 20);
    // Never more under way to one peer than its lane, nor more held than
    // the driver may.
    assert!(report.peak_inflight <= 4, "{report:?}");
    assert!(report.peak_waiting <= 6, "{report:?}");
    drop(lost_sender);
    assert_eq!(budget.stats().used, 0);
}

/// What waits for a peer's lane goes in the order it came, what a group
/// cannot do without before its entries: a heartbeat that came behind three
/// appends is carried as soon as the append under way is answered.
#[tokio::test]
async fn what_a_group_cannot_do_without_goes_before_the_entries_that_wait() {
    let paths = Paths::new(1, &[5], &[]);
    let budget = MemoryBudget::new(1024 * 1024, 0).unwrap();
    let (lost_sender, lost) = std::sync::mpsc::sync_channel(8);
    // A channel of one: a frame is sent once the driver has taken the one
    // before it.
    let (sender, receiver) = mpsc::channel(1);
    let (report, ()) = tokio::join!(drive(Receiver::Single(receiver), &paths, 8), async {
        for id in 1..=3 {
            sender
                .send(frame(5, id, &lost_sender, &budget))
                .await
                .unwrap();
        }
        for id in 4..=5 {
            sender
                .send(frame(5, id, &lost_sender, &budget).urgent_for_test())
                .await
                .unwrap();
        }
        // One more, for a peer that answers: once it is carried the driver
        // holds all five, the first under way and four waiting.
        sender
            .send(frame(9, 99, &lost_sender, &budget))
            .await
            .unwrap();
        drop(sender);
        paths.until(1).await;
        assert_eq!(paths.carried(9), vec![99]);
        for carried in 2..=6 {
            paths.open(5, 1);
            paths.until(carried).await;
        }
    });
    let report = report.unwrap();
    assert_eq!((report.attempted, report.accepted), (6, 6));
    assert_eq!(report.peak_inflight, 2);
    assert_eq!(paths.carried(5), vec![1, 4, 5, 2, 3]);
    assert!(lost.try_recv().is_err());
    drop(lost_sender);
    assert_eq!(budget.stats().used, 0);
}

/// A frame its peer refuses is told to its owner as a lost one is: a lane
/// that was full and a peer that had no room were dropped untold, and the
/// group took the frame to be on its way.
#[tokio::test]
async fn a_frame_its_peer_refuses_is_told_to_its_owner() {
    for refusal in [
        PeerSendError::Busy,
        PeerSendError::Rejected(focal_wire::AccessError::Capacity),
        PeerSendError::RouteChanged,
    ] {
        let paths = Paths::new(2, &[], &[(7, refusal.clone())]);
        let budget = MemoryBudget::new(1024 * 1024, 0).unwrap();
        let (lost_sender, lost) = std::sync::mpsc::sync_channel(8);
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(frame(7, 1, &lost_sender, &budget))
            .await
            .unwrap();
        drop(sender);
        let report = drive(Receiver::Single(receiver), &paths, 4).await.unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.accepted, 0);
        assert_eq!(lost.try_recv(), Ok(7), "{refusal:?}");
    }
}

/// Hundreds of groups' frames, for peers that answer and for peers that
/// answer nothing, in a driver that holds a hundred: every frame for a peer
/// that answers is carried while the others answer nothing, every frame is
/// accepted or told, and when the driver is dropped with frames under way
/// and waiting, every charge is given back.
#[tokio::test]
async fn many_groups_share_the_driver_and_a_dropped_driver_gives_everything_back() {
    let paths = Paths::new(4, &[2, 3], &[]);
    let budget = MemoryBudget::new(8 * 1024 * 1024, 0).unwrap();
    let (lost_sender, lost) = std::sync::mpsc::sync_channel(1024);
    // The owner's channel holds all twelve hundred before the driver runs.
    let (sender, receiver) = mpsc::channel(1200);
    // Three hundred frames each for two peers that answer nothing, and for
    // two that answer, a group's at a time: all four interleaved.
    let mut id = 0u128;
    for _ in 0..300 {
        for peer in [2u64, 5, 3, 6] {
            id += 1;
            sender
                .send(frame(peer, id, &lost_sender, &budget))
                .await
                .unwrap();
        }
    }
    // Boxed, so that dropping it below drops the driver itself and all it
    // holds.
    let mut driver = Box::pin(drive(Receiver::Single(receiver), &paths, 100));
    tokio::select! {
        report = &mut driver => panic!("the driver ended with peers that answer nothing: {report:?}"),
        () = paths.until(600) => {}
    }
    assert_eq!(paths.carried(5).len(), 300);
    assert_eq!(paths.carried(6).len(), 300);
    assert!(paths.carried(2).is_empty() && paths.carried(3).is_empty());
    // What the driver had no room for, of the peers that answer nothing,
    // was told; what it still holds is under way or waiting, a hundred at
    // most, charged.
    let mut told = 0usize;
    while let Ok(peer) = lost.try_recv() {
        assert!(peer == 2 || peer == 3);
        told += 1;
    }
    assert!((500..=600).contains(&told), "{told} frames were told");
    assert!(budget.stats().used > 0);
    // The driver is dropped with frames under way and waiting.
    drop(sender);
    drop(driver);
    drop(lost_sender);
    assert_eq!(budget.stats().used, 0);
}
