//! Real disk owners and authenticated protocol dispatch under controllable loss.
//! Socket/TLS mechanics are separately tested by focal-wire's real QUIC suite.
use crate::fleet::*;
use focal_consensus::NodeConfig;
use focal_ledger::{Session, SessionLimits};
use focal_model::*;
use focal_wire::*;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn peer(node: u64) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(node as u128),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Node { node_id: node },
    })
    .unwrap()
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(999),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn request(id: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    }
}
struct Fleet {
    hosts: Vec<ReplicaHost>,
    owners: Vec<ReplicaOwner>,
    pumps: Vec<tokio::task::JoinHandle<()>>,
    isolated: Arc<AtomicU8>,
}
impl Fleet {
    fn open(root: &std::path::Path) -> Self {
        let mut hosts = vec![];
        let mut owners = vec![];
        let mut channels = vec![];
        for node in 1..=3 {
            let mut config = NodeConfig::single(node, [7; 16], [8; 16]);
            config.voters = vec![1, 2, 3];
            let session = Session::open(
                root.join(node.to_string()),
                ledger(),
                config,
                SessionLimits::default(),
            )
            .unwrap();
            let mut service = ReplicaConfig::new(RootCommandId::from_u128(3));
            service.tick = Duration::from_millis(20);
            service.request_timeout = Duration::from_millis(500);
            let (host, owner, channel) =
                ReplicaHost::spawn(session, service, ReplicaHost::wire_limits()).unwrap();
            hosts.push(host);
            owners.push(owner);
            channels.push(channel);
        }
        let isolated = Arc::new(AtomicU8::new(0));
        let pumps = channels
            .into_iter()
            .enumerate()
            .map(|(index, mut channel)| {
                let targets = hosts.clone();
                let isolated = isolated.clone();
                tokio::spawn(async move {
                    while let Some(frame) = channel.recv().await {
                        let source = index as u8 + 1;
                        if isolated.load(Ordering::SeqCst) == source
                            || isolated.load(Ordering::SeqCst) == frame.target as u8
                        {
                            continue;
                        }
                        let target = &targets[frame.target as usize - 1];
                        let response = dispatch(
                            target,
                            peer(source as u64),
                            frame.request.clone(),
                            &ReplicaHost::wire_limits(),
                        )
                        .await;
                        assert!(
                            matches!(
                                response.result,
                                Response::PeerAccepted
                                    | Response::Error(
                                        AccessError::Unavailable
                                            | AccessError::OutcomeUnknown
                                            | AccessError::Capacity
                                    )
                            ),
                            "{response:?}"
                        );
                    }
                })
            })
            .collect();
        Self {
            hosts,
            owners,
            pumps,
            isolated,
        }
    }
    async fn leader(&self, excluding: Option<usize>) -> usize {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (index, host) in self.hosts.iter().enumerate() {
                    let p = host.progress();
                    if Some(index) != excluding && p.node == p.leader && p.term > 0 {
                        // A current-term read barrier establishes serving readiness.
                        let reply = dispatch(
                            host,
                            actor(),
                            request(
                                9000,
                                Operation::Read(ReadRequest {
                                    consistency: ReadConsistency::Linearizable,
                                    query: ReadQuery::Objects(vec![]),
                                    max_items: 1,
                                }),
                            ),
                            &ReplicaHost::wire_limits(),
                        )
                        .await;
                        if matches!(reply.result, Response::Read(_)) {
                            return index;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("no ready leader")
    }
    async fn all_at(&self, sequence: SessionSeq) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.hosts.iter().any(|h| h.progress().sequence < sequence) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replicas did not converge");
    }
    async fn retry_exact(
        &self,
        mut candidate: usize,
        request: &RequestEnvelope,
    ) -> ResponseEnvelope {
        let mut last = None;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let reply = dispatch(
                    &self.hosts[candidate],
                    actor(),
                    request.clone(),
                    &ReplicaHost::wire_limits(),
                )
                .await;
                if !matches!(
                    reply.result,
                    Response::Error(AccessError::Unavailable | AccessError::OutcomeUnknown)
                ) {
                    return reply;
                }
                last = Some((candidate, self.hosts[candidate].progress(), reply));
                // A completed read or converged prefix does not grant a lease
                // on this leader. Preserve the complete request during healing.
                candidate = self.leader(None).await;
            }
        })
        .await;
        result
            .unwrap_or_else(|error| panic!("exact retry did not complete: {error}; last={last:?}"))
    }
    async fn stop(mut self) {
        for host in &self.hosts {
            host.stop().await.unwrap();
        }
        for owner in self.owners.drain(..) {
            owner.join().unwrap();
        }
        for pump in self.pumps.drain(..) {
            pump.abort();
            if let Err(error) = pump.await {
                assert!(error.is_cancelled(), "replication task panicked: {error}");
            }
        }
    }
}
impl Drop for Fleet {
    fn drop(&mut self) {
        // Cancels transport references on test failure; no permanent task cycle.
        for pump in &self.pumps {
            pump.abort();
        }
    }
}

#[path = "fleet_summary_tests.rs"]
mod summary_tests;

#[path = "fleet_reconciliation_tests.rs"]
mod reconciliation_tests;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_commit_read_barrier_partition_retry_and_restart_use_the_real_owner() {
    let root = tempfile::tempdir().unwrap();
    let fleet = Fleet::open(root.path());
    let leader = fleet.leader(None).await;
    let epoch = request(
        1,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let first = fleet.retry_exact(leader, &epoch).await;
    let Response::Submitted(MutationReply::Committed(receipt)) = &first.result else {
        panic!("{first:?}");
    };
    assert_eq!(receipt.sequence, SessionSeq(1));
    fleet.all_at(SessionSeq(1)).await;
    fleet.isolated.store(leader as u8 + 1, Ordering::SeqCst);
    let next = request(
        2,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let isolated = dispatch(
        &fleet.hosts[leader],
        actor(),
        next.clone(),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(
            isolated.result,
            Response::Error(AccessError::OutcomeUnknown | AccessError::Unavailable)
        ),
        "{isolated:?}"
    );
    let replacement = fleet.leader(Some(leader)).await;
    let committed = fleet.retry_exact(replacement, &next).await;
    let Response::Submitted(MutationReply::Committed(receipt)) = &committed.result else {
        panic!("{committed:?}");
    };
    assert_eq!(receipt.sequence, SessionSeq(2));
    fleet.isolated.store(0, Ordering::SeqCst);
    fleet.all_at(SessionSeq(2)).await;
    assert_eq!(fleet.retry_exact(replacement, &epoch).await, first);
    fleet.stop().await;
    let fleet = Fleet::open(root.path());
    let leader = fleet.leader(None).await;
    assert_eq!(fleet.retry_exact(leader, &next).await, committed);
    assert_eq!(fleet.retry_exact(leader, &epoch).await, first);
    fleet.all_at(SessionSeq(2)).await;
    fleet.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_retry_rediscovery_preserves_receipt_after_cached_owner_loses_leadership() {
    let root = tempfile::tempdir().unwrap();
    let fleet = Fleet::open(root.path());
    let previous = fleet.leader(None).await;
    let request = request(
        41,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let first = fleet.retry_exact(previous, &request).await;
    assert!(matches!(
        first.result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    fleet.all_at(SessionSeq(1)).await;
    let target = (previous + 1) % fleet.hosts.len();
    fleet.hosts[previous]
        .transfer_leader(target as u64 + 1)
        .await
        .unwrap();
    let replacement = fleet.leader(Some(previous)).await;
    assert_ne!(replacement, previous);
    let stale = dispatch(
        &fleet.hosts[previous],
        actor(),
        request.clone(),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(
            stale.result,
            Response::Error(AccessError::Unavailable | AccessError::OutcomeUnknown)
        ),
        "cached owner unexpectedly retained authority: {stale:?}"
    );
    assert_eq!(fleet.retry_exact(previous, &request).await, first);
    assert_eq!(fleet.hosts[replacement].progress().sequence, SessionSeq(1));
    fleet.stop().await;
}
