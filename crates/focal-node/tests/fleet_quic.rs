#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Three disk-backed session owners using real mutually authenticated QUIC.
use focal_consensus::{DurableNode, NodeConfig};
use focal_ledger::{
    LedgerError, MembershipChange, Session, SessionLimits, SessionMembershipRequest,
};
use focal_memory::MemoryBudget;
use focal_model::*;
use focal_node::{
    fleet::{
        FleetReplica, FleetReplication, FleetTenant, ReplicaConfig, ReplicaFleet, ReplicaHost,
        ReplicaOwner,
    },
    replication::{
        ReplicationDriverError, ReplicationReport, drive_fleet_replication, drive_replication,
    },
};
use focal_wire::*;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
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
fn wire_limits() -> WireLimits {
    WireLimits {
        request_timeout: Duration::from_secs(2),
        ..ReplicaHost::wire_limits()
    }
}
struct CertificateIdentity {
    certificate: Vec<u8>,
    key: Vec<u8>,
    name: String,
}
impl CertificateIdentity {
    fn tls(&self) -> TlsIdentity {
        TlsIdentity::from_pkcs8(vec![self.certificate.clone()], self.key.clone())
    }
}
struct Pki {
    certificate: Certificate,
    key: KeyPair,
}
impl Pki {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().unwrap();
        Self {
            certificate: params.self_signed(&key).unwrap(),
            key,
        }
    }
    fn issue(&self, name: String, node: bool) -> CertificateIdentity {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.clone()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = if node {
            vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ]
        } else {
            vec![ExtendedKeyUsagePurpose::ClientAuth]
        };
        let issuer = Issuer::from_ca_cert_der(self.certificate.der(), &self.key).unwrap();
        let certificate = params.signed_by(&key, &issuer).unwrap();
        CertificateIdentity {
            certificate: certificate.der().to_vec(),
            key: key.serialize_der(),
            name,
        }
    }
    fn roots(&self) -> Vec<Vec<u8>> {
        vec![self.certificate.der().to_vec()]
    }
    fn connector(&self, identity: &CertificateIdentity) -> QuicConnector {
        let tls = client_tls(identity.tls(), self.roots(), &wire_limits()).unwrap();
        QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire_limits()).unwrap()
    }
}
struct Replica {
    host: ReplicaHost,
    owner: Option<ReplicaOwner>,
    server: Arc<QuicServer>,
    serving: Option<JoinHandle<Result<(), WireError>>>,
    pool: Arc<PeerConnectionPool>,
    driver: Option<JoinHandle<Result<ReplicationReport, ReplicationDriverError>>>,
    actor: QuicRemote,
}
struct Fleet {
    replicas: Vec<Replica>,
    actor_connector: QuicConnector,
    routes: BTreeMap<u64, PeerEndpoint>,
    revision: u64,
}
enum Outgoing {
    Single(tokio::sync::mpsc::Receiver<focal_node::fleet::ReplicationFrame>),
    Grouped(FleetReplication),
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_membership_commits_catchup_promotion_and_exact_failover_retry() {
    let directory = tempfile::tempdir().unwrap();
    let mut fleet = Fleet::open(directory.path(), true).await;
    let leader = fleet.leader(None).await;
    let target = (leader + 1) % 3;
    let initial = fleet.replicas[leader].host.membership().await.unwrap();
    let removal = SessionMembershipRequest {
        id: [1; 16],
        expected_index: initial.view().configuration_index,
        expected: initial.view().configuration.clone(),
        change: MembershipChange::Remove {
            node: target as u64 + 1,
        },
    };
    let removed = fleet.replicas[leader]
        .host
        .change_membership(removal.clone())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "membership remove failed: {error}; replicas={:?}",
                fleet.diagnostics()
            )
        });
    assert_eq!(removed.view().configuration.voters.len(), 2);
    assert_eq!(
        fleet.replicas[leader]
            .host
            .change_membership(removal.clone())
            .await
            .unwrap()
            .view(),
        removed.view()
    );
    let mut conflict = removal.clone();
    conflict.change = MembershipChange::AddLearner { node: 4 };
    assert!(matches!(
        fleet.replicas[leader]
            .host
            .change_membership(conflict)
            .await,
        Err(LedgerError::MembershipConflict)
    ));
    let add = SessionMembershipRequest {
        id: [2; 16],
        expected_index: removed.view().configuration_index,
        expected: removed.view().configuration.clone(),
        change: MembershipChange::AddLearner {
            node: target as u64 + 1,
        },
    };
    let added = fleet.replicas[leader]
        .host
        .change_membership(add)
        .await
        .unwrap();
    assert_eq!(added.view().configuration.learners, vec![target as u64 + 1]);
    let promote = SessionMembershipRequest {
        id: [3; 16],
        expected_index: added.view().configuration_index,
        expected: added.view().configuration.clone(),
        change: MembershipChange::Promote {
            node: target as u64 + 1,
        },
    };
    let promoted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match fleet.replicas[leader]
                .host
                .change_membership(promote.clone())
                .await
            {
                Ok(reply) => break reply,
                Err(LedgerError::Consensus(focal_consensus::ConsensusError::LearnerBehind)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("promotion: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(promoted.view().configuration.voters, vec![1, 2, 3]);
    let receipt = promoted.view().latest.clone().unwrap();
    fleet.isolate(leader);
    assert!(
        fleet.replicas[leader].host.membership().await.is_err(),
        "isolated cached leader cannot complete quorum membership read"
    );
    let next = fleet.leader(Some(leader)).await;
    let retried = fleet.replicas[next]
        .host
        .change_membership(promote.clone())
        .await
        .unwrap();
    assert_eq!(retried.view().latest.as_ref(), Some(&receipt));
    fleet.stop().await;
    let reopened = Fleet::open(directory.path(), true).await;
    let leader = reopened.leader(None).await;
    let recovered = reopened.replicas[leader]
        .host
        .change_membership(promote)
        .await
        .unwrap();
    assert_eq!(recovered.view().latest.as_ref(), Some(&receipt));
    reopened.stop().await;
}
impl Fleet {
    fn diagnostics(&self) -> Vec<(focal_node::fleet::ReplicaProgress, PeerPoolStats, bool)> {
        self.replicas
            .iter()
            .map(|replica| {
                (
                    replica.host.progress(),
                    replica.pool.stats(),
                    replica.driver.as_ref().is_none_or(JoinHandle::is_finished),
                )
            })
            .collect()
    }
    async fn open(path: &Path, grouped: bool) -> Self {
        let pki = Pki::new();
        let identities: Vec<_> = (1..=3)
            .map(|node| pki.issue(format!("node-{node}.focal.test"), true))
            .collect();
        let actor_identity = pki.issue("actor.focal.test".into(), false);
        let actor = pki.connector(&actor_identity);
        let mut pending = Vec::new();
        let mut routes = BTreeMap::new();
        for (index, identity) in identities.iter().enumerate() {
            let id = index as u64 + 1;
            let mut config = NodeConfig::single(id, [7; 16], [8; 16]);
            config.voters = vec![1, 2, 3];
            let mut service = ReplicaConfig::new(RootCommandId::from_u128(3));
            service.tick = Duration::from_millis(20);
            service.request_timeout = Duration::from_millis(500);
            let (host, owner, channel) = if grouped {
                let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
                let tenant = budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
                let node =
                    DurableNode::open_in(config, path.join(id.to_string()), &tenant).unwrap();
                let session =
                    Session::from_node_in(ledger(), node, SessionLimits::default(), &tenant)
                        .unwrap();
                let (mut hosts, owner, outgoing) = ReplicaFleet::spawn(
                    id,
                    vec![FleetReplica {
                        session,
                        config: service,
                    }],
                    vec![FleetTenant {
                        tenant: ledger().tenant,
                        weight: 1,
                        budget: tenant,
                    }],
                    budget,
                    wire_limits(),
                )
                .unwrap();
                (
                    hosts.remove(&ledger()).unwrap(),
                    owner,
                    Outgoing::Grouped(outgoing),
                )
            } else {
                let session = Session::open(
                    path.join(id.to_string()),
                    ledger(),
                    config,
                    SessionLimits::default(),
                )
                .unwrap();
                let (host, owner, channel) =
                    ReplicaHost::spawn(session, service, wire_limits()).unwrap();
                (host, owner, Outgoing::Single(channel))
            };
            let peers = PeerRegistry::new(16).unwrap();
            for (source, peer) in identities.iter().enumerate() {
                peers
                    .register_certificate(
                        &peer.certificate,
                        PeerGrant {
                            principal: ParticipantId::from_u128(source as u128 + 1),
                            tenants: BTreeSet::from([ledger().tenant]),
                            role: PeerRole::Node {
                                node_id: source as u64 + 1,
                            },
                        },
                    )
                    .unwrap();
            }
            peers
                .register_certificate(
                    &actor_identity.certificate,
                    PeerGrant {
                        principal: ParticipantId::from_u128(999),
                        tenants: BTreeSet::from([ledger().tenant]),
                        role: PeerRole::Actor,
                    },
                )
                .unwrap();
            let tls = server_tls(identity.tls(), pki.roots(), &wire_limits()).unwrap();
            let server = Arc::new(
                QuicServer::bind("127.0.0.1:0".parse().unwrap(), tls, peers, wire_limits())
                    .unwrap(),
            );
            let serving = server.clone();
            let handler = host.clone();
            let server_task = tokio::spawn(async move { serving.serve(handler).await });
            let pool = Arc::new(
                PeerConnectionPool::new(
                    pki.connector(identity),
                    PeerPoolLimits {
                        max_routes: 3,
                        max_connections: 3,
                        max_inflight: 8,
                        attempts: 1,
                        timeout: Duration::from_millis(500),
                        retry_backoff: Duration::ZERO,
                        ..PeerPoolLimits::default()
                    },
                )
                .unwrap(),
            );
            let endpoint = PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: identity.name.clone(),
                name: None,
            };
            routes.insert(id, endpoint.clone());
            pending.push((host, owner, channel, server, server_task, pool, endpoint));
        }
        let mut replicas = Vec::new();
        for (host, owner, channel, server, serving, pool, endpoint) in pending {
            pool.replace_routes(1, routes.clone()).unwrap();
            let sending = pool.clone();
            let driver = tokio::spawn(async move {
                match channel {
                    Outgoing::Single(channel) => drive_replication(channel, &sending, 8).await,
                    Outgoing::Grouped(channel) => {
                        drive_fleet_replication(channel, &sending, 8).await
                    }
                }
            });
            let remote = actor
                .connect(endpoint.address, &endpoint.server_name)
                .await
                .unwrap();
            replicas.push(Replica {
                host,
                owner: Some(owner),
                server,
                serving: Some(serving),
                pool,
                driver: Some(driver),
                actor: remote,
            });
        }
        Self {
            replicas,
            actor_connector: actor,
            routes,
            revision: 1,
        }
    }
    async fn leader(&self, excluding: Option<usize>) -> usize {
        let mut last_probe = String::new();
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (index, replica) in self.replicas.iter().enumerate() {
                    let progress = replica.host.progress();
                    if Some(index) != excluding
                        && !progress.stopped
                        && progress.node == progress.leader
                        && progress.term > 0
                    {
                        let probe = request(
                            9000,
                            Operation::Read(ReadRequest {
                                consistency: ReadConsistency::Linearizable,
                                query: ReadQuery::Objects(vec![]),
                                max_items: 1,
                            }),
                        );
                        match replica.actor.request(&probe).await {
                            Ok(response) if matches!(response.result, Response::Read(_)) => {
                                return index;
                            }
                            response => {
                                last_probe = format!("node {}: {response:?}", progress.node)
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        result.unwrap_or_else(|error| panic!("no quorum leader excluding {excluding:?}: {error}; last probe={last_probe}; replicas={:?}",self.diagnostics()))
    }
    async fn all_at(&self, sequence: SessionSeq) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.replicas.iter().any(|replica| {
                !replica.host.progress().stopped && replica.host.progress().sequence < sequence
            }) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("QUIC replicas did not publish the committed prefix");
    }
    fn isolate(&mut self, index: usize) {
        self.revision += 1;
        for (source, replica) in self.replicas.iter().enumerate() {
            let routes = if source == index {
                BTreeMap::new()
            } else {
                self.routes
                    .iter()
                    .filter(|(node, _)| **node != index as u64 + 1)
                    .map(|(node, endpoint)| (*node, endpoint.clone()))
                    .collect()
            };
            replica.pool.replace_routes(self.revision, routes).unwrap();
        }
    }
    async fn stop_node(&mut self, index: usize) {
        let replica = &mut self.replicas[index];
        if replica.owner.is_none() {
            return;
        }
        replica.host.stop().await.unwrap();
        replica.server.close();
        replica.pool.close();
        replica.actor.close();
        replica.owner.take().unwrap().join().unwrap();
        let report = replica.driver.take().unwrap().await.unwrap().unwrap();
        assert!(report.peak_inflight <= 8);
        assert_eq!(
            report.attempted,
            report.accepted + report.lost + report.saturated
        );
        replica.serving.take().unwrap().await.unwrap().unwrap();
    }
    async fn stop(mut self) {
        for index in 0..self.replicas.len() {
            self.stop_node(index).await;
        }
    }
}
impl Drop for Fleet {
    fn drop(&mut self) {
        for replica in &mut self.replicas {
            replica.server.close();
            replica.pool.close();
            if let Some(task) = &replica.serving {
                task.abort();
            }
            if let Some(task) = &replica.driver {
                task.abort();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_quic_replicas_preserve_quorum_retry_and_disk_recovery_after_leader_loss() {
    quorum_retry_and_recovery(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouped_egress_preserves_quorum_retry_and_disk_recovery_after_leader_loss() {
    quorum_retry_and_recovery(true).await;
}

async fn quorum_retry_and_recovery(grouped: bool) {
    let directory = tempfile::tempdir().unwrap();
    let mut fleet = Fleet::open(directory.path(), grouped).await;
    let leader = fleet.leader(None).await;
    let first_request = request(
        1,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let first = fleet.replicas[leader]
        .actor
        .request(&first_request)
        .await
        .unwrap();
    let Response::Submitted(MutationReply::Committed(receipt)) = &first.result else {
        panic!("first epoch failed: {first:?}")
    };
    assert_eq!(receipt.sequence, SessionSeq(1));
    fleet.all_at(SessionSeq(1)).await;
    fleet.isolate(leader);
    let retry_request = request(
        2,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let uncertain = fleet.replicas[leader]
        .actor
        .request(&retry_request)
        .await
        .unwrap();
    assert!(
        matches!(
            uncertain.result,
            Response::Error(AccessError::OutcomeUnknown | AccessError::Unavailable)
        ),
        "isolated leader answered {uncertain:?}"
    );
    fleet.stop_node(leader).await;
    let replacement = fleet.leader(Some(leader)).await;
    let committed = fleet.replicas[replacement]
        .actor
        .request(&retry_request)
        .await
        .unwrap();
    let Response::Submitted(MutationReply::Committed(receipt)) = &committed.result else {
        panic!("retry failed: {committed:?}")
    };
    assert_eq!(receipt.sequence, SessionSeq(2));
    fleet.all_at(SessionSeq(2)).await;
    assert_eq!(
        fleet.replicas[replacement]
            .actor
            .request(&first_request)
            .await
            .unwrap(),
        first
    );
    fleet.stop().await;

    let fleet = Fleet::open(directory.path(), grouped).await;
    let leader = fleet.leader(None).await;
    fleet.all_at(SessionSeq(2)).await;
    assert_eq!(
        fleet.replicas[leader]
            .actor
            .request(&retry_request)
            .await
            .unwrap(),
        committed
    );
    assert_eq!(
        fleet.replicas[leader]
            .actor
            .request(&first_request)
            .await
            .unwrap(),
        first
    );
    fleet.stop().await;
}

#[path = "fleet_quic/managed.rs"]
mod managed;
