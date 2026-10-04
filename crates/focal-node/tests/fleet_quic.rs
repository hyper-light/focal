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
    /// The frames the replica's driver holds at most.
    inflight: usize,
}
struct Fleet {
    replicas: Vec<Replica>,
    actor_connector: QuicConnector,
    routes: BTreeMap<u64, PeerEndpoint>,
    revision: u64,
    /// The relays the replicas reach each other through, where the fleet
    /// is shaped; kept until the fleet is dropped.
    _relays: Vec<relay::Relay>,
}
/// A wait on the fleet, charged to the periods of the replicas running when
/// it began (27 §3.1 P8): what an allowance holds at their tick, however
/// long the machine takes to run them, and no longer than `FROZEN` while
/// the slowest runs none. A replica stopped since runs no more periods,
/// and while another runs it is charged none: the wait is charged to the
/// others' as they run.
struct FleetWait {
    live: Vec<usize>,
    deadline: focal_timing::ProgressDeadline,
}
impl FleetWait {
    fn check(&mut self, fleet: &Fleet) -> Result<(), focal_timing::Spent> {
        let stopped = |index: &usize| fleet.replicas[*index].host.progress().stopped;
        let running = self.live.iter().any(|index| !stopped(index));
        let periods: Vec<u64> = self
            .live
            .iter()
            .map(|index| {
                if running && stopped(index) {
                    u64::MAX
                } else {
                    fleet.replicas[*index].host.periods()
                }
            })
            .collect();
        self.deadline.check(&periods)
    }
}
/// A path between the replicas slower than the loopback and out of order:
/// every datagram between them crosses a relay that delays it `delay` and
/// up to `jitter` more, and where `loss` is given loses one datagram in so
/// many, so the streams of frames sent together complete in any order; and
/// the connectors of the nodes in `old` offer what a binary before the
/// ordered profile offered, so their frames go plain.
#[derive(Clone, Copy)]
struct Shaped<'a> {
    delay: Duration,
    jitter: Duration,
    loss: Option<u64>,
    old: &'a [u64],
}
/// The fleet's tick: its owners' period.
const TICK: Duration = Duration::from_millis(20);
/// How long a replica's owner may run no period before a wait calls it
/// wedged: the waits are charged to the owners' periods, not to the clock.
const FROZEN: Duration = Duration::from_secs(60);
/// What a binary before the ordered profile offered in its Hello.
const OLDER_PROFILES: [u16; 4] = [
    NATIVE_PROTOCOL_VERSION,
    PEER_PROTOCOL_VERSION,
    MANAGED_PROTOCOL_VERSION,
    PROTOCOL_VERSION,
];
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
    let mut wait = fleet.wait(Duration::from_secs(5));
    let promoted = loop {
        match fleet.replicas[leader]
            .host
            .change_membership(promote.clone())
            .await
        {
            Ok(reply) => break reply,
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::LearnerBehind)) => {
                if let Err(spent) = wait.check(&fleet) {
                    panic!(
                        "the learner never caught up: {spent}; replicas={:?}",
                        fleet.diagnostics()
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(error) => panic!("promotion: {error}"),
        }
    };
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
        Self::open_with(path, grouped, 3, false).await
    }
    /// Three voters, and beyond them `members - 3` fresh nodes: each with
    /// an empty log and a session that is not yet native, admitted to
    /// nothing until a test admits it — a replacement copy as the
    /// placement agent opens one. With `native`, every session is opened
    /// with native hosting (its own content store and seed store), so the
    /// group can activate native history.
    async fn open_with(path: &Path, grouped: bool, members: u64, native: bool) -> Self {
        Self::open_shaped(path, grouped, members, native, None).await
    }
    async fn open_shaped(
        path: &Path,
        grouped: bool,
        members: u64,
        native: bool,
        shaped: Option<Shaped<'_>>,
    ) -> Self {
        let pki = Pki::new();
        let mut relays = Vec::new();
        let identities: Vec<_> = (1..=members)
            .map(|node| pki.issue(format!("node-{node}.focal.test"), true))
            .collect();
        let actor_identity = pki.issue("actor.focal.test".into(), false);
        let actor = pki.connector(&actor_identity);
        let mut pending = Vec::new();
        let mut routes = BTreeMap::new();
        for (index, identity) in identities.iter().enumerate() {
            let id = index as u64 + 1;
            let config = if id <= 3 {
                let mut config = NodeConfig::single(id, [7; 16], [8; 16]);
                config.voters = vec![1, 2, 3];
                config
            } else {
                NodeConfig::joining(id, [7; 16], [8; 16], vec![1, 2, 3], vec![])
            };
            let mut service = ReplicaConfig::new(RootCommandId::from_u128(3));
            service.tick = TICK;
            service.request_timeout = Duration::from_millis(500);
            let (host, owner, channel) = if grouped {
                let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
                let tenant = budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
                let node =
                    DurableNode::open_in(config, path.join(id.to_string()), &tenant).unwrap();
                let session = if native {
                    let content_dir = path.join(format!("content-{id}"));
                    let store = focal_evidence::ContentStore::open(
                        &content_dir,
                        focal_evidence::StoreLimits {
                            max_content_bytes: 64 << 20,
                            max_staging_bytes: 128 << 20,
                            max_uploads: 16,
                            chunk_bytes: 4096,
                            max_manifest_bytes: 1 << 20,
                        },
                    )
                    .unwrap();
                    drop(store);
                    Session::from_node_in_hosted(
                        ledger(),
                        node,
                        SessionLimits::default(),
                        &tenant,
                        focal_ledger::NativeHosting {
                            limits: focal_ledger::NativeSessionLimits::standard(ContentDomainId(
                                ledger().tenant.0,
                            )),
                            reader: focal_evidence::ContentReader::open(&content_dir).unwrap(),
                            seeds: focal_evidence::SeedStore::open(
                                content_dir.join("seeds"),
                                focal_memory::DiskBudget::new(
                                    focal_memory::DiskBudgetConfig::default(),
                                )
                                .unwrap(),
                            )
                            .unwrap(),
                            range: focal_memory::RangeId(1),
                        },
                    )
                    .unwrap()
                } else {
                    Session::from_node_in(ledger(), node, SessionLimits::default(), &tenant)
                        .unwrap()
                };
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
                QuicServer::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    tls,
                    peers,
                    wire_limits(),
                    focal_memory::MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
                )
                .unwrap(),
            );
            let serving = server.clone();
            let handler = host.clone();
            let server_task = tokio::spawn(async move { serving.serve(handler).await });
            let connector = match shaped {
                Some(shaped) if shaped.old.contains(&id) => {
                    pki.connector(identity).offering(&OLDER_PROFILES).unwrap()
                }
                _ => pki.connector(identity),
            };
            // A shaped fleet carries a burst as the product does: a lane of
            // the consensus window to each peer and a driver that holds
            // every lane; the loopback fleets keep their eight, which the
            // tests of a full driver are written to.
            let lane = shaped.map_or(2, |_| focal_consensus::DEFAULT_INFLIGHT_WINDOW);
            let inflight = shaped.map_or(8, |_| {
                focal_consensus::DEFAULT_INFLIGHT_WINDOW * usize::try_from(members).unwrap()
            });
            let pool = Arc::new(
                PeerConnectionPool::new(
                    connector,
                    PeerPoolLimits {
                        max_routes: usize::try_from(members).unwrap(),
                        max_connections: usize::try_from(members).unwrap(),
                        max_inflight: inflight,
                        per_peer_inflight: lane,
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
            // The replicas reach this one through its relay, where there is
            // one; its actor reaches it directly.
            let reached = match shaped {
                Some(shaped) => {
                    let relay = match shaped.loss {
                        Some(loss) => {
                            relay::Relay::lossy(endpoint.address, shaped.delay, shaped.jitter, loss)
                        }
                        None => relay::Relay::new(endpoint.address, shaped.delay, shaped.jitter),
                    };
                    let front = relay.front();
                    relays.push(relay);
                    PeerEndpoint {
                        address: front,
                        ..endpoint.clone()
                    }
                }
                None => endpoint.clone(),
            };
            routes.insert(id, reached);
            pending.push((
                host,
                owner,
                channel,
                server,
                server_task,
                pool,
                endpoint,
                inflight,
            ));
        }
        let mut replicas = Vec::new();
        for (host, owner, channel, server, serving, pool, endpoint, inflight) in pending {
            pool.replace_routes(1, routes.clone()).unwrap();
            let sending = pool.clone();
            let driver = tokio::spawn(async move {
                match channel {
                    Outgoing::Single(channel) => {
                        drive_replication(channel, &sending, inflight).await
                    }
                    Outgoing::Grouped(channel) => {
                        drive_fleet_replication(channel, &sending, inflight).await
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
                inflight,
            });
        }
        Self {
            replicas,
            actor_connector: actor,
            routes,
            revision: 1,
            _relays: relays,
        }
    }
    /// The replica that leads and answers a linearizable read, the wait
    /// charged to the replicas' periods ([`FleetWait`]).
    async fn leader(&self, excluding: Option<usize>) -> usize {
        let mut last_probe = String::new();
        let mut wait = self.wait(Duration::from_secs(10));
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
                        response => last_probe = format!("node {}: {response:?}", progress.node),
                    }
                }
            }
            if let Err(spent) = wait.check(self) {
                panic!(
                    "no quorum leader excluding {excluding:?}: {spent}; last probe={last_probe}; replicas={:?}",
                    self.diagnostics()
                );
            }
            tokio::time::sleep(TICK).await;
        }
    }
    /// A wait of what `allowance` holds at the running replicas' tick
    /// ([`FleetWait`]).
    fn wait(&self, allowance: Duration) -> FleetWait {
        let live: Vec<usize> = self
            .replicas
            .iter()
            .enumerate()
            .filter(|(_, replica)| !replica.host.progress().stopped)
            .map(|(index, _)| index)
            .collect();
        let periods: Vec<u64> = live
            .iter()
            .map(|index| self.replicas[*index].host.periods())
            .collect();
        FleetWait {
            live,
            deadline: focal_timing::ProgressDeadline::begin(
                &periods,
                focal_timing::ProgressDeadline::periods(allowance, TICK),
                FROZEN,
            ),
        }
    }
    /// A wait for every running replica to publish `sequence`, charged to
    /// the running owners' own periods (27 §3.1 P8): what ten seconds hold
    /// at their tick, however long that takes on the machine the test runs
    /// on; the report names each replica's state when the wait is spent.
    async fn all_at(&self, sequence: SessionSeq) {
        let mut wait = self.wait(Duration::from_secs(10));
        while self.replicas.iter().any(|replica| {
            let progress = replica.host.progress();
            !progress.stopped && progress.sequence < sequence
        }) {
            if let Err(spent) = wait.check(self) {
                let replicas: Vec<_> = self
                    .replicas
                    .iter()
                    .map(|replica| {
                        let progress = replica.host.progress();
                        (
                            progress.node,
                            progress.stopped,
                            progress.sequence,
                            progress.frames_held,
                            progress.frames_let_go,
                        )
                    })
                    .collect();
                panic!(
                    "QUIC replicas did not publish {sequence:?}: {spent}; (node, stopped, sequence, frames held, let go): {replicas:?}"
                );
            }
            tokio::time::sleep(TICK).await;
        }
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
    /// Every replica reaches every other again, at a new route revision.
    fn reconnect(&mut self) {
        self.revision += 1;
        for replica in &self.replicas {
            replica
                .pool
                .replace_routes(self.revision, self.routes.clone())
                .unwrap();
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
        assert!(report.peak_inflight <= replica.inflight, "{report:?}");
        // Every frame attempted was handed to a send or given up for the
        // room (refused), and every frame sent was accepted, lost or
        // refused at the pool's bound — none dropped in silence.
        assert_eq!(report.attempted, report.sent + report.refused, "{report:?}");
        assert_eq!(
            report.sent,
            report.accepted + report.lost + report.saturated,
            "{report:?}"
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
    // The isolated leader answers that it cannot know once its request
    // time, in its own periods, has passed; a client whose own wait ends
    // first knows the same of the outcome (`focal_client` maps a wire
    // timeout to it), and on a loaded machine it may end first.
    let uncertain = match fleet.replicas[leader].actor.request(&retry_request).await {
        Ok(answer) => answer.result,
        Err(WireError::Timeout) => Response::Error(AccessError::OutcomeUnknown),
        Err(error) => panic!("the isolated leader's connection failed: {error:?}"),
    };
    assert!(
        matches!(
            uncertain,
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
#[path = "support/relay.rs"]
mod relay;

/// A peer's appends are stepped in the order they left it (27 §12, the
/// audit's F42). Each frame to a peer goes on its own stream, and a path
/// that loses completes the streams in any order — a datagram lost is
/// sent again a round trip and an acknowledgement delay later, the frame
/// behind it arriving first: an append that overtook the one before it
/// was refused by the core and the member probed. With the ordered
/// profile a frame that overtook another is held for it and stepped in
/// its order, and a refusal is left only for what a loss costs — a frame
/// let go past its patience (the one before it lost twice, or its
/// exchange given up), found stale or not stepped, and the appends behind
/// it until the leader sends again — and for a term's first exchange.
/// Each follower counts the refusals that are none of those
/// (`ReplicaProgress::appends_rejected_in_order`): there are none, while
/// the path made the followers hold frames for the ones they overtook.
/// With every connector offering what a binary before the ordered profile
/// offered, the frames go plain and the burst commits as well.
///
/// What the test may not claim is how many appends a run refuses: how
/// many of the frames a loss overtakes name entries their follower lacks
/// rests on how the leader's sends fall against the path's recovery,
/// which the machine's load moves. The plain burst's followers refused 29
/// on the ubuntu CI of 27b0531, 14 on a laptop running the test alone and
/// none in three copies run there at once beside a suite, whose ordered
/// bursts held 61 to 70 frames: the count is printed, never compared. A
/// first statement held the ordered
/// refusals to the frames let go, one each, which a loss with appends
/// behind it on the path passed (9 refused, 5 let go, the same CI).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peers_appends_are_stepped_in_their_order_across_a_lossy_path() {
    let plain = appends_refused_across_a_lossy_path(&[1, 2, 3]).await;
    let ordered = appends_refused_across_a_lossy_path(&[]).await;
    println!(
        "lossy path: {} appends refused with plain frames; {} with ordered ones ({} in order), {} held, {} let go past their patience",
        plain.refused, ordered.refused, ordered.in_order, ordered.held, ordered.let_go
    );
    assert!(
        ordered.held > 0,
        "the path made no ordered frame overtake another: the test has no teeth"
    );
    assert_eq!(
        ordered.in_order, 0,
        "an ordered frame was refused for overtaking within its patience: {} of {} refused, {} let go",
        ordered.in_order, ordered.refused, ordered.let_go
    );
}
/// What the replicas did, while they followed, with a burst's appends
/// across a path.
struct Refusals {
    /// Appends refused for not holding the entry before them.
    refused: u64,
    /// Of those, the ones the order should have spared
    /// (`ReplicaProgress::appends_rejected_in_order`).
    in_order: u64,
    /// Frames held for the one they overtook.
    held: u64,
    /// Frames let go past their patience or their lane.
    let_go: u64,
}
/// The actor's connections to the replicas, a slot each. A slot is dialed
/// once however many requests find it empty at once, and a connection
/// leaves it only while the one a request found lost is still the one it
/// holds, as the product's client keeps its routes (`RouteConnections`,
/// the audit's F60). Each request that found its slot empty used to dial
/// one of its own: the slots of a replica that began to lead took a dial
/// from every request in flight, past the sixteen connections a server
/// holds for one identity, and the server closed the least recently used
/// under the requests they carried ("replaced", the gate on 7a4fba1).
struct ActorConnections {
    slots: Vec<tokio::sync::Mutex<Option<(u64, QuicRemote)>>>,
    dials: std::sync::atomic::AtomicU64,
}
impl ActorConnections {
    fn new(slots: usize) -> Self {
        Self {
            slots: (0..slots).map(|_| tokio::sync::Mutex::new(None)).collect(),
            dials: std::sync::atomic::AtomicU64::new(0),
        }
    }
    /// The connection in `slot` to `replica`, with the dial it came from,
    /// dialed with the slot held where none is: a request that finds the
    /// slot dialing waits for that dial. None while it cannot be opened,
    /// which the caller waits out as a leader not there.
    async fn connect(
        &self,
        fleet: &Fleet,
        replica: usize,
        slot: usize,
    ) -> Option<(u64, QuicRemote)> {
        let mut held = self.slots[slot].lock().await;
        if let Some(connection) = held.as_ref() {
            return Some(connection.clone());
        }
        let remote = fleet
            .actor_connector
            .connect(
                fleet.replicas[replica].server.local_addr().unwrap(),
                &fleet.routes[&(replica as u64 + 1)].server_name,
            )
            .await
            .ok()?;
        let dial = self
            .dials
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *held = Some((dial, remote.clone()));
        Some((dial, remote))
    }
    /// Forget the connection of `dial` in `slot`, while it is still held.
    async fn forget(&self, slot: usize, dial: u64) {
        let mut held = self.slots[slot].lock().await;
        if held.as_ref().is_some_and(|(held, _)| *held == dial) {
            *held = None;
        }
    }
}
/// The appends the followers refused while a burst of entries crossed a
/// path of 5 ms each way that loses one datagram in fifty, the connectors
/// of the nodes in `old` offering the older profiles. A frame whose
/// datagram the path lost arrives a loss detection later — the
/// acknowledgement of what was sent behind it, a round trip and the delay
/// the peer may hold it — after the frame sent behind it, which is how one
/// QUIC stream overtakes another on a real path; a datagram lost twice
/// (one in twenty-five hundred) arrives a probe timeout later still. Two
/// entries are proposed a tick, so an append is on the path as the next
/// leaves; the burst is long enough for a thousand appends — the entries
/// and the commits after them — to cross to the followers, a fiftieth of
/// them overtaken.
async fn appends_refused_across_a_lossy_path(old: &[u64]) -> Refusals {
    const BURST: u128 = 512;
    let directory = tempfile::tempdir().unwrap();
    let fleet = Fleet::open_shaped(
        directory.path(),
        true,
        3,
        false,
        Some(Shaped {
            delay: Duration::from_millis(5),
            jitter: Duration::from_millis(2),
            loss: Some(50),
            old,
        }),
    )
    .await;
    let leader = fleet.leader(None).await;
    // A burst of entries, each a millisecond after the one before, from
    // eight connections at once: the leader's window to each follower
    // holds many appends in flight, each on its own stream, so the
    // jittered path completes them in any order. Each request is asked
    // again, exactly, until its commit is answered: an owner that the path
    // keeps waiting past its request time answers that it cannot know yet,
    // and the exact retry finds the receipt (27 §3.1). Every attempt goes
    // to the replica that leads at the time, as the client follows a leader
    // that moved, over eight connections opened to it when it first leads
    // and again where one is lost, each dialed once (`ActorConnections`): a
    // test that asked the first leader alone waited out its budget on a
    // follower when leadership moved under load.
    const CONNECTIONS: u128 = 8;
    let connections =
        ActorConnections::new(fleet.replicas.len() * usize::try_from(CONNECTIONS).unwrap());
    let connections = &connections;
    let fleet_ref = &fleet;
    let replicas = &fleet.replicas;
    // A refused attempt is asked again after the pause the client itself
    // takes (`RetryPolicy::pause`, the audit's F64): its exponential step
    // spread by full jitter. A fixed pause kept every refused request in
    // step with the others: under load a request whose turn fell just
    // after each slot freed lost every one, refused `Capacity` for its
    // whole budget while the rest committed (six copies at once, all six).
    let policy = focal_client::RetryPolicy::default();
    let policy = &policy;
    let asked: Vec<_> = (1..=BURST)
        .map(|epoch| {
            let connection = usize::try_from(epoch % CONNECTIONS).unwrap();
            async move {
                tokio::time::sleep((TICK / 2).saturating_mul(u32::try_from(epoch).unwrap())).await;
                // Each request opens the principal's epoch under its own
                // identity: one committed entry per request, as the quorum
                // test's exact retry commits a second.
                let envelope = request(
                    epoch,
                    Operation::OpenEpoch {
                        epoch: RequestEpoch(1),
                    },
                );
                // Asked again until committed, the wait charged to what it
                // waits on — the group committing the entries before it: its
                // budget, what a minute holds at the replicas' tick, runs
                // only while no replica's sequence advances. A loaded runner
                // commits slowly and a request refused for the room waits
                // for those ahead of it; charged to the periods alone, six
                // copies at once (a group committing six entries a second)
                // spent the budget of the earliest requests while the burst
                // went on committing. Each restart takes a commit, so the
                // burst's entries bound them. A leader without the room is
                // asked again after the client's pause, never in a loop that
                // keeps it busy refusing.
                let periods = || -> Vec<u64> {
                    replicas.iter().map(|replica| replica.host.periods()).collect()
                };
                let committed = || {
                    replicas
                        .iter()
                        .map(|replica| replica.host.progress().sequence)
                        .max()
                        .unwrap_or_default()
                };
                let budget =
                    focal_timing::ProgressDeadline::periods(Duration::from_secs(60), TICK);
                let mut wait = focal_timing::ProgressDeadline::begin(&periods(), budget, FROZEN);
                let mut seen = committed();
                let mut backoffs = 0u32;
                loop {
                    let leading = replicas
                        .iter()
                        .position(|replica| {
                            let progress = replica.host.progress();
                            progress.node == progress.leader
                        })
                        .unwrap_or(leader);
                    let slot = leading * usize::try_from(CONNECTIONS).unwrap() + connection;
                    let answer = match connections.connect(fleet_ref, leading, slot).await {
                        Some((dial, actor)) => match actor.request(&envelope).await {
                            Ok(answer) => answer.result,
                            Err(WireError::Timeout) => Response::Error(AccessError::OutcomeUnknown),
                            // A connection lost is opened again for the next
                            // attempt, which waits as for a leader not there.
                            // A loss ends the exchange under way with the
                            // error its stream met — a write or a read on a
                            // closed connection is `Io` — so the connection's
                            // own state says whether it was lost, as the
                            // product's peer pool asks it.
                            Err(error) if matches!(error, WireError::Connection) || actor.closed() => {
                                connections.forget(slot, dial).await;
                                Response::Error(AccessError::Unavailable)
                            }
                            Err(error) => panic!("entry {epoch}: {error:?}"),
                        },
                        None => Response::Error(AccessError::Unavailable),
                    };
                    match answer {
                        Response::Submitted(MutationReply::Committed(receipt)) => return receipt,
                        Response::Error(
                            refused @ (AccessError::OutcomeUnknown
                            | AccessError::Unavailable
                            | AccessError::Capacity),
                        ) => {
                            let now = committed();
                            if now > seen {
                                seen = now;
                                wait = focal_timing::ProgressDeadline::begin(
                                    &periods(),
                                    budget,
                                    FROZEN,
                                );
                            }
                            if let Err(spent) = wait.check(&periods()) {
                                // Where each replica stood, with what its
                                // session still waits to commit.
                                let mut state = Vec::new();
                                for replica in replicas {
                                    let progress = replica.host.progress();
                                    let pending = replica
                                        .host
                                        .diagnostics()
                                        .await
                                        .map(|reply| {
                                            let value = reply.value();
                                            (
                                                value.pending,
                                                value.persistence_pending,
                                                value.checkpoint_pending,
                                                value.applied_index,
                                                value.committed_index,
                                            )
                                        })
                                        .ok();
                                    state.push((
                                        progress.node,
                                        progress.leader,
                                        progress.term,
                                        progress.role,
                                        progress.sequence,
                                        pending,
                                        progress.frames_held,
                                        progress.frames_let_go,
                                        progress.appends_rejected,
                                        progress.peers_unreachable,
                                    ));
                                }
                                panic!(
                                    "entry {epoch}: {refused:?} after {spent}; (node, leader, term, role, sequence, (pending, persisting, checkpointing, applied, committed), held, let go, rejected, unreachable): {state:?}"
                                );
                            }
                            if !matches!(refused, AccessError::OutcomeUnknown) {
                                tokio::time::sleep(policy.pause(backoffs)).await;
                                backoffs = backoffs.saturating_add(1);
                            }
                        }
                        other => panic!("entry {epoch}: {other:?}"),
                    }
                }
            }
        })
        .collect();
    let receipts = futures_util::future::join_all(asked).await;
    assert_eq!(receipts.len(), usize::try_from(BURST).unwrap());
    fleet
        .all_at(SessionSeq(u64::try_from(BURST).unwrap()))
        .await;
    let mut refusals = Refusals {
        refused: 0,
        in_order: 0,
        held: 0,
        let_go: 0,
    };
    let mut lost = 0;
    // Every replica's, leadership having possibly moved: what a replica
    // refused or held it did while it followed.
    for replica in &fleet.replicas {
        let progress = replica.host.progress();
        lost += progress.peers_unreachable + progress.dropped_replication;
        refusals.refused += progress.appends_rejected;
        refusals.in_order += progress.appends_rejected_in_order;
        refusals.held += progress.frames_held;
        refusals.let_go += progress.frames_let_go;
    }
    println!(
        "lossy path, older offers {old:?}: {} appends refused, {} frames held, {} let go, {lost} exchanges lost",
        refusals.refused, refusals.held, refusals.let_go
    );
    fleet.stop().await;
    refusals
}

/// A follower that lost ordered appends lets the ones held behind them go
/// once their patience has passed, under a replica's own owner as under a
/// group's (27 §12). The frames the leader sent while the follower could
/// not be reached never come, and those sent after were held for them; a
/// replica's own owner let held frames go only beside a group's progress,
/// which it never makes, and the follower held every frame after the gap
/// for ever (the evidence scenario's copy, 2026-10-03).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_lost_ordered_appends_lets_the_held_ones_go_under_its_own_owner() {
    let directory = tempfile::tempdir().unwrap();
    let mut fleet = Fleet::open(directory.path(), false).await;
    let leader = fleet.leader(None).await;
    let follower = (0..fleet.replicas.len())
        .find(|index| *index != leader)
        .unwrap();
    let actor = fleet.replicas[leader].actor.clone();
    let first = committed(&actor, 1).await;
    fleet.all_at(first).await;
    // The follower cannot be reached: the appends sent to it are lost, the
    // order's sequences spent on them, while the other two commit.
    fleet.isolate(follower);
    for id in 2..=4 {
        committed(&actor, id).await;
    }
    fleet.reconnect();
    let last = committed(&actor, 5).await;
    fleet.all_at(last).await;
    let progress = fleet.replicas[follower].host.progress();
    assert!(
        progress.frames_let_go > 0 || progress.frames_held == 0,
        "the follower held {} frames and let none go",
        progress.frames_held
    );
    fleet.stop().await;
}
/// One entry committed through `actor` under request `id`.
async fn committed(actor: &QuicRemote, id: u128) -> SessionSeq {
    let answer = actor
        .request(&request(
            id,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1),
            },
        ))
        .await
        .unwrap();
    let Response::Submitted(MutationReply::Committed(receipt)) = answer.result else {
        panic!("entry {id}: {answer:?}")
    };
    receipt.sequence
}

/// The leader of the moment and its membership view: a fresh group may
/// still be handing leadership on, and replicas sharing a machine with
/// other tests re-elect, so a schedule follows the leader it finds, under
/// a counted budget.
async fn membership_on_leader(fleet: &Fleet) -> (usize, focal_node::fleet::MembershipReply) {
    let mut moved = 0;
    loop {
        let leader = fleet.leader(None).await;
        match fleet.replicas[leader].host.membership().await {
            Ok(reply) => return (leader, reply),
            Err(LedgerError::NotReady { .. }) if moved < 40 => {
                moved += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("membership: {error}; replicas={:?}", fleet.diagnostics()),
        }
    }
}
/// One membership change through the leader of the moment: asked again
/// with its exact identity while its outcome is unknown, of the next
/// leader when this one has handed on, under counted budgets. A learner
/// behind is left to the caller.
async fn change_on_leader(
    fleet: &Fleet,
    request: &SessionMembershipRequest,
) -> Result<(usize, focal_node::fleet::MembershipReply), LedgerError> {
    let (mut unknown, mut moved) = (0, 0);
    loop {
        let leader = fleet.leader(None).await;
        match fleet.replicas[leader]
            .host
            .change_membership(request.clone())
            .await
        {
            Ok(reply) => return Ok((leader, reply)),
            Err(LedgerError::OutcomeUnknown) if unknown < 40 => {
                unknown += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(LedgerError::NotReady { .. }) if moved < 40 => {
                moved += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::LearnerBehind)) => {
                return Err(LedgerError::Consensus(
                    focal_consensus::ConsensusError::LearnerBehind,
                ));
            }
            Err(error) => panic!(
                "membership change: {error} after {unknown} unknown, {moved} moved; replicas={:?}",
                fleet.diagnostics()
            ),
        }
    }
}
/// Checkpoint every voter, the leader of the moment last; returns which
/// replica that was. Each compacts its log behind a snapshot of its own,
/// as the cadence does on every replica: a voter that leads after a
/// hand-off seeds a late member from its own snapshot, not from a log kept
/// from the first entry. The leader after a drained one had compacted its
/// own (macOS CI at 27b0531); the voter these tests handed leadership to
/// had not, and brought the member up by appends, hiding that its snapshot
/// would not have named it.
async fn checkpoint_every_voter(fleet: &Fleet) -> usize {
    let (leader, view) = membership_on_leader(fleet).await;
    for voter in view.view().configuration.voters.clone() {
        let replica = usize::try_from(voter - 1).unwrap();
        if replica != leader {
            checkpoint_follower(fleet, replica).await;
        }
    }
    checkpoint_on_leader(fleet).await
}
/// Checkpoint the follower `replica`, asked again a tick later while its
/// refusal is one that passes as the owner's own checkpoint waits it out —
/// not ready, a write or a room it waits for, or a prefix its delivery has
/// yet to reach (a native follower that committed what it has not applied:
/// `CheckpointIndex`, which a first version took for a failure) — charged to
/// the replicas' own periods.
async fn checkpoint_follower(fleet: &Fleet, replica: usize) {
    let periods = || -> Vec<u64> {
        fleet
            .replicas
            .iter()
            .map(|replica| replica.host.periods())
            .collect()
    };
    let mut wait = focal_timing::ProgressDeadline::begin(
        &periods(),
        focal_timing::ProgressDeadline::periods(Duration::from_secs(10), TICK),
        FROZEN,
    );
    loop {
        let refused = match fleet.replicas[replica].host.checkpoint().await {
            Ok(()) => return,
            Err(
                refused @ (LedgerError::NotReady { .. }
                | LedgerError::Capacity
                | LedgerError::Consensus(
                    focal_consensus::ConsensusError::CheckpointIndex
                    | focal_consensus::ConsensusError::PersistencePending
                    | focal_consensus::ConsensusError::Capacity,
                )),
            ) => refused,
            Err(LedgerError::Native(error))
                if error.class() == focal_ledger::FailureClass::Retryable =>
            {
                LedgerError::Native(error)
            }
            Err(error) => panic!(
                "checkpoint of {replica}: {error}; replicas={:?}",
                fleet.diagnostics()
            ),
        };
        if let Err(spent) = wait.check(&periods()) {
            panic!(
                "checkpoint of {replica}: {refused} after {spent}; replicas={:?}",
                fleet.diagnostics()
            );
        }
        tokio::time::sleep(TICK).await;
    }
}
/// Checkpoint on the leader of the moment; returns which replica did.
async fn checkpoint_on_leader(fleet: &Fleet) -> usize {
    let mut moved = 0;
    loop {
        let leader = fleet.leader(None).await;
        match fleet.replicas[leader].host.checkpoint().await {
            Ok(()) => return leader,
            Err(LedgerError::NotReady { .. }) if moved < 40 => {
                moved += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("checkpoint: {error}; replicas={:?}", fleet.diagnostics()),
        }
    }
}
/// Hand leadership from whoever leads to `to` (a voter), and wait until it
/// leads; asked again of the next leader when the asked one had handed on.
async fn hand_off(fleet: &Fleet, to: usize) {
    let mut moved = 0;
    loop {
        let (leader, view) = membership_on_leader(fleet).await;
        if leader == to {
            return;
        }
        match fleet.replicas[leader]
            .host
            .transfer_leader_checked(focal_control::ControlTransfer {
                expected_configuration_index: view.view().configuration_index,
                expected: view.view().configuration.clone(),
                target: to as u64 + 1,
            })
            .await
        {
            Ok(()) => {}
            Err(LedgerError::NotReady { .. }) if moved < 40 => {
                moved += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(error) => panic!("transfer: {error}; replicas={:?}", fleet.diagnostics()),
        }
        let mut wait = fleet.wait(Duration::from_secs(10));
        let led = loop {
            if fleet.replicas[to].host.progress().leader == to as u64 + 1 {
                break true;
            }
            if wait.check(fleet).is_err() {
                break false;
            }
            tokio::time::sleep(TICK).await;
        };
        if led {
            return;
        }
        moved += 1;
        assert!(
            moved < 40,
            "leadership never moved to {to}: replicas={:?}",
            fleet.diagnostics()
        );
    }
}
/// Promote `node` through the leader of the moment once it has caught up.
async fn promote_when_caught_up(fleet: &Fleet, node: u64, id: [u8; 16]) {
    let mut wait = fleet.wait(Duration::from_secs(20));
    let promoted = loop {
        let (_, current) = membership_on_leader(fleet).await;
        if current.view().configuration.voters.contains(&node) {
            break current;
        }
        let promote = SessionMembershipRequest {
            id,
            expected_index: current.view().configuration_index,
            expected: current.view().configuration.clone(),
            change: MembershipChange::Promote { node },
        };
        match change_on_leader(fleet, &promote).await {
            Ok((_, reply)) => break reply,
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::LearnerBehind)) => {
                if let Err(spent) = wait.check(fleet) {
                    panic!(
                        "promotion never took: {spent}; replicas={:?}",
                        fleet.diagnostics()
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(error) => panic!("promotion: {error}; replicas={:?}", fleet.diagnostics()),
        }
    };
    assert!(promoted.view().configuration.voters.contains(&node));
}
/// Wait until `replica` applied at least `index` (and, when asked, is
/// native), from its own diagnostics.
async fn applied_at_least(fleet: &Fleet, replica: usize, index: u64, native: bool) {
    let mut wait = fleet.wait(Duration::from_secs(30));
    loop {
        if let Ok(reply) = fleet.replicas[replica].host.diagnostics().await
            && reply.value().applied_index >= index
            && (!native || reply.value().native_active)
        {
            break;
        }
        if let Err(spent) = wait.check(fleet) {
            panic!(
                "replica {replica} never reached {index}: {spent}; replicas={:?}",
                fleet.diagnostics()
            );
        }
        tokio::time::sleep(TICK).await;
    }
}

/// A member whose log ends before the leader's first retained entry — one
/// that was away while the leader checkpointed and compacted, as a
/// replacement copy's empty log is to a leader that checkpointed — is
/// brought up by a snapshot, by the leader that admitted it and by the one
/// that leads after a hand-off before it caught up: a drained session
/// leader's replacement stayed at index 0 for the whole budget on Linux CI
/// (`drain_leader`, 2026-10-01/02) while its leader beat it and never
/// appended to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_behind_a_compacted_log_is_brought_up_by_snapshot_by_any_leader() {
    let directory = tempfile::tempdir().unwrap();
    let mut fleet = Fleet::open(directory.path(), true).await;
    let (leader, initial) = membership_on_leader(&fleet).await;
    let target = (leader + 1) % 3;
    let target_node = target as u64 + 1;
    // The target hears nothing more; it is removed and the leader
    // checkpoints, so its log is retained only from the removal on.
    fleet.isolate(target);
    let removal = SessionMembershipRequest {
        id: [11; 16],
        expected_index: initial.view().configuration_index,
        expected: initial.view().configuration.clone(),
        change: MembershipChange::Remove { node: target_node },
    };
    let (_, removed) = change_on_leader(&fleet, &removal).await.unwrap();
    assert_eq!(removed.view().configuration.voters.len(), 2);
    let leader = checkpoint_every_voter(&fleet).await;
    let compacted = fleet.replicas[leader].host.diagnostics().await.unwrap();
    let compacted_applied = compacted.value().applied_index;
    assert_eq!(
        compacted.value().log_entries_since_checkpoint,
        0,
        "{:?}",
        compacted.value()
    );
    assert!(compacted_applied >= removed.view().configuration_index);
    // Back, and admitted as a learner: what it lacks is behind the
    // leader's first retained entry, so it comes by snapshot.
    fleet.reconnect();
    let (_, current) = membership_on_leader(&fleet).await;
    let add = SessionMembershipRequest {
        id: [12; 16],
        expected_index: current.view().configuration_index,
        expected: current.view().configuration.clone(),
        change: MembershipChange::AddLearner { node: target_node },
    };
    let (leader, added) = change_on_leader(&fleet, &add).await.unwrap();
    assert_eq!(added.view().configuration.learners, vec![target_node]);
    // Leadership is handed to the other voter before the learner caught
    // up: the new leader's view of the learner starts afresh, and it must
    // snapshot it as the old one would have.
    let other = (0..3)
        .find(|index| *index != leader && *index != target)
        .unwrap();
    hand_off(&fleet, other).await;
    applied_at_least(&fleet, target, compacted_applied, false).await;
    promote_when_caught_up(&fleet, target_node, [13; 16]).await;
    fleet.stop().await;
}

/// The replacement copy's own shape (`drain_leader`'s): a node never a
/// member, with an empty log and a session not yet native, admitted as a
/// learner to a group whose leader has checkpointed — it is brought up by
/// that leader's snapshot, and by the next leader's after a hand-off before
/// it caught up, and promoted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_copy_with_an_empty_log_is_brought_up_by_snapshot_by_any_leader() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = Fleet::open_with(directory.path(), true, 4, false).await;
    let fresh = 3usize;
    let fresh_node = 4u64;
    // The leader checkpoints before the fresh copy is admitted: nothing of
    // its log before the checkpoint is retained for a late member.
    let leader = checkpoint_every_voter(&fleet).await;
    let compacted = fleet.replicas[leader].host.diagnostics().await.unwrap();
    let compacted_applied = compacted.value().applied_index;
    assert_eq!(
        compacted.value().log_entries_since_checkpoint,
        0,
        "{:?}",
        compacted.value()
    );
    assert!(compacted_applied > 0);
    let (_, initial) = membership_on_leader(&fleet).await;
    let add = SessionMembershipRequest {
        id: [21; 16],
        expected_index: initial.view().configuration_index,
        expected: initial.view().configuration.clone(),
        change: MembershipChange::AddLearner { node: fresh_node },
    };
    let (leader, added) = change_on_leader(&fleet, &add).await.unwrap();
    assert_eq!(added.view().configuration.learners, vec![fresh_node]);
    // Leadership moves before the learner caught up.
    let other = (leader + 1) % 3;
    hand_off(&fleet, other).await;
    applied_at_least(&fleet, fresh, compacted_applied, false).await;
    promote_when_caught_up(&fleet, fresh_node, [22; 16]).await;
    fleet.stop().await;
}

/// The same, with the group native (the drained leader's session was): the
/// snapshot the fresh copy restores carries a native section and an
/// activation its own session has not applied; it comes up by it, from the
/// leader that admitted it and from the one leading after a hand-off, and
/// is promoted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_copy_is_brought_up_by_a_native_snapshot_by_any_leader() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = Fleet::open_with(directory.path(), true, 4, true).await;
    let fresh = 3usize;
    let fresh_node = 4u64;
    // The voters promise the native decoder to each other and the leader
    // activates native over the empty prefix; the fresh copy, a member of
    // nothing yet, is asked for its promise once it is admitted.
    let leader = fleet.leader(None).await;
    managed::install_support(&fleet, leader).await;
    let mut moved = 0;
    loop {
        let leader = fleet.leader(None).await;
        match fleet.replicas[leader]
            .host
            .activate_native(focal_node::fleet::ActivateNativeCall {
                profile: focal_ledger::NativeContentProfile::AuthoredV1,
                chunk_bytes: 1024 * 1024,
                max_manifest_bytes: 8 * 1024 * 1024,
            })
            .await
        {
            Ok(()) => break,
            Err(LedgerError::NotReady { .. }) if moved < 40 => {
                moved += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("activation: {error}; replicas={:?}", fleet.diagnostics()),
        }
    }
    let mut wait = fleet.wait(Duration::from_secs(10));
    loop {
        let leader = fleet.leader(None).await;
        if let Ok(reply) = fleet.replicas[leader].host.diagnostics().await
            && reply.value().native_active
            && reply.value().native_authoritative
        {
            break;
        }
        if let Err(spent) = wait.check(&fleet) {
            panic!(
                "native never activated: {spent}; replicas={:?}",
                fleet.diagnostics()
            );
        }
        tokio::time::sleep(TICK).await;
    }
    let leader = checkpoint_every_voter(&fleet).await;
    let compacted = fleet.replicas[leader].host.diagnostics().await.unwrap();
    let compacted_applied = compacted.value().applied_index;
    assert_eq!(
        compacted.value().log_entries_since_checkpoint,
        0,
        "{:?}",
        compacted.value()
    );
    let (_, initial) = membership_on_leader(&fleet).await;
    let add = SessionMembershipRequest {
        id: [31; 16],
        expected_index: initial.view().configuration_index,
        expected: initial.view().configuration.clone(),
        change: MembershipChange::AddLearner { node: fresh_node },
    };
    // A native group admits a learner only once its leader has recorded
    // the learner's promise, which it asks for once the admission is
    // queued (the service's discovery asks the candidate first): the first
    // ask is held and answered unknown, the promise is fetched, and the
    // exact request passes.
    let (leader, first) = {
        let leader = fleet.leader(None).await;
        (
            leader,
            fleet.replicas[leader]
                .host
                .change_membership(add.clone())
                .await,
        )
    };
    assert!(
        matches!(
            first,
            Err(LedgerError::OutcomeUnknown | LedgerError::NotReady { .. })
        ),
        "{:?}",
        first.as_ref().err().map(|error| error.to_string())
    );
    managed::promise_of(&fleet, leader, fresh_node).await;
    let (leader, added) = change_on_leader(&fleet, &add).await.unwrap();
    assert_eq!(added.view().configuration.learners, vec![fresh_node]);
    let other = (leader + 1) % 3;
    hand_off(&fleet, other).await;
    applied_at_least(&fleet, fresh, compacted_applied, true).await;
    // The promises the new leader holds were taken at the configuration
    // before the admission; promotion wants the candidate's at the current
    // one, which the service's discovery asks again for.
    let (leader, _) = membership_on_leader(&fleet).await;
    managed::promise_of(&fleet, leader, fresh_node).await;
    promote_when_caught_up(&fleet, fresh_node, [32; 16]).await;
    fleet.stop().await;
}
