#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Actual placed evidence copies, private coordinator witnesses and Raft attachment over mTLS QUIC.
use focal_consensus::{DurableNode, NodeConfig};
use focal_evidence::{ContentStore, StoreLimits};
use focal_ledger::{Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::MemoryBudget;
use focal_model::*;
use focal_node::{
    config::{Durability, FailureDomain, Placement, Topology},
    content_host::{ContentHost, ContentOwner},
    custody::{CustodyConfig, CustodyPolicy},
    evidence_service::{EvidenceCoordinator, EvidencePlacement, FleetService},
    fleet::{
        FleetManager, FleetReplica, FleetReplication, FleetTenant, ManagedFleetConfig,
        ReplicaConfig, ReplicaFleet, ReplicaHost, ReplicaOwner, ReplicationFrame,
    },
    managed_service::ManagedService,
    placement::{self, NodeFacts},
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
const ACTOR: ParticipantId = ParticipantId::from_u128(909);
const ROOT: RootCommandId = RootCommandId::from_u128(910);
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(901),
        session: SessionId::from_u128(902),
    }
}
fn wire_limits() -> WireLimits {
    WireLimits {
        request_timeout: Duration::from_secs(15),
        ..ReplicaHost::wire_limits()
    }
}
fn store_limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 1024 * 1024,
        max_staging_bytes: 4 * 1024 * 1024,
        max_uploads: 16,
        chunk_bytes: 4096,
        max_manifest_bytes: 64 * 1024,
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
fn submit(id: u128, command: Command) -> RequestEnvelope {
    request(
        id,
        Operation::Submit {
            expected_revision: None,
            command,
        },
    )
}
async fn eventual(remote: &QuicRemote, request: &RequestEnvelope) -> ResponseEnvelope {
    // Unknown delivery may have persisted bytes or a command. Retry the exact
    // envelope; never invent a new upload/offset/request identity to make progress.
    let mut last = None;
    for _ in 0..8 {
        let response = remote.request(request).await.unwrap();
        if !matches!(
            response.result,
            Response::Error(
                AccessError::OutcomeUnknown | AccessError::Unavailable | AccessError::Capacity
            )
        ) {
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("exact retry did not complete: {last:?}")
}
fn committed(response: &ResponseEnvelope) -> &MutationReceipt {
    let Response::Submitted(MutationReply::Committed(receipt)) = &response.result else {
        panic!("expected committed receipt, got {response:?}")
    };
    receipt
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
#[derive(Clone)]
#[allow(
    clippy::large_enum_variant,
    reason = "one test-owned service per replica; both variants are cloned handles"
)]
enum TestService {
    Single(FleetService),
    Managed(ManagedService),
}
impl RequestHandler for TestService {
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        match self {
            Self::Single(service) => service.handle_accounted(request),
            Self::Managed(service) => service.handle_accounted(request),
        }
    }
}
enum TestReplication {
    Single(tokio::sync::mpsc::Receiver<ReplicationFrame>),
    Managed(FleetReplication),
}
impl TestReplication {
    async fn drive(
        self,
        pool: &PeerConnectionPool,
    ) -> Result<ReplicationReport, ReplicationDriverError> {
        match self {
            Self::Single(channel) => drive_replication(channel, pool, 4).await,
            Self::Managed(channel) => drive_fleet_replication(channel, pool, 4).await,
        }
    }
}
struct Replica {
    host: ReplicaHost,
    manager: Option<FleetManager>,
    owner: Option<ReplicaOwner>,
    content: ContentHost,
    content_owner: Option<ContentOwner>,
    server: Arc<QuicServer>,
    serving: Option<JoinHandle<Result<(), WireError>>>,
    pool: Arc<PeerConnectionPool>,
    driver: Option<JoinHandle<()>>,
    actor: QuicRemote,
    other: QuicRemote,
    rogue_node: QuicRemote,
}
struct Fleet {
    replicas: Vec<Replica>,
    routes: BTreeMap<u64, PeerEndpoint>,
    revision: u64,
}
impl Fleet {
    async fn open(path: &Path, managed: bool) -> Self {
        let pki = Pki::new();
        let identities: Vec<_> = (1..=3)
            .map(|id| pki.issue(format!("evidence-{id}.focal.test"), true))
            .collect();
        let actor_id = pki.issue("actor.focal.test".into(), false);
        let other_id = pki.issue("other.focal.test".into(), false);
        let rogue_id = pki.issue("unassigned-node.focal.test".into(), true);
        let actor = pki.connector(&actor_id);
        let other = pki.connector(&other_id);
        let rogue = pki.connector(&rogue_id);
        let facts: Vec<_> = (1..=3)
            .map(|id| NodeFacts {
                id,
                topology: Topology {
                    region: Some(format!("region-{id}")),
                    zone: Some(format!("zone-{id}")),
                },
                verified: true,
                eligible: true,
            })
            .collect();
        let placement = Placement::default();
        let plan = placement::plan(
            &facts,
            &Durability {
                survive: FailureDomain::Node,
                max_failures: 1,
            },
            &placement,
        )
        .unwrap();
        assert_eq!(plan.voters, vec![1, 2, 3]);
        assert_eq!(plan.content_copies, vec![1, 2]);
        let policy = CustodyPolicy {
            ledger: ledger(),
            route_epoch: RouteEpoch(1),
            policy_revision: 1,
            peers: plan.voters.iter().copied().collect(),
        };
        let assignment =
            EvidencePlacement::verified(policy.scope(), &plan, &facts, &placement).unwrap();
        let mut pending = Vec::new();
        let mut routes = BTreeMap::new();
        for (index, identity) in identities.iter().enumerate() {
            let id = index as u64 + 1;
            let mut config = NodeConfig::single(id, [91; 16], [92; 16]);
            config.voters = plan.voters.clone();
            // Campaign node1 before starting clients. Slower follower elections
            // stabilize setup; the later transfer uses Raft's explicit protocol.
            config.election_tick = match id {
                1 => 10,
                _ => 100,
            };
            let mut resources = None;
            let mut session = if managed {
                let budget = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024).unwrap();
                let tenant = budget.child(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
                let wal = SharedWal::open_with_budget(
                    path.join(id.to_string()).join("ledger"),
                    WalOptions::new(WalIdentity {
                        node: id,
                        cluster: [91; 16],
                        stream: 0,
                    }),
                    WalWriterLimits::default(),
                    budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap(),
                )
                .unwrap();
                let node = DurableNode::open_on_wal_in(config, wal.clone(), &tenant).unwrap();
                let session =
                    Session::from_node_in(ledger(), node, SessionLimits::default(), &tenant)
                        .unwrap();
                resources = Some((budget, tenant, wal));
                session
            } else {
                Session::open(
                    path.join(id.to_string()).join("ledger"),
                    ledger(),
                    config,
                    SessionLimits::default(),
                )
                .unwrap()
            };
            if id == 1 {
                session.campaign().unwrap();
            }
            let mut config = ReplicaConfig::new(ROOT);
            config.tick = Duration::from_millis(20);
            config.request_timeout = Duration::from_secs(1);
            let (host, owner, channel, manager) = if let Some((budget, tenant, wal)) = resources {
                let (manager, owner, channel) = ReplicaFleet::spawn_managed(
                    id,
                    [91; 16],
                    vec![wal],
                    vec![FleetTenant {
                        tenant: ledger().tenant,
                        weight: 1,
                        budget: tenant,
                    }],
                    budget,
                    wire_limits(),
                    ManagedFleetConfig {
                        max_sessions: 4,
                        management_queue: 8,
                    },
                )
                .unwrap();
                let host = manager
                    .install(1, FleetReplica { session, config })
                    .await
                    .unwrap()
                    .value()
                    .host()
                    .clone();
                (
                    host,
                    owner,
                    TestReplication::Managed(channel),
                    Some(manager),
                )
            } else {
                let (host, owner, channel) =
                    ReplicaHost::spawn(session, config, wire_limits()).unwrap();
                (host, owner, TestReplication::Single(channel), None)
            };
            let allowance = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
            let store =
                ContentStore::open(path.join(id.to_string()).join("content"), store_limits())
                    .unwrap();
            let (content, content_owner) = ContentHost::spawn(
                store,
                CustodyConfig::new(id),
                wire_limits(),
                allowance.clone(),
            )
            .unwrap();
            content.install_policy(policy.clone()).await.unwrap();
            let (evidence, evidence_driver) = EvidenceCoordinator::channel(
                content.clone(),
                id,
                vec![assignment.clone()],
                allowance,
                2,
            )
            .unwrap();
            let service = match &manager {
                Some(manager) => TestService::Managed(ManagedService::new(
                    manager.clone(),
                    content.clone(),
                    evidence,
                )),
                None => TestService::Single(FleetService {
                    replica: host.clone(),
                    content: content.clone(),
                    evidence,
                }),
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
            for (identity, principal, role) in [
                (&actor_id, ACTOR, PeerRole::Actor),
                (&other_id, ParticipantId::from_u128(999), PeerRole::Actor),
                (
                    &rogue_id,
                    ParticipantId::from_u128(998),
                    PeerRole::Node { node_id: 999 },
                ),
            ] {
                peers
                    .register_certificate(
                        &identity.certificate,
                        PeerGrant {
                            principal,
                            tenants: BTreeSet::from([ledger().tenant]),
                            role,
                        },
                    )
                    .unwrap();
            }
            let server = Arc::new(
                QuicServer::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    server_tls(identity.tls(), pki.roots(), &wire_limits()).unwrap(),
                    peers,
                    wire_limits(),
                )
                .unwrap(),
            );
            let serving_server = server.clone();
            let serving = tokio::spawn(async move { serving_server.serve(service).await });
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
            };
            routes.insert(id, endpoint.clone());
            pending.push((
                host,
                manager,
                owner,
                channel,
                content,
                content_owner,
                evidence_driver,
                server,
                serving,
                pool,
                endpoint,
            ));
        }
        let mut replicas = Vec::new();
        for (
            host,
            manager,
            owner,
            channel,
            content,
            content_owner,
            evidence_driver,
            server,
            serving,
            pool,
            endpoint,
        ) in pending
        {
            pool.replace_routes(1, routes.clone()).unwrap();
            let sending = pool.clone();
            let driver = tokio::spawn(async move {
                let (replication, evidence) =
                    tokio::join!(channel.drive(&sending), evidence_driver.run(&sending));
                assert!(replication.unwrap().peak_inflight <= 4);
                evidence.unwrap();
            });
            replicas.push(Replica {
                host,
                manager,
                owner: Some(owner),
                content,
                content_owner: Some(content_owner),
                server,
                serving: Some(serving),
                pool,
                driver: Some(driver),
                actor: actor
                    .connect(endpoint.address, &endpoint.server_name)
                    .await
                    .unwrap(),
                other: other
                    .connect(endpoint.address, &endpoint.server_name)
                    .await
                    .unwrap(),
                rogue_node: rogue
                    .connect(endpoint.address, &endpoint.server_name)
                    .await
                    .unwrap(),
            });
        }
        Self {
            replicas,
            routes,
            revision: 1,
        }
    }
    async fn leader(&self) -> usize {
        self.leader_at(None).await
    }
    async fn leader_at(&self, target: Option<u64>) -> usize {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (index, replica) in self.replicas.iter().enumerate() {
                    let progress = replica.host.progress();
                    if !progress.stopped
                        && progress.leader == progress.node
                        && target.is_none_or(|target| progress.node == target)
                    {
                        let response = replica
                            .actor
                            .request(&request(
                                9000,
                                Operation::Read(ReadRequest {
                                    consistency: ReadConsistency::Linearizable,
                                    query: ReadQuery::Objects(vec![]),
                                    max_items: 1,
                                }),
                            ))
                            .await
                            .unwrap();
                        if matches!(response.result, Response::Read(_)) {
                            return index;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("no quorum-authoritative evidence leader")
    }
    async fn all_at(&self, sequence: SessionSeq) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self
                .replicas
                .iter()
                .any(|r| !r.host.progress().stopped && r.host.progress().sequence < sequence)
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    fn omit_route(&mut self, source: usize, target: u64) {
        self.omit_routes(source, &[target]);
    }
    fn omit_routes(&mut self, source: usize, targets: &[u64]) {
        self.revision += 1;
        let mut routes = self.routes.clone();
        for target in targets {
            routes.remove(target);
        }
        self.replicas[source]
            .pool
            .replace_routes(self.revision, routes)
            .unwrap();
    }
    fn restore_routes(&mut self) {
        self.revision += 1;
        for replica in &self.replicas {
            replica
                .pool
                .replace_routes(self.revision, self.routes.clone())
                .unwrap();
        }
    }
    async fn stop_ordering(&mut self, index: usize) {
        if let Some(owner) = self.replicas[index].owner.take() {
            self.replicas[index].host.stop().await.unwrap();
            if let Some(manager) = &self.replicas[index].manager {
                manager.shutdown().await.unwrap();
            }
            owner.join().unwrap();
        }
    }
    async fn stop(mut self) {
        for index in 0..self.replicas.len() {
            self.stop_ordering(index).await;
        }
        for replica in &self.replicas {
            replica.server.close();
        }
        for replica in &mut self.replicas {
            replica.serving.take().unwrap().await.unwrap().unwrap();
        }
        for replica in &mut self.replicas {
            replica.driver.take().unwrap().await.unwrap();
            replica.pool.close();
            replica.content.stop().await.unwrap();
            replica.content_owner.take().unwrap().join().unwrap();
        }
    }
}
impl Drop for Fleet {
    fn drop(&mut self) {
        for replica in &self.replicas {
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
async fn upload(remote: &QuicRemote, id: u8, bytes: &[u8]) -> RequestEnvelope {
    let begin = request(
        1000 + u128::from(id),
        Operation::Upload(UploadRequest::Begin {
            upload: [id; 16],
            length: bytes.len() as u64,
            digest: ContentHash(*blake3::hash(bytes).as_bytes()),
            class: ContentClass::Evidence,
        }),
    );
    assert_eq!(
        remote.request(&begin).await.unwrap().result,
        Response::Upload(UploadReply::Offset(0))
    );
    for (index, chunk) in bytes.chunks(4096).enumerate() {
        let append = request(
            2000 + index as u128,
            Operation::Upload(UploadRequest::Append {
                upload: [id; 16],
                offset: (index * 4096) as u64,
                bytes: chunk.to_vec(),
            }),
        );
        assert_eq!(
            remote.request(&append).await.unwrap().result,
            Response::Upload(UploadReply::Offset((index * 4096 + chunk.len()) as u64))
        );
    }
    request(
        3000 + u128::from(id),
        Operation::Upload(UploadRequest::Seal { upload: [id; 16] }),
    )
}
async fn download(remote: &QuicRemote, reference: &ContentRef) -> Response {
    let mut bytes = Vec::new();
    loop {
        let response = remote
            .request(&request(
                4000,
                Operation::Download {
                    content: reference.clone(),
                    offset: bytes.len() as u64,
                    max_bytes: 65536,
                },
            ))
            .await
            .unwrap()
            .result;
        let Response::Content(chunk) = response else {
            return response;
        };
        assert_eq!(chunk.offset, bytes.len() as u64);
        assert!(chunk.bytes.len() <= 65536);
        assert!(!chunk.bytes.is_empty() || chunk.eof);
        bytes.extend_from_slice(&chunk.bytes);
        if chunk.eof {
            return Response::Content(ContentChunk {
                offset: 0,
                bytes,
                eof: true,
            });
        }
    }
}
fn assert_bytes(response: Response, expected: &[u8]) {
    let Response::Content(chunk) = response else {
        panic!("download rejected: {response:?}");
    };
    assert_eq!(chunk.offset, 0);
    assert!(chunk.eof);
    assert_eq!(chunk.bytes.len(), expected.len());
    assert!(chunk.bytes == expected, "durable bytes differ");
}

fn claim() -> NewClaim {
    let validation = ValidationContent {
        ledger: ledger(),
        schema: SCHEMA_MAJOR,
        claim: ClaimId::from_u128(501),
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        description: "acknowledge receipt".into(),
        quality_bar: None,
        evaluator: ParticipantId::from_u128(999),
        handlers: vec![],
        evidence_schemas: BTreeSet::new(),
        contributed_by: BTreeSet::from([ParticipantId::from_u128(999)]),
        policy_revision: 1,
    };
    NewClaim {
        id: ClaimId::from_u128(501),
        content: ClaimContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            occurrence: OccurrenceId::from_u128(501),
            description: "return durable test evidence".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(ParticipantId::from_u128(999)),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(ACTOR),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(ROOT),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: vec![RequirementRef {
                id: ValidationId::from_u128(504),
                specification: validation.specification_hash().unwrap(),
            }],
            deadline: None,
        },
        validations: vec![NewValidation {
            id: ValidationId::from_u128(504),
            content: validation,
        }],
    }
}
fn attach(id: u128, reference: &ContentRef) -> RequestEnvelope {
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(502),
        epoch: 1,
    };
    submit(
        id,
        Command::AttachArtifact {
            claim: claim().id,
            receipt: fence,
            evidence_set: EvidenceSetId::from_u128(503),
            artifact: NewArtifact {
                id: ArtifactId::from_u128(id),
                content: ArtifactContent {
                    ledger: ledger(),
                    schema: SCHEMA_MAJOR,
                    kind: "test_report".into(),
                    schema_hash: focal_evidence::test_report_schema(),
                    metadata: vec![],
                    payload: ArtifactPayload::Content(reference.clone()),
                    producer: ACTOR,
                    receipt: Some(fence),
                    inputs: BTreeSet::new(),
                    visibility: BTreeSet::new(),
                },
            },
        },
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placed_copies_artifact_attachment_and_cold_leader_pull_use_real_quic() {
    evidence_scenario(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_routing_preserves_custody_artifact_retry_and_cold_leader_failover_over_quic() {
    evidence_scenario(true).await;
}
async fn evidence_scenario(managed: bool) {
    let data = tempfile::tempdir().unwrap();
    let mut fleet = Fleet::open(data.path(), managed).await;
    let leader = fleet.leader().await;
    assert_eq!(leader, 0);
    let mut bytes = br#"{"passed":7,"failed":0,"skipped":1}"#.to_vec();
    bytes.resize(20 * 1024, b' ');
    let seal = upload(&fleet.replicas[leader].actor, 1, &bytes).await;
    let sealed = eventual(&fleet.replicas[leader].actor, &seal).await;
    let Response::Upload(UploadReply::Sealed(reference)) = &sealed.result else {
        panic!("placed seal failed: {sealed:?}")
    };
    let reference = reference.clone();
    for index in [0, 1] {
        assert_bytes(
            download(&fleet.replicas[index].actor, &reference).await,
            &bytes,
        );
    }
    assert!(
        matches!(
            download(&fleet.replicas[2].actor, &reference).await,
            Response::Error(_)
        ),
        "non-copy voter must begin without payload bytes"
    );
    // An actor cannot use node custody operations or mutate another actor's upload.
    let verify = request(
        5000,
        Operation::Custody(CustodyRequest::Verify {
            policy_revision: 1,
            content: reference.clone(),
        }),
    );
    assert_eq!(
        fleet.replicas[0]
            .actor
            .request(&verify)
            .await
            .unwrap()
            .result,
        Response::Error(AccessError::Unauthorized)
    );
    assert_eq!(
        fleet.replicas[0]
            .rogue_node
            .request(&verify)
            .await
            .unwrap()
            .result,
        Response::Error(AccessError::Unauthorized)
    );
    let steal = request(
        5001,
        Operation::Upload(UploadRequest::Seal { upload: [1; 16] }),
    );
    assert!(matches!(
        fleet.replicas[0]
            .other
            .request(&steal)
            .await
            .unwrap()
            .result,
        Response::Error(_)
    ));
    let mut cross_tenant = request(
        5002,
        Operation::Download {
            content: reference.clone(),
            offset: 0,
            max_bytes: 4096,
        },
    );
    cross_tenant.ledger.tenant = TenantId::from_u128(999);
    assert_eq!(
        fleet.replicas[0]
            .actor
            .request(&cross_tenant)
            .await
            .unwrap()
            .result,
        Response::Error(AccessError::Unauthorized)
    );
    // A fresh local seal cannot claim success if the declared remote copy is unreachable.
    let second_bytes = br#"{"passed":8,"failed":0,"skipped":1}"#.to_vec();
    let second_seal = upload(&fleet.replicas[leader].actor, 2, &second_bytes).await;
    fleet.omit_route(leader, 2);
    let missing = fleet.replicas[leader]
        .actor
        .request(&second_seal)
        .await
        .unwrap();
    assert!(
        matches!(
            missing.result,
            Response::Error(AccessError::OutcomeUnknown | AccessError::Unavailable)
        ),
        "missing required copy returned {missing:?}"
    );
    fleet.restore_routes();
    let resumed = eventual(&fleet.replicas[leader].actor, &second_seal).await;
    let Response::Upload(UploadReply::Sealed(second_reference)) = &resumed.result else {
        panic!("same seal retry failed: {resumed:?}")
    };
    let second_reference = second_reference.clone();
    assert_bytes(
        download(&fleet.replicas[1].actor, &second_reference).await,
        &second_bytes,
    );
    for remote in [&fleet.replicas[leader].actor, &fleet.replicas[leader].other] {
        committed(
            &remote
                .request(&request(
                    1,
                    Operation::OpenEpoch {
                        epoch: RequestEpoch(1),
                    },
                ))
                .await
                .unwrap(),
        );
    }
    for (id, command) in [
        (2, Command::GenerateClaim { claim: claim() }),
        (3, Command::PostClaim { claim: claim().id }),
        (
            4,
            Command::AcquireReceipt {
                claim: claim().id,
                receipt: ReceiptId::from_u128(502),
                epoch: 1,
            },
        ),
        (
            5,
            Command::BeginEvidenceSet {
                claim: claim().id,
                receipt: ReceiptFence {
                    receipt: ReceiptId::from_u128(502),
                    epoch: 1,
                },
                evidence_set: EvidenceSetId::from_u128(503),
            },
        ),
    ] {
        let remote = if id <= 3 {
            &fleet.replicas[leader].other
        } else {
            &fleet.replicas[leader].actor
        };
        committed(&remote.request(&submit(id, command)).await.unwrap());
    }
    let attachment = attach(6, &reference);
    let first = eventual(&fleet.replicas[leader].actor, &attachment).await;
    let sequence = committed(&first).sequence;
    fleet.all_at(sequence).await;
    assert!(
        matches!(
            download(&fleet.replicas[2].actor, &reference).await,
            Response::Error(_)
        ),
        "Raft publication must not invent a payload copy"
    );
    fleet.omit_routes(2, &[1, 2]);
    assert_eq!(
        fleet.replicas[2].actor.request(&attachment).await.unwrap(),
        first,
        "a cold follower must replay a committed receipt without fresh custody"
    );
    assert!(matches!(
        download(&fleet.replicas[2].actor, &reference).await,
        Response::Error(_)
    ));
    fleet.restore_routes();
    // Transfer uses Raft's real protocol, so the cold target is deterministic.
    // Stop the former ordering owner after handoff; its independent content
    // custody remains available when new attachments need all declared copies.
    fleet.replicas[leader]
        .host
        .transfer_leader(3)
        .await
        .unwrap();
    let replacement = fleet.leader_at(Some(3)).await;
    assert_eq!(replacement, 2);
    fleet.stop_ordering(leader).await;
    // An immutable receipt needs no fresh custody certification. Both required
    // copies are unreachable from this leader, which still has no local payload.
    fleet.omit_routes(replacement, &[1, 2]);
    let retried = fleet.replicas[replacement]
        .actor
        .request(&attachment)
        .await
        .unwrap();
    assert_eq!(retried, first);
    assert!(
        matches!(
            download(&fleet.replicas[replacement].actor, &reference).await,
            Response::Error(_)
        ),
        "exact receipt replay must not require payload fetching"
    );
    let conflict = fleet.replicas[replacement]
        .actor
        .request(&attach(6, &second_reference))
        .await
        .unwrap();
    assert!(
        matches!(
            conflict.result,
            Response::Submitted(MutationReply::Domain(DomainOutcome::Refuse {
                code: ErrorCode::IdempotencyConflict,
                ..
            }))
        ),
        "changed intent did not return its durable identity conflict: {conflict:?}"
    );
    fleet.restore_routes();
    let new_attachment = eventual(&fleet.replicas[replacement].actor, &attach(7, &reference)).await;
    committed(&new_attachment);
    assert_bytes(
        download(&fleet.replicas[replacement].actor, &reference).await,
        &bytes,
    );
    let second_attachment = eventual(
        &fleet.replicas[replacement].actor,
        &attach(8, &second_reference),
    )
    .await;
    let sequence = committed(&second_attachment).sequence;
    let CommandResult::Artifact(artifact) = committed(&second_attachment).outcome else {
        panic!("attachment returned no artifact identity")
    };
    fleet.all_at(sequence).await;
    let object = ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Artifact,
        id: ObjectId(artifact.id.0),
    };
    let read = fleet.replicas[replacement]
        .actor
        .request(&request(
            6000,
            Operation::Read(ReadRequest {
                consistency: ReadConsistency::Linearizable,
                query: ReadQuery::Objects(vec![object]),
                max_items: 1,
            }),
        ))
        .await
        .unwrap();
    let Response::Read(page) = read.result else {
        panic!("artifact graph read failed")
    };
    assert!(
        matches!(page.objects.as_slice(),[ReadObject::Artifact {id,..}] if *id==artifact.id),
        "published artifact missing from graph page: {page:?}"
    );
    fleet.stop().await;
    // Reopen the actual disk files after releasing their owners. Successful
    // remote custody survived beyond the transport process and RAM caches.
    for id in 1..=3 {
        let store = ContentStore::open(
            data.path().join(id.to_string()).join("content"),
            store_limits(),
        )
        .unwrap();
        assert_eq!(store.read_bytes(&reference, bytes.len()).unwrap(), bytes);
        assert_eq!(
            store
                .read_bytes(&second_reference, second_bytes.len())
                .unwrap(),
            second_bytes
        );
    }
}
