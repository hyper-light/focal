#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Control RPCs and Raft traffic use the same real, certificate-authenticated QUIC service.
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::*;
use focal_enrollment::{BootstrapAuthority, EnrollmentLimits, EnrollmentRegistry};
use focal_memory::MemoryBudget;
use focal_model::{
    LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId,
};
use focal_node::control_host::*;
use focal_wire::*;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [101; 16];
const GROUP: [u8; 16] = [102; 16];
const OPERATOR: [u8; 16] = [103; 16];
struct RejectUnverifiedEvidence;
impl AuthorityVerifier for RejectUnverifiedEvidence {
    fn verify_enrollment(&self, _: &NodeEnrollment) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_session_fence(&self, _: &SessionFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_replica_ready(&self, _: &ReplicaReady) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_delegation(&self, _: &DelegationFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
fn namespace() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(101),
        session: SessionId::from_u128(102),
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
fn limits() -> WireLimits {
    WireLimits {
        request_timeout: Duration::from_secs(2),
        ..ControlHost::wire_limits()
    }
}
fn request(sequence: u64, revision: u64) -> ControlRequest {
    ControlRequest {
        id: ControlRequestId {
            client: OPERATOR,
            sequence,
        },
        acknowledged_through: 0,
        command: ControlCommand::Root(RootCommand {
            expected_revision: revision,
            operation: RootOperation::RegisterRegion {
                region: RegionRecord {
                    id: RegionId::from_u128(u128::from(sequence)),
                    label: format!("region-{sequence}"),
                    authority_epoch: 1,
                },
                expected_epoch: None,
            },
        }),
    }
}
fn envelope(rpc: ControlRpc) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(99),
        operation: Operation::Control {
            group: GROUP,
            request: rpc.encode(128 * 1024).unwrap(),
        },
    }
}
async fn call(remote: &QuicRemote, rpc: ControlRpc) -> ControlReply {
    let response = remote.request(&envelope(rpc)).await.unwrap();
    let Response::Control { response } = response.result else {
        panic!("expected metadata response")
    };
    ControlReply::decode(&response, limits().max_frame_bytes as usize).unwrap()
}
struct Identity {
    certificate: Vec<u8>,
    key: Vec<u8>,
    name: String,
}
impl Identity {
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
    fn issue(&self, name: String, node: bool) -> Identity {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.clone()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        if node {
            params
                .extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
        }
        let issuer = Issuer::from_ca_cert_der(self.certificate.der(), &self.key).unwrap();
        let certificate = params.signed_by(&key, &issuer).unwrap();
        Identity {
            certificate: certificate.der().to_vec(),
            key: key.serialize_der(),
            name,
        }
    }
    fn roots(&self) -> Vec<Vec<u8>> {
        vec![self.certificate.der().to_vec()]
    }
    fn connector(&self, id: &Identity) -> QuicConnector {
        QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(id.tls(), self.roots(), &limits()).unwrap(),
            limits(),
        )
        .unwrap()
    }
}
struct Replica {
    host: ControlHost,
    owner: Option<ControlOwner>,
    server: Arc<QuicServer>,
    serving: tokio::task::JoinHandle<Result<(), WireError>>,
    pool: Arc<PeerConnectionPool>,
    driver: tokio::task::JoinHandle<()>,
    remote: QuicRemote,
}
async fn leader(replicas: &[Replica], exclude: usize) -> usize {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            for (index, replica) in replicas.iter().enumerate() {
                let progress = replica.host.progress();
                if index != exclude
                    && progress.leader == progress.node
                    && matches!(
                        call(&replica.remote, ControlRpc::Read(ControlRead::State)).await,
                        ControlReply::Read(_)
                    )
                {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("no quorum-authoritative metadata leader")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_metadata_owners_use_mutual_tls_with_majority_retry_and_scoped_operator_auth() {
    let data = tempfile::tempdir().unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let authority = BootstrapAuthority::open_or_create(
        data.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        now,
    )
    .unwrap();
    let root = RootDirectory::new(
        focal_directory::ClusterId(CLUSTER),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    let enrollment = EnrollmentRegistry::new(
        CLUSTER,
        authority.ca_certificate().to_vec(),
        4,
        EnrollmentLimits::default(),
    )
    .unwrap();
    let bootstrap = ControlBootstrap::root(&root, &enrollment).unwrap();
    let pki = Pki::new();
    let identities: Vec<_> = (1..=3)
        .map(|id| pki.issue(format!("control-{id}.focal.test"), true))
        .collect();
    let operator = pki.issue("operator.focal.test".into(), false);
    let actor = pki.issue("actor.focal.test".into(), false);
    let operator_connector = pki.connector(&operator);
    let actor_connector = pki.connector(&actor);
    let mut routes = BTreeMap::new();
    let mut pending = Vec::new();
    for (index, identity) in identities.iter().enumerate() {
        let id = index as u64 + 1;
        let mut config = NodeConfig::single(id, CLUSTER, GROUP);
        config.voters = vec![1, 2, 3];
        let allowance = budget();
        let replica = ControlReplica::open(
            ControlOptions::new(config),
            bootstrap.clone(),
            allowance.clone(),
            data.path().join(id.to_string()),
        )
        .unwrap();
        let mut config = ControlHostConfig::new(namespace());
        config.tick = Duration::from_millis(25);
        config.request_timeout = Duration::from_millis(400);
        let (host, owner, channel) =
            ControlHost::spawn(replica, RejectUnverifiedEvidence, config, allowance).unwrap();
        let peers = PeerRegistry::new(8).unwrap();
        for (source, identity) in identities.iter().enumerate() {
            peers
                .register_certificate(
                    &identity.certificate,
                    PeerGrant {
                        principal: ParticipantId::from_u128(source as u128 + 1),
                        tenants: BTreeSet::from([namespace().tenant]),
                        role: PeerRole::Node {
                            node_id: source as u64 + 1,
                        },
                    },
                )
                .unwrap();
        }
        for (identity, role) in [(&operator, PeerRole::Runtime), (&actor, PeerRole::Actor)] {
            peers
                .register_certificate(
                    &identity.certificate,
                    PeerGrant {
                        principal: ParticipantId(OPERATOR),
                        tenants: BTreeSet::from([namespace().tenant]),
                        role,
                    },
                )
                .unwrap();
        }
        let tls = server_tls(identity.tls(), pki.roots(), &limits()).unwrap();
        let server = Arc::new(
            QuicServer::bind("127.0.0.1:0".parse().unwrap(), tls, peers, limits()).unwrap(),
        );
        let serving_server = server.clone();
        let handler = host.clone();
        let serving = tokio::spawn(async move { serving_server.serve(handler).await });
        let pool = Arc::new(
            PeerConnectionPool::new(
                pki.connector(identity),
                PeerPoolLimits {
                    max_routes: 3,
                    max_connections: 3,
                    max_inflight: 4,
                    attempts: 1,
                    timeout: Duration::from_millis(150),
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
        pending.push((host, owner, channel, server, serving, pool, endpoint));
    }
    let mut replicas = Vec::new();
    for (host, owner, mut channel, server, serving, pool, endpoint) in pending {
        pool.replace_routes(1, routes.clone()).unwrap();
        let sending = pool.clone();
        // One owned send future per bounded receiver: no unbounded task fan-out.
        let driver = tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                let _ = sending.send(frame.target, &frame.request).await;
                drop(frame);
            }
        });
        let remote = operator_connector
            .connect(endpoint.address, &endpoint.server_name)
            .await
            .unwrap();
        replicas.push(Replica {
            host,
            owner: Some(owner),
            server,
            serving,
            pool,
            driver,
            remote,
        });
    }
    replicas[0].host.campaign().await.unwrap();
    let first = leader(&replicas, usize::MAX).await;
    let ControlReply::Committed(receipt) =
        call(&replicas[first].remote, ControlRpc::Submit(request(1, 0))).await
    else {
        panic!("write did not commit")
    };
    assert_eq!(receipt.revisions.root, 1);
    // Enrolled nodes discover public control state and membership through the
    // same quorum barrier, without receiving operator/control-write authority.
    let node_connector = pki.connector(&identities[0]);
    let node_remote = node_connector
        .connect(
            replicas[first].server.local_addr().unwrap(),
            &identities[first].name,
        )
        .await
        .unwrap();
    let discovery = |rpc: ControlRpc| {
        let mut packet = envelope(rpc);
        let Operation::Control { group, request } = packet.operation else {
            panic!("control envelope")
        };
        packet.operation = Operation::PeerControl { group, request };
        packet
    };
    for query in [ControlRead::State, ControlRead::Membership] {
        let result = node_remote
            .request(&discovery(ControlRpc::Read(query.clone())))
            .await
            .unwrap();
        let Response::Control { response } = result.result else {
            panic!("node discovery response")
        };
        match ControlReply::decode(&response, limits().max_frame_bytes as usize).unwrap() {
            ControlReply::Read(ControlReadResult::State(snapshot)) => {
                assert_eq!(snapshot.revisions.root, 1)
            }
            ControlReply::Read(ControlReadResult::Membership(membership)) => {
                assert_eq!(membership.node, replicas[first].host.progress().node);
                assert_eq!(membership.leader, membership.node);
                assert_eq!(membership.voters, vec![1, 2, 3]);
                assert!(membership.learners.is_empty());
                assert!(membership.applied_index >= receipt.committed_index);
                assert!(membership.term > 0);
            }
            value => panic!("unexpected discovery result: {value:?}"),
        }
    }
    let mut wrong_group = discovery(ControlRpc::Read(ControlRead::Membership));
    if let Operation::PeerControl { group, .. } = &mut wrong_group.operation {
        *group = [99; 16];
    }
    let Response::Control { response } = node_remote.request(&wrong_group).await.unwrap().result
    else {
        panic!("wrong-group response")
    };
    assert_eq!(
        ControlReply::decode(&response, 65536).unwrap(),
        ControlReply::Rejected(ControlFailure::WrongOwner)
    );
    let mut forged_write = discovery(ControlRpc::Read(ControlRead::State));
    if let Operation::PeerControl { request, .. } = &mut forged_write.operation {
        *request = vec![0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    }
    let Response::Control { response } = node_remote.request(&forged_write).await.unwrap().result
    else {
        panic!("forged-submit response")
    };
    assert_eq!(
        ControlReply::decode(&response, 65536).unwrap(),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    assert_eq!(
        node_remote
            .request(&envelope(ControlRpc::Submit(request(2, 1))))
            .await
            .unwrap()
            .result,
        Response::Error(AccessError::Unauthorized)
    );
    let actor_remote = actor_connector
        .connect(
            replicas[first].server.local_addr().unwrap(),
            &identities[first].name,
        )
        .await
        .unwrap();
    assert_eq!(
        actor_remote
            .request(&envelope(ControlRpc::Read(ControlRead::State)))
            .await
            .unwrap()
            .result,
        Response::Error(AccessError::Unauthorized)
    );
    let mut cross_namespace = envelope(ControlRpc::Read(ControlRead::State));
    cross_namespace.ledger.session = SessionId::from_u128(999);
    let Response::Control { response } = replicas[first]
        .remote
        .request(&cross_namespace)
        .await
        .unwrap()
        .result
    else {
        panic!("control rejection expected")
    };
    assert_eq!(
        ControlReply::decode(&response, 65536).unwrap(),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    let mut forged = request(2, 1);
    forged.id.client = [77; 16];
    assert_eq!(
        call(&replicas[first].remote, ControlRpc::Submit(forged)).await,
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    let isolated = replicas[first].host.progress().node;
    for replica in &replicas {
        let allowed = if replica.host.progress().node == isolated {
            BTreeMap::new()
        } else {
            routes
                .iter()
                .filter(|(id, _)| **id != isolated)
                .map(|(id, route)| (*id, route.clone()))
                .collect()
        };
        replica.pool.replace_routes(2, allowed).unwrap();
    }
    // The isolated former leader cannot serve node discovery from its stale RAM.
    let Response::Control { response } = node_remote
        .request(&discovery(ControlRpc::Read(ControlRead::Membership)))
        .await
        .unwrap()
        .result
    else {
        panic!("isolated discovery response")
    };
    assert!(matches!(
        ControlReply::decode(&response, 65536).unwrap(),
        ControlReply::Rejected(
            ControlFailure::Unavailable
                | ControlFailure::NotLeader { .. }
                | ControlFailure::NotReady
        )
    ));
    let retry = request(2, 1);
    assert!(matches!(
        call(&replicas[first].remote, ControlRpc::Submit(retry.clone())).await,
        ControlReply::Rejected(
            ControlFailure::OutcomeUnknown
                | ControlFailure::NotReady
                | ControlFailure::NotLeader { .. }
        )
    ));
    let replacement = leader(&replicas, first).await;
    let ControlReply::Committed(receipt) = call(
        &replicas[replacement].remote,
        ControlRpc::Submit(retry.clone()),
    )
    .await
    else {
        panic!("majority retry did not commit")
    };
    assert_eq!(receipt.revisions.root, 2);
    assert_eq!(
        call(&replicas[replacement].remote, ControlRpc::Submit(retry)).await,
        ControlReply::Committed(receipt)
    );
    for replica in &replicas {
        replica.pool.replace_routes(3, routes.clone()).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    for replica in &replicas {
        replica.host.stop().await.unwrap();
    }
    for replica in &mut replicas {
        replica.owner.take().unwrap().join().unwrap();
        replica.server.close();
        replica.pool.close();
    }
    for replica in replicas {
        replica.driver.await.unwrap();
        replica.serving.await.unwrap().unwrap();
    }
}
