#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Real one-port enrollment and data with durable root decisions and distinct certificates.
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::*;
use focal_enrollment::*;
use focal_memory::MemoryBudget;
use focal_model::{
    LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId,
};
use focal_node::{
    cluster::InviteIntent, control_host::*, network_listener::NetworkListener, quorum_enrollment::*,
};
use focal_wire::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [121; 16];
const GROUP: [u8; 16] = [122; 16];
const SIGNER: [u8; 16] = [123; 16];
const BOOTSTRAP_NAME: &str = "bootstrap.focal.test";
struct NoAuthority;
impl AuthorityVerifier for NoAuthority {
    fn verify_enrollment(&self, _: &NodeEnrollment) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_session_fence(&self, _: &SessionFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_replica_ready(&self, _: &ReplicaReady) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_custody(&self, _: &CustodyProof) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_delegation(&self, _: &DelegationFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn namespace() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(121),
        session: SessionId::from_u128(122),
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
fn discovery() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation: Operation::PeerControl {
            group: GROUP,
            request: ControlRpc::Read(ControlRead::Membership)
                .encode(64)
                .unwrap(),
        },
    }
}
fn connector(material: &CredentialMaterial, ca: &[u8]) -> QuicConnector {
    QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(
            TlsIdentity::from_pkcs8(
                material.certificate_chain().to_vec(),
                material.private_key_der().to_vec(),
            ),
            vec![ca.to_vec()],
            &limits(),
        )
        .unwrap(),
        limits(),
    )
    .unwrap()
}
fn raw_tls(
    ca: &[u8],
    material: Option<&CredentialMaterial>,
    protocols: Vec<Vec<u8>>,
) -> quinn::ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(ca.to_vec())).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots);
    let mut tls = match material {
        Some(material) => builder
            .with_client_auth_cert(
                material
                    .certificate_chain()
                    .iter()
                    .cloned()
                    .map(CertificateDer::from)
                    .collect(),
                PrivatePkcs8KeyDer::from(material.private_key_der().to_vec()).into(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    tls.alpn_protocols = protocols;
    quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ))
}
async fn connect_raw(
    config: quinn::ClientConfig,
    address: std::net::SocketAddr,
    name: &str,
) -> (quinn::Endpoint, quinn::Connection) {
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint.connect(address, name).unwrap().await.unwrap();
    (endpoint, connection)
}
// Deliberately represent the public serialized request to exercise hostile role
// and CSR input. Test-only secrets are never formatted or logged.
#[derive(Serialize, Deserialize)]
struct SubmittedJoin {
    schema: u16,
    invitation: [u8; 16],
    cluster: [u8; 16],
    role: EnrollmentRole,
    trust: [u8; 32],
    secret: Vec<u8>,
    request: [u8; 16],
    csr: Vec<u8>,
}
async fn edited_join(
    address: std::net::SocketAddr,
    invitation: &Invitation,
    key: &JoinKey,
    edit: impl FnOnce(&mut SubmittedJoin),
) -> JoinResponse {
    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(invitation.client_config().unwrap())
            .unwrap(),
    ));
    let (_endpoint, connection) =
        connect_raw(config, address, &invitation.trust().server_name).await;
    let request = invitation
        .request_after_quic(&connection, key, now())
        .unwrap();
    let mut submitted: SubmittedJoin = postcard::from_bytes(&request.encode().unwrap()).unwrap();
    edit(&mut submitted);
    let payload = postcard::to_stdvec(&submitted).unwrap();
    let mut header = b"FCLENR01".to_vec();
    header.extend_from_slice(&1u16.to_be_bytes());
    header.extend_from_slice(&1u16.to_be_bytes());
    header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    let (mut send, mut receive) = connection.open_bi().await.unwrap();
    send.write_all(&header).await.unwrap();
    send.write_all(&payload).await.unwrap();
    send.finish().unwrap();
    let mut header = [0; 16];
    receive.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[..8], b"FCLENR01");
    assert_eq!(&header[10..12], &2u16.to_be_bytes());
    let length = u32::from_be_bytes(header[12..16].try_into().unwrap()) as usize;
    assert!(length <= MAX_MESSAGE_BYTES);
    let mut payload = vec![0; length];
    receive.read_exact(&mut payload).await.unwrap();
    let mut end = [0];
    assert_eq!(receive.read(&mut end).await.unwrap(), None);
    postcard::from_bytes(&payload).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_port_enrolls_pinned_keys_then_requires_committed_grants_for_data() {
    let disk = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        disk.path().join("ca"),
        CLUSTER,
        vec![BOOTSTRAP_NAME.into()],
        now(),
    )
    .unwrap();
    let ca = authority.ca_certificate().to_vec();
    let enrollment_identity = authority.server_identity();
    let key = JoinKey::open_or_create(disk.path().join("founder-key"), CLUSTER).unwrap();
    let draft = FoundingEnrollmentDraft::open_or_create(
        disk.path().join("founder"),
        &authority,
        &key,
        1,
        [1; 16],
        EnrollmentLimits::default(),
        now(),
    )
    .unwrap();
    let directory = RootDirectory::new(
        focal_directory::ClusterId(CLUSTER),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    let bootstrap = ControlBootstrap::root(&directory, draft.registry()).unwrap();
    let mut control = ControlReplica::open(
        ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP)),
        bootstrap,
        budget(),
        disk.path().join("wal"),
    )
    .unwrap();
    control.drain(&NoAuthority).unwrap();
    control.campaign().unwrap();
    control.drain(&NoAuthority).unwrap();
    control.read_index(b"founding-genesis".to_vec()).unwrap();
    assert!(!control.drain(&NoAuthority).unwrap().read_states.is_empty());
    let founder_name = draft.receipt().identity.server_name.clone();
    let founder = key.complete(draft.receipt(), &ca, now()).unwrap();
    assert_ne!(
        founder.certificate_chain()[0],
        enrollment_identity.certificate_chain()[0]
    );
    let (host, owner, _outgoing) = ControlHost::spawn(
        control,
        NoAuthority,
        ControlHostConfig::new(namespace()),
        budget(),
    )
    .unwrap();
    let principal = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(SIGNER),
        tenants: BTreeSet::from([namespace().tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let router = LocalEnrollmentControl::new(host.clone(), principal).unwrap();
    let (enrollment, driver) = QuorumEnrollmentHost::create(
        authority,
        disk.path().join("signer"),
        QuorumEnrollmentConfig::new(
            host.progress().identity,
            ParticipantId(SIGNER),
            BOOTSTRAP_NAME.into(),
            BTreeSet::from([namespace().tenant]),
        ),
        budget(),
    )
    .unwrap();
    let signing = tokio::spawn(async move { driver.run(&router).await });
    let registry = PeerRegistry::new(16).unwrap();
    let network_budget = MemoryBudget::new(16 * 1024 * 1024, 0).unwrap();
    let listener = Arc::new(
        NetworkListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &founder,
            Some(&enrollment_identity),
            &ca,
            registry.clone(),
            limits(),
            network_budget.clone(),
        )
        .unwrap(),
    );
    let listener_residency = network_budget.stats().used;
    assert!(listener_residency > 0);
    let outside = listener.clone();
    let outside_data = host.clone();
    std::thread::spawn(move || {
        use std::future::Future;
        let mut serving =
            Box::pin(outside.serve(outside_data.clone(), None::<QuorumEnrollmentHost>));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            serving.as_mut().poll(&mut context),
            std::task::Poll::Ready(Err(WireError::Connection))
        ));
        drop(serving);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        assert!(matches!(
            runtime.block_on(outside.serve(outside_data, None::<QuorumEnrollmentHost>)),
            Err(WireError::Connection)
        ));
    })
    .join()
    .unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let joining = enrollment.clone();
    let data = host.clone();
    let serving = listener.clone();
    let service = tokio::spawn(async move {
        serving
            .serve(
                data,
                Some(move |request| {
                    let joining = joining.clone();
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move { JoinHandler::handle(&joining, request).await }
                }),
            )
            .await
    });
    let invitation = enrollment
        .invite(
            RequestId::from_u128(1),
            InviteIntent {
                endpoint: address.to_string(),
                role: EnrollmentRole::Node,
                lifetime_seconds: 600,
            },
        )
        .await
        .unwrap();
    let joining_key = JoinKey::open_or_create(disk.path().join("joined-key"), CLUSTER).unwrap();
    let client = EnrollmentClient::bind(
        "127.0.0.1:0".parse().unwrap(),
        TransportLimits {
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .unwrap();
    // Changing only the leaf pin keeps normal CA/name validation valid, but no
    // application request or invitation secret may reach the handler.
    let mut rogue_token = invitation.expose_token().unwrap().to_string();
    let last = rogue_token.pop().unwrap();
    rogue_token.push(if last == '0' { '1' } else { '0' });
    let rogue = Invitation::parse(&rogue_token).unwrap();
    assert!(
        client
            .redeem(address, &rogue, &joining_key, now())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let receipt = client
        .redeem(address, &invitation, &joining_key, now())
        .await
        .unwrap();
    assert_eq!(receipt.identity.node_id, Some(2));
    assert_eq!(receipt.identity.role, EnrollmentRole::Node);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        client
            .redeem(address, &invitation, &joining_key, now())
            .await
            .unwrap(),
        receipt
    );
    let credentials = joining_key.complete(&receipt, &ca, now()).unwrap();
    let connector = connector(&credentials, &ca);
    // TLS issuance alone does not grant the data path admission.
    assert!(connector.connect(address, &founder_name).await.is_err());
    let grant = enrollment
        .authorize_certificate(receipt.certificate.clone())
        .await
        .unwrap();
    assert_eq!(grant.role, PeerRole::Node { node_id: 2 });
    let fingerprint = registry
        .register_certificate(&receipt.certificate, grant)
        .unwrap();
    let remote = connector.connect(address, &founder_name).await.unwrap();
    let Response::Control { response } = remote.request(&discovery()).await.unwrap().result else {
        panic!("membership response")
    };
    let ControlReply::Read(ControlReadResult::Membership(membership)) =
        ControlReply::decode(&response, 65536).unwrap()
    else {
        panic!("membership result")
    };
    assert_eq!(membership.voters, vec![1]);
    assert!(membership.learners.is_empty()); // enrollment never silently promotes a voter
    let absent_identity = QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        raw_tls(&ca, None, vec![ALPN.to_vec()]),
        limits(),
    )
    .unwrap();
    assert!(
        absent_identity
            .connect(address, &founder_name)
            .await
            .is_err()
    );
    // If both ALPNs are offered, the server chooses its data preference and the
    // matching issued-node leaf, even if enrollment appears first on the client.
    let (_endpoint, both) = connect_raw(
        raw_tls(
            &ca,
            Some(&credentials),
            vec![ENROLLMENT_ALPN.to_vec(), ALPN.to_vec()],
        ),
        address,
        &founder_name,
    )
    .await;
    let handshake = both
        .handshake_data()
        .unwrap()
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .unwrap();
    assert_eq!(handshake.protocol.as_deref(), Some(ALPN));
    let identity = both
        .peer_identity()
        .unwrap()
        .downcast::<Vec<CertificateDer<'static>>>()
        .unwrap();
    assert_eq!(identity[0].as_ref(), founder.certificate_chain()[0]);
    both.close(0u8.into(), b"done");
    let unknown = QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        raw_tls(&ca, None, vec![b"rogue/1".to_vec()]),
        limits(),
    )
    .unwrap();
    assert!(unknown.connect(address, &founder_name).await.is_err());
    assert!(matches!(
        edited_join(address, &invitation, &joining_key, |request| request.role =
            EnrollmentRole::Client)
        .await,
        JoinResponse::Rejected(JoinFailure::Unauthorized)
    ));
    // CSR-requested CA, names and server/code-signing privileges never survive
    // a client invitation; the committed authority selects the issued identity.
    let client_invitation = enrollment
        .invite(
            RequestId::from_u128(2),
            InviteIntent {
                endpoint: address.to_string(),
                role: EnrollmentRole::Client,
                lifetime_seconds: 600,
            },
        )
        .await
        .unwrap();
    let client_key = JoinKey::open_or_create(disk.path().join("client-key"), CLUSTER).unwrap();
    let attack = KeyPair::generate().unwrap();
    let mut parameters = CertificateParams::new(vec!["admin.focal.test".into()]).unwrap();
    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    parameters.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    parameters.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::CodeSigning,
    ];
    let csr = parameters
        .serialize_request(&attack)
        .unwrap()
        .der()
        .to_vec();
    let JoinResponse::Enrolled(client_receipt) =
        edited_join(address, &client_invitation, &client_key, |request| {
            request.csr = csr
        })
        .await
    else {
        panic!("assigned client certificate")
    };
    assert_eq!(client_receipt.identity.role, EnrollmentRole::Client);
    assert_eq!(client_receipt.identity.node_id, None);
    assert!(!client_receipt.identity.server_name.contains("admin"));
    // Public registry reconstruction verifies the exact non-CA certificate and
    // permitted EKUs before returning its server-owned Actor grant.
    let client_grant = enrollment
        .authorize_certificate(client_receipt.certificate.clone())
        .await
        .unwrap();
    assert_eq!(client_grant.role, PeerRole::Actor);
    registry
        .register_certificate(&client_receipt.certificate, client_grant)
        .unwrap();
    let actor_connector = QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(
            TlsIdentity::from_pkcs8(
                vec![client_receipt.certificate.clone(), ca.clone()],
                attack.serialize_der(),
            ),
            vec![ca.clone()],
            &limits(),
        )
        .unwrap(),
        limits(),
    )
    .unwrap();
    let actor = actor_connector
        .connect(address, &founder_name)
        .await
        .unwrap();
    assert_eq!(
        actor.request(&discovery()).await.unwrap().result,
        Response::Error(AccessError::Unauthorized)
    );
    actor.close();
    // A CSR's requested ServerAuth was removed: ordinary server verification
    // rejects the otherwise valid issued client certificate at its assigned name.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(ca.clone())).unwrap();
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap();
    use rustls::client::danger::ServerCertVerifier;
    assert!(
        verifier
            .verify_server_cert(
                &CertificateDer::from(client_receipt.certificate),
                &[],
                &rustls::pki_types::ServerName::try_from(client_receipt.identity.server_name)
                    .unwrap(),
                &[],
                rustls::pki_types::UnixTime::now()
            )
            .is_err()
    );
    // Closed transient handshakes/enrollment exchanges release their owned
    // reservations; only the listener runtime and this established data
    // connection remain resident.
    tokio::time::timeout(Duration::from_secs(2), async {
        while network_budget.stats().used != listener_residency + 64 * 1024 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let pressure = network_budget
        .reserve(
            focal_memory::BudgetKind::Control,
            focal_memory::BudgetLane::Ordinary,
            network_budget.stats().limit - network_budget.stats().used,
        )
        .unwrap()
        .commit();
    assert!(connector.connect(address, &founder_name).await.is_err());
    assert!(matches!(
        remote.request(&discovery()).await.unwrap().result,
        Response::Control { .. }
    ));
    drop(pressure);
    registry.revoke(fingerprint).unwrap();
    // Revocation is checked before decoding a stream, so no error envelope with
    // a caller-controlled request identity is manufactured after rejection.
    assert!(remote.request(&discovery()).await.is_err());
    remote.close();
    listener.close();
    service.await.unwrap().unwrap();
    // close() ends ingress; the still-owned Endpoint retains its runtime charge.
    // NetworkService additionally waits for final socket/runtime release.
    assert_eq!(network_budget.stats().used, listener_residency);
    drop(listener);
    enrollment.stop().await.unwrap();
    signing.await.unwrap().unwrap();
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[test]
fn listener_bind_rejects_runtimes_missing_drivers() {
    let disk = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        disk.path().join("ca"),
        CLUSTER,
        vec![BOOTSTRAP_NAME.into()],
        now(),
    )
    .unwrap();
    let identity = authority.server_identity();
    for (io, time) in [(false, false), (true, false), (false, true)] {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        if io {
            builder.enable_io();
        }
        if time {
            builder.enable_time();
        }
        let runtime = builder.build().unwrap();
        let memory = budget();
        let result = runtime.block_on(async {
            NetworkListener::bind(
                "127.0.0.1:0".parse().unwrap(),
                &identity,
                None,
                authority.ca_certificate(),
                PeerRegistry::new(1).unwrap(),
                limits(),
                memory.clone(),
            )
        });
        assert!(matches!(result, Err(WireError::Connection)));
        assert_eq!(memory.stats().used, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrollment_connection_without_time_returns_unknown_and_closes() {
    let disk = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        disk.path().join("ca"),
        CLUSTER,
        vec![BOOTSTRAP_NAME.into()],
        now(),
    )
    .unwrap();
    let server = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(
                authority.server_identity().server_config().unwrap(),
            )
            .unwrap(),
        )),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(raw_tls(
        authority.ca_certificate(),
        None,
        vec![ENROLLMENT_ALPN.to_vec()],
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    for enable_io in [false, true] {
        let (client_connection, server_connection) = tokio::join!(
            client
                .connect(server.local_addr().unwrap(), BOOTSTRAP_NAME)
                .unwrap(),
            async { server.accept().await.unwrap().await }
        );
        let client_connection = client_connection.unwrap();
        let server_connection = server_connection.unwrap();
        let admitted = calls.clone();
        let result = std::thread::spawn(move || {
            let mut builder = tokio::runtime::Builder::new_current_thread();
            if enable_io {
                builder.enable_io();
            }
            builder
                .build()
                .unwrap()
                .block_on(serve_enrollment_connection(
                    server_connection,
                    move |_| {
                        admitted.fetch_add(1, Ordering::SeqCst);
                        async { JoinResponse::Rejected(JoinFailure::Unavailable) }
                    },
                    Duration::from_secs(1),
                ))
        })
        .join()
        .unwrap();
        assert!(matches!(result, Err(JoinTransportError::OutcomeUnknown)));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), client_connection.closed())
                .await
                .unwrap(),
            quinn::ConnectionError::ApplicationClosed(_)
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.close(0u8.into(), b"done");
    client.close(0u8.into(), b"done");
}
