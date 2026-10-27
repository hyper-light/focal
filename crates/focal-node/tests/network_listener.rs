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
use focal_wire::net::Sender;
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

#[path = "support/owners.rs"]
mod owners;

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
/// A connector of a node's credential `material`.
fn connector(material: &CredentialMaterial, ca: &[u8]) -> QuicConnector {
    QuicConnector::bind_standalone(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(
            TlsIdentity::from_pkcs8(
                material.certificate_chain().to_vec(),
                material.private_key_der().to_vec(),
            ),
            vec![ca.to_vec()],
            &limits(),
            Sender::Node,
        )
        .unwrap(),
        limits(),
    )
    .unwrap()
}
/// A client TLS offering `protocols`, presenting a node's credential `material` or nothing (a
/// joiner's, which the data protocol refuses).
fn raw_tls(ca: &[u8], material: Option<&CredentialMaterial>, protocols: Vec<Vec<u8>>) -> ClientTls {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(ca.to_vec())).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(focal_wire::crypto_provider())
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
    let role = if material.is_some() {
        Sender::Node
    } else {
        Sender::Joiner
    };
    ClientTls::from_config(tls, role)
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
    let request = invitation.request(key, now()).unwrap();
    let mut submitted: SubmittedJoin = postcard::from_bytes(&request.encode().unwrap()).unwrap();
    edit(&mut submitted);
    let payload = postcard::to_stdvec(&submitted).unwrap();
    // The joiner's own endpoint, its TLS the invitation's: the message goes
    // only once the server's leaf has passed the invitation's pins.
    let connector = QuicConnector::bind_standalone(
        "127.0.0.1:0".parse().unwrap(),
        ClientTls::from_config(invitation.client_config().unwrap(), Sender::Joiner),
        limits(),
    )
    .unwrap();
    let link = connector
        .enrollment(
            address,
            &invitation.trust().server_name,
            invitation.trust().pins(),
        )
        .await
        .unwrap();
    let answer = link
        .ask(JOIN, payload, Duration::from_secs(2))
        .await
        .unwrap();
    postcard::from_bytes(&answer).unwrap()
}

#[test]
fn one_port_enrolls_pinned_keys_then_requires_committed_grants_for_data() {
    focal_rt::block_on(async move {
        let disk = tempfile::tempdir().unwrap();
        let authority = BootstrapAuthority::open_or_create(
            disk.path().join("ca"),
            &focal_seal::test_keys_beside(disk.path().join("ca")).unwrap(),
            CLUSTER,
            vec![BOOTSTRAP_NAME.into()],
            now(),
        )
        .unwrap();
        let ca = authority.ca_certificate().to_vec();
        // The issuers as the authority holds them at genesis, and its server
        // certificate, for what the test needs after the authority moves into
        // its driver.
        let issuers = authority.issuers().unwrap();
        let authority_server = authority.server_certificate().to_vec();
        let enrollment_identity = authority.server_identity();
        let key = JoinKey::open_or_create(
            disk.path().join("founder-key"),
            &focal_seal::test_keys_beside(disk.path().join("founder-key")).unwrap(),
            CLUSTER,
        )
        .unwrap();
        let draft = FoundingEnrollmentDraft::open_or_create(
            disk.path().join("founder"),
            &focal_seal::test_keys_beside(disk.path().join("founder")).unwrap(),
            &authority,
            &key,
            1,
            [1; 16],
            EnrollmentLimits::default(),
            0,
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
        let founder = key
            .complete(
                draft.receipt(),
                authority.issuers().unwrap().trusted(),
                now(),
            )
            .unwrap();
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
            &focal_seal::test_keys_beside(disk.path().join("signer")).unwrap(),
            QuorumEnrollmentConfig::new(
                host.progress().identity,
                ParticipantId(SIGNER),
                BOOTSTRAP_NAME.into(),
                BTreeSet::from([namespace().tenant]),
            ),
            budget(),
        )
        .unwrap();
        let signing = focal_rt::spawn(async move { driver.run(&router).await }).unwrap();
        let registry = PeerRegistry::new(16).unwrap();
        // The listener's budget carries a completion reserve, as the node's does:
        // nodes' bodies are admitted on the completion lane, participants' on the
        // ordinary one, so that control traffic is never starved by participants.
        let network_budget = MemoryBudget::new(16 * 1024 * 1024, 4 * 1024 * 1024).unwrap();
        let listener = std::rc::Rc::new(
            NetworkListener::bind(
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                &founder,
                Some(&enrollment_identity),
                std::slice::from_ref(&ca),
                registry.clone(),
                limits(),
                network_budget.clone(),
            )
            .unwrap(),
        );
        let listener_residency = network_budget.stats().used;
        assert!(listener_residency > 0);
        // Off its shard the listener cannot be served at all: it owns its
        // shard's endpoint, which no other thread may reach (`!Send`), so the
        // compiler refuses what a runtime check once guarded.
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let joining = enrollment.clone();
        let data = host.clone();
        let serving = listener.clone();
        let service = focal_rt::spawn(async move {
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
        })
        .unwrap();
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
        let joining_key = JoinKey::open_or_create(
            disk.path().join("joined-key"),
            &focal_seal::test_keys_beside(disk.path().join("joined-key")).unwrap(),
            CLUSTER,
        )
        .unwrap();
        let client = EnrollmentClient::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportLimits {
                timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap();
        // Changing only the leaf pin keeps normal CA/name validation valid, but no
        // application request or invitation secret may reach the handler. The
        // pin is the server certificate's fingerprint, encoded once in the
        // token (24 §11): its last digit is flipped where it stands.
        let mut rogue_token = invitation.expose_token().unwrap().to_string();
        let pin: String = server_fingerprint(authority_server.as_slice())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(rogue_token.matches(&pin).count(), 1, "{rogue_token}");
        let at = rogue_token.find(&pin).unwrap() + pin.len() - 1;
        let last = rogue_token.remove(at);
        rogue_token.insert(at, if last == '0' { '1' } else { '0' });
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
        let credentials = joining_key
            .complete(&receipt, issuers.trusted(), now())
            .unwrap();
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
        let Response::Control { response } = remote.request(&discovery()).await.unwrap().result
        else {
            panic!("membership response")
        };
        let ControlReply::Read(ControlReadResult::Membership(membership)) =
            ControlReply::decode(&response, 65536).unwrap()
        else {
            panic!("membership result")
        };
        assert_eq!(membership.voters, vec![1]);
        assert!(membership.learners.is_empty()); // enrollment never silently promotes a voter
        let absent_identity = QuicConnector::bind_standalone(
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
        // matching issued-node leaf, even if enrollment appears first on the client:
        // the client verifies the leaf by the founder's own name, and the data
        // protocol answers.
        let both = QuicConnector::bind_standalone(
            "127.0.0.1:0".parse().unwrap(),
            raw_tls(
                &ca,
                Some(&credentials),
                vec![ENROLLMENT_ALPN.to_vec(), ALPN.to_vec()],
            ),
            limits(),
        )
        .unwrap();
        let both = both.connect(address, &founder_name).await.unwrap();
        assert!(matches!(
            both.request(&discovery()).await.unwrap().result,
            Response::Control { .. }
        ));
        both.close().unwrap();
        let unknown = QuicConnector::bind_standalone(
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
        let client_key = JoinKey::open_or_create(
            disk.path().join("client-key"),
            &focal_seal::test_keys_beside(disk.path().join("client-key")).unwrap(),
            CLUSTER,
        )
        .unwrap();
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
        let actor_connector = QuicConnector::bind_standalone(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(
                TlsIdentity::from_pkcs8(
                    vec![client_receipt.certificate.clone(), ca.clone()],
                    attack.serialize_der(),
                ),
                vec![ca.clone()],
                &limits(),
                Sender::Participant,
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
        actor.close().unwrap();
        // A CSR's requested ServerAuth was removed: ordinary server verification
        // rejects the otherwise valid issued client certificate at its assigned name.
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(ca.clone())).unwrap();
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            roots,
            focal_wire::crypto_provider(),
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
        // reservations; only the listener's own memory and this established data
        // connection remain charged: its socket (charged at bind) and what its
        // admission holds for the one connection left — the connection's window
        // and the receive chunks the endpoint pooled for its life — with nothing
        // pending. Charged to the listener's admission changes: each release it
        // still owes pairs with a place it took, so the changes made so far bound
        // them. An enrollment exchange ends without a change, its join timeout
        // bounding it inside the frozen window.
        let taken = listener.admission().await.changes;
        owners::counted_async(
            async || vec![listener.admission().await.changes],
            taken,
            Duration::from_millis(5),
            async {
                loop {
                    let admission = listener.admission().await;
                    if admission.pending == 0
                        && admission.connections == 1
                        && admission.pooled > 0
                        && network_budget.stats().used == listener_residency + admission.bytes
                    {
                        break;
                    }
                    focal_rt::time::sleep(Duration::from_millis(5))
                        .await
                        .unwrap();
                }
            },
        )
        .await
        .unwrap();
        // A participant holds a connection through the pressure that follows.
        let actor = actor_connector
            .connect(address, &founder_name)
            .await
            .unwrap();
        // Its handshake's own reservations go back once it has ended, after the
        // connection is up: filled before, the lane had room again for the dial
        // below (Linux CI, bus-replicated 7a79f0d7: the dial refused at a full
        // lane was admitted). And a connection closed before keeps its window's
        // grant while it drains (RFC 9000 §10.2), given back only as the
        // endpoint drops it: given back after a fill, it funded the dial below
        // (macOS CI on b6cf8e33 and 7a381f1f, 4 of 180 runs on a loaded Linux
        // host, each admitting the dial into exactly one connection's grant,
        // 14,268 and 11,440 bytes, a closed connection gave back after the
        // fill). Filled once the listener holds the two connections, nothing
        // pending and nothing closed.
        let joined = listener.admission().await.changes;
        owners::counted_async(
            async || vec![listener.admission().await.changes],
            joined,
            Duration::from_millis(5),
            async {
                loop {
                    let admission = listener.admission().await;
                    if admission.pending == 0
                        && admission.connections == 2
                        && admission.held == admission.connections
                        && network_budget.stats().used == listener_residency + admission.bytes
                    {
                        break;
                    }
                    focal_rt::time::sleep(Duration::from_millis(5))
                        .await
                        .unwrap();
                }
            },
        )
        .await
        .unwrap();
        // The ordinary lane is filled, and filled again with what the refused
        // dial below gives back once its handshake has ended.
        let fill = |pressure: &mut Option<focal_memory::Allocation>| {
            let stats = network_budget.stats();
            let room = stats.limit - stats.completion_reserve - stats.ordinary_used;
            let mut more = network_budget
                .reserve(
                    focal_memory::BudgetKind::Control,
                    focal_memory::BudgetLane::Ordinary,
                    room,
                )
                .unwrap()
                .commit();
            match pressure {
                Some(held) => held.absorb(&mut more).unwrap(),
                None => *pressure = Some(more),
            }
        };
        // The lane is filled, then the listener's receive ring brought to its least with the lane
        // kept full. What the ring grew by is the endpoint's own, charged to this lane as it grew,
        // and given back as quiet drains shrink it (hyper-raft docs/transport.md §4aa): given back
        // after a fill, it funded what the full lane was to refuse, a dial or a participant's body
        // (Linux CI on compare/work-wait 35a2e5c0; 7 of 40 runs on a loaded Linux host, each with
        // the ring shrunk between the fill and the refused request). At a full lane the ring cannot
        // grow (a growth is reserved first), so it only shrinks. The node's requests, funded from
        // the completion reserve and served at the full lane, are the drains that shrink it: a ring
        // of `n` buffers shrinks to the most any of a spell of `n` drains wanted, each request a
        // drain at least; under load a drain takes more than one message and the spell begins
        // again (5 of 78 runs on a loaded Linux host found the ring grown after a pass count of its
        // size squared), so the drive is charged to the ring's shrinks, each of which the lane is
        // filled again behind, and ends with the ring at its least.
        let mut pressure = None;
        fill(&mut pressure);
        let grown = listener.admission().await;
        owners::counted_async(
            async || vec![listener.admission().await.ring_shrunk],
            u64::try_from(grown.ring_buffers).unwrap(),
            Duration::from_millis(5),
            async {
                while listener.admission().await.ring_charged > 0 {
                    assert!(matches!(
                        remote.request(&discovery()).await.unwrap().result,
                        Response::Control { .. }
                    ));
                    fill(&mut pressure);
                }
            },
        )
        .await
        .unwrap();
        let quiet = listener.admission().await;
        assert_eq!(quiet.ring_charged, 0, "the ring at its least: {quiet:?}");
        fill(&mut pressure);
        // The ordinary lane is full: no connection is admitted (a dial of its
        // own, as the route's is held and shared), and a participant's body is
        // refused before any of it is allocated — the stream is reset as
        // capacity, the connection kept. A node's body is funded from the
        // completion reserve and served.
        assert!(
            connector
                .connect_alone(address, &founder_name)
                .await
                .is_err()
        );
        // Filled again once everything the refused dial held is given back:
        // nothing pending, and the budget holding the listener's residency,
        // what it admitted and the pressure alone. Filled once nothing was
        // pending, the dial's last release could come after the fill and fund
        // the body below (Linux CI, a run of ab/short-copy).
        let settled = listener.admission().await.changes;
        let held = pressure.as_ref().map_or(0, focal_memory::Allocation::bytes);
        owners::counted_async(
            async || vec![listener.admission().await.changes],
            settled,
            Duration::from_millis(5),
            async {
                loop {
                    let admission = listener.admission().await;
                    // The refused dial gone whole: no handshake pending, the
                    // listener back to its two connections, and none closed.
                    if admission.pending == 0
                        && admission.connections == 2
                        && admission.held == admission.connections
                        && network_budget.stats().used
                            == listener_residency + admission.bytes + held
                    {
                        break;
                    }
                    focal_rt::time::sleep(Duration::from_millis(5))
                        .await
                        .unwrap();
                }
            },
        )
        .await
        .unwrap();
        // Nothing has grown the ring since the lane was first filled: a growth is reserved first,
        // and the full lane refuses it.
        let quiet = listener.admission().await;
        assert_eq!(quiet.ring_charged, 0, "the ring at its least: {quiet:?}");
        fill(&mut pressure);
        let refused = actor.request(&discovery()).await;
        assert!(
            refused.is_err(),
            "a participant's body at a full lane: {:?}; listener {:?}",
            refused.map(|reply| reply.result),
            listener.admission().await
        );
        assert!(matches!(
            remote.request(&discovery()).await.unwrap().result,
            Response::Control { .. }
        ));
        drop(pressure);
        assert_eq!(
            actor.request(&discovery()).await.unwrap().result,
            Response::Error(AccessError::Unauthorized)
        );
        actor.close().unwrap();
        registry.revoke(fingerprint).unwrap();
        // Revocation is checked before decoding a stream, so no error envelope with
        // a caller-controlled request identity is manufactured after rejection.
        assert!(remote.request(&discovery()).await.is_err());
        remote.close().unwrap();
        listener.close();
        service.await.unwrap().unwrap();
        // close() ends the listener's owner, and its endpoint with it: every
        // byte the listener held — its socket and the receive chunks it pooled
        // for its life — is given back once its service has ended.
        assert_eq!(network_budget.stats().used, 0);
        drop(listener);
        enrollment.stop().await.unwrap();
        signing.await.unwrap().unwrap();
        host.stop().await.unwrap();
        owner.join().unwrap();
    })
    .unwrap()
}

#[test]
fn off_a_shard_a_listener_is_refused_and_holds_nothing() {
    let disk = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        disk.path().join("ca"),
        &focal_seal::test_keys_beside(disk.path().join("ca")).unwrap(),
        CLUSTER,
        vec![BOOTSTRAP_NAME.into()],
        now(),
    )
    .unwrap();
    let identity = authority.server_identity();
    let memory = budget();
    let result = NetworkListener::bind(
        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
        &identity,
        None,
        &[authority.ca_certificate().to_vec()],
        PeerRegistry::new(1).unwrap(),
        limits(),
        memory.clone(),
    );
    assert!(matches!(result, Err(WireError::Connection)));
    assert_eq!(memory.stats().used, 0);
}
