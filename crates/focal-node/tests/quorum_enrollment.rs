#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::*;
use focal_enrollment::*;
use focal_memory::MemoryBudget;
use focal_model::{LedgerId, ParticipantId, RequestId, SessionId, TenantId};
use focal_node::{cluster::InviteIntent, control_host::*, quorum_enrollment::*};
use focal_wire::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [61; 16];
const GROUP: [u8; 16] = [62; 16];
const OPERATOR: [u8; 16] = [63; 16];
// This verifier rejects all evidence-bearing directory operations. Tests never
// invent a positive attestation: region registration and partition sealing need
// authenticated operator authority, while enrollment uses genuine CSR/CA checks.
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
    fn verify_custody(&self, _: &CustodyProof) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_delegation(&self, _: &DelegationFence) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
fn namespace() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(61),
        session: SessionId::from_u128(62),
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(OPERATOR),
        tenants: [namespace().tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn root_bootstrap(authority: &BootstrapAuthority) -> ControlBootstrap {
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
    ControlBootstrap::root(&root, &enrollment).unwrap()
}
struct Rig {
    directories: Vec<tempfile::TempDir>,
    hosts: Vec<ControlHost>,
    owners: Vec<ControlOwner>,
    routers: Vec<tokio::task::JoinHandle<()>>,
    isolated: Arc<AtomicU64>,
    bootstrap: ControlBootstrap,
    group: [u8; 16],
}
impl Rig {
    fn new(bootstrap: ControlBootstrap, group: [u8; 16]) -> Self {
        let mut value = Self {
            directories: (0..3).map(|_| tempfile::tempdir().unwrap()).collect(),
            hosts: vec![],
            owners: vec![],
            routers: vec![],
            isolated: Arc::new(AtomicU64::new(0)),
            bootstrap,
            group,
        };
        value.start();
        value
    }
    fn start(&mut self) {
        let mut channels = Vec::new();
        for (offset, directory) in self.directories.iter().enumerate() {
            let id = offset as u64 + 1;
            let mut config = NodeConfig::single(id, CLUSTER, self.group);
            config.voters = vec![1, 2, 3];
            let allowance = budget();
            let replica = ControlReplica::open(
                ControlOptions::new(config),
                self.bootstrap.clone(),
                allowance.clone(),
                directory.path(),
            )
            .unwrap();
            let mut config = ControlHostConfig::new(namespace());
            config.tick = Duration::from_millis(25);
            config.request_timeout = Duration::from_millis(350);
            let (host, owner, channel) =
                ControlHost::spawn(replica, RejectUnverifiedEvidence, config, allowance).unwrap();
            self.hosts.push(host);
            self.owners.push(owner);
            channels.push((id, channel));
        }
        for (from, mut channel) in channels {
            let hosts = self.hosts.clone();
            let isolated = self.isolated.clone();
            self.routers.push(tokio::spawn(async move {
                while let Some(frame) = channel.recv().await {
                    let excluded = isolated.load(Ordering::SeqCst);
                    if excluded == from || excluded == frame.target {
                        continue;
                    }
                    let Some(target) = hosts.get(frame.target.saturating_sub(1) as usize) else {
                        continue;
                    };
                    let verified = verify_request(
                        peer(PeerRole::Node { node_id: from }),
                        frame.request.clone(),
                        &ControlHost::wire_limits(),
                    )
                    .unwrap();
                    let _ = target.handle(&verified).await;
                    drop(frame);
                }
            }));
        }
    }
    async fn leader(&self, exclude: u64) -> usize {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for (index, host) in self.hosts.iter().enumerate() {
                    let status = host.progress();
                    if status.node != exclude
                        && status.leader == status.node
                        && host
                            .read(
                                peer(PeerRole::Runtime),
                                RequestId::from_u128(900),
                                ControlRead::State,
                            )
                            .await
                            .is_ok()
                    {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn state(&self, index: usize) -> ControlSnapshot {
        let ControlReadResult::State(snapshot) = self.hosts[index]
            .read(
                peer(PeerRole::Runtime),
                RequestId::from_u128(901),
                ControlRead::State,
            )
            .await
            .unwrap()
        else {
            panic!("state expected")
        };
        snapshot
    }
    async fn stop(&mut self) {
        for host in &self.hosts {
            host.stop().await.unwrap();
        }
        for owner in self.owners.drain(..) {
            owner.join().unwrap();
        }
        for router in self.routers.drain(..) {
            router.await.unwrap();
        }
        self.hosts.clear();
    }
}
fn registry(snapshot: &ControlSnapshot) -> EnrollmentRegistry {
    let ControlBootstrap::Root { enrollment, .. } = &snapshot.state else {
        panic!("root expected")
    };
    EnrollmentRegistry::restore(enrollment, CLUSTER, EnrollmentLimits::default()).unwrap()
}
fn join_request(
    material: &CredentialMaterial,
    invitation: &Invitation,
    key: &JoinKey,
) -> JoinRequest {
    let mut client = rustls::ClientConnection::new(
        Arc::new(invitation.client_config().unwrap()),
        rustls::pki_types::ServerName::try_from(invitation.trust().server_name.clone()).unwrap(),
    )
    .unwrap();
    let mut server =
        rustls::ServerConnection::new(Arc::new(material.server_config().unwrap())).unwrap();
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            server.process_new_packets().unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return invitation.request_after_tls(&client, key, now()).unwrap();
        }
    }
    panic!("TLS handshake did not finish")
}

#[derive(Clone)]
struct Router {
    hosts: Vec<ControlHost>,
    selected: Arc<AtomicU64>,
    fault: Arc<AtomicU64>,
}
impl Router {
    fn new(rig: &Rig, leader: usize) -> Self {
        Self {
            hosts: rig.hosts.clone(),
            selected: Arc::new(AtomicU64::new(leader as u64)),
            fault: Arc::new(AtomicU64::new(0)),
        }
    }
    fn host(&self) -> &ControlHost {
        &self.hosts[self.selected.load(Ordering::SeqCst) as usize]
    }
}
impl EnrollmentControl for Router {
    fn identity(&self) -> ControlIdentity {
        self.host().progress().identity
    }
    fn principal(&self) -> ParticipantId {
        ParticipantId(OPERATOR)
    }
    fn read_state(&self, id: RequestId) -> ControlFuture<'_, ControlSnapshot> {
        Box::pin(async move {
            match self
                .host()
                .read(peer(PeerRole::Runtime), id, ControlRead::State)
                .await?
            {
                ControlReadResult::State(state) => Ok(state),
                _ => Err(ControlFailure::Invalid),
            }
        })
    }
    fn submit(&self, request: ControlRequest) -> ControlFuture<'_, ControlReceipt> {
        Box::pin(async move {
            let mode = self.fault.swap(0, Ordering::SeqCst);
            if mode == 1 {
                return Err(ControlFailure::OutcomeUnknown);
            }
            let receipt = self.host().submit(peer(PeerRole::Runtime), request).await?;
            if mode == 2 {
                return Err(ControlFailure::OutcomeUnknown);
            }
            Ok(receipt)
        })
    }
}
fn authority(path: &std::path::Path) -> BootstrapAuthority {
    BootstrapAuthority::open_or_create(
        path,
        CLUSTER,
        vec!["enroll.focal.internal".to_owned()],
        now(),
    )
    .unwrap()
}
fn signer_config(identity: ControlIdentity) -> QuorumEnrollmentConfig {
    let mut config = QuorumEnrollmentConfig::new(
        identity,
        ParticipantId(OPERATOR),
        "enroll.focal.internal".into(),
        [namespace().tenant].into_iter().collect(),
    );
    config.request_timeout = Duration::from_secs(2);
    config
}
fn intent(role: EnrollmentRole) -> InviteIntent {
    InviteIntent {
        endpoint: "127.0.0.1:7444".into(),
        role,
        lifetime_seconds: 600,
    }
}
fn serve(
    driver: QuorumEnrollmentDriver,
    router: Router,
) -> tokio::task::JoinHandle<Result<(), QuorumEnrollmentError>> {
    tokio::spawn(async move { driver.run(&router).await })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_decisions_survive_lost_responses_leader_change_and_signer_restart() {
    let private = tempfile::tempdir().unwrap();
    let ca_path = private.path().join("authority");
    let staging = private.path().join("signer");
    let ca = authority(&ca_path);
    let material = ca.server_identity();
    let mut rig = Rig::new(root_bootstrap(&ca), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let first = rig.leader(0).await;
    let router = Router::new(&rig, first);
    let config = signer_config(router.identity());
    let (host, driver) =
        QuorumEnrollmentHost::create(ca, &staging, config.clone(), budget()).unwrap();
    let task = serve(driver, router.clone());
    let request = RequestId::from_u128(1);
    let node_intent = intent(EnrollmentRole::Node);
    // A lost send leaves durable exact proposal custody but releases no token.
    router.fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        host.invite(request, node_intent.clone()).await,
        Err(QuorumEnrollmentError::Control(
            ControlFailure::OutcomeUnknown
        ))
    ));
    assert_eq!(registry(&rig.state(first).await).revision(), 0);
    host.stop().await.unwrap();
    task.await.unwrap().unwrap();
    // Isolate the original leader and retry after the majority elects another.
    rig.isolated.store(first as u64 + 1, Ordering::SeqCst);
    let replacement = rig.leader(first as u64 + 1).await;
    router.selected.store(replacement as u64, Ordering::SeqCst);
    let (host, driver) =
        QuorumEnrollmentHost::open(authority(&ca_path), &staging, config.clone(), budget())
            .unwrap();
    let task = serve(driver, router.clone());
    let invitation = host.invite(request, node_intent.clone()).await.unwrap();
    let token = invitation.expose_token().unwrap();
    assert_eq!(registry(&rig.state(replacement).await).revision(), 1);
    assert!(matches!(
        host.invite(request, intent(EnrollmentRole::Client)).await,
        Err(QuorumEnrollmentError::IntentConflict)
    ));
    assert!(
        host.invite(request, node_intent.clone())
            .await
            .unwrap()
            .expose_token()
            .unwrap()
            == token
    );
    let keys = tempfile::tempdir().unwrap();
    let key = JoinKey::open_or_create(keys.path().join("node"), CLUSTER).unwrap();
    let join = join_request(&material, &invitation, &key);
    // Actual quorum commit succeeds, but the signer sees an unknown outcome.
    router.fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        host.redeem(join.clone()).await,
        Err(QuorumEnrollmentError::Control(
            ControlFailure::OutcomeUnknown
        ))
    ));
    let committed = registry(&rig.state(replacement).await)
        .enrollments()
        .next()
        .unwrap()
        .clone();
    assert_eq!(committed.identity.node_id, Some(4));
    assert_eq!(committed.identity.role, EnrollmentRole::Node);
    host.stop().await.unwrap();
    task.await.unwrap().unwrap();
    rig.isolated.store(0, Ordering::SeqCst);
    rig.stop().await;
    rig.start();
    let leader = rig.leader(0).await;
    let router = Router::new(&rig, leader);
    let (host, driver) =
        QuorumEnrollmentHost::open(authority(&ca_path), &staging, config.clone(), budget())
            .unwrap();
    let task = serve(driver, router.clone());
    let recovered = host.redeem(join.clone()).await.unwrap();
    assert_eq!(recovered, committed);
    assert_eq!(registry(&rig.state(leader).await).enrollments().count(), 1);
    assert!(
        host.invite(request, node_intent)
            .await
            .unwrap()
            .expose_token()
            .unwrap()
            == token
    );
    let grant = host
        .authorize_certificate(recovered.certificate.clone())
        .await
        .unwrap();
    assert_eq!(grant.role, PeerRole::Node { node_id: 4 });
    assert_eq!(grant.tenants, config.tenants);
    // Competing CSR/request identities cannot reuse the committed invitation.
    let other = JoinKey::open_or_create(keys.path().join("other"), CLUSTER).unwrap();
    let other_join = join_request(&material, &invitation, &other);
    assert!(matches!(
        host.redeem(other_join).await,
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Used))
    ));
    // Client certificates receive Actor authority only, never Runtime or voter.
    router.fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        host.invite(RequestId::from_u128(2), intent(EnrollmentRole::Client))
            .await,
        Err(QuorumEnrollmentError::Control(
            ControlFailure::OutcomeUnknown
        ))
    ));
    // Another authenticated operator wins the comparison while this signer's
    // invitation is unresolved. Exact retry rebases the same persisted token.
    let public = registry(&rig.state(leader).await);
    let command = public.prepare_revoke(invitation.id(), now()).unwrap();
    let external = [64; 16];
    rig.hosts[leader]
        .submit(
            AuthenticatedPeer::local(PeerGrant {
                principal: ParticipantId(external),
                tenants: [namespace().tenant].into_iter().collect(),
                role: PeerRole::Runtime,
            })
            .unwrap(),
            ControlRequest {
                id: ControlRequestId {
                    client: external,
                    sequence: 1,
                },
                acknowledged_through: 0,
                command: ControlCommand::Enrollment(command),
            },
        )
        .await
        .unwrap();
    let actor_invite = host
        .invite(RequestId::from_u128(2), intent(EnrollmentRole::Client))
        .await
        .unwrap();
    let actor_key = JoinKey::open_or_create(keys.path().join("actor"), CLUSTER).unwrap();
    let actor = host
        .redeem(join_request(&material, &actor_invite, &actor_key))
        .await
        .unwrap();
    assert_eq!(actor.identity.node_id, None);
    let grant = host.authorize_certificate(actor.certificate).await.unwrap();
    assert_eq!(grant.role, PeerRole::Actor);
    host.revoke(invitation.id()).await.unwrap();
    host.revoke(invitation.id()).await.unwrap();
    assert!(matches!(
        host.authorize_certificate(recovered.certificate).await,
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Revoked))
    ));
    assert!(matches!(
        host.redeem(join).await,
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Revoked))
    ));
    // New admission after expiry fails even though the CSR was constructed
    // while the invitation was valid.
    let mut short = intent(EnrollmentRole::Client);
    short.lifetime_seconds = 2;
    let expiring = host.invite(RequestId::from_u128(3), short).await.unwrap();
    let expired_key = JoinKey::open_or_create(keys.path().join("expired"), CLUSTER).unwrap();
    let expired_join = join_request(&material, &expiring, &expired_key);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(matches!(
        host.redeem(expired_join).await,
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Expired))
    ));
    // Losing an entire secret directory must not regenerate a second token for
    // its stable operator request. Its durable parent marker detects the loss.
    let slot = blake3::derive_key(
        "focal.quorum-enrollment.invite-request.v1",
        &RequestId::from_u128(2).0,
    );
    std::fs::remove_dir_all(staging.join(blake3::Hash::from_bytes(slot).to_hex().as_str()))
        .unwrap();
    assert!(matches!(
        host.invite(RequestId::from_u128(2), intent(EnrollmentRole::Client))
            .await,
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Corrupt))
    ));
    assert!(matches!(
        task.await.unwrap(),
        Err(QuorumEnrollmentError::Stopped)
    ));
    // Lost initialized journal cannot create a second sequence authority.
    std::fs::remove_file(staging.join("journal.bin")).unwrap();
    assert!(matches!(
        QuorumEnrollmentHost::open(authority(&ca_path), &staging, config.clone(), budget()),
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Corrupt))
    ));
    assert!(matches!(
        QuorumEnrollmentHost::create(authority(&ca_path), &staging, config, budget()),
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Corrupt))
    ));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_and_wrong_authority_cannot_release_new_enrollment() {
    let private = tempfile::tempdir().unwrap();
    let ca = authority(&private.path().join("authority"));
    let mut rig = Rig::new(root_bootstrap(&ca), GROUP);
    rig.hosts[0].campaign().await.unwrap();
    let leader = rig.leader(0).await;
    let router = Router::new(&rig, leader);
    let config = signer_config(router.identity());
    let (host, driver) =
        QuorumEnrollmentHost::create(ca, private.path().join("signer"), config.clone(), budget())
            .unwrap();
    let task = serve(driver, router.clone());
    rig.isolated.store(leader as u64 + 1, Ordering::SeqCst);
    assert!(
        host.invite(RequestId::from_u128(8), intent(EnrollmentRole::Node))
            .await
            .is_err()
    );
    host.stop().await.unwrap();
    task.await.unwrap().unwrap();
    let new_leader = rig.leader(leader as u64 + 1).await;
    assert_eq!(registry(&rig.state(new_leader).await).revision(), 0);
    router.selected.store(new_leader as u64, Ordering::SeqCst);
    // Same cluster identifier with another private CA is not the root authority.
    let rogue = authority(&private.path().join("rogue"));
    let (host, driver) =
        QuorumEnrollmentHost::create(rogue, private.path().join("rogue-signer"), config, budget())
            .unwrap();
    let task = serve(driver, router);
    assert!(matches!(
        host.invite(RequestId::from_u128(9), intent(EnrollmentRole::Node))
            .await,
        Err(QuorumEnrollmentError::Identity)
    ));
    host.stop().await.unwrap();
    task.await.unwrap().unwrap();
    rig.stop().await;
}

#[tokio::test]
async fn queue_and_state_allocations_are_bounded_and_released_on_cancellation() {
    let private = tempfile::tempdir().unwrap();
    let ca = authority(&private.path().join("authority"));
    let root = ControlIdentity {
        cluster: focal_directory::ClusterId(CLUSTER),
        group: GROUP,
        scope: ControlScope::Root,
        genesis: [1; 32],
    };
    let mut config = signer_config(root);
    config.queue_items = 1;
    let allowance = budget();
    let (host, driver) =
        QuorumEnrollmentHost::create(ca, private.path().join("signer"), config, allowance.clone())
            .unwrap();
    let mut first = Box::pin(host.invite(RequestId::from_u128(1), intent(EnrollmentRole::Node)));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(first.as_mut(), &mut context).is_pending());
    let held = allowance.stats().used;
    assert!(matches!(
        host.invite(RequestId::from_u128(2), intent(EnrollmentRole::Node))
            .await,
        Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Capacity))
    ));
    assert_eq!(allowance.stats().used, held);
    drop(first);
    assert_eq!(allowance.stats().used, held); // admitted work outlives its caller
    drop(driver);
    assert_eq!(allowance.stats().used, 0);
}
