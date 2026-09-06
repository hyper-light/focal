use super::*;
use crate::{
    config::{Durability, Topology},
    content_host::ContentOwner,
    custody::CustodyConfig,
};
use focal_evidence::{ContentStore, StoreLimits, UploadId};
use std::time::Duration;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn scope(revision: u64) -> CustodyScope {
    CustodyScope {
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        policy_revision: revision,
    }
}
fn placement(revision: u64, copies: &[u64]) -> EvidencePlacement {
    EvidencePlacement::verified(
        scope(revision),
        &PlacementPlan {
            voters: vec![1],
            content_copies: copies.to_vec(),
            preferred_leader: 1,
            durability: Durability::default(),
        },
        &(1..=3)
            .map(|id| NodeFacts {
                id,
                topology: Topology::default(),
                verified: true,
                eligible: true,
            })
            .collect::<Vec<_>>(),
        &Placement::default(),
    )
    .unwrap()
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(9),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap()
}
fn request(id: u128, operation: Operation) -> VerifiedRequest {
    verify_request(
        actor(),
        RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: ledger(),
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(id),
            operation,
        },
        &WireLimits::default(),
    )
    .unwrap()
}
fn artifact_request(id: u128, payload: ArtifactPayload) -> VerifiedRequest {
    request(
        id,
        Operation::Submit {
            expected_revision: None,
            command: Command::RegisterArtifact {
                artifact: NewArtifact {
                    id: ArtifactId::from_u128(id),
                    content: ArtifactContent {
                        ledger: ledger(),
                        schema: SCHEMA_MAJOR,
                        kind: "test_report".into(),
                        schema_hash: focal_evidence::test_report_schema(),
                        metadata: vec![],
                        payload,
                        producer: ParticipantId::from_u128(9),
                        receipt: None,
                        inputs: BTreeSet::new(),
                        visibility: BTreeSet::new(),
                    },
                },
            },
        },
    )
}
fn report() -> Vec<u8> {
    br#"{"passed":1,"failed":0,"skipped":0}"#.to_vec()
}
fn seal_request() -> VerifiedRequest {
    request(
        7,
        Operation::Upload(UploadRequest::Seal { upload: [7; 16] }),
    )
}
fn store_limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 4096,
        max_staging_bytes: 8192,
        max_uploads: 4,
        chunk_bytes: 4096,
        max_manifest_bytes: 4096,
    }
}
struct Fixture {
    host: ContentHost,
    owner: ContentOwner,
    reference: ContentRef,
    budget: MemoryBudget,
    directory: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
        let mut store = ContentStore::open(directory.path(), store_limits()).unwrap();
        let id = UploadId(upload_scope(&actor(), ledger(), [7; 16]));
        store
            .begin(
                id,
                ContentDomainId(ledger().tenant.0),
                ContentClass::Evidence,
                report().len() as u64,
                None,
            )
            .unwrap();
        store.append(id, 0, &report()).unwrap();
        let reference = store.seal(id).unwrap();
        let (host, owner) = ContentHost::spawn(
            store,
            CustodyConfig::new(1),
            WireLimits::default(),
            budget.clone(),
        )
        .unwrap();
        Self {
            host,
            owner,
            reference,
            budget,
            directory,
        }
    }
    async fn close(self) {
        self.host.stop().await.unwrap();
        self.owner.join().unwrap();
        assert_eq!(self.budget.stats().used, 0);
    }
}
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
            timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .unwrap()
}
fn replacement(budget: &MemoryBudget, value: EvidencePlacement) -> PlacementRow {
    let bytes = value.bytes().unwrap() * 2 + 4096;
    PlacementRow {
        placement: value,
        _allocation: budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
            .unwrap()
            .commit(),
    }
}

#[tokio::test]
async fn empty_owner_reconciles_cancellation_after_content_cas_and_fences_completed_proofs() {
    let fixture = Fixture::new();
    let (coordinator, mut driver) =
        EvidenceCoordinator::channel(fixture.host.clone(), 1, vec![], fixture.budget.clone(), 1)
            .unwrap();
    let first = placement(1, &[1]);
    driver
        .replace(None, replacement(&fixture.budget, first.clone()))
        .await
        .unwrap();
    let pool = pool();
    let old_snapshot = driver.snapshot(ledger(), RouteEpoch(1)).unwrap();
    let (send, receive) = oneshot::channel();
    coordinator
        .admit(seal_request(), JobKind::Seal(send))
        .unwrap();
    let job = driver.receiver.recv().await.unwrap();
    let completed = process(&fixture.host, &pool, 1, Ok(old_snapshot), job).await;
    let Completed::Seal {
        ref _allocation, ..
    } = completed
    else {
        panic!("seal completion");
    };
    let steady = fixture.budget.stats().used - _allocation.bytes();
    let next = placement(2, &[1]);
    {
        let update = driver.replace(
            Some(first.scope()),
            replacement(&fixture.budget, next.clone()),
        );
        let mut update = std::pin::pin!(update);
        std::future::poll_fn(|cx| {
            assert!(update.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // The content owner runs on its own thread. Stop polling the placement
        // future until that CAS commits, then cancel before map publication.
        fixture.host.check_policy(next.scope()).await.unwrap();
    }
    assert_eq!(driver.placements.get(&ledger()).unwrap().placement, first);
    assert_eq!(
        fixture.host.check_policy(first.scope()).await,
        Err(AccessError::Unavailable)
    );
    driver
        .replace(
            Some(first.scope()),
            replacement(&fixture.budget, next.clone()),
        )
        .await
        .unwrap();
    completed.send(&driver.placements);
    assert_eq!(receive.await.unwrap(), Err(AccessError::Unavailable));
    assert_eq!(fixture.budget.stats().used, steady);
    assert_eq!(
        driver
            .replace(None, replacement(&fixture.budget, placement(3, &[1])))
            .await,
        Err(AccessError::Unavailable)
    );
    driver
        .replace(Some(first.scope()), replacement(&fixture.budget, next))
        .await
        .unwrap(); // exact target retry
    drop(coordinator);
    drop(driver);
    pool.close();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_mailbox_replaces_policy_while_all_transfer_slots_are_blocked() {
    let fixture = Fixture::new();
    let first = placement(1, &[1, 2]);
    fixture
        .host
        .install_policy(first.custody_policy())
        .await
        .unwrap();
    let (coordinator, driver) = EvidenceCoordinator::channel(
        fixture.host.clone(),
        1,
        vec![first.clone()],
        fixture.budget.clone(),
        1,
    )
    .unwrap();
    let pool = pool();
    let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: blackhole.local_addr().unwrap(),
                server_name: "localhost".into(),
            },
        )]),
    )
    .unwrap();
    let exercise = async {
        let next = placement(2, &[1]);
        {
            let request = coordinator.attest(artifact_request(
                10,
                ArtifactPayload::Content(fixture.reference.clone()),
            ));
            let mut request = std::pin::pin!(request);
            let mut datagram = [0; 2048];
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result=blackhole.recv(&mut datagram)=>{assert!(result.unwrap()>0);}
                    _=&mut request=>panic!("custody unexpectedly completed without a peer"),
                }
            })
            .await
            .unwrap();
            tokio::time::timeout(
                Duration::from_secs(1),
                coordinator.replace_placement(Some(first.scope()), next.clone()),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(request.as_mut().now_or_never().is_none());
            assert_eq!(
                fixture.host.check_policy(first.scope()).await,
                Err(AccessError::Unavailable)
            );
            pool.close();
            assert!(request.await.is_err());
        }
        let checked = coordinator
            .attest(artifact_request(11, ArtifactPayload::Inline(report())))
            .await
            .unwrap();
        assert_eq!(checked.witness.scope, next.scope());
        drop(checked);
        drop(coordinator);
    };
    let (result, ()) = tokio::join!(driver.run(&pool), exercise);
    result.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn required_new_copies_apply_to_existing_content_and_restart_retains_bytes() {
    let fixture = Fixture::new();
    let (coordinator, driver) =
        EvidenceCoordinator::channel(fixture.host.clone(), 1, vec![], fixture.budget.clone(), 1)
            .unwrap();
    let pool = pool();
    let next = placement(2, &[1, 2]);
    let exercise = async {
        let first = placement(1, &[1]);
        coordinator
            .replace_placement(None, first.clone())
            .await
            .unwrap();
        let witness = coordinator
            .attest(artifact_request(
                12,
                ArtifactPayload::Content(fixture.reference.clone()),
            ))
            .await
            .unwrap();
        assert_eq!(
            coordinator.seal(seal_request()).await.unwrap(),
            fixture.reference
        );
        coordinator
            .replace_placement(Some(first.scope()), next.clone())
            .await
            .unwrap();
        assert!(
            witness
                .witness
                .validate(&witness.request, next.scope(), &[1])
                .is_err()
        );
        drop(witness);
        assert_eq!(
            coordinator.seal(seal_request()).await,
            Err(AccessError::OutcomeUnknown)
        );
        assert!(
            coordinator
                .attest(artifact_request(
                    13,
                    ArtifactPayload::Content(fixture.reference.clone())
                ))
                .await
                .is_err()
        );
        assert_eq!(
            fixture
                .host
                .read_bytes(next.scope(), fixture.reference.clone(), 4096)
                .await
                .unwrap()
                .value(),
            &report()
        );
        drop(coordinator);
    };
    let (result, ()) = tokio::join!(driver.run(&pool), exercise);
    result.unwrap();
    pool.close();
    fixture.host.stop().await.unwrap();
    fixture.owner.join().unwrap();
    assert_eq!(fixture.budget.stats().used, 0);
    let (reopened, owner) = ContentHost::spawn(
        ContentStore::open(fixture.directory.path(), store_limits()).unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        fixture.budget.clone(),
    )
    .unwrap();
    reopened
        .replace_policy(None, next.custody_policy())
        .await
        .unwrap();
    let (recovered_coordinator, mut recovered_driver) =
        EvidenceCoordinator::channel(reopened.clone(), 1, vec![], fixture.budget.clone(), 1)
            .unwrap();
    recovered_driver
        .replace(None, replacement(&fixture.budget, next.clone()))
        .await
        .unwrap();
    assert_eq!(
        recovered_driver
            .placements
            .get(&ledger())
            .unwrap()
            .placement,
        next
    );
    drop(recovered_coordinator);
    drop(recovered_driver);
    assert_eq!(
        reopened
            .read_bytes(next.scope(), fixture.reference, 4096)
            .await
            .unwrap()
            .value(),
        &report()
    );
    reopened.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(fixture.budget.stats().used, 0);
}

#[tokio::test]
async fn replacement_admission_is_bounded_and_dropped_replies_retry_exactly() {
    let fixture = Fixture::new();
    let first = placement(1, &[1]);
    fixture
        .host
        .install_policy(first.custody_policy())
        .await
        .unwrap();
    let (coordinator, mut driver) = EvidenceCoordinator::channel(
        fixture.host.clone(),
        1,
        vec![first.clone()],
        fixture.budget.clone(),
        1,
    )
    .unwrap();
    let next = placement(2, &[1]);
    let steady = fixture.budget.stats().used;
    let pressure = fixture
        .budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            fixture.budget.stats().limit - steady,
        )
        .unwrap();
    assert_eq!(
        coordinator
            .replace_placement(Some(first.scope()), next.clone())
            .await,
        Err(AccessError::Capacity)
    );
    assert!(driver.control.is_empty());
    drop(pressure);
    fixture.host.check_policy(first.scope()).await.unwrap();
    {
        let update = coordinator.replace_placement(Some(first.scope()), next.clone());
        let mut update = std::pin::pin!(update);
        std::future::poll_fn(|cx| {
            assert!(update.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    let pending = fixture.budget.stats().used;
    assert_eq!(
        coordinator
            .replace_placement(Some(first.scope()), next.clone())
            .await,
        Err(AccessError::Capacity)
    );
    assert_eq!(fixture.budget.stats().used, pending);
    let update = driver.control.recv().await.unwrap();
    let result = driver.replace(update.expected, update.row).await;
    assert!(result.is_ok());
    assert!(update.reply.send(result).is_err());
    assert_eq!(fixture.budget.stats().used, steady);
    let retry = coordinator.replace_placement(Some(first.scope()), next.clone());
    let apply = async {
        let update = driver.control.recv().await.unwrap();
        let result = driver.replace(update.expected, update.row).await;
        update.reply.send(result).unwrap();
    };
    let (result, ()) = tokio::join!(retry, apply);
    result.unwrap();
    assert_eq!(driver.placements.get(&ledger()).unwrap().placement, next);
    assert_eq!(fixture.budget.stats().used, steady);
    drop(coordinator);
    drop(driver);
    fixture.close().await;
}

#[test]
fn managed_artifact_custody_transfer_namespace_binds_the_full_stream_key() {
    let legacy = artifact_request(77, ArtifactPayload::Inline(report()));
    assert_eq!(
        custody_request_id(&legacy).unwrap(),
        RequestId::from_u128(77)
    );
    let (_, mut request) = legacy.into_parts();
    let Operation::Submit {
        expected_revision,
        command,
    } = request.operation
    else {
        panic!("submit")
    };
    request.protocol = MANAGED_PROTOCOL_VERSION;
    let mut key = ManagedRequestKey {
        stream: RequestStreamIdentity {
            cluster: [1; 16],
            ledger: ledger(),
            principal: actor().principal(),
            slot: 0,
            generation: 1,
        },
        ordinal: 1,
        id: request.request_id,
    };
    let operation = ManagedOperation::Submit {
        expected_revision,
        command,
    };
    request.operation = Operation::Managed {
        key,
        operation: operation.clone(),
    };
    let original = verify_request(actor(), request.clone(), &WireLimits::default()).unwrap();
    let transfer = custody_request_id(&original).unwrap();
    assert_eq!(custody_request_id(&original).unwrap(), transfer);
    key.stream.slot = 1;
    request.operation = Operation::Managed { key, operation };
    let other = verify_request(actor(), request, &WireLimits::default()).unwrap();
    assert_ne!(custody_request_id(&other).unwrap(), transfer);
    assert!(artifact(&original.request().operation).is_some());
}
