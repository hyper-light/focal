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
    artifact_with_schema(id, payload, focal_evidence::test_report_schema())
}
fn artifact_with_schema(
    id: u128,
    payload: ArtifactPayload,
    schema_hash: ContentHash,
) -> VerifiedRequest {
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
                        schema_hash,
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
        Self::with_bytes(&report())
    }
    fn with_bytes(bytes: &[u8]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
        let mut store = ContentStore::open(directory.path(), store_limits()).unwrap();
        let id = UploadId(upload_scope(&actor(), ledger(), [7; 16]));
        store
            .begin(
                id,
                ContentDomainId(ledger().tenant.0),
                ContentClass::Evidence,
                bytes.len() as u64,
                None,
            )
            .unwrap();
        store.append(id, 0, bytes).unwrap();
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

#[test]
fn error_diagnostics_are_shape_checked_for_inline_and_durable_content() {
    focal_rt::block_on(async move {
        let diagnostic = br#"{"code":"tool_unavailable","message":"The tool could not run","details":"No work result was produced"}"#;
        let fixture = Fixture::with_bytes(diagnostic);
        let (coordinator, driver) =
            EvidenceCoordinator::channel(fixture.host.clone(), 1, vec![], fixture.budget.clone(), 1)
                .unwrap();
        let pool = pool();
        let exercise = async {
            let placement = placement(1, &[1]);
            coordinator
                .replace_placement(None, placement.clone())
                .await
                .unwrap();
            for payload in [
                ArtifactPayload::Inline(diagnostic.to_vec()),
                ArtifactPayload::Content(fixture.reference.clone()),
            ] {
                let witnessed = coordinator
                    .attest(artifact_with_schema(
                        88,
                        payload,
                        focal_evidence::error_report_schema(),
                    ), far())
                    .await
                    .unwrap();
                let attestation = witnessed
                    .witness
                    .validate(&witnessed.request, placement.scope(), &[1])
                    .unwrap();
                assert!(attestation.durable && attestation.schema_valid);
                assert_eq!(attestation.custody_revision, 1);
            }
            for payload in [
                ArtifactPayload::Inline(diagnostic.to_vec()),
                ArtifactPayload::Content(fixture.reference.clone()),
            ] {
                assert!(matches!(
                    coordinator
                        .attest(artifact_with_schema(
                            89,
                            payload,
                            focal_evidence::test_report_schema()
                        ), far())
                        .await,
                    Err(AccessError::InvalidRequest)
                ));
            }
            assert!(matches!(
                coordinator
                    .attest(artifact_with_schema(
                        90,
                        ArtifactPayload::Inline(diagnostic.to_vec()),
                        ContentHash::default()
                    ), far())
                    .await,
                Err(AccessError::UnsupportedOperation)
            ));
            assert!(matches!(
                coordinator
                    .attest(artifact_with_schema(
                        91,
                        ArtifactPayload::Inline(br#"{"code":"x","message":" "}"#.to_vec()),
                        focal_evidence::error_report_schema()
                    ), far())
                    .await,
                Err(AccessError::InvalidRequest)
            ));
            let mut oversized = fixture.reference.clone();
            oversized.length = 65537;
            assert!(matches!(
                coordinator
                    .attest(artifact_with_schema(
                        92,
                        ArtifactPayload::Content(oversized),
                        focal_evidence::error_report_schema()
                    ), far())
                    .await,
                Err(AccessError::Capacity)
            ));
            drop(coordinator);
        };
        let (result, ()) = hyper_rt::combine::join2(driver.run(&pool, None), exercise).await;
        result.unwrap();
        pool.close();
        fixture.close().await;
    }).unwrap()
}
fn pool() -> PeerConnectionPool {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = identity.cert.der().to_vec();
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![cert.clone()], identity.signing_key.serialize_der()),
        vec![cert],
        &WireLimits::default(),
        focal_wire::net::Sender::Node,
    )
    .unwrap();
    let limits = WireLimits::default();
    let budget = focal_wire::endpoint_budget(&limits, None).unwrap();
    let connector =
        QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, limits, budget).unwrap();
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

#[test]
fn empty_owner_reconciles_cancellation_after_content_cas_and_fences_completed_proofs() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let (coordinator, mut driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![],
            fixture.budget.clone(),
            1,
        )
        .unwrap();
        let first = placement(1, &[1]);
        driver
            .replace(None, replacement(&fixture.budget, first.clone()))
            .await
            .unwrap();
        let pool = pool();
        let old_snapshot = driver.snapshot(ledger(), RouteEpoch(1)).unwrap();
        let (send, receive) = hyper_rt::sync::oneshot().unwrap();
        coordinator
            .admit(seal_request(), JobKind::Seal(send), far())
            .await
            .unwrap();
        let job = driver.receiver.recv().await.unwrap();
        let (not_asked, given_up) = (AtomicU64::new(0), AtomicU64::new(0));
        let health = CopyHealth {
            liveness: None,
            not_asked: &not_asked,
            given_up: &given_up,
        };
        let completed = process(&fixture.host, &pool, &health, 1, Ok(old_snapshot), job).await;
        let Completed::Seal {
            ref _allocation, ..
        } = completed
        else {
            panic!("seal completion");
        };
        let steady = fixture.budget.stats().used - _allocation.bytes();
        let next = placement(2, &[1]);
        // A replacement cut between its content CAS and its map publication (the
        // owner's future dropped, or the process, once the content owner on its
        // own thread has committed) leaves content ahead of the map, and nothing
        // else: the cut is produced as its effects, not as a poll the content
        // owner might already have answered.
        fixture
            .host
            .replace_policy(Some(first.scope()), next.custody_policy())
            .await
            .unwrap();
        fixture.host.check_policy(next.scope()).await.unwrap();
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
        let _ = completed.send(&driver.placements);
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
    })
    .unwrap()
}

#[test]
fn control_mailbox_replaces_policy_while_all_transfer_slots_are_blocked() {
    focal_rt::block_on(async move {
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
        let blackhole =
            hyper_rt::udp::UdpSocket::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                .unwrap();
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
        let exercise = async {
            let next = placement(2, &[1]);
            {
                let request = coordinator.attest(
                    artifact_request(10, ArtifactPayload::Content(fixture.reference.clone())),
                    far(),
                );
                let mut request = std::pin::pin!(request);
                let mut datagram = [0; 2048];
                // No owner runs either: the request reaches the path, and the
                // placement is replaced while it stays stuck there, or the wait
                // is wedged — bounded by the frozen window alone
                // (`crate::test_waits`).
                crate::test_waits::charged(
                    Vec::new,
                    Duration::from_secs(2),
                    Duration::from_millis(10),
                    async {
                        match hyper_rt::combine::race2(
                            blackhole.recv_from(&mut datagram),
                            request.as_mut(),
                        )
                        .await
                        {
                            hyper_rt::combine::Either::First(received) => {
                                assert!(received.unwrap().0 > 0);
                            }
                            hyper_rt::combine::Either::Second(_) => {
                                panic!("custody unexpectedly completed without a peer")
                            }
                        }
                    },
                )
                .await
                .unwrap();
                crate::test_waits::charged(
                    Vec::new,
                    Duration::from_secs(1),
                    Duration::from_millis(10),
                    coordinator.replace_placement(Some(first.scope()), next.clone()),
                )
                .await
                .unwrap()
                .unwrap();
                // Polled with the task's own waker: a hyper-rt future refuses a foreign one.
                assert!(
                    std::future::poll_fn(|cx| std::task::Poll::Ready(
                        request.as_mut().poll(cx).is_pending()
                    ))
                    .await
                );
                assert_eq!(
                    fixture.host.check_policy(first.scope()).await,
                    Err(AccessError::Unavailable)
                );
                pool.close();
                assert!(request.await.is_err());
            }
            let checked = coordinator
                .attest(
                    artifact_request(11, ArtifactPayload::Inline(report())),
                    far(),
                )
                .await
                .unwrap();
            assert_eq!(checked.witness.scope, next.scope());
            drop(checked);
            drop(coordinator);
        };
        let (result, ()) = hyper_rt::combine::join2(driver.run(&pool, None), exercise).await;
        result.unwrap();
        fixture.close().await;
    })
    .unwrap()
}

#[test]
fn required_new_copies_apply_to_existing_content_and_restart_retains_bytes() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let (coordinator, driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![],
            fixture.budget.clone(),
            1,
        )
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
                .attest(
                    artifact_request(12, ArtifactPayload::Content(fixture.reference.clone())),
                    far(),
                )
                .await
                .unwrap();
            assert_eq!(
                coordinator.seal(seal_request(), far()).await.unwrap(),
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
                coordinator.seal(seal_request(), far()).await,
                Err(AccessError::OutcomeUnknown)
            );
            assert!(
                coordinator
                    .attest(
                        artifact_request(13, ArtifactPayload::Content(fixture.reference.clone())),
                        far()
                    )
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
        let (result, ()) = hyper_rt::combine::join2(driver.run(&pool, None), exercise).await;
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
    })
    .unwrap()
}

#[test]
fn replacement_admission_is_bounded_and_dropped_replies_retry_exactly() {
    focal_rt::block_on(async move {
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
        assert!(matches!(driver.control.try_recv(), Ok(None)));
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
        let (result, ()) = hyper_rt::combine::join2(retry, apply).await;
        result.unwrap();
        assert_eq!(driver.placements.get(&ledger()).unwrap().placement, next);
        assert_eq!(fixture.budget.stats().used, steady);
        drop(coordinator);
        drop(driver);
        fixture.close().await;
    })
    .unwrap()
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

/// One artifact-bearing native frame: a work output for slot zero under a
/// current receipt, with its payload inline, encoded exactly as a client would.
fn native_work_frame(request: u128, payload: &[u8]) -> VerifiedRequest {
    use focal_core::native::{fixtures as fx, input_codec};
    let principal = ParticipantId::from_u128(9);
    let parent = focal_model::lifecycle::evidence::Parent {
        ledger: ledger(),
        claim: ClaimId::from_u128(31),
        issuer: ParticipantId::from_u128(8),
        holder: principal,
        receipt: ReceiptFence {
            receipt: ReceiptId::from_u128(41),
            epoch: 1,
        },
        status: ClaimStatus::Received,
        local_complete: false,
        latest_response: None,
        next_cycle: 1,
    };
    let (artifact, _) = fx::work_artifact(ledger(), 51, &parent, 0, payload).unwrap();
    let input = fx::submit_work(
        RequestKey {
            principal,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(request),
        },
        fx::binding(ledger(), 31),
        0,
        artifact,
    );
    let plan = input_codec::EncodingPlan::prepare(
        input_codec::InputFrame::Request {
            ledger: ledger(),
            profile: focal_ledger::NativeContentProfile::AuthoredV1,
            input: &input,
        },
        input_codec::EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 28,
        },
    )
    .unwrap();
    let mut frame = vec![0; plan.quote().bytes];
    plan.write_into(&mut frame).unwrap();
    verify_request(
        AuthenticatedPeer::local(PeerGrant {
            principal,
            tenants: BTreeSet::from([ledger().tenant]),
            role: PeerRole::Actor,
        })
        .unwrap(),
        RequestEnvelope {
            protocol: NATIVE_PROTOCOL_VERSION,
            ledger: ledger(),
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(request),
            operation: Operation::Native { frame },
        },
        &WireLimits::default(),
    )
    .unwrap()
}

#[test]
fn native_inline_payloads_are_sealed_locally_and_replicated_to_every_required_copy() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let (coordinator, driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![],
            fixture.budget.clone(),
            1,
        )
        .unwrap();
        let pool = pool();
        let exercise = async {
            // This node is the only required copy: the payload is sealed under
            // the placement and readable as durable content afterwards.
            let local = placement(1, &[1]);
            coordinator
                .replace_placement(None, local.clone())
                .await
                .unwrap();
            let evidenced = coordinator
                .attest_native(native_work_frame(21, &report()), far())
                .await
                .unwrap();
            let payload = evidenced.evidence.custody().payload();
            let reference = ContentRef {
                domain: payload.domain,
                root: payload.root,
                length: payload.length,
                class: payload.class,
            };
            assert_eq!(payload.length, report().len() as u64);
            assert_eq!(
                fixture
                    .host
                    .read_bytes(local.scope(), reference.clone(), 4096)
                    .await
                    .unwrap()
                    .value(),
                &report()
            );
            drop(evidenced);
            // A second required copy that cannot be reached refuses the frame
            // before admission: custody is never claimed on one node's word.
            let wider = placement(2, &[1, 2]);
            coordinator
                .replace_placement(Some(local.scope()), wider.clone())
                .await
                .unwrap();
            assert!(
                coordinator
                    .attest_native(native_work_frame(22, &report()), far())
                    .await
                    .is_err()
            );
            drop(coordinator);
        };
        let (result, ()) = hyper_rt::combine::join2(driver.run(&pool, None), exercise).await;
        result.unwrap();
        pool.close();
        fixture.close().await;
    })
    .unwrap()
}

/// Participants' artifact jobs past what the driver runs and queues wait at the coordinator's
/// door and are each served in turn, none refused: 500 agents' work at 50 claims a second met a
/// queue of four that refused the rest (`access: Capacity`, CI 39848959332).
#[test]
fn artifact_jobs_past_the_queue_wait_at_the_door_and_are_each_served() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let (coordinator, driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![],
            fixture.budget.clone(),
            1,
        )
        .unwrap();
        let pool = pool();
        let exercise = async {
            coordinator
                .replace_placement(None, placement(1, &[1]))
                .await
                .unwrap();
            const JOBS: u128 = 8;
            let mut jobs = focal_rt::Concurrent::new(JOBS as usize).unwrap();
            for id in 0..JOBS {
                let coordinator = &coordinator;
                jobs.push(async move {
                    coordinator
                        .attest_native(native_work_frame(40 + id, &report()), far())
                        .await
                })
                .unwrap();
            }
            let mut served = 0;
            while let Some(evidenced) = jobs.join_next().await {
                evidenced.unwrap();
                served += 1;
            }
            assert_eq!(served, JOBS);
            drop(jobs);
            let door = coordinator.door_stats();
            assert!(door.held >= 1, "{door:?}");
            assert_eq!(door.timed_out, 0, "{door:?}");
            drop(coordinator);
        };
        let (result, ()) = hyper_rt::combine::join2(driver.run(&pool, None), exercise).await;
        result.unwrap();
        pool.close();
        fixture.close().await;
    })
    .unwrap()
}

/// A job held at the door no longer than its request's time: with the queue full and nothing
/// taking from it, the job is refused as its deadline passes, and counted.
#[test]
fn a_job_held_past_its_request_time_is_refused_and_counted() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let (coordinator, driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![],
            fixture.budget.clone(),
            1,
        )
        .unwrap();
        let (send, _receive) = hyper_rt::sync::oneshot().unwrap();
        coordinator
            .admit(seal_request(), JobKind::Seal(send), far())
            .await
            .unwrap();
        let (send, _receive) = hyper_rt::sync::oneshot().unwrap();
        let soon =
            focal_rt::time::Instant::now().saturating_add(std::time::Duration::from_millis(20));
        assert_eq!(
            coordinator
                .admit(seal_request(), JobKind::Seal(send), soon)
                .await,
            Err(AccessError::Capacity)
        );
        assert_eq!(
            coordinator.door_stats(),
            EvidenceDoorStats {
                held: 1,
                timed_out: 1,
                window: 1,
                copies_unavailable: 0,
                copies_given_up: 0,
                refused_memory: 0,
                refused_same: 0,
                jobs_capacity: 0,
                jobs_unavailable: 0,
                jobs_unknown: 0,
                jobs_other: 0,
            }
        );
        // The job the queue took goes with the driver that never ran, and its charge with it.
        drop(driver);
        drop(coordinator);
        fixture.close().await;
    })
    .unwrap()
}

/// More calls at once than the content owner's queue holds are each served: those past it wait at
/// its door for the owner to take one before them, where they were refused for capacity (1,612
/// artifact jobs refused so in a work run with a content copy isolated; compare/work-faults on
/// cae493e6). The owner is held while twice its queue's calls are asked, so those past the queue
/// are held whatever the threads' timing; let go, it serves every one.
#[test]
fn calls_past_the_content_owners_queue_wait_at_its_door_and_are_each_served() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let release = fixture.host.hold_owner().unwrap();
        let calls = 2 * 32;
        let mut asked = focal_rt::Concurrent::new(calls).unwrap();
        for _ in 0..calls {
            asked.push(fixture.host.disk_stats()).ok().unwrap();
        }
        // Each call is asked once: queued, or held at the door.
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(asked.poll_next(cx).is_pending()))
                .await
        );
        let (held, _) = fixture.host.door_stats();
        assert!(
            held >= 31,
            "{held} held of {calls}, past a queue of 32 and the held owner's"
        );
        release.send(()).unwrap();
        let mut served = 0;
        while let Some(result) = asked.join_next().await {
            result.unwrap();
            served += 1;
        }
        assert_eq!(served, calls);
        drop(asked);
        assert_eq!(fixture.host.door_stats().1, 0);
        fixture.close().await;
    })
    .unwrap()
}

/// A required copy the liveness view holds dead is not asked: its job is refused at once as
/// `Unavailable`, an outcome its participant knows and asks again on, and counted, with no dial to
/// the copy and nothing waiting out the pool's request time (one node of three killed left 406
/// submissions of unknown outcome, compare/work-faults 2026-10-22). Held alive again, it is asked;
/// held dead while it is waited for, the wait ends and the job is refused.
#[test]
fn a_copy_the_liveness_view_holds_dead_is_not_asked_and_its_job_is_refused_at_once() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let held = placement(1, &[1, 2]);
        fixture
            .host
            .install_policy(held.custody_policy())
            .await
            .unwrap();
        let (coordinator, driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![held],
            fixture.budget.clone(),
            1,
        )
        .unwrap();
        let pool = pool();
        let blackhole =
            hyper_rt::udp::UdpSocket::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                .unwrap();
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
        let (liveness, publisher) = crate::liveness::LivenessHandle::channel(
            &fixture.budget,
            crate::liveness::LivenessConfig::default(),
            1,
            ledger(),
        )
        .unwrap();
        let held_as = |status| {
            let mut view = liveness.view();
            view.members.insert(
                2,
                crate::liveness::MemberView {
                    generation: 1,
                    status,
                    incarnation: 1,
                    confirmed: true,
                    decided_at_ms: 0,
                    suspected_by: None,
                    confirmations: 0,
                    extensions: 0,
                    extended_ms: 0,
                    timeout_ms: 0,
                    rtt_ms: None,
                },
            );
            view
        };
        publisher.publish_view(held_as(crate::liveness::MemberStatus::Dead));
        let exercise = async {
            assert_eq!(
                coordinator.seal(seal_request(), far()).await,
                Err(AccessError::Unavailable)
            );
            assert_eq!(coordinator.door_stats().copies_unavailable, 1);
            assert_eq!(pool.stats().dials, 0);
            publisher.publish_view(held_as(crate::liveness::MemberStatus::Alive));
            {
                let request = coordinator.attest(
                    artifact_request(10, ArtifactPayload::Content(fixture.reference.clone())),
                    far(),
                );
                let mut request = std::pin::pin!(request);
                let mut datagram = [0; 2048];
                // The copy's address hears the job's dial, or the wait is wedged: bounded by the
                // frozen window alone (`crate::test_waits`).
                crate::test_waits::charged(
                    Vec::new,
                    Duration::from_secs(2),
                    Duration::from_millis(10),
                    async {
                        match hyper_rt::combine::race2(
                            blackhole.recv_from(&mut datagram),
                            request.as_mut(),
                        )
                        .await
                        {
                            hyper_rt::combine::Either::First(received) => {
                                assert!(received.unwrap().0 > 0);
                            }
                            hyper_rt::combine::Either::Second(_) => {
                                panic!("custody completed without the copy")
                            }
                        }
                    },
                )
                .await
                .unwrap();
                assert_eq!(coordinator.door_stats().copies_unavailable, 1);
                // The copy dies while the job waits for it: the job is refused once the view
                // says so, its wait bounded by the frozen window alone.
                publisher.publish_view(held_as(crate::liveness::MemberStatus::Dead));
                let refused = crate::test_waits::charged(
                    Vec::new,
                    Duration::from_secs(2),
                    Duration::from_millis(10),
                    request.as_mut(),
                )
                .await
                .unwrap();
                assert!(matches!(refused, Err(AccessError::Unavailable)));
                let stats = coordinator.door_stats();
                assert_eq!((stats.copies_unavailable, stats.copies_given_up), (1, 1));
                // Both jobs' participants were answered `Unavailable`, and counted so.
                assert_eq!((stats.jobs_unavailable, stats.jobs_capacity), (2, 0));
            }
            drop(coordinator);
        };
        let (result, ()) =
            hyper_rt::combine::join2(driver.run(&pool, Some(&liveness)), exercise).await;
        result.unwrap();
        drop(liveness);
        drop(publisher);
        fixture.close().await;
    })
    .unwrap()
}

/// Custody receipts and the obligation they answer (doc 04 §7, R8): sealing
/// records this node's own receipt, an unreachable required copy holds
/// none and is named missing, a placement change does not inherit older
/// receipts, and a receipt survives the store's reopen.
#[test]
fn replication_records_a_receipt_per_verified_copy_and_the_obligation_reads_them() {
    focal_rt::block_on(async move {
        let fixture = Fixture::new();
        let (coordinator, driver) = EvidenceCoordinator::channel(
            fixture.host.clone(),
            1,
            vec![],
            fixture.budget.clone(),
            1,
        )
        .unwrap();
        let pool = pool();
        let exercise = async {
            let first = placement(1, &[1]);
            coordinator
                .replace_placement(None, first.clone())
                .await
                .unwrap();
            assert!(
                fixture
                    .host
                    .receipt(first.scope(), fixture.reference.root, 1)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                coordinator.seal(seal_request(), far()).await.unwrap(),
                fixture.reference
            );
            let receipt = fixture
                .host
                .receipt(first.scope(), fixture.reference.root, 1)
                .await
                .unwrap()
                .expect("this node's own receipt");
            assert_eq!(receipt.node, 1);
            assert_eq!(receipt.policy_revision, 1);
            assert_eq!(receipt.length, fixture.reference.length);
            let (obligation, _) = coordinator
                .obligation(seal_request(), fixture.reference.clone(), far())
                .await
                .unwrap();
            assert_eq!(obligation.required, BTreeSet::from([1]));
            assert_eq!(obligation.held, BTreeSet::from([1]));
            assert!(obligation.satisfied());
            // A second required copy nobody can reach: the older receipt does
            // not count at the new scope, this node re-verifies its own store,
            // and the copy is named missing.
            let next = placement(2, &[1, 2]);
            coordinator
                .replace_placement(Some(first.scope()), next.clone())
                .await
                .unwrap();
            let (obligation, _) = coordinator
                .obligation(seal_request(), fixture.reference.clone(), far())
                .await
                .unwrap();
            assert_eq!(obligation.required, BTreeSet::from([1, 2]));
            assert_eq!(obligation.held, BTreeSet::from([1]));
            assert!(!obligation.satisfied());
            assert_eq!(obligation.missing().collect::<Vec<_>>(), vec![2]);
            assert!(
                fixture
                    .host
                    .receipt(next.scope(), fixture.reference.root, 2)
                    .await
                    .unwrap()
                    .is_none()
            );
            let renewed = fixture
                .host
                .receipt(next.scope(), fixture.reference.root, 1)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(renewed.policy_revision, 2);
        };
        match hyper_rt::combine::race2(exercise, driver.run(&pool, None)).await {
            hyper_rt::combine::Either::First(()) => {}
            hyper_rt::combine::Either::Second(result) => panic!("driver stopped: {result:?}"),
        }
        let root = fixture.reference.root;
        fixture.host.stop().await.unwrap();
        fixture.owner.join().unwrap();
        assert_eq!(fixture.budget.stats().used, 0);
        let reopened = ContentStore::open(fixture.directory.path(), store_limits()).unwrap();
        let kept = reopened
            .custody_receipt(ledger(), root, 1)
            .unwrap()
            .unwrap();
        assert_eq!(kept.policy_revision, 2);
        assert!(
            reopened
                .custody_receipt(ledger(), root, 2)
                .unwrap()
                .is_none()
        );
    })
    .unwrap()
}

/// Only a phase begun against an artifact names one for its custody check.
#[test]
fn a_phase_beginning_frame_names_its_artifact_and_other_frames_do_not() {
    use focal_core::native::{EvaluationKey, EvaluationTarget, fixtures as fx, input_codec};
    let limits = focal_ledger::NativeSessionLimits::standard(ContentDomainId(ledger().tenant.0));
    let encode = |input: &focal_core::native::NativeInput| -> Vec<u8> {
        let plan = input_codec::EncodingPlan::prepare(
            input_codec::InputFrame::Request {
                ledger: ledger(),
                profile: focal_ledger::NativeContentProfile::AuthoredV1,
                input,
            },
            input_codec::EncodingLimits {
                bytes: 1 << 20,
                visits: 1 << 28,
            },
        )
        .unwrap();
        let mut frame = vec![0; plan.quote().bytes];
        plan.write_into(&mut frame).unwrap();
        frame
    };
    let request = RequestKey {
        principal: ParticipantId::from_u128(9),
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(23),
    };
    let work = fx::begin_work(
        request,
        fx::binding(ledger(), 31),
        EvaluationKey {
            claim: ClaimId::from_u128(31),
            validation: ValidationId::from_u128(61),
            target: EvaluationTarget::Work {
                response: TestamentId::from_u128(71),
                slot: 0,
                artifact: ArtifactId::from_u128(51),
            },
            generation: 1,
        },
        fx::binding(ledger(), 31),
    );
    assert_eq!(
        crate::native_ingress::evaluation_artifact_of_frame(&limits, &encode(&work)).unwrap(),
        Some(ArtifactId::from_u128(51))
    );
    let increment = focal_core::native::NativeInput {
        request,
        command: focal_core::native::NativeCommand::BeginIncrement {
            claim: fx::binding(ledger(), 31),
            key: EvaluationKey {
                claim: ClaimId::from_u128(31),
                validation: ValidationId::from_u128(61),
                target: EvaluationTarget::Increment {
                    artifact: ArtifactId::from_u128(52),
                },
                generation: 1,
            },
            expected: fx::binding(ledger(), 31),
        },
    };
    assert_eq!(
        crate::native_ingress::evaluation_artifact_of_frame(&limits, &encode(&increment)).unwrap(),
        Some(ArtifactId::from_u128(52))
    );
    let posted = fx::post(request, fx::binding(ledger(), 31));
    assert_eq!(
        crate::native_ingress::evaluation_artifact_of_frame(&limits, &encode(&posted)).unwrap(),
        None
    );
    let Operation::Native { frame } = native_work_frame(24, &report()).request().operation.clone()
    else {
        panic!("a native frame");
    };
    assert_eq!(
        crate::native_ingress::evaluation_artifact_of_frame(&limits, &frame).unwrap(),
        None
    );
    assert!(crate::native_ingress::evaluates_artifact(14));
    assert!(crate::native_ingress::evaluates_artifact(18));
    assert!(!crate::native_ingress::evaluates_artifact(6));
}

/// What [`striped`] was asked to move, as the peer of a test saw it.
#[derive(Default)]
struct Moved {
    /// Every chunk that was begun, in the order it was.
    begun: Vec<u32>,
    /// The chunks that were answered.
    answered: BTreeSet<u32>,
    flying: usize,
    most: usize,
}
impl Moved {
    fn begin(&mut self, index: u32) {
        self.begun.push(index);
        self.flying += 1;
        self.most = self.most.max(self.flying);
    }
    fn end(&mut self, index: u32, answered: bool) {
        self.flying -= 1;
        if answered {
            self.answered.insert(index);
        }
    }
}

#[test]
fn a_transfer_keeps_as_many_chunks_in_flight_as_the_path_holds() {
    focal_rt::block_on(async move {
        let moved = std::cell::RefCell::new(Moved::default());
        let result = striped(
            || 8,
            8,
            5..300,
            |_| false,
            async |index: u32| {
                moved.borrow_mut().begin(index);
                if index == 5 {
                    // The first is answered last: the others go on beside it.
                    while moved.borrow().answered.len() < 294 {
                        hyper_rt::futures::yield_now().await;
                    }
                } else {
                    hyper_rt::futures::yield_now().await;
                }
                moved.borrow_mut().end(index, true);
                Ok(())
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        let moved = moved.into_inner();
        assert_eq!(moved.most, 8);
        assert_eq!(moved.begun.len(), 295);
        assert_eq!(moved.answered, (5..300).collect());
    })
    .unwrap()
}

/// A chunk the copy holds is not moved (the audit's F50): the held are
/// passed over in their place, and what remains goes in full.
#[test]
fn a_transfer_moves_only_the_chunks_the_copy_lacks() {
    focal_rt::block_on(async move {
        let moved = std::cell::RefCell::new(Moved::default());
        let result = striped(
            || 4,
            4,
            0..30,
            |index| index % 3 == 0,
            async |index: u32| {
                moved.borrow_mut().begin(index);
                hyper_rt::futures::yield_now().await;
                moved.borrow_mut().end(index, true);
                Ok(())
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        let moved = moved.into_inner();
        assert!(moved.most <= 4);
        assert_eq!(moved.begun.len(), 20);
        assert_eq!(
            moved.answered,
            (0..30).filter(|index| index % 3 != 0).collect()
        );
    })
    .unwrap()
}

/// What a copy has room for is found as a sender finds what a path
/// carries: half as many on a refusal, one more for every round answered.
#[test]
fn a_chunk_refused_the_room_is_moved_again_and_the_room_is_found() {
    focal_rt::block_on(async move {
        let moved = std::cell::RefCell::new(Moved::default());
        let refusals = std::cell::Cell::new(0_usize);
        let result = striped(
            || 8,
            8,
            0..400,
            |_| false,
            async |index: u32| {
                moved.borrow_mut().begin(index);
                hyper_rt::futures::yield_now().await;
                // The copy has room for four at once.
                let refused = moved.borrow().flying > 4;
                moved.borrow_mut().end(index, !refused);
                if refused {
                    refusals.set(refusals.get() + 1);
                    Err(AccessError::Capacity)
                } else {
                    Ok(())
                }
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        let moved = moved.into_inner();
        assert_eq!(moved.answered, (0..400).collect());
        assert_eq!(moved.begun.len(), 400 + refusals.get());
        // It was refused when it began with eight and each time it tried a
        // fifth, which is once in the rounds it takes to come back to five:
        // far fewer than the chunks it moved.
        assert!((2..=100).contains(&refusals.get()), "{}", refusals.get());
    })
    .unwrap()
}

#[test]
fn a_transfer_that_is_refused_the_room_throughout_is_refused_and_ends() {
    focal_rt::block_on(async move {
        for width in [1, 2, 8, 16] {
            let moved = std::cell::RefCell::new(Moved::default());
            let result = striped(
                || width,
                width,
                0..1000,
                |_| false,
                async |index: u32| {
                    moved.borrow_mut().begin(index);
                    hyper_rt::futures::yield_now().await;
                    moved.borrow_mut().end(index, false);
                    Err(AccessError::Capacity)
                },
            )
            .await;
            assert_eq!(result, Err(AccessError::Capacity));
            // Every chunk it had begun, and as many again at most.
            assert!(moved.borrow().begun.len() <= width * 2, "{width}");
        }
        // One chunk that is never taken beside others that are: the transfer
        // ends when nothing but it is left.
        let moved = std::cell::RefCell::new(Moved::default());
        let result = striped(
            || 4,
            4,
            0..32,
            |_| false,
            async |index: u32| {
                moved.borrow_mut().begin(index);
                hyper_rt::futures::yield_now().await;
                moved.borrow_mut().end(index, index != 7);
                if index == 7 {
                    Err(AccessError::Capacity)
                } else {
                    Ok(())
                }
            },
        )
        .await;
        assert_eq!(result, Err(AccessError::Capacity));
        let moved = moved.into_inner();
        assert!(!moved.answered.contains(&7));
        assert!(moved.begun.len() <= 32 * 2);
    })
    .unwrap()
}

#[test]
fn a_copy_that_takes_chunks_in_their_order_only_is_sent_them_one_after_another() {
    focal_rt::block_on(async move {
        let moved = std::cell::RefCell::new(Moved::default());
        let next = std::cell::Cell::new(3_u32);
        let result = striped(
            || 8,
            8,
            3..40,
            |_| false,
            async |index: u32| {
                moved.borrow_mut().begin(index);
                hyper_rt::futures::yield_now().await;
                // The copy of the release before: a chunk past the first it
                // lacks is refused, one it has is taken again.
                let taken = index <= next.get();
                if index == next.get() {
                    next.set(index + 1);
                }
                moved.borrow_mut().end(index, taken);
                if taken {
                    Ok(())
                } else {
                    Err(AccessError::InvalidRequest)
                }
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(next.get(), 40);
        let moved = moved.into_inner();
        assert_eq!(moved.answered, (3..40).collect());
        // From the refusal on, one at a time and in order, from the first.
        let from = moved.begun.iter().rposition(|index| *index == 3).unwrap();
        assert!(moved.begun[from..].iter().copied().eq(3..40));
        // A refusal that has nothing to do with order ends the transfer.
        let result = striped(
            || 1,
            1,
            0..4,
            |_| false,
            async |_| Err(AccessError::InvalidRequest),
        )
        .await;
        assert_eq!(result, Err(AccessError::InvalidRequest));
        let result = striped(
            || 8,
            8,
            0..4,
            |_| false,
            async |_| Err(AccessError::Unauthorized),
        )
        .await;
        assert_eq!(result, Err(AccessError::Unauthorized));
        assert_eq!(
            striped(|| 8, 8, 4..4, |_| false, async |_| Ok(())).await,
            Ok(())
        );
    })
    .unwrap()
}

#[test]
fn a_transfer_goes_by_as_many_streams_as_the_path_holds_when_a_chunk_is_begun() {
    focal_rt::block_on(async move {
        let moved = std::cell::RefCell::new(Moved::default());
        // What the path holds: one stream until eight chunks were answered,
        // four from then on, and two after forty.
        let width = || match moved.borrow().answered.len() {
            0..8 => 1,
            8..40 => 4,
            _ => 2,
        };
        let seen = std::cell::RefCell::new(Vec::new());
        let result = striped(
            width,
            4,
            0..64,
            |_| false,
            async |index: u32| {
                moved.borrow_mut().begin(index);
                let answered = moved.borrow().answered.len();
                let flying = moved.borrow().flying;
                seen.borrow_mut().push((answered, flying));
                hyper_rt::futures::yield_now().await;
                moved.borrow_mut().end(index, true);
                Ok(())
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        let seen = seen.into_inner();
        assert_eq!(seen.len(), 64);
        // What was begun while the path held more goes on; nothing is begun
        // beside more than the path holds.
        for (answered, flying) in &seen {
            let most = match answered {
                0..8 => 1,
                8..44 => 4,
                _ => 2,
            };
            assert!(flying <= &most, "{answered} {flying}");
        }
        assert!(seen.iter().any(|(_, flying)| *flying == 4));
    })
    .unwrap()
}

/// A deadline no test waits out: a participant job's request time, far off.
fn far() -> focal_rt::time::Instant {
    focal_rt::time::Instant::now().saturating_add(std::time::Duration::from_secs(600))
}
