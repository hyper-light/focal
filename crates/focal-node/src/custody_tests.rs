use super::*;
use focal_evidence::StoreLimits;
use std::collections::BTreeSet;

fn ledger(tenant: u128, session: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(tenant),
        session: SessionId::from_u128(session),
    }
}
fn memory() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap()
}
fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 4096,
        max_staging_bytes: 16384,
        max_uploads: 8,
        chunk_bytes: 4,
        max_manifest_bytes: 4096,
    }
}
fn policy(ledger: LedgerId) -> CustodyPolicy {
    CustodyPolicy {
        ledger,
        route_epoch: RouteEpoch(1),
        policy_revision: 1,
        peers: [1, 2].into_iter().collect(),
    }
}
fn peer(node: Option<u64>) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(9),
        tenants: [TenantId::from_u128(1), TenantId::from_u128(2)]
            .into_iter()
            .collect(),
        role: node.map_or(PeerRole::Runtime, |node_id| PeerRole::Node { node_id }),
    })
    .unwrap()
}
fn request(
    ledger: LedgerId,
    epoch: u64,
    node: Option<u64>,
    id: u128,
    operation: Operation,
) -> VerifiedRequest {
    // An ask of what a copy holds rides the ordered profile (the audit's
    // F50); every other request here is of the base.
    let protocol = match &operation {
        Operation::Custody(CustodyRequest::OpenHeld { .. }) => ORDERED_PROTOCOL_VERSION,
        _ => PROTOCOL_VERSION,
    };
    verify_request(
        peer(node),
        RequestEnvelope {
            protocol,
            ledger,
            route_epoch: RouteEpoch(epoch),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(id),
            operation,
        },
        &WireLimits::default(),
    )
    .unwrap()
}
fn stored(store: &mut ContentStore, id: u8, tenant: u128, bytes: &[u8]) -> ContentRef {
    let upload = UploadId([id; 16]);
    store
        .begin(
            upload,
            ContentDomainId(TenantId::from_u128(tenant).0),
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    for (index, part) in bytes.chunks(4).enumerate() {
        store.append(upload, (index * 4) as u64, part).unwrap();
    }
    store.seal(upload).unwrap()
}
fn custody(ledger: LedgerId, id: u128, operation: CustodyRequest) -> VerifiedRequest {
    request(ledger, 1, Some(2), id, Operation::Custody(operation))
}

#[test]
fn session_transfer_namespaces_policy_fences_and_expiry_share_one_budget() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let a = stored(&mut sender, 1, 1, b"abcdefgh");
    let b = stored(&mut sender, 2, 2, b"QRSTUVWX");
    let ma = sender.export_manifest(&a).unwrap();
    let mb = sender.export_manifest(&b).unwrap();
    let budget = memory();
    let mut config = CustodyConfig::new(1);
    config.max_transfers = 2;
    config.max_transfer_bytes = 16;
    let mut receiver = CustodyStore::new(
        ContentStore::open(target.path(), limits()).unwrap(),
        config,
        budget.clone(),
    )
    .unwrap();
    let la = ledger(1, 10);
    let lb = ledger(2, 10);
    receiver.install_policy(policy(la)).unwrap();
    receiver.install_policy(policy(lb)).unwrap();
    let transfer = [8; 16];
    for (id, ledger, content, manifest) in [(1, la, &a, &ma), (2, lb, &b, &mb)] {
        let result = receiver
            .request_whole(&custody(
                ledger,
                id,
                CustodyRequest::Open {
                    transfer,
                    policy_revision: 1,
                    content: content.clone(),
                    manifest: manifest.encoded().to_vec(),
                },
            ))
            .unwrap();
        assert_eq!(
            result.value(),
            &CustodyReply::Opened {
                chunks: 2,
                next_missing: 0
            }
        );
    }
    assert_eq!(receiver.retained(), (2, 16));
    let exhausted = receiver.request_whole(&custody(
        la,
        3,
        CustodyRequest::Open {
            transfer: [9; 16],
            policy_revision: 1,
            content: a.clone(),
            manifest: ma.encoded().to_vec(),
        },
    ));
    assert!(matches!(exhausted, Err(AccessError::Capacity)));
    assert_eq!(receiver.retained(), (2, 16));
    for (ledger, bytes) in [(la, b"abcdefgh"), (lb, b"QRSTUVWX")] {
        for (index, part) in bytes.chunks(4).enumerate() {
            receiver
                .request_whole(&custody(
                    ledger,
                    4,
                    CustodyRequest::Chunk {
                        transfer,
                        index: index as u32,
                        bytes: part.to_vec(),
                    },
                ))
                .unwrap();
        }
    }
    let sealed_a = receiver
        .request_whole(&custody(la, 5, CustodyRequest::Seal { transfer }))
        .unwrap();
    let sealed_b = receiver
        .request_whole(&custody(lb, 6, CustodyRequest::Seal { transfer }))
        .unwrap();
    assert_eq!(
        sealed_a.value(),
        &CustodyReply::Durable {
            policy_revision: 1,
            content: a.clone()
        }
    );
    assert_eq!(
        sealed_b.value(),
        &CustodyReply::Durable {
            policy_revision: 1,
            content: b.clone()
        }
    );
    drop(sealed_a);
    drop(sealed_b);
    let before = budget.stats().used;
    let mut changed = policy(la);
    changed.policy_revision = 2;
    changed.route_epoch = RouteEpoch(2);
    receiver.install_policy(changed).unwrap();
    assert_eq!(receiver.retained(), (2, 16));
    assert_eq!(budget.stats().used, before);
    assert!(matches!(
        receiver.request_whole(&custody(la, 7, CustodyRequest::Seal { transfer })),
        Err(AccessError::Unavailable)
    ));
    let missing = request(
        la,
        2,
        Some(2),
        8,
        Operation::Custody(CustodyRequest::Seal { transfer }),
    );
    assert!(matches!(
        receiver.request_whole(&missing),
        Err(AccessError::Unavailable)
    ));
    let excluded = request(
        lb,
        1,
        Some(3),
        9,
        Operation::Custody(CustodyRequest::Seal { transfer }),
    );
    assert!(matches!(
        receiver.request_whole(&excluded),
        Err(AccessError::Unauthorized)
    ));
    receiver
        .expire(std::time::Instant::now() + Duration::from_secs(61))
        .unwrap();
    assert_eq!(receiver.retained(), (0, 0));
    receiver.content().verify(&a).unwrap();
    receiver.content().verify(&b).unwrap();
    drop(receiver);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn export_cache_reuses_parsed_tree_and_accounts_detached_results() {
    let root = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(root.path(), limits()).unwrap();
    let content = stored(&mut store, 1, 1, b"abcdefgh");
    let budget = memory();
    let mut custody = CustodyStore::new(store, CustodyConfig::new(1), budget.clone()).unwrap();
    let policy = policy(ledger(1, 1));
    let scope = policy.scope();
    custody.install_policy(policy).unwrap();
    let exported = custody.export_manifest(scope, content.clone()).unwrap();
    assert_eq!(exported.value().chunks(), 2);
    // Losing the manifest after export does not trigger a read/parse per chunk;
    // each chunk still authenticates against the retained immutable descriptor.
    let domain = content
        .domain
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::fs::remove_file(
        root.path()
            .join("objects")
            .join(domain)
            .join(format!("{}.manifest", content.root)),
    )
    .unwrap();
    let first = custody
        .read_transfer_chunk(scope, content.clone(), 0)
        .unwrap();
    let second = custody
        .read_transfer_chunk(scope, content.clone(), 1)
        .unwrap();
    assert_eq!(first.value(), b"abcd");
    assert_eq!(second.value(), b"efgh");
    assert!(
        custody
            .read_transfer_chunk(scope, content, usize::MAX)
            .is_err()
    );
    drop(custody);
    assert!(budget.stats().used > 0);
    let mapped = first.map(|bytes| ContentChunk {
        offset: 0,
        bytes,
        eof: false,
    });
    drop(second);
    drop(exported);
    assert!(budget.stats().used > 0);
    assert_eq!(mapped.value().bytes, b"abcd");
    drop(mapped);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn network_manifest_and_read_chunk_require_exact_authorized_transfer() {
    let root = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(root.path(), limits()).unwrap();
    let content = stored(&mut store, 1, 1, b"abcdefgh");
    let budget = memory();
    let mut owner = CustodyStore::new(store, CustodyConfig::new(1), budget.clone()).unwrap();
    let ledger = ledger(1, 1);
    owner.install_policy(policy(ledger)).unwrap();
    let manifest = owner
        .request_whole(&custody(
            ledger,
            1,
            CustodyRequest::Manifest {
                policy_revision: 1,
                content: content.clone(),
                max_bytes: 4096,
            },
        ))
        .unwrap();
    let CustodyReply::Manifest { manifest, .. } = manifest.value() else {
        panic!("manifest response")
    };
    let transfer = [9; 16];
    assert!(matches!(
        owner.request_whole(&custody(
            ledger,
            2,
            CustodyRequest::ReadChunk {
                transfer,
                index: 0,
                max_bytes: 4
            }
        )),
        Err(AccessError::Unavailable)
    ));
    let opened = owner
        .request_whole(&custody(
            ledger,
            3,
            CustodyRequest::Open {
                transfer,
                policy_revision: 1,
                content: content.clone(),
                manifest: manifest.clone(),
            },
        ))
        .unwrap();
    assert_eq!(
        opened.value(),
        &CustodyReply::Opened {
            chunks: 2,
            next_missing: 2
        }
    );
    drop(opened);
    assert!(matches!(
        owner.request_whole(&custody(
            ledger,
            4,
            CustodyRequest::ReadChunk {
                transfer,
                index: 0,
                max_bytes: 3
            }
        )),
        Err(AccessError::Capacity)
    ));
    let first = owner
        .request_whole(&custody(
            ledger,
            5,
            CustodyRequest::ReadChunk {
                transfer,
                index: 0,
                max_bytes: 4,
            },
        ))
        .unwrap();
    assert_eq!(
        first.value(),
        &CustodyReply::Chunk {
            index: 0,
            bytes: b"abcd".to_vec()
        }
    );
    let alias = request(
        ledger,
        1,
        Some(1),
        6,
        Operation::Custody(CustodyRequest::ReadChunk {
            transfer,
            index: 0,
            max_bytes: 4,
        }),
    );
    assert!(matches!(
        owner.request_whole(&alias),
        Err(AccessError::Unavailable)
    ));
}

#[tokio::test]
async fn download_upper_bound_returns_store_sized_pages_with_exact_eof() {
    let root = tempfile::tempdir().unwrap();
    let store_limits = StoreLimits {
        max_content_bytes: 16 * 1024,
        max_staging_bytes: 16 * 1024,
        chunk_bytes: 4 * 1024,
        ..limits()
    };
    let mut store = ContentStore::open(root.path(), store_limits).unwrap();
    let bytes = vec![42; 6 * 1024];
    let upload = UploadId([1; 16]);
    store
        .begin(
            upload,
            ContentDomainId(TenantId::from_u128(1).0),
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    for (index, part) in bytes.chunks(4 * 1024).enumerate() {
        store
            .append(upload, (index * 4 * 1024) as u64, part)
            .unwrap();
    }
    let reference = store.seal(upload).unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        store,
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let ledger = ledger(1, 1);
    host.install_policy(policy(ledger)).await.unwrap();
    for (offset, length, eof) in [(0, 4 * 1024, false), (4 * 1024, 2 * 1024, true)] {
        let response = host
            .request(request(
                ledger,
                1,
                None,
                1,
                Operation::Download {
                    content: reference.clone(),
                    offset,
                    max_bytes: 65 * 1024,
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            response.value().result,
            Response::Content(ContentChunk {
                offset,
                bytes: vec![42; length],
                eof,
            })
        );
    }
    host.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn one_actor_hosts_multiple_ledgers_and_external_seal_never_attests_custody() {
    let root = tempfile::tempdir().unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        ContentStore::open(root.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let a = ledger(1, 1);
    let b = ledger(2, 1);
    host.install_policy(policy(a)).await.unwrap();
    host.clone().install_policy(policy(b)).await.unwrap();
    let upload = [7; 16];
    let bytes = b"abcdefgh";
    for ledger in [a, b] {
        let begin = request(
            ledger,
            1,
            None,
            1,
            Operation::Upload(UploadRequest::Begin {
                upload,
                length: 8,
                digest: ContentHash(*blake3::hash(bytes).as_bytes()),
                class: ContentClass::Evidence,
            }),
        );
        let response = host.request(begin).await.unwrap();
        assert_eq!(
            response.value().result,
            Response::Upload(UploadReply::Offset(0))
        );
        drop(response);
        for (index, part) in bytes.chunks(4).enumerate() {
            let append = request(
                ledger,
                1,
                None,
                2,
                Operation::Upload(UploadRequest::Append {
                    upload,
                    offset: (index * 4) as u64,
                    bytes: part.to_vec(),
                }),
            );
            let response = host.request(append).await.unwrap();
            assert_eq!(
                response.value().result,
                Response::Upload(UploadReply::Offset(((index + 1) * 4) as u64))
            );
        }
        let denied = host
            .request(request(
                ledger,
                1,
                None,
                3,
                Operation::Upload(UploadRequest::Seal { upload }),
            ))
            .await;
        assert!(matches!(denied, Err(AccessError::UnsupportedOperation)));
    }
    let a_ref = host
        .seal_upload(
            policy(a).scope(),
            request(
                a,
                1,
                None,
                4,
                Operation::Upload(UploadRequest::Seal { upload }),
            ),
        )
        .await
        .unwrap();
    let b_ref = host
        .seal_upload(
            policy(b).scope(),
            request(
                b,
                1,
                None,
                4,
                Operation::Upload(UploadRequest::Seal { upload }),
            ),
        )
        .await
        .unwrap();
    assert_ne!(a_ref.domain, b_ref.domain);
    assert_ne!(a_ref.root, b_ref.root);
    let descriptor = host
        .export_manifest(policy(a).scope(), a_ref.clone())
        .await
        .unwrap();
    let chunk = host
        .clone()
        .read_transfer_chunk(policy(a).scope(), a_ref.clone(), 1)
        .await
        .unwrap();
    assert_eq!(chunk.value(), b"efgh");
    let full = host
        .read_bytes(policy(b).scope(), b_ref.clone(), 8)
        .await
        .unwrap();
    assert_eq!(full.value(), bytes);
    let denied = host
        .request(request(
            a,
            1,
            None,
            5,
            Operation::Download {
                content: b_ref,
                offset: 0,
                max_bytes: 4,
            },
        ))
        .await;
    assert!(matches!(denied, Err(AccessError::Unauthorized)));
    let downloaded = host
        .request(request(
            a,
            1,
            None,
            6,
            Operation::Download {
                content: a_ref.clone(),
                offset: 4,
                max_bytes: 4,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        downloaded.value().result,
        Response::Content(ContentChunk {
            offset: 4,
            bytes: b"efgh".to_vec(),
            eof: true
        })
    );
    drop(downloaded);
    host.stop().await.unwrap();
    owner.join().unwrap();
    assert!(budget.stats().used > 0);
    drop(descriptor);
    drop(chunk);
    drop(full);
    assert_eq!(budget.stats().used, 0);
    assert!(matches!(
        host.install_policy(policy(a)).await,
        Err(AccessError::Unavailable)
    ));
    // The one writer lock is released at shutdown; exact scoped upload seals
    // and immutable content survive actual disk reopen under a fresh actor.
    let (recovered, owner) = ContentHost::spawn(
        ContentStore::open(root.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    recovered.install_policy(policy(a)).await.unwrap();
    assert_eq!(
        recovered
            .seal_upload(
                policy(a).scope(),
                request(
                    a,
                    1,
                    None,
                    4,
                    Operation::Upload(UploadRequest::Seal { upload })
                )
            )
            .await
            .unwrap(),
        a_ref
    );
    let replay = recovered
        .read_bytes(policy(a).scope(), a_ref, 8)
        .await
        .unwrap();
    assert_eq!(replay.value(), bytes);
    drop(replay);
    recovered.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn policy_capacity_and_memory_rejection_leave_all_installed_facts_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let budget = memory();
    let mut config = CustodyConfig::new(1);
    config.max_policies = 1;
    let mut owner = CustodyStore::new(
        ContentStore::open(root.path(), limits()).unwrap(),
        config,
        budget.clone(),
    )
    .unwrap();
    let initial = policy(ledger(1, 1));
    owner.install_policy(initial.clone()).unwrap();
    assert_eq!(
        owner.install_policy(policy(ledger(1, 2))),
        Err(AccessError::Capacity)
    );
    let mut conflict = initial.clone();
    conflict.peers = BTreeSet::from([1]);
    assert_eq!(
        owner.install_policy(conflict),
        Err(AccessError::Unavailable)
    );
    let mut next = initial.clone();
    next.policy_revision = 2;
    assert_eq!(
        owner.replace_policy(None, next.clone()),
        Err(AccessError::Unavailable)
    );
    assert_eq!(
        owner.replace_policy(
            Some(CustodyScope {
                ledger: ledger(1, 2),
                ..initial.scope()
            }),
            next.clone()
        ),
        Err(AccessError::Unavailable)
    );
    let pressure = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    assert_eq!(
        owner.replace_policy(Some(initial.scope()), next.clone()),
        Err(AccessError::Capacity)
    );
    assert_eq!(owner.installed(initial.ledger), Some(&initial));
    drop(pressure);
    owner
        .replace_policy(Some(initial.scope()), next.clone())
        .unwrap();
    owner
        .replace_policy(Some(initial.scope()), next.clone())
        .unwrap();
    assert_eq!(owner.installed(initial.ledger), Some(&next));
    assert_eq!(
        owner.replace_policy(Some(next.scope()), initial),
        Err(AccessError::Unavailable)
    );
    drop(owner);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn policy_update_without_timer_rejects_before_enqueue_and_releases_reservation() {
    let root = tempfile::tempdir().unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        ContentStore::open(root.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let initial = budget.stats().used;
    let no_time = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert_eq!(
        no_time.block_on(host.replace_policy(None, policy(ledger(1, 1)))),
        Err(AccessError::Unavailable)
    );
    assert_eq!(budget.stats().used, initial);
    drop(host);
    owner.join().unwrap();
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn missing_runtime_rejects_before_enqueue_and_dropping_handles_stops_the_owner() {
    let root = tempfile::tempdir().unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        ContentStore::open(root.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let before = budget.stats().used;
    {
        let mut future = std::pin::pin!(host.stop());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            future.as_mut().poll(&mut context),
            std::task::Poll::Ready(Err(AccessError::Unavailable))
        ));
    }
    assert_eq!(budget.stats().used, before);
    drop(host);
    owner.join().unwrap();
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn abandoned_transfer_expires_without_another_network_request() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let content = stored(&mut sender, 1, 1, b"abcdefgh");
    let manifest = sender.export_manifest(&content).unwrap();
    let budget = memory();
    let mut config = CustodyConfig::new(1);
    config.transfer_ttl = Duration::from_millis(20);
    let (host, owner) = ContentHost::spawn(
        ContentStore::open(target.path(), limits()).unwrap(),
        config,
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let ledger = ledger(1, 1);
    host.install_policy(policy(ledger)).await.unwrap();
    let baseline = budget.stats().used;
    let opened = host
        .request(custody(
            ledger,
            1,
            CustodyRequest::Open {
                transfer: [5; 16],
                policy_revision: 1,
                content,
                manifest: manifest.encoded().to_vec(),
            },
        ))
        .await
        .unwrap();
    drop(opened);
    assert!(budget.stats().used > baseline);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(budget.stats().used, baseline);
    host.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn seed_chunks_are_served_to_installed_peers_and_announced_pending_peers_only() {
    let dir = tempfile::tempdir().unwrap();
    let ledger = ledger(1, 9);
    let seed_root = dir.path().join("seeds");
    let mut seeds = focal_evidence::SeedStore::open(
        crate::custody::seed_directory(&seed_root, ledger),
        focal_memory::DiskBudget::new(focal_memory::DiskBudgetConfig::default()).unwrap(),
    )
    .unwrap();
    let hash = seeds.install(b"seed chunk bytes").unwrap();
    let mut config = CustodyConfig::new(1);
    config.seed_root = Some(seed_root);
    let mut store = CustodyStore::new(
        ContentStore::open(dir.path().join("content"), limits()).unwrap(),
        config,
        memory(),
    )
    .unwrap();
    let read = |node: u64, epoch: u64, id: u128, hash: ContentHash| {
        request(
            ledger,
            epoch,
            Some(node),
            id,
            Operation::Custody(CustodyRequest::SeedChunk {
                hash,
                max_bytes: 1024,
            }),
        )
    };
    // Nothing is served for a ledger without a policy or an announcement.
    assert!(matches!(
        store.request_whole(&read(2, 1, 1, hash)),
        Err(AccessError::Unauthorized)
    ));
    store.install_policy(policy(ledger)).unwrap();
    let served = store.request_whole(&read(2, 1, 2, hash)).unwrap();
    assert!(matches!(
        served.value(),
        CustodyReply::SeedChunk { hash: read_back, bytes }
            if *read_back == hash && bytes == b"seed chunk bytes"
    ));
    // A node outside the installed placement, and a route nobody announced.
    assert!(matches!(
        store.request_whole(&read(3, 1, 3, hash)),
        Err(AccessError::Unauthorized)
    ));
    assert!(matches!(
        store.request_whole(&read(3, 2, 4, hash)),
        Err(AccessError::Unavailable)
    ));
    // An announced pending placement admits its peers at the pending route
    // for reads (seeds, manifests, chunks, verifications and the cancel
    // that ends a pull); the announcement must lie beyond the installed
    // scope.
    let pending = CustodyScope {
        ledger,
        route_epoch: RouteEpoch(2),
        policy_revision: 2,
    };
    assert!(matches!(
        store.announce_pending(
            ledger,
            Some((
                CustodyScope {
                    route_epoch: RouteEpoch(1),
                    ..pending
                },
                BTreeSet::from([1, 3])
            ))
        ),
        Err(AccessError::InvalidRequest)
    ));
    let peers = BTreeSet::from([1, 2, 3]);
    store
        .announce_pending(ledger, Some((pending, peers.clone())))
        .unwrap();
    store
        .announce_pending(ledger, Some((pending, peers)))
        .unwrap();
    assert!(store.request_whole(&read(3, 2, 5, hash)).is_ok());
    assert!(matches!(
        store.request_whole(&read(3, 1, 6, hash)),
        Err(AccessError::Unauthorized)
    ));
    assert!(matches!(
        store.request_whole(&read(4, 2, 7, hash)),
        Err(AccessError::Unauthorized)
    ));
    let cancel = request(
        ledger,
        2,
        Some(3),
        8,
        Operation::Custody(CustodyRequest::Cancel { transfer: [8; 16] }),
    );
    assert!(store.request_whole(&cancel).is_ok());
    // A write from a pending peer at its route is refused: it holds no
    // installed policy there.
    let seal = request(
        ledger,
        2,
        Some(3),
        8,
        Operation::Custody(CustodyRequest::Seal { transfer: [8; 16] }),
    );
    assert!(matches!(
        store.request_whole(&seal),
        Err(AccessError::Unavailable)
    ));
    // An unknown seed is not found; a withdrawn announcement refuses again.
    assert!(
        store
            .request_whole(&read(3, 2, 9, ContentHash([7; 32])))
            .is_err()
    );
    store.announce_pending(ledger, None).unwrap();
    assert!(matches!(
        store.request_whole(&read(3, 2, 10, hash)),
        Err(AccessError::Unavailable)
    ));
}

/// An ask of what a copy holds is admitted where an open is (the audit's
/// F50): from the installed placement's peers, and from an announced
/// pending placement's peers at its route, since a copy being prepared
/// opens the transfers it pulls through before its placement activates;
/// from no other node. Held to the installed placement alone, a copy's pull
/// of an object it lacked was refused and the delivery that needed the
/// object waited on it for ever.
#[test]
fn an_ask_of_what_a_copy_holds_is_admitted_where_an_open_is() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let bytes: Vec<u8> = (1..=16).collect();
    let content = stored(&mut sender, 1, 1, &bytes);
    let manifest = sender.export_manifest(&content).unwrap();
    let mut store = CustodyStore::new(
        ContentStore::open(target.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        memory(),
    )
    .unwrap();
    let ledger = ledger(1, 10);
    store.install_policy(policy(ledger)).unwrap();
    let pending = CustodyScope {
        ledger,
        route_epoch: RouteEpoch(2),
        policy_revision: 2,
    };
    store
        .announce_pending(ledger, Some((pending, BTreeSet::from([1, 2, 3]))))
        .unwrap();
    let ask = |node: u64, route: u64, revision: u64, id: u128, held: bool| {
        let transfer = [u8::try_from(id).unwrap(); 16];
        let operation = if held {
            CustodyRequest::OpenHeld {
                transfer,
                policy_revision: revision,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            }
        } else {
            CustodyRequest::Open {
                transfer,
                policy_revision: revision,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            }
        };
        request(ledger, route, Some(node), id, Operation::Custody(operation))
    };
    // An installed peer at the installed route, either way.
    assert!(matches!(
        store.request_whole(&ask(2, 1, 1, 1, true)).unwrap().value(),
        CustodyReply::OpenedHeld { chunks: 4, held } if held == &vec![0]
    ));
    assert!(matches!(
        store
            .request_whole(&ask(2, 1, 1, 2, false))
            .unwrap()
            .value(),
        CustodyReply::Opened {
            chunks: 4,
            next_missing: 0
        }
    ));
    // A pending peer at the pending route, either way.
    assert!(matches!(
        store.request_whole(&ask(3, 2, 2, 3, true)).unwrap().value(),
        CustodyReply::OpenedHeld { chunks: 4, .. }
    ));
    assert!(matches!(
        store
            .request_whole(&ask(3, 2, 2, 4, false))
            .unwrap()
            .value(),
        CustodyReply::Opened { chunks: 4, .. }
    ));
    // A pending peer at the installed route, and a node of neither.
    for held in [true, false] {
        assert!(matches!(
            store.request_whole(&ask(3, 1, 1, 5, held)),
            Err(AccessError::Unauthorized)
        ));
        assert!(matches!(
            store.request_whole(&ask(4, 2, 2, 6, held)),
            Err(AccessError::Unauthorized)
        ));
    }
}

/// A transfer goes by several streams and its chunks arrive in no order:
/// a copy takes any chunk of the manifest, the last before the first,
/// counts what it lacks first, and seals what it has whole.
#[test]
fn the_chunks_of_a_manifest_are_taken_in_any_order() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    // A manifest that names two hundred chunks.
    let wide = StoreLimits {
        max_manifest_bytes: 16384,
        ..limits()
    };
    let mut sender = ContentStore::open(source.path(), wide.clone()).unwrap();
    // 200 chunks of four bytes and one of two: more than three words of
    // bits.
    let bytes: Vec<u8> = (0..802_u32).map(|at| (at % 251) as u8).collect();
    let content = stored(&mut sender, 1, 1, &bytes);
    let manifest = sender.export_manifest(&content).unwrap();
    let mut receiver = CustodyStore::new(
        ContentStore::open(target.path(), wide).unwrap(),
        CustodyConfig::new(1),
        memory(),
    )
    .unwrap();
    let ledger = ledger(1, 10);
    receiver.install_policy(policy(ledger)).unwrap();
    let transfer = [8; 16];
    let open = |receiver: &mut CustodyStore| {
        let opened = receiver
            .request_whole(&custody(
                ledger,
                1,
                CustodyRequest::Open {
                    transfer,
                    policy_revision: 1,
                    content: content.clone(),
                    manifest: manifest.encoded().to_vec(),
                },
            ))
            .unwrap();
        let CustodyReply::Opened {
            chunks,
            next_missing,
        } = opened.value()
        else {
            panic!("opened")
        };
        (*chunks, *next_missing)
    };
    let chunk = |receiver: &mut CustodyStore, index: usize, bytes: &[u8]| {
        receiver
            .request_whole(&custody(
                ledger,
                2,
                CustodyRequest::Chunk {
                    transfer,
                    index: index as u32,
                    bytes: bytes.to_vec(),
                },
            ))
            .map(|reply| reply.value().clone())
    };
    let part = |index: usize| bytes.chunks(4).nth(index).unwrap();
    assert_eq!(open(&mut receiver), (201, 0));
    // The last, and one the manifest does not name.
    assert_eq!(
        chunk(&mut receiver, 200, part(200)),
        Ok(CustodyReply::ChunkStored { index: 200 })
    );
    assert!(chunk(&mut receiver, 201, part(0)).is_err());
    // A chunk that is not the one the manifest names there is not taken.
    assert!(chunk(&mut receiver, 7, part(8)).is_err());
    for index in [3, 1, 2, 64, 65, 128] {
        chunk(&mut receiver, index, part(index)).unwrap();
    }
    // Nothing that was taken ahead is the first that is lacked.
    assert_eq!(open(&mut receiver), (201, 0));
    assert!(matches!(
        receiver.request_whole(&custody(ledger, 3, CustodyRequest::Seal { transfer })),
        Err(AccessError::InvalidRequest)
    ));
    // The first moves past everything taken after it without a gap.
    chunk(&mut receiver, 0, part(0)).unwrap();
    assert_eq!(open(&mut receiver), (201, 4));
    // A chunk sent again is taken again and moves nothing.
    chunk(&mut receiver, 2, part(2)).unwrap();
    assert_eq!(open(&mut receiver), (201, 4));
    for index in (4..200).rev() {
        if ![64, 65, 128].contains(&index) {
            chunk(&mut receiver, index, part(index)).unwrap();
        }
    }
    assert_eq!(open(&mut receiver), (201, 201));
    let sealed = receiver
        .request_whole(&custody(ledger, 4, CustodyRequest::Seal { transfer }))
        .unwrap();
    assert_eq!(
        sealed.value(),
        &CustodyReply::Durable {
            policy_revision: 1,
            content: content.clone()
        }
    );
}

/// A copy's inventory of an object is every chunk it holds verified, not
/// the prefix before the first it lacks (the audit's F50): chunks 0, 2 and
/// 3 of four taken under one transfer are held under the next, which says
/// so to a sender that asks (`OpenHeld`), and names 1 as the first lacked
/// to one that asks as an older binary does (`Open`); chunk 1 then makes
/// the object whole and the seal is given — it was refused, the chunks
/// past the gap forgotten. A chunk whose file holds other bytes is lacked,
/// and the chunk sent again installs over it.
#[test]
fn a_copy_tells_what_it_holds_and_a_gap_filled_seals_the_object() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    // Four chunks of four bytes.
    let bytes: Vec<u8> = (1..=16).collect();
    let content = stored(&mut sender, 1, 1, &bytes);
    let manifest = sender.export_manifest(&content).unwrap();
    let mut receiver = CustodyStore::new(
        ContentStore::open(target.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        memory(),
    )
    .unwrap();
    let ledger = ledger(1, 10);
    receiver.install_policy(policy(ledger)).unwrap();
    let part = |index: usize| bytes.chunks(4).nth(index).unwrap();
    let open = |receiver: &mut CustodyStore, transfer: [u8; 16]| {
        let opened = receiver
            .request_whole(&custody(
                ledger,
                1,
                CustodyRequest::Open {
                    transfer,
                    policy_revision: 1,
                    content: content.clone(),
                    manifest: manifest.encoded().to_vec(),
                },
            ))
            .unwrap();
        let CustodyReply::Opened {
            chunks,
            next_missing,
        } = opened.value()
        else {
            panic!("opened")
        };
        (*chunks, *next_missing)
    };
    let open_held = |receiver: &mut CustodyStore, transfer: [u8; 16]| {
        let opened = receiver
            .request_whole(&custody(
                ledger,
                1,
                CustodyRequest::OpenHeld {
                    transfer,
                    policy_revision: 1,
                    content: content.clone(),
                    manifest: manifest.encoded().to_vec(),
                },
            ))
            .unwrap();
        let CustodyReply::OpenedHeld { chunks, held } = opened.value() else {
            panic!("opened held")
        };
        (*chunks, held.clone())
    };
    let chunk = |receiver: &mut CustodyStore, transfer: [u8; 16], index: usize| {
        receiver
            .request_whole(&custody(
                ledger,
                2,
                CustodyRequest::Chunk {
                    transfer,
                    index: index as u32,
                    bytes: part(index).to_vec(),
                },
            ))
            .map(|reply| reply.value().clone())
    };
    let seal = |receiver: &mut CustodyStore, transfer: [u8; 16]| {
        receiver
            .request_whole(&custody(ledger, 3, CustodyRequest::Seal { transfer }))
            .map(|reply| reply.value().clone())
    };
    // The first transfer takes chunks 0, 2 and 3 and is cancelled.
    let first = [1; 16];
    assert_eq!(open(&mut receiver, first), (4, 0));
    for index in [0, 2, 3] {
        assert_eq!(
            chunk(&mut receiver, first, index),
            Ok(CustodyReply::ChunkStored {
                index: index as u32
            })
        );
    }
    receiver
        .request_whole(&custody(
            ledger,
            4,
            CustodyRequest::Cancel { transfer: first },
        ))
        .unwrap();
    // The next transfer finds them held: told whole to a sender that
    // asks, and as the first lacked to one that asks the old way.
    let second = [2; 16];
    assert_eq!(open_held(&mut receiver, second), (4, vec![0b1101]));
    receiver
        .request_whole(&custody(
            ledger,
            4,
            CustodyRequest::Cancel { transfer: second },
        ))
        .unwrap();
    let third = [3; 16];
    assert_eq!(open(&mut receiver, third), (4, 1));
    assert!(matches!(
        seal(&mut receiver, third),
        Err(AccessError::InvalidRequest)
    ));
    // The gap filled makes the object whole: the seal is given.
    assert_eq!(
        chunk(&mut receiver, third, 1),
        Ok(CustodyReply::ChunkStored { index: 1 })
    );
    assert_eq!(open(&mut receiver, third), (4, 4));
    assert_eq!(
        seal(&mut receiver, third),
        Ok(CustodyReply::Durable {
            policy_revision: 1,
            content: content.clone()
        })
    );
    // A chunk file that holds other bytes under chunk 2's name is lacked;
    // the chunk sent again installs over it and the object seals.
    let objects = target.path().join("objects");
    let mut corrupted = 0;
    for domain in std::fs::read_dir(&objects).unwrap() {
        for entry in std::fs::read_dir(domain.unwrap().path()).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_some_and(|extension| extension == "chunk")
                && std::fs::read(&path).unwrap() == part(2)
            {
                std::fs::write(&path, part(0)).unwrap();
                corrupted += 1;
            }
        }
    }
    assert_eq!(corrupted, 1);
    let fourth = [4; 16];
    assert_eq!(open_held(&mut receiver, fourth), (4, vec![0b1011]));
    assert_eq!(
        chunk(&mut receiver, fourth, 2),
        Ok(CustodyReply::ChunkStored { index: 2 })
    );
    assert_eq!(
        seal(&mut receiver, fourth),
        Ok(CustodyReply::Durable {
            policy_revision: 1,
            content: content.clone()
        })
    );
}

/// A chunk goes in parts where its path takes longer than a transfer's
/// lease to carry it whole (the audit's F49): each part taken renews the
/// lease, parts go in order with an exact retry taking nothing and a gap
/// refused, the last part makes the chunk — verified against the
/// manifest's hash and installed under the same name — and a transfer that
/// expires or is cancelled holds no part of the volume.
#[test]
fn a_chunk_in_parts_renews_the_lease_and_is_the_chunk_once_whole() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    // Three chunks of four bytes.
    let bytes: Vec<u8> = (1..=12).collect();
    let content = stored(&mut sender, 1, 1, &bytes);
    let manifest = sender.export_manifest(&content).unwrap();
    let mut receiver = CustodyStore::new(
        ContentStore::open(target.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        memory(),
    )
    .unwrap();
    let ledger = ledger(1, 10);
    receiver.install_policy(policy(ledger)).unwrap();
    let transfer = [9; 16];
    let open = |receiver: &mut CustodyStore| {
        let opened = receiver
            .request_whole(&custody(
                ledger,
                1,
                CustodyRequest::Open {
                    transfer,
                    policy_revision: 1,
                    content: content.clone(),
                    manifest: manifest.encoded().to_vec(),
                },
            ))
            .unwrap();
        let CustodyReply::Opened { next_missing, .. } = opened.value() else {
            panic!("opened")
        };
        *next_missing
    };
    let part = |receiver: &mut CustodyStore, index: u32, offset: u32, bytes: &[u8]| {
        receiver
            .request_whole(&custody(
                ledger,
                2,
                CustodyRequest::ChunkPart {
                    transfer,
                    index,
                    offset,
                    bytes: bytes.to_vec(),
                },
            ))
            .map(|reply| reply.value().clone())
    };
    let staged = |receiver: &CustodyStore, index: usize| {
        receiver
            .content()
            .staged_chunk_bytes(&manifest, index)
            .unwrap()
    };
    assert_eq!(open(&mut receiver), 0);
    // Two bytes of chunk 0, then the same two again, then a gap.
    assert_eq!(
        part(&mut receiver, 0, 0, &bytes[0..2]),
        Ok(CustodyReply::PartStored {
            index: 0,
            staged: 2
        })
    );
    assert_eq!(
        part(&mut receiver, 0, 0, &bytes[0..2]),
        Ok(CustodyReply::PartStored {
            index: 0,
            staged: 2
        })
    );
    assert!(matches!(
        part(&mut receiver, 0, 3, &bytes[3..4]),
        Err(AccessError::InvalidRequest)
    ));
    assert_eq!(staged(&receiver, 0), 2);
    // The part renewed the lease: the transfer outlives the time its open
    // alone gave it, by the part's time, and not longer.
    let after_part = std::time::Instant::now();
    receiver
        .expire(after_part + Duration::from_secs(59))
        .unwrap();
    assert_eq!(receiver.retained().0, 1);
    // The rest of chunk 0, in one part: whole, verified and installed.
    assert_eq!(
        part(&mut receiver, 0, 2, &bytes[2..4]),
        Ok(CustodyReply::ChunkStored { index: 0 })
    );
    assert_eq!(staged(&receiver, 0), 0);
    assert_eq!(open(&mut receiver), 1);
    assert_eq!(
        receiver
            .content()
            .read_transfer_chunk(&manifest, 0)
            .unwrap(),
        bytes[0..4].to_vec()
    );
    // A chunk whose last part is not the chunk's bytes is not the chunk:
    // discarded whole, to be sent again.
    assert_eq!(
        part(&mut receiver, 1, 0, &bytes[4..6]),
        Ok(CustodyReply::PartStored {
            index: 1,
            staged: 2
        })
    );
    assert!(matches!(
        part(&mut receiver, 1, 2, &[0, 0]),
        Err(AccessError::InvalidRequest)
    ));
    assert_eq!(staged(&receiver, 1), 0);
    assert_eq!(open(&mut receiver), 1);
    // Part of chunk 1 again, then the transfer expires: the part is gone
    // with it, and the chunk installed whole stays.
    part(&mut receiver, 1, 0, &bytes[4..6]).unwrap();
    assert_eq!(staged(&receiver, 1), 2);
    receiver
        .expire(std::time::Instant::now() + Duration::from_secs(61))
        .unwrap();
    assert_eq!(receiver.retained().0, 0);
    assert_eq!(staged(&receiver, 1), 0);
    receiver
        .content()
        .read_transfer_chunk(&manifest, 0)
        .unwrap();
    // Opened again, chunk 1 in parts and chunk 2 whole, sealed: durable.
    assert_eq!(open(&mut receiver), 1);
    part(&mut receiver, 1, 0, &bytes[4..5]).unwrap();
    part(&mut receiver, 1, 1, &bytes[5..7]).unwrap();
    assert_eq!(
        part(&mut receiver, 1, 3, &bytes[7..8]),
        Ok(CustodyReply::ChunkStored { index: 1 })
    );
    receiver
        .request_whole(&custody(
            ledger,
            3,
            CustodyRequest::Chunk {
                transfer,
                index: 2,
                bytes: bytes[8..12].to_vec(),
            },
        ))
        .unwrap();
    assert_eq!(open(&mut receiver), 3);
    let sealed = receiver
        .request_whole(&custody(ledger, 4, CustodyRequest::Seal { transfer }))
        .unwrap();
    assert!(matches!(
        sealed.value(),
        CustodyReply::Durable { content: found, .. } if *found == content
    ));
    receiver
        .request_whole(&custody(ledger, 8, CustodyRequest::Cancel { transfer }))
        .unwrap();
    assert_eq!(receiver.retained().0, 0);
    // A cancelled transfer's parts go too.
    let other = [10; 16];
    receiver
        .request_whole(&custody(
            ledger,
            5,
            CustodyRequest::Open {
                transfer: other,
                policy_revision: 1,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            },
        ))
        .unwrap();
    // The chunks are installed already: a part of one adds nothing and
    // answers that the chunk is held whole.
    assert_eq!(
        receiver
            .request_whole(&custody(
                ledger,
                6,
                CustodyRequest::ChunkPart {
                    transfer: other,
                    index: 0,
                    offset: 0,
                    bytes: bytes[0..1].to_vec(),
                },
            ))
            .map(|reply| reply.value().clone()),
        Ok(CustodyReply::ChunkStored { index: 0 })
    );
    receiver
        .request_whole(&custody(
            ledger,
            7,
            CustodyRequest::Cancel { transfer: other },
        ))
        .unwrap();
    assert_eq!(receiver.retained().0, 0);
}

/// The pull of a chunk in parts: a part is cut from the verified chunk at
/// the offset asked, no longer than asked, and says how long the chunk is.
#[test]
fn a_chunk_is_read_in_parts_from_a_verified_whole() {
    let source = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(source.path(), limits()).unwrap();
    let bytes: Vec<u8> = (1..=8).collect();
    let content = stored(&mut store, 1, 1, &bytes);
    let manifest = store.export_manifest(&content).unwrap();
    let mut sender = CustodyStore::new(store, CustodyConfig::new(1), memory()).unwrap();
    let ledger = ledger(1, 10);
    sender.install_policy(policy(ledger)).unwrap();
    let transfer = [11; 16];
    sender
        .request_whole(&custody(
            ledger,
            1,
            CustodyRequest::Open {
                transfer,
                policy_revision: 1,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            },
        ))
        .unwrap();
    let read = |sender: &mut CustodyStore, index: u32, offset: u32, max_bytes: u32| {
        sender
            .request_whole(&custody(
                ledger,
                2,
                CustodyRequest::ReadChunkPart {
                    transfer,
                    index,
                    offset,
                    max_bytes,
                },
            ))
            .map(|reply| reply.value().clone())
    };
    assert_eq!(
        read(&mut sender, 1, 1, 2),
        Ok(CustodyReply::ChunkPart {
            index: 1,
            offset: 1,
            length: 4,
            bytes: bytes[5..7].to_vec()
        })
    );
    assert_eq!(
        read(&mut sender, 1, 3, 8),
        Ok(CustodyReply::ChunkPart {
            index: 1,
            offset: 3,
            length: 4,
            bytes: bytes[7..8].to_vec()
        })
    );
    // Past the chunk's end: nothing to read there. (A request for no
    // bytes at all is the wire's to refuse, before it reaches the owner.)
    assert!(matches!(
        read(&mut sender, 1, 4, 1),
        Err(AccessError::InvalidRequest)
    ));
}

/// A receiver holding `bytes` of tenant 1 imported whole under transfer
/// `[8; 16]`, every chunk taken: what a seal is asked of.
fn imported(
    target: &std::path::Path,
    bytes: &[u8],
) -> (CustodyStore, LedgerId, ContentRef, MemoryBudget) {
    let source = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let content = stored(&mut sender, 1, 1, bytes);
    let manifest = sender.export_manifest(&content).unwrap();
    let budget = memory();
    let mut receiver = CustodyStore::new(
        ContentStore::open(target, limits()).unwrap(),
        CustodyConfig::new(1),
        budget.clone(),
    )
    .unwrap();
    let ledger = ledger(1, 10);
    receiver.install_policy(policy(ledger)).unwrap();
    receiver
        .request_whole(&custody(
            ledger,
            1,
            CustodyRequest::Open {
                transfer: [8; 16],
                policy_revision: 1,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            },
        ))
        .unwrap();
    for (index, part) in bytes.chunks(4).enumerate() {
        receiver
            .request_whole(&custody(
                ledger,
                2,
                CustodyRequest::Chunk {
                    transfer: [8; 16],
                    index: index as u32,
                    bytes: part.to_vec(),
                },
            ))
            .unwrap();
    }
    (receiver, ledger, content, budget)
}
fn manifest_installed(root: &std::path::Path, content: &ContentRef) -> bool {
    root.join("objects")
        .join(
            content
                .domain
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
        .join(format!("{}.manifest", content.root))
        .exists()
}

/// A seal reads every chunk back a slice — a chunk — at a time, the store
/// keeping the pass between slices, and other requests are answered between
/// them (the audit's F51): before, `complete_import` read and hashed the
/// whole object in one call of the content owner, up to a gibibyte. The
/// manifest is installed only at the pass's end; a seal asked after it is
/// answered at once.
#[test]
fn a_seal_goes_a_chunk_a_slice_and_other_requests_are_answered_between() {
    let target = tempfile::tempdir().unwrap();
    let bytes: Vec<u8> = (0..64u8).collect();
    let (mut receiver, ledger, content, _budget) = imported(target.path(), &bytes);
    let mut scratch = vec![0u8; receiver.content().max_chunk_bytes()];
    let Step::Pending(pass) = receiver
        .request(&custody(
            ledger,
            3,
            CustodyRequest::Seal { transfer: [8; 16] },
        ))
        .unwrap()
    else {
        panic!("a seal of sixteen chunks is a pass");
    };
    let mut step = receiver.advance(pass, &mut scratch).unwrap();
    let mut slices = 1;
    // Between two slices, a read of the same transfer is answered whole.
    let read = receiver
        .request_whole(&custody(
            ledger,
            4,
            CustodyRequest::ReadChunk {
                transfer: [8; 16],
                index: 5,
                max_bytes: 4,
            },
        ))
        .unwrap();
    assert_eq!(
        read.value(),
        &CustodyReply::Chunk {
            index: 5,
            bytes: bytes[20..24].to_vec()
        }
    );
    assert!(!manifest_installed(target.path(), &content));
    while let Step::Pending(pass) = step {
        assert!(!manifest_installed(target.path(), &content), "{slices}");
        step = receiver.advance(pass, &mut scratch).unwrap();
        slices += 1;
    }
    // Sixteen chunks read back, then the stream checked and the manifest
    // installed.
    assert_eq!(slices, 17);
    let Step::Done(sealed) = step else {
        unreachable!()
    };
    assert_eq!(
        sealed.value(),
        &CustodyReply::Durable {
            policy_revision: 1,
            content: content.clone()
        }
    );
    assert!(manifest_installed(target.path(), &content));
    // A lost reply's retry: answered at once.
    assert!(matches!(
        receiver.request(&custody(
            ledger,
            5,
            CustodyRequest::Seal { transfer: [8; 16] }
        )),
        Ok(Step::Done(_))
    ));
}

/// A chunk that turns corrupt on the volume while a seal reads the object
/// back ends the pass with nothing installed, and the next ask begins it
/// again from the first chunk; a cancel during a seal ends it too (the
/// audit's F51).
#[test]
fn a_seal_that_meets_a_corrupt_chunk_or_a_cancel_installs_nothing() {
    let target = tempfile::tempdir().unwrap();
    let bytes: Vec<u8> = (0..64u8).collect();
    let (mut receiver, ledger, content, _budget) = imported(target.path(), &bytes);
    let mut scratch = vec![0u8; receiver.content().max_chunk_bytes()];
    let Step::Pending(pass) = receiver
        .request(&custody(
            ledger,
            3,
            CustodyRequest::Seal { transfer: [8; 16] },
        ))
        .unwrap()
    else {
        panic!("a pass");
    };
    let Step::Pending(pass) = receiver.advance(pass, &mut scratch).unwrap() else {
        panic!("a pass");
    };
    // The last chunk's file is overwritten with other bytes of its length.
    let domain: String = content
        .domain
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let last = blake3::hash(&bytes[60..64]);
    let path = target.path().join("objects").join(&domain).join(format!(
        "{}.chunk",
        focal_model::ContentHash(*last.as_bytes())
    ));
    std::fs::write(&path, b"wxyz").unwrap();
    let mut step = Ok(Step::Pending(pass));
    for _ in 0..32 {
        match step {
            Ok(Step::Pending(pass)) => step = receiver.advance(pass, &mut scratch),
            _ => break,
        }
    }
    assert!(matches!(step, Err(AccessError::InvalidRequest)), "corrupt");
    assert!(!manifest_installed(target.path(), &content));
    // The chunk is sent again; the next seal begins from the first chunk and
    // installs the manifest.
    receiver
        .request_whole(&custody(
            ledger,
            4,
            CustodyRequest::Chunk {
                transfer: [8; 16],
                index: 15,
                bytes: bytes[60..64].to_vec(),
            },
        ))
        .unwrap();
    receiver
        .request_whole(&custody(
            ledger,
            5,
            CustodyRequest::Seal { transfer: [8; 16] },
        ))
        .unwrap();
    assert!(manifest_installed(target.path(), &content));

    // A second transfer cancelled while its seal is under way.
    let other = tempfile::tempdir().unwrap();
    let (mut receiver, ledger, content, budget) = imported(other.path(), &bytes);
    let Step::Pending(pass) = receiver
        .request(&custody(
            ledger,
            3,
            CustodyRequest::Seal { transfer: [8; 16] },
        ))
        .unwrap()
    else {
        panic!("a pass");
    };
    let Step::Pending(pass) = receiver.advance(pass, &mut scratch).unwrap() else {
        panic!("a pass");
    };
    receiver
        .request_whole(&custody(
            ledger,
            4,
            CustodyRequest::Cancel { transfer: [8; 16] },
        ))
        .unwrap();
    assert!(matches!(
        receiver.advance(pass, &mut scratch),
        Err(AccessError::Unavailable)
    ));
    assert!(!manifest_installed(other.path(), &content));
    drop(receiver);
    assert_eq!(budget.stats().used, 0);
}

/// An open's inventory of what the copy holds reads a chunk a slice, and an
/// open asked again while it runs advances the same inventory (the audit's
/// F51); a verification of an installed object goes a chunk a slice too.
#[test]
fn an_inventory_and_a_verification_go_a_chunk_a_slice() {
    let target = tempfile::tempdir().unwrap();
    let bytes: Vec<u8> = (0..64u8).collect();
    let (mut receiver, ledger, content, budget) = imported(target.path(), &bytes);
    let source = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let held = stored(&mut sender, 1, 1, &bytes);
    let manifest = sender.export_manifest(&held).unwrap();
    let mut scratch = vec![0u8; receiver.content().max_chunk_bytes()];
    // The object held whole, a new transfer of it opens with an inventory.
    let open = |id: u128| {
        custody(
            ledger,
            id,
            CustodyRequest::OpenHeld {
                transfer: [9; 16],
                policy_revision: 1,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            },
        )
    };
    let Step::Pending(pass) = receiver.request(&open(10)).unwrap() else {
        panic!("an inventory of sixteen chunks is a pass");
    };
    let Step::Pending(_) = receiver.advance(pass, &mut scratch).unwrap() else {
        panic!("a pass");
    };
    // Asked again: the same inventory, carried on from where it stands.
    let Step::Pending(mut pass) = receiver.request(&open(11)).unwrap() else {
        panic!("a pass");
    };
    let mut slices = 1;
    let held = loop {
        match receiver.advance(pass, &mut scratch).unwrap() {
            Step::Pending(next) => pass = next,
            Step::Done(reply) => break reply,
        }
        slices += 1;
    };
    assert_eq!(slices, 16, "fifteen chunks left, then the answer");
    assert_eq!(
        held.value(),
        &CustodyReply::OpenedHeld {
            chunks: 16,
            held: vec![0xffff]
        }
    );
    drop(held);
    // A verification of the installed object: a chunk a slice, then the
    // stream.
    receiver
        .request_whole(&custody(
            ledger,
            12,
            CustodyRequest::Seal { transfer: [8; 16] },
        ))
        .unwrap();
    let verify = custody(
        ledger,
        13,
        CustodyRequest::Verify {
            policy_revision: 1,
            content: content.clone(),
        },
    );
    let Step::Pending(mut pass) = receiver.request(&verify).unwrap() else {
        panic!("a verification of sixteen chunks is a pass");
    };
    // An ask of the same verification while it runs joins it: a peer that
    // gave up waiting and asked again.
    let again = custody(
        ledger,
        14,
        CustodyRequest::Verify {
            policy_revision: 1,
            content: content.clone(),
        },
    );
    let Step::Pending(joined) = receiver.request(&again).unwrap() else {
        panic!("joined the pass under way");
    };
    let mut slices = 1;
    let verified = loop {
        match receiver.advance(pass, &mut scratch).unwrap() {
            Step::Pending(next) => pass = next,
            Step::Done(reply) => break reply,
        }
        slices += 1;
    };
    assert_eq!(slices, 17, "sixteen chunks, then the stream");
    let durable = CustodyReply::Durable {
        policy_revision: 1,
        content: content.clone(),
    };
    assert_eq!(verified.value(), &durable);
    drop(verified);
    // The ask that joined is answered by the pass it joined. Sent to begin
    // again, a pass longer than the asker's patience would never end.
    let Step::Done(answer) = receiver.advance(joined, &mut scratch).unwrap() else {
        panic!("answered by the pass it joined");
    };
    assert_eq!(answer.value(), &durable);
    drop(answer);
    // A verification asked once the last is done reads the object afresh.
    let Step::Pending(_) = receiver.request(&verify).unwrap() else {
        panic!("verified afresh");
    };
    drop(receiver);
    assert_eq!(budget.stats().used, 0);
}

/// An upload's seal is kept by the store between asks, a chunk an ask, and
/// once done its reference answers every later ask at once: an upload's id
/// is never staged again once finished, so the reference stands. At the
/// bound of seals — as many as the store stages uploads — a seal that is
/// done gives its place up to a new one; one under way never does (the
/// audit's F51).
#[test]
fn an_upload_seal_is_kept_between_asks_and_answers_once_done() {
    let target = tempfile::tempdir().unwrap();
    let budget = memory();
    let mut store = CustodyStore::new(
        ContentStore::open(
            target.path(),
            StoreLimits {
                max_uploads: 2,
                ..limits()
            },
        )
        .unwrap(),
        CustodyConfig::new(1),
        budget.clone(),
    )
    .unwrap();
    let mut scratch = vec![0u8; store.content().max_chunk_bytes()];
    let bytes: Vec<u8> = (0..16u8).collect();
    let staged = |store: &mut CustodyStore, id: u8| {
        let id = UploadId([id; 16]);
        store
            .content_mut()
            .begin(
                id,
                ContentDomainId([1; 16]),
                ContentClass::Evidence,
                16,
                None,
            )
            .unwrap();
        for (index, part) in bytes.chunks(4).enumerate() {
            store
                .content_mut()
                .append(id, (index * 4) as u64, part)
                .unwrap();
        }
        id
    };
    let first = staged(&mut store, 1);
    // The first ask begins the seal; then four chunks, and the manifest.
    assert_eq!(store.seal_slice(first, &mut scratch).unwrap(), None);
    let mut slices = 0;
    let reference = loop {
        slices += 1;
        if let Some(reference) = store.seal_slice(first, &mut scratch).unwrap() {
            break reference;
        }
    };
    assert_eq!(slices, 5, "four chunks, then the manifest");
    // Asked again, a lost reply's retry: answered at once.
    assert_eq!(
        store.seal_slice(first, &mut scratch).unwrap(),
        Some(reference.clone())
    );
    store.content_mut().finish(first).unwrap();
    // Two more uploads, the store's bound of staged ones. The second's seal
    // under way and the first's done fill the bound of seals; the third's
    // takes the place of the one that is done.
    let second = staged(&mut store, 2);
    let third = staged(&mut store, 3);
    assert_eq!(store.seal_slice(second, &mut scratch).unwrap(), None);
    assert_eq!(store.seal_slice(third, &mut scratch).unwrap(), None);
    // With both under way, a seal asked anew finds no place: the first's,
    // its upload finished, was given up and begins again.
    assert!(matches!(
        store.seal_slice(first, &mut scratch),
        Err(AccessError::Capacity)
    ));
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

/// A backup's content restore is kept by the store between asks with the
/// backup's manifest, charged, a chunk an ask. Done, it releases what it
/// held and answers an ask of the same backup at once. Another backup is
/// refused while one is under way, and takes the place of one that is done
/// (the audit's F51).
#[test]
fn a_restore_is_kept_between_asks_and_answers_once_done() {
    use focal_ledger::backup::{BackupChunk, BackupManifest, BackupObject, CONTENT_DIR};
    let source = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let bytes: Vec<u8> = (0..16u8).collect();
    let content = stored(&mut sender, 1, 1, &bytes);
    let transfer = sender.export_manifest(&content).unwrap();
    // The backup's directory: the object's manifest and its chunks.
    let backup = tempfile::tempdir().unwrap();
    let directory = backup.path().join(CONTENT_DIR);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(format!("{}.manifest", content.root)),
        transfer.encoded(),
    )
    .unwrap();
    let mut chunks = Vec::new();
    for index in 0..transfer.chunks() {
        let chunk = sender.read_transfer_chunk(&transfer, index).unwrap();
        let hash = ContentHash(*blake3::hash(&chunk).as_bytes());
        std::fs::write(directory.join(format!("{hash}.chunk")), &chunk).unwrap();
        chunks.push(BackupChunk {
            hash,
            length: chunk.len() as u32,
        });
    }
    let manifest = |created_ms: u64| BackupManifest {
        schema: 1,
        created_ms,
        prefix: focal_ledger::EvidencePrefix {
            cluster: [1; 16],
            ledger: ledger(1, 1),
            group: focal_directory::LogGroupId([2; 16]),
            genesis: ContentHash([3; 32]),
            node: 1,
            sequence: SessionSeq(1),
            index: RaftIndex(1),
            term: RaftTerm(1),
            route: RouteEpoch(1),
            placement_epoch: 1,
            membership_epoch: 1,
            operation: focal_directory::OperationId([4; 16]),
            placement_digest: ContentHash([5; 32]),
            checkpoint: ContentHash([6; 32]),
            checkpoint_bytes: 0,
            artifacts: 1,
        },
        native_sequence: 0,
        native_genesis: ContentHash([7; 32]),
        profile: 0,
        domain: content.domain,
        decoder_predecessor: [0; 32],
        decoder_successor: [0; 32],
        configuration: Default::default(),
        checkpoint_hash: ContentHash([6; 32]),
        checkpoint_bytes: 0,
        seeds: Vec::new(),
        content: vec![BackupObject {
            root: content.root,
            length: content.length,
            class: content.class,
            chunks: chunks.clone(),
        }],
        archived_through: 0,
        retired_families: 0,
    };
    let target = tempfile::tempdir().unwrap();
    let budget = memory();
    let work = memory();
    let mut store = CustodyStore::new(
        ContentStore::open(target.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        budget.clone(),
    )
    .unwrap();
    let mut scratch = vec![0u8; store.content().max_chunk_bytes()];
    let begin = |created_ms: u64| {
        RestoreAsk::Begin(backup.path().to_path_buf(), Box::new(manifest(created_ms)))
    };
    let carry_on = |created_ms: u64| {
        RestoreAsk::Continue(
            backup.path().to_path_buf(),
            (ContentHash([6; 32]), created_ms),
        )
    };
    let idle = budget.stats().used;
    // The first ask begins it, the manifest it holds charged.
    assert_eq!(
        store.restore_slice(begin(1), &work, &mut scratch).unwrap(),
        None
    );
    assert!(budget.stats().used > idle, "the manifest held is charged");
    // Another backup is refused while this one is under way.
    assert!(matches!(
        store.restore_slice(begin(2), &work, &mut scratch),
        Err(AccessError::Unavailable)
    ));
    // Asked to carry on, it goes a slice an ask: the object's manifest, four
    // chunks read in, four read back, its manifest installed, and the end.
    let mut slices = 0;
    let imported = loop {
        slices += 1;
        if let Some(imported) = store
            .restore_slice(carry_on(1), &work, &mut scratch)
            .unwrap()
        {
            break imported;
        }
    };
    assert_eq!((imported, slices), (1, 11));
    assert_eq!(budget.stats().used, idle, "done, what it held is released");
    assert_eq!(work.stats().used, 0);
    // Asked again, a lost reply's retry, it is answered at once.
    assert_eq!(
        store.restore_slice(begin(1), &work, &mut scratch).unwrap(),
        Some(1)
    );
    assert_eq!(
        store
            .restore_slice(carry_on(1), &work, &mut scratch)
            .unwrap(),
        Some(1)
    );
    // Another backup takes the place of the one that is done, and an ask
    // that names the first carries the second on no further: an asker of
    // the first still waiting would have been answered with another's count.
    assert_eq!(
        store.restore_slice(begin(2), &work, &mut scratch).unwrap(),
        None
    );
    assert!(matches!(
        store.restore_slice(carry_on(1), &work, &mut scratch),
        Err(AccessError::Unavailable)
    ));
    assert_eq!(
        store
            .restore_slice(carry_on(2), &work, &mut scratch)
            .unwrap(),
        None
    );
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

/// Through the content owner's queue, a seal and a verification of an
/// object of a hundred chunks — as many as the store's manifest bound names
/// at these limits — each go a chunk a slice to their answers, one queued
/// command a slice, the answer given by the last (the audit's F51).
#[tokio::test]
async fn a_seal_and_a_verification_go_through_the_owner_a_slice_at_a_time() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut sender = ContentStore::open(source.path(), limits()).unwrap();
    let bytes: Vec<u8> = (0..400u32).map(|at| (at % 251) as u8).collect();
    let content = stored(&mut sender, 1, 1, &bytes);
    let manifest = sender.export_manifest(&content).unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        ContentStore::open(target.path(), limits()).unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let ledger = ledger(1, 1);
    host.install_policy(policy(ledger)).await.unwrap();
    let opened = host
        .request(custody(
            ledger,
            1,
            CustodyRequest::Open {
                transfer: [5; 16],
                policy_revision: 1,
                content: content.clone(),
                manifest: manifest.encoded().to_vec(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        opened.value().result,
        Response::Custody(CustodyReply::Opened {
            chunks: 100,
            next_missing: 0
        })
    ));
    drop(opened);
    for (index, part) in bytes.chunks(4).enumerate() {
        drop(
            host.request(custody(
                ledger,
                2,
                CustodyRequest::Chunk {
                    transfer: [5; 16],
                    index: index as u32,
                    bytes: part.to_vec(),
                },
            ))
            .await
            .unwrap(),
        );
    }
    let sealed = host
        .request(custody(
            ledger,
            3,
            CustodyRequest::Seal { transfer: [5; 16] },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            &sealed.value().result,
            Response::Custody(CustodyReply::Durable { content: sealed, .. }) if *sealed == content
        ),
        "{:?}",
        sealed.value().result
    );
    drop(sealed);
    let verified = host
        .request(custody(
            ledger,
            4,
            CustodyRequest::Verify {
                policy_revision: 1,
                content: content.clone(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        verified.value().result,
        Response::Custody(CustodyReply::Durable { .. })
    ));
    drop(verified);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

/// What a small request waits for on the content owner while an upload of
/// 256 MiB seals at 1 MiB chunks (the audit's F51): each wait, from asked
/// to answered, as quantiles beside the seal's own time. A seal at once was
/// one command, and a request queued behind it waited for all of it; a
/// seal a chunk a slice leaves it a slice to wait for. A measurement, run
/// by name on a quiet machine with half a gibibyte of disk free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement; run by name on a quiet machine"]
async fn measure_small_request_waits_while_a_large_upload_seals() {
    const OBJECT: u64 = 256 << 20;
    const CHUNK: usize = 1 << 20;
    let root = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(1 << 30, 256 << 20).unwrap();
    let (host, owner) = ContentHost::spawn(
        ContentStore::open(
            root.path(),
            StoreLimits {
                max_content_bytes: OBJECT,
                max_staging_bytes: OBJECT,
                max_uploads: 1,
                chunk_bytes: CHUNK,
                max_manifest_bytes: 64 * 1024,
            },
        )
        .unwrap(),
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let ledger = ledger(1, 1);
    host.install_policy(policy(ledger)).await.unwrap();
    let upload = [7; 16];
    // Half a frame an append: its bytes and its envelope within the one
    // frame a request may be (`WireLimits::max_frame_bytes`).
    let append = WireLimits::default().max_frame_bytes as u64 / 2;
    // No two chunks alike, so each is a file of its own: the byte at a
    // position, mixed with its chunk's index.
    let part = |offset: u64| -> Vec<u8> {
        (offset..offset + append)
            .map(|at| ((at % 251) as u8) ^ ((at / CHUNK as u64) as u8))
            .collect()
    };
    let mut whole = blake3::Hasher::new();
    for index in 0..OBJECT / append {
        whole.update(&part(index * append));
    }
    host.request(request(
        ledger,
        1,
        None,
        1,
        Operation::Upload(UploadRequest::Begin {
            upload,
            length: OBJECT,
            digest: ContentHash(*whole.finalize().as_bytes()),
            class: ContentClass::Evidence,
        }),
    ))
    .await
    .unwrap();
    for index in 0..OBJECT / append {
        host.request(request(
            ledger,
            1,
            None,
            2 + u128::from(index),
            Operation::Upload(UploadRequest::Append {
                upload,
                offset: index * append,
                bytes: part(index * append),
            }),
        ))
        .await
        .unwrap();
    }
    let seal = request(
        ledger,
        1,
        None,
        1 << 40,
        Operation::Upload(UploadRequest::Seal { upload }),
    );
    let started = std::time::Instant::now();
    let sealing = {
        let host = host.clone();
        let scope = policy(ledger).scope();
        tokio::spawn(async move {
            host.seal_upload(scope, seal)
                .await
                .map(|_| started.elapsed())
        })
    };
    let mut waits = Vec::new();
    while !sealing.is_finished() {
        let asked = std::time::Instant::now();
        host.disk_stats().await.unwrap();
        waits.push(asked.elapsed());
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let sealed = sealing.await.unwrap().unwrap();
    waits.sort();
    let quantile = |q: f64| waits[((waits.len() - 1) as f64 * q) as usize];
    println!(
        "a seal of {} MiB at {} KiB chunks took {sealed:?}; {} small requests during it waited p50 {:?}, p90 {:?}, p99 {:?}, max {:?}",
        OBJECT >> 20,
        CHUNK >> 10,
        waits.len(),
        quantile(0.5),
        quantile(0.9),
        quantile(0.99),
        quantile(1.0),
    );
    host.stop().await.unwrap();
    owner.join().unwrap();
}
