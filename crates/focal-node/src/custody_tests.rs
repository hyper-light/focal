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
    verify_request(
        peer(node),
        RequestEnvelope {
            protocol: PROTOCOL_VERSION,
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
            .request(&custody(
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
    let exhausted = receiver.request(&custody(
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
                .request(&custody(
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
        .request(&custody(la, 5, CustodyRequest::Seal { transfer }))
        .unwrap();
    let sealed_b = receiver
        .request(&custody(lb, 6, CustodyRequest::Seal { transfer }))
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
        receiver.request(&custody(la, 7, CustodyRequest::Seal { transfer })),
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
        receiver.request(&missing),
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
        receiver.request(&excluded),
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
        .request(&custody(
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
        owner.request(&custody(
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
        .request(&custody(
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
        owner.request(&custody(
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
        .request(&custody(
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
        owner.request(&alias),
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
