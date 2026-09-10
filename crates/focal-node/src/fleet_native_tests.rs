//! The native profile through the replicated host: activation on a fresh
//! ledger, frame admission, pending resolution and linearizable reads.
use super::*;
use crate::content_host::ContentHost;
use crate::custody::CustodyConfig;
use focal_client::input::BuildContext;
use focal_client::operations::parse_native_json;
use focal_consensus::{DurableNode, NodeConfig};
use focal_evidence::{ContentReader, ContentStore, StoreLimits};
use focal_ledger::{NativeHosting, NativeSessionLimits, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::RangeId;
use focal_model::{ContentDomainId, RootCommandId};
use focal_native_client::{CompileLimits, Resolved};
use serde_json::json;
use std::collections::BTreeSet;

const CLUSTER: [u8; 16] = [152; 16];
const ISSUER: ParticipantId = ParticipantId::from_u128(91);
const WORKER: ParticipantId = ParticipantId::from_u128(92);

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(152),
        session: SessionId::from_u128(1),
    }
}
fn domain() -> ContentDomainId {
    ContentDomainId(ledger().tenant.0)
}
fn peer(principal: ParticipantId) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal,
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn envelope(request: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation,
    }
}
fn ids(seed: u128) -> impl FnMut() -> Result<[u8; 16], focal_client::input::InputError> {
    let mut next = seed;
    move || {
        next += 1;
        Ok(next.to_be_bytes())
    }
}
fn frame(
    actor: ParticipantId,
    request: u128,
    name: &str,
    document: serde_json::Value,
    resolved: &Resolved,
) -> Vec<u8> {
    let operation = parse_native_json(name, &serde_json::to_vec(&document).unwrap()).unwrap();
    let context = BuildContext {
        ledger: ledger(),
        actor,
        root: RootCommandId::from_u128(152),
        policy_revision: 1,
    };
    let limits = CompileLimits::default();
    let compiled = focal_native_client::compile(
        &operation,
        &context,
        RequestId::from_u128(request),
        &mut ids(request * 1000),
        resolved,
        &limits,
    )
    .unwrap();
    focal_native_client::encode_frame(
        ledger(),
        focal_ledger::NativeContentProfile::AuthoredV1,
        &compiled.input,
        limits.encoding(),
    )
    .unwrap()
}
async fn call(host: &ReplicaHost, actor: ParticipantId, request: RequestEnvelope) -> Response {
    dispatch(host, peer(actor), request, &ReplicaHost::wire_limits())
        .await
        .result
}
fn read(query: NativeReadQuery) -> Operation {
    Operation::NativeRead(NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query,
        max_items: 16,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_replicated_ledger_activates_admits_frames_and_serves_linearizable_reads() {
    let dir = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(768 * 1024 * 1024, 256 * 1024 * 1024).unwrap();
    let tenant = budget.child(384 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let content_dir = dir.path().join("content");
    let store = ContentStore::open(
        &content_dir,
        StoreLimits {
            max_content_bytes: 64 << 20,
            max_staging_bytes: 128 << 20,
            max_uploads: 16,
            chunk_bytes: 4096,
            max_manifest_bytes: 1 << 20,
        },
    )
    .unwrap();
    let wal = SharedWal::open_with_budget(
        dir.path().join("wal"),
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 1,
        }),
        WalWriterLimits::default(),
        budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, CLUSTER, ledger().session.0),
        wal.clone(),
        &tenant,
    )
    .unwrap();
    let mut session = Session::from_node_in_hosted(
        ledger(),
        node,
        SessionLimits::default(),
        &tenant,
        NativeHosting {
            limits: NativeSessionLimits::standard(domain()),
            reader: ContentReader::open(&content_dir).unwrap(),
            seeds: focal_evidence::SeedStore::open(
                content_dir.join("seeds"),
                focal_memory::DiskBudget::new(focal_memory::DiskBudgetConfig::default()).unwrap(),
            )
            .unwrap(),
            range: RangeId(1),
        },
    )
    .unwrap();
    session.campaign().unwrap();
    for _ in 0..8 {
        session.poll().unwrap();
    }
    let mut config = ReplicaConfig::new(RootCommandId::from_u128(152));
    // Four applied entries past a snapshot checkpoint the replica (26 §3).
    config.checkpoint_after_entries = 4;
    config.tick = Duration::from_millis(20);
    config.request_timeout = Duration::from_millis(2000);
    let (host, owner, _outgoing) =
        ReplicaHost::spawn(session, config, ReplicaHost::wire_limits()).unwrap();
    let (content, content_owner) = ContentHost::spawn(
        store,
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let chunking = content.import_chunking();
    let call_activation = ActivateNativeCall {
        profile: focal_ledger::NativeContentProfile::AuthoredV1,
        chunk_bytes: chunking.0,
        max_manifest_bytes: chunking.1,
    };
    let mut last = Err(LedgerError::Failed);
    for _ in 0..200 {
        last = host.activate_native(call_activation).await;
        match &last {
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::PersistencePending))
            | Err(LedgerError::Managed(focal_ledger::ManagedError::Unsupported)) => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            _ => break,
        }
    }
    last.unwrap();
    let mut active = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let diagnostics = host.diagnostics().await.unwrap();
        if diagnostics.value().native_active && diagnostics.value().native_authoritative {
            active = true;
            break;
        }
    }
    assert!(active, "the fresh ledger never became native");

    // Standing, then a creation frame, then linearizable object reads.
    let standing = call(&host, ISSUER, envelope(1, read(NativeReadQuery::Standing))).await;
    let Response::NativeRead(page) = standing else {
        panic!("standing: {standing:?}")
    };
    assert!(matches!(page.objects[..], [NativeObject::Standing(_)]));
    let claim_document = json!({
        "description": "Replicated native claim.",
        "target": format!("{WORKER}"),
        "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}]
    });
    let create = frame(
        ISSUER,
        2,
        "claim.submit",
        claim_document,
        &Resolved::default(),
    );
    let claim = ClaimId::from_u128(2001);
    let mut reply = call(
        &host,
        ISSUER,
        envelope(
            2,
            Operation::Native {
                frame: create.clone(),
            },
        ),
    )
    .await;
    for _ in 0..50 {
        if !matches!(reply, Response::Error(AccessError::OutcomeUnknown)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        reply = call(
            &host,
            ISSUER,
            envelope(
                2,
                Operation::Native {
                    frame: create.clone(),
                },
            ),
        )
        .await;
    }
    let Response::Native(NativeMutationReply::Committed(receipt)) = reply else {
        panic!("create: {reply:?}")
    };
    assert_eq!(receipt.counts.created, 1);
    let objects = call(
        &host,
        ISSUER,
        envelope(
            3,
            read(NativeReadQuery::Objects(vec![NativeObjectRef::Claim(
                claim,
            )])),
        ),
    )
    .await;
    let Response::NativeRead(page) = objects else {
        panic!("objects: {objects:?}")
    };
    assert!(
        matches!(page.objects[..], [NativeObject::Claim(_)]),
        "{page:?}"
    );
    let again = call(&host, ISSUER, envelope(4, read(NativeReadQuery::Standing))).await;
    assert!(matches!(again, Response::NativeRead(_)), "{again:?}");

    // Post, then the second participant acquires the receipt under the
    // claim's committed binding, both compiled from one read of the claim.
    let resolved = |page: &NativeReadPage| Resolved::from_objects(ledger(), &page.objects).unwrap();
    let post = frame(
        ISSUER,
        5,
        "claim.post",
        json!({"claim": format!("{claim}")}),
        &resolved(&page),
    );
    let mut reply = call(
        &host,
        ISSUER,
        envelope(
            5,
            Operation::Native {
                frame: post.clone(),
            },
        ),
    )
    .await;
    for _ in 0..50 {
        if !matches!(reply, Response::Error(AccessError::OutcomeUnknown)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        reply = call(
            &host,
            ISSUER,
            envelope(
                5,
                Operation::Native {
                    frame: post.clone(),
                },
            ),
        )
        .await;
    }
    assert!(
        matches!(reply, Response::Native(NativeMutationReply::Committed(_))),
        "post: {reply:?}"
    );
    let Response::NativeRead(posted) = call(
        &host,
        WORKER,
        envelope(
            6,
            read(NativeReadQuery::Objects(vec![NativeObjectRef::Claim(
                claim,
            )])),
        ),
    )
    .await
    else {
        panic!("posted read")
    };
    let acquire = frame(
        WORKER,
        7,
        "receipt.acquire",
        json!({"claim": format!("{claim}")}),
        &resolved(&posted),
    );
    let mut reply = call(
        &host,
        WORKER,
        envelope(
            7,
            Operation::Native {
                frame: acquire.clone(),
            },
        ),
    )
    .await;
    for _ in 0..50 {
        if !matches!(reply, Response::Error(AccessError::OutcomeUnknown)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        reply = call(
            &host,
            WORKER,
            envelope(
                7,
                Operation::Native {
                    frame: acquire.clone(),
                },
            ),
        )
        .await;
    }
    let Response::Native(NativeMutationReply::Committed(receipt)) = reply else {
        panic!("acquire: {reply:?}")
    };
    assert_eq!(receipt.counts.receipts, 1, "{receipt:?}");

    // Movement through the host (25 §6): the one member moves to this node's
    // replica, every step a session decision, the readiness fact stated by
    // the replica itself from its committed rows and verified against the
    // authority's digest of the frozen member, then activation and cleanup.
    let view = host.range_view(false).await.unwrap();
    assert_eq!(view.epoch.0, 1);
    assert_eq!(view.members.len(), 1);
    assert!(view.members[0].holder.is_none());
    assert!(view.pending.is_none() && view.history.is_empty());
    let member = view.members[0].id;
    let replica = focal_ranges::ReplicaId {
        node: 1,
        generation: 1,
    };
    let operation = host.move_range(member, replica).await.unwrap();
    // The same request names the same transfer.
    assert_eq!(host.move_range(member, replica).await.unwrap(), operation);
    let mut pending = None;
    for _ in 0..200 {
        let view = host.range_view(false).await.unwrap();
        if let Some(found) = view.pending
            && !view.in_flight
        {
            pending = Some(found);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let pending = pending.expect("the begin record applied");
    assert_eq!(pending.operation, operation);
    assert_eq!(pending.replacements.len(), 1);
    assert_eq!(pending.replacements[0].holder, Some(replica));
    let target = pending.replacements[0].id;
    // Seed identity, then the barrier.
    let digests = host.range_view(true).await.unwrap();
    let digest = digests.members[0].digest.unwrap();
    host.propose_range(focal_ranges::RangeOperation::Snapshot {
        operation,
        range: target,
        hash: digest,
    })
    .await
    .unwrap();
    async fn settled(host: &ReplicaHost) -> RangeView {
        for _ in 0..200 {
            let view = host.range_view(false).await.unwrap();
            if !view.in_flight {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("a movement record never applied");
    }
    let view = settled(&host).await;
    assert_eq!(view.pending.as_ref().unwrap().snapshots, vec![target]);
    host.propose_range(focal_ranges::RangeOperation::Barrier { operation })
        .await
        .unwrap();
    let view = settled(&host).await;
    assert!(view.pending.as_ref().unwrap().barrier.is_some());
    // Fenced: a mutation on the moving member is refused, retryable.
    let fenced = frame(
        ISSUER,
        8,
        "claim.submit",
        json!({
            "description": "Fenced while moving.",
            "target": format!("{WORKER}"),
            "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}]
        }),
        &Resolved::default(),
    );
    let refused = call(
        &host,
        ISSUER,
        envelope(
            8,
            Operation::Native {
                frame: fenced.clone(),
            },
        ),
    )
    .await;
    assert!(
        matches!(
            refused,
            Response::Native(NativeMutationReply::Refused(_))
                | Response::Error(AccessError::Unavailable | AccessError::Capacity)
        ),
        "fenced: {refused:?}"
    );
    // The replica states its readiness; the authority verifies it against
    // its own digest and attests it; a forged digest is refused.
    let fact = host
        .range_fact(RangeFactRequest::Ready {
            operation,
            range: target,
        })
        .await
        .unwrap();
    let verifier = focal_ledger::LedgerRangeVerifier::new(view.genesis);
    let digests = host.range_view(true).await.unwrap();
    let RangeFact::Ready(ready) = fact.clone() else {
        panic!("ready fact")
    };
    let mut forged = ready.clone();
    forged.state = focal_model::ContentHash([9; 32]);
    assert!(crate::fleet::verify_fact(&verifier, &digests, 1, RangeFact::Ready(forged)).is_err());
    assert!(crate::fleet::verify_fact(&verifier, &digests, 2, fact.clone()).is_err());
    let step = crate::fleet::verify_fact(&verifier, &digests, 1, fact).unwrap();
    host.propose_range(step).await.unwrap();
    let view = settled(&host).await;
    assert_eq!(view.pending.as_ref().unwrap().ready, vec![target]);
    host.activate_range(vec![]).await.unwrap();
    let view = settled(&host).await;
    assert_eq!(view.epoch.0, 2);
    assert!(view.pending.is_none());
    assert_eq!(view.members.len(), 1);
    assert_eq!(view.members[0].id, target);
    assert_eq!(view.members[0].holder, Some(replica));
    assert_eq!(view.history.len(), 1);
    assert_eq!(view.refusals, 0);
    // Admission reopens and the retired map is cleaned up.
    let mut reply = call(
        &host,
        ISSUER,
        envelope(
            8,
            Operation::Native {
                frame: fenced.clone(),
            },
        ),
    )
    .await;
    for _ in 0..50 {
        if !matches!(reply, Response::Error(AccessError::OutcomeUnknown)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        reply = call(
            &host,
            ISSUER,
            envelope(
                8,
                Operation::Native {
                    frame: fenced.clone(),
                },
            ),
        )
        .await;
    }
    assert!(
        matches!(reply, Response::Native(NativeMutationReply::Committed(_))),
        "after activation: {reply:?}"
    );
    let mut recovery = focal_ranges::RecoveryProof {
        ledger: ledger(),
        epoch: view.epoch,
        through: view.prefix,
        manifest: view.history[0].proofs,
        attestation: focal_model::ContentHash([0; 32]),
    };
    verifier.attest_recovery(&mut recovery).unwrap();
    host.propose_range(focal_ranges::RangeOperation::Cleanup {
        operation,
        recovery,
    })
    .await
    .unwrap();
    let view = settled(&host).await;
    assert!(view.history.is_empty());
    // A layout change through the host (25 §8): the member divides at an
    // affinity near its middle, the map follows with the new member under
    // the parent's holder, and a merge joins them back.
    let member = view.members[0].id;
    assert!(view.members[0].entries > 1);
    let at = host
        .split_point(member)
        .await
        .unwrap()
        .expect("rows under several affinities divide");
    host.propose_layout(focal_ledger::LayoutOperation::Split {
        at,
        id: RangeId::from_u128(0x5eed),
    })
    .await
    .unwrap();
    let mut split = None;
    for _ in 0..200 {
        let view = host.range_view(false).await.unwrap();
        if view.members.len() == 2 {
            split = Some(view);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let split = split.expect("the split applied");
    assert_eq!(split.epoch.0, 3);
    assert_eq!(split.members[1].id, RangeId::from_u128(0x5eed));
    assert_eq!(split.members[1].start, Some(at));
    assert!(
        split
            .members
            .iter()
            .all(|member| member.holder == Some(replica))
    );
    assert!(split.members.iter().all(|member| member.entries > 0));
    host.propose_layout(focal_ledger::LayoutOperation::Merge { left: member })
        .await
        .unwrap();
    let mut merged = None;
    for _ in 0..200 {
        let view = host.range_view(false).await.unwrap();
        if view.members.len() == 1 {
            merged = Some(view);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let merged = merged.expect("the merge applied");
    assert_eq!(merged.epoch.0, 4);
    assert_eq!(merged.members[0].entries, view.members[0].entries);
    // The log's retirement boundary (26 §3): with far more than four entries
    // applied, the replica has checkpointed by cadence and keeps fewer than
    // four past its snapshot; the retention floor names the archive as the
    // blocker while nothing is archived.
    let mut compacted = false;
    for _ in 0..200 {
        let diagnostics = host.diagnostics().await.unwrap();
        let value = diagnostics.value();
        if value.applied_index > 4 && value.log_entries_since_checkpoint < 4 {
            let retention = value.retention.as_ref().expect("a native session's floor");
            assert_eq!(retention.archived, 0);
            assert_eq!(retention.floor, 0);
            assert_eq!(retention.blocker, "archive");
            assert!(retention.published > 0);
            compacted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(compacted, "the replica never checkpointed by cadence");

    host.stop().await.unwrap();
    owner.join().unwrap();
    content.stop().await.unwrap();
    content_owner.join().unwrap();
    drop(wal);
}
