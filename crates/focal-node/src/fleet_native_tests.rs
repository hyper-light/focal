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
            range: RangeId(1),
        },
    )
    .unwrap();
    session.campaign().unwrap();
    for _ in 0..8 {
        session.poll().unwrap();
    }
    let mut config = ReplicaConfig::new(RootCommandId::from_u128(152));
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

    host.stop().await.unwrap();
    owner.join().unwrap();
    content.stop().await.unwrap();
    content_owner.join().unwrap();
    drop(wal);
}
