use super::*;
use crate::{
    content_host::ContentHost,
    custody::{CustodyConfig, CustodyPolicy},
};
use focal_consensus::NodeConfig;
use focal_directory::*;
use focal_evidence::{ContentStore, StoreLimits, UploadId};
use focal_ledger::{Session, SessionLimits, SessionPlacementRequest, Submission};
use focal_model::*;
use focal_wire::WireLimits;
use std::collections::{BTreeMap, BTreeSet};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn input(id: u128, command: Command) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: ledger(),
        principal: ParticipantId::from_u128(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(1)),
            policy_revision: 1,
            logical_time: 0,
            evidence: vec![],
        },
        command,
    }
}
fn register(session: &mut Session, id: u128, payload: ArtifactPayload) {
    let artifact = NewArtifact {
        id: ArtifactId::from_u128(id),
        content: ArtifactContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            kind: "document".into(),
            schema_hash: ContentHash([9; 32]),
            metadata: Vec::new(),
            payload,
            producer: ParticipantId::from_u128(1),
            receipt: None,
            inputs: BTreeSet::new(),
            visibility: BTreeSet::new(),
        },
    };
    let hash = artifact.content.content_hash().unwrap();
    let mut request = input(id, Command::RegisterArtifact { artifact });
    // Intentionally supplied trusted admission fixture: verified-through custody
    // must independently read the bytes, even when an earlier attestation exists.
    request.authority.evidence.push(EvidenceAttestation {
        descriptor_hash: hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    });
    assert!(matches!(
        session.submit_local(&request).unwrap(),
        Submission::Committed(_)
    ));
}
fn place(session: &mut Session, route: u64) -> SessionPlacementRequest {
    let members = BTreeMap::from([(1, 1)]);
    let request = SessionPlacementRequest {
        expected_index: session.placement().map_or(0, |fence| fence.index.0),
        expected_configuration_index: session.membership().unwrap().configuration_index,
        operation: OperationId::from_u128(u128::from(route)),
        kind: if route == 1 {
            SessionFenceKind::Created
        } else {
            SessionFenceKind::Cutover
        },
        from_route: RouteEpoch(route - 1),
        to_route: RouteEpoch(route),
        membership_epoch: 1,
        placement_epoch: route,
        placement: PlacementSpec {
            policy: PlacementPolicy {
                durability: DurabilityIntent {
                    survive: FailureClass::Node,
                    max_failures: 0,
                },
                residency: BTreeSet::new(),
                home_regions: BTreeSet::new(),
                required_memory: 0,
            },
            placement: Placement {
                preferred_leader: 1,
                voters: members.clone(),
                materializers: members.clone(),
                content_copies: members,
            },
        },
    };
    session.propose_placement(&request).unwrap();
    for _ in 0..6 {
        session.poll().unwrap();
    }
    assert!(session.placement_receipt(&request).unwrap().is_some());
    request
}
fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 64,
        max_staging_bytes: 128,
        max_uploads: 4,
        chunk_bytes: 4,
        max_manifest_bytes: 4096,
    }
}
fn memory() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap()
}
fn policy(route: u64) -> CustodyPolicy {
    CustodyPolicy {
        ledger: ledger(),
        route_epoch: RouteEpoch(route),
        policy_revision: route,
        peers: BTreeSet::from([1]),
    }
}
fn fixture(path: &std::path::Path) -> (Session, ContentStore, ContentRef) {
    let mut config = SessionLimits::default();
    // Reference projection must not clone the artifact or be capped by the
    // ordinary response-byte limit: even a one-byte read bound remains valid.
    config.graph.range.max_query_bytes = 1;
    let mut session = Session::open(
        path.join("wal"),
        ledger(),
        NodeConfig::single(1, [8; 16], ledger().session.0),
        config,
    )
    .unwrap();
    session.campaign().unwrap();
    for _ in 0..6 {
        session.poll().unwrap();
    }
    place(&mut session, 1);
    assert!(matches!(
        session
            .submit_local(&input(
                1,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1)
                }
            ))
            .unwrap(),
        Submission::Committed(_)
    ));
    let mut store = ContentStore::open(path.join("content"), limits()).unwrap();
    let upload = UploadId([7; 16]);
    store
        .begin(
            upload,
            ContentDomainId(ledger().tenant.0),
            ContentClass::Evidence,
            12,
            None,
        )
        .unwrap();
    for (index, part) in b"abcdefghijkl".chunks(4).enumerate() {
        store.append(upload, (index * 4) as u64, part).unwrap();
    }
    let reference = store.seal(upload).unwrap();
    register(&mut session, 10, ArtifactPayload::Inline(vec![42; 8192]));
    register(
        &mut session,
        20,
        ArtifactPayload::Content(reference.clone()),
    );
    (session, store, reference)
}
fn finish(
    mut state: Box<CustodyVerification>,
    owner: &mut CustodyStore,
    budget: &MemoryBudget,
) -> Result<VerifiedCustody, AccessError> {
    for _ in 0..64 {
        match state.advance(owner, budget)? {
            CustodyVerificationProgress::Pending(next) => state = next,
            CustodyVerificationProgress::Complete(witness) => return Ok(*witness),
        }
    }
    panic!("bounded fixture verification did not finish");
}
fn chunk_path(path: &std::path::Path, bytes: &[u8]) -> std::path::PathBuf {
    let domain: String = ledger()
        .tenant
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    path.join("content/objects").join(domain).join(format!(
        "{}.chunk",
        ContentHash(*blake3::hash(bytes).as_bytes())
    ))
}

#[tokio::test]
async fn exact_checkpoint_and_all_artifacts_survive_advance_compaction_and_content_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (mut session, store, reference) = fixture(dir.path());
    let snapshot = session.checkpoint_evidence(0, 30_000).unwrap();
    let prefix = snapshot.prefix().clone();
    assert_eq!(prefix.artifacts, 2);
    let checkpoint = snapshot.checkpoint().to_vec();
    // Newer materialized state contains an additional absent content object.
    // The already captured prefix must remain exactly its previous artifact set.
    let mut absent = reference;
    absent.root = ContentHash([211; 32]);
    register(&mut session, 30, ArtifactPayload::Content(absent));
    let cutover = place(&mut session, 2);
    session.checkpoint().unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        store,
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let witness = host.verify_prefix(snapshot, None).await.unwrap();
    assert_eq!(witness.manifest().prefix, prefix);
    assert!(witness.manifest().prefix.sequence < session.sequence());
    assert_eq!(witness.manifest().verified_artifacts, 2);
    assert_eq!(witness.manifest().verified_content_references, 1);
    assert_eq!(witness.manifest().verified_content_bytes, 12);
    assert!(
        witness
            .verify_cutover(&session.placement_witness(&cutover).unwrap().unwrap())
            .is_err()
    );
    let next = session.checkpoint_evidence(30_000, 30_000).unwrap();
    assert!(host.verify_prefix(next, None).await.is_err());
    let manifest_hash = witness.digest();
    drop(witness);
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(host);
    assert_eq!(budget.stats().used, 0);
    drop(session);
    let reopened = ContentStore::open(dir.path().join("content"), limits()).unwrap();
    let durable_checkpoint = reopened
        .read_custody_record(
            CustodyRecordKind::Checkpoint,
            prefix.checkpoint,
            8 * 1024 * 1024,
        )
        .unwrap();
    assert_eq!(durable_checkpoint, checkpoint);
    let bytes = reopened
        .read_custody_record(CustodyRecordKind::Manifest, manifest_hash, 4096)
        .unwrap();
    let (manifest, rest): (CustodyPrefixManifest, _) = postcard::take_from_bytes(&bytes).unwrap();
    assert!(rest.is_empty());
    assert_eq!(manifest.prefix, prefix);
    assert_eq!(manifest.verified_artifacts, 2);
}

#[test]
fn missing_and_corrupt_last_chunks_never_produce_an_empty_or_partial_seal() {
    for missing in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (mut session, store, _) = fixture(dir.path());
        let snapshot = session.checkpoint_evidence(0, 30_000).unwrap();
        let path = chunk_path(dir.path(), b"ijkl");
        if missing {
            std::fs::remove_file(path).unwrap();
        } else {
            std::fs::write(path, b"BAD!").unwrap();
        }
        let budget = memory();
        let mut owner = CustodyStore::new(store, CustodyConfig::new(1), budget.clone()).unwrap();
        let state = CustodyVerification::new(snapshot, None, &budget).unwrap();
        assert!(finish(Box::new(state), &mut owner, &budget).is_err());
        assert_eq!(
            std::fs::read_dir(dir.path().join("content/custody"))
                .unwrap()
                .count(),
            0
        );
        assert_eq!(budget.stats().used, 0);
    }
}

#[test]
fn preparing_scope_keeps_active_ingress_and_rejects_policy_change_mid_verification() {
    let dir = tempfile::tempdir().unwrap();
    let (mut session, store, _) = fixture(dir.path());
    let cutover = place(&mut session, 2);
    let budget = memory();
    let mut owner = CustodyStore::new(store, CustodyConfig::new(1), budget.clone()).unwrap();
    owner.install_policy(policy(1)).unwrap();
    let snapshot = session.checkpoint_evidence(0, 30_000).unwrap();
    let state = CustodyVerification::new(snapshot, Some(policy(1).scope()), &budget).unwrap();
    let witness = finish(Box::new(state), &mut owner, &budget).unwrap();
    assert_eq!(witness.manifest().prefix.route, RouteEpoch(2));
    assert_eq!(
        witness.manifest().prefix.sequence,
        session.placement().unwrap().sequence
    );
    witness
        .verify_cutover(&session.placement_witness(&cutover).unwrap().unwrap())
        .unwrap();
    assert_eq!(owner.installed(ledger()), Some(&policy(1)));
    drop(witness);

    let state = CustodyVerification::new(
        session.checkpoint_evidence(30_000, 30_000).unwrap(),
        Some(policy(1).scope()),
        &budget,
    )
    .unwrap();
    let CustodyVerificationProgress::Pending(state) =
        Box::new(state).advance(&mut owner, &budget).unwrap()
    else {
        panic!("checkpoint slice")
    };
    owner
        .replace_policy(Some(policy(1).scope()), policy(2))
        .unwrap();
    assert!(matches!(
        state.advance(&mut owner, &budget),
        Err(AccessError::Unavailable)
    ));
    assert_eq!(owner.installed(ledger()), Some(&policy(2)));
}

#[tokio::test]
async fn canceled_continuation_releases_owned_charge_and_expired_or_dropped_snapshot_fails() {
    let dir = tempfile::tempdir().unwrap();
    let (mut session, store, _) = fixture(dir.path());
    let clock = std::time::Instant::now();
    let now = || u64::try_from(clock.elapsed().as_millis()).unwrap();
    let budget = memory();
    let (host, owner) = ContentHost::spawn(
        store,
        CustodyConfig::new(1),
        WireLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let baseline = budget.stats().used;
    let progress = host
        .begin_verification(session.checkpoint_evidence(now(), 30_000).unwrap(), None)
        .await
        .unwrap();
    assert!(matches!(progress, CustodyVerificationProgress::Pending(_)));
    assert_eq!(budget.stats().used, baseline + RESIDENT_BYTES);
    drop(progress);
    assert_eq!(budget.stats().used, baseline);

    let expired_snapshot = session.checkpoint_evidence(now(), 30_000).unwrap();
    // Expire the owner's actual registry before a new continuation is created.
    // This has no fast-fsync assumption. Elapsed capture TTL during stalled IO
    // is separately exercised by the grouped physical-writer regression.
    let expired_at = expired_snapshot.expires_at();
    session.advance_read_clock(expired_at).unwrap();
    assert!(matches!(
        host.verify_prefix(expired_snapshot, None).await,
        Err(AccessError::SnapshotExpired)
    ));
    assert_eq!(budget.stats().used, baseline);
    let snapshot = session
        .checkpoint_evidence(expired_at + now(), 30_000)
        .unwrap();
    drop(session);
    assert!(matches!(
        host.verify_prefix(snapshot, None).await,
        Err(AccessError::SnapshotExpired)
    ));
    assert_eq!(budget.stats().used, baseline);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[test]
fn admission_pressure_and_wrong_physical_owner_fail_before_sealing() {
    let dir = tempfile::tempdir().unwrap();
    let (mut session, store, _) = fixture(dir.path());
    let clock = std::time::Instant::now();
    let now = || u64::try_from(clock.elapsed().as_millis()).unwrap();
    let budget = MemoryBudget::new(RESIDENT_BYTES - 1, 0).unwrap();
    assert!(matches!(
        CustodyVerification::new(
            session.checkpoint_evidence(now(), 30_000).unwrap(),
            None,
            &budget
        ),
        Err(AccessError::Capacity)
    ));
    assert_eq!(budget.stats().used, 0);
    assert_eq!(
        std::fs::read_dir(dir.path().join("content/checkpoints"))
            .unwrap()
            .count(),
        0
    );
    let budget = memory();
    let mut owner = CustodyStore::new(store, CustodyConfig::new(2), budget.clone()).unwrap();
    let state = CustodyVerification::new(
        session.checkpoint_evidence(now(), 30_000).unwrap(),
        None,
        &budget,
    )
    .unwrap();
    assert!(matches!(
        Box::new(state).advance(&mut owner, &budget),
        Err(AccessError::Unavailable)
    ));
    assert_eq!(
        std::fs::read_dir(dir.path().join("content/custody"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(budget.stats().used, 0);
}
