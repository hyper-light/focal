//! A populated legacy ledger imported through the node path: the replica
//! worker hands its inline legacy payloads to the exclusive content writer,
//! which seals them with the canonical chunking; the worker then proposes the
//! import under the recorded root, applies it, and diagnostics report the
//! native ledger. Proposing before sealing is a typed custody refusal.
use super::*;
use crate::content_host::ContentHost;
use crate::custody::CustodyConfig;
use focal_consensus::{DurableNode, NodeConfig};
use focal_evidence::{ContentReader, ContentStore, StoreLimits};
use focal_ledger::{
    ActivationKind, LedgerActivation, NativeHosting, NativeSessionError, NativeSessionLimits,
    SessionLimits,
};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::RangeId;
use focal_model::{
    ActionType, ArtifactContent, ArtifactId, ArtifactPayload, AuthenticatedInput, AuthorityContext,
    CanonicalContent, Cause, ClaimContent, ClaimId, Command, Confidence, ContentDomainId,
    EvidenceAttestation, EvidenceSetId, NewArtifact, NewClaim, NewValidation, OccurrenceId,
    OutcomeKind, ReceiptId, Relation, RelationKind, RelationTarget, RequirementRef, RootCommandId,
    SCHEMA_MAJOR, TestamentId, ValidationContent, ValidationId, ValidationKind, ValidationMode,
    ValidationPhase,
};
use std::collections::BTreeSet;

const CLUSTER: [u8; 16] = [151; 16];
const ISSUER: ParticipantId = ParticipantId::from_u128(91);
const WORKER: ParticipantId = ParticipantId::from_u128(92);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(93);
const CHUNK: usize = 8;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(151),
        session: SessionId::from_u128(1),
    }
}
fn domain() -> ContentDomainId {
    ContentDomainId(ledger().tenant.0)
}
fn input(n: u128, actor: ParticipantId, command: Command) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: ledger(),
        principal: actor,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(n),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(30)),
            policy_revision: 1,
            logical_time: 1000,
            evidence: Vec::new(),
        },
        command,
    }
}
fn new_claim(id: u128) -> NewClaim {
    let cid = ClaimId::from_u128(id);
    let vid = ValidationId::from_u128(id + 100_000);
    let validation = ValidationContent {
        ledger: ledger(),
        schema: SCHEMA_MAJOR,
        claim: cid,
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        description: "acknowledged receipt".into(),
        quality_bar: None,
        evaluator: EVALUATOR,
        handlers: Vec::new(),
        evidence_schemas: BTreeSet::new(),
        contributed_by: BTreeSet::from([ISSUER]),
        policy_revision: 1,
    };
    NewClaim {
        id: cid,
        content: ClaimContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            occurrence: OccurrenceId::from_u128(id),
            description: "legacy work".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(ISSUER),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(WORKER),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(RootCommandId::from_u128(30)),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: vec![RequirementRef {
                id: vid,
                specification: validation.specification_hash().unwrap(),
            }],
            deadline: None,
        },
        validations: vec![NewValidation {
            id: vid,
            content: validation,
        }],
    }
}
fn legacy(session: &mut Session, request: AuthenticatedInput) {
    let key = RequestKey {
        principal: request.principal,
        epoch: request.request_epoch,
        id: request.request_id,
    };
    match session.propose(&request).unwrap() {
        focal_ledger::Submission::Committed(_) => return,
        focal_ledger::Submission::Pending(_) => {}
        focal_ledger::Submission::Domain(outcome) => panic!("refused: {outcome:?}"),
    }
    for _ in 0..16 {
        session.poll().unwrap();
        if session.receipt(&key).is_some() {
            return;
        }
    }
    panic!("legacy command never committed");
}
/// One satisfied claim whose closed testament carries an inline artifact.
fn populate(session: &mut Session) {
    for (i, principal) in [ISSUER, WORKER, EVALUATOR].into_iter().enumerate() {
        legacy(
            session,
            input(
                900 + i as u128,
                principal,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1),
                },
            ),
        );
    }
    let claim = new_claim(1);
    let cid = claim.id;
    legacy(
        session,
        input(1_000, ISSUER, Command::GenerateClaim { claim }),
    );
    legacy(
        session,
        input(1_001, ISSUER, Command::PostClaim { claim: cid }),
    );
    legacy(
        session,
        input(
            1_002,
            WORKER,
            Command::AcquireReceipt {
                claim: cid,
                receipt: ReceiptId::from_u128(11),
                epoch: 1,
            },
        ),
    );
    let fence = session.read_at_least(SessionSeq(0)).unwrap().claims[&cid]
        .lifecycle()
        .receipt
        .as_ref()
        .unwrap()
        .fence;
    let set = EvidenceSetId::from_u128(51);
    legacy(
        session,
        input(
            1_003,
            WORKER,
            Command::BeginEvidenceSet {
                claim: cid,
                receipt: fence,
                evidence_set: set,
            },
        ),
    );
    let artifact = NewArtifact {
        id: ArtifactId::from_u128(61),
        content: ArtifactContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            kind: "test_report".into(),
            schema_hash: focal_evidence::test_report_schema(),
            metadata: Vec::new(),
            payload: ArtifactPayload::Inline(br#"{"passed":3,"failed":0,"skipped":0}"#.to_vec()),
            producer: WORKER,
            receipt: Some(fence),
            inputs: BTreeSet::new(),
            visibility: BTreeSet::new(),
        },
    };
    let mut attach = input(
        1_004,
        WORKER,
        Command::AttachArtifact {
            claim: cid,
            receipt: fence,
            evidence_set: set,
            artifact: artifact.clone(),
        },
    );
    attach.authority.evidence.push(EvidenceAttestation {
        descriptor_hash: artifact.content.content_hash().unwrap(),
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    });
    legacy(session, attach);
    let reference = focal_model::ArtifactRef {
        id: artifact.id,
        hash: artifact.content.content_hash().unwrap(),
    };
    legacy(
        session,
        input(
            1_005,
            WORKER,
            Command::CloseTestament {
                claim: cid,
                receipt: fence,
                testament: TestamentId::from_u128(71),
                evidence_set: set,
                manifest: vec![reference],
                summary: "finished".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
            },
        ),
    );
    legacy(
        session,
        input(
            1_006,
            ISSUER,
            Command::AcknowledgeTestament {
                claim: cid,
                testament: TestamentId::from_u128(71),
            },
        ),
    );
    legacy(
        session,
        input(
            1_007,
            ISSUER,
            Command::BeginWholeWorkValidation { claim: cid },
        ),
    );
    legacy(
        session,
        input(1_008, ISSUER, Command::CompleteWholeWork { claim: cid }),
    );
}

/// The successor floor and the local promise become durable on later polls;
/// the ledger harness retries the same two conditions.
async fn activate(host: &ReplicaHost, call: ActivateNativeCall) -> Result<(), LedgerError> {
    let mut last = Err(LedgerError::Failed);
    for _ in 0..200 {
        last = host.activate_native(call).await;
        match &last {
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::PersistencePending))
            | Err(LedgerError::Managed(focal_ledger::ManagedError::Unsupported)) => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            _ => return last,
        }
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_populated_ledger_is_imported_through_the_node_after_its_host_seals_the_payloads() {
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
            chunk_bytes: CHUNK,
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
    populate(&mut session);
    assert!(session.legacy_populated());
    let mut config = ReplicaConfig::new(RootCommandId::from_u128(151));
    config.tick = Duration::from_millis(20);
    config.request_timeout = Duration::from_millis(500);
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
    assert_eq!(chunking.0, CHUNK);
    let call = ActivateNativeCall {
        profile: focal_ledger::NativeContentProfile::ProjectionOnly,
        chunk_bytes: chunking.0,
        max_manifest_bytes: chunking.1,
    };
    // Without sealed custody the proposal is refused; nothing is proposed.
    assert!(matches!(
        activate(&host, call).await,
        Err(LedgerError::Native(NativeSessionError::CustodyPending))
    ));
    let import = host
        .import_payloads()
        .await
        .unwrap()
        .expect("populated prefix");
    assert_eq!(import.domain, domain());
    assert_eq!(import.payloads.len(), 1);
    for bytes in import.payloads {
        content
            .seal_import_inline(import.domain, bytes, chunking.0)
            .await
            .unwrap();
    }
    activate(&host, call).await.unwrap();
    let mut active = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let diagnostics = host.diagnostics().await.unwrap();
        if diagnostics.value().native_active && diagnostics.value().native_authoritative {
            active = true;
            assert!(!diagnostics.value().native_import_pending);
            break;
        }
    }
    assert!(active, "the imported ledger never became native");
    assert!(host.progress().import_pending.is_none());
    host.stop().await.unwrap();
    owner.join().unwrap();
    content.stop().await.unwrap();
    content_owner.join().unwrap();
    drop(wal);
    let _ = LedgerActivation::V1;
    let _ = ActivationKind::Imported;
}
