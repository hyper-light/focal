use super::*;
use crate::native::record_codec::RowFamily;
use crate::native::record_codec::checkpoint::EncodingPlan;
use crate::tests::{ISSUER, WORKER, input, ledger, new_claim, setup};
use crate::{ApplyResult, Core as LegacyCore};
use focal_evidence::{BuiltinSchemaError, ContentStore, StoreLimits, UploadId};
use focal_model::CanonicalContent;
use focal_model::lifecycle::{
    aggregation, artifact_descriptor, claim::ClaimOrigin, claim_descriptor,
    evidence::ResponseLimits, validation, validation_descriptor,
};
use focal_model::{
    ArtifactContent, ArtifactPayload, AuthenticatedInput, ClaimStatus, Command, Confidence,
    ContentClass, ContentDomainId, Deadline, EvidenceAttestation, EvidenceSetId, NewArtifact,
    ObjectRef, OutcomeKind, Relation, RelationTarget, SCHEMA_MAJOR, WaitPredicate,
};

struct AnySchema;
impl NativeSchemaVerifier for AnySchema {
    fn maximum_bytes(&self, _: ContentHash) -> Result<usize, BuiltinSchemaError> {
        Ok(1 << 20)
    }
    fn verify(&self, _: ContentHash, _: &[u8]) -> Result<(), BuiltinSchemaError> {
        Ok(())
    }
}

const CHUNK: usize = 8;
fn domain() -> ContentDomainId {
    ContentDomainId::from_u128(77)
}
fn open_store(dir: &std::path::Path) -> ContentStore {
    ContentStore::open(
        dir.join("content"),
        StoreLimits {
            max_content_bytes: 64 << 20,
            max_staging_bytes: 128 << 20,
            max_uploads: 16,
            chunk_bytes: CHUNK,
            max_manifest_bytes: 1 << 20,
        },
    )
    .unwrap()
}
fn limits() -> recovery::Limits {
    let declaration = validation::Limits {
        handlers: 32,
        attempts: 64,
        slot_bytes: 4096,
    };
    recovery::Limits {
        native: NativeLimits::default(),
        acceptance: aggregation::Limits {
            max_slots: 256,
            max_checks: 4096,
            max_results: 8192,
            max_updates: 8192,
        },
        artifact: artifact_descriptor::Limits {
            kind_bytes: 1024,
            metadata_bytes: 65_536,
            inline_bytes: 1024 * 1024,
            inputs: 256,
            visibility_labels: 256,
            visibility_label_bytes: 4096,
            construction_bytes: 4 * 1024 * 1024,
        },
        claim: claim_descriptor::Limits {
            description_bytes: 65_536,
            relations: 4096,
            scopes: 256,
            scope_key_bytes: 4096,
            requirements: 4096,
            slots: 256,
            checks: 4096,
            construction_bytes: 4 * 1024 * 1024,
        },
        declaration,
        validation: validation_descriptor::Limits {
            declaration,
            description_bytes: 65_536,
            quality_bar_bytes: 65_536,
            contributors: 256,
            construction_bytes: 4 * 1024 * 1024,
        },
        response: ResponseLimits {
            artifacts: 256,
            diagnostics: 256,
            summary_bytes: 65_536,
            construction_bytes: 4 * 1024 * 1024,
        },
        creation_objects: 4096,
        work: recovery::Work {
            parsing: 1 << 30,
            source: 1 << 30,
            model: 1 << 30,
            lookup: 1 << 30,
        },
    }
}
fn request(limits: &recovery::Limits) -> ImportRequest<'_> {
    ImportRequest {
        ledger: ledger(),
        logical_time: 5_000,
        intent: ContentHash([7; 32]),
        content_domain: domain(),
        chunk_bytes: CHUNK,
        max_manifest_bytes: 1 << 20,
        range: RangeId(41),
        limits,
        encoding: EncodingLimits {
            bytes: 8 << 20,
            visits: 1 << 30,
            rows: 1 << 20,
        },
        inspection: InspectionLimits {
            bytes: 8 << 20,
            visits: 1 << 30,
            rows: 1 << 20,
            row_bytes: 4 << 20,
        },
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 << 20, 64 << 20).unwrap()
}
fn apply(core: &mut LegacyCore, request: AuthenticatedInput) -> ApplyResult {
    let prepared = core
        .prepare(&request)
        .unwrap_or_else(|error| panic!("prepare {}: {error:?}", request.command.code()));
    core.apply(SessionSeq(core.sequence().0 + 1), prepared)
        .unwrap()
}
fn attested(mut request: AuthenticatedInput, artifact: &NewArtifact) -> AuthenticatedInput {
    request.authority.evidence.push(EvidenceAttestation {
        descriptor_hash: artifact.content.content_hash().unwrap(),
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    });
    request
}
fn artifact(id: u128, payload: ArtifactPayload, receipt: Option<ReceiptFence>) -> NewArtifact {
    NewArtifact {
        id: ArtifactId::from_u128(id),
        content: ArtifactContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            kind: "test_report".into(),
            schema_hash: ContentHash([1; 32]),
            metadata: b"meta".to_vec(),
            payload,
            producer: WORKER,
            receipt,
            inputs: std::collections::BTreeSet::new(),
            visibility: std::collections::BTreeSet::from(["team".to_string()]),
        },
    }
}

/// A populated legacy history: a satisfied claim with an inline artifact, a
/// closed testament and a validation run; a posted dependent with monitors;
/// a cancelled and released claim; a superseded claim; a standalone artifact
/// backed by uploaded content.
fn legacy(store: &mut ContentStore) -> LegacyCore {
    let mut core = setup();
    let c1 = new_claim(1);
    let id1 = c1.id;
    apply(
        &mut core,
        input(1_000, ISSUER, Command::GenerateClaim { claim: c1 }),
    );
    let mut c2 = new_claim(2);
    let id2 = c2.id;
    c2.content.relations.insert(Relation {
        kind: focal_model::RelationKind::DependsOn,
        target: RelationTarget::Object(ObjectRef::claim(ledger(), id1)),
    });
    apply(
        &mut core,
        input(1_001, ISSUER, Command::GenerateClaim { claim: c2 }),
    );
    apply(
        &mut core,
        input(1_002, ISSUER, Command::PostClaim { claim: id2 }),
    );
    let deadline = |timer: u128| Deadline {
        timer: TimerId::from_u128(timer),
        generation: 1,
        at: 1_000_000,
    };
    apply(
        &mut core,
        input(
            1_003,
            ISSUER,
            Command::RegisterMonitor {
                monitor: MonitorId::from_u128(31),
                owner: id2,
                roots: std::collections::BTreeSet::from([WaitPredicate::Satisfied(id1)]),
                deadline: deadline(1),
            },
        ),
    );
    apply(
        &mut core,
        input(
            1_004,
            ISSUER,
            Command::RegisterMonitor {
                monitor: MonitorId::from_u128(32),
                owner: id2,
                roots: std::collections::BTreeSet::from([
                    WaitPredicate::Terminal(id1),
                    WaitPredicate::Released(id2),
                ]),
                deadline: deadline(2),
            },
        ),
    );
    apply(
        &mut core,
        input(1_010, ISSUER, Command::PostClaim { claim: id1 }),
    );
    apply(
        &mut core,
        input(
            1_011,
            WORKER,
            Command::AcquireReceipt {
                claim: id1,
                receipt: ReceiptId::from_u128(11),
                epoch: 1,
            },
        ),
    );
    let fence = core.snapshot().claims[&id1]
        .lifecycle()
        .receipt
        .as_ref()
        .unwrap()
        .fence;
    let set = EvidenceSetId::from_u128(51);
    apply(
        &mut core,
        input(
            1_012,
            WORKER,
            Command::BeginEvidenceSet {
                claim: id1,
                receipt: fence,
                evidence_set: set,
            },
        ),
    );
    let inline = artifact(
        61,
        ArtifactPayload::Inline(b"PASS: twenty-three bytes".to_vec()),
        Some(fence),
    );
    let reference = match apply(
        &mut core,
        attested(
            input(
                1_013,
                WORKER,
                Command::AttachArtifact {
                    claim: id1,
                    receipt: fence,
                    evidence_set: set,
                    artifact: inline.clone(),
                },
            ),
            &inline,
        ),
    )
    .receipt
    .outcome
    {
        focal_model::CommandResult::Artifact(reference) => reference,
        other => panic!("{other:?}"),
    };
    apply(
        &mut core,
        input(
            1_014,
            WORKER,
            Command::CloseTestament {
                claim: id1,
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
    apply(
        &mut core,
        input(
            1_015,
            ISSUER,
            Command::AcknowledgeTestament {
                claim: id1,
                testament: TestamentId::from_u128(71),
            },
        ),
    );
    apply(
        &mut core,
        input(
            1_016,
            ISSUER,
            Command::BeginWholeWorkValidation { claim: id1 },
        ),
    );
    apply(
        &mut core,
        input(1_017, ISSUER, Command::CompleteWholeWork { claim: id1 }),
    );
    // A cancelled, released claim and a superseded one.
    let c3 = new_claim(3);
    let id3 = c3.id;
    apply(
        &mut core,
        input(1_020, ISSUER, Command::GenerateClaim { claim: c3 }),
    );
    apply(
        &mut core,
        input(
            1_021,
            ISSUER,
            Command::CancelClaim {
                claim: id3,
                reason: "not needed".into(),
            },
        ),
    );
    apply(
        &mut core,
        input(1_022, ISSUER, Command::ReleaseScope { claim: id3 }),
    );
    let c4 = new_claim(4);
    let id4 = c4.id;
    apply(
        &mut core,
        input(1_030, ISSUER, Command::GenerateClaim { claim: c4 }),
    );
    let mut c5 = new_claim(5);
    c5.content.relations.insert(Relation {
        kind: focal_model::RelationKind::Supersedes,
        target: RelationTarget::Object(ObjectRef::claim(ledger(), id4)),
    });
    apply(
        &mut core,
        input(
            1_031,
            ISSUER,
            Command::SupersedeClaim {
                predecessor: id4,
                successor: c5,
            },
        ),
    );
    // A standalone artifact whose bytes live in the content tree.
    let uploaded = upload(store, UploadId([9; 16]), b"standalone content payload");
    let standalone = artifact(62, ArtifactPayload::Content(uploaded), None);
    apply(
        &mut core,
        attested(
            input(
                1_040,
                WORKER,
                Command::RegisterArtifact {
                    artifact: standalone.clone(),
                },
            ),
            &standalone,
        ),
    );
    core
}
fn upload(store: &mut ContentStore, id: UploadId, bytes: &[u8]) -> focal_model::ContentRef {
    store
        .begin(
            id,
            domain(),
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    for (index, block) in bytes.chunks(CHUNK).enumerate() {
        store.append(id, (index * CHUNK) as u64, block).unwrap();
    }
    let reference = store.seal(id).unwrap();
    store.finish(id).unwrap();
    reference
}
fn seal_inline(store: &mut ContentStore, core: &LegacyCore) {
    for payload in inline_payloads(core.snapshot()) {
        store
            .seal_import_inline(domain(), payload.bytes, CHUNK)
            .unwrap();
    }
}
fn claim(core: &Core<NativeState>, id: ClaimId) -> &ClaimState {
    match core.state.rows.get(&Key::Claim(id)) {
        Some(Row::Claim(owned)) => owned.claim().unwrap(),
        other => panic!("claim row {id:?}: {other:?}"),
    }
}
fn count(core: &Core<NativeState>, pick: impl Fn(&Key) -> bool) -> usize {
    core.state
        .rows
        .entries()
        .filter(|entry| pick(&entry.key))
        .count()
}

#[test]
fn a_populated_legacy_prefix_imports_restores_and_reencodes_identically() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open_store(dir.path());
    let legacy = legacy(&mut store);
    seal_inline(&mut store, &legacy);
    let state = legacy.snapshot();
    let limits = limits();
    let imported = import(state, request(&limits), budget(), &store, &AnySchema).unwrap();
    let core = &imported.core;
    assert_eq!(core.native_sequence(), SessionSeq(1));
    let id = |n: u128| ClaimId::from_u128(n);
    let first = claim(core, id(1));
    assert_eq!(first.status(), ClaimStatus::Satisfied);
    assert_eq!(first.origin(), ClaimOrigin::Legacy);
    assert_eq!(first.created(), SessionSeq(1));
    assert!(first.receipt().is_some());
    assert!(first.acceptance().declarations().is_empty());
    let lifecycle = state.claims[&id(1)].lifecycle();
    // One native revision per recorded status fact plus the scope release.
    let expected = lifecycle.history.len() + usize::from(lifecycle.released);
    assert!(lifecycle.released);
    assert_eq!(first.binding().revision, ObjectRevision(expected as u64));
    assert!(first.released());
    assert_eq!(first.binding().content, state.claims[&id(1)].content_hash());
    let second = claim(core, id(2));
    assert_eq!(second.status(), ClaimStatus::Posted);
    assert_eq!(
        second.graph().obligations(),
        &[focal_model::lifecycle::graph::Obligation {
            kind: focal_model::lifecycle::graph::Kind::DependsOn,
            target: id(1),
        }]
    );
    let scopes: Vec<_> = second.scopes().iter().collect();
    assert_eq!(scopes.len(), 2);
    assert!(
        second
            .scopes()
            .monitor(MonitorId::from_u128(31))
            .unwrap()
            .released()
            .is_some()
    );
    assert!(
        second
            .scopes()
            .monitor(MonitorId::from_u128(32))
            .unwrap()
            .active()
    );
    let third = claim(core, id(3));
    assert_eq!(third.status(), ClaimStatus::Cancelled);
    assert!(third.released());
    assert_eq!(claim(core, id(4)).status(), ClaimStatus::Superseded);
    let fifth = claim(core, id(5));
    assert_eq!(fifth.status(), ClaimStatus::Generated);
    assert_eq!(
        fifth.lineage().corrections().first().map(|c| c.kind),
        Some(focal_model::lifecycle::succession::CorrectionKind::Supersedes)
    );
    // Legacy rows retain the exact frozen objects.
    for (tid, testament) in &state.testaments {
        match core.state.rows.get(&Key::LegacyTestament(*tid)) {
            Some(Row::LegacyTestament(row)) => {
                let decoded: focal_model::Testament =
                    focal_model::durable_v1::decode(row.bytes()).unwrap();
                assert_eq!(&decoded, testament);
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        count(core, |key| matches!(key, Key::LegacyDefinition(_))),
        state.validations.len()
    );
    assert_eq!(
        count(core, |key| matches!(key, Key::LegacyRun(..))),
        state.runs.len()
    );
    assert_eq!(
        count(core, |key| matches!(key, Key::LegacyEvidenceSet(_))),
        state.evidence_sets.len()
    );
    assert!(!state.runs.is_empty() && !state.evidence_sets.is_empty());
    // Artifacts are native rows with re-verified custody.
    for (aid, legacy_artifact) in &state.artifacts {
        match core.state.rows.get(&Key::Artifact(*aid)) {
            Some(Row::Artifact(row)) => {
                let native = row.get().unwrap();
                assert_eq!(native.descriptor().producer(), WORKER);
                assert_eq!(native.custody().local_revision(), 1);
                if let ArtifactPayload::Content(reference) = &legacy_artifact.content().payload {
                    assert_eq!(native.custody().payload().root, reference.root);
                }
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        count(core, |key| matches!(key, Key::Artifact(_))),
        state.artifacts.len()
    );
    // The restored core re-encodes to the image's rows under its own range:
    // only the incarnation in the frame differs, never a row.
    let plan = EncodingPlan::prepare(core, request(&limits).encoding).unwrap();
    let mut again = vec![0; plan.quote().bytes];
    plan.write_into(&mut again).unwrap();
    assert_eq!(plan.header().unwrap().range, RangeId(41));
    assert_eq!(again.len(), imported.image.len());
    let restored = StructuralCheckpoint::inspect(&again, request(&limits).inspection).unwrap();
    let original =
        StructuralCheckpoint::inspect(&imported.image, request(&limits).inspection).unwrap();
    assert_eq!(original.header().range, IMPORT_RANGE);
    assert_eq!(restored.header().rows, original.header().rows);
    let rows = |image: StructuralCheckpoint<'_>| -> Vec<(RowFamily, Vec<u8>)> {
        image
            .rows(1 << 30)
            .unwrap()
            .map(|row| row.map(|row| (row.family(), row.body().to_vec())).unwrap())
            .collect()
    };
    assert_eq!(rows(restored), rows(original));
    // Another replica computes the same root from the same legacy core.
    let other = tempfile::tempdir().unwrap();
    let mut second_store = open_store(other.path());
    upload(
        &mut second_store,
        UploadId([9; 16]),
        b"standalone content payload",
    );
    seal_inline(&mut second_store, &legacy);
    let twin = import(
        state,
        ImportRequest {
            range: RangeId(42),
            ..request(&limits)
        },
        budget(),
        &second_store,
        &AnySchema,
    )
    .unwrap();
    assert_eq!(twin.root, imported.root);
    assert_eq!(twin.image, imported.image);
    assert_eq!(twin.core.state.rows.id(), RangeId(42));
}

#[test]
fn import_refuses_empty_prefixes_missing_content_and_synthetic_corpora_without_panicking() {
    let limits = limits();
    let dir = tempfile::tempdir().unwrap();
    let mut store = open_store(dir.path());
    let empty = LegacyCore::new(ledger(), crate::Limits::default());
    assert!(matches!(
        import(
            empty.snapshot(),
            request(&limits),
            budget(),
            &store,
            &AnySchema
        ),
        Err(ImportError::Unsupported(_))
    ));
    let legacy = legacy(&mut store);
    // Inline payloads were never sealed locally: custody cannot be re-verified.
    let refused = import(
        legacy.snapshot(),
        request(&limits),
        budget(),
        &store,
        &AnySchema,
    );
    assert!(matches!(
        refused,
        Err(ImportError::Evidence(_) | ImportError::Content(_))
    ));
    // The synthetic broad corpus carries rows admission refuses.
    let bytes = include_bytes!("../../fixtures/durable-v1-nested/broad.cp1");
    let broad = LegacyCore::decode_checkpoint(bytes).unwrap();
    let outcome = import(
        broad.snapshot(),
        request(&limits),
        budget(),
        &store,
        &AnySchema,
    );
    assert!(outcome.is_err());
    // A budget too small for the translation workspace refuses before writing.
    seal_inline(&mut store, &legacy);
    let tiny = MemoryBudget::new(1 << 20, 1 << 20).unwrap();
    assert!(matches!(
        import(
            legacy.snapshot(),
            request(&limits),
            tiny,
            &store,
            &AnySchema
        ),
        Err(ImportError::Memory(_))
    ));
}
