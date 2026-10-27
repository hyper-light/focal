use super::*;
#[path = "native_artifact_recovery_tests.rs"]
mod recovery;
use focal_model::lifecycle::artifact_descriptor::{ArtifactSpec, Limits as ArtifactLimits};
use focal_model::{
    ArtifactId, LedgerId, ParticipantId, RequestEpoch, RequestId, SessionId, TenantId,
};

const REPORT: &[u8] =
    br#"{"code":"missing_dependency","message":"The requested dependency is unavailable."}"#;

fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 2 * 1024 * 1024,
        max_staging_bytes: 4 * 1024 * 1024,
        max_uploads: 8,
        chunk_bytes: 13,
        max_manifest_bytes: 128 * 1024,
    }
}

fn store(path: &Path) -> ContentStore {
    ContentStore::open(path, limits(), &crate::store::test_keys(path)).unwrap()
}

fn request() -> RequestKey {
    RequestKey {
        principal: ParticipantId::from_u128(1),
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(2),
    }
}

fn domain() -> ContentDomainId {
    ContentDomainId::from_u128(3)
}

fn specification(payload: PayloadSpec<'_>) -> ArtifactSpec<'_> {
    ArtifactSpec {
        ledger: LedgerId {
            tenant: TenantId::from_u128(4),
            session: SessionId::from_u128(5),
        },
        id: ArtifactId::from_u128(6),
        schema: 1,
        kind: "error",
        schema_hash: crate::error_report_schema(),
        metadata: b"{}",
        payload,
        producer: request().principal,
        receipt: None,
        result: None,
        work: None,
        inputs: &[],
        visibility: &["internal"],
    }
}

fn descriptor(spec: ArtifactSpec<'_>) -> ArtifactDescriptor {
    ArtifactDescriptor::prepare(
        spec,
        ArtifactLimits {
            kind_bytes: 128,
            metadata_bytes: 1024,
            inline_bytes: 2 * 1024 * 1024,
            inputs: 16,
            visibility_labels: 16,
            visibility_label_bytes: 128,
            construction_bytes: 3 * 1024 * 1024,
        },
    )
    .unwrap()
    .build()
    .unwrap()
}

fn budget() -> MemoryBudget {
    MemoryBudget::new(32 * 1024 * 1024, 16 * 1024 * 1024).unwrap()
}

/// A `Cell` that is `Sync`, since a verifier is shared with the
/// materializer's workers.
#[derive(Debug, Default)]
struct SyncCell<T>(std::sync::Mutex<T>);
impl<T: Copy> SyncCell<T> {
    fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(value))
    }
    fn get(&self) -> T {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    fn set(&self, value: T) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
    }
}
struct ChangingSchemas {
    schema: ContentHash,
    maximum: SyncCell<usize>,
    present: SyncCell<bool>,
    verifications: SyncCell<usize>,
}

impl ChangingSchemas {
    fn new(maximum: usize) -> Self {
        Self {
            schema: ContentHash([87; 32]),
            maximum: SyncCell::new(maximum),
            present: SyncCell::new(true),
            verifications: SyncCell::new(0),
        }
    }
}

impl NativeSchemaVerifier for ChangingSchemas {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, crate::BuiltinSchemaError> {
        if schema != self.schema || !self.present.get() {
            return Err(crate::BuiltinSchemaError::Unsupported);
        }
        Ok(self.maximum.get())
    }

    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), crate::BuiltinSchemaError> {
        self.verifications.set(self.verifications.get() + 1);
        if bytes.len() > self.maximum_bytes(schema)? {
            return Err(crate::BuiltinSchemaError::Capacity);
        }
        Ok(())
    }
}

#[test]
fn pinned_verification_budget_rejects_schema_and_maximum_changes_before_funding_or_io() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let registry = ChangingSchemas::new(REPORT.len() + 10);
    let artifact = descriptor(ArtifactSpec {
        schema_hash: registry.schema,
        ..specification(PayloadSpec::Inline(REPORT))
    });
    let pinned = NativeVerificationBudget::for_schema(registry.schema, &registry).unwrap();
    assert_eq!(pinned.schema(), registry.schema);
    assert_eq!(pinned.maximum_bytes(), REPORT.len() + 10);
    let before = files(root.path());
    // Any attempted workspace debit would fail with Memory, exposing wrong
    // validation order even if the reservation subsequently refunded itself.
    let unfunded = MemoryBudget::new(1, 1).unwrap();
    assert!(matches!(
        store.verify_native_artifact_with_budget(
            RequestKey {
                principal: ParticipantId::from_u128(99),
                ..request()
            },
            &artifact,
            domain(),
            &unfunded,
            &registry,
            &pinned,
        ),
        Err(NativeEvidenceError::WrongRequest)
    ));
    for maximum in [REPORT.len() + 9, REPORT.len() + 11] {
        registry.maximum.set(maximum);
        assert!(matches!(
            pinned.check_schema(registry.schema, &registry),
            Err(NativeEvidenceError::VerificationBudgetChanged)
        ));
        assert!(matches!(
            store.verify_native_artifact_with_budget(
                request(),
                &artifact,
                domain(),
                &unfunded,
                &registry,
                &pinned,
            ),
            Err(NativeEvidenceError::VerificationBudgetChanged)
        ));
        assert_eq!(unfunded.stats().used, 0);
        assert_eq!(registry.verifications.get(), 0);
        assert_eq!(files(root.path()), before);
    }
    registry.maximum.set(pinned.maximum_bytes());
    let different = descriptor(ArtifactSpec {
        schema_hash: ContentHash([88; 32]),
        ..specification(PayloadSpec::Inline(REPORT))
    });
    assert!(matches!(
        store.verify_native_artifact_with_budget(
            request(),
            &different,
            domain(),
            &unfunded,
            &registry,
            &pinned,
        ),
        Err(NativeEvidenceError::VerificationBudgetChanged)
    ));
    registry.present.set(false);
    assert!(matches!(
        store.verify_native_artifact_with_budget(
            request(),
            &artifact,
            domain(),
            &unfunded,
            &registry,
            &pinned,
        ),
        Err(NativeEvidenceError::Schema(
            crate::BuiltinSchemaError::Unsupported
        ))
    ));
    assert_eq!(files(root.path()), before);
    assert_eq!(registry.verifications.get(), 0);
    registry.present.set(true);
    let exact = MemoryBudget::new(pinned.peak_bytes(), pinned.peak_bytes()).unwrap();
    let token = store
        .verify_native_artifact_with_budget(
            request(),
            &artifact,
            domain(),
            &exact,
            &registry,
            &pinned,
        )
        .unwrap();
    token.check(request(), &artifact).unwrap();
    assert_eq!(registry.verifications.get(), 1);
    assert_eq!(exact.stats().used, pinned.retained_bytes());
    assert_eq!(token.retained_bytes(), pinned.retained_bytes());
    drop(token);
    assert_eq!(exact.stats().used, 0);
}

#[test]
fn unknown_and_unbounded_verification_contracts_refuse_before_payload_access() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let registry = ChangingSchemas::new(REPORT.len());
    let artifact = descriptor(ArtifactSpec {
        schema_hash: registry.schema,
        ..specification(PayloadSpec::Inline(REPORT))
    });
    let pinned = NativeVerificationBudget::for_schema(registry.schema, &registry).unwrap();
    let unfunded = MemoryBudget::new(1, 1).unwrap();
    let before = files(root.path());
    for maximum in [MAX_TRANSFER_MANIFEST_BYTES + 1, usize::MAX] {
        registry.maximum.set(maximum);
        assert!(matches!(
            NativeVerificationBudget::for_schema(registry.schema, &registry),
            Err(NativeEvidenceError::Content(ContentError::Capacity))
        ));
        assert!(matches!(
            store.verify_native_artifact(request(), &artifact, domain(), &unfunded, &registry),
            Err(NativeEvidenceError::Content(ContentError::Capacity))
        ));
        assert!(matches!(
            store.verify_native_artifact_with_budget(
                request(),
                &artifact,
                domain(),
                &unfunded,
                &registry,
                &pinned,
            ),
            Err(NativeEvidenceError::Content(ContentError::Capacity))
        ));
    }
    assert!(matches!(
        NativeVerificationBudget::for_schema(registry.schema, &BuiltinNativeSchemas),
        Err(NativeEvidenceError::Schema(
            crate::BuiltinSchemaError::Unsupported
        ))
    ));
    assert!(matches!(
        store.verify_native_artifact(
            request(),
            &artifact,
            domain(),
            &unfunded,
            &BuiltinNativeSchemas,
        ),
        Err(NativeEvidenceError::Schema(
            crate::BuiltinSchemaError::Unsupported
        ))
    ));
    assert!(matches!(
        NativeVerificationBudget::for_schema(ContentHash([0; 32]), &registry),
        Err(NativeEvidenceError::Contract(
            ContractError::InvalidManifest
        ))
    ));
    assert_eq!(unfunded.stats().used, 0);
    assert_eq!(registry.verifications.get(), 0);
    assert_eq!(files(root.path()), before);
}

#[test]
fn verification_budget_accepts_exact_maximum_and_explicit_empty_custom_contract() {
    for (schema, maximum) in [
        (crate::error_report_schema(), crate::ERROR_REPORT_MAX_BYTES),
        (crate::test_report_schema(), crate::TEST_REPORT_MAX_BYTES),
    ] {
        let quote = NativeVerificationBudget::for_schema(schema, &BuiltinNativeSchemas).unwrap();
        assert_eq!(quote.maximum_bytes(), maximum);
        assert_eq!(
            quote.peak_bytes(),
            STORE_WORKSPACE + maximum + quote.retained_bytes()
        );
        assert_eq!(quote.retained_bytes(), size_of::<VerifiedNativeArtifact>());
        quote.check_schema(schema, &BuiltinNativeSchemas).unwrap();
    }
    let registry = ChangingSchemas::new(MAX_TRANSFER_MANIFEST_BYTES);
    let maximum = NativeVerificationBudget::for_schema(registry.schema, &registry).unwrap();
    assert_eq!(maximum.maximum_bytes(), MAX_TRANSFER_MANIFEST_BYTES);
    registry.maximum.set(0);
    let empty = NativeVerificationBudget::for_schema(registry.schema, &registry).unwrap();
    assert_eq!(empty.maximum_bytes(), 0);
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let artifact = descriptor(ArtifactSpec {
        schema_hash: registry.schema,
        ..specification(PayloadSpec::Inline(&[]))
    });
    let budget = MemoryBudget::new(empty.peak_bytes(), empty.peak_bytes()).unwrap();
    let token = store
        .verify_native_artifact_with_budget(
            request(),
            &artifact,
            domain(),
            &budget,
            &registry,
            &empty,
        )
        .unwrap();
    assert_eq!(token.custody().payload().length, 0);
    assert_eq!(budget.stats().used, empty.retained_bytes());
    drop(token);
    assert_eq!(budget.stats().used, 0);
}

fn uploaded(store: &mut ContentStore, payload: &[u8]) -> ContentRef {
    let id = UploadId([7; 16]);
    store
        .begin(
            id,
            domain(),
            ContentClass::Evidence,
            payload.len() as u64,
            Some(ContentHash(*blake3::hash(payload).as_bytes())),
        )
        .unwrap();
    let mut offset = 0;
    for block in payload.chunks(store.upload_chunk_bytes()) {
        offset = store.append(id, offset, block).unwrap();
    }
    let sealed = store.seal(id).unwrap();
    store.finish(id).unwrap();
    sealed
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), files);
            } else {
                files.insert(
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    crate::store::stored_bytes(&entry.path()),
                );
            }
        }
    }
    let mut output = BTreeMap::new();
    walk(root, root, &mut output);
    output
}

#[test]
fn verified_inline_content_survives_reopen_and_matches_original_upload_tree_bytes() {
    let native_root = tempfile::tempdir().unwrap();
    let old_root = tempfile::tempdir().unwrap();
    let mut native = store(native_root.path());
    let mut original = store(old_root.path());
    let descriptor = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let budget = budget();
    let verified = native
        .verify_native_artifact(
            request(),
            &descriptor,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    verified.check(request(), &descriptor).unwrap();
    let custody = verified.custody();
    let native_ref = reference(custody.payload());
    let original_ref = uploaded(&mut original, REPORT);
    assert_eq!(native_ref, original_ref);
    // Each data directory seals under keys of its own (29 §11): the two trees are the same
    // objects opened, and as many files, never the same stored bytes.
    assert_eq!(
        native.export_manifest(&native_ref).unwrap().encoded(),
        original.export_manifest(&original_ref).unwrap().encoded()
    );
    assert_eq!(
        files(&native_root.path().join("objects")).len(),
        files(&old_root.path().join("objects")).len()
    );
    assert_ne!(
        files(&native_root.path().join("objects")),
        files(&old_root.path().join("objects"))
    );
    assert_eq!(native.staged(), (0, 0));
    assert!(files(&native_root.path().join("staging")).is_empty());
    assert_eq!(custody.local_revision(), 1);
    assert_eq!(budget.stats().used, verified.retained_bytes());
    drop(verified);
    assert_eq!(budget.stats().used, 0);
    drop(native);
    drop(original);
    let mut reopened = store(native_root.path());
    assert_eq!(
        reopened.read_bytes(&native_ref, REPORT.len()).unwrap(),
        REPORT
    );
    custody.check(request(), &descriptor).unwrap();
    let pointer_descriptor = descriptor_for_pointer(custody.payload());
    let verification = NativeVerificationBudget::for_schema(
        pointer_descriptor.schema_hash(),
        &BuiltinNativeSchemas,
    )
    .unwrap();
    let verified = reopened
        .verify_native_artifact_with_budget(
            request(),
            &pointer_descriptor,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
            &verification,
        )
        .unwrap();
    assert_eq!(verified.retained_bytes(), verification.retained_bytes());
    assert_eq!(verified.custody().payload(), custody.payload());
    assert_eq!(
        reopened.read_bytes(&native_ref, REPORT.len()).unwrap(),
        REPORT
    );
}

fn descriptor_for_pointer(pointer: ContentPointer) -> ArtifactDescriptor {
    descriptor(specification(PayloadSpec::Content(pointer)))
}

#[test]
fn custody_token_rejects_artifact_address_and_request_substitution() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let budget = budget();
    let original = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let verified = store
        .verify_native_artifact(
            request(),
            &original,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let address = descriptor(ArtifactSpec {
        id: ArtifactId::from_u128(99),
        ..specification(PayloadSpec::Inline(REPORT))
    });
    assert_eq!(address.content_hash(), original.content_hash());
    assert!(matches!(
        verified.check(request(), &address),
        Err(NativeEvidenceError::WrongRequest)
    ));
    let content = descriptor(ArtifactSpec {
        metadata: b"changed",
        ..specification(PayloadSpec::Inline(REPORT))
    });
    assert!(matches!(
        verified.check(request(), &content),
        Err(NativeEvidenceError::WrongRequest)
    ));
    for changed in [
        RequestKey {
            principal: ParticipantId::from_u128(99),
            ..request()
        },
        RequestKey {
            epoch: RequestEpoch(2),
            ..request()
        },
        RequestKey {
            id: RequestId::from_u128(99),
            ..request()
        },
    ] {
        assert!(matches!(
            verified.check(changed, &original),
            Err(NativeEvidenceError::WrongRequest)
        ));
        assert!(matches!(
            verified.custody().check(changed, &original),
            Err(NativeEvidenceError::WrongRequest)
        ));
    }
    verified.check(request(), &original).unwrap();
}

#[test]
fn referenced_evidence_verifies_real_manifest_coordinates_and_schema() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let actual = uploaded(&mut store, REPORT);
    let pointer = pointer(&actual);
    let budget = budget();
    let before = files(root.path());
    let correct = descriptor_for_pointer(pointer);
    let verified = store
        .verify_native_artifact(
            request(),
            &correct,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    assert_eq!(verified.custody().payload(), pointer);
    drop(verified);
    for altered in [
        ContentPointer {
            domain: ContentDomainId::from_u128(99),
            ..pointer
        },
        ContentPointer {
            root: ContentHash([99; 32]),
            ..pointer
        },
        ContentPointer {
            length: pointer.length + 1,
            ..pointer
        },
        ContentPointer {
            class: ContentClass::Document,
            ..pointer
        },
        ContentPointer {
            class: ContentClass::Checkpoint,
            ..pointer
        },
    ] {
        let altered = descriptor_for_pointer(altered);
        assert!(matches!(
            store.verify_native_artifact(
                request(),
                &altered,
                domain(),
                &budget,
                &BuiltinNativeSchemas
            ),
            Err(NativeEvidenceError::Content(_))
        ));
        assert_eq!(budget.stats().used, 0);
        assert_eq!(files(root.path()), before);
    }
    let wrong_schema = descriptor(ArtifactSpec {
        schema_hash: crate::test_report_schema(),
        ..specification(PayloadSpec::Content(pointer))
    });
    assert!(matches!(
        store.verify_native_artifact(
            request(),
            &wrong_schema,
            domain(),
            &budget,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Schema(
            crate::BuiltinSchemaError::Invalid
        ))
    ));
    assert_eq!(budget.stats().used, 0);
    store
        .verify_native_artifact(
            request(),
            &correct,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    assert_eq!(files(root.path()), before);
}

#[test]
fn corrupt_chunk_or_manifest_cannot_produce_a_local_custody_token() {
    for corrupt_manifest in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut store = store(root.path());
        let actual = uploaded(&mut store, REPORT);
        let artifact = descriptor_for_pointer(pointer(&actual));
        let path = if corrupt_manifest {
            store.manifest_file(actual.domain, actual.root)
        } else {
            let manifest = store.manifest(&actual).unwrap();
            store.chunk_file(actual.domain, manifest.chunks.last().unwrap().hash)
        };
        let original = fs::read(&path).unwrap();
        let mut corrupt = original.clone();
        corrupt[0] ^= 1;
        fs::write(&path, &corrupt).unwrap();
        let budget = budget();
        assert!(matches!(
            store.verify_native_artifact(
                request(),
                &artifact,
                domain(),
                &budget,
                &BuiltinNativeSchemas
            ),
            Err(NativeEvidenceError::Content(ContentError::Corrupt))
        ));
        assert_eq!(budget.stats().used, 0);
        assert_eq!(fs::read(&path).unwrap(), corrupt);
        fs::write(&path, original).unwrap();
        let restored = store
            .verify_native_artifact(
                request(),
                &artifact,
                domain(),
                &budget,
                &BuiltinNativeSchemas,
            )
            .unwrap();
        assert_eq!(restored.custody().payload(), pointer(&actual));
    }
}

#[test]
fn authenticated_request_and_builtin_schema_refusals_leave_storage_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let before = files(root.path());
    let budget = budget();
    let valid = descriptor(specification(PayloadSpec::Inline(REPORT)));
    for wrong in [
        RequestKey {
            principal: ParticipantId::from_u128(0),
            ..request()
        },
        RequestKey {
            principal: ParticipantId::from_u128(99),
            ..request()
        },
        RequestKey {
            epoch: RequestEpoch(0),
            ..request()
        },
        RequestKey {
            id: RequestId::from_u128(0),
            ..request()
        },
    ] {
        assert!(matches!(
            store.verify_native_artifact(wrong, &valid, domain(), &budget, &BuiltinNativeSchemas),
            Err(NativeEvidenceError::WrongRequest)
        ));
    }
    assert!(matches!(
        store.verify_native_artifact(
            request(),
            &valid,
            ContentDomainId::from_u128(0),
            &budget,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::WrongRequest)
    ));
    let unknown = descriptor(ArtifactSpec {
        schema_hash: ContentHash([99; 32]),
        ..specification(PayloadSpec::Inline(REPORT))
    });
    assert!(matches!(
        store.verify_native_artifact(
            request(),
            &unknown,
            domain(),
            &budget,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Schema(
            crate::BuiltinSchemaError::Unsupported
        ))
    ));
    for invalid in [
        b"{}".as_slice(),
        b"not JSON",
        br#"{"code":"x","message":""}"#,
    ] {
        let artifact = descriptor(specification(PayloadSpec::Inline(invalid)));
        assert!(matches!(
            store.verify_native_artifact(
                request(),
                &artifact,
                domain(),
                &budget,
                &BuiltinNativeSchemas
            ),
            Err(NativeEvidenceError::Schema(
                crate::BuiltinSchemaError::Invalid
            ))
        ));
        assert_eq!(budget.stats().used, 0);
    }
    let oversized = vec![b' '; crate::ERROR_REPORT_MAX_BYTES + 1];
    let artifact = descriptor(specification(PayloadSpec::Inline(&oversized)));
    assert!(matches!(
        store.verify_native_artifact(
            request(),
            &artifact,
            domain(),
            &budget,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Content(ContentError::Capacity))
    ));
    assert_eq!(budget.stats().used, 0);
    assert_eq!(files(root.path()), before);
    assert!(!root.path().join("objects").join(hex(&domain().0)).exists());
    store
        .verify_native_artifact(request(), &valid, domain(), &budget, &BuiltinNativeSchemas)
        .unwrap();
}

#[test]
fn memory_pressure_refuses_before_writes_and_completion_permit_releases_after_custody() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let before = files(root.path());
    let verification =
        NativeVerificationBudget::for_schema(artifact.schema_hash(), &BuiltinNativeSchemas)
            .unwrap();
    // Charged what this payload holds, never the schema's maximum.
    let workspace = verification
        .peak_for(
            REPORT.len(),
            REPORT.len().div_ceil(store.limits.chunk_bytes),
        )
        .unwrap();
    assert!(workspace < verification.peak_bytes() / 100, "{workspace}");
    let short = MemoryBudget::new(workspace - 1, workspace - 1).unwrap();
    assert!(matches!(
        store.verify_native_artifact_with_budget(
            request(),
            &artifact,
            domain(),
            &short,
            &BuiltinNativeSchemas,
            &verification
        ),
        Err(NativeEvidenceError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(short.stats().used, 0);
    assert_eq!(files(root.path()), before);
    assert!(!root.path().join("objects").join(hex(&domain().0)).exists());
    let budget = MemoryBudget::new(workspace + 1024, workspace).unwrap();
    let pressure = budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 1024)
        .unwrap();
    assert!(matches!(
        budget.reserve(BudgetKind::Query, BudgetLane::Ordinary, 1),
        Err(MemoryError::Capacity { .. })
    ));
    let verified = store
        .verify_native_artifact_with_budget(
            request(),
            &artifact,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
            &verification,
        )
        .unwrap();
    assert_eq!(
        budget.stats().used,
        pressure.bytes() + verified.retained_bytes()
    );
    assert_eq!(budget.stats().ordinary_used, 1024);
    assert_eq!(verified.retained_bytes(), verification.retained_bytes());
    let custody = verified.custody();
    drop(verified);
    assert_eq!(budget.stats().used, 1024);
    drop(pressure);
    assert_eq!(budget.stats().used, 0);
    assert_eq!(
        store
            .read_bytes(&reference(custody.payload()), REPORT.len())
            .unwrap(),
        REPORT
    );
}

#[test]
fn local_content_and_manifest_limits_refuse_without_leaking_memory_or_staging_ids() {
    for limits in [
        StoreLimits {
            max_content_bytes: 8,
            ..limits()
        },
        StoreLimits {
            max_manifest_bytes: MANIFEST_MAGIC.len(),
            ..limits()
        },
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut store =
            ContentStore::open(root.path(), limits, &crate::store::test_keys(root.path())).unwrap();
        let before = files(root.path());
        let budget = budget();
        let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
        assert!(matches!(
            store.verify_native_artifact(
                request(),
                &artifact,
                domain(),
                &budget,
                &BuiltinNativeSchemas
            ),
            Err(NativeEvidenceError::Content(ContentError::Capacity))
        ));
        assert_eq!(budget.stats().used, 0);
        assert_eq!(store.staged(), (0, 0));
        assert_eq!(files(root.path()), before);
    }
}

fn full_parent_with_funded_workspace() -> (MemoryBudget, MemoryBudget, focal_memory::Reservation) {
    let parent = budget();
    let workspace =
        NativeVerificationBudget::for_schema(crate::error_report_schema(), &BuiltinNativeSchemas)
            .unwrap()
            .peak_bytes();
    let pool = parent
        .funded_child(BudgetLane::Ordinary, workspace)
        .unwrap();
    let free = parent.stats().limit - parent.stats().used;
    let pressure = parent
        .reserve(BudgetKind::Query, BudgetLane::Completion, free)
        .unwrap();
    assert_eq!(parent.stats().used, parent.stats().limit);
    assert!(matches!(
        parent.reserve(BudgetKind::Payload, BudgetLane::Completion, 1),
        Err(MemoryError::Capacity { .. })
    ));
    (parent, pool, pressure)
}

#[test]
fn funded_custody_succeeds_at_full_parent_and_returns_workspace_to_its_pool() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let (parent, pool, pressure) = full_parent_with_funded_workspace();
    let parent_before = parent.stats();
    let pool_before = pool.stats();
    let verified = store
        .verify_native_artifact(request(), &artifact, domain(), &pool, &BuiltinNativeSchemas)
        .unwrap();
    let retained = verified.retained_bytes();
    assert_eq!(retained, size_of::<VerifiedNativeArtifact>());
    assert_eq!(pool.stats().used, retained);
    assert_eq!(pool.stats().ordinary_used, 0);
    assert_eq!(pool.stats().by_kind[BudgetKind::Payload as usize], retained);
    assert_eq!(parent.stats().used, parent_before.used);
    assert_eq!(parent.stats().ordinary_used, parent_before.ordinary_used);
    assert_eq!(
        parent.stats().by_kind[BudgetKind::Payload as usize],
        retained
    );
    assert_eq!(
        parent.stats().by_kind[BudgetKind::Reserved as usize],
        parent_before.by_kind[BudgetKind::Reserved as usize] - retained
    );

    // The verifier returned its entire temporary workspace without exposing it
    // to unrelated parent admission. Another funded operation can spend it now.
    let reused = pool
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            pool_before.limit - retained,
        )
        .unwrap();
    assert_eq!(pool.stats().used, pool_before.limit);
    assert_eq!(parent.stats().used, parent_before.used);
    drop(reused);
    assert_eq!(pool.stats().used, retained);

    let before_check = (parent.stats(), pool.stats());
    for wrong in [
        RequestKey {
            id: RequestId::from_u128(91),
            ..request()
        },
        RequestKey {
            epoch: RequestEpoch(2),
            ..request()
        },
    ] {
        assert!(matches!(
            verified.check(wrong, &artifact),
            Err(NativeEvidenceError::WrongRequest)
        ));
        assert!(matches!(
            verified.custody().check(wrong, &artifact),
            Err(NativeEvidenceError::WrongRequest)
        ));
    }
    verified.check(request(), &artifact).unwrap();
    assert_eq!((parent.stats(), pool.stats()), before_check);
    let content = reference(verified.custody().payload());
    drop(verified);
    assert_eq!(pool.stats(), pool_before);
    assert_eq!(parent.stats(), parent_before);
    assert_eq!(store.staged(), (0, 0));

    drop(store);
    let reopened =
        ContentStore::open(root.path(), limits(), &crate::store::test_keys(root.path())).unwrap();
    assert_eq!(reopened.read_bytes(&content, REPORT.len()).unwrap(), REPORT);
    drop(pool);
    assert_eq!(parent.stats().used, pressure.bytes());
    assert_eq!(parent.stats().ordinary_used, 0);
    drop(pressure);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn funded_schema_and_corrupt_content_refusals_restore_all_credit() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let actual = uploaded(&mut store, REPORT);
    let artifact = descriptor_for_pointer(pointer(&actual));
    let (parent, pool, pressure) = full_parent_with_funded_workspace();
    let parent_before = parent.stats();
    let pool_before = pool.stats();
    let before_files = files(root.path());
    let malformed = descriptor(specification(PayloadSpec::Inline(b"{}")));
    assert!(matches!(
        store.verify_native_artifact(
            request(),
            &malformed,
            domain(),
            &pool,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Schema(
            crate::BuiltinSchemaError::Invalid
        ))
    ));
    assert_eq!(pool.stats(), pool_before);
    assert_eq!(parent.stats(), parent_before);
    assert_eq!(files(root.path()), before_files);

    let manifest = store.manifest(&actual).unwrap();
    let chunk = store.chunk_file(actual.domain, manifest.chunks.last().unwrap().hash);
    let original = fs::read(&chunk).unwrap();
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    fs::write(&chunk, &corrupt).unwrap();
    let corrupt_files = files(root.path());
    assert!(matches!(
        store.verify_native_artifact(request(), &artifact, domain(), &pool, &BuiltinNativeSchemas),
        Err(NativeEvidenceError::Content(ContentError::Corrupt))
    ));
    assert_eq!(pool.stats(), pool_before);
    assert_eq!(parent.stats(), parent_before);
    assert_eq!(files(root.path()), corrupt_files);
    assert_eq!(store.staged(), (0, 0));

    // Repair the actual bytes and reopen the store; no test-only custody fact
    // substitutes for another real verification after the refusal.
    fs::write(&chunk, original).unwrap();
    drop(store);
    let mut reopened =
        ContentStore::open(root.path(), limits(), &crate::store::test_keys(root.path())).unwrap();
    let verified = reopened
        .verify_native_artifact(request(), &artifact, domain(), &pool, &BuiltinNativeSchemas)
        .unwrap();
    verified.check(request(), &artifact).unwrap();
    assert_eq!(verified.custody().payload(), pointer(&actual));
    assert_eq!(reopened.read_bytes(&actual, REPORT.len()).unwrap(), REPORT);
    drop(verified);
    assert_eq!(pool.stats(), pool_before);
    assert_eq!(parent.stats(), parent_before);
    drop(pool);
    assert_eq!(parent.stats().used, pressure.bytes());
    drop(pressure);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn funded_custody_token_keeps_full_backing_after_the_pool_owner_drops() {
    let root = tempfile::tempdir().unwrap();
    let mut store = store(root.path());
    let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let (parent, pool, pressure) = full_parent_with_funded_workspace();
    let verified = store
        .verify_native_artifact(request(), &artifact, domain(), &pool, &BuiltinNativeSchemas)
        .unwrap();
    let held = parent.stats();
    let content = reference(verified.custody().payload());
    drop(pool);
    assert_eq!(parent.stats(), held);
    assert_eq!(parent.stats().used, parent.stats().limit);
    assert!(matches!(
        parent.reserve(BudgetKind::Payload, BudgetLane::Completion, 1),
        Err(MemoryError::Capacity { .. })
    ));
    verified.check(request(), &artifact).unwrap();
    assert_eq!(store.read_bytes(&content, REPORT.len()).unwrap(), REPORT);
    drop(verified);
    assert_eq!(parent.stats().used, pressure.bytes());
    assert_eq!(parent.stats().ordinary_used, 0);
    assert_eq!(parent.stats().by_kind[BudgetKind::Payload as usize], 0);
    assert_eq!(parent.stats().by_kind[BudgetKind::Reserved as usize], 0);
    drop(pressure);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn complete_manifest_header_is_preflighted_before_installing_any_chunk() {
    let original_root = tempfile::tempdir().unwrap();
    let one_chunk = StoreLimits {
        chunk_bytes: REPORT.len(),
        ..limits()
    };
    let mut original = ContentStore::open(
        original_root.path(),
        one_chunk.clone(),
        &crate::store::test_keys(original_root.path()),
    )
    .unwrap();
    let expected = uploaded(&mut original, REPORT);
    // The manifest's plaintext length, what the limit bounds (its file is sealed, longer).
    let exact_manifest_bytes = original.manifest_length(&expected).unwrap();
    // The one-chunk estimate fits; only the complete encoded header exposes the
    // refusal. This reaches the late-size path that formerly left orphan chunks.
    assert!(40 < exact_manifest_bytes - 1);
    let root = tempfile::tempdir().unwrap();
    let mut current = ContentStore::open(
        root.path(),
        StoreLimits {
            max_manifest_bytes: exact_manifest_bytes - 1,
            ..one_chunk.clone()
        },
        &crate::store::test_keys(root.path()),
    )
    .unwrap();
    let before = files(root.path());
    let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let budget = budget();
    assert!(matches!(
        current.verify_native_artifact(
            request(),
            &artifact,
            domain(),
            &budget,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Content(ContentError::Capacity))
    ));
    assert_eq!(files(root.path()), before);
    assert!(!root.path().join("objects").join(hex(&domain().0)).exists());
    assert_eq!(current.staged(), (0, 0));
    assert_eq!(budget.stats().used, 0);
    drop(current);
    let mut current = ContentStore::open(
        root.path(),
        StoreLimits {
            max_manifest_bytes: exact_manifest_bytes,
            ..one_chunk
        },
        &crate::store::test_keys(root.path()),
    )
    .unwrap();
    let verified = current
        .verify_native_artifact(
            request(),
            &artifact,
            domain(),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    assert_eq!(reference(verified.custody().payload()), expected);
    assert_eq!(current.read_bytes(&expected, REPORT.len()).unwrap(), REPORT);
}

/// A bundle the node made itself (a seal's, an archive's) is sealed by what the store holds,
/// not by an inline payload's bound: a 3 MiB bundle is refused as an inline payload, sealed as
/// a bundle, and read back whole. Bounded as an inline payload was, every seal bundle worth
/// proposing was refused and no outcome was ever sealed.
#[test]
fn a_node_bundle_past_the_inline_bound_seals_and_reads_back() {
    let root = tempfile::tempdir().unwrap();
    // A node's store: 64 KiB chunks, 64 MiB of content.
    let mut store = ContentStore::open(
        root.path(),
        StoreLimits {
            max_content_bytes: 64 << 20,
            max_staging_bytes: 64 << 20,
            max_uploads: 8,
            chunk_bytes: 64 << 10,
            max_manifest_bytes: 128 << 10,
        },
        &crate::store::test_keys(root.path()),
    )
    .unwrap();
    let bytes: Vec<u8> = (0..3usize << 20).map(|index| (index % 251) as u8).collect();
    assert!(bytes.len() > MAX_TRANSFER_MANIFEST_BYTES);
    let chunk = store.limits.chunk_bytes;
    assert!(matches!(
        store.seal_import_inline(domain(), &bytes, chunk),
        Err(ContentError::Capacity)
    ));
    let reference = store.seal_bundle(domain(), &bytes, chunk).unwrap();
    assert_eq!(reference.length, bytes.len() as u64);
    assert_eq!(
        store.read_bytes(&reference, bytes.len()).unwrap(),
        bytes,
        "the sealed bundle reads back whole"
    );
    // Sealing it again installs nothing new and names the same object.
    assert_eq!(
        store.seal_bundle(domain(), &bytes, chunk).unwrap(),
        reference
    );
}

/// The same payload sealed again, as every artifact carrying the same bytes is, is verified where
/// it lies and promised and written nothing: the volume's estimate, which each committed write
/// lowers until the next sample, stands. A chunk gone from the volume is promised and written
/// again, alone, and the object reads back whole.
#[test]
fn a_payload_sealed_again_writes_only_what_the_volume_lacks() {
    let root = tempfile::tempdir().unwrap();
    // Sampled once, at the first promise: from then on only committed writes move the estimate.
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        sample_interval: u32::MAX,
    })
    .unwrap();
    let mut store = ContentStore::open_with_disk(
        root.path(),
        limits(),
        disk.clone(),
        &crate::store::test_keys(root.path()),
        ContentForm::Sealed,
    )
    .unwrap();
    let chunk = store.limits.chunk_bytes;
    let reference = store.seal_import_inline(domain(), REPORT, chunk).unwrap();
    let free = disk.stats().free.unwrap();
    assert_eq!(
        store.seal_import_inline(domain(), REPORT, chunk).unwrap(),
        reference
    );
    assert_eq!(disk.stats().free, Some(free), "nothing promised or written");
    assert_eq!(disk.stats().outstanding, 0);
    // A chunk lost: promised and written again, alone.
    let directory = domain_directory(root.path(), domain());
    let key = store.keys.domain(&directory, domain(), false).unwrap();
    let first = ContentHash(*blake3::hash(&REPORT[..chunk]).as_bytes());
    std::fs::remove_file(key.path(&directory, first, sealing::Kind::Chunk).unwrap()).unwrap();
    assert_eq!(
        store.seal_import_inline(domain(), REPORT, chunk).unwrap(),
        reference
    );
    let rewritten = free - disk.stats().free.unwrap();
    assert!(
        rewritten > 0 && rewritten <= key.file_bytes(chunk).unwrap() + key.record_bytes(),
        "{rewritten} bytes promised for one lost chunk"
    );
    assert_eq!(store.read_bytes(&reference, REPORT.len()).unwrap(), REPORT);
}
