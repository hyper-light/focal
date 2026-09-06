use super::*;
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::MemoryBudget;
use focal_model::lifecycle::{
    Binding, Principal,
    artifact_descriptor::{ArtifactSpec, ContentPointer, Limits as ArtifactLimits, PayloadSpec},
    validation as v,
};
use focal_model::{
    ArtifactId, ArtifactRef, ClaimId, ContentDomainId, ContentHash, ContentRef, Deadline,
    HandlerRef, LedgerId, ObjectId, ObjectRef, ObjectRevision, ParticipantId, RequestEpoch,
    RequestId, RequestKey, SessionId, TenantId, TimerId, ValidationId, ValidationKind,
    ValidationMode, ValidationPhase, ValidatorId, VerdictValue,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(2);
const PAYLOAD: &[u8] = br#"{"passed":0,"failed":1,"skipped":0}"#;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(3),
        session: SessionId::from_u128(4),
    }
}
fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([8; 32]),
        revision: ObjectRevision(1),
    }
}
fn request() -> RequestKey {
    RequestKey {
        principal: EVALUATOR,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(5),
    }
}
fn descriptor(id: u128, payload: PayloadSpec<'_>) -> ArtifactDescriptor {
    ArtifactDescriptor::prepare(
        ArtifactSpec {
            ledger: ledger(),
            id: ArtifactId::from_u128(id),
            schema: 1,
            kind: "test-report",
            schema_hash: focal_evidence::test_report_schema(),
            metadata: b"{\"source\":\"compiler\"}",
            payload,
            producer: EVALUATOR,
            receipt: None,
            result: Some(focal_model::lifecycle::artifact_descriptor::ResultProvenance {
                claim: ClaimId::from_u128(200), validation: ValidationId::from_u128(300),
                target: Target::Admission { claim: binding(200) }, generation: 1,
                attempt: v::Attempt { phase: v::Phase::Programmatic, index: 0,
                    handler: ValidatorId::from_u128(7), version: ContentHash([7; 32]),
                    evaluator: EVALUATOR, definition: ContentHash([9; 32]) },
                value: VerdictValue::Fail,
            }),
            inputs: &[
                ObjectRef::claim(ledger(), ClaimId::from_u128(200)),
                ObjectRef {
                    ledger: ledger(),
                    kind: focal_model::ObjectKind::Validation,
                    id: ObjectId::from_u128(300),
                },
            ],
            visibility: &["internal", "team:test"],
        },
        ArtifactLimits {
            kind_bytes: 64,
            metadata_bytes: 256,
            inline_bytes: 4096,
            inputs: 8,
            visibility_labels: 8,
            visibility_label_bytes: 64,
            construction_bytes: 32768,
        },
    )
    .unwrap()
    .build()
    .unwrap()
}
fn store_limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 8192,
        max_staging_bytes: 16384,
        max_uploads: 4,
        chunk_bytes: 13,
        max_manifest_bytes: 4096,
    }
}
fn declaration() -> v::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(7),
        version: ContentHash([7; 32]),
        agentic: false,
    };
    v::Declaration::new(
        Principal::Actor(ISSUER),
        v::DeclarationSpec {
            binding: binding(300),
            claim: ClaimId::from_u128(200),
            issuer: ISSUER,
            declaration_index: 0,
            kind: ValidationKind::Test,
            phase: ValidationPhase::Admission,
            mode: ValidationMode::Required,
            target: v::TargetDeclaration::Admission,
            program: v::Program::Programmatic {
                check: v::PhasePolicy {
                    evaluator: EVALUATOR,
                    definition: ContentHash([9; 32]),
                    required_policy: None,
                    handlers: &[v::HandlerPolicy {
                        handler: &handler,
                        attempts: 1,
                        proof_schema: focal_evidence::test_report_schema(),
                        diagnostic_schema: focal_evidence::error_report_schema(),
                    }],
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(10),
                generation: 1,
                at: 100,
            },
        },
        v::Limits {
            handlers: 1,
            attempts: 1,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn accepted(
    descriptor: &ArtifactDescriptor,
    custody: NativeLocalCustody,
) -> (EvidenceFacts, NativeAccepted) {
    let declaration = declaration();
    let ready = v::Evaluation::materialize(
        Principal::Actor(ISSUER),
        &declaration,
        v::Materialization {
            binding: declaration.binding(),
            target: Target::Admission {
                claim: binding(200),
            },
            slot_name: None,
            generation: 1,
            receipt: None,
        },
    )
    .unwrap();
    let mut owner = v::OwnerState {
        evaluation: ready.binding(),
        target: ready.target(),
        parent: v::ParentState::Open,
        readiness: v::Readiness::AdmissionPosted,
        cohort: v::Cohort::Open,
        authority: v::Authority {
            evaluator: EVALUATOR,
            definition: ContentHash([9; 32]),
            generation: 1,
            receipt: None,
            deadline: ready.deadline(),
            policy_evidence: None,
            state: v::AuthorityState::Live,
        },
        logical_time: 1,
    };
    let evaluation = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    owner.evaluation = evaluation.binding();
    let attempt = evaluation.current_attempt().unwrap();
    let facts = EvidenceFacts {
        binding: descriptor.binding(),
        claim: ClaimId::from_u128(200),
        validation: ValidationId::from_u128(300),
        target: evaluation.target(),
        generation: evaluation.generation(),
        attempt,
        producer: EVALUATOR,
        value: VerdictValue::Fail,
        kind: v::EvidenceKind::Proof,
        schema: descriptor.schema_hash(),
        custody_revision: Some(custody.local_revision()),
    };
    let report = v::Report {
        generation: 1,
        attempt,
        value: VerdictValue::Fail,
        evidence: ArtifactRef {
            id: descriptor.id(),
            hash: descriptor.content_hash(),
        },
    };
    let transition = evaluation
        .report(
            Principal::Actor(EVALUATOR),
            &evaluation.binding(),
            &owner,
            report,
            &facts,
        )
        .unwrap();
    assert_eq!(transition.next.state(), v::State::ValidationFailed);
    assert!(transition.next.current_attempt().is_err());
    let result = transition.result.unwrap();
    (
        facts,
        NativeAccepted::new(
            result,
            attempt,
            ResultArtifact::from_result(result).unwrap(),
            SessionSeq(8),
            3,
        )
        .unwrap(),
    )
}

struct Fixture {
    root: tempfile::TempDir,
    store: ContentStore,
    artifact: NativeArtifact,
    accepted: NativeAccepted,
}
fn fixture(id: u128) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(root.path(), store_limits()).unwrap();
    let descriptor = descriptor(id, PayloadSpec::Inline(PAYLOAD));
    let budget = MemoryBudget::new(32 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let witness = store
        .verify_native_artifact(
            request(),
            &descriptor,
            ContentDomainId::from_u128(11),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    witness.check(request(), &descriptor).unwrap();
    let custody = witness.custody();
    let (facts, accepted) = accepted(&descriptor, custody);
    let artifact = NativeArtifact::new(descriptor, custody, facts).unwrap();
    drop(witness);
    assert_eq!(budget.stats().used, 0);
    Fixture {
        root,
        store,
        artifact,
        accepted,
    }
}

fn payload_pointer(descriptor: &ArtifactDescriptor) -> *const u8 {
    match descriptor.payload() {
        PayloadSpec::Inline(bytes) => bytes.as_ptr(),
        _ => panic!("inline payload expected"),
    }
}

#[test]
fn input_moves_buffers_and_fallible_copy_remains_independent_after_source_drop() {
    let descriptor = descriptor(400, PayloadSpec::Inline(PAYLOAD));
    let payload = payload_pointer(&descriptor);
    let metadata = descriptor.metadata().as_ptr();
    let labels = descriptor.visibility().map(str::as_ptr).collect::<Vec<_>>();
    let heap = descriptor_heap(&descriptor).unwrap();
    let input = NativeArtifactInput::new(descriptor).unwrap();
    assert_eq!(
        input.heap_charge().unwrap(),
        NativeArtifactInput::container_charge() + heap
    );
    assert_eq!(payload_pointer(input.get().unwrap()), payload);
    let copied = input.copy().unwrap();
    assert!(copied.heap_charge().unwrap() <= input.heap_charge().unwrap());
    assert_eq!(copied.get(), input.get());
    assert_ne!(copied.0.as_ptr(), input.0.as_ptr());
    assert_ne!(payload_pointer(copied.get().unwrap()), payload);
    assert_ne!(copied.get().unwrap().metadata().as_ptr(), metadata);
    for (label, old) in copied.get().unwrap().visibility().zip(labels) {
        assert_ne!(label.as_ptr(), old);
    }
    let descriptor = input.into_descriptor().unwrap();
    assert_eq!(payload_pointer(&descriptor), payload);
    assert_eq!(descriptor.metadata().as_ptr(), metadata);
    let identity = descriptor.intent_fingerprint();
    drop(descriptor);
    assert_eq!(copied.get().unwrap().intent_fingerprint(), identity);
    assert_eq!(
        copied.get().unwrap().payload(),
        PayloadSpec::Inline(PAYLOAD)
    );
}

#[test]
fn copied_artifact_retains_synced_content_and_exact_result_facts_after_reopen() {
    let Fixture {
        root,
        store,
        artifact,
        accepted,
    } = fixture(400);
    let original_payload = payload_pointer(artifact.descriptor());
    let descriptor_hash = artifact.descriptor().content_hash();
    let nested = descriptor_heap(artifact.descriptor()).unwrap();
    let original = OwnedArtifact::new(artifact).unwrap();
    assert_eq!(
        payload_pointer(original.get().unwrap().descriptor()),
        original_payload
    );
    assert_eq!(
        original.heap_charge().unwrap(),
        OwnedArtifact::container_charge() + nested
    );
    let copied = original.copy().unwrap();
    assert!(copied.heap_charge().unwrap() <= original.heap_charge().unwrap());
    assert_ne!(copied.0.as_ptr(), original.0.as_ptr());
    assert_ne!(
        payload_pointer(copied.get().unwrap().descriptor()),
        original_payload
    );
    assert_eq!(
        copied.get().unwrap().descriptor(),
        original.get().unwrap().descriptor()
    );
    let record = OwnedAccepted::new(accepted).unwrap();
    let record_copy = record.copy().unwrap();
    assert_eq!(record_copy.get(), record.get());
    assert_ne!(record_copy.0.as_ptr(), record.0.as_ptr());
    assert_eq!(
        record.heap_charge().unwrap(),
        OwnedAccepted::container_charge()
    );
    drop(record);
    drop(original);
    drop(store);
    let store = ContentStore::open(root.path(), store_limits()).unwrap();
    let retained = copied.get().unwrap();
    retained
        .custody()
        .check(request(), retained.descriptor())
        .unwrap();
    assert_eq!(retained.descriptor().content_hash(), descriptor_hash);
    let pointer = retained.custody().payload();
    let reference = ContentRef {
        domain: pointer.domain,
        root: pointer.root,
        length: pointer.length,
        class: pointer.class,
    };
    assert_eq!(store.read_bytes(&reference, 4096).unwrap(), PAYLOAD);
    let accepted = record_copy.get().unwrap();
    assert_eq!(accepted.sequence(), SessionSeq(8));
    assert_eq!(accepted.ordinal(), 3);
    assert_eq!(accepted.attempt(), retained.facts().unwrap().attempt);
    assert_eq!(
        accepted.result().evidence(),
        Some(accepted.artifact().reference())
    );
    assert_eq!(accepted.result().verdict(), retained.facts().unwrap().value);
    assert_eq!(accepted.result().binding().revision, ObjectRevision(3));
}

#[test]
fn content_pointer_copy_and_address_bound_custody_preserve_distinct_identity() {
    let Fixture {
        root: _root,
        mut store,
        artifact,
        accepted: _,
    } = fixture(400);
    let pointer: ContentPointer = artifact.custody().payload();
    let addressed = descriptor(401, PayloadSpec::Content(pointer));
    let budget = MemoryBudget::new(32 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let verified = store
        .verify_native_artifact(
            request(),
            &addressed,
            ContentDomainId::from_u128(11),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let input = NativeArtifactInput::new(addressed).unwrap();
    let copied = input.copy().unwrap();
    assert_eq!(
        copied.get().unwrap().payload(),
        PayloadSpec::Content(pointer)
    );
    assert_eq!(copied.get(), input.get());
    assert!(copied.heap_charge().unwrap() <= input.heap_charge().unwrap());
    let changed_address = descriptor(402, PayloadSpec::Content(pointer));
    assert_eq!(
        changed_address.content_hash(),
        input.get().unwrap().content_hash()
    );
    assert_ne!(
        changed_address.intent_fingerprint(),
        input.get().unwrap().intent_fingerprint()
    );
    assert!(verified.check(request(), &changed_address).is_err());
    let mut wrong_request = request();
    wrong_request.id = RequestId::from_u128(999);
    assert!(verified.check(wrong_request, input.get().unwrap()).is_err());
    drop(input);
    verified.check(request(), copied.get().unwrap()).unwrap();
}

#[test]
fn accepted_history_rejects_reconstructed_attempts_and_replacement_evidence() {
    let original = fixture(400);
    let replacement = fixture(401);
    let accepted = original.accepted;
    assert_eq!(
        NativeAccepted::new(
            accepted.result(),
            accepted.attempt(),
            accepted.artifact(),
            SessionSeq(0),
            0
        ),
        Err(ContractError::InvalidCut)
    );
    for attempt in [
        Attempt {
            index: accepted.attempt().index + 1,
            ..accepted.attempt()
        },
        Attempt {
            phase: v::Phase::Quality,
            ..accepted.attempt()
        },
    ] {
        assert_eq!(
            NativeAccepted::new(
                accepted.result(),
                attempt,
                accepted.artifact(),
                accepted.sequence(),
                accepted.ordinal()
            ),
            Err(ContractError::StaleEvaluation)
        );
    }
    assert_eq!(
        NativeAccepted::new(
            accepted.result(),
            Attempt {
                evaluator: ISSUER,
                ..accepted.attempt()
            },
            accepted.artifact(),
            accepted.sequence(),
            accepted.ordinal()
        ),
        Err(ContractError::WrongActor)
    );
    assert_eq!(
        NativeAccepted::new(
            accepted.result(),
            accepted.attempt(),
            replacement.accepted.artifact(),
            accepted.sequence(),
            accepted.ordinal()
        ),
        Err(ContractError::MissingEvidence)
    );
    assert_eq!(original.accepted, accepted);
}

#[test]
fn artifact_row_rejects_mismatched_verified_descriptor_facts() {
    let fixture = fixture(400);
    let source = &fixture.artifact;
    let clone = || copy_descriptor(source.descriptor()).unwrap();
    let mut facts = source.facts().unwrap();
    facts.binding.content = ContentHash([90; 32]);
    assert!(matches!(
        NativeArtifact::new(clone(), source.custody(), facts),
        Err(ContractError::ContentConflict)
    ));
    let mut facts = source.facts().unwrap();
    facts.binding.revision = ObjectRevision(2);
    assert!(matches!(
        NativeArtifact::new(clone(), source.custody(), facts),
        Err(ContractError::StaleRevision)
    ));
    let mut facts = source.facts().unwrap();
    facts.producer = ISSUER;
    assert!(matches!(
        NativeArtifact::new(clone(), source.custody(), facts),
        Err(ContractError::WrongActor)
    ));
    let mut facts = source.facts().unwrap();
    facts.schema = focal_evidence::error_report_schema();
    assert!(matches!(
        NativeArtifact::new(clone(), source.custody(), facts),
        Err(ContractError::MissingEvidence)
    ));
    for revision in [None, Some(0), Some(2)] {
        let mut facts = source.facts().unwrap();
        facts.custody_revision = revision;
        assert!(matches!(
            NativeArtifact::new(clone(), source.custody(), facts),
            Err(ContractError::MissingEvidence)
        ));
    }
    source
        .custody()
        .check(request(), source.descriptor())
        .unwrap();
}

#[test]
fn invalid_singletons_and_unfunded_or_overflowing_capacity_return_errors() {
    let input = NativeArtifactInput(Vec::new());
    assert!(input.get().is_none());
    assert!(matches!(input.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(input.heap_charge(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        input.into_descriptor(),
        Err(MemoryError::MissingKey)
    ));
    let malformed = NativeArtifactInput(vec![
        descriptor(400, PayloadSpec::Inline(PAYLOAD)),
        descriptor(401, PayloadSpec::Inline(PAYLOAD)),
    ]);
    assert!(malformed.get().is_none());
    assert!(matches!(
        malformed.into_descriptor(),
        Err(MemoryError::MissingKey)
    ));
    let artifact = OwnedArtifact(Vec::new());
    assert!(artifact.get().is_none());
    assert!(matches!(artifact.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        artifact.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    let accepted = OwnedAccepted(Vec::new());
    assert!(accepted.get().is_none());
    assert!(matches!(accepted.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        accepted.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    assert!(matches!(
        singleton(
            descriptor(400, PayloadSpec::Inline(PAYLOAD)),
            INPUT_CONTAINER - 1
        ),
        Err(MemoryError::Capacity { .. })
    ));
    assert!(matches!(
        container_heap::<ArtifactDescriptor>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert!(matches!(
        container_heap::<NativeArtifact>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert!(matches!(
        container_heap::<NativeAccepted>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
}
