use super::report_tests::{
    EVALUATOR, ISSUER, QUALITY, artifact_spec, context, copy_report, creation, descriptor, key,
    report_for, request, running,
};
use super::*;
use focal_evidence::{
    BuiltinNativeSchemas, BuiltinSchemaError, ContentStore, NativeEvidenceError,
    NativeSchemaVerifier, StoreLimits, error_report_schema,
};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    artifact_descriptor::{ArtifactDescriptor, ArtifactSpec, PayloadSpec},
    succession::CorrectionKind,
};
use focal_model::{
    ArtifactRef, ContentDomainId, ContentRef, ObjectId, ObjectKind, ObjectRef, ParticipantId,
    VerdictValue,
};
use std::{cell::Cell, path::PathBuf};

const DOMAIN: ContentDomainId = ContentDomainId::from_u128(901);

#[derive(Default)]
struct RecordingSchemas {
    lookups: Cell<usize>,
    verifications: Cell<usize>,
    refuse: Cell<bool>,
}
impl NativeSchemaVerifier for RecordingSchemas {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
        self.lookups.set(self.lookups.get() + 1);
        if self.refuse.get() {
            return Err(BuiltinSchemaError::Unsupported);
        }
        BuiltinNativeSchemas.maximum_bytes(schema)
    }

    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
        self.verifications.set(self.verifications.get() + 1);
        if self.refuse.get() {
            return Err(BuiltinSchemaError::Invalid);
        }
        BuiltinNativeSchemas.verify(schema, bytes)
    }
}
impl RecordingSchemas {
    fn counts(&self) -> (usize, usize) {
        (self.lookups.get(), self.verifications.get())
    }
}

struct Store {
    directory: tempfile::TempDir,
    content: ContentStore,
}
impl Store {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let content = ContentStore::open(
            directory.path(),
            StoreLimits {
                max_content_bytes: 2 * 1024 * 1024,
                max_staging_bytes: 4 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 17,
                max_manifest_bytes: 128 * 1024,
            },
        )
        .unwrap();
        Self { directory, content }
    }

    // Include directories too: a rejected request must not create even a new
    // content-tree path, upload, or partially sealed object.
    fn files(&self) -> Vec<(PathBuf, Option<Vec<u8>>)> {
        let mut found = Vec::new();
        let mut pending = vec![self.directory.path().to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                let relative = path
                    .strip_prefix(self.directory.path())
                    .unwrap()
                    .to_path_buf();
                if path.is_dir() {
                    found.push((relative, None));
                    pending.push(path);
                } else {
                    found.push((relative, Some(std::fs::read(path).unwrap())));
                }
            }
        }
        found.sort();
        found
    }
}

fn ordinary_report(core: &Core<NativeState>, id: u128) -> NativeInput {
    report_for(
        core,
        None,
        id,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(902, EVALUATOR, VerdictValue::Pass)),
    )
}

fn replace_artifact(input: &mut NativeInput, descriptor: ArtifactDescriptor) {
    let NativeCommand::ReportAdmission {
        report, artifact, ..
    } = &mut input.command
    else {
        panic!("expected report")
    };
    report.evidence = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    *artifact = NativeArtifactInput::new(descriptor).unwrap();
}

fn refused_before_custody(
    owner: &mut NativeOwner,
    store: &mut Store,
    schemas: &RecordingSchemas,
    context: NativeContext,
    input: NativeInput,
    expected: ContractError,
) {
    let files = store.files();
    let budget = owner.budget_stats();
    let sequence = owner.effective().sequence();
    let pending = owner.pending_len();
    let evaluation = *owner.effective().evaluation(key(1)).unwrap();
    let request = input.request;
    let counts = schemas.counts();
    let result = owner.prepare_with_custody(context, input, &mut store.content, DOMAIN, schemas);
    assert!(
        matches!(result, Err(NativeOwnerError::Native(NativeError::Contract(error))) if error == expected),
        "expected {expected:?}, got {result:?}"
    );
    assert_eq!(schemas.counts(), counts);
    assert_eq!(store.files(), files);
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.effective().sequence(), sequence);
    assert_eq!(owner.pending_len(), pending);
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), evaluation);
    assert!(owner.effective().recorded(request).is_none());
}

#[test]
fn report_actor_frame_and_attempt_refusals_precede_all_schema_or_content_access() {
    let core = running(&[(focal_model::ValidationMode::Required, false)]);
    let original = ordinary_report(&core, 911);
    let mut owner = NativeOwner::new(core).unwrap();
    let mut store = Store::new();
    let schemas = RecordingSchemas::default();
    schemas.refuse.set(true);
    for case in 0..9 {
        let mut input = copy_report(&original);
        let NativeCommand::ReportAdmission {
            claim,
            expected,
            report,
            ..
        } = &mut input.command
        else {
            panic!("expected report")
        };
        let error = match case {
            0 => {
                input.request.principal = ParticipantId::from_u128(999);
                ContractError::WrongActor
            }
            1 => {
                claim.revision.0 += 1;
                ContractError::StaleRevision
            }
            2 => {
                expected.revision.0 += 1;
                ContractError::StaleRevision
            }
            3 => {
                expected.content = ContentHash([99; 32]);
                ContractError::ContentConflict
            }
            4 => {
                report.generation += 1;
                ContractError::StaleEvaluation
            }
            5 => {
                report.attempt.index += 1;
                ContractError::StaleEvaluation
            }
            6 => {
                report.attempt.phase = validation::Phase::Quality;
                ContractError::StaleEvaluation
            }
            7 => {
                report.attempt.version = ContentHash([98; 32]);
                ContractError::StaleEvaluation
            }
            _ => {
                report.attempt.definition = ContentHash([97; 32]);
                ContractError::StaleEvaluation
            }
        };
        let context = context(input.request.principal, 100);
        refused_before_custody(&mut owner, &mut store, &schemas, context, input, error);
    }
    refused_before_custody(
        &mut owner,
        &mut store,
        &schemas,
        NativeContext {
            principal: Principal::Node(EVALUATOR),
            logical_time: 100,
        },
        copy_report(&original),
        ContractError::WrongActor,
    );
    refused_before_custody(
        &mut owner,
        &mut store,
        &schemas,
        context(EVALUATOR, 29),
        copy_report(&original),
        ContractError::InvalidCut,
    );
    assert_eq!(schemas.counts(), (0, 0));
}

#[test]
fn descriptor_provenance_schema_and_input_references_are_checked_before_custody() {
    let core = running(&[(focal_model::ValidationMode::Required, false)]);
    let original = ordinary_report(&core, 921);
    let NativeCommand::ReportAdmission { artifact, .. } = &original.command else {
        panic!("expected report")
    };
    let provenance = artifact.get().unwrap().result_provenance().unwrap();
    let ledger = artifact.get().unwrap().ledger();
    let mut owner = NativeOwner::new(core).unwrap();
    let mut store = Store::new();
    let schemas = RecordingSchemas::default();
    schemas.refuse.set(true);
    for case in 0..8 {
        let mut input = copy_report(&original);
        let mut spec = artifact_spec(902, EVALUATOR, VerdictValue::Pass);
        spec.result = Some(provenance);
        let mut reference = ObjectRef {
            ledger,
            kind: ObjectKind::Claim,
            id: ObjectId::from_u128(999),
        };
        let mut altered = provenance;
        let expected = match case {
            0 => {
                spec.result = None;
                ContractError::MissingEvidence
            }
            1 => {
                altered.attempt.index += 1;
                spec.result = Some(altered);
                ContractError::MissingEvidence
            }
            2 => {
                altered.value = VerdictValue::Fail;
                spec.result = Some(altered);
                ContractError::MissingEvidence
            }
            3 => {
                spec.schema_hash = error_report_schema();
                ContractError::MissingEvidence
            }
            4 => ContractError::InvalidTarget,
            5 => {
                reference.kind = ObjectKind::Artifact;
                ContractError::InvalidTarget
            }
            6 => {
                reference.kind = ObjectKind::Testament;
                ContractError::InvalidTarget
            }
            _ => ContractError::WrongLedger,
        };
        let references = [reference];
        if (4..7).contains(&case) {
            spec.inputs = &references;
        }
        replace_artifact(&mut input, descriptor(spec));
        if case == 7 {
            // Cross-ledger descriptor inputs are rejected by construction. A
            // foreign owner frame is representable ingress and must still be
            // rejected before the custody adapter or schema registry runs.
            let NativeCommand::ReportAdmission { claim, .. } = &mut input.command else {
                panic!("expected report")
            };
            claim.ledger.session = focal_model::SessionId::from_u128(999);
        }
        refused_before_custody(
            &mut owner,
            &mut store,
            &schemas,
            context(EVALUATOR, 100),
            input,
            expected,
        );
    }
    for id in [false, true] {
        let mut input = copy_report(&original);
        let NativeCommand::ReportAdmission { report, .. } = &mut input.command else {
            panic!("expected report")
        };
        let expected = if id {
            report.evidence.id = ArtifactId::from_u128(999);
            ContractError::WrongObject
        } else {
            report.evidence.hash = ContentHash([99; 32]);
            ContractError::ContentConflict
        };
        refused_before_custody(
            &mut owner,
            &mut store,
            &schemas,
            context(EVALUATOR, 100),
            input,
            expected,
        );
    }
    assert_eq!(schemas.counts(), (0, 0));
}

#[test]
fn pending_parent_fences_and_expired_attempts_refuse_before_custody() {
    for correction in [None, Some(CorrectionKind::Supersedes)] {
        let core = running(&[(focal_model::ValidationMode::Required, false)]);
        let mut input = ordinary_report(&core, 931);
        let claim = core.native_claim(key(1).claim).unwrap().binding();
        let mut owner = NativeOwner::new(core).unwrap();
        let control = correction.map_or_else(
            || NativeInput {
                request: request(ISSUER, 932),
                command: NativeCommand::Cancel { expected: claim },
            },
            |kind| creation(932, 2, &[], Some(kind)),
        );
        owner.prepare(context(ISSUER, 90), control, None).unwrap();
        let NativeCommand::ReportAdmission {
            claim, expected, ..
        } = &mut input.command
        else {
            panic!("expected report")
        };
        // Use the current effective frame so this exercises the actual retained
        // authority fence rather than merely rejecting an old row revision.
        *claim = owner.effective().claim(key(1).claim).unwrap().binding();
        *expected = owner.effective().evaluation(key(1)).unwrap().binding();
        let mut store = Store::new();
        let schemas = RecordingSchemas::default();
        refused_before_custody(
            &mut owner,
            &mut store,
            &schemas,
            context(EVALUATOR, 100),
            input,
            ContractError::StaleEvaluation,
        );
        assert!(
            owner
                .effective()
                .evaluation(key(1))
                .unwrap()
                .fence()
                .is_some()
        );
        assert!(
            owner
                .committed()
                .evaluation(key(1))
                .unwrap()
                .fence()
                .is_none()
        );
    }
    let core = running(&[(focal_model::ValidationMode::Required, false)]);
    let input = ordinary_report(&core, 933);
    let mut owner = NativeOwner::new(core).unwrap();
    let mut store = Store::new();
    refused_before_custody(
        &mut owner,
        &mut store,
        &RecordingSchemas::default(),
        context(EVALUATOR, 1000),
        input,
        ContractError::StaleEvaluation,
    );
}

#[test]
fn valid_local_report_and_exact_pending_and_committed_retries_need_no_new_custody_or_budget() {
    let core = running(&[(focal_model::ValidationMode::Required, false)]);
    let input = ordinary_report(&core, 941);
    let retry = copy_report(&input);
    let committed_retry = copy_report(&input);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let mut store = Store::new();
    let before_files = store.files();
    let schemas = RecordingSchemas::default();
    let NativeStaging::Prepared { candidate, outcome } = owner
        .prepare_with_custody(
            context(EVALUATOR, 100),
            input,
            &mut store.content,
            DOMAIN,
            &schemas,
        )
        .unwrap()
    else {
        panic!("expected fresh report")
    };
    assert!(schemas.lookups.get() > 0);
    assert_eq!(schemas.verifications.get(), 1);
    assert_ne!(store.files(), before_files);
    assert_eq!(
        owner.effective().evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    assert!(
        owner
            .committed()
            .evaluation(key(1))
            .unwrap()
            .last_result()
            .is_none()
    );
    let artifact = owner
        .effective()
        .artifact(ArtifactId::from_u128(902))
        .unwrap();
    let payload = artifact.custody().payload();
    let reference = ContentRef {
        domain: payload.domain,
        root: payload.root,
        length: payload.length,
        class: payload.class,
    };
    let PayloadSpec::Inline(expected) = artifact.descriptor().payload() else {
        panic!("expected authored inline evidence")
    };
    assert_eq!(
        store
            .content
            .read_bytes(&reference, expected.len())
            .unwrap(),
        expected
    );
    let sealed = store.files();
    let counts = schemas.counts();
    schemas.refuse.set(true);
    let pressure = parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used,
        )
        .unwrap();
    let budget = parent.stats();
    // Even invalid storage domain and a now-failing registry are immaterial to
    // exact retries. Their original candidate and custody already exist.
    assert_eq!(
        owner
            .prepare_with_custody(
                context(EVALUATOR, 0),
                retry,
                &mut store.content,
                ContentDomainId::from_u128(0),
                &schemas,
            )
            .unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: Some(candidate)
        }
    );
    assert_eq!(parent.stats(), budget);
    assert_eq!(owner.publish_after_durable(candidate).unwrap(), outcome);
    let committed_budget = parent.stats();
    assert_eq!(
        owner
            .prepare_with_custody(
                context(EVALUATOR, 0),
                committed_retry,
                &mut store.content,
                ContentDomainId::from_u128(0),
                &schemas,
            )
            .unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: None
        }
    );
    assert_eq!(parent.stats(), committed_budget);
    assert_eq!(schemas.counts(), counts);
    assert_eq!(store.files(), sealed);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!((outcome.artifacts, outcome.results), (1, 1));
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn schema_refusal_does_not_seal_content_or_consume_the_report_request() {
    let core = running(&[(focal_model::ValidationMode::Required, false)]);
    let input = ordinary_report(&core, 951);
    let retry = copy_report(&input);
    let mut owner = NativeOwner::new(core).unwrap();
    let mut store = Store::new();
    let files = store.files();
    let budget = owner.budget_stats();
    let schemas = RecordingSchemas::default();
    schemas.refuse.set(true);
    let error = owner
        .prepare_with_custody(
            context(EVALUATOR, 100),
            input,
            &mut store.content,
            DOMAIN,
            &schemas,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        NativeOwnerError::Native(NativeError::Evidence(NativeEvidenceError::Schema(
            BuiltinSchemaError::Unsupported
        )))
    ));
    assert!(schemas.lookups.get() > 0);
    assert_eq!(schemas.verifications.get(), 0);
    assert_eq!(store.files(), files);
    assert_eq!(owner.budget_stats(), budget);
    assert!(
        owner
            .effective()
            .recorded(request(EVALUATOR, 951))
            .is_none()
    );
    assert_eq!(owner.pending_len(), 0);
    schemas.refuse.set(false);
    assert!(matches!(
        owner
            .prepare_with_custody(
                context(EVALUATOR, 100),
                retry,
                &mut store.content,
                DOMAIN,
                &schemas,
            )
            .unwrap(),
        NativeStaging::Prepared { .. }
    ));
    assert_eq!(schemas.verifications.get(), 1);
}

// A separate owner-view constructor exercises authority against a pending
// quality transition, without exposing or importing detached owner candidates.
fn quality_report(owner: &NativeOwner, spec: ArtifactSpec<'_>) -> NativeInput {
    let view = owner.effective();
    let state = view.evaluation(key(1)).unwrap();
    let attempt = state
        .bind(view.definition(key(1).validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = descriptor(spec)
        .with_result_provenance(
            focal_model::lifecycle::artifact_descriptor::ResultProvenance {
                claim: key(1).claim,
                validation: key(1).validation,
                target: state.target(),
                generation: state.generation(),
                attempt,
                value: VerdictValue::Pass,
            },
        )
        .unwrap();
    NativeInput {
        request: request(QUALITY, 963),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(key(1).claim).unwrap().binding(),
            key: key(1),
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value: VerdictValue::Pass,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}

#[test]
fn pending_quality_report_inherits_input_visibility_before_verification() {
    let core = running(&[(focal_model::ValidationMode::Required, true)]);
    let input = ordinary_report(&core, 961);
    let mut owner = NativeOwner::new(core).unwrap();
    let mut store = Store::new();
    let schemas = RecordingSchemas::default();
    let NativeStaging::Prepared {
        candidate: first, ..
    } = owner
        .prepare_with_custody(
            context(EVALUATOR, 100),
            input,
            &mut store.content,
            DOMAIN,
            &schemas,
        )
        .unwrap()
    else {
        panic!("expected fresh report")
    };
    let inputs = [ObjectRef {
        ledger: owner.effective().ledger(),
        kind: ObjectKind::Artifact,
        id: ObjectId::from_u128(902),
    }];
    let mut spec = artifact_spec(903, QUALITY, VerdictValue::Pass);
    spec.inputs = &inputs;
    spec.visibility = &[];
    let invalid = quality_report(&owner, spec);
    refused_before_custody(
        &mut owner,
        &mut store,
        &schemas,
        context(QUALITY, 101),
        invalid,
        ContractError::InvalidPolicy,
    );
    spec.visibility = &["internal"];
    let valid = quality_report(&owner, spec);
    let NativeStaging::Prepared {
        candidate: second, ..
    } = owner
        .prepare_with_custody(
            context(QUALITY, 101),
            valid,
            &mut store.content,
            DOMAIN,
            &schemas,
        )
        .unwrap()
    else {
        panic!("expected fresh quality report")
    };
    assert_eq!(owner.pending_len(), 2);
    assert_eq!(schemas.verifications.get(), 2);
    assert_eq!(
        owner.effective().evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    owner.publish_after_durable(first).unwrap();
    owner.publish_after_durable(second).unwrap();
    assert!(
        owner
            .committed()
            .artifact(ArtifactId::from_u128(903))
            .is_some()
    );
}
