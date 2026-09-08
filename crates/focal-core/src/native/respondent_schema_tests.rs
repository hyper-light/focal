//! Schema selection follows the action's actual new evidence. Retained reports
//! and an exhausted receipt do not acquire a new diagnostic-verifier dependency.
use super::*;
use focal_evidence::{BuiltinSchemaError, NativeSchemaVerifier};
use std::cell::Cell;

#[derive(Default)]
struct NoSchemas {
    maximum: Cell<usize>,
    verification: Cell<usize>,
}
impl NativeSchemaVerifier for NoSchemas {
    fn maximum_bytes(&self, _: ContentHash) -> Result<usize, BuiltinSchemaError> {
        self.maximum.set(self.maximum.get() + 1);
        Err(BuiltinSchemaError::Unsupported)
    }
    fn verify(&self, _: ContentHash, _: &[u8]) -> Result<(), BuiltinSchemaError> {
        self.verification.set(self.verification.get() + 1);
        Err(BuiltinSchemaError::Unsupported)
    }
}
fn stage_with(
    f: &mut Fixture,
    input: NativeInput,
    schemas: &impl NativeSchemaVerifier,
) -> Result<NativeStaging, NativeOwnerError> {
    let time = f.owner.effective().logical_time() + 1;
    f.owner.prepare_with_custody(
        context(input.request.principal, time),
        input,
        &mut f.store,
        ContentDomainId::from_u128(93),
        schemas,
    )
}
fn publish_with(
    f: &mut Fixture,
    input: NativeInput,
    schemas: &impl NativeSchemaVerifier,
) -> NativeOutcome {
    let NativeStaging::Prepared { candidate, .. } = stage_with(f, input, schemas).unwrap() else {
        panic!("fresh")
    };
    f.owner.publish_after_durable(candidate).unwrap()
}

#[test]
fn closing_and_posting_retained_failure_testimony_never_query_a_new_schema() {
    let configured = limits(2);
    let mut f = Fixture::with_limits(configured);
    let diagnostic = f.diagnostic(94_001);
    let unavailable = NoSchemas::default();
    let command = f.close(94_002, OutcomeKind::Failed, vec![], vec![diagnostic]);
    let close = f.input(SUBJECT, command);
    let retry = duplicate(&close);
    let outcome = publish_with(&mut f, close, &unavailable);
    existing(
        stage_with(&mut f, retry, &unavailable).unwrap(),
        outcome,
        None,
    );
    let post = post_input(&mut f, 94_002);
    publish_with(&mut f, post, &unavailable);
    assert_eq!(unavailable.maximum.get(), 0);
    assert_eq!(unavailable.verification.get(), 0);
    let response = f
        .owner
        .committed()
        .response(TestamentId::from_u128(94_002))
        .unwrap();
    assert_eq!(response.state(), ResponseState::Posted);
    assert_eq!(response.reported_outcome(), OutcomeKind::Failed);
    assert_eq!(response.diagnostics().len(), 1);
    assert_eq!(response.diagnostics()[0].artifact(), diagnostic);
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 3,
            closes: 3,
            posts: 3
        }
    );
}

#[test]
fn zero_remaining_receipt_recovers_without_pinning_an_unneeded_schema() {
    let mut configured = limits(2);
    // The test-only retained-row copy is one bounded batch; four complete
    // histories exceed the smaller command fixture's 32-entry batch ceiling.
    configured.range.max_batch_entries = 128;
    let mut f = Fixture::with_limits(configured);
    for id in 95_001..=95_004 {
        f.commit(SUBJECT, f.close(id, OutcomeKind::Complete, vec![], vec![]));
        let post = post_input(&mut f, id);
        let NativeStaging::Prepared { candidate, .. } = prepare(&mut f, post).unwrap() else {
            panic!("post")
        };
        f.owner.publish_after_durable(candidate).unwrap();
    }
    assert_eq!(credit(&f, configured), RespondentCredit::default());
    let sequence = f.owner.committed().sequence();
    let core = copied(&f, configured);
    let unavailable = NoSchemas::default();
    let owner = NativeOwner::with_schemas(core, &unavailable).unwrap();
    assert_eq!(unavailable.maximum.get(), 0);
    assert_eq!(unavailable.verification.get(), 0);
    assert_eq!(owner.committed().sequence(), sequence);
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        4
    );
    for id in 95_001..=95_004 {
        assert_eq!(
            owner
                .committed()
                .response(TestamentId::from_u128(id))
                .unwrap()
                .state(),
            ResponseState::Posted
        );
    }
}

const CUSTOM_SCHEMA: ContentHash = ContentHash([173; 32]);
const CUSTOM_PAYLOAD: &[u8] = b"A bounded custom work diagnostic.";
#[derive(Default)]
struct CustomOnly {
    custom_maximum: Cell<usize>,
    custom_verification: Cell<usize>,
    unavailable_builtin: Cell<usize>,
}
impl NativeSchemaVerifier for CustomOnly {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
        if schema != CUSTOM_SCHEMA {
            self.unavailable_builtin
                .set(self.unavailable_builtin.get() + 1);
            return Err(BuiltinSchemaError::Unsupported);
        }
        self.custom_maximum.set(self.custom_maximum.get() + 1);
        Ok(CUSTOM_PAYLOAD.len())
    }
    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
        if schema != CUSTOM_SCHEMA || bytes != CUSTOM_PAYLOAD {
            return Err(BuiltinSchemaError::Invalid);
        }
        self.custom_verification
            .set(self.custom_verification.get() + 1);
        Ok(())
    }
}

#[test]
fn custom_work_diagnostic_uses_ordinary_verification_when_builtin_schema_is_unavailable() {
    let configured = limits(2);
    let mut f = Fixture::with_limits(configured);
    let parent = f.parent();
    let artifact = NativeArtifactInput::new(descriptor(ArtifactSpec {
        ledger: parent.ledger,
        id: ArtifactId::from_u128(96_001),
        schema: 1,
        kind: "error",
        schema_hash: CUSTOM_SCHEMA,
        metadata: b"{}",
        payload: PayloadSpec::Inline(CUSTOM_PAYLOAD),
        producer: SUBJECT,
        receipt: Some(parent.receipt),
        result: None,
        work: Some(WorkProvenance {
            claim: parent.claim,
            cycle: parent.next_cycle,
            role: WorkRole::Diagnostic {
                reason: EvidenceFailure::Work,
            },
        }),
        inputs: &[],
        visibility: &[],
    }))
    .unwrap();
    let reference = ArtifactRef {
        id: artifact.get().unwrap().id(),
        hash: artifact.get().unwrap().content_hash(),
    };
    let input = f.input(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: f.claim(),
            reason: EvidenceFailure::Work,
            artifact,
        },
    );
    let retry = duplicate(&input);
    let custom = CustomOnly::default();
    let outcome = publish_with(&mut f, input, &custom);
    assert_eq!(custom.custom_verification.get(), 1);
    assert!(custom.custom_maximum.get() > 0);
    assert_eq!(custom.unavailable_builtin.get(), 0);
    let calls = custom.custom_maximum.get();
    existing(stage_with(&mut f, retry, &custom).unwrap(), outcome, None);
    assert_eq!(custom.custom_maximum.get(), calls);
    assert_eq!(custom.custom_verification.get(), 1);
    assert_eq!(
        credit(&f, configured),
        RespondentCredit {
            diagnostics: 3,
            closes: 4,
            posts: 4
        }
    );
    let record = f.owner.committed().artifact(reference.id).unwrap();
    assert_eq!(record.descriptor().schema_hash(), CUSTOM_SCHEMA);
    assert_eq!(record.descriptor().content_hash(), reference.hash);
    assert!(record.custody().local_revision() > 0);
    let command = f.close(96_002, OutcomeKind::Failed, vec![], vec![reference]);
    let close = f.input(SUBJECT, command);
    publish_with(&mut f, close, &custom);
    let post = post_input(&mut f, 96_002);
    publish_with(&mut f, post, &custom);
    assert_eq!(custom.custom_maximum.get(), calls);
    assert_eq!(custom.custom_verification.get(), 1);
    assert_eq!(custom.unavailable_builtin.get(), 0);
}
