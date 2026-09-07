use super::super::report_tests::{
    Custody, EVALUATOR, ISSUER, QUALITY, SUBJECT, artifact_spec, begin, binding, context,
    copy_report, core, creation, descriptor, key, post, publish, report, report_for, request,
    running, verified,
};
use super::*;
use focal_evidence::{BuiltinSchemaError, StoreLimits, error_report_schema, test_report_schema};
use focal_model::lifecycle::{aggregation, artifact_descriptor::ResultProvenance};
use focal_model::{
    ArtifactRef, ContentRef, Deadline, HandlerRef, TimerId, ValidationMode, ValidatorId,
    VerdictValue,
};
use std::cell::Cell;

const DOMAIN: ContentDomainId = ContentDomainId::from_u128(1900);

struct Store {
    _directory: tempfile::TempDir,
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
        Self {
            _directory: directory,
            content,
        }
    }
}

fn stage(owner: &mut NativeOwner, input: NativeInput, time: u64) -> NativeCandidate {
    match owner
        .prepare(context(input.request.principal, time), input, None)
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        existing => panic!("expected fresh operation, got {existing:?}"),
    }
}

fn start(owner: &mut NativeOwner, index: u32, id: u128) -> NativeCandidate {
    let claim = owner.effective().claim(key(index).claim).unwrap().binding();
    let evaluation = owner.effective().evaluation(key(index)).unwrap().binding();
    stage(owner, begin(id, claim, index, evaluation), 30)
}

fn posted(requirements: &[(ValidationMode, bool)]) -> Core<NativeState> {
    let mut core = core();
    publish(&mut core, 10, creation(1, 1, requirements, None));
    publish(&mut core, 20, post(2, binding(1)));
    core
}

fn report_input(owner: &NativeOwner, index: u32, id: u128, value: VerdictValue) -> NativeInput {
    let view = owner.effective();
    let state = view.evaluation(key(index)).unwrap();
    let attempt = state
        .bind(view.definition(key(index).validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = descriptor(artifact_spec(20_000 + id, attempt.evaluator, value))
        .with_result_provenance(ResultProvenance {
            claim: key(index).claim,
            validation: key(index).validation,
            target: state.target(),
            generation: state.generation(),
            attempt,
            value,
        })
        .unwrap();
    NativeInput {
        request: request(attempt.evaluator, id),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(key(index).claim).unwrap().binding(),
            key: key(index),
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}

fn report_stage(owner: &mut NativeOwner, store: &mut Store, input: NativeInput) -> NativeCandidate {
    match owner
        .prepare_with_custody(
            context(input.request.principal, 100),
            input,
            &mut store.content,
            DOMAIN,
            &BuiltinNativeSchemas,
        )
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        existing => panic!("expected fresh report, got {existing:?}"),
    }
}

fn exhaust(parent: &MemoryBudget) -> Allocation {
    parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used,
        )
        .unwrap()
        .commit()
}

fn fallback_posted() -> Core<NativeState> {
    let mut input = creation(1, 1, &[(ValidationMode::Required, true)], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("expected creation")
    };
    let prior = &declarations[1];
    let handler = |id, agentic| HandlerRef {
        id: ValidatorId::from_u128(id),
        version: ContentHash([id as u8; 32]),
        agentic,
    };
    let check = [handler(91, false), handler(191, false)];
    let quality = [handler(92, true), handler(192, true)];
    let policy = |handler, attempts| validation::HandlerPolicy {
        handler,
        attempts,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    };
    let check_steps = [policy(&check[0], 2), policy(&check[1], 1)];
    let quality_steps = [policy(&quality[0], 1), policy(&quality[1], 1)];
    declarations[1] = validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: prior.binding(),
            claim: prior.claim(),
            issuer: ISSUER,
            declaration_index: prior.declaration_index(),
            kind: prior.kind(),
            phase: prior.declared_phase(),
            mode: prior.mode(),
            target: validation::TargetDeclaration::Admission,
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: EVALUATOR,
                    definition: ContentHash([14; 32]),
                    handlers: &check_steps,
                    required_policy: None,
                },
                quality: Some(validation::PhasePolicy {
                    evaluator: QUALITY,
                    definition: ContentHash([15; 32]),
                    handlers: &quality_steps,
                    required_policy: None,
                }),
            },
            deadline: Deadline {
                timer: TimerId(prior.binding().object.0),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap();
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &[],
        declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 32,
            max_updates: 32,
        },
    )
    .unwrap();
    let mut core = core();
    publish(&mut core, 10, input);
    publish(&mut core, 20, post(2, binding(1)));
    core
}

#[test]
fn held_begin_capacity_completes_every_retry_fallback_and_quality_phase_with_real_custody() {
    let core = fallback_posted();
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    assert_eq!(owner.book.remaining_reports(key(1)), Some(5));
    let hold = owner.book.funded_capacity();
    assert!(hold > 0);
    let pressure = exhaust(&parent);
    let exhausted = parent.stats();
    let mut store = Store::new();
    let mut candidates = Vec::new();
    let mut results = Vec::new();
    for (offset, (value, handler)) in [
        (VerdictValue::Error, 91),
        (VerdictValue::Error, 91),
        (VerdictValue::Pass, 191),
        (VerdictValue::Error, 92),
        (VerdictValue::Pass, 192),
    ]
    .into_iter()
    .enumerate()
    {
        let input = report_input(&owner, 1, 100 + offset as u128, value);
        let NativeCommand::ReportAdmission { report, .. } = &input.command else {
            panic!("report")
        };
        assert_eq!(report.attempt.handler, ValidatorId::from_u128(handler));
        assert_eq!(report.attempt.index, offset as u32);
        let candidate = report_stage(&mut owner, &mut store, input);
        candidates.push(candidate);
        let result = owner
            .effective()
            .evaluation(key(1))
            .unwrap()
            .last_result()
            .unwrap();
        results.push(result);
        assert_eq!(
            owner.book.remaining_reports(key(1)),
            Some(4 - offset as u32)
        );
        assert_eq!(owner.book.funded_capacity(), hold);
        assert_eq!(parent.stats().used, exhausted.used);
        assert_eq!(parent.stats().ordinary_used, exhausted.ordinary_used);
        let artifact = owner
            .effective()
            .artifact(result.evidence().unwrap().id)
            .unwrap();
        let payload = artifact.custody().payload();
        let bytes = store
            .content
            .read_bytes(
                &ContentRef {
                    domain: payload.domain,
                    root: payload.root,
                    length: payload.length,
                    class: payload.class,
                },
                65536,
            )
            .unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(
            artifact.descriptor().result_provenance().unwrap().value,
            value
        );
    }
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
    for candidate in candidates {
        owner.publish_after_durable(candidate).unwrap();
    }
    for result in results {
        assert_eq!(
            owner
                .committed()
                .result(NativeResultKey::of(result))
                .unwrap()
                .result(),
            result
        );
    }
    assert_eq!(
        owner.committed().claim(key(1).claim).unwrap().status(),
        ClaimStatus::Posted
    );
    assert_eq!(
        owner
            .committed()
            .claim(key(1).claim)
            .unwrap()
            .response_count(),
        0
    );
    assert!(owner.book.funded_capacity() < hold);
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn discarded_report_returns_its_attempt_and_credit_for_identical_retry_under_pressure() {
    let core = posted(&[(ValidationMode::Required, true)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    let input = report_input(&owner, 1, 111, VerdictValue::Error);
    let retry = copy_report(&input);
    let original = *owner.effective().evaluation(key(1)).unwrap();
    let pressure = exhaust(&parent);
    let before = parent.stats();
    let source = owner.book.source().stats();
    let mut store = Store::new();
    let first = report_stage(&mut owner, &mut store, input);
    assert_eq!(owner.book.remaining_reports(key(1)), Some(2));
    assert_eq!(owner.discard_from(first).unwrap(), 1);
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), original);
    assert_eq!(owner.book.remaining_reports(key(1)), Some(3));
    assert_eq!(owner.book.source().stats(), source);
    assert_eq!(parent.stats(), before);
    let second = report_stage(&mut owner, &mut store, retry);
    assert_ne!(first, second);
    owner.publish_after_durable(second).unwrap();
    assert_eq!(owner.book.remaining_reports(key(1)), Some(2));
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn tail_begin_rollback_restores_funding_and_grant_buffer_growth_without_disturbing_older_candidate()
{
    let core = posted(&[(ValidationMode::Observe, false); 3]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let before = parent.stats();
    let first = start(&mut owner, 1, 3);
    let after_first = parent.stats();
    let second = start(&mut owner, 2, 4);
    let after_second = parent.stats();
    let third = start(&mut owner, 3, 5);
    assert_eq!(owner.book.len(), 3);
    assert_eq!(owner.discard_from(third).unwrap(), 1);
    assert_eq!(owner.book.len(), 2);
    assert_eq!(parent.stats(), after_second);
    assert_eq!(owner.book.remaining_reports(key(3)), None);
    assert!(!owner.effective().evaluation(key(3)).unwrap().has_begun());
    assert_eq!(owner.discard_from(second).unwrap(), 1);
    assert_eq!(parent.stats(), after_first);
    assert_eq!(owner.oldest(), Some(first));
    assert_eq!(owner.book.remaining_reports(key(1)), Some(2));
    assert_eq!(owner.discard_from(first).unwrap(), 1);
    assert_eq!(owner.book.len(), 0);
    assert_eq!(owner.book.funded_capacity(), 0);
    assert_eq!(parent.stats(), before);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn begin_refusal_under_pressure_leaves_no_responsibility_or_grant_and_can_retry() {
    let core = posted(&[(ValidationMode::Required, false)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let claim = owner.effective().claim(key(1).claim).unwrap().binding();
    let evaluation = owner.effective().evaluation(key(1)).unwrap().binding();
    let before = parent.stats();
    let pressure = exhaust(&parent);
    let full = parent.stats();
    assert!(
        owner
            .prepare(context(EVALUATOR, 30), begin(3, claim, 1, evaluation), None)
            .is_err()
    );
    assert_eq!(parent.stats(), full);
    assert_eq!(owner.book.len(), 0);
    assert_eq!(owner.pending_len(), 0);
    assert!(!owner.effective().evaluation(key(1)).unwrap().has_begun());
    assert!(owner.effective().recorded(request(EVALUATOR, 3)).is_none());
    drop(pressure);
    let candidate = start(&mut owner, 1, 3);
    assert_eq!(owner.book.remaining_reports(key(1)), Some(2));
    owner.discard_from(candidate).unwrap();
    assert_eq!(parent.stats(), before);
}

fn finish_recovered(mut core: Core<NativeState>, index: u32, status: ClaimStatus) {
    let claim = core.native_claim(key(index).claim).unwrap().binding();
    let cut = core.native_claim(key(index).claim).unwrap().terminal_cut();
    let receipt = core.native_claim(key(index).claim).unwrap().receipt();
    let parent = core.state.budget.clone();
    // Recovery must be repeatable after an admission refusal, preserving the
    // identical Core; no construction may silently drop an open obligation.
    let pressure = exhaust(&parent);
    let refusal = NativeOwner::new(core).unwrap_err();
    core = refusal.core;
    assert_eq!(
        core.native_claim(key(index).claim).unwrap().binding(),
        claim
    );
    drop(pressure);
    let mut owner = NativeOwner::new(core).unwrap();
    assert_eq!(owner.book.len(), 1);
    assert_eq!(owner.book.remaining_reports(key(index)), Some(3));
    let pressure = exhaust(&parent);
    let mut store = Store::new();
    for (id, value) in [(301, VerdictValue::Pass), (302, VerdictValue::Pass)] {
        let input = report_input(&owner, index, id, value);
        let candidate = report_stage(&mut owner, &mut store, input);
        owner.publish_after_durable(candidate).unwrap();
    }
    let actual = owner.committed().claim(key(index).claim).unwrap();
    assert_eq!(actual.binding(), claim);
    assert_eq!(actual.status(), status);
    assert_eq!(actual.terminal_cut(), cut);
    assert_eq!(actual.receipt(), receipt);
    assert_eq!(actual.response_count(), 0);
    assert_eq!(owner.book.remaining_reports(key(index)), None);
    assert_eq!(
        owner.committed().evaluation(key(index)).unwrap().state(),
        validation::State::Validated
    );
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn owner_reconstruction_funds_begun_sibling_after_original_post_failure() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
    ]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        200,
        1,
        VerdictValue::Fail,
        descriptor(artifact_spec(1920, EVALUATOR, VerdictValue::Fail)),
    );
    let token = verified(&mut custody, &input);
    let candidate = report(&core, input, &[], &token);
    core.publish_native(candidate).unwrap();
    assert_eq!(
        core.native_claim(key(2).claim).unwrap().status(),
        ClaimStatus::PostFailed
    );
    finish_recovered(core, 2, ClaimStatus::PostFailed);
}

#[test]
fn owner_reconstruction_funds_begun_observe_chain_after_receipt_without_testament() {
    let mut core = running(&[(ValidationMode::Observe, true)]);
    let expected = core.native_claim(key(1).claim).unwrap().binding();
    publish(
        &mut core,
        40,
        NativeInput {
            request: request(SUBJECT, 201),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(1921),
            },
        },
    );
    assert_eq!(
        core.native_claim(key(1).claim).unwrap().status(),
        ClaimStatus::Received
    );
    finish_recovered(core, 1, ClaimStatus::Received);
}

struct ChangedSchemas {
    wider: Cell<bool>,
    verified: Cell<usize>,
}
impl NativeSchemaVerifier for ChangedSchemas {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
        BuiltinNativeSchemas
            .maximum_bytes(schema)
            .map(|maximum| maximum - usize::from(self.wider.get()))
    }
    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
        self.verified.set(self.verified.get() + 1);
        BuiltinNativeSchemas.verify(schema, bytes)
    }
}

#[test]
fn pinned_schema_change_cannot_spend_a_report_grant_or_prevent_exact_contract_retry() {
    let core = posted(&[(ValidationMode::Required, false)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let candidate = start(&mut owner, 1, 3);
    owner.publish_after_durable(candidate).unwrap();
    let input = report_input(&owner, 1, 401, VerdictValue::Pass);
    let retry = copy_report(&input);
    let pressure = exhaust(&parent);
    let before = parent.stats();
    let source = owner.book.source().stats();
    let mut store = Store::new();
    let schemas = ChangedSchemas {
        wider: Cell::new(true),
        verified: Cell::new(0),
    };
    assert!(
        owner
            .prepare_with_custody(
                context(EVALUATOR, 100),
                input,
                &mut store.content,
                DOMAIN,
                &schemas
            )
            .is_err()
    );
    assert_eq!(schemas.verified.get(), 0);
    assert_eq!(parent.stats(), before);
    assert_eq!(owner.book.source().stats(), source);
    assert_eq!(owner.book.remaining_reports(key(1)), Some(2));
    schemas.wider.set(false);
    let NativeStaging::Prepared { candidate, .. } = owner
        .prepare_with_custody(
            context(EVALUATOR, 100),
            retry,
            &mut store.content,
            DOMAIN,
            &schemas,
        )
        .unwrap()
    else {
        panic!("fresh report")
    };
    assert_eq!(schemas.verified.get(), 1);
    owner.publish_after_durable(candidate).unwrap();
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn retired_grant_keeps_published_and_pinned_pages_charged_until_owner_releases_them() {
    let core = posted(&[(ValidationMode::Required, false)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    let mut store = Store::new();
    let error = report_input(&owner, 1, 501, VerdictValue::Error);
    let error = report_stage(&mut owner, &mut store, error);
    owner.publish_after_durable(error).unwrap();
    let pin = owner.pin(0, 100).unwrap();
    let pass = report_input(&owner, 1, 502, VerdictValue::Pass);
    let pass = report_stage(&mut owner, &mut store, pass);
    owner.publish_after_durable(pass).unwrap();
    assert_eq!(owner.book.remaining_reports(key(1)), None);
    assert!(owner.book.source().stats().used > 0);
    assert!(owner.book.funded_capacity() >= owner.book.source().stats().used);
    assert_eq!(
        pin.with_evaluation(key(1), 1, |state| state.state())
            .unwrap(),
        Some(validation::State::Validating)
    );
    let retained = owner.book.source().stats().used;
    owner.release(&pin).unwrap();
    assert!(owner.book.source().stats().used < retained);
    assert!(owner.book.source().stats().used > 0);
    drop(owner);
    assert_eq!(
        parent.stats().used,
        parent.stats().by_kind[BudgetKind::ReadPins as usize]
    );
    assert!(parent.stats().used > 0);
    drop(pin);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn ordinary_mutation_cannot_spend_candidate_serials_promised_to_the_live_report_chain() {
    let core = posted(&[(ValidationMode::Required, false)]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let begun = start(&mut owner, 1, 3);
    owner.publish_after_durable(begun).unwrap();
    // Two actual report attempts and one authority-control ticket remain.
    owner.next_serial = u64::MAX - 3;
    let serial = owner.next_serial;
    let budget = parent.stats();
    let source = owner.book.source().stats();
    let funding = owner.book.funded_capacity();
    let sequence = owner.effective().sequence();
    let evaluation = *owner.effective().evaluation(key(1)).unwrap();
    let refusal = owner
        .prepare(context(ISSUER, 40), creation(601, 2, &[], None), None)
        .unwrap_err();
    assert!(matches!(
        refusal,
        NativeOwnerError::Native(NativeError::Capacity("completion candidate margin"))
    ));
    assert_eq!(owner.next_serial, serial);
    assert_eq!(parent.stats(), budget);
    assert_eq!(owner.book.source().stats(), source);
    assert_eq!(owner.book.funded_capacity(), funding);
    assert_eq!(owner.book.remaining_reports(key(1)), Some(2));
    assert_eq!(owner.effective().sequence(), sequence);
    assert_eq!(*owner.effective().evaluation(key(1)).unwrap(), evaluation);
    assert_eq!(owner.pending_len(), 0);
    assert!(owner.effective().claim(ClaimId::from_u128(2)).is_none());
    assert!(owner.effective().recorded(request(ISSUER, 601)).is_none());

    let pressure = exhaust(&parent);
    let mut store = Store::new();
    for (offset, value) in [VerdictValue::Error, VerdictValue::Pass]
        .into_iter()
        .enumerate()
    {
        let input = report_input(&owner, 1, 602 + offset as u128, value);
        let candidate = report_stage(&mut owner, &mut store, input);
        assert_eq!(candidate.serial, serial + 1 + offset as u64);
        owner.publish_after_durable(candidate).unwrap();
    }
    assert_eq!(owner.next_serial, u64::MAX - 1);
    assert_eq!(owner.book.remaining_reports(key(1)), None);
    assert_eq!(
        owner.committed().evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}
