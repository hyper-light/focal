use super::*;

#[path = "report_artifact_tests.rs"]
mod artifact_view_tests;

use focal_evidence::{
    BuiltinNativeSchemas, ContentStore, StoreLimits, VerifiedNativeArtifact, error_report_schema,
    test_report_schema,
};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    aggregation,
    artifact_descriptor::{
        ArtifactDescriptor, ArtifactSpec, Limits as ArtifactLimits, PayloadSpec,
    },
    claim::{ClaimDefinition, ClaimTerminalCut},
    graph, scope,
    succession::{Correction, CorrectionKind, Lineage},
};
use focal_model::{
    ArtifactRef, Cause, ContentDomainId, ContentRef, Deadline, HandlerRef, ObjectId, ObjectKind,
    ObjectRef, ObjectRevision, ParticipantId, RequestEpoch, RequestId, RootCommandId, SessionId,
    TenantId, TimerId, ValidationKind, ValidationMode, ValidationPhase, ValidatorId, VerdictValue,
};

pub(super) const ISSUER: ParticipantId = ParticipantId::from_u128(61);
pub(super) const SUBJECT: ParticipantId = ParticipantId::from_u128(62);
pub(super) const EVALUATOR: ParticipantId = ParticipantId::from_u128(63);
pub(super) const QUALITY: ParticipantId = ParticipantId::from_u128(64);
const PROOF: &[u8] = br#"{"passed":3,"failed":0,"skipped":0}"#;
const FAILED_PROOF: &[u8] = br#"{"passed":0,"failed":1,"skipped":0}"#;
const DIAGNOSTIC: &[u8] =
    br#"{"code":"tool_unavailable","message":"The evaluator could not reach its tool."}"#;

pub(super) fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(71),
        session: SessionId::from_u128(72),
    }
}
pub(super) fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([11; 32]),
        revision: ObjectRevision(1),
    }
}
pub(super) fn request(actor: ParticipantId, id: u128) -> RequestKey {
    RequestKey {
        principal: actor,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(id),
    }
}
pub(super) fn context(actor: ParticipantId, logical_time: u64) -> NativeContext {
    NativeContext {
        principal: Principal::Actor(actor),
        logical_time,
    }
}
pub(super) fn key(index: u32) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(1),
        validation: ValidationId::from_u128(100 + u128::from(index)),
        target: EvaluationTarget::Admission,
        generation: 1,
    }
}
fn native_limits() -> NativeLimits {
    NativeLimits {
        range: RangeConfig {
            page_entries: 4,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 16,
        // Include the future response registrations and their completion seals.
        // Tests exercising refusal replace this with the specific bound under test.
        plan_edges: 4096,
        preparation_bytes: 1024 * 1024,
        evaluations_per_claim: 16,
        ..NativeLimits::default()
    }
}
pub(super) fn core() -> Core<NativeState> {
    Core::new_native(
        ledger(),
        RangeId(81),
        native_limits(),
        MemoryBudget::new(80 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}
fn definition(
    claim: u128,
    index: u32,
    mode: ValidationMode,
    quality: bool,
) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(91),
        version: ContentHash([12; 32]),
        agentic: false,
    };
    let quality_handler = HandlerRef {
        id: ValidatorId::from_u128(92),
        version: ContentHash([13; 32]),
        agentic: true,
    };
    let steps = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    let quality_steps = [validation::HandlerPolicy {
        handler: &quality_handler,
        attempts: 1,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(claim * 100 + u128::from(index)),
            claim: ClaimId::from_u128(claim),
            issuer: ISSUER,
            declaration_index: index,
            kind: if index == 0 {
                ValidationKind::Receipt
            } else {
                ValidationKind::Test
            },
            phase: if index == 0 {
                ValidationPhase::WholeWork
            } else {
                ValidationPhase::Admission
            },
            mode,
            target: if index == 0 {
                validation::TargetDeclaration::Delivery
            } else {
                validation::TargetDeclaration::Admission
            },
            program: if index == 0 {
                validation::Program::Delivery
            } else {
                validation::Program::Programmatic {
                    check: validation::PhasePolicy {
                        evaluator: EVALUATOR,
                        definition: ContentHash([14; 32]),
                        handlers: &steps,
                        required_policy: None,
                    },
                    quality: quality.then_some(validation::PhasePolicy {
                        evaluator: QUALITY,
                        definition: ContentHash([15; 32]),
                        handlers: &quality_steps,
                        required_policy: None,
                    }),
                }
            },
            deadline: Deadline {
                timer: TimerId::from_u128(claim * 100 + u128::from(index)),
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
    .unwrap()
}
pub(super) fn creation(
    id: u128,
    claim: u128,
    requirements: &[(ValidationMode, bool)],
    correction: Option<CorrectionKind>,
) -> NativeInput {
    let mut declarations = vec![definition(claim, 0, ValidationMode::Required, false)];
    for (offset, &(mode, quality)) in requirements.iter().enumerate() {
        declarations.push(definition(
            claim,
            u32::try_from(offset + 1).unwrap(),
            mode,
            quality,
        ));
    }
    let lineage = match correction {
        Some(kind) => Lineage::new(
            binding(claim),
            Cause::Root(RootCommandId::from_u128(claim)),
            &[Correction {
                kind,
                predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(1)),
            }],
            1,
        )
        .unwrap(),
        None => Lineage::root(binding(claim), RootCommandId::from_u128(claim)).unwrap(),
    };
    let claim = Proposal {
        definition: ClaimDefinition {
            binding: binding(claim),
            issuer: ISSUER,
            subject: SUBJECT,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage,
            acceptance: aggregation::AcceptancePolicy::new(
                binding(claim),
                ISSUER,
                &[],
                &declarations,
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 16,
                    max_results: 32,
                    max_updates: 32,
                },
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 16,
                children: 8,
            },
        },
        owner: None,
    };
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Create {
            claims: vec![claim],
            declarations,
        },
    }
}
pub(super) fn post(id: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(ISSUER, id),
        command: NativeCommand::Post { expected },
    }
}
pub(super) fn begin(id: u128, claim: Binding, index: u32, expected: Binding) -> NativeInput {
    NativeInput {
        request: request(EVALUATOR, id),
        command: NativeCommand::BeginAdmission {
            claim,
            key: key(index),
            expected,
        },
    }
}
pub(super) fn prepared(result: Result<NativePreparation, NativeError>) -> NativePrepared {
    match result.unwrap() {
        NativePreparation::Prepared(value) => value,
        NativePreparation::Existing { .. } => panic!("new request unexpectedly replayed"),
    }
}
pub(super) fn publish(
    core: &mut Core<NativeState>,
    time: u64,
    input: NativeInput,
) -> NativeOutcome {
    let next = prepared(core.prepare_native(context(input.request.principal, time), input, &[]));
    core.publish_native(next).unwrap()
}
pub(super) fn running(requirements: &[(ValidationMode, bool)]) -> Core<NativeState> {
    let mut core = core();
    initialize(&mut core, requirements);
    core
}
fn initialize(core: &mut Core<NativeState>, requirements: &[(ValidationMode, bool)]) {
    publish(core, 10, creation(1, 1, requirements, None));
    publish(core, 20, post(2, binding(1)));
    for index in 1..=u32::try_from(requirements.len()).unwrap() {
        let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
        let state = core.native_evaluation(key(index)).unwrap().binding();
        publish(core, 30, begin(10 + u128::from(index), claim, index, state));
    }
}
pub(super) fn artifact_spec(
    id: u128,
    producer: ParticipantId,
    value: VerdictValue,
) -> ArtifactSpec<'static> {
    let diagnostic = matches!(value, VerdictValue::Error | VerdictValue::Incomplete);
    ArtifactSpec {
        ledger: ledger(),
        id: ArtifactId::from_u128(id),
        schema: 1,
        kind: if diagnostic { "error" } else { "test-report" },
        schema_hash: if diagnostic {
            error_report_schema()
        } else {
            test_report_schema()
        },
        metadata: b"{}",
        payload: PayloadSpec::Inline(if diagnostic {
            DIAGNOSTIC
        } else if value == VerdictValue::Fail {
            FAILED_PROOF
        } else {
            PROOF
        }),
        producer,
        receipt: None,
        result: None,
        work: None,
        inputs: &[],
        visibility: &["internal"],
    }
}
pub(super) fn descriptor(spec: ArtifactSpec<'_>) -> ArtifactDescriptor {
    ArtifactDescriptor::prepare(
        spec,
        ArtifactLimits {
            kind_bytes: 128,
            metadata_bytes: 1024,
            inline_bytes: 65536,
            inputs: 16,
            visibility_labels: 16,
            visibility_label_bytes: 128,
            construction_bytes: 128 * 1024,
        },
    )
    .unwrap()
    .build()
    .unwrap()
}
pub(super) struct Custody {
    path: tempfile::TempDir,
    store: ContentStore,
    budget: MemoryBudget,
}
impl Custody {
    pub(super) fn new() -> Self {
        let path = tempfile::tempdir().unwrap();
        let store = Self::open(path.path());
        Self {
            path,
            store,
            budget: MemoryBudget::new(32 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        }
    }
    fn open(path: &std::path::Path) -> ContentStore {
        ContentStore::open(
            path,
            StoreLimits {
                max_content_bytes: 2 * 1024 * 1024,
                max_staging_bytes: 4 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 17,
                max_manifest_bytes: 128 * 1024,
            },
        )
        .unwrap()
    }
    fn verify(
        &mut self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
    ) -> VerifiedNativeArtifact {
        self.store
            .verify_native_artifact(
                request,
                descriptor,
                ContentDomainId::from_u128(93),
                &self.budget,
                &BuiltinNativeSchemas,
            )
            .unwrap()
    }
}
fn report_input(
    id: u128,
    claim: Binding,
    key: EvaluationKey,
    state: &validation::EvaluationState,
    definition: &validation::Declaration,
    value: VerdictValue,
    artifact: ArtifactDescriptor,
) -> NativeInput {
    let attempt = state.bind(definition).unwrap().current_attempt().unwrap();
    let artifact = artifact
        .with_result_provenance(
            focal_model::lifecycle::artifact_descriptor::ResultProvenance {
                claim: key.claim,
                validation: key.validation,
                target: state.target(),
                generation: state.generation(),
                attempt,
                value,
            },
        )
        .unwrap();
    let evidence = ArtifactRef {
        id: ArtifactId(artifact.binding().object.0),
        hash: artifact.content_hash(),
    };
    NativeInput {
        request: request(attempt.evaluator, id),
        command: NativeCommand::ReportAdmission {
            claim,
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: key.generation,
                attempt,
                value,
                evidence,
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}
pub(super) fn report_for(
    core: &Core<NativeState>,
    tail: Option<&NativePrepared>,
    id: u128,
    index: u32,
    value: VerdictValue,
    artifact: ArtifactDescriptor,
) -> NativeInput {
    let claim = tail
        .map_or_else(
            || core.native_claim(key(index).claim),
            |tail| tail.claim(key(index).claim),
        )
        .unwrap();
    let state = tail
        .map_or_else(
            || core.native_evaluation(key(index)),
            |tail| tail.evaluation(key(index)),
        )
        .unwrap();
    let definition = tail
        .map_or_else(
            || core.native_definition(key(index).validation),
            |tail| tail.definition(key(index).validation),
        )
        .unwrap();
    report_input(
        id,
        claim.binding(),
        key(index),
        state,
        definition,
        value,
        artifact,
    )
}
pub(super) fn copy_report(input: &NativeInput) -> NativeInput {
    let NativeCommand::ReportAdmission {
        claim,
        key,
        expected,
        report,
        artifact,
    } = &input.command
    else {
        panic!("expected report")
    };
    NativeInput {
        request: input.request,
        command: NativeCommand::ReportAdmission {
            claim: *claim,
            key: *key,
            expected: *expected,
            report: *report,
            artifact: artifact.copy().unwrap(),
        },
    }
}
pub(super) fn verified(custody: &mut Custody, input: &NativeInput) -> VerifiedNativeArtifact {
    let NativeCommand::ReportAdmission { artifact, .. } = &input.command else {
        panic!("expected report")
    };
    custody.verify(input.request, artifact.get().unwrap())
}
pub(super) fn report(
    core: &Core<NativeState>,
    input: NativeInput,
    pending: &[&NativePrepared],
    custody: &VerifiedNativeArtifact,
) -> NativePrepared {
    prepared(core.prepare_native_evidenced(
        context(input.request.principal, 100),
        input,
        pending,
        Some(custody),
    ))
}
pub(super) fn events(core: &Core<NativeState>, outcome: NativeOutcome) -> Vec<NativeFact> {
    (0..outcome.events)
        .map(|ordinal| {
            let event = core.native_event(outcome.sequence, ordinal).unwrap();
            assert_eq!(
                (event.invocation, event.sequence, event.ordinal),
                (outcome.invocation, outcome.sequence, ordinal)
            );
            event.fact
        })
        .collect()
}

fn check_stored(
    core: &Core<NativeState>,
    outcome: NativeOutcome,
    result: validation::AcceptedResult,
) {
    let result_key = NativeResultKey::of(result);
    let stored = core.native_result(result_key).unwrap();
    assert_eq!(stored.result(), result);
    assert_eq!((stored.sequence(), stored.ordinal()), (outcome.sequence, 2));
    assert_eq!(
        (stored.attempt().phase, Some(stored.attempt().index)),
        (result.phase(), result.attempt())
    );
    assert_eq!(Some(stored.attempt().evaluator), result.reporter());
    assert_eq!(stored.artifact().result(), result);
    let reference = result.evidence().unwrap();
    assert_eq!(stored.artifact().reference(), reference);
    let artifact = core.native_artifact(reference.id).unwrap();
    assert_eq!(artifact.descriptor().content_hash(), reference.hash);
    assert_eq!(artifact.facts().unwrap().attempt, stored.attempt());
    assert_eq!(artifact.facts().unwrap().target, result.target());
    assert_eq!(artifact.facts().unwrap().value, result.verdict());
    assert_eq!(artifact.facts().unwrap().custody_revision, Some(1));
    assert_eq!(artifact.descriptor().receipt(), None);
    let facts = events(core, outcome);
    assert_eq!(
        facts[0],
        NativeFact::Artifact {
            binding: artifact.descriptor().binding()
        }
    );
    assert!(
        matches!(facts[1], NativeFact::Evaluation { kind: NativeEvaluationEventKind::Reported, attempt: Some(attempt), after, state, .. }
        if attempt == stored.attempt() && after == result.binding() && state == result.resulting_state())
    );
    assert_eq!(facts[2], NativeFact::Accepted { key: result_key });
    assert_eq!((outcome.artifacts, outcome.results), (1, 1));
    let changed_evaluations = facts
        .iter()
        .filter_map(|fact| match fact {
            NativeFact::Evaluation { key, .. } => Some(*key),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        usize::try_from(outcome.evaluations).unwrap(),
        changed_evaluations.len()
    );
}

#[test]
fn complete_pending_creation_begin_and_report_publish_atomically_with_real_custody() {
    let mut core = core();
    let mut custody = Custody::new();
    let created = prepared(core.prepare_native(
        context(ISSUER, 10),
        creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ));
    let posted =
        prepared(core.prepare_native(context(ISSUER, 20), post(2, binding(1)), &[&created]));
    let claim = posted.claim(key(1).claim).unwrap().binding();
    let begun = prepared(core.prepare_native(
        context(EVALUATOR, 30),
        begin(3, claim, 1, posted.evaluation(key(1)).unwrap().binding()),
        &[&created, &posted],
    ));
    let input = report_for(
        &core,
        Some(&begun),
        4,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(401, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let next = report(&core, input, &[&created, &posted, &begun], &token);
    let accepted = next.evaluation(key(1)).unwrap().last_result().unwrap();
    let result_key = NativeResultKey::of(accepted);
    assert_eq!(next.result(result_key).unwrap().result(), accepted);
    assert_eq!(
        next.artifact(ArtifactId::from_u128(401))
            .unwrap()
            .descriptor()
            .content_hash(),
        accepted.evidence().unwrap().hash
    );
    assert_eq!(
        next.evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    assert_eq!(
        next.claim(key(1).claim).unwrap().status(),
        ClaimStatus::Posted
    );
    assert_eq!(next.claim(key(1).claim).unwrap().response_count(), 0);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_evaluation(key(1)).is_none());
    assert!(core.native_claim(key(1).claim).is_none());
    let before = core.pin_native(0, 100).unwrap();
    let refused = core.publish_native(next).unwrap_err();
    assert_eq!(core.native_sequence(), SessionSeq(0));
    for candidate in [created, posted, begun] {
        core.publish_native(candidate).unwrap();
    }
    let outcome = core.publish_native(refused.prepared).unwrap();
    assert_eq!(outcome.sequence, SessionSeq(4));
    assert_eq!(
        core.native_evaluation(key(1)).unwrap().last_result(),
        Some(accepted)
    );
    check_stored(&core, outcome, accepted);
    assert_eq!(
        before
            .with_evaluation(key(1), 1, |row| row.state())
            .unwrap(),
        None
    );
    assert_eq!(
        before
            .with_artifact(ArtifactId::from_u128(401), 1, |row| row
                .descriptor()
                .binding())
            .unwrap(),
        None
    );
    assert_eq!(
        before
            .with_result(result_key, 1, |row| row.result())
            .unwrap(),
        None
    );
    assert_eq!(
        events(&core, outcome).len(),
        usize::try_from(outcome.events).unwrap()
    );
    core.release_native(&before).unwrap();
    let pointer = token.custody().payload();
    let reference = ContentRef {
        domain: pointer.domain,
        root: pointer.root,
        length: pointer.length,
        class: pointer.class,
    };
    drop(token);
    drop(custody.store);
    let reopened = Custody::open(custody.path.path());
    assert_eq!(reopened.read_bytes(&reference, PROOF.len()).unwrap(), PROOF);
}

#[test]
fn first_failure_and_original_evidence_survive_later_pending_sibling_report() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Required, false),
    ]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        31,
        2,
        VerdictValue::Incomplete,
        descriptor(artifact_spec(402, EVALUATOR, VerdictValue::Incomplete)),
    );
    let token = verified(&mut custody, &input);
    let first = report(&core, input, &[], &token);
    let original = first.claim(key(1).claim).unwrap().terminal_cut().unwrap();
    assert_eq!(first.outcome().evaluations, 2);
    assert!(first.evaluation(key(1)).unwrap().sealed().is_some());
    assert!(first.evaluation(key(1)).unwrap().last_result().is_none());
    let ClaimTerminalCut::Required(cut) = original else {
        panic!("expected required cause")
    };
    assert_eq!(cut.cause().kind(), aggregation::BlockingKind::Incomplete);
    assert_eq!(cut.sequence(), first.outcome().sequence);
    assert_eq!(
        first.claim(key(1).claim).unwrap().status(),
        ClaimStatus::PostFailed
    );
    let input = report_for(
        &core,
        Some(&first),
        32,
        1,
        VerdictValue::Fail,
        descriptor(artifact_spec(403, EVALUATOR, VerdictValue::Fail)),
    );
    let token = verified(&mut custody, &input);
    let second = report(&core, input, &[&first], &token);
    let result1 = first.evaluation(key(2)).unwrap().last_result().unwrap();
    let result2 = second.evaluation(key(1)).unwrap().last_result().unwrap();
    assert_eq!(
        second.claim(key(1).claim).unwrap().terminal_cut(),
        Some(original)
    );
    assert_eq!(
        second.claim(key(1).claim).unwrap().local_sealed_at(),
        Some(cut.sequence())
    );
    assert_eq!(
        second.evaluation(key(1)).unwrap().state(),
        validation::State::ValidationFailed
    );
    assert_eq!(
        second.evaluation(key(2)).unwrap().state(),
        validation::State::ValidationIncomplete
    );
    let first_outcome = core.publish_native(first).unwrap();
    let second_outcome = core.publish_native(second).unwrap();
    check_stored(&core, first_outcome, result1);
    check_stored(&core, second_outcome, result2);
    assert_eq!(
        events(&core, first_outcome)
            .iter()
            .filter(|fact| matches!(fact, NativeFact::Claim(_)))
            .count(),
        1
    );
    assert_eq!(
        events(&core, second_outcome)
            .iter()
            .filter(|fact| matches!(fact, NativeFact::Claim(_)))
            .count(),
        0
    );
    assert_eq!(
        core.native_claim(key(1).claim).unwrap().terminal_cut(),
        Some(original)
    );
}

#[test]
fn observe_failure_is_retained_without_blocking_required_pass_or_creating_testimony() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    let mut custody = Custody::new();
    for (request, index, value) in [(41, 2, VerdictValue::Fail), (42, 1, VerdictValue::Pass)] {
        let input = report_for(
            &core,
            None,
            request,
            index,
            value,
            descriptor(artifact_spec(request + 400, EVALUATOR, value)),
        );
        let token = verified(&mut custody, &input);
        let next = report(&core, input, &[], &token);
        let result = next.evaluation(key(index)).unwrap().last_result().unwrap();
        let outcome = core.publish_native(next).unwrap();
        check_stored(&core, outcome, result);
    }
    assert_eq!(
        core.native_evaluation(key(2)).unwrap().state(),
        validation::State::ValidationFailedNotRequired
    );
    assert_eq!(
        core.native_evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    let claim = core.native_claim(key(1).claim).unwrap();
    assert_eq!(claim.status(), ClaimStatus::Posted);
    assert_eq!(claim.terminal_cut(), None);
    assert_eq!(claim.receipt(), None);
    assert_eq!(claim.response_count(), 0);
}

#[test]
fn exact_pending_and_committed_retries_do_not_need_a_second_custody_token() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        51,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(451, EVALUATOR, VerdictValue::Pass)),
    );
    let pending_retry = copy_report(&input);
    let committed_retry = copy_report(&input);
    let mut conflict = copy_report(&input);
    let NativeCommand::ReportAdmission {
        report: changed, ..
    } = &mut conflict.command
    else {
        panic!()
    };
    changed.value = VerdictValue::Fail;
    let token = verified(&mut custody, &input);
    let next = report(&core, input, &[], &token);
    let expected = next.outcome();
    drop(token);
    assert!(
        matches!(core.prepare_native(context(EVALUATOR, 1), pending_retry, &[&next]).unwrap(), NativePreparation::Existing { outcome, committed: false } if outcome == expected)
    );
    assert!(matches!(
        core.prepare_native(context(EVALUATOR, 100), conflict, &[&next]),
        Err(NativeError::RequestConflict)
    ));
    core.publish_native(next).unwrap();
    let before = core.native_budget();
    assert!(
        matches!(core.prepare_native(context(EVALUATOR, 9999), committed_retry, &[]).unwrap(), NativePreparation::Existing { outcome, committed: true } if outcome == expected)
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), expected.sequence);
}

#[test]
fn error_retry_and_quality_followup_keep_each_real_attempt_and_no_early_acceptance() {
    let mut core = running(&[(ValidationMode::Required, true)]);
    let mut custody = Custody::new();
    let mut accepted = Vec::new();
    for (id, actor, value, phase, index, resulting) in [
        (
            61,
            EVALUATOR,
            VerdictValue::Error,
            validation::Phase::Programmatic,
            0,
            validation::State::Validating,
        ),
        (
            62,
            EVALUATOR,
            VerdictValue::Pass,
            validation::Phase::Programmatic,
            1,
            validation::State::ValidatingQualityBar,
        ),
        (
            63,
            QUALITY,
            VerdictValue::Pass,
            validation::Phase::Quality,
            2,
            validation::State::Validated,
        ),
    ] {
        let input = report_for(
            &core,
            None,
            id,
            1,
            value,
            descriptor(artifact_spec(id + 400, actor, value)),
        );
        let token = verified(&mut custody, &input);
        let next = report(&core, input, &[], &token);
        let state = next.evaluation(key(1)).unwrap();
        assert_eq!(state.state(), resulting);
        let result = state.last_result().unwrap();
        assert_eq!(
            (result.phase(), result.attempt(), result.reporter()),
            (phase, Some(index), Some(actor))
        );
        assert_eq!(
            result.is_terminal(),
            resulting == validation::State::Validated
        );
        accepted.push(result);
        let outcome = core.publish_native(next).unwrap();
        check_stored(&core, outcome, result);
        assert!(
            !events(&core, outcome)
                .iter()
                .any(|fact| matches!(fact, NativeFact::Claim(_)))
        );
    }
    assert_eq!(accepted.len(), 3);
    for result in accepted {
        assert_eq!(
            core.native_result(NativeResultKey::of(result))
                .unwrap()
                .result(),
            result
        );
    }
    assert_eq!(
        core.native_claim(key(1).claim).unwrap().status(),
        ClaimStatus::Posted
    );
    assert_eq!(core.native_claim(key(1).claim).unwrap().response_count(), 0);
}

#[test]
fn only_final_error_exhaustion_fails_admission_at_its_actual_publication() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let first_sequence = core.native_sequence().0 + 1;
    for id in [71, 72] {
        let input = report_for(
            &core,
            None,
            id,
            1,
            VerdictValue::Error,
            descriptor(artifact_spec(id + 400, EVALUATOR, VerdictValue::Error)),
        );
        let token = verified(&mut custody, &input);
        let next = report(&core, input, &[], &token);
        core.publish_native(next).unwrap();
        let claim = core.native_claim(key(1).claim).unwrap();
        if id == 71 {
            assert_eq!(claim.terminal_cut(), None);
        }
    }
    let first = core.native_artifact(ArtifactId::from_u128(471)).unwrap();
    let second = core.native_artifact(ArtifactId::from_u128(472)).unwrap();
    assert_eq!(first.descriptor().payload(), second.descriptor().payload());
    assert_eq!(first.descriptor().metadata(), b"{}");
    assert_eq!(
        first.descriptor().metadata(),
        second.descriptor().metadata()
    );
    assert_eq!(first.custody().payload(), second.custody().payload());
    assert_ne!(
        first.descriptor().content_hash(),
        second.descriptor().content_hash()
    );
    assert_eq!(first.facts().unwrap().attempt.index, 0);
    assert_eq!(second.facts().unwrap().attempt.index, 1);
    let claim = core.native_claim(key(1).claim).unwrap();
    let Some(ClaimTerminalCut::Required(cut)) = claim.terminal_cut() else {
        panic!()
    };
    assert_eq!(claim.status(), ClaimStatus::PostFailed);
    assert_eq!(cut.sequence(), SessionSeq(first_sequence + 1));
    assert_eq!(cut.cause().key().attempt, Some(1));
    assert_eq!(cut.cause().kind(), aggregation::BlockingKind::Errored);
}

#[test]
fn custody_is_required_for_new_requests_and_cannot_be_rebound_to_other_inputs() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        81,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(481, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let old = *core.native_evaluation(key(1)).unwrap();
    let budget = core.native_budget();
    assert!(matches!(
        core.prepare_native(context(EVALUATOR, 100), copy_report(&input), &[]),
        Err(NativeError::Contract(ContractError::MissingEvidence))
    ));
    for case in 0..3 {
        let mut bad = copy_report(&input);
        match case {
            0 => bad.request.id = RequestId::from_u128(99),
            1 => bad.request.epoch = RequestEpoch(2),
            _ => {
                let NativeCommand::ReportAdmission {
                    report, artifact, ..
                } = &mut bad.command
                else {
                    panic!()
                };
                *artifact = NativeArtifactInput::new(descriptor(artifact_spec(
                    482,
                    EVALUATOR,
                    VerdictValue::Pass,
                )))
                .unwrap();
                let changed = artifact.get().unwrap();
                report.evidence = ArtifactRef {
                    id: ArtifactId(changed.binding().object.0),
                    hash: changed.content_hash(),
                };
            }
        }
        assert!(
            core.prepare_native_evidenced(context(EVALUATOR, 100), bad, &[], Some(&token))
                .is_err(),
            "case {case}"
        );
        assert_eq!(core.native_evaluation(key(1)), Some(&old));
        assert_eq!(core.native_budget(), budget);
        assert!(core.native_artifact(ArtifactId::from_u128(481)).is_none());
        assert!(core.native_outcome(request(EVALUATOR, 81)).is_none());
    }
    let next = report(&core, input, &[], &token);
    core.publish_native(next).unwrap();
}

#[test]
fn actor_attempt_revision_target_and_deadline_must_match_actual_owner_rows() {
    let core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        91,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(491, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let old = *core.native_evaluation(key(1)).unwrap();
    let prefix = core.native_sequence();
    let budget = core.native_budget();
    for case in 0..11 {
        let mut bad = copy_report(&input);
        let mut owner = context(EVALUATOR, 100);
        let NativeCommand::ReportAdmission {
            claim,
            key: target,
            expected,
            report,
            ..
        } = &mut bad.command
        else {
            panic!()
        };
        match case {
            0 => owner.principal = Principal::Actor(ISSUER),
            1 => owner.principal = Principal::Node(EVALUATOR),
            2 => claim.revision = ObjectRevision(1),
            3 => expected.revision = ObjectRevision(1),
            4 => target.generation = 2,
            5 => report.generation = 2,
            6 => report.attempt.index += 1,
            7 => report.attempt.version = ContentHash([99; 32]),
            8 => report.attempt.evaluator = SUBJECT,
            9 => report.evidence.hash = ContentHash([99; 32]),
            _ => owner.logical_time = 1000,
        }
        assert!(
            core.prepare_native_evidenced(owner, bad, &[], Some(&token))
                .is_err(),
            "case {case}"
        );
        assert_eq!(core.native_sequence(), prefix);
        assert_eq!(core.native_evaluation(key(1)), Some(&old));
        assert_eq!(core.native_budget(), budget);
        assert!(core.native_artifact(ArtifactId::from_u128(491)).is_none());
        assert!(core.native_outcome(request(EVALUATOR, 91)).is_none());
    }
}

#[test]
fn verified_schema_is_not_a_verdict_and_evidence_kind_and_declaration_schema_are_checked() {
    let core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    for case in 0..3 {
        let (value, mut spec) = match case {
            0 => (
                VerdictValue::Pass,
                artifact_spec(501, EVALUATOR, VerdictValue::Error),
            ),
            1 => (
                VerdictValue::Error,
                artifact_spec(502, EVALUATOR, VerdictValue::Pass),
            ),
            _ => (
                VerdictValue::Error,
                artifact_spec(503, EVALUATOR, VerdictValue::Error),
            ),
        };
        if case == 2 {
            spec.kind = "test-report";
        }
        let input = report_for(
            &core,
            None,
            101 + u128::try_from(case).unwrap(),
            1,
            value,
            descriptor(spec),
        );
        let token = verified(&mut custody, &input);
        let before = core.native_budget();
        assert!(
            core.prepare_native_evidenced(context(EVALUATOR, 100), input, &[], Some(&token))
                .is_err(),
            "case {case}"
        );
        assert_eq!(core.native_budget(), before);
        assert_eq!(
            core.native_evaluation(key(1)).unwrap().state(),
            validation::State::Validating
        );
        assert_eq!(
            core.native_claim(key(1).claim).unwrap().status(),
            ClaimStatus::Posted
        );
    }
}

#[test]
fn pending_control_fences_reports_while_amends_preserves_live_authority() {
    for correction in [
        None,
        Some(CorrectionKind::Supersedes),
        Some(CorrectionKind::Amends),
    ] {
        let mut core = running(&[(ValidationMode::Required, false)]);
        let mut custody = Custody::new();
        let claim = core.native_claim(key(1).claim).unwrap().binding();
        let input = match correction {
            None => NativeInput {
                request: request(ISSUER, 111),
                command: NativeCommand::Cancel { expected: claim },
            },
            Some(kind) => creation(111, 2, &[], Some(kind)),
        };
        let control = prepared(core.prepare_native(context(ISSUER, 90), input, &[]));
        let input = report_for(
            &core,
            Some(&control),
            112,
            1,
            VerdictValue::Pass,
            descriptor(artifact_spec(511, EVALUATOR, VerdictValue::Pass)),
        );
        let token = verified(&mut custody, &input);
        let before = core.native_budget();
        let result = core.prepare_native_evidenced(
            context(EVALUATOR, 100),
            input,
            &[&control],
            Some(&token),
        );
        if correction == Some(CorrectionKind::Amends) {
            let next = prepared(result);
            assert_eq!(
                next.evaluation(key(1)).unwrap().state(),
                validation::State::Validated
            );
            core.publish_native(control).unwrap();
            core.publish_native(next).unwrap();
        } else {
            assert!(
                matches!(
                    result,
                    Err(NativeError::Contract(ContractError::StaleEvaluation))
                ),
                "{result:?}"
            );
            assert_eq!(core.native_budget(), before);
            assert!(core.native_artifact(ArtifactId::from_u128(511)).is_none());
            core.publish_native(control).unwrap();
            assert!(core.native_evaluation(key(1)).unwrap().fence().is_some());
            assert!(
                core.native_evaluation(key(1))
                    .unwrap()
                    .last_result()
                    .is_none()
            );
        }
    }
}

#[test]
fn artifact_inputs_and_inherited_visibility_resolve_the_effective_pending_owner() {
    let mut core = running(&[(ValidationMode::Required, true)]);
    let mut custody = Custody::new();
    let first_input = report_for(
        &core,
        None,
        121,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(521, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &first_input);
    let first = report(&core, first_input, &[], &token);
    let source = [ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Artifact,
        id: ObjectId::from_u128(521),
    }];
    for case in 0..3 {
        let unknown = [ObjectRef {
            ledger: ledger(),
            kind: ObjectKind::Artifact,
            id: ObjectId::from_u128(999),
        }];
        let wrong_family = [ObjectRef {
            ledger: ledger(),
            kind: ObjectKind::Testament,
            id: ObjectId::from_u128(999),
        }];
        let mut spec = artifact_spec(522, QUALITY, VerdictValue::Pass);
        spec.inputs = match case {
            0 => &unknown,
            1 => &wrong_family,
            _ => &source,
        };
        if case == 2 {
            spec.visibility = &[];
        }
        let input = report_for(
            &core,
            Some(&first),
            122,
            1,
            VerdictValue::Pass,
            descriptor(spec),
        );
        let token = verified(&mut custody, &input);
        let before = core.native_budget();
        assert!(
            core.prepare_native_evidenced(context(QUALITY, 100), input, &[&first], Some(&token))
                .is_err(),
            "case {case}"
        );
        assert_eq!(core.native_budget(), before);
        assert_eq!(
            first.evaluation(key(1)).unwrap().state(),
            validation::State::ValidatingQualityBar
        );
        assert!(first.artifact(ArtifactId::from_u128(522)).is_none());
    }
    let mut spec = artifact_spec(522, QUALITY, VerdictValue::Pass);
    spec.inputs = &source;
    let input = report_for(
        &core,
        Some(&first),
        122,
        1,
        VerdictValue::Pass,
        descriptor(spec),
    );
    let token = verified(&mut custody, &input);
    let second = report(&core, input, &[&first], &token);
    core.publish_native(first).unwrap();
    let outcome = core.publish_native(second).unwrap();
    check_stored(
        &core,
        outcome,
        core.native_evaluation(key(1))
            .unwrap()
            .last_result()
            .unwrap(),
    );
    assert_eq!(
        core.native_artifact(ArtifactId::from_u128(522))
            .unwrap()
            .descriptor()
            .inputs(),
        &source
    );
}

#[test]
fn original_result_is_immutable_and_artifact_address_collision_never_overwrites_evidence() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    let mut custody = Custody::new();
    let input = report_for(
        &core,
        None,
        131,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(531, EVALUATOR, VerdictValue::Pass)),
    );
    let mut replacement = copy_report(&input);
    let token = verified(&mut custody, &input);
    let next = report(&core, input, &[], &token);
    let original = next.evaluation(key(1)).unwrap().last_result().unwrap();
    let outcome = core.publish_native(next).unwrap();
    replacement.request.id = RequestId::from_u128(132);
    let NativeCommand::ReportAdmission {
        expected,
        report,
        artifact,
        ..
    } = &mut replacement.command
    else {
        panic!()
    };
    *expected = original.binding();
    report.value = VerdictValue::Fail;
    *artifact = NativeArtifactInput::new(descriptor(artifact_spec(
        532,
        EVALUATOR,
        VerdictValue::Fail,
    )))
    .unwrap();
    let descriptor = artifact.get().unwrap();
    report.evidence = ArtifactRef {
        id: ArtifactId(descriptor.binding().object.0),
        hash: descriptor.content_hash(),
    };
    let token = verified(&mut custody, &replacement);
    let before = core.native_budget();
    assert!(
        core.prepare_native_evidenced(context(EVALUATOR, 100), replacement, &[], Some(&token))
            .is_err()
    );
    assert_eq!(core.native_budget(), before);
    let input = report_for(
        &core,
        None,
        133,
        2,
        VerdictValue::Fail,
        self::descriptor(artifact_spec(531, EVALUATOR, VerdictValue::Fail)),
    );
    let token = verified(&mut custody, &input);
    assert!(
        core.prepare_native_evidenced(context(EVALUATOR, 100), input, &[], Some(&token))
            .is_err()
    );
    assert_eq!(core.native_budget(), before);
    assert_eq!(
        core.native_evaluation(key(2)).unwrap().state(),
        validation::State::Validating
    );
    check_stored(&core, outcome, original);
    assert!(core.native_artifact(ArtifactId::from_u128(532)).is_none());
}

#[test]
fn reports_use_completion_reserve_publish_without_capacity_and_rollback_failed_neighbor_copy() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let budget = core.state.budget.clone();
    let input = report_for(
        &core,
        None,
        141,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(541, EVALUATOR, VerdictValue::Pass)),
    );
    let token = verified(&mut custody, &input);
    let before = budget.stats();
    let failed = super::prepare::fail_copies_after(0, || {
        core.prepare_native_evidenced(
            context(EVALUATOR, 100),
            copy_report(&input),
            &[],
            Some(&token),
        )
    });
    assert!(
        matches!(
            failed,
            Err(NativeError::Memory(MemoryError::AllocationFailed))
        ),
        "{failed:?}"
    );
    assert_eq!(budget.stats(), before);
    assert!(core.native_artifact(ArtifactId::from_u128(541)).is_none());
    assert!(core.native_outcome(request(EVALUATOR, 141)).is_none());
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            before.limit - before.completion_reserve - before.ordinary_used,
        )
        .unwrap();
    let next = report(&core, input, &[], &token);
    let result = next.evaluation(key(1)).unwrap().last_result().unwrap();
    let now = budget.stats();
    let rest = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            now.limit - now.used,
        )
        .unwrap();
    let outcome = core.publish_native(next).unwrap();
    check_stored(&core, outcome, result);
    drop(rest);
    drop(pressure);
}

#[test]
fn artifact_and_result_capacity_refuse_atomic_report_without_consuming_request_or_clock() {
    for cap_artifacts in [true, false] {
        let mut limits = native_limits();
        if cap_artifacts {
            limits.artifacts = 1;
        } else {
            limits.results = 1;
        }
        let mut core = Core::new_native(
            ledger(),
            RangeId(81),
            limits,
            MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        initialize(
            &mut core,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, false),
            ],
        );
        let mut custody = Custody::new();
        let input = report_for(
            &core,
            None,
            151,
            2,
            VerdictValue::Pass,
            descriptor(artifact_spec(551, EVALUATOR, VerdictValue::Pass)),
        );
        let token = verified(&mut custody, &input);
        let next = report(&core, input, &[], &token);
        let outcome = core.publish_native(next).unwrap();
        let input = report_for(
            &core,
            None,
            152,
            1,
            VerdictValue::Pass,
            descriptor(artifact_spec(552, EVALUATOR, VerdictValue::Pass)),
        );
        let token = verified(&mut custody, &input);
        let before = core.native_budget();
        assert!(matches!(
            core.prepare_native_evidenced(context(EVALUATOR, 999), input, &[], Some(&token)),
            Err(NativeError::Capacity(_))
        ));
        assert_eq!(core.native_budget(), before);
        assert_eq!(core.native_sequence(), outcome.sequence);
        assert!(core.native_artifact(ArtifactId::from_u128(552)).is_none());
        assert!(core.native_outcome(request(EVALUATOR, 152)).is_none());
        assert_eq!(
            core.native_evaluation(key(1)).unwrap().state(),
            validation::State::Validating
        );
        // The rejected input did not advance trusted time or reserve its request.
        let claim = core.native_claim(key(1).claim).unwrap().binding();
        publish(
            &mut core,
            101,
            NativeInput {
                request: request(ISSUER, 152),
                command: NativeCommand::Cancel { expected: claim },
            },
        );
        assert_eq!(
            core.native_claim(key(1).claim).unwrap().status(),
            ClaimStatus::Cancelled
        );
    }
}

#[test]
fn content_pointer_report_verifies_actual_reopened_payload_before_owner_admission() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let source = descriptor(artifact_spec(561, EVALUATOR, VerdictValue::Pass));
    let token = custody.verify(request(EVALUATOR, 160), &source);
    let pointer = token.custody().payload();
    drop(token);
    drop(custody.store);
    custody.store = Custody::open(custody.path.path());
    let mut spec = artifact_spec(562, EVALUATOR, VerdictValue::Pass);
    spec.payload = PayloadSpec::Content(pointer);
    let input = report_for(&core, None, 161, 1, VerdictValue::Pass, descriptor(spec));
    let token = verified(&mut custody, &input);
    assert_eq!(token.custody().payload(), pointer);
    let next = report(&core, input, &[], &token);
    let result = next.evaluation(key(1)).unwrap().last_result().unwrap();
    let outcome = core.publish_native(next).unwrap();
    check_stored(&core, outcome, result);
    assert_eq!(
        core.native_artifact(ArtifactId::from_u128(562))
            .unwrap()
            .descriptor()
            .payload(),
        PayloadSpec::Content(pointer)
    );
}

#[test]
fn actual_custody_cannot_authorize_missing_or_substituted_result_provenance() {
    let core = running(&[(ValidationMode::Required, false)]);
    let mut custody = Custody::new();
    let original = report_for(
        &core,
        None,
        161,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(561, EVALUATOR, VerdictValue::Pass)),
    );
    let NativeCommand::ReportAdmission { artifact, .. } = &original.command else {
        panic!()
    };
    let provenance = artifact.get().unwrap().result_provenance().unwrap();
    let prefix = core.native_sequence();
    for case in 0..5 {
        let mut bad = copy_report(&original);
        let NativeCommand::ReportAdmission {
            artifact, report, ..
        } = &mut bad.command
        else {
            panic!()
        };
        let plain = descriptor(artifact_spec(561, EVALUATOR, VerdictValue::Pass));
        let changed = if case == 0 {
            plain
        } else {
            let mut changed = provenance;
            match case {
                1 => changed.attempt.index += 1,
                2 => changed.attempt.definition = ContentHash([44; 32]),
                3 => changed.generation += 1,
                _ => changed.value = VerdictValue::Fail,
            }
            plain.with_result_provenance(changed).unwrap()
        };
        report.evidence = ArtifactRef {
            id: changed.id(),
            hash: changed.content_hash(),
        };
        *artifact = NativeArtifactInput::new(changed).unwrap();
        let token = verified(&mut custody, &bad);
        let before = core.native_budget();
        assert!(matches!(
            core.prepare_native_evidenced(context(EVALUATOR, 100), bad, &[], Some(&token)),
            Err(NativeError::Contract(ContractError::MissingEvidence))
        ));
        assert_eq!(core.native_budget(), before);
        assert_eq!(core.native_sequence(), prefix);
        assert!(core.native_outcome(original.request).is_none());
        assert!(core.native_artifact(ArtifactId::from_u128(561)).is_none());
    }
}
