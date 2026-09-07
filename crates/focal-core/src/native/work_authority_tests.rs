use super::*;
use crate::native::report_tests::{self as f, EVALUATOR, ISSUER, SUBJECT, binding};
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::{BudgetLane, Change, Entry};
use focal_model::lifecycle::{
    aggregation::{self, PublicationPosition},
    artifact_descriptor::{ResultProvenance, WorkProvenance, WorkRole},
};
use focal_model::{
    ArtifactRef, Confidence, ContentDomainId, Deadline, HandlerRef, OutcomeKind, TimerId,
    ValidationKind, ValidationMode, ValidationPhase, ValidatorId, VerdictValue,
};

fn aggregation_limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 2,
        max_checks: 8,
        max_results: 32,
        max_updates: 16,
    }
}
fn definition(index: u32, mode: ValidationMode) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(70),
        version: ContentHash([70; 32]),
        agentic: false,
    };
    let steps = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: focal_evidence::test_report_schema(),
        diagnostic_schema: focal_evidence::error_report_schema(),
    }];
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(300 + u128::from(index)),
            claim: ClaimId::from_u128(1),
            issuer: ISSUER,
            declaration_index: index,
            kind: ValidationKind::Test,
            phase: ValidationPhase::WholeWork,
            mode,
            target: validation::TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "output",
            },
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: EVALUATOR,
                    definition: ContentHash([71; 32]),
                    handlers: &steps,
                    required_policy: None,
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(300 + u128::from(index)),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 2,
            attempts: 4,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

struct Fixture {
    core: Core<NativeState>,
    serial: u128,
}
impl Fixture {
    fn new(present: bool) -> Self {
        let mut core = Core::new_native(
            binding(1).ledger,
            RangeId(3901),
            NativeLimits {
                range: RangeConfig {
                    page_entries: 1,
                    max_batch_entries: 128,
                    ..RangeConfig::default()
                },
                plan_nodes: 16,
                plan_edges: 2048,
                preparation_bytes: 1024 * 1024,
                evaluations_per_claim: 32,
                ..NativeLimits::default()
            },
            MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let mut input = f::creation(1, 1, &[], None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut input.command
        else {
            panic!("create")
        };
        declarations.extend([
            definition(1, ValidationMode::Required),
            definition(2, ValidationMode::Observe),
        ]);
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(1),
            ISSUER,
            &[aggregation::SlotPolicy {
                slot: 0,
                missing_declaration_index: 20,
                mode: ValidationMode::Required,
                checks: &[
                    aggregation::CheckPolicy {
                        declaration_index: 1,
                        validation: ValidationId::from_u128(301),
                        mode: ValidationMode::Required,
                    },
                    aggregation::CheckPolicy {
                        declaration_index: 2,
                        validation: ValidationId::from_u128(302),
                        mode: ValidationMode::Observe,
                    },
                ],
            }],
            declarations,
            aggregation_limits(),
        )
        .unwrap();
        f::publish(&mut core, 1, input);
        f::publish(&mut core, 2, f::post(2, binding(1)));
        let mut fixture = Self { core, serial: 10 };
        fixture.send(
            SUBJECT,
            NativeCommand::AcquireReceipt {
                expected: fixture.claim(),
                receipt: ReceiptId::from_u128(701),
            },
        );
        let mut manifest = Vec::new();
        if present {
            let parent = evidence::Parent::from_claim(
                fixture.core.native_claim(ClaimId::from_u128(1)).unwrap(),
            )
            .unwrap();
            let mut spec = f::artifact_spec(800, SUBJECT, VerdictValue::Pass);
            spec.receipt = Some(parent.receipt);
            spec.work = Some(WorkProvenance {
                claim: parent.claim,
                cycle: parent.next_cycle,
                role: WorkRole::Output { slot: 0 },
            });
            spec.visibility = &["internal", "team:qa"];
            let descriptor = f::descriptor(spec);
            let reference = ArtifactRef {
                id: descriptor.id(),
                hash: descriptor.content_hash(),
            };
            let directory = tempfile::tempdir().unwrap();
            let mut store = ContentStore::open(
                directory.path(),
                StoreLimits {
                    max_content_bytes: 1024 * 1024,
                    max_staging_bytes: 2 * 1024 * 1024,
                    max_uploads: 8,
                    chunk_bytes: 17,
                    max_manifest_bytes: 65536,
                },
            )
            .unwrap();
            fixture.serial += 1;
            let request = f::request(SUBJECT, fixture.serial);
            let custody = store
                .verify_native_artifact(
                    request,
                    &descriptor,
                    ContentDomainId::from_u128(93),
                    &fixture.core.state.budget,
                    &BuiltinNativeSchemas,
                )
                .unwrap();
            let input = NativeInput {
                request,
                command: NativeCommand::SubmitWork {
                    claim: fixture.claim(),
                    slot: 0,
                    artifact: NativeArtifactInput::new(descriptor).unwrap(),
                },
            };
            let prepared = f::prepared(fixture.core.prepare_native_evidenced(
                f::context(SUBJECT, fixture.serial as u64),
                input,
                &[],
                Some(&custody),
            ));
            fixture.core.publish_native(prepared).unwrap();
            manifest.push(evidence::SlotBinding {
                slot: 0,
                artifact: reference,
            });
        }
        fixture.send(
            SUBJECT,
            NativeCommand::CloseResponse {
                claim: fixture.claim(),
                response: binding(900),
                report: NativeResponseInput {
                    summary: "Respondent-authored response for authority checks.".into(),
                    confidence: Confidence::Committed,
                    outcome: OutcomeKind::Complete,
                    manifest,
                    diagnostics: vec![],
                },
            },
        );
        fixture.send(
            SUBJECT,
            NativeCommand::PostResponse {
                claim: fixture.claim(),
                expected: fixture.response().identity().binding,
            },
        );
        fixture.send(
            ISSUER,
            NativeCommand::ReceiveResponse {
                claim: fixture.claim(),
                expected: fixture.response().identity().binding,
            },
        );
        fixture
    }
    fn send(&mut self, actor: ParticipantId, command: NativeCommand) {
        self.serial += 1;
        f::publish(
            &mut self.core,
            self.serial as u64,
            NativeInput {
                request: f::request(actor, self.serial),
                command,
            },
        );
    }
    fn claim(&self) -> Binding {
        self.core
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .binding()
    }
    fn response(&self) -> &Response {
        self.core
            .native_response(TestamentId::from_u128(900))
            .unwrap()
    }
    fn view(&self) -> View<'_> {
        View {
            state: &self.core.state,
            tail: None,
        }
    }
    fn key(&self, index: u32) -> EvaluationKey {
        let target = if self.response().manifest().is_empty() {
            EvaluationTarget::MissingSlot {
                response: TestamentId::from_u128(900),
                slot: 0,
            }
        } else {
            EvaluationTarget::Work {
                response: TestamentId::from_u128(900),
                slot: 0,
                artifact: ArtifactId::from_u128(800),
            }
        };
        EvaluationKey {
            claim: ClaimId::from_u128(1),
            validation: ValidationId::from_u128(300 + u128::from(index)),
            target,
            generation: 1,
        }
    }
    /// Publish only checked model prerequisites in this private helper fixture.
    /// No external WholeWork command, outcome, or durable encoding is introduced.
    fn replace(&mut self, key: Key, row: Row, heap: usize) {
        let next = self
            .core
            .state
            .rows
            .prepare_batch_with(
                self.core.state.rows.prefix() + 1,
                vec![Change::Put(Entry::new(key, row, heap))],
                BudgetLane::Completion,
                |_| panic!("one-entry leaves require no neighbor copier"),
            )
            .unwrap();
        self.core.state.rows.publish(next).unwrap();
    }
    fn enter(&mut self) {
        let view = self.view();
        let claim = view.claim(ClaimId::from_u128(1)).unwrap();
        let decision = aggregation::ClaimAggregation::new(claim, aggregation_limits()).unwrap();
        let Some(Row::Response(source)) = view.get(Key::Response(TestamentId::from_u128(900)))
        else {
            panic!("response")
        };
        let transition = source
            .get()
            .unwrap()
            .plan_begin(
                &source.get().unwrap().identity().binding,
                claim,
                Principal::Actor(ISSUER),
                &decision.decision(),
            )
            .unwrap();
        let next = source
            .transition(
                transition,
                PublicationPosition {
                    sequence: SessionSeq(view.prefix().0 + 1),
                    ordinal: 0,
                },
            )
            .unwrap();
        let heap = next.heap_charge().unwrap();
        self.replace(
            Key::Response(TestamentId::from_u128(900)),
            Row::Response(next),
            heap,
        );
    }
    fn begin(&mut self, index: u32) {
        let view = self.view();
        let key = self.key(index);
        let expected = view.evaluation(key).unwrap().binding();
        let aggregate = aggregation::ClaimAggregation::new(
            view.claim(key.claim).unwrap(),
            aggregation_limits(),
        )
        .unwrap();
        let begun = super::begin(
            &view,
            f::context(EVALUATOR, 100),
            self.claim(),
            key,
            expected,
            self.core.limits,
            &aggregate.decision(),
        )
        .unwrap();
        let state = OwnedEvaluation::new(begun.next).unwrap();
        let heap = state.heap_charge().unwrap();
        self.replace(Key::Evaluation(key), Row::Evaluation(state), heap);
    }
    fn report(
        &self,
        index: u32,
        value: VerdictValue,
        labels: &[&str],
    ) -> (validation::Report, ArtifactDescriptor) {
        let key = self.key(index);
        let state = self.core.native_evaluation(key).unwrap();
        let definition = self.core.native_definition(key.validation).unwrap();
        let attempt = state.bind(definition).unwrap().current_attempt().unwrap();
        let mut spec = f::artifact_spec(1000 + u128::from(index), EVALUATOR, value);
        spec.receipt = state.receipt();
        spec.visibility = labels;
        spec.result = Some(ResultProvenance {
            claim: key.claim,
            validation: key.validation,
            target: state.target(),
            generation: key.generation,
            attempt,
            value,
        });
        let descriptor = f::descriptor(spec);
        (
            validation::Report {
                generation: key.generation,
                attempt,
                value,
                evidence: ArtifactRef {
                    id: descriptor.id(),
                    hash: descriptor.content_hash(),
                },
            },
            descriptor,
        )
    }
}

#[test]
fn external_work_begin_requires_prior_checked_response_entry_and_exact_designation() {
    let mut fixture = Fixture::new(true);
    {
        let view = fixture.view();
        let key = fixture.key(1);
        let aggregate = aggregation::ClaimAggregation::new(
            view.claim(key.claim).unwrap(),
            aggregation_limits(),
        )
        .unwrap();
        assert!(
            super::begin(
                &view,
                f::context(EVALUATOR, 100),
                fixture.claim(),
                key,
                view.evaluation(key).unwrap().binding(),
                fixture.core.limits,
                &aggregate.decision()
            )
            .is_err()
        );
    }
    fixture.enter();
    let view = fixture.view();
    let key = fixture.key(1);
    let expected = view.evaluation(key).unwrap().binding();
    let aggregate =
        aggregation::ClaimAggregation::new(view.claim(key.claim).unwrap(), aggregation_limits())
            .unwrap();
    let before = fixture.core.state.budget.stats();
    for principal in [
        Principal::Actor(ISSUER),
        Principal::Actor(SUBJECT),
        Principal::Node(EVALUATOR),
    ] {
        let result = super::begin(
            &view,
            NativeContext {
                principal,
                logical_time: 100,
            },
            fixture.claim(),
            key,
            expected,
            fixture.core.limits,
            &aggregate.decision(),
        );
        assert!(matches!(
            result,
            Err(NativeError::Contract(ContractError::WrongActor))
        ));
    }
    let begun = super::begin(
        &view,
        f::context(EVALUATOR, 100),
        fixture.claim(),
        key,
        expected,
        fixture.core.limits,
        &aggregate.decision(),
    )
    .unwrap();
    assert_eq!(begun.next.state(), validation::State::Validating);
    assert!(begun.next.has_begun());
    assert_eq!(begun.next.target(), view.evaluation(key).unwrap().target());
    assert_eq!(
        view.evaluation(key).unwrap().state(),
        validation::State::Ready
    );
    assert_eq!(fixture.core.state.budget.stats(), before);
}

#[test]
fn missing_slots_and_other_families_never_authorize_external_attempts() {
    let mut fixture = Fixture::new(false);
    fixture.enter();
    let view = fixture.view();
    let key = fixture.key(1);
    let expected = view.evaluation(key).unwrap().binding();
    let aggregate =
        aggregation::ClaimAggregation::new(view.claim(key.claim).unwrap(), aggregation_limits())
            .unwrap();
    for target in [
        key.target,
        EvaluationTarget::Admission,
        EvaluationTarget::Increment {
            artifact: ArtifactId::from_u128(800),
        },
        EvaluationTarget::Delivery {
            response: TestamentId::from_u128(900),
        },
    ] {
        assert!(matches!(
            super::begin(
                &view,
                f::context(EVALUATOR, 100),
                fixture.claim(),
                EvaluationKey { target, ..key },
                expected,
                fixture.core.limits,
                &aggregate.decision()
            ),
            Err(NativeError::Contract(ContractError::InvalidTarget))
        ));
    }
    assert_eq!(
        view.evaluation(key).unwrap().state(),
        validation::State::Ready
    );
    assert!(view.evaluation(key).unwrap().last_result().is_none());
}

#[test]
fn report_authorization_is_allocation_free_and_preserves_inherited_visibility() {
    let mut fixture = Fixture::new(true);
    fixture.enter();
    fixture.begin(1);
    let key = fixture.key(1);
    let view = fixture.view();
    let expected = view.evaluation(key).unwrap().binding();
    let before = fixture.core.state.budget.stats();
    for labels in [&["internal"][..], &["team:qa"][..], &[][..]] {
        let (report, descriptor) = fixture.report(1, VerdictValue::Pass, labels);
        assert!(matches!(
            super::report(
                &view,
                f::context(EVALUATOR, 100),
                fixture.claim(),
                key,
                expected,
                report,
                &descriptor,
                fixture.core.limits
            ),
            Err(NativeError::Contract(ContractError::InvalidPolicy))
        ));
    }
    let (report, descriptor) = fixture.report(
        1,
        VerdictValue::Pass,
        &["internal", "team:qa", "team:review"],
    );
    let (_, authorization) = super::report(
        &view,
        f::context(EVALUATOR, 100),
        fixture.claim(),
        key,
        expected,
        report,
        &descriptor,
        fixture.core.limits,
    )
    .unwrap();
    assert_eq!(authorization.attempt(), report.attempt);
    assert_eq!(authorization.schema(), focal_evidence::test_report_schema());
    for (actor, mut changed, time) in [
        (ISSUER, report, 100),
        (EVALUATOR, report, 1000),
        (EVALUATOR, report, 100),
    ] {
        if actor == EVALUATOR && time == 100 {
            changed.generation += 1;
        }
        assert!(
            super::report(
                &view,
                f::context(actor, time),
                fixture.claim(),
                key,
                expected,
                changed,
                &descriptor,
                fixture.core.limits
            )
            .is_err()
        );
    }
    assert_eq!(fixture.core.state.budget.stats(), before);
    assert!(view.evaluation(key).unwrap().last_result().is_none());
}

#[test]
fn bounded_registry_and_response_source_fences_refuse_before_any_report() {
    let mut fixture = Fixture::new(true);
    fixture.enter();
    fixture.begin(1);
    let view = fixture.view();
    let key = fixture.key(1);
    let (report, descriptor) = fixture.report(1, VerdictValue::Pass, &["internal", "team:qa"]);
    let expected = view.evaluation(key).unwrap().binding();
    for case in 0..4 {
        let mut key = key;
        let mut claim = fixture.claim();
        let mut limits = fixture.core.limits;
        match case {
            0 => key.generation += 1,
            1 => claim.revision.0 += 1,
            2 => limits.evaluations_per_claim = 1,
            _ => limits.plan_edges = 1,
        }
        assert!(
            super::report(
                &view,
                f::context(EVALUATOR, 100),
                claim,
                key,
                expected,
                report,
                &descriptor,
                limits
            )
            .is_err()
        );
    }
}

#[test]
fn already_begun_sibling_can_report_after_checked_work_response_and_claim_failure() {
    let mut fixture = Fixture::new(true);
    fixture.enter();
    fixture.begin(1);
    fixture.begin(2);
    let required = fixture.key(1);
    let sibling = fixture.key(2);
    let (report, descriptor) = fixture.report(1, VerdictValue::Fail, &["internal", "team:qa"]);
    let view = fixture.view();
    let registered =
        super::super::admission_authority::registered_any(&view, fixture.claim(), required)
            .unwrap();
    let owner = super::report_owner(&view, &registered, 100, fixture.core.limits).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 1024 * 1024,
            max_staging_bytes: 2 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 65536,
        },
    )
    .unwrap();
    let custody = store
        .verify_native_artifact(
            f::request(EVALUATOR, 9000),
            &descriptor,
            ContentDomainId::from_u128(93),
            &fixture.core.state.budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let evaluation = registered.state.bind(registered.definition).unwrap();
    let completed = evaluation
        .report(
            Principal::Actor(EVALUATOR),
            &evaluation.binding(),
            &owner,
            report,
            &validation::EvidenceFacts {
                binding: descriptor.binding(),
                claim: required.claim,
                validation: required.validation,
                target: registered.state.target(),
                generation: required.generation,
                attempt: report.attempt,
                producer: EVALUATOR,
                value: VerdictValue::Fail,
                kind: validation::EvidenceKind::Proof,
                schema: descriptor.schema_hash(),
                custody_revision: Some(custody.custody().local_revision()),
            },
        )
        .unwrap();
    let accepted = completed.result.unwrap();
    let evaluation_row = OwnedEvaluation::new(completed.next.into_state()).unwrap();
    let source_claim = view.owned_claim(required.claim).unwrap();
    let mut claim_row = source_claim.copy().unwrap();
    let (claim, _) = claim_row.parts_mut().unwrap();
    let begun_claim = aggregation::ClaimAggregation::new(claim, aggregation_limits()).unwrap();
    claim
        .observe_evaluation(&claim.binding(), &evaluation, &begun_claim.decision())
        .unwrap();
    let response = fixture.response();
    let delivery_key = EvaluationKey {
        claim: required.claim,
        validation: ValidationId::from_u128(100),
        target: EvaluationTarget::Delivery {
            response: TestamentId::from_u128(900),
        },
        generation: 1,
    };
    let delivery = view
        .evaluation(delivery_key)
        .unwrap()
        .last_result()
        .unwrap();
    let mut response_aggregate = aggregation::ResponseAggregation::new(
        claim,
        response.evaluation().unwrap(),
        &[delivery],
        aggregation_limits(),
    )
    .unwrap();
    let cut = SessionSeq(view.prefix().0 + 1);
    let update = response_aggregate.apply(cut, &[accepted]).unwrap();
    let mut work_row = *fixture
        .core
        .native_work(ArtifactId::from_u128(800))
        .unwrap();
    let parent = evidence::Parent::from_claim(claim).unwrap();
    work_row.state = work_row
        .state
        .observe_evaluation(&work_row.state.binding(), &parent, response, &evaluation)
        .unwrap();
    work_row.state = work_row
        .state
        .apply_aggregate(&work_row.state.binding(), &update)
        .unwrap();
    let work_row = OwnedWork::new(work_row).unwrap();
    let transition = response
        .plan_aggregate(&response.identity().binding, &update)
        .unwrap()
        .unwrap();
    let Some(Row::Response(source)) = view.get(Key::Response(TestamentId::from_u128(900))) else {
        panic!("response")
    };
    let response_row = source
        .transition(
            transition,
            PublicationPosition {
                sequence: cut,
                ordinal: 0,
            },
        )
        .unwrap();
    let mut aggregate = aggregation::ClaimAggregation::new(claim, aggregation_limits()).unwrap();
    aggregate.apply(cut, &[update]).unwrap();
    claim
        .apply_aggregate(&claim.binding(), &aggregate.decision())
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::ValidationFailed);
    let heap = claim_row.heap_charge().unwrap();
    fixture.replace(Key::Claim(required.claim), Row::Claim(claim_row), heap);
    let heap = response_row.heap_charge().unwrap();
    fixture.replace(
        Key::Response(TestamentId::from_u128(900)),
        Row::Response(response_row),
        heap,
    );
    let heap = work_row.heap_charge().unwrap();
    fixture.replace(
        Key::Work(ArtifactId::from_u128(800)),
        Row::Work(work_row),
        heap,
    );
    let heap = evaluation_row.heap_charge().unwrap();
    fixture.replace(
        Key::Evaluation(required),
        Row::Evaluation(evaluation_row),
        heap,
    );
    let view = fixture.view();
    assert_eq!(fixture.response().state(), ResponseState::ValidationFailed);
    assert_eq!(
        fixture
            .core
            .native_work(ArtifactId::from_u128(800))
            .unwrap()
            .state
            .state(),
        WorkArtifactState::ValidationFailed
    );
    assert_eq!(
        view.evaluation(sibling).unwrap().state(),
        validation::State::Validating
    );
    let before_cut = view.claim(sibling.claim).unwrap().terminal_cut();
    let (report, descriptor) = fixture.report(2, VerdictValue::Pass, &["internal", "team:qa"]);
    let (_, authorized) = super::report(
        &view,
        f::context(EVALUATOR, 100),
        fixture.claim(),
        sibling,
        view.evaluation(sibling).unwrap().binding(),
        report,
        &descriptor,
        fixture.core.limits,
    )
    .unwrap();
    assert_eq!(authorized.attempt(), report.attempt);
    assert_eq!(
        view.claim(sibling.claim).unwrap().terminal_cut(),
        before_cut
    );
}
