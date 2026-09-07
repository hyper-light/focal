use super::*;
use crate::native::report_tests as fixture;
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_model::{
    ContentDomainId, Deadline, HandlerRef, TimerId, ValidationKind, ValidationMode,
    ValidationPhase, ValidatorId, VerdictValue,
};

fn definition(index: u32, attempts: u32, slot: bool) -> validation::Declaration {
    definition_with_quality_policy(index, attempts, slot, None)
}

fn definition_with_quality_policy(
    index: u32,
    attempts: u32,
    slot: bool,
    required: Option<ContentHash>,
) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(91),
        version: ContentHash([12; 32]),
        agentic: false,
    };
    let handlers = [validation::HandlerPolicy {
        handler: &handler,
        attempts,
        proof_schema: focal_evidence::test_report_schema(),
        diagnostic_schema: focal_evidence::error_report_schema(),
    }];
    let quality_handler = HandlerRef {
        id: ValidatorId::from_u128(92),
        version: ContentHash([13; 32]),
        agentic: true,
    };
    let quality_handlers = [validation::HandlerPolicy {
        handler: &quality_handler,
        attempts: 1,
        proof_schema: focal_evidence::test_report_schema(),
        diagnostic_schema: focal_evidence::error_report_schema(),
    }];
    validation::Declaration::new(
        Principal::Actor(fixture::ISSUER),
        validation::DeclarationSpec {
            binding: fixture::binding(100 + u128::from(index)),
            claim: ClaimId::from_u128(1),
            issuer: fixture::ISSUER,
            declaration_index: index,
            kind: ValidationKind::Test,
            phase: if slot {
                ValidationPhase::WholeWork
            } else {
                ValidationPhase::Admission
            },
            mode: ValidationMode::Observe,
            target: if slot {
                validation::TargetDeclaration::WholeWorkSlot {
                    index: 0,
                    name: "report",
                }
            } else {
                validation::TargetDeclaration::Admission
            },
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: fixture::EVALUATOR,
                    definition: ContentHash([14; 32]),
                    handlers: &handlers,
                    required_policy: None,
                },
                quality: required.map(|grant| validation::PhasePolicy {
                    evaluator: fixture::QUALITY,
                    definition: ContentHash([15; 32]),
                    handlers: &quality_handlers,
                    required_policy: Some(grant),
                }),
            },
            deadline: Deadline {
                timer: TimerId::from_u128(100 + u128::from(index)),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 1,
            attempts: attempts.checked_add(u32::from(required.is_some())).unwrap(),
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn creation(count: u32, attempts: u32, slot: bool) -> NativeInput {
    let mut input = fixture::creation(1, 1, &[], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("creation fixture returned another operation");
    };
    for index in 1..=count {
        declarations.push(definition(index, attempts, false));
    }
    let checks = [aggregation::CheckPolicy {
        declaration_index: count + 1,
        validation: ValidationId::from_u128(101 + u128::from(count)),
        mode: ValidationMode::Observe,
    }];
    let slots = [aggregation::SlotPolicy {
        slot: 0,
        missing_declaration_index: count + 2,
        mode: ValidationMode::Observe,
        checks: &checks,
    }];
    if slot {
        declarations.push(definition(count + 1, 1, true));
    }
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        fixture::binding(1),
        fixture::ISSUER,
        if slot { &slots } else { &[] },
        declarations,
        aggregation::Limits {
            max_slots: 4,
            max_checks: 64,
            max_results: 128,
            max_updates: 128,
        },
    )
    .unwrap();
    input
}

fn core(visits: usize) -> Core<NativeState> {
    Core::new_native(
        fixture::binding(1).ledger,
        RangeId(882),
        NativeLimits {
            range: RangeConfig {
                page_entries: 4,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            preparation_bytes: 1024 * 1024,
            plan_nodes: 16,
            plan_edges: visits,
            evaluations_per_claim: 64,
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn posted(count: u32, visits: usize, attempts: u32, slot: bool) -> Core<NativeState> {
    let mut core = core(visits);
    fixture::publish(&mut core, 10, creation(count, attempts, slot));
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    core
}

fn begin(core: &Core<NativeState>, index: u32, id: u128) -> NativeInput {
    fixture::begin(
        id,
        core.native_claim(ClaimId::from_u128(1)).unwrap().binding(),
        index,
        core.native_evaluation(fixture::key(index))
            .unwrap()
            .binding(),
    )
}

fn bound(core: &Core<NativeState>) -> usize {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let claim = view.owned_claim(ClaimId::from_u128(1)).unwrap();
    aggregation::admission_completion_visits(claim.claim().unwrap(), claim.registrations().unwrap())
        .unwrap()
}

fn project(core: &Core<NativeState>, visits: usize) -> Result<(), ContractError> {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let claim = view.owned_claim(ClaimId::from_u128(1)).unwrap();
    aggregation::project_admission(
        claim.claim().unwrap(),
        claim.registrations().unwrap(),
        &crate::native::admission_view::AdmissionRows {
            view: &view,
            sequence: view.prefix(),
            claim: ClaimId::from_u128(1),
            report: None,
        },
        aggregation::AdmissionLimits {
            declarations: 64,
            evaluations: 64,
            visits,
        },
    )
    .map(|_| ())
}

#[test]
fn begin_refuses_currently_valid_cohort_whose_later_results_exceed_visit_limit() {
    let core = posted(30, 4096, 1, false);
    assert_eq!(bound(&core), 4278);
    project(&core, 4096).unwrap();
    let state = *core.native_evaluation(fixture::key(1)).unwrap();
    let stats = core.native_stats();
    let budget = core.state.budget.stats();
    let input = begin(&core, 1, 3);
    let request = input.request;
    assert!(matches!(
        core.prepare_native(fixture::context(fixture::EVALUATOR, 30), input, &[]),
        Err(NativeError::Capacity(
            "admission completion projection visits"
        ))
    ));
    assert_eq!(core.native_evaluation(fixture::key(1)), Some(&state));
    assert_eq!(core.native_stats(), stats);
    assert_eq!(core.state.budget.stats(), budget);
    assert!(core.native_outcome(request).is_none());
}

#[test]
fn exact_visit_boundary_admits_and_covers_every_real_sibling_report() {
    let mut core = posted(30, 4278, 1, false);
    for index in 1..=30 {
        let input = begin(&core, index, 10 + u128::from(index));
        fixture::publish(&mut core, 30, input);
    }
    let directory = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 1024 * 1024,
            max_staging_bytes: 2 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 1024,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap();
    let custody_budget = MemoryBudget::new(16 * 1024 * 1024, 0).unwrap();
    for index in 1..=30 {
        let artifact = fixture::descriptor(fixture::artifact_spec(
            200 + u128::from(index),
            fixture::EVALUATOR,
            VerdictValue::Pass,
        ));
        let input = fixture::report_for(
            &core,
            None,
            100 + u128::from(index),
            index,
            VerdictValue::Pass,
            artifact,
        );
        let NativeCommand::ReportAdmission { artifact, .. } = &input.command else {
            panic!("report fixture returned another operation");
        };
        let custody = store
            .verify_native_artifact(
                input.request,
                artifact.get().unwrap(),
                ContentDomainId::from_u128(883),
                &custody_budget,
                &BuiltinNativeSchemas,
            )
            .unwrap();
        let next = fixture::report(&core, input, &[], &custody);
        assert_eq!(next.outcome().events, 3);
        core.publish_native(next).unwrap();
    }
    assert_eq!(bound(&core), 4278);
    project(&core, 4278).unwrap();
    assert!(matches!(project(&core, 4277), Err(ContractError::Capacity)));
    assert!(matches!(project(&core, 4096), Err(ContractError::Capacity)));
    for index in 1..=30 {
        assert_eq!(
            core.native_evaluation(fixture::key(index)).unwrap().state(),
            validation::State::Validated
        );
    }
    assert_eq!(custody_budget.stats().used, 0);
}

#[test]
fn projection_bound_counts_slots_and_checks_but_not_retry_history_length() {
    let single = posted(1, 256, 1, false);
    let retry = posted(1, 256, u32::MAX, false);
    let with_slot = posted(1, 256, 1, true);
    assert_eq!(bound(&single), 15);
    assert_eq!(bound(&retry), 15);
    assert_eq!(bound(&with_slot), 26);
    assert_eq!(
        retry
            .native_definition(fixture::key(1).validation)
            .unwrap()
            .attempt_bound(),
        u32::MAX
    );
}

#[test]
fn begin_requires_failure_write_capacity_and_checks_actor_first() {
    let mut core = posted(1, 256, 1, false);
    let baseline = core.native_stats();
    let budget = core.state.budget.stats();
    for capacity in 4..11 {
        core.limits.range.max_batch_entries = capacity;
        assert!(matches!(
            core.prepare_native(
                fixture::context(fixture::EVALUATOR, 30),
                begin(&core, 1, 3),
                &[]
            ),
            Err(NativeError::Capacity("admission completion write set"))
        ));
        let mut wrong_actor = begin(&core, 1, 3);
        wrong_actor.request.principal = fixture::SUBJECT;
        assert!(matches!(
            core.prepare_native(fixture::context(fixture::SUBJECT, 30), wrong_actor, &[]),
            Err(NativeError::Contract(ContractError::WrongActor))
        ));
        assert_eq!(core.native_stats(), baseline);
        assert_eq!(core.state.budget.stats(), budget);
    }
    core.limits.range.max_batch_entries = 11;
    let next = fixture::prepared(core.prepare_native(
        fixture::context(fixture::EVALUATOR, 30),
        begin(&core, 1, 3),
        &[],
    ));
    assert_eq!(next.outcome().events, 1);
    core.publish_native(next).unwrap();
}

#[test]
fn pending_post_cohort_is_checked_without_consuming_or_dropping_the_predecessor() {
    let mut core = core(4096);
    fixture::publish(&mut core, 10, creation(30, 1, false));
    let post = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 20),
        fixture::post(2, fixture::binding(1)),
        &[],
    ));
    let input = fixture::begin(
        3,
        post.claim(ClaimId::from_u128(1)).unwrap().binding(),
        1,
        post.evaluation(fixture::key(1)).unwrap().binding(),
    );
    let budget = core.state.budget.stats();
    assert!(matches!(
        core.prepare_native(fixture::context(fixture::EVALUATOR, 30), input, &[&post]),
        Err(NativeError::Capacity(
            "admission completion projection visits"
        ))
    ));
    assert_eq!(core.state.budget.stats(), budget);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
    core.publish_native(post).unwrap();
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Posted
    );
}

#[test]
fn numeric_bound_never_substitutes_for_complete_definition_resolution() {
    struct MissingReceipt<'a, 'b>(crate::native::admission_view::AdmissionRows<'a, 'b>);
    impl aggregation::AdmissionView for MissingReceipt<'_, '_> {
        fn prefix(&self) -> SessionSeq {
            aggregation::AdmissionView::prefix(&self.0)
        }
        fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
            if id == ValidationId::from_u128(100) {
                None
            } else {
                aggregation::AdmissionView::declaration(&self.0, id)
            }
        }
        fn evaluation(
            &self,
            row: aggregation::RegisteredEvaluation,
        ) -> Option<&validation::EvaluationState> {
            aggregation::AdmissionView::evaluation(&self.0, row)
        }
        fn accepted(
            &self,
            result: &validation::AcceptedResult,
        ) -> Option<aggregation::PublishedAdmissionResult<'_>> {
            aggregation::AdmissionView::accepted(&self.0, result)
        }
    }
    let core = posted(1, 256, 1, false);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let row = view.owned_claim(ClaimId::from_u128(1)).unwrap();
    let missing = MissingReceipt(crate::native::admission_view::AdmissionRows {
        view: &view,
        sequence: view.prefix(),
        claim: ClaimId::from_u128(1),
        report: None,
    });
    assert_eq!(bound(&core), 15);
    assert!(matches!(
        check(
            row.claim().unwrap(),
            row.registrations().unwrap(),
            &missing,
            core.limits
        ),
        Err(NativeError::Contract(ContractError::MissingEvidence))
    ));
}

#[test]
fn native_begin_refuses_unavailable_future_quality_policy_before_publication() {
    let mut input = creation(1, 1, false);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("creation fixture returned another operation");
    };
    declarations[1] = definition_with_quality_policy(1, 1, false, Some(ContentHash([67; 32])));
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        fixture::binding(1),
        fixture::ISSUER,
        &[],
        declarations,
        aggregation::Limits {
            max_slots: 4,
            max_checks: 4,
            max_results: 8,
            max_updates: 8,
        },
    )
    .unwrap();
    let mut core = core(256);
    fixture::publish(&mut core, 10, input);
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    let stats = core.native_stats();
    let budget = core.state.budget.stats();
    let old = *core.native_evaluation(fixture::key(1)).unwrap();
    let input = begin(&core, 1, 3);
    let request = input.request;
    assert!(matches!(
        core.prepare_native(fixture::context(fixture::EVALUATOR, 30), input, &[]),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    assert_eq!(core.native_evaluation(fixture::key(1)), Some(&old));
    assert_eq!(core.native_stats(), stats);
    assert_eq!(core.state.budget.stats(), budget);
    assert!(core.native_outcome(request).is_none());
}
