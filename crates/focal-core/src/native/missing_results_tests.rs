use super::*;
use crate::native::report_tests::{self as fixture, EVALUATOR, ISSUER, SUBJECT, binding};
use focal_model::lifecycle::aggregation;
use focal_model::{
    Confidence, Deadline, HandlerRef, OutcomeKind, TimerId, ValidationKind, ValidationPhase,
    ValidatorId, VerdictValue,
};

fn limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 2,
        max_checks: 8,
        max_results: 32,
        max_updates: 8,
    }
}

fn declaration(index: u32, mode: ValidationMode) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(81),
        version: ContentHash([81; 32]),
        agentic: false,
    };
    let handlers = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 1,
        proof_schema: focal_evidence::test_report_schema(),
        diagnostic_schema: focal_evidence::error_report_schema(),
    }];
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(100 + u128::from(index)),
            claim: ClaimId::from_u128(1),
            issuer: ISSUER,
            declaration_index: index,
            kind: ValidationKind::Inspection,
            phase: ValidationPhase::WholeWork,
            mode,
            target: validation::TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "output",
            },
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: EVALUATOR,
                    definition: ContentHash([82; 32]),
                    handlers: &handlers,
                    required_policy: Some(ContentHash([83; 32])),
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(100 + u128::from(index)),
                generation: 1,
                at: 10,
            },
        },
        validation::Limits {
            handlers: 1,
            attempts: 1,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

struct Fixture {
    core: Core<NativeState>,
    claim: ClaimState,
    response: Response,
    acceptance: aggregation::ClaimAggregation,
    entry: PublicationPosition,
}

impl Fixture {
    fn new() -> Self {
        let mut initial = fixture::creation(1, 1, &[], None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut initial.command
        else {
            panic!("create fixture")
        };
        for (index, mode) in [
            (1, ValidationMode::Required),
            (2, ValidationMode::Observe),
            (3, ValidationMode::Required),
        ] {
            declarations.push(declaration(index, mode));
        }
        let checks: Vec<_> = declarations
            .iter()
            .skip(1)
            .map(|declaration| aggregation::CheckPolicy {
                declaration_index: declaration.declaration_index(),
                validation: ValidationId(declaration.binding().object.0),
                mode: declaration.mode(),
            })
            .collect();
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(1),
            ISSUER,
            &[aggregation::SlotPolicy {
                slot: 0,
                missing_declaration_index: 4,
                mode: ValidationMode::Required,
                checks: &checks,
            }],
            declarations,
            limits(),
        )
        .unwrap();
        let mut core = fixture::core();
        fixture::publish(&mut core, 1, initial);
        fixture::publish(&mut core, 2, fixture::post(2, binding(1)));
        let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
        fixture::publish(
            &mut core,
            3,
            NativeInput {
                request: fixture::request(SUBJECT, 3),
                command: NativeCommand::AcquireReceipt {
                    expected: claim,
                    receipt: ReceiptId::from_u128(701),
                },
            },
        );
        let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
        fixture::publish(
            &mut core,
            4,
            NativeInput {
                request: fixture::request(SUBJECT, 4),
                command: NativeCommand::CloseResponse {
                    claim,
                    response: binding(900),
                    report: NativeResponseInput {
                        summary: "Respondent reports completion; declared output is absent.".into(),
                        confidence: Confidence::Committed,
                        outcome: OutcomeKind::Complete,
                        manifest: vec![],
                        diagnostics: vec![],
                    },
                },
            },
        );
        for (time, actor) in [(5, SUBJECT), (6, ISSUER)] {
            let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
            let response = as_response(
                core.state
                    .rows
                    .get(&Key::Response(TestamentId::from_u128(900))),
            )
            .unwrap()
            .identity()
            .binding;
            let command = if actor == SUBJECT {
                NativeCommand::PostResponse {
                    claim,
                    expected: response,
                }
            } else {
                NativeCommand::ReceiveResponse {
                    claim,
                    expected: response,
                }
            };
            fixture::publish(
                &mut core,
                time,
                NativeInput {
                    request: fixture::request(actor, u128::from(time)),
                    command,
                },
            );
        }
        let source = core.native_claim(ClaimId::from_u128(1)).unwrap();
        let mut claim = source.try_copy(source.copy_charge().unwrap()).unwrap();
        let mut acceptance = aggregation::ClaimAggregation::new(&claim, limits()).unwrap();
        claim
            .request_evaluation(
                &claim.binding(),
                Principal::Actor(ISSUER),
                &acceptance.decision(),
            )
            .unwrap();
        acceptance.rebind(&claim).unwrap();
        let source = as_response(
            core.state
                .rows
                .get(&Key::Response(TestamentId::from_u128(900))),
        )
        .unwrap();
        let mut response = source.try_copy(source.retained_bytes().unwrap()).unwrap();
        response
            .apply(
                response
                    .plan_begin(
                        &response.identity().binding,
                        &claim,
                        Principal::Actor(ISSUER),
                        &acceptance.decision(),
                    )
                    .unwrap(),
            )
            .unwrap();
        let entry = PublicationPosition {
            sequence: SessionSeq(core.native_sequence().0 + 1),
            ordinal: 3,
        };
        Self {
            core,
            claim,
            response,
            acceptance,
            entry,
        }
    }
    fn view(&self) -> View<'_> {
        View {
            state: &self.core.state,
            tail: None,
        }
    }
    fn registry(&self) -> &RegistrationSet {
        match self.core.state.rows.get(&Key::Claim(ClaimId::from_u128(1))) {
            Some(Row::Claim(row)) => row.registrations().unwrap(),
            _ => panic!("retained claim"),
        }
    }
    fn extras(&self) -> (Extras, Scratch) {
        let mut scratch = Scratch {
            used: 0,
            max: 1024 * 1024,
        };
        let mut extras = Extras::new(16, 128 * 1024).unwrap();
        extras.begin_journal(16, &mut scratch).unwrap();
        for _ in 0..3 {
            extras
                .record(NativeFact::Registrations {
                    claim: self.claim.binding(),
                })
                .unwrap();
        }
        extras
            .record(NativeFact::Response {
                claim: ClaimId::from_u128(1),
                before: None,
                after: self.response.identity().binding,
                state: ResponseState::Validating,
            })
            .unwrap();
        (extras, scratch)
    }
    #[allow(clippy::too_many_arguments)] // Exercise individual owner-frame refusals independently.
    fn run(
        &self,
        context: NativeContext,
        registry: &RegistrationSet,
        entry: PublicationPosition,
        limits: NativeLimits,
        meta: &mut Meta,
        extras: &mut Extras,
        scratch: &mut Scratch,
    ) -> Result<usize, NativeError> {
        prepare(
            context,
            &self.view(),
            &self.claim,
            &self.response,
            &self.acceptance.decision(),
            entry,
            registry,
            limits,
            meta,
            extras,
            scratch,
        )
    }
    fn assert_source_unchanged(&self) {
        for index in 1..=3 {
            let key = EvaluationKey {
                claim: ClaimId::from_u128(1),
                validation: ValidationId::from_u128(100 + index),
                target: EvaluationTarget::MissingSlot {
                    response: TestamentId::from_u128(900),
                    slot: 0,
                },
                generation: 1,
            };
            let row = self.core.native_evaluation(key).unwrap();
            assert_eq!(row.state(), validation::State::Ready);
            assert!(!row.has_begun());
            assert!(row.last_result().is_none());
            assert!(
                row.bind(self.core.native_definition(key.validation).unwrap())
                    .unwrap()
                    .suppression()
                    .is_none()
            );
        }
    }
}

#[test]
fn full_missing_cohort_uses_actual_journal_positions_without_worker_or_artifact() {
    let fixture = Fixture::new();
    let (mut extras, mut scratch) = fixture.extras();
    let mut meta = fixture.view().meta();
    let before = meta;
    assert_eq!(
        fixture
            .run(
                fixture::context(ISSUER, 100),
                fixture.registry(),
                fixture.entry,
                fixture.core.limits,
                &mut meta,
                &mut extras,
                &mut scratch
            )
            .unwrap(),
        3
    );
    assert_eq!(meta.evaluations, before.evaluations);
    assert_eq!(meta.artifacts, before.artifacts);
    assert_eq!(meta.results, before.results + 2);
    assert_eq!(extras.rows.len(), 5);
    let events = extras.journal.as_ref().unwrap();
    let mut result_count = 0;
    for extra in &extras.rows {
        match &extra.row {
            Row::Evaluation(row) => {
                let next = row.get().unwrap();
                assert!(!next.has_begun());
                if next.binding().object == binding(102).object {
                    assert_eq!(
                        next.bind(
                            fixture
                                .core
                                .native_definition(ValidationId(next.binding().object.0))
                                .unwrap()
                        )
                        .unwrap()
                        .suppression(),
                        Some(validation::Suppression::MissingTarget)
                    );
                    assert!(next.last_result().is_none());
                } else {
                    assert_eq!(next.state(), validation::State::ValidationIncomplete);
                }
            }
            Row::MissingResult(row) => {
                let result = row.get().unwrap();
                let expected = if result_count == 0 { 5 } else { 8 };
                assert_eq!(result.sequence(), fixture.entry.sequence);
                assert_eq!(result.ordinal(), expected);
                assert_eq!(
                    events[usize::try_from(result.ordinal()).unwrap()],
                    NativeFact::Missing {
                        key: NativeResultKey::of(result.result())
                    }
                );
                assert_eq!(result.result_ref(), &result.result());
                assert_eq!(result.result().verdict(), VerdictValue::Incomplete);
                assert!(result.result().evidence().is_none());
                result_count += 1;
            }
            _ => panic!("only evaluation and internal missing result rows"),
        }
    }
    assert_eq!(result_count, 2);
    fixture.assert_source_unchanged();
}

#[test]
fn invalid_actor_cut_and_omitted_registry_refuse_before_staging_a_result() {
    let fixture = Fixture::new();
    let empty = RegistrationSet::new(&fixture.claim, 16, size_of::<RegistrationSet>()).unwrap();
    for bad in 0..4 {
        let (mut extras, mut scratch) = fixture.extras();
        let used = scratch.used;
        let mut meta = fixture.view().meta();
        let mut entry = fixture.entry;
        if bad == 1 {
            entry.sequence.0 += 1;
        }
        if bad == 2 {
            entry.ordinal += 1;
        }
        let context = fixture::context(if bad == 0 { SUBJECT } else { ISSUER }, 100);
        assert!(
            fixture
                .run(
                    context,
                    if bad == 3 { &empty } else { fixture.registry() },
                    entry,
                    fixture.core.limits,
                    &mut meta,
                    &mut extras,
                    &mut scratch
                )
                .is_err()
        );
        assert_eq!(extras.events(), 4);
        assert!(extras.rows.is_empty());
        assert_eq!(scratch.used, used);
        assert_eq!(meta.results, fixture.view().meta().results);
        fixture.assert_source_unchanged();
    }
}

#[test]
fn incomplete_candidate_capacity_refusal_leaves_all_retained_sources_unchanged() {
    let fixture = Fixture::new();
    for short in [true, false] {
        let (mut extras, mut scratch) = fixture.extras();
        let mut meta = fixture.view().meta();
        let mut limits = fixture.core.limits;
        if short {
            scratch.max = scratch.used
                + OwnedEvaluation::container_charge()
                + OwnedMissingResult::container_charge()
                - 1;
        } else {
            limits.results = meta.results + 1;
        }
        let before = fixture.core.native_budget();
        assert!(
            fixture
                .run(
                    fixture::context(ISSUER, 100),
                    fixture.registry(),
                    fixture.entry,
                    limits,
                    &mut meta,
                    &mut extras,
                    &mut scratch
                )
                .is_err()
        );
        assert_eq!(meta.results, fixture.view().meta().results);
        drop(extras);
        fixture.assert_source_unchanged();
        assert_eq!(fixture.core.native_budget(), before);
    }
}
