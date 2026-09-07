use super::*;
use crate::native::report_tests::{self as fixture, EVALUATOR, ISSUER, SUBJECT, binding};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::aggregation;
use focal_model::{
    Confidence, Deadline, HandlerRef, ObjectRevision, OutcomeKind, TimerId, ValidationKind,
    ValidationPhase, ValidatorId,
};

fn aggregation_limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 2,
        max_checks: 4,
        max_results: 16,
        max_updates: 8,
    }
}

fn declaration(mode: ValidationMode) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(301),
        version: ContentHash([31; 32]),
        agentic: false,
    };
    let handlers = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: focal_evidence::test_report_schema(),
        diagnostic_schema: focal_evidence::error_report_schema(),
    }];
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(101),
            claim: ClaimId::from_u128(1),
            issuer: ISSUER,
            declaration_index: 1,
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
                    definition: ContentHash([32; 32]),
                    handlers: &handlers,
                    required_policy: None,
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(302),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 1,
            attempts: 2,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

/// A real authored claim and response, with no fabricated readiness capability.
/// The respondent may claim Complete while omitting a declared output; assessing
/// that omission belongs to the independently checked validation lifecycle.
struct Fixture {
    core: Core<NativeState>,
    claim: ClaimState,
    response: Response,
}

impl Fixture {
    fn new(mode: ValidationMode) -> Self {
        let mut initial = fixture::creation(1, 1, &[], None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut initial.command
        else {
            panic!("fixture creation")
        };
        declarations.push(declaration(mode));
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(1),
            ISSUER,
            &[aggregation::SlotPolicy {
                slot: 0,
                missing_declaration_index: 2,
                mode: ValidationMode::Required,
                checks: &[aggregation::CheckPolicy {
                    declaration_index: 1,
                    validation: ValidationId::from_u128(101),
                    mode,
                }],
            }],
            declarations,
            aggregation_limits(),
        )
        .unwrap();
        let mut core = fixture::core();
        fixture::publish(&mut core, 1, initial);
        fixture::publish(&mut core, 2, fixture::post(2, binding(1)));
        let expected = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
        fixture::publish(
            &mut core,
            3,
            NativeInput {
                request: fixture::request(SUBJECT, 3),
                command: NativeCommand::AcquireReceipt {
                    expected,
                    receipt: ReceiptId::from_u128(701),
                },
            },
        );
        let retained = core.native_claim(ClaimId::from_u128(1)).unwrap();
        let mut claim = retained
            .try_copy_for_response(retained.copy_for_response_charge().unwrap())
            .unwrap();
        let parent = evidence::Parent::from_claim(&claim).unwrap();
        let mut response = Response::close(
            evidence::ResponseIdentity {
                binding: binding(900),
                claim: parent.claim,
                receipt: parent.receipt,
                cycle: parent.next_cycle,
                prior: parent.latest_response,
            },
            &parent,
            Principal::Actor(SUBJECT),
            &[],
            &[],
            evidence::CloseReport {
                summary: "Respondent reports completion without an output.",
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                diagnostics: &[],
                limits: evidence::ResponseLimits {
                    artifacts: 1,
                    diagnostics: 0,
                    summary_bytes: 128,
                    construction_bytes: 4096,
                },
            },
        )
        .unwrap()
        .response;
        claim
            .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &response)
            .unwrap();
        for actor in [SUBJECT, ISSUER] {
            let parent = evidence::Parent::from_claim(&claim).unwrap();
            let next = if actor == SUBJECT {
                response.plan_post(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(actor),
                )
            } else {
                response.plan_receive(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(actor),
                )
            }
            .unwrap();
            response.apply(next).unwrap();
            claim
                .observe_response(&claim.binding(), Principal::Actor(actor), &response)
                .unwrap();
        }
        Self {
            core,
            claim,
            response,
        }
    }

    fn evaluation(&self) -> validation::Evaluation<'_> {
        validation::Evaluation::materialize_work(
            Principal::Actor(ISSUER),
            self.core
                .native_definition(ValidationId::from_u128(101))
                .unwrap(),
            &self.claim,
            &self.response,
            None,
        )
        .unwrap()
    }

    fn owner(&self, evaluation: &validation::Evaluation<'_>) -> validation::OwnerState {
        let aggregate =
            aggregation::ClaimAggregation::new(&self.claim, aggregation_limits()).unwrap();
        evaluation
            .work_owner(&self.claim, &self.response, None, &aggregate.decision(), 10)
            .unwrap()
    }

    fn result(&self) -> validation::AcceptedResult {
        let evaluation = self.evaluation();
        evaluation
            .begin(
                Principal::Actor(ISSUER),
                &evaluation.binding(),
                &self.owner(&evaluation),
            )
            .unwrap()
            .result
            .unwrap()
    }
}

#[test]
fn missing_required_result_preserves_exact_model_fact_and_original_cut() {
    let fixture = Fixture::new(ValidationMode::Required);
    let result = fixture.result();
    assert_eq!(
        result.target(),
        validation::Target::MissingSlot {
            response: fixture.response.identity().binding,
            slot: 0,
        }
    );
    assert_eq!(result.binding().revision, ObjectRevision(2));
    assert_eq!(result.receipt(), Some(fixture.response.identity().receipt));
    assert_eq!(
        result.generation(),
        u64::from(fixture.response.identity().cycle)
    );
    assert!(result.is_terminal());
    assert_eq!(result.phase(), validation::Phase::MissingTarget);
    assert_eq!(result.verdict(), VerdictValue::Incomplete);
    assert_eq!(
        (result.attempt(), result.reporter(), result.evidence()),
        (None, None, None)
    );
    assert!(result.programmatic_evidence().is_none());
    for (sequence, ordinal) in [(SessionSeq(1), 0), (SessionSeq(u64::MAX), u32::MAX)] {
        let stored = NativeMissingResult::new(result, sequence, ordinal).unwrap();
        assert_eq!(stored.result(), result);
        assert_eq!(stored.sequence(), sequence);
        assert_eq!(stored.ordinal(), ordinal);
    }
    assert_eq!(fixture.response.state(), ResponseState::Received);
    assert_eq!(fixture.claim.status(), ClaimStatus::TestamentAcknowledged);
    assert_eq!(fixture.evaluation().state(), validation::State::Ready);
}

#[test]
fn missing_observe_has_no_result_to_store() {
    let fixture = Fixture::new(ValidationMode::Observe);
    let evaluation = fixture.evaluation();
    let transition = evaluation
        .begin(
            Principal::Actor(ISSUER),
            &evaluation.binding(),
            &fixture.owner(&evaluation),
        )
        .unwrap();
    assert!(transition.result.is_none());
    assert_eq!(transition.next.state(), validation::State::Ready);
    assert_eq!(
        transition.next.suppression(),
        Some(validation::Suppression::MissingTarget)
    );
    assert!(!transition.next.has_begun());
}

#[test]
fn missing_assessment_refuses_wrong_actors_and_stale_owner_fences() {
    let fixture = Fixture::new(ValidationMode::Required);
    let evaluation = fixture.evaluation();
    let owner = fixture.owner(&evaluation);
    for actor in [
        Principal::Actor(SUBJECT),
        Principal::Node(ISSUER),
        Principal::Node(EVALUATOR),
    ] {
        assert_eq!(
            evaluation
                .begin(actor, &evaluation.binding(), &owner)
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    for changed in 0..5 {
        let mut stale = owner;
        match changed {
            0 => stale.authority.receipt.as_mut().unwrap().epoch += 1,
            1 => stale.authority.generation += 1,
            2 => stale.authority.definition = ContentHash([91; 32]),
            3 => {
                stale.authority.state =
                    validation::AuthorityState::Fenced(validation::AuthorityFence {
                        reason: validation::FenceReason::Cancellation,
                        cause: ContentHash([92; 32]),
                    })
            }
            _ => stale.logical_time = evaluation.deadline().at,
        }
        assert!(
            evaluation
                .begin(Principal::Actor(ISSUER), &evaluation.binding(), &stale)
                .is_err()
        );
        assert_eq!(evaluation.state(), validation::State::Ready);
        assert!(evaluation.last_result().is_none());
    }
    let transition = evaluation
        .begin(Principal::Actor(EVALUATOR), &evaluation.binding(), &owner)
        .unwrap();
    let result = transition.result.unwrap();
    assert!(NativeMissingResult::new(result, SessionSeq(7), 0).is_ok());
    assert!(
        transition
            .next
            .begin(Principal::Actor(ISSUER), &transition.next.binding(), &owner)
            .is_err()
    );
}

#[test]
fn zero_publication_cut_and_real_delivery_result_are_refused() {
    let fixture = Fixture::new(ValidationMode::Required);
    assert_eq!(
        NativeMissingResult::new(fixture.result(), SessionSeq(0), 0),
        Err(ContractError::InvalidCut)
    );
    let delivery = validation::Evaluation::materialize_delivery(
        Principal::Actor(ISSUER),
        fixture
            .core
            .native_definition(ValidationId::from_u128(100))
            .unwrap(),
        &fixture.claim,
        &fixture.response,
    )
    .unwrap();
    let owner = delivery
        .delivery_owner(&fixture.claim, &fixture.response, 10)
        .unwrap();
    let result = delivery
        .receive_delivery(Principal::Actor(ISSUER), &delivery.binding(), &owner)
        .unwrap()
        .result
        .unwrap();
    assert_eq!(result.phase(), validation::Phase::Delivery);
    assert_eq!(
        NativeMissingResult::new(result, SessionSeq(7), 0),
        Err(ContractError::InvalidTarget)
    );
}

#[test]
fn externally_reported_outcomes_cannot_impersonate_a_missing_slot() {
    for value in [
        VerdictValue::Incomplete,
        VerdictValue::Error,
        VerdictValue::Pass,
    ] {
        let core = fixture::running(&[(ValidationMode::Required, false)]);
        let mut custody = fixture::Custody::new();
        let input = fixture::report_for(
            &core,
            None,
            40,
            1,
            value,
            fixture::descriptor(fixture::artifact_spec(500, EVALUATOR, value)),
        );
        let verified = fixture::verified(&mut custody, &input);
        let prepared = fixture::report(&core, input, &[], &verified);
        let result = prepared
            .evaluation(fixture::key(1))
            .unwrap()
            .last_result()
            .unwrap();
        assert!(result.attempt().is_some());
        assert_eq!(result.reporter(), Some(EVALUATOR));
        assert!(result.evidence().is_some());
        assert_eq!(
            NativeMissingResult::new(result, SessionSeq(7), 0),
            Err(ContractError::InvalidTarget)
        );
    }
}

#[test]
fn missing_materialization_refuses_malformed_target_receipt_and_generation() {
    let fixture = Fixture::new(ValidationMode::Required);
    let declaration = fixture
        .core
        .native_definition(ValidationId::from_u128(101))
        .unwrap();
    let ordinary = fixture.evaluation();
    let materialization = validation::Materialization {
        binding: ordinary.binding(),
        target: ordinary.target(),
        slot_name: Some("output"),
        generation: ordinary.generation(),
        receipt: ordinary.receipt(),
    };
    for changed in 0..6 {
        let mut malformed = materialization;
        match changed {
            0 => malformed.generation = 0,
            1 => malformed.receipt = None,
            2 => malformed.receipt.as_mut().unwrap().receipt = ReceiptId::from_u128(0),
            3 => malformed.receipt.as_mut().unwrap().epoch = 0,
            4 => malformed.slot_name = Some("other"),
            _ => {
                malformed.target = validation::Target::MissingSlot {
                    response: fixture.response.identity().binding,
                    slot: 1,
                }
            }
        }
        assert!(
            validation::Evaluation::materialize(Principal::Actor(ISSUER), declaration, malformed)
                .is_err()
        );
    }
}

#[test]
fn owned_result_copy_has_an_exact_precharged_unique_container() {
    let fixture = Fixture::new(ValidationMode::Required);
    let stored = NativeMissingResult::new(fixture.result(), SessionSeq(9), 3).unwrap();
    let charge = OwnedMissingResult::container_charge();
    let budget = MemoryBudget::new(charge * 2, 0).unwrap();
    let first_permit = budget
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, charge)
        .unwrap();
    let first = OwnedMissingResult::new(stored).unwrap();
    assert_eq!(first.heap_charge().unwrap(), charge);
    let insufficient = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 1)
        .unwrap();
    assert!(
        budget
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, charge)
            .is_err()
    );
    assert_eq!(first.get(), Some(&stored));
    drop(insufficient);
    let second_permit = budget
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, charge)
        .unwrap();
    let second = first.copy().unwrap();
    assert_eq!(second.heap_charge().unwrap(), charge);
    assert_eq!(second.get(), first.get());
    assert_ne!(second.0.as_ptr(), first.0.as_ptr());
    drop(first);
    drop(first_permit);
    assert_eq!(second.get(), Some(&stored));
    drop(second);
    drop(second_permit);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn malformed_owned_shape_cannot_be_read_charged_or_copied() {
    let fixture = Fixture::new(ValidationMode::Required);
    let stored = NativeMissingResult::new(fixture.result(), SessionSeq(9), 3).unwrap();
    for malformed in [
        OwnedMissingResult(Vec::new()),
        OwnedMissingResult(vec![stored, stored]),
    ] {
        assert!(malformed.get().is_none());
        assert_eq!(malformed.heap_charge(), Err(MemoryError::MissingKey));
        assert!(matches!(malformed.copy(), Err(MemoryError::MissingKey)));
    }
}
