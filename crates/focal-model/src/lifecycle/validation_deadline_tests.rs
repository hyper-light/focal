use super::super::tests::{self as fixtures, EVALUATOR, ISSUER, binding};
use super::*;
use crate::lifecycle::{aggregation, claim, graph, succession};
use crate::{ObjectRevision, ReceiptId, RootCommandId, SessionSeq, ValidationPhase};

fn aggregation_limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 4,
        max_checks: 8,
        max_results: 32,
        max_updates: 8,
    }
}
fn cut() -> ClaimCut {
    ClaimCut {
        position: SessionSeq(8),
        cause: ContentHash([89; 32]),
    }
}
struct Fixture {
    claim: ClaimState,
    definitions: Vec<Declaration>,
}
impl Fixture {
    fn new(target: TargetDeclaration<'static>, policy: bool, quality: bool) -> Self {
        let program = if target == TargetDeclaration::Delivery {
            Program::Delivery
        } else {
            let Program::Programmatic { mut check, quality } = fixtures::programmatic(quality)
            else {
                unreachable!()
            };
            check.required_policy = policy.then_some(ContentHash([90; 32]));
            Program::Programmatic { check, quality }
        };
        let mut spec = fixtures::specification(ValidationMode::Required, program);
        spec.target = target;
        spec.phase = match target {
            TargetDeclaration::Admission => ValidationPhase::Admission,
            TargetDeclaration::Increment => ValidationPhase::Increment,
            _ => ValidationPhase::WholeWork,
        };
        if target == TargetDeclaration::Delivery {
            spec.kind = ValidationKind::Receipt;
        }
        let declaration =
            Declaration::new(Principal::Actor(ISSUER), spec, fixtures::limits()).unwrap();
        let mut definitions = vec![declaration];
        if target != TargetDeclaration::Delivery {
            definitions.push(
                Declaration::new(
                    Principal::Actor(ISSUER),
                    DeclarationSpec {
                        binding: binding(101),
                        declaration_index: 5,
                        kind: ValidationKind::Receipt,
                        phase: ValidationPhase::WholeWork,
                        target: TargetDeclaration::Delivery,
                        program: Program::Delivery,
                        ..spec
                    },
                    fixtures::limits(),
                )
                .unwrap(),
            );
        }
        let checks = [aggregation::CheckPolicy {
            declaration_index: 4,
            validation: ValidationId::from_u128(100),
            mode: ValidationMode::Required,
        }];
        let slots = [aggregation::SlotPolicy {
            slot: 0,
            missing_declaration_index: 6,
            mode: ValidationMode::Required,
            checks: &checks,
        }];
        let mut definition = claim::tests::definition(4);
        definition.binding = binding(200);
        definition.issuer = ISSUER;
        definition.subject = ParticipantId::from_u128(99);
        definition.deadline = None;
        definition.lineage =
            succession::Lineage::root(binding(200), RootCommandId::from_u128(1)).unwrap();
        definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(200),
            ISSUER,
            if matches!(target, TargetDeclaration::WholeWorkSlot { .. }) {
                &slots
            } else {
                &[]
            },
            &definitions,
            aggregation_limits(),
        )
        .unwrap();
        let mut claim = ClaimState::generate(Principal::Actor(ISSUER), definition).unwrap();
        claim
            .post_owned(Principal::Actor(ISSUER), claim.binding())
            .unwrap();
        if target != TargetDeclaration::Admission {
            let snapshot = graph::Snapshot::capture(
                &[&claim],
                graph::Limits {
                    nodes: 4,
                    edges: 8,
                    visits: 64,
                },
            )
            .unwrap();
            let start = snapshot.start(ClaimId(claim.binding().object.0)).unwrap();
            let aggregate =
                aggregation::ClaimAggregation::new(&claim, aggregation_limits()).unwrap();
            claim
                .acquire_receipt(
                    &claim.binding(),
                    Principal::Actor(claim.subject()),
                    ReceiptFence {
                        receipt: ReceiptId::from_u128(10),
                        epoch: 1,
                    },
                    &aggregate.admission(),
                    &start,
                    &[],
                )
                .unwrap();
        }
        Self { claim, definitions }
    }
    fn ready(&self, missing: bool) -> Evaluation<'_> {
        let declaration = &self.definitions[0];
        let target = match declaration.target() {
            TargetDeclaration::Admission => Target::Admission {
                claim: self.claim.binding(),
            },
            TargetDeclaration::Increment => Target::Increment {
                claim: self.claim.binding(),
                artifact: binding(400),
            },
            TargetDeclaration::Delivery => Target::Delivery {
                response: binding(300),
            },
            TargetDeclaration::WholeWorkSlot { index, .. } if missing => Target::MissingSlot {
                response: binding(300),
                slot: index,
            },
            TargetDeclaration::WholeWorkSlot { index, .. } => Target::Artifact {
                response: binding(300),
                slot: index,
                artifact: binding(400),
            },
        };
        Evaluation::materialize(
            Principal::Actor(ISSUER),
            declaration,
            Materialization {
                binding: declaration.binding(),
                target,
                slot_name: match declaration.target() {
                    TargetDeclaration::WholeWorkSlot { name, .. } => Some(name),
                    _ => None,
                },
                generation: 1,
                receipt: self.claim.receipt().map(|receipt| receipt.fence),
            },
        )
        .unwrap()
    }
}

#[test]
fn exact_deadline_fences_ready_all_target_roles_without_handler_policy_or_result() {
    for (target, missing) in [
        (TargetDeclaration::Admission, false),
        (TargetDeclaration::Increment, false),
        (TargetDeclaration::Delivery, false),
        (
            TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "output",
            },
            false,
        ),
        (
            TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "output",
            },
            true,
        ),
    ] {
        let fixture = Fixture::new(target, true, false);
        let ready = fixture.ready(missing);
        let original_claim = fixture.claim.binding();
        let next = ready
            .fence_deadline(
                &ready.binding(),
                &fixture.claim,
                ready.deadline(),
                ready.deadline().at,
                cut(),
            )
            .unwrap();
        assert_eq!(next.binding(), ready.binding().next().unwrap());
        assert_eq!(
            next.fence(),
            Some(AuthorityFence {
                reason: FenceReason::Deadline(ready.deadline()),
                cause: cut().cause
            })
        );
        assert_eq!(next.state(), State::Ready);
        assert!(!next.has_begun());
        assert_eq!(next.last_result(), None);
        assert_eq!(next.receipt(), ready.receipt());
        assert_eq!(next.target(), ready.target());
        assert_eq!(next.generation(), ready.generation());
        assert_eq!(
            next.bind(&fixture.definitions[0]).unwrap().suppression(),
            ready.suppression()
        );
        assert_eq!(fixture.claim.binding(), original_claim);
        assert!(next.bind(&fixture.definitions[0]).unwrap().audit_finished());
    }
}

#[test]
fn timer_frame_rejects_wrong_generation_early_time_stale_binding_source_and_cut() {
    let fixture = Fixture::new(TargetDeclaration::Admission, false, false);
    let ready = fixture.ready(false);
    for deadline in [
        Deadline {
            timer: crate::TimerId::from_u128(999),
            ..ready.deadline()
        },
        Deadline {
            generation: 2,
            ..ready.deadline()
        },
        Deadline {
            at: 99,
            ..ready.deadline()
        },
    ] {
        assert_eq!(
            ready.fence_deadline(&ready.binding(), &fixture.claim, deadline, 100, cut()),
            Err(ContractError::InvalidCut)
        );
    }
    assert_eq!(
        ready.fence_deadline(
            &ready.binding(),
            &fixture.claim,
            ready.deadline(),
            99,
            cut()
        ),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(
        ready.fence_deadline(
            &ready.binding().next().unwrap(),
            &fixture.claim,
            ready.deadline(),
            100,
            cut()
        ),
        Err(ContractError::StaleRevision)
    );
    for invalid in [
        ClaimCut {
            position: SessionSeq(0),
            ..cut()
        },
        ClaimCut {
            cause: ContentHash([0; 32]),
            ..cut()
        },
    ] {
        assert_eq!(
            ready.fence_deadline(
                &ready.binding(),
                &fixture.claim,
                ready.deadline(),
                100,
                invalid
            ),
            Err(ContractError::InvalidCut)
        );
    }
    let definition = claim::tests::definition(4);
    let other = ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    assert_eq!(
        ready.fence_deadline(&ready.binding(), &other, ready.deadline(), 100, cut()),
        Err(ContractError::WrongObject)
    );
    let substituted = Fixture::new(TargetDeclaration::Admission, true, false);
    assert_eq!(
        ready
            .into_state()
            .bind(&substituted.definitions[0])
            .unwrap_err(),
        ContractError::ContentConflict
    );
    assert_eq!(
        ready.fence_deadline(
            &ready.binding(),
            &substituted.claim,
            ready.deadline(),
            100,
            cut(),
        ),
        Err(ContractError::InvalidPolicy)
    );
    let mut future = ready;
    future.stored.target = Target::Admission {
        claim: fixture.claim.binding().next().unwrap(),
    };
    assert_eq!(
        future.fence_deadline(
            &future.binding(),
            &fixture.claim,
            future.deadline(),
            100,
            cut()
        ),
        Err(ContractError::StaleRevision)
    );
    let mut exhausted = ready;
    exhausted.stored.binding.revision = ObjectRevision(u64::MAX);
    assert_eq!(
        exhausted.fence_deadline(
            &exhausted.binding(),
            &fixture.claim,
            exhausted.deadline(),
            100,
            cut()
        ),
        Err(ContractError::Capacity)
    );
    assert_eq!(ready.fence(), None);
    assert_eq!(ready.state(), State::Ready);
}

#[test]
fn deadline_preserves_retry_and_quality_evidence_and_closes_only_authority() {
    let fixture = Fixture::new(TargetDeclaration::Admission, false, true);
    let ready = fixture.ready(false);
    let owner = ready.admission_owner(&fixture.claim, 1).unwrap();
    let begun = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    let (report, evidence) = fixtures::report_parts(&begun, VerdictValue::Error);
    let retry = begun
        .report(
            Principal::Actor(EVALUATOR),
            &begun.binding(),
            &fixtures::owner_for(&begun),
            report,
            &evidence,
        )
        .unwrap()
        .next;
    let (report, evidence) = fixtures::report_parts(&retry, VerdictValue::Pass);
    let quality = retry
        .report(
            Principal::Actor(EVALUATOR),
            &retry.binding(),
            &fixtures::owner_for(&retry),
            report,
            &evidence,
        )
        .unwrap()
        .next;
    assert_eq!(quality.state(), State::ValidatingQualityBar);
    for evaluation in [begun, retry, quality] {
        let next = evaluation
            .fence_deadline(
                &evaluation.binding(),
                &fixture.claim,
                evaluation.deadline(),
                100,
                cut(),
            )
            .unwrap();
        assert_eq!(next.state(), evaluation.state());
        assert_eq!(next.last_result(), evaluation.last_result());
        assert_eq!(
            next.bind(&fixture.definitions[0])
                .unwrap()
                .current_attempt()
                .unwrap(),
            evaluation.current_attempt().unwrap()
        );
        assert!(next.has_begun());
        assert!(next.bind(&fixture.definitions[0]).unwrap().audit_finished());
        assert_eq!(
            next.bind(&fixture.definitions[0])
                .unwrap()
                .admission_report_owner(&fixture.claim, 100)
                .unwrap_err(),
            ContractError::StaleEvaluation
        );
    }
}

#[test]
fn exact_historical_deadline_survives_receipt_adoption_and_parent_sealing() {
    let mut fixture = Fixture::new(TargetDeclaration::Increment, true, false);
    let state = fixture.ready(false).into_state();
    let original_receipt = state.receipt().unwrap();
    fixture
        .claim
        .apply(
            &fixture.claim.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::AdoptReceipt {
                previous: original_receipt,
                replacement: claim::ReceiptEntitlement {
                    holder: ParticipantId::from_u128(98),
                    fence: ReceiptFence {
                        receipt: ReceiptId::from_u128(11),
                        epoch: 2,
                    },
                },
            },
        )
        .unwrap();
    fixture
        .claim
        .apply(
            &fixture.claim.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Cancel {
                cut: ClaimCut {
                    position: SessionSeq(5),
                    ..cut()
                },
            },
        )
        .unwrap();
    let original_claim = (fixture.claim.binding(), fixture.claim.terminal_cut());
    let evaluation = state.bind(&fixture.definitions[0]).unwrap();
    assert_eq!(
        evaluation.fence_deadline(
            &evaluation.binding(),
            &fixture.claim,
            evaluation.deadline(),
            100,
            ClaimCut {
                position: SessionSeq(4),
                ..cut()
            }
        ),
        Err(ContractError::InvalidCut)
    );
    let next = evaluation
        .fence_deadline(
            &evaluation.binding(),
            &fixture.claim,
            evaluation.deadline(),
            100,
            cut(),
        )
        .unwrap();
    assert_eq!(next.receipt(), Some(original_receipt));
    assert_eq!(next.target(), state.target());
    assert_eq!(
        next.fence().unwrap().reason,
        FenceReason::Deadline(evaluation.deadline())
    );
    assert_eq!(
        (fixture.claim.binding(), fixture.claim.terminal_cut()),
        original_claim
    );
}

#[test]
fn repeat_deadline_and_original_control_or_terminal_result_are_exact_noops() {
    let fixture = Fixture::new(TargetDeclaration::Admission, false, false);
    let ready = fixture.ready(false);
    let owner = ready.admission_owner(&fixture.claim, 1).unwrap();
    let begun = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    let (report, evidence) = fixtures::report_parts(&begun, VerdictValue::Pass);
    let terminal = begun
        .report(
            Principal::Actor(EVALUATOR),
            &begun.binding(),
            &fixtures::owner_for(&begun),
            report,
            &evidence,
        )
        .unwrap()
        .next;
    let mut owner = fixtures::owner_for(&begun);
    owner.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::ReceiptAdoption,
        cause: ContentHash([91; 32]),
    });
    let controlled = begun.record_fence(&begun.binding(), &owner).unwrap();
    let timed = begun
        .fence_deadline(
            &begun.binding(),
            &fixture.claim,
            begun.deadline(),
            100,
            cut(),
        )
        .unwrap()
        .bind(&fixture.definitions[0])
        .unwrap();
    for evaluation in [terminal, controlled, timed] {
        assert_eq!(
            evaluation
                .fence_deadline(
                    &evaluation.binding(),
                    &fixture.claim,
                    evaluation.deadline(),
                    101,
                    ClaimCut {
                        cause: ContentHash([92; 32]),
                        ..cut()
                    }
                )
                .unwrap(),
            evaluation.into_state()
        );
        assert_eq!(
            evaluation.fence_deadline(
                &evaluation.binding(),
                &fixture.claim,
                Deadline {
                    generation: 2,
                    ..evaluation.deadline()
                },
                101,
                cut()
            ),
            Err(ContractError::InvalidCut)
        );
    }
}
