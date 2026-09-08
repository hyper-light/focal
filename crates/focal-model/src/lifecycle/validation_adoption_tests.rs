use super::super::tests::{self as fixtures, EVALUATOR, ISSUER, binding};
use super::*;
use crate::lifecycle::claim::{ClaimCut, ClaimState, ReceiptEntitlement};
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
        let mut spec = fixtures::specification(ValidationMode::Observe, program);
        spec.target = target;
        spec.phase = match target {
            TargetDeclaration::Admission => ValidationPhase::Admission,
            TargetDeclaration::Increment => ValidationPhase::Increment,
            _ => ValidationPhase::WholeWork,
        };
        if target == TargetDeclaration::Delivery {
            spec.kind = ValidationKind::Receipt;
            spec.mode = ValidationMode::Required;
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
                        mode: ValidationMode::Required,
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
            mode: ValidationMode::Observe,
        }];
        let slots = [aggregation::SlotPolicy {
            slot: 0,
            missing_declaration_index: 6,
            mode: ValidationMode::Observe,
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
        {
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
                        epoch: 3,
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
                receipt: if declaration.target() == TargetDeclaration::Admission {
                    None
                } else {
                    self.claim.receipt().map(|receipt| receipt.fence)
                },
            },
        )
        .unwrap()
    }
}

fn replacement() -> ReceiptEntitlement {
    ReceiptEntitlement {
        holder: ParticipantId::from_u128(98),
        fence: ReceiptFence {
            receipt: ReceiptId::from_u128(11),
            epoch: 4,
        },
    }
}
fn adoption(fixture: &Fixture) -> crate::lifecycle::claim::ReceiptAdoption<'_> {
    fixture
        .claim
        .prepare_receipt_adoption(
            &fixture.claim.binding(),
            Principal::Actor(ISSUER),
            fixture.claim.receipt().unwrap().fence,
            replacement(),
            cut(),
        )
        .unwrap()
}

#[test]
fn adoption_fences_every_ready_role_without_handler_policy_or_invented_evidence() {
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
        let token = adoption(&fixture);
        let next = ready
            .into_state()
            .adopt_receipt(&fixture.definitions[0], &token)
            .unwrap();
        assert_eq!(next.binding(), ready.binding().next().unwrap());
        assert_eq!(
            next.fence(),
            Some(AuthorityFence {
                reason: FenceReason::ReceiptAdoption,
                cause: cut().cause
            })
        );
        assert_eq!(next.state(), State::Ready);
        assert!(!next.has_begun());
        assert_eq!(next.last_result(), None);
        assert_eq!(next.receipt(), ready.receipt());
        assert_eq!(next.target(), ready.target());
        assert_eq!(next.generation(), ready.generation());
        assert_eq!(next.sealed(), ready.sealed());
        assert!(next.bind(&fixture.definitions[0]).unwrap().audit_finished());
        assert_eq!(
            next.adopt_receipt(&fixture.definitions[0], &token).unwrap(),
            next
        );
    }
}

#[test]
fn adoption_invalidates_begun_admission_increment_and_work_reports_without_erasing_attempts() {
    for target in [
        TargetDeclaration::Admission,
        TargetDeclaration::Increment,
        TargetDeclaration::WholeWorkSlot {
            index: 0,
            name: "output",
        },
    ] {
        let fixture = Fixture::new(target, false, true);
        let ready = fixture.ready(false);
        let begun = ready
            .begin(
                Principal::Actor(EVALUATOR),
                &ready.binding(),
                &fixtures::owner_for(&ready),
            )
            .unwrap()
            .next;
        let partial = fixtures::report_value(&begun, VerdictValue::Pass).next;
        assert_eq!(partial.state(), State::ValidatingQualityBar);
        let next = partial
            .into_state()
            .adopt_receipt(&fixture.definitions[0], &adoption(&fixture))
            .unwrap();
        let fenced = next.bind(&fixture.definitions[0]).unwrap();
        assert_eq!(
            fenced.current_attempt().unwrap(),
            partial.current_attempt().unwrap()
        );
        assert_eq!(fenced.last_result(), partial.last_result());
        assert_eq!(fenced.state(), partial.state());
        assert_eq!(fenced.receipt(), partial.receipt());
        let (report, facts) = fixtures::report_parts(&fenced, VerdictValue::Pass);
        let owner = fixtures::owner_for(&fenced);
        assert!(
            fenced
                .report(
                    Principal::Actor(owner.authority.evaluator),
                    &fenced.binding(),
                    &owner,
                    report,
                    &facts
                )
                .is_err()
        );
        assert_eq!(partial.into_state().fence(), None);
    }
}

#[test]
fn terminal_results_and_earlier_fences_survive_later_adoptions_exactly() {
    let fixture = Fixture::new(TargetDeclaration::Increment, false, false);
    let ready = fixture.ready(false);
    let begun = ready
        .begin(
            Principal::Actor(EVALUATOR),
            &ready.binding(),
            &fixtures::owner_for(&ready),
        )
        .unwrap()
        .next;
    let terminal = fixtures::report_value(&begun, VerdictValue::Pass)
        .next
        .into_state();
    let token = adoption(&fixture);
    assert_eq!(
        terminal
            .adopt_receipt(&fixture.definitions[0], &token)
            .unwrap(),
        terminal
    );
    let deadline_fenced = ready
        .fence_deadline(
            &ready.binding(),
            &fixture.claim,
            ready.deadline(),
            ready.deadline().at,
            cut(),
        )
        .unwrap();
    assert_eq!(
        deadline_fenced
            .adopt_receipt(&fixture.definitions[0], &token)
            .unwrap(),
        deadline_fenced
    );
    let fenced = begun
        .into_state()
        .adopt_receipt(&fixture.definitions[0], &token)
        .unwrap();
    let mut adopted = fixture
        .claim
        .try_copy(fixture.claim.copy_charge().unwrap())
        .unwrap();
    adopted.apply_receipt_adoption(&token).unwrap();
    let later = adopted
        .prepare_receipt_adoption(
            &adopted.binding(),
            Principal::Actor(ISSUER),
            replacement().fence,
            ReceiptEntitlement {
                holder: EVALUATOR,
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(12),
                    epoch: 5,
                },
            },
            ClaimCut {
                position: SessionSeq(9),
                cause: ContentHash([90; 32]),
            },
        )
        .unwrap();
    assert_eq!(
        fenced
            .adopt_receipt(&fixture.definitions[0], &later)
            .unwrap(),
        fenced
    );
    assert_eq!(
        terminal
            .adopt_receipt(&fixture.definitions[0], &later)
            .unwrap(),
        terminal
    );
    let historical_ready = ready
        .into_state()
        .adopt_receipt(&fixture.definitions[0], &later)
        .unwrap();
    assert_eq!(historical_ready.receipt(), ready.receipt());
    assert_eq!(historical_ready.fence().unwrap().cause, later.cut().cause);
}

#[test]
fn declaration_receipt_and_revision_substitution_refuse_without_mutating_the_source() {
    let fixture = Fixture::new(TargetDeclaration::Increment, false, false);
    let ready = fixture.ready(false).into_state();
    let token = adoption(&fixture);
    for receipt in [
        None,
        Some(ReceiptFence {
            receipt: ReceiptId::from_u128(999),
            epoch: 3,
        }),
        Some(replacement().fence),
    ] {
        let mut changed = ready;
        changed.receipt = receipt;
        assert!(matches!(
            changed.adopt_receipt(&fixture.definitions[0], &token),
            Err(ContractError::StaleReceipt)
        ));
    }
    let mut exhausted = ready;
    exhausted.binding.revision = ObjectRevision(u64::MAX);
    assert!(matches!(
        exhausted.adopt_receipt(&fixture.definitions[0], &token),
        Err(ContractError::Capacity)
    ));
    let mut wrong = fixtures::specification(ValidationMode::Observe, fixtures::programmatic(false));
    wrong.target = TargetDeclaration::Increment;
    wrong.phase = ValidationPhase::Increment;
    wrong.deadline.at += 1;
    let substituted =
        Declaration::new(Principal::Actor(ISSUER), wrong, fixtures::limits()).unwrap();
    assert!(ready.adopt_receipt(&substituted, &token).is_err());
    let mut future_target = ready;
    future_target.target = Target::Increment {
        claim: fixture.claim.binding().next().unwrap(),
        artifact: binding(400),
    };
    assert!(
        future_target
            .adopt_receipt(&fixture.definitions[0], &token)
            .is_err()
    );
    assert_eq!(fixture.claim.binding(), token.binding());
    assert_eq!(ready.fence(), None);
}

#[test]
fn registry_adoption_preserves_old_ordinals_and_reopens_only_new_receipt_increment_targets() {
    let fixture = Fixture::new(TargetDeclaration::Increment, false, false);
    let ready = fixture.ready(false);
    let mut registry = aggregation::RegistrationSet::new(&fixture.claim, 8, 4096).unwrap();
    registry.register(&fixture.claim, &ready, 65536).unwrap();
    registry.seal_increment_targets(&fixture.claim).unwrap();
    let original = registry.try_copy(registry.copy_charge().unwrap()).unwrap();
    let token = adoption(&fixture);
    registry.adopt_receipt(&token).unwrap();
    assert!(!registry.increment_targets_sealed());
    assert!(!registry.is_sealed());
    assert_eq!(registry.rows(), original.rows());
    assert_eq!(registry.max_rows(), original.max_rows());
    assert_eq!(
        registry.retained_heap_bytes().unwrap(),
        original.retained_heap_bytes().unwrap()
    );
    let unchanged = registry.try_copy(registry.copy_charge().unwrap()).unwrap();
    assert!(registry.adopt_receipt(&token).is_err());
    assert_eq!(registry, unchanged);
    let mut claim = fixture
        .claim
        .try_copy(fixture.claim.copy_charge().unwrap())
        .unwrap();
    claim.apply_receipt_adoption(&token).unwrap();
    let current = Evaluation::materialize(
        Principal::Actor(ISSUER),
        &fixture.definitions[0],
        Materialization {
            binding: fixture.definitions[0].binding(),
            target: Target::Increment {
                claim: claim.binding(),
                artifact: binding(401),
            },
            slot_name: None,
            generation: 2,
            receipt: Some(replacement().fence),
        },
    )
    .unwrap();
    registry.register(&claim, &current, 65536).unwrap();
    assert_eq!(registry.rows().len(), 2);
    assert_eq!(registry.rows()[0], original.rows()[0]);
    assert_eq!(registry.rows()[1].receipt(), Some(replacement().fence));
    let mut audit_sealed = original.try_copy(original.copy_charge().unwrap()).unwrap();
    audit_sealed.seal_targets(&fixture.claim).unwrap();
    let sealed_before = audit_sealed
        .try_copy(audit_sealed.copy_charge().unwrap())
        .unwrap();
    assert!(audit_sealed.adopt_receipt(&token).is_err());
    assert_eq!(audit_sealed, sealed_before);
}
