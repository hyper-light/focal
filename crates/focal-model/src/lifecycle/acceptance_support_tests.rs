use super::*;
use crate::lifecycle::{
    aggregation, claim, creation, graph, ownership, scope, succession, validation as v,
};
use crate::{
    ClaimStatus, ContentHash, HandlerRef, ObjectRevision, RootCommandId, SessionSeq,
    ValidationKind, ValidationPhase, ValidatorId, VerdictValue,
};
use v::tests::{EVALUATOR, ISSUER, binding, programmatic};

fn limits() -> Limits {
    Limits {
        max_slots: 4,
        max_checks: 8,
        max_results: 8,
        max_updates: 8,
    }
}
fn spec(
    id: u128,
    index: u32,
    target: v::TargetDeclaration<'static>,
) -> v::DeclarationSpec<'static> {
    let mut spec = v::tests::specification(ValidationMode::Required, programmatic(false));
    spec.binding = binding(id);
    spec.declaration_index = index;
    spec.target = target;
    spec.phase = match target {
        v::TargetDeclaration::Admission => ValidationPhase::Admission,
        v::TargetDeclaration::Increment => ValidationPhase::Increment,
        _ => ValidationPhase::WholeWork,
    };
    if target == v::TargetDeclaration::Delivery {
        spec.kind = ValidationKind::Receipt;
        spec.program = v::Program::Delivery;
    }
    spec
}
fn build(spec: v::DeclarationSpec<'_>) -> Declaration {
    Declaration::new(Principal::Actor(spec.issuer), spec, v::tests::limits()).unwrap()
}
fn full_definitions() -> Vec<Declaration> {
    vec![
        build(spec(201, 0, v::TargetDeclaration::Delivery)),
        build(spec(202, 1, v::TargetDeclaration::Admission)),
        build(spec(
            203,
            2,
            v::TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "output",
            },
        )),
    ]
}
fn policy(definitions: &[Declaration]) -> AcceptancePolicy {
    let check = [CheckPolicy {
        declaration_index: 2,
        validation: ValidationId::from_u128(203),
        mode: ValidationMode::Required,
    }];
    AcceptancePolicy::new(
        binding(200),
        ISSUER,
        &[SlotPolicy {
            slot: 0,
            missing_declaration_index: 3,
            mode: ValidationMode::Required,
            checks: &check,
        }],
        definitions,
        limits(),
    )
    .unwrap()
}
fn generated(definitions: &[Declaration]) -> claim::ClaimState {
    claim::ClaimState::generate(
        Principal::Actor(ISSUER),
        claim::ClaimDefinition {
            binding: binding(200),
            issuer: ISSUER,
            subject: EVALUATOR,
            deadline: None,
            max_responses: 2,
            created: SessionSeq(1),
            graph: graph::Declaration::empty(),
            lineage: succession::Lineage::root(binding(200), RootCommandId::from_u128(1)).unwrap(),
            acceptance: policy(definitions),
            scope_limits: scope::ScopeLimits {
                scopes: 2,
                roots: 4,
                children: 4,
            },
        },
    )
    .unwrap()
}
fn posted(definitions: &[Declaration]) -> claim::ClaimState {
    let mut claim = generated(definitions);
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Post {
                standing: claim::PostingStanding {
                    binding: claim.binding(),
                    standing: claim::PredicateState::Passed,
                    target: claim::PredicateState::Passed,
                },
            },
        )
        .unwrap();
    claim
}
fn admission<'a>(claim: &claim::ClaimState, definitions: &'a [Declaration]) -> Evaluation<'a> {
    Evaluation::materialize_admission(Principal::Actor(ISSUER), &definitions[1], claim, 1).unwrap()
}
fn begun<'a>(claim: &claim::ClaimState, definitions: &'a [Declaration]) -> Evaluation<'a> {
    let ready = admission(claim, definitions);
    let owner = ready.admission_owner(claim, 1).unwrap();
    ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next
}
struct View<'a> {
    rows: &'a [claim::ClaimState],
}
impl creation::EffectiveClaims for View<'_> {
    fn ledger(&self) -> crate::LedgerId {
        binding(200).ledger
    }
    fn prefix(&self) -> SessionSeq {
        SessionSeq(1)
    }
    fn claim(&self, id: ClaimId) -> Option<&claim::ClaimState> {
        self.rows.iter().find(|row| row.binding().object.0 == id.0)
    }
}
fn cancellation<'a>(view: &'a View<'a>) -> ownership::CancellationPlan<'a> {
    ownership::CancellationPlan::prepare(
        view,
        view.rows[0].binding(),
        Principal::Actor(ISSUER),
        claim::ClaimCut {
            position: SessionSeq(2),
            cause: ContentHash([9; 32]),
        },
        ownership::Limits {
            nodes: 8,
            edge_visits: 16,
            bytes: 65536,
        },
    )
    .unwrap()
}

#[test]
fn complete_definition_set_is_order_independent_and_includes_slot_coverage() {
    let definitions = full_definitions();
    let policy = policy(&definitions);
    policy.check_declarations(&definitions).unwrap();
    policy.check_declarations(definitions.iter().rev()).unwrap();
    assert_eq!(
        policy.check_declaration(&definitions[2]).unwrap().target(),
        ObligationTarget::Slot(0)
    );
}
#[test]
fn omission_extra_and_duplicate_cannot_satisfy_the_manifest() {
    let definitions = full_definitions();
    let policy = policy(&definitions);
    assert!(policy.check_declarations(&definitions[..2]).is_err());
    assert!(
        policy
            .check_declarations([&definitions[0], &definitions[0], &definitions[2]])
            .is_err()
    );
    assert!(
        policy
            .check_declarations([
                &definitions[0],
                &definitions[1],
                &definitions[2],
                &definitions[2]
            ])
            .is_err()
    );
}
#[test]
fn same_public_binding_cannot_substitute_different_semantic_definition() {
    let definitions = full_definitions();
    let policy = policy(&definitions);
    let mut altered = spec(202, 1, v::TargetDeclaration::Admission);
    altered.deadline.at += 1;
    let altered = build(altered);
    assert_eq!(altered.binding(), definitions[1].binding());
    assert_eq!(
        policy.check_declaration(&altered),
        Err(ContractError::InvalidPolicy)
    );
    assert!(
        policy
            .check_declarations([&definitions[0], &altered, &definitions[2]])
            .is_err()
    );
}
#[test]
fn declaration_identity_revision_index_issuer_target_and_mode_are_pinned() {
    let definitions = full_definitions();
    let policy = policy(&definitions);
    let base = spec(202, 1, v::TargetDeclaration::Admission);
    let mut changed = base;
    changed.binding.revision = ObjectRevision(2);
    assert!(policy.check_declaration(&build(changed)).is_err());
    let mut changed = base;
    changed.claim = ClaimId::from_u128(999);
    assert!(policy.check_declaration(&build(changed)).is_err());
    let mut changed = base;
    changed.issuer = EVALUATOR;
    assert!(policy.check_declaration(&build(changed)).is_err());
    let mut changed = base;
    changed.declaration_index = 4;
    assert!(policy.check_declaration(&build(changed)).is_err());
    let mut changed = base;
    changed.mode = ValidationMode::Observe;
    assert!(policy.check_declaration(&build(changed)).is_err());
    let changed = spec(202, 1, v::TargetDeclaration::Increment);
    assert!(policy.check_declaration(&build(changed)).is_err());
}
#[test]
fn admission_materialization_and_begin_require_actual_posted_owner() {
    let definitions = full_definitions();
    let mut claim = generated(&definitions);
    assert!(
        Evaluation::materialize_admission(Principal::Actor(ISSUER), &definitions[1], &claim, 1)
            .is_err()
    );
    claim = posted(&definitions);
    assert!(
        Evaluation::materialize_admission(Principal::Node(ISSUER), &definitions[1], &claim, 1)
            .is_err()
    );
    assert!(
        Evaluation::materialize_admission(Principal::Actor(ISSUER), &definitions[0], &claim, 1)
            .is_err()
    );
    let ready = admission(&claim, &definitions);
    let owner = ready.admission_owner(&claim, 1).unwrap();
    assert_eq!(owner.authority.evaluator, EVALUATOR);
    assert!(
        ready
            .begin(Principal::Actor(ISSUER), &ready.binding(), &owner)
            .is_err()
    );
    let begun = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    assert!(begun.has_begun());
    assert_eq!(begun.state(), State::Validating);
    let expired = ready.admission_owner(&claim, 100).unwrap();
    assert!(
        ready
            .begin(Principal::Actor(EVALUATOR), &ready.binding(), &expired)
            .is_err()
    );
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Cancel {
                cut: claim::ClaimCut {
                    position: SessionSeq(1),
                    cause: ContentHash([1; 32]),
                },
            },
        )
        .unwrap();
    assert!(ready.admission_owner(&claim, 1).is_err());
}
#[test]
fn admission_policy_capability_is_not_invented_by_the_owner_frame() {
    let mut definitions = full_definitions();
    const HANDLER: HandlerRef = HandlerRef {
        id: ValidatorId::from_u128(12),
        version: ContentHash([12; 32]),
        agentic: false,
    };
    const STEPS: [v::HandlerPolicy<'static>; 1] = [v::HandlerPolicy {
        handler: &HANDLER,
        attempts: 1,
        proof_schema: ContentHash([21; 32]),
        diagnostic_schema: ContentHash([22; 32]),
    }];
    let mut changed = spec(202, 1, v::TargetDeclaration::Admission);
    changed.program = v::Program::Programmatic {
        check: v::PhasePolicy {
            evaluator: EVALUATOR,
            definition: ContentHash([40; 32]),
            handlers: &STEPS,
            required_policy: Some(ContentHash([50; 32])),
        },
        quality: None,
    };
    definitions[1] = build(changed);
    let claim = posted(&definitions);
    let ready = admission(&claim, &definitions);
    assert!(matches!(
        ready.admission_owner(&claim, 1),
        Err(ContractError::InvalidPolicy)
    ));
}
#[test]
fn compact_registry_allocates_on_registration_and_has_no_policy_copy() {
    let definitions = full_definitions();
    let claim = posted(&definitions);
    let ready = admission(&claim, &definitions);
    let mut registry = aggregation::RegistrationSet::new(&claim, 1024, 65536).unwrap();
    assert_eq!(registry.retained_heap_bytes().unwrap(), 0);
    assert_eq!(registry.heap_allocations().unwrap(), 0);
    assert!(registry.register(&claim, &ready, 65536).unwrap());
    assert!(!registry.register(&claim, &ready, 65536).unwrap());
    assert_eq!(registry.rows().len(), 1);
    assert_eq!(registry.heap_allocations().unwrap(), 1);
    let advanced = begun(&claim, &definitions).into_state();
    registry.rows()[0]
        .check_state(advanced, &definitions[1])
        .unwrap();
    assert!(
        claim
            .acceptance()
            .checked_registration(&advanced.bind(&definitions[1]).unwrap())
            .is_err()
    );
    assert!(
        registry
            .try_copy(registry.copy_charge().unwrap() - 1)
            .is_err()
    );
    assert_eq!(
        registry,
        registry.try_copy(registry.copy_charge().unwrap()).unwrap()
    );
}
#[test]
fn registry_capacity_and_generation_refusal_leave_membership_unchanged() {
    let definitions = full_definitions();
    let claim = posted(&definitions);
    let ready = admission(&claim, &definitions);
    let mut registry = aggregation::RegistrationSet::new(&claim, 1, 65536).unwrap();
    assert!(
        registry
            .register(&claim, &ready, registry.retained_bytes().unwrap())
            .is_err()
    );
    assert!(registry.rows().is_empty());
    registry.register(&claim, &ready, 65536).unwrap();
    let later =
        Evaluation::materialize_admission(Principal::Actor(ISSUER), &definitions[1], &claim, 2)
            .unwrap();
    assert_eq!(
        registry.register(&claim, &later, 65536),
        Err(ContractError::StaleEvaluation)
    );
    let wrong = claim.acceptance().checked_registration(&later).unwrap();
    assert!(
        wrong
            .check_state(ready.into_state(), &definitions[1])
            .is_err()
    );
    assert_eq!(registry.rows().len(), 1);
}
#[test]
fn registry_binding_rejects_policy_substitution_under_same_content_stamp() {
    let definitions = full_definitions();
    let original = posted(&definitions);
    let registry = aggregation::RegistrationSet::new(&original, 8, 65536).unwrap();
    let mut changed = full_definitions();
    let mut altered = spec(202, 1, v::TargetDeclaration::Admission);
    altered.deadline.at += 1;
    changed[1] = build(altered);
    let altered = posted(&changed);
    assert_eq!(original.binding(), altered.binding());
    assert_eq!(registry.check(&altered), Err(ContractError::InvalidPolicy));
}
#[test]
fn cancellation_fences_ready_and_begun_rows_without_fabricating_outcomes() {
    let definitions = full_definitions();
    let claim = posted(&definitions);
    let ready = admission(&claim, &definitions).into_state();
    let begun = begun(&claim, &definitions).into_state();
    let rows = [claim];
    let view = View { rows: &rows };
    let cancellation = cancellation(&view);
    for original in [ready, begun] {
        let fenced = original
            .cancel(&definitions[1], &cancellation.transitions()[0])
            .unwrap();
        assert_eq!(fenced.state(), original.state());
        assert_eq!(fenced.has_begun(), original.has_begun());
        assert_eq!(fenced.last_result(), original.last_result());
        assert_eq!(
            fenced.binding().revision.0,
            original.binding().revision.0 + 1
        );
        assert_eq!(fenced.fence().unwrap().reason, v::FenceReason::Cancellation);
        assert_eq!(
            fenced
                .cancel(&definitions[1], &cancellation.transitions()[0])
                .unwrap(),
            fenced
        );
    }
}
#[test]
fn cancellation_preserves_terminal_results_but_fences_terminal_parents_begun_checks() {
    let definitions = full_definitions();
    let mut claim = posted(&definitions);
    let begun = begun(&claim, &definitions);
    let terminal = v::tests::report_value(&begun, VerdictValue::Pass)
        .next
        .into_state();
    assert!(terminal.state().is_terminal());
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(ISSUER),
            claim::ClaimIntent::Cancel {
                cut: claim::ClaimCut {
                    position: SessionSeq(1),
                    cause: ContentHash([1; 32]),
                },
            },
        )
        .unwrap();
    let rows = [claim];
    let view = View { rows: &rows };
    let cancellation = cancellation(&view);
    let token = &cancellation.transitions()[0];
    assert!(!token.changes_state());
    assert_eq!(terminal.cancel(&definitions[1], token).unwrap(), terminal);
    assert!(
        begun
            .into_state()
            .cancel(&definitions[1], token)
            .unwrap()
            .fence()
            .is_some()
    );
    assert_eq!(rows[0].status(), ClaimStatus::Cancelled);
}

#[test]
fn supersession_fence_requires_actual_new_predecessor_control() {
    let definitions = full_definitions();
    let claim = posted(&definitions);
    let evaluation = begun(&claim, &definitions).into_state();
    let rows = [claim];
    let view = View { rows: &rows };
    for kind in [
        succession::CorrectionKind::Supersedes,
        succession::CorrectionKind::Amends,
    ] {
        let next = binding(300);
        let proposal = creation::Proposal {
            owner: None,
            definition: claim::ClaimDefinition {
                binding: next,
                issuer: ISSUER,
                subject: EVALUATOR,
                deadline: None,
                max_responses: 2,
                created: SessionSeq(2),
                graph: graph::Declaration::empty(),
                lineage: succession::Lineage::new(
                    next,
                    crate::Cause::Root(RootCommandId::from_u128(1)),
                    &[succession::Correction {
                        kind,
                        predecessor: crate::ObjectRef::claim(next.ledger, ClaimId::from_u128(200)),
                    }],
                    1,
                )
                .unwrap(),
                acceptance: aggregation::acceptance_for(next, ISSUER),
                scope_limits: scope::ScopeLimits {
                    scopes: 2,
                    roots: 4,
                    children: 4,
                },
            },
        };
        let plan = creation::CreationPlan::prepare(
            Principal::Actor(ISSUER),
            vec![proposal],
            &view,
            claim::ClaimCut {
                position: SessionSeq(2),
                cause: ContentHash([4; 32]),
            },
            creation::Limits {
                nodes: 8,
                edge_visits: 16,
                bytes: 1024 * 1024,
            },
        )
        .unwrap();
        assert!(plan.retained_bytes().unwrap() < 1024 * 1024);
        let token = plan.supersession(rows[0].binding()).unwrap();
        if kind == succession::CorrectionKind::Amends {
            assert!(token.is_none());
        } else {
            let token = token.unwrap();
            let fenced = evaluation
                .supersede(&definitions[1], &rows[0], &token)
                .unwrap();
            assert_eq!(fenced.state(), evaluation.state());
            assert_eq!(fenced.fence().unwrap().reason, v::FenceReason::Supersession);
            assert_eq!(fenced.last_result(), evaluation.last_result());
            assert_eq!(rows[0].status(), ClaimStatus::Posted);
        }
    }
}
