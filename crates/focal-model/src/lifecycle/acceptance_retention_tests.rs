use super::*;
use crate::lifecycle::validation::tests::{
    binding, limits as definition_limits, owner_for, programmatic, ready, report_value,
    specification,
};
use crate::lifecycle::validation::{self as v, DeclarationSpec, Program};
use crate::{ContentHash, ValidationKind, ValidationPhase, VerdictValue};

fn limits() -> Limits {
    Limits {
        max_slots: 8,
        max_checks: 16,
        max_results: 32,
        max_updates: 8,
    }
}

#[test]
fn acceptance_rejects_alternate_definition_materialization_registration_and_results() {
    let original = DeclarationSpec {
        target: TargetDeclaration::Admission,
        phase: ValidationPhase::Admission,
        ..specification(ValidationMode::Required, programmatic(false))
    };
    let delivery = DeclarationSpec {
        binding: binding(900),
        declaration_index: 900,
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        ..original
    };
    let definitions = [
        Declaration::new(
            Principal::Actor(original.issuer),
            original,
            definition_limits(),
        )
        .unwrap(),
        Declaration::new(
            Principal::Actor(original.issuer),
            delivery,
            definition_limits(),
        )
        .unwrap(),
    ];
    let policy =
        AcceptancePolicy::new(binding(200), original.issuer, &[], &definitions, limits()).unwrap();
    let mut registry = EvaluationRegistry::from_policy(&policy, limits()).unwrap();
    registry.register(&ready(&definitions[0])).unwrap();
    let Program::Programmatic { check, .. } = original.program else {
        panic!()
    };
    let mut reordered = check.handlers.to_vec();
    reordered.reverse();
    let mut proof = check.handlers.to_vec();
    proof[0].proof_schema = ContentHash([222; 32]);
    let mut changed = Vec::new();
    for check in [
        v::PhasePolicy {
            handlers: &reordered,
            ..check
        },
        v::PhasePolicy {
            handlers: &proof,
            ..check
        },
        v::PhasePolicy {
            required_policy: Some(ContentHash([223; 32])),
            ..check
        },
    ] {
        changed.push(DeclarationSpec {
            program: Program::Programmatic {
                check,
                quality: None,
            },
            ..original
        });
    }
    changed.push(DeclarationSpec {
        deadline: crate::Deadline {
            at: 101,
            ..original.deadline
        },
        ..original
    });
    for spec in changed {
        let other =
            Declaration::new(Principal::Actor(spec.issuer), spec, definition_limits()).unwrap();
        let evaluation = ready(&other);
        assert_eq!(other.binding(), definitions[0].binding());
        assert!(registry.register(&evaluation).is_err());
        assert!(
            registry
                .materialize(
                    Principal::Actor(spec.issuer),
                    &other,
                    Materialization {
                        binding: other.binding(),
                        target: evaluation.target(),
                        slot_name: None,
                        generation: evaluation.generation(),
                        receipt: None,
                    }
                )
                .is_err()
        );
        assert_eq!(registry.rows().len(), 1);
        let active = evaluation
            .begin(
                Principal::Actor(evaluation.evaluator().unwrap()),
                &evaluation.binding(),
                &owner_for(&evaluation),
            )
            .unwrap()
            .next;
        let result = report_value(&active, VerdictValue::Pass).result.unwrap();
        assert!(result.is_terminal());
        assert_eq!(
            policy.check_result(result).unwrap_err(),
            ContractError::InvalidPolicy
        );
        assert!(registry.check_result(result).is_err());
    }
    // Reconstruction by value is accepted; no pointer identity or process nonce
    // prevents normal owner eviction/reload of the immutable definition.
    let exact = Declaration::new(
        Principal::Actor(original.issuer),
        original,
        definition_limits(),
    )
    .unwrap();
    let evaluation = ready(&exact);
    registry.register(&evaluation).unwrap();
    let active = evaluation
        .begin(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &owner_for(&evaluation),
        )
        .unwrap()
        .next;
    registry
        .check_result(report_value(&active, VerdictValue::Pass).result.unwrap())
        .unwrap();
}
