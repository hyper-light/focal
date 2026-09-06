use super::*;
use crate::lifecycle::validation::{self as v, tests as fixture};

fn definition() -> Declaration {
    let spec = fixture::specification(ValidationMode::Required, fixture::programmatic(true));
    Declaration::new(Principal::Actor(spec.issuer), spec, fixture::limits()).unwrap()
}

fn retained() -> Declaration {
    let mut definition = definition();
    let OwnedTarget::WholeWorkSlot { name, .. } = &mut definition.spec.target else {
        panic!("slot fixture")
    };
    name.reserve_exact(32);
    let OwnedProgram::Programmatic {
        check,
        quality: Some(quality),
    } = &mut definition.spec.program
    else {
        panic!("two-phase fixture")
    };
    check.handlers.reserve_exact(8);
    quality.handlers.reserve_exact(8);
    definition
}

#[test]
fn copied_definition_outlives_original_and_continues_both_validation_phases() {
    let original = retained();
    let fingerprint = original.intent_fingerprint();
    let ready = fixture::ready(&original);
    let active = ready
        .begin(
            Principal::Actor(ready.evaluator().unwrap()),
            &ready.binding(),
            &fixture::owner_for(&ready),
        )
        .unwrap()
        .next
        .into_state();
    let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied.definition_stamp(), original.definition_stamp());
    assert_eq!(copied.target(), original.target());
    assert_eq!(copied.intent_fingerprint(), fingerprint);
    let (
        OwnedTarget::WholeWorkSlot { name: before, .. },
        OwnedTarget::WholeWorkSlot { name: after, .. },
    ) = (&original.spec.target, &copied.spec.target)
    else {
        panic!("slots")
    };
    assert_ne!(before.as_ptr(), after.as_ptr());
    assert_ne!(
        original
            .policy(Phase::Programmatic)
            .unwrap()
            .handlers
            .as_ptr(),
        copied
            .policy(Phase::Programmatic)
            .unwrap()
            .handlers
            .as_ptr()
    );
    assert_ne!(
        original.policy(Phase::Quality).unwrap().handlers.as_ptr(),
        copied.policy(Phase::Quality).unwrap().handlers.as_ptr()
    );
    assert!(copied.retained_heap_bytes().unwrap() < original.retained_heap_bytes().unwrap());
    drop(original);
    let active = active.bind(&copied).unwrap();
    let quality = fixture::report_value(&active, VerdictValue::Pass).next;
    assert_eq!(quality.state(), v::State::ValidatingQualityBar);
    let finished = fixture::report_value(&quality, VerdictValue::Pass);
    assert_eq!(finished.next.state(), v::State::Validated);
    assert!(finished.result.unwrap().is_terminal());
    assert_eq!(copied.intent_fingerprint(), fingerprint);
}

#[test]
fn declaration_copy_preflights_before_allocation_and_recovers_from_each_partial_failure() {
    let original = retained();
    let fingerprint = original.intent_fingerprint();
    let charge = original.copy_charge().unwrap();
    assert_eq!(original.copy_heap_allocations().unwrap(), 3);
    assert_eq!(original.heap_allocations().unwrap(), 3);
    bytes::fail_after(9, || {
        assert!(matches!(
            original.try_copy(charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(9));
    });
    for after in 0..3 {
        assert!(
            matches!(
                bytes::fail_after(after, || original.try_copy(charge)),
                Err(ContractError::Capacity)
            ),
            "allocation {after}"
        );
        assert_eq!(original.intent_fingerprint(), fingerprint);
        let copied = original.try_copy(charge).unwrap();
        assert_eq!(copied.intent_fingerprint(), fingerprint);
        assert_eq!(copied.retained_bytes().unwrap(), charge);
        assert_eq!(copied.heap_allocations().unwrap(), 3);
    }
}

#[test]
fn construction_and_copy_charges_cover_delivery_admission_increment_and_agentic_shapes() {
    let Program::Programmatic {
        quality: Some(quality),
        ..
    } = fixture::programmatic(true)
    else {
        panic!("quality")
    };
    let base = fixture::specification(ValidationMode::Required, fixture::programmatic(true));
    let cases = [
        (
            DeclarationSpec {
                target: TargetDeclaration::Delivery,
                kind: ValidationKind::Receipt,
                program: Program::Delivery,
                ..base
            },
            0,
        ),
        (
            DeclarationSpec {
                target: TargetDeclaration::Admission,
                phase: ValidationPhase::Admission,
                program: fixture::programmatic(false),
                ..base
            },
            1,
        ),
        (
            DeclarationSpec {
                target: TargetDeclaration::Increment,
                phase: ValidationPhase::Increment,
                ..base
            },
            2,
        ),
        (
            DeclarationSpec {
                program: Program::Agentic { check: quality },
                ..base
            },
            2,
        ),
        (base, 3),
    ];
    for (spec, allocations) in cases {
        let plan =
            Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits()).unwrap();
        let charge = plan.construction_charge();
        assert_eq!(plan.construction_heap_allocations().unwrap(), allocations);
        assert_eq!(
            plan.construction_heap_bytes().unwrap() + size_of::<Declaration>(),
            charge
        );
        let original = plan.build().unwrap();
        assert_eq!(original.copy_heap_allocations().unwrap(), allocations);
        assert_eq!(original.heap_allocations().unwrap(), allocations);
        assert_eq!(original.copy_charge().unwrap(), charge);
        let copied = original.try_copy(charge).unwrap();
        assert_eq!(copied.intent_fingerprint(), original.intent_fingerprint());
        assert_eq!(copied.target(), original.target());
        assert_eq!(copied.retained_bytes().unwrap(), charge);
        assert_eq!(
            copied.retained_heap_bytes().unwrap(),
            original.retained_heap_bytes().unwrap()
        );
    }
}

#[test]
fn validation_intent_distinguishes_handler_schema_order_and_target_under_same_binding() {
    let base = fixture::specification(ValidationMode::Required, fixture::programmatic(true));
    let original =
        Declaration::new(Principal::Actor(base.issuer), base, fixture::limits()).unwrap();
    let fingerprint = original.intent_fingerprint();
    let ready = fixture::ready(&original).into_state();
    let Program::Programmatic { check, quality } = base.program else {
        panic!("programmatic")
    };
    let mut changed_handler = check.handlers[0].handler.clone();
    changed_handler.version = ContentHash([90; 32]);
    for change in 0..7 {
        let mut steps = check.handlers.to_vec();
        let mut spec = base;
        match change {
            0 => steps[0].handler = &changed_handler,
            1 => steps[0].proof_schema = ContentHash([91; 32]),
            2 => steps[0].diagnostic_schema = ContentHash([92; 32]),
            3 => steps.reverse(),
            4 => {
                spec.target = TargetDeclaration::WholeWorkSlot {
                    index: 0,
                    name: "another-slot",
                }
            }
            5 => spec.deadline.at += 1,
            6 => spec.mode = ValidationMode::Observe,
            _ => unreachable!(),
        }
        spec.program = Program::Programmatic {
            check: PhasePolicy {
                handlers: &steps,
                ..check
            },
            quality,
        };
        let replacement =
            Declaration::new(Principal::Actor(spec.issuer), spec, fixture::limits()).unwrap();
        assert_eq!(replacement.binding(), original.binding());
        assert_ne!(
            replacement.intent_fingerprint(),
            fingerprint,
            "change {change}"
        );
        assert!(
            matches!(
                ready.bind(&replacement),
                Err(ContractError::ContentConflict)
            ),
            "change {change}"
        );
        let copied = replacement
            .try_copy(replacement.copy_charge().unwrap())
            .unwrap();
        assert_eq!(
            copied.intent_fingerprint(),
            replacement.intent_fingerprint()
        );
    }
    assert_eq!(definition().intent_fingerprint(), fingerprint);
    let mut retained = original.try_copy(original.copy_charge().unwrap()).unwrap();
    retained.attempts += 1;
    assert_ne!(retained.intent_fingerprint(), fingerprint);
}
