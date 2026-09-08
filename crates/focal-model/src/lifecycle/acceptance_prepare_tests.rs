use super::*;
use crate::lifecycle::validation as v;
use crate::{ValidationKind, ValidationPhase};
use v::tests::{ISSUER, binding, programmatic};

fn limits() -> Limits {
    Limits {
        max_slots: 4,
        max_checks: 16,
        max_results: 16,
        max_updates: 8,
    }
}
fn declaration(id: u128, index: u32, target: TargetDeclaration<'static>) -> Declaration {
    let mut spec = v::tests::specification(ValidationMode::Required, programmatic(false));
    spec.binding = binding(id);
    spec.declaration_index = index;
    spec.target = target;
    spec.phase = match target {
        TargetDeclaration::Admission => ValidationPhase::Admission,
        TargetDeclaration::Increment => ValidationPhase::Increment,
        _ => ValidationPhase::WholeWork,
    };
    if target == TargetDeclaration::Delivery {
        spec.kind = ValidationKind::Receipt;
        spec.program = v::Program::Delivery;
    }
    Declaration::new(Principal::Actor(ISSUER), spec, v::tests::limits()).unwrap()
}
fn declarations() -> Vec<Declaration> {
    vec![
        declaration(
            203,
            7,
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output",
            },
        ),
        declaration(201, 2, TargetDeclaration::Delivery),
        declaration(202, 5, TargetDeclaration::Admission),
        declaration(204, 9, TargetDeclaration::Increment),
    ]
}
const CHECK: CheckPolicy = CheckPolicy {
    declaration_index: 7,
    validation: ValidationId::from_u128(203),
    mode: ValidationMode::Required,
};
fn slots(checks: &[CheckPolicy]) -> [SlotPolicy<'_>; 2] {
    [
        SlotPolicy {
            slot: 4,
            missing_declaration_index: 6,
            mode: ValidationMode::Required,
            checks,
        },
        SlotPolicy {
            slot: 99,
            missing_declaration_index: 1,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ]
}
fn prepare<'a>(slots: &'a [SlotPolicy<'a>], declarations: &'a [Declaration]) -> AcceptancePlan<'a> {
    AcceptancePolicy::prepare(binding(200), ISSUER, slots, declarations, limits()).unwrap()
}

#[test]
fn borrowed_preparation_preserves_original_order_independent_policy_and_exact_identity() {
    let mut declarations = declarations();
    let checks = [CHECK];
    let slots = slots(&checks);
    let reference = AcceptancePolicy::from_records(
        binding(200),
        ISSUER,
        &slots,
        declarations.iter().map(record).collect(),
        limits(),
    )
    .unwrap();
    let expected = reference.intent_fingerprint();
    for _ in 0..declarations.len() {
        let plan = bytes::fail_after(0, || prepare(&slots, &declarations));
        assert_eq!(plan.intent_fingerprint(), expected);
        assert_eq!(
            plan.construction_heap_bytes(),
            reference.copy_heap_bytes().unwrap()
        );
        assert_eq!(
            plan.construction_heap_allocations(),
            reference.copy_heap_allocations().unwrap()
        );
        let charge = plan.construction_charge();
        let policy = plan.build(charge).unwrap();
        assert_eq!(policy, reference);
        assert_eq!(policy.retained_bytes().unwrap(), charge);
        assert_eq!(policy.slots().last().unwrap().checks.len(), 0);
        declarations.rotate_left(1);
    }
}

#[test]
fn every_fallible_buffer_refusal_preserves_borrowed_source_and_allows_exact_retry() {
    let declarations = declarations();
    let checks = [CHECK];
    let slots = slots(&checks);
    let plan = prepare(&slots, &declarations);
    let charge = plan.construction_charge();
    let allocations = plan.construction_heap_allocations();
    let intent = plan.intent_fingerprint();
    assert!(matches!(
        plan.build(charge - 1),
        Err(ContractError::Capacity)
    ));
    for at in 0..allocations {
        let result = bytes::fail_after(at, || prepare(&slots, &declarations).build(charge));
        assert!(
            matches!(result, Err(ContractError::Capacity)),
            "allocation {at}"
        );
        assert_eq!(prepare(&slots, &declarations).intent_fingerprint(), intent);
        assert_eq!(
            declarations[0].target(),
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output"
            }
        );
        assert_eq!(slots[0].checks, &[CHECK]);
    }
    let policy =
        bytes::fail_after(allocations, || prepare(&slots, &declarations).build(charge)).unwrap();
    assert_eq!(policy.intent_fingerprint(), intent);
    assert_eq!(policy.heap_allocations().unwrap(), allocations);
}

#[test]
fn full_correspondence_and_required_receipt_are_checked_before_any_owned_allocation() {
    let declarations = declarations();
    let checks = [CHECK];
    let slots = slots(&checks);
    let check = |slots: &[SlotPolicy<'_>], declarations: &[Declaration]| {
        bytes::fail_after(0, || {
            assert!(matches!(
                AcceptancePolicy::prepare(binding(200), ISSUER, slots, declarations, limits()),
                Err(ContractError::InvalidPolicy)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
    };
    // Omitted slot declaration, missing pure Receipt, and an unreferenced
    // external declaration all refuse in the borrowed phase.
    check(&slots, &declarations[1..]);
    let no_receipt = vec![
        declaration(
            203,
            7,
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output",
            },
        ),
        declaration(202, 5, TargetDeclaration::Admission),
    ];
    check(&slots, &no_receipt);
    check(&[], &declarations);
    let mismatched = [CheckPolicy {
        mode: ValidationMode::Observe,
        ..CHECK
    }];
    check(&self::slots(&mismatched), &declarations);
    let collision = [
        SlotPolicy {
            missing_declaration_index: 2,
            ..slots[0]
        },
        slots[1],
    ];
    check(&collision, &declarations);
    let duplicate_index = vec![
        declaration(
            203,
            7,
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output",
            },
        ),
        declaration(201, 2, TargetDeclaration::Delivery),
        declaration(202, 2, TargetDeclaration::Admission),
    ];
    check(&slots, &duplicate_index);
    let duplicate_id = vec![
        declaration(
            203,
            7,
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output",
            },
        ),
        declaration(201, 2, TargetDeclaration::Delivery),
        declaration(201, 5, TargetDeclaration::Admission),
    ];
    check(&slots, &duplicate_id);
}

#[test]
fn no_output_contract_retains_only_its_required_delivery_and_all_bounds_are_checked() {
    let declarations = [declaration(201, 2, TargetDeclaration::Delivery)];
    let plan = prepare(&[], &declarations);
    assert_eq!(plan.construction_heap_allocations(), 1);
    assert_eq!(
        plan.construction_heap_bytes(),
        size_of::<DeclaredObligation>()
    );
    let charge = plan.construction_charge();
    let policy = plan.build(charge).unwrap();
    assert_eq!(policy.slot_count(), 0);
    assert_eq!(
        policy.declarations()[0].target(),
        ObligationTarget::Delivery
    );
    let mut configured = limits();
    configured.max_checks = 0;
    assert!(
        AcceptancePolicy::prepare(binding(200), ISSUER, &[], &declarations, configured).is_err()
    );
    configured = limits();
    configured.max_updates = 0;
    assert!(matches!(
        AcceptancePolicy::prepare(binding(200), ISSUER, &[], &declarations, configured),
        Err(ContractError::Capacity)
    ));
    let zero = ParticipantId::from_u128(0);
    assert!(AcceptancePolicy::prepare(binding(200), zero, &[], &declarations, limits()).is_err());
}
