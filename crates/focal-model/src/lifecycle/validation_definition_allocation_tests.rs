use super::*;
use crate::lifecycle::memory as bytes;
use crate::lifecycle::validation::tests as fixture;

#[test]
fn prepare_allocates_nothing_and_each_build_buffer_failure_preserves_exact_retry() {
    let Program::Programmatic { check, quality } = fixture::programmatic(true) else {
        panic!("programmatic fixture")
    };
    let spec = DeclarationSpec {
        target: TargetDeclaration::WholeWorkSlot {
            index: 7,
            name: "résultat final",
        },
        program: Program::Programmatic {
            check: PhasePolicy {
                required_policy: Some(ContentHash([91; 32])),
                ..check
            },
            quality: quality.map(|policy| PhasePolicy {
                required_policy: Some(ContentHash([92; 32])),
                ..policy
            }),
        },
        ..fixture::specification(ValidationMode::Required, fixture::programmatic(true))
    };
    let prepare =
        || Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits()).unwrap();
    let plan = bytes::fail_after(0, || {
        let plan = prepare();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        plan
    });
    let fingerprint = plan.intent_fingerprint();
    let charge = plan.construction_charge();
    assert_eq!(plan.construction_heap_allocations().unwrap(), 3);
    let original = plan.build().unwrap();
    assert_eq!(original.intent_fingerprint(), fingerprint);
    assert_eq!(original.retained_bytes().unwrap(), charge);

    // The slot text, check handlers and quality handlers each own one buffer.
    // A refusal after any successful prefix yields no candidate; the same
    // borrowed input can be retried without changing its identity or policy.
    for allowed in 0..3 {
        bytes::fail_after(allowed, || {
            let plan = prepare();
            assert_eq!(plan.intent_fingerprint(), fingerprint);
            assert!(matches!(plan.build(), Err(ContractError::Capacity)));
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
        let retry = bytes::fail_after(3, || {
            let retry = prepare().build().unwrap();
            assert_eq!(bytes::remaining_allocations(), Some(0));
            retry
        });
        assert_eq!(retry.intent_fingerprint(), fingerprint);
        assert_eq!(retry.definition_stamp(), original.definition_stamp());
        assert_eq!(retry.target(), spec.target);
        assert_eq!(retry.deadline(), spec.deadline);
        assert_eq!(retry.retained_bytes().unwrap(), charge);
        assert_eq!(retry.heap_allocations().unwrap(), 3);

        bytes::fail_after(allowed, || {
            assert!(matches!(
                retry.try_copy(charge),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
        assert_eq!(retry.intent_fingerprint(), fingerprint);
        let copied = bytes::fail_after(3, || {
            let copied = retry.try_copy(charge).unwrap();
            assert_eq!(bytes::remaining_allocations(), Some(0));
            copied
        });
        assert_eq!(copied.intent_fingerprint(), fingerprint);
        assert_eq!(copied.retained_bytes().unwrap(), charge);
        assert_eq!(copied.definition_stamp(), original.definition_stamp());
    }
}

#[test]
fn delivery_build_and_copy_need_no_buffer_allocation() {
    let spec = DeclarationSpec {
        kind: ValidationKind::Receipt,
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        ..fixture::specification(ValidationMode::Required, Program::Delivery)
    };
    bytes::fail_after(0, || {
        let plan =
            Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits()).unwrap();
        let fingerprint = plan.intent_fingerprint();
        let charge = plan.construction_charge();
        assert_eq!(plan.construction_heap_bytes().unwrap(), 0);
        assert_eq!(plan.construction_heap_allocations().unwrap(), 0);
        let declaration = plan.build().unwrap();
        let copied = declaration.try_copy(charge).unwrap();
        assert_eq!(declaration.retained_bytes().unwrap(), charge);
        assert_eq!(copied.retained_heap_bytes().unwrap(), 0);
        assert_eq!(copied.heap_allocations().unwrap(), 0);
        assert_eq!(declaration.intent_fingerprint(), fingerprint);
        assert_eq!(copied.intent_fingerprint(), fingerprint);
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
}
