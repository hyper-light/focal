use super::*;

#[derive(Clone, Copy)]
enum AllocationFault {
    Fail,
    Capacity(usize),
}

std::thread_local! {
    static ALLOCATION_FAULT: std::cell::Cell<Option<AllocationFault>> = const { std::cell::Cell::new(None) };
    static ALLOCATION_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn allocation_capacity(count: usize) -> Result<usize, MemoryError> {
    ALLOCATION_ATTEMPTS.set(ALLOCATION_ATTEMPTS.get() + 1);
    match ALLOCATION_FAULT.get() {
        None => Ok(count),
        Some(AllocationFault::Fail) => Err(MemoryError::AllocationFailed),
        Some(AllocationFault::Capacity(capacity)) => Ok(capacity),
    }
}

fn with_fault<T>(fault: AllocationFault, action: impl FnOnce() -> T) -> T {
    struct Reset {
        fault: Option<AllocationFault>,
        attempts: usize,
    }
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATION_FAULT.set(self.fault);
            ALLOCATION_ATTEMPTS.set(self.attempts);
        }
    }
    let _reset = Reset {
        fault: ALLOCATION_FAULT.replace(Some(fault)),
        attempts: ALLOCATION_ATTEMPTS.replace(0),
    };
    action()
}

fn entry(ordinal: u32, family: NativeCreatedFamily) -> NativeCreatedObject {
    NativeCreatedObject {
        ordinal,
        family,
        schema: 1,
        content: ContentHash([7; 32]),
        requested: ObjectId::from_u128(10 + u128::from(ordinal)),
        resolved: ObjectId::from_u128(20 + u128::from(ordinal)),
    }
}

fn candidate(entries: Vec<NativeCreatedObject>) -> Result<NativeCreationResult, NativeError> {
    let count = entries.len();
    let visits = NativeCreationResult::inspection_visits(count).unwrap();
    let bytes = NativeCreationResult::construction_heap(entries.capacity()).unwrap();
    NativeCreationResult::from_owned(entries, count, visits, bytes)
}

fn assert_contract(result: Result<NativeCreationResult, NativeError>, expected: ContractError) {
    match result {
        Err(NativeError::Contract(actual)) => assert_eq!(actual, expected),
        other => panic!("expected {expected:?}, got {other:?}"),
    }
}

#[test]
fn moving_owned_entries_preserves_addresses_order_and_actual_capacity() {
    let expected = [
        entry(0, NativeCreatedFamily::Claim),
        entry(1, NativeCreatedFamily::Validation),
    ];
    let mut entries = Vec::with_capacity(8);
    entries.extend_from_slice(&expected);
    let pointer = entries.as_ptr();
    let capacity = entries.capacity();
    let bytes = NativeCreationResult::construction_heap(capacity).unwrap();
    with_fault(AllocationFault::Fail, || {
        let result = NativeCreationResult::from_owned(entries, 2, 3, bytes).unwrap();
        assert_eq!(result.entries(), expected);
        assert_eq!(result.entries().as_ptr(), pointer);
        assert_eq!(result.heap_charge().unwrap(), bytes);
        assert_eq!(
            result.retained_heap_bytes().unwrap(),
            capacity * size_of::<NativeCreatedObject>()
        );
        assert_eq!(ALLOCATION_ATTEMPTS.get(), 0);
    });
}

#[test]
fn family_scoped_ids_may_reuse_bytes_but_duplicates_within_a_family_refuse() {
    let first = entry(0, NativeCreatedFamily::Claim);
    let second = NativeCreatedObject {
        ordinal: 1,
        family: NativeCreatedFamily::Validation,
        ..first
    };
    let result = candidate(vec![first, second]).unwrap();
    assert_eq!(result.entries(), [first, second]);

    for family in [NativeCreatedFamily::Claim, NativeCreatedFamily::Validation] {
        let first = entry(0, family);
        let duplicate_requested = NativeCreatedObject {
            requested: first.requested,
            ..entry(1, family)
        };
        let duplicate_resolved = NativeCreatedObject {
            resolved: first.resolved,
            ..entry(1, family)
        };
        for duplicate in [duplicate_requested, duplicate_resolved] {
            assert_contract(
                candidate(vec![first, duplicate]),
                ContractError::ContentConflict,
            );
        }
    }
}

#[test]
fn malformed_ordinals_schemas_and_zero_identities_refuse() {
    for ordinal in [1, u32::MAX] {
        assert_contract(
            candidate(vec![NativeCreatedObject {
                ordinal,
                ..entry(0, NativeCreatedFamily::Claim)
            }]),
            ContractError::InvalidManifest,
        );
    }
    assert_contract(
        candidate(vec![
            entry(0, NativeCreatedFamily::Claim),
            NativeCreatedObject {
                ordinal: 0,
                ..entry(1, NativeCreatedFamily::Validation)
            },
        ]),
        ContractError::InvalidManifest,
    );
    for family in [NativeCreatedFamily::Claim, NativeCreatedFamily::Validation] {
        for schema in [0, 2, u16::MAX] {
            assert_contract(
                candidate(vec![NativeCreatedObject {
                    schema,
                    ..entry(0, family)
                }]),
                ContractError::InvalidPolicy,
            );
        }
        for malformed in [
            NativeCreatedObject {
                requested: ObjectId::from_u128(0),
                ..entry(0, family)
            },
            NativeCreatedObject {
                resolved: ObjectId::from_u128(0),
                ..entry(0, family)
            },
            NativeCreatedObject {
                content: ContentHash([0; 32]),
                ..entry(0, family)
            },
        ] {
            assert_contract(candidate(vec![malformed]), ContractError::InvalidTarget);
        }
    }
}

#[test]
fn object_visit_and_actual_buffer_byte_bounds_are_exact() {
    assert!(matches!(
        NativeCreationResult::from_owned(Vec::new(), 1, 1, usize::MAX),
        Err(NativeError::Capacity("creation result objects"))
    ));
    assert!(matches!(
        NativeCreationResult::from_owned(
            vec![entry(0, NativeCreatedFamily::Claim)],
            0,
            1,
            usize::MAX,
        ),
        Err(NativeError::Capacity("creation result objects"))
    ));
    for count in [1u32, 2, 4] {
        let entries: Vec<_> = (0..count)
            .map(|ordinal| entry(ordinal, NativeCreatedFamily::Claim))
            .collect();
        let count = entries.len();
        let visits = NativeCreationResult::inspection_visits(count).unwrap();
        let bytes = NativeCreationResult::construction_heap(entries.capacity()).unwrap();
        assert!(matches!(
            NativeCreationResult::from_owned(entries.clone(), count, visits - 1, bytes),
            Err(NativeError::Capacity("creation result visits"))
        ));
        let result = NativeCreationResult::from_owned(entries, count, visits, bytes).unwrap();
        assert_eq!(result.entries().len(), count);
    }

    let mut entries = Vec::with_capacity(8);
    entries.push(entry(0, NativeCreatedFamily::Claim));
    let bytes = NativeCreationResult::construction_heap(entries.capacity()).unwrap();
    assert!(matches!(
        NativeCreationResult::from_owned(entries, 1, 1, bytes - 1),
        Err(NativeError::Memory(MemoryError::Capacity { requested, available }))
            if requested == bytes && available == bytes - 1
    ));
    assert_eq!(NativeCreationResult::construction_heap(0).unwrap(), 0);
    assert_eq!(
        NativeCreationResult::construction_heap(1).unwrap(),
        size_of::<NativeCreatedObject>() + ALLOCATION
    );
    assert!(matches!(
        NativeCreationResult::construction_heap(usize::MAX),
        Err(MemoryError::CounterExhausted("creation result heap"))
    ));
}

#[test]
fn inspection_quotes_zero_and_triangular_counts_without_intermediate_overflow() {
    for (count, expected) in [(0, 0), (1, 1), (2, 3), (3, 6), (4, 10), (16, 136)] {
        assert_eq!(
            NativeCreationResult::inspection_visits(count).unwrap(),
            expected
        );
    }
    let count = 1usize << (usize::BITS / 2);
    assert!(count.checked_mul(count + 1).is_none());
    assert_eq!(
        NativeCreationResult::inspection_visits(count).unwrap(),
        (count / 2) * (count + 1)
    );
    for count in [usize::MAX - 1, usize::MAX] {
        assert_eq!(
            NativeCreationResult::inspection_visits(count).unwrap_err(),
            MemoryError::CounterExhausted("creation result visits")
        );
    }
}

#[test]
fn copies_compact_spare_capacity_and_precheck_the_exact_quote() {
    let mut entries = Vec::with_capacity(8);
    entries.extend([
        entry(0, NativeCreatedFamily::Claim),
        entry(1, NativeCreatedFamily::Validation),
    ]);
    let source = candidate(entries).unwrap();
    let expected = source.entries().to_vec();
    let pointer = source.entries().as_ptr();
    let charge = source.heap_charge().unwrap();
    let quote = NativeCreationResult::construction_heap(source.entries().len()).unwrap();
    assert!(quote < charge);
    with_fault(AllocationFault::Fail, || {
        assert!(matches!(
            source.try_copy(quote - 1),
            Err(MemoryError::Capacity { requested, available })
                if requested == quote && available == quote - 1
        ));
        assert_eq!(ALLOCATION_ATTEMPTS.get(), 0);
    });
    let copied = source.try_copy(quote).unwrap();
    assert_eq!(copied.entries(), expected);
    assert_ne!(copied.entries().as_ptr(), pointer);
    assert_eq!(copied.heap_charge().unwrap(), quote);
    assert_eq!(source.entries(), expected);
    assert_eq!(source.entries().as_ptr(), pointer);
    assert_eq!(source.heap_charge().unwrap(), charge);
    drop(source);
    assert_eq!(copied.entries(), expected);
}

#[test]
fn allocation_failure_or_capacity_mismatch_leaves_the_source_unchanged() {
    let source = candidate(vec![
        entry(0, NativeCreatedFamily::Claim),
        entry(1, NativeCreatedFamily::Validation),
    ])
    .unwrap();
    let expected = source.entries().to_vec();
    let pointer = source.entries().as_ptr();
    let charge = source.heap_charge().unwrap();
    for fault in [
        AllocationFault::Fail,
        AllocationFault::Capacity(source.entries().len() + 1),
        AllocationFault::Capacity(source.entries().len() - 1),
    ] {
        with_fault(fault, || {
            let refusal = source.try_copy(usize::MAX).unwrap_err();
            match fault {
                AllocationFault::Fail | AllocationFault::Capacity(1) => {
                    assert_eq!(refusal, MemoryError::AllocationFailed);
                }
                AllocationFault::Capacity(capacity) => assert!(matches!(
                    refusal,
                    MemoryError::Capacity { requested, available }
                        if requested == NativeCreationResult::construction_heap(capacity).unwrap()
                            && available == charge
                )),
            }
            assert_eq!(ALLOCATION_ATTEMPTS.get(), 1);
            assert_eq!(source.entries(), expected);
            assert_eq!(source.entries().as_ptr(), pointer);
            assert_eq!(source.heap_charge().unwrap(), charge);
        });
        assert_eq!(source.try_copy(charge).unwrap().entries(), expected);
    }
}

#[test]
fn inline_owner_moves_without_a_singleton_and_copies_independently() {
    let result = candidate(vec![entry(0, NativeCreatedFamily::Claim)]).unwrap();
    let expected = result.entries().to_vec();
    let pointer = result.entries().as_ptr();
    let charge = result.heap_charge().unwrap();
    let owned = with_fault(AllocationFault::Fail, || {
        let owned = OwnedCreationResult::new(result).unwrap();
        assert_eq!(OwnedCreationResult::container_charge(), 0);
        assert_eq!(
            size_of::<OwnedCreationResult>(),
            size_of::<NativeCreationResult>()
        );
        assert_eq!(owned.get().entries().as_ptr(), pointer);
        assert_eq!(owned.get().entries(), expected);
        assert_eq!(owned.heap_charge().unwrap(), charge);
        assert_eq!(ALLOCATION_ATTEMPTS.get(), 0);
        assert!(matches!(owned.copy(), Err(MemoryError::AllocationFailed)));
        assert_eq!(ALLOCATION_ATTEMPTS.get(), 1);
        assert_eq!(owned.get().entries().as_ptr(), pointer);
        assert_eq!(owned.get().entries(), expected);
        owned
    });
    let copied = owned.copy().unwrap();
    assert_ne!(copied.get().entries().as_ptr(), pointer);
    assert_eq!(copied.heap_charge().unwrap(), charge);
    drop(owned);
    assert_eq!(copied.get().entries(), expected);
}
