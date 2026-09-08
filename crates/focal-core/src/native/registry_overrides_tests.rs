use super::*;
use crate::native::prepare::Scratch;
use crate::native::report_tests as fixture;
use focal_model::ValidationMode;

std::thread_local! {
    static EXCESS_CAPACITY: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn allocation_capacity(requested: usize) -> Result<usize, NativeError> {
    requested
        .checked_add(EXCESS_CAPACITY.with(std::cell::Cell::get))
        .ok_or(NativeError::Capacity("test registry allocation capacity"))
}

fn with_excess_capacity<T>(extra: usize, action: impl FnOnce() -> T) -> T {
    struct Reset(usize);
    impl Drop for Reset {
        fn drop(&mut self) {
            EXCESS_CAPACITY.with(|value| value.set(self.0));
        }
    }
    let _reset = Reset(EXCESS_CAPACITY.with(|value| value.replace(extra)));
    action()
}

fn owned(ids: &[u128]) -> Vec<(ClaimState, RegistrationSet)> {
    let mut core = fixture::core();
    let mut serial = 100u128;
    for &id in ids {
        fixture::publish(
            &mut core,
            10,
            fixture::creation(
                serial,
                id,
                &[
                    (ValidationMode::Observe, false),
                    (ValidationMode::Required, false),
                ],
                None,
            ),
        );
        serial += 1;
        let binding = core.native_claim(ClaimId::from_u128(id)).unwrap().binding();
        fixture::publish(&mut core, 10, fixture::post(serial, binding));
        serial += 1;
    }
    ids.iter()
        .map(|&id| {
            let id = ClaimId::from_u128(id);
            let claim = core.native_claim(id).unwrap();
            let registry = core.native_registrations(id).unwrap();
            assert_eq!(registry.rows().len(), 2);
            (
                claim.try_copy(claim.retained_bytes().unwrap()).unwrap(),
                registry
                    .try_copy(registry.retained_bytes().unwrap())
                    .unwrap(),
            )
        })
        .collect()
}

fn scratch() -> Scratch {
    Scratch {
        used: 17,
        max: usize::MAX,
    }
}

#[test]
fn empty_and_one_move_complete_native_membership_without_collection_allocation() {
    let mut overrides = RegistryOverrides::new();
    assert!(overrides.is_empty());
    assert_eq!(overrides.len(), 0);
    overrides.check_rows(&[]).unwrap();
    let (claim, mut registry) = owned(&[1]).pop().unwrap();
    registry.seal_targets(&claim).unwrap();
    let original = registry.rows().to_vec();
    let pointer = registry.rows().as_ptr();
    let retained = registry.retained_bytes().unwrap();
    let mut scratch = Scratch { used: 17, max: 17 };
    overrides.insert(&claim, registry, 1, &mut scratch).unwrap();
    assert_eq!(scratch.used, 17);
    assert_eq!(overrides.len(), 1);
    assert!(!overrides.is_empty());
    overrides.check_rows(std::slice::from_ref(&claim)).unwrap();
    let mut moved = overrides.into_iter();
    let (id, registry) = moved.next().unwrap();
    assert!(moved.next().is_none());
    assert_eq!(id, ClaimId(claim.binding().object.0));
    assert_eq!(registry.rows(), original);
    assert_eq!(registry.rows().as_ptr(), pointer);
    assert_eq!(registry.retained_bytes().unwrap(), retained);
    assert!(registry.is_sealed());
    assert!(registry.increment_targets_sealed());
    registry.check(&claim).unwrap();
}

#[test]
fn distinct_native_registries_spill_in_canonical_order_and_keep_their_owned_buffers() {
    let mut overrides = RegistryOverrides::new();
    let mut scratch = scratch();
    let mut claims = Vec::new();
    let mut expected = Vec::new();
    for (claim, mut registry) in owned(&[3, 1, 2]) {
        let id = ClaimId(claim.binding().object.0);
        if id == ClaimId::from_u128(1) {
            registry.seal_increment_targets(&claim).unwrap();
        }
        if id == ClaimId::from_u128(2) {
            registry.seal_targets(&claim).unwrap();
        }
        expected.push((
            id,
            registry.rows().to_vec(),
            registry.rows().as_ptr(),
            registry.is_sealed(),
            registry.increment_targets_sealed(),
        ));
        overrides.insert(&claim, registry, 3, &mut scratch).unwrap();
        claims.push(claim);
    }
    assert!(scratch.used > 17);
    assert_eq!(overrides.len(), 3);
    claims.sort_unstable_by_key(|claim| claim.binding().object);
    expected.sort_unstable_by_key(|row| row.0);
    overrides.check_rows(&claims).unwrap();
    let actual = overrides.into_iter().collect::<Vec<_>>();
    assert_eq!(actual.len(), 3);
    for ((id, registry), (expected_id, rows, pointer, sealed, increments)) in
        actual.iter().zip(expected)
    {
        assert_eq!(*id, expected_id);
        assert_eq!(registry.rows(), rows);
        assert_eq!(registry.rows().as_ptr(), pointer);
        assert_eq!(registry.is_sealed(), sealed);
        assert_eq!(registry.increment_targets_sealed(), increments);
        registry
            .check(&claims[usize::try_from(u128::from_be_bytes(id.0) - 1).unwrap()])
            .unwrap();
    }
}

#[test]
fn duplicate_wrong_owner_and_maximum_refusals_preserve_the_existing_override_and_scratch() {
    for refusal in 0..3 {
        let mut originals = owned(&[1, 2]);
        let (other, other_registry) = originals.pop().unwrap();
        let (claim, registry) = originals.pop().unwrap();
        let original_rows = registry.rows().to_vec();
        let pointer = registry.rows().as_ptr();
        let duplicate = registry
            .try_copy(registry.retained_bytes().unwrap())
            .unwrap();
        let mut overrides = RegistryOverrides::new();
        let mut scratch = scratch();
        overrides.insert(&claim, registry, 2, &mut scratch).unwrap();
        let before = scratch.used;
        let result = match refusal {
            0 => overrides.insert(&claim, duplicate, 2, &mut scratch),
            1 => overrides.insert(&claim, other_registry, 2, &mut scratch),
            _ => overrides.insert(&other, other_registry, 1, &mut scratch),
        };
        assert!(result.is_err());
        assert_eq!(scratch.used, before);
        assert_eq!(overrides.len(), 1);
        overrides.check_rows(std::slice::from_ref(&claim)).unwrap();
        let entries = overrides.into_iter().collect::<Vec<_>>();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, ClaimId::from_u128(1));
        assert_eq!(entries[0].1.rows(), original_rows);
        assert_eq!(entries[0].1.rows().as_ptr(), pointer);
    }
    let (claim, registry) = owned(&[1]).pop().unwrap();
    let mut overrides = RegistryOverrides::new();
    let mut scratch = scratch();
    assert!(overrides.insert(&claim, registry, 0, &mut scratch).is_err());
    assert!(overrides.is_empty());
    assert_eq!(scratch.used, 17);
}

#[test]
fn final_claim_rows_must_be_sorted_unique_and_contain_each_exact_override_owner() {
    let mut values = owned(&[1, 2]);
    let (second, _) = values.pop().unwrap();
    let (first, registry) = values.pop().unwrap();
    let mut overrides = RegistryOverrides::new();
    overrides
        .insert(&first, registry, 2, &mut scratch())
        .unwrap();
    assert!(overrides.check_rows(&[]).is_err());
    let foreign = crate::native::tests::owned_claim_fixture();
    assert_eq!(foreign.binding().object, first.binding().object);
    assert!(overrides.check_rows(&[foreign]).is_err());
    assert!(overrides.check_rows(std::slice::from_ref(&second)).is_err());
    let duplicate = first.try_copy(first.retained_bytes().unwrap()).unwrap();
    assert!(
        overrides
            .check_rows(&[
                first.try_copy(first.retained_bytes().unwrap()).unwrap(),
                duplicate
            ])
            .is_err()
    );
    let reversed = vec![second, first];
    assert!(overrides.check_rows(&reversed).is_err());
    assert!(RegistryOverrides::new().check_rows(&reversed).is_err());
    let mut sorted = reversed;
    sorted.sort_unstable_by_key(|claim| claim.binding().object);
    overrides.check_rows(&sorted).unwrap();
    assert_eq!(overrides.len(), 1);
}

#[test]
fn refused_spill_precharge_does_not_move_one_or_consume_any_scratch() {
    let mut generous = RegistryOverrides::new();
    let mut allowance = scratch();
    let mut baseline_claims = Vec::new();
    for (claim, registry) in owned(&[1, 2]) {
        generous
            .insert(&claim, registry, 2, &mut allowance)
            .unwrap();
        baseline_claims.push(claim);
    }
    let exact = allowance.used;
    assert!(exact > 17);
    generous.check_rows(&baseline_claims).unwrap();
    drop(generous);

    let mut values = owned(&[1, 2]);
    let (second, incoming) = values.pop().unwrap();
    let (first, registry) = values.pop().unwrap();
    let rows = registry.rows().to_vec();
    let pointer = registry.rows().as_ptr();
    let mut overrides = RegistryOverrides::new();
    let mut limited = Scratch {
        used: 17,
        max: exact - 1,
    };
    overrides.insert(&first, registry, 2, &mut limited).unwrap();
    assert!(
        overrides
            .insert(&second, incoming, 2, &mut limited)
            .is_err()
    );
    assert_eq!(limited.used, 17);
    assert_eq!(overrides.len(), 1);
    overrides.check_rows(std::slice::from_ref(&first)).unwrap();
    let (_, retained) = overrides.into_iter().next().unwrap();
    assert_eq!(retained.rows(), rows);
    assert_eq!(retained.rows().as_ptr(), pointer);
}

#[test]
fn excess_actual_spill_or_growth_capacity_refuses_before_moving_existing_memberships() {
    for prior in [1usize, 2] {
        let mut values = owned(&[1, 2, 3]);
        let mut overrides = RegistryOverrides::new();
        let mut allowance = scratch();
        let mut claims = Vec::new();
        let mut expected = Vec::new();
        for (claim, registry) in values.drain(..prior) {
            expected.push((
                ClaimId(claim.binding().object.0),
                registry.rows().to_vec(),
                registry.rows().as_ptr(),
            ));
            overrides
                .insert(&claim, registry, 4, &mut allowance)
                .unwrap();
            claims.push(claim);
        }
        let before = allowance.used;
        let (incoming_claim, incoming) = values.remove(0);
        let retry = incoming
            .try_copy(incoming.retained_bytes().unwrap())
            .unwrap();
        assert!(
            with_excess_capacity(1, || overrides.insert(
                &incoming_claim,
                incoming,
                4,
                &mut allowance
            ))
            .is_err()
        );
        assert_eq!(allowance.used, before);
        assert_eq!(overrides.len(), prior);
        overrides.check_rows(&claims).unwrap();
        // Resetting the injection admits the exact same owner and membership.
        overrides
            .insert(&incoming_claim, retry, 4, &mut allowance)
            .unwrap();
        claims.push(incoming_claim);
        overrides.check_rows(&claims).unwrap();
        let moved = overrides.into_iter().collect::<Vec<_>>();
        assert_eq!(moved.len(), prior + 1);
        for ((id, registry), (expected_id, rows, pointer)) in moved.iter().zip(expected) {
            assert_eq!(*id, expected_id);
            assert_eq!(registry.rows(), rows);
            assert_eq!(registry.rows().as_ptr(), pointer);
        }
    }
}

#[test]
fn received_response_delivery_membership_moves_intact_among_admission_overrides() {
    let (response_claim, response_registry) =
        crate::native::response_tests::registry_override_fixture();
    let response = response_claim.latest_response().unwrap();
    let delivery = response_registry
        .rows()
        .iter()
        .copied()
        .find(|row| {
            matches!(
                row.target(),
                focal_model::lifecycle::validation::Target::Delivery { .. }
            )
        })
        .expect("claimant receipt materialized the actual Delivery member");
    let focal_model::lifecycle::validation::Target::Delivery { response: target } =
        delivery.target()
    else {
        panic!("Delivery target")
    };
    assert_eq!(target.object.0, response.testament.0);
    assert_eq!(target.content, response.content);
    assert_eq!(delivery.generation(), u64::from(response.cycle));
    assert_eq!(delivery.receipt(), Some(response.receipt));
    let rows = response_registry.rows().to_vec();
    let pointer = response_registry.rows().as_ptr();
    let flags = (
        response_registry.is_sealed(),
        response_registry.increment_targets_sealed(),
    );
    let retained = response_registry.retained_bytes().unwrap();

    let mut admission = owned(&[2, 3]);
    let third = admission.pop().unwrap();
    let second = admission.pop().unwrap();
    let mut overrides = RegistryOverrides::new();
    let mut allowance = scratch();
    let mut claims = Vec::new();
    for (claim, registry) in [third, (response_claim, response_registry), second] {
        overrides
            .insert(&claim, registry, 3, &mut allowance)
            .unwrap();
        claims.push(claim);
    }
    claims.sort_unstable_by_key(|claim| claim.binding().object);
    overrides.check_rows(&claims).unwrap();
    assert!(allowance.used > 17);
    let moved = overrides.into_iter().collect::<Vec<_>>();
    assert_eq!(
        moved.iter().map(|row| row.0).collect::<Vec<_>>(),
        [
            ClaimId::from_u128(1),
            ClaimId::from_u128(2),
            ClaimId::from_u128(3)
        ]
    );
    let registry = &moved[0].1;
    assert_eq!(registry.rows(), rows);
    assert_eq!(registry.rows().as_ptr(), pointer);
    assert_eq!(registry.retained_bytes().unwrap(), retained);
    assert_eq!(
        (registry.is_sealed(), registry.increment_targets_sealed()),
        flags
    );
    assert!(registry.rows().contains(&delivery));
    registry.check(&claims[0]).unwrap();
}

#[test]
fn two_authorized_increment_registry_seals_publish_together_after_the_actual_pending_source() {
    use crate::native::*;
    use focal_memory::BudgetLane;
    use focal_model::{ClaimStatus, ContentHash, ReceiptId, SessionSeq};

    let mut core = fixture::core();
    for id in [1u128, 2] {
        fixture::publish(
            &mut core,
            20,
            fixture::creation(id * 10, id, &[(ValidationMode::Observe, false)], None),
        );
        let expected = core.native_claim(ClaimId::from_u128(id)).unwrap().binding();
        fixture::publish(&mut core, 20, fixture::post(id * 10 + 1, expected));
    }
    let expected = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    fixture::publish(
        &mut core,
        30,
        NativeInput {
            request: fixture::request(fixture::SUBJECT, 101),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(701),
            },
        },
    );
    let pinned = core.pin_native(0, 100).unwrap();
    let original_sequence = core.native_sequence();
    let original = [1u128, 2].map(|id| {
        let id = ClaimId::from_u128(id);
        let registry = core.native_registrations(id).unwrap();
        assert!(!registry.increment_targets_sealed());
        (id, registry.rows().to_vec())
    });
    let expected = core.native_claim(ClaimId::from_u128(2)).unwrap().binding();
    let receipt = fixture::prepared(core.prepare_native(
        fixture::context(fixture::SUBJECT, 30),
        NativeInput {
            request: fixture::request(fixture::SUBJECT, 102),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(702),
            },
        },
        &[],
    ));
    let view = View {
        state: &core.state,
        tail: Some(&receipt),
    };
    let mut allowance = Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    };
    let mut rows = allowance.reserve::<ClaimState>(2).unwrap();
    let mut registry = RegistryOverrides::new();
    for id in [2u128, 1] {
        let claim = view.claim(ClaimId::from_u128(id)).unwrap();
        assert_eq!(claim.status(), ClaimStatus::Received);
        let mut authorized = increment_seal::prepare(
            &view,
            fixture::context(fixture::ISSUER, 30),
            claim.binding(),
            core.limits,
            &mut allowance,
        )
        .unwrap();
        assert_eq!(authorized.rows.len(), 1);
        assert_eq!(authorized.registry.len(), 1);
        for (owner, replacement) in authorized.registry {
            assert_eq!(owner, ClaimId::from_u128(id));
            registry
                .insert(
                    &authorized.rows[0],
                    replacement,
                    core.limits.plan_nodes,
                    &mut allowance,
                )
                .unwrap();
        }
        rows.append(&mut authorized.rows);
    }
    assert_eq!(registry.len(), 2);
    let events =
        claim_changes::event_count(&rows, &view, NativeOperation::SealIncrementTargets).unwrap();
    assert_eq!(events, 2);
    let sequence = SessionSeq(receipt.outcome().sequence.0 + 1);
    let outcome = NativeOutcome {
        ledger: view.ledger(),
        invocation: fixture::request(fixture::ISSUER, 103).into(),
        sequence,
        logical_time: 30,
        operation: NativeOperation::SealIncrementTargets,
        // Identity for this isolated internal composition, whose two source
        // authorizations above are genuine native increment-seal preparations.
        intent: ContentHash([103; 32]),
        created: 0,
        changed: 2,
        definitions: 0,
        evaluations: 0,
        artifacts: 0,
        results: 0,
        receipts: 0,
        responses: 0,
        result_testaments: 0,
        events: u32::try_from(events).unwrap(),
    };
    let mut meta = view.meta();
    meta.events += events;
    meta.outcomes += 1;
    meta.logical_time = 30;
    let changes = claim_changes::changes(
        transactions::Plan {
            rows,
            registry,
            created: 0,
        },
        prepare::Extras::new(0, core.limits.preparation_bytes).unwrap(),
        meta,
        outcome,
        &view,
        core.limits,
        core.limits.preparation_bytes,
        &mut allowance,
    )
    .unwrap();
    let range = core
        .state
        .rows
        .prepare_after_with(
            &receipt.range,
            sequence.0,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    let sealed = NativePrepared {
        range,
        outcome,
        writes: mutation::WriteSet::unrecorded(),
    };
    assert_eq!(core.native_sequence(), original_sequence);
    for (id, members) in &original {
        assert_eq!(core.native_registrations(*id).unwrap().rows(), members);
        assert!(
            !core
                .native_registrations(*id)
                .unwrap()
                .increment_targets_sealed()
        );
        assert!(
            !receipt
                .registrations(*id)
                .unwrap()
                .increment_targets_sealed()
        );
        assert_eq!(receipt.registrations(*id).unwrap().rows(), members);
        assert!(
            sealed
                .registrations(*id)
                .unwrap()
                .increment_targets_sealed()
        );
        assert!(!sealed.registrations(*id).unwrap().is_sealed());
        assert_eq!(sealed.registrations(*id).unwrap().rows(), members);
    }
    core.publish_native(receipt).unwrap();
    assert!(original.iter().all(|(id, _)| {
        !core
            .native_registrations(*id)
            .unwrap()
            .increment_targets_sealed()
    }));
    core.publish_native(sealed).unwrap();
    assert_eq!(core.native_sequence(), sequence);
    assert_eq!(
        core.native_outcome(fixture::request(fixture::ISSUER, 103)),
        Some(outcome)
    );
    for (ordinal, (id, members)) in original.iter().enumerate() {
        let claim = core.native_claim(*id).unwrap();
        let registry = core.native_registrations(*id).unwrap();
        assert_eq!(claim.status(), ClaimStatus::Received);
        assert!(registry.increment_targets_sealed());
        assert!(!registry.is_sealed());
        assert_eq!(registry.rows(), members);
        assert_eq!(
            core.native_event(sequence, u32::try_from(ordinal).unwrap())
                .unwrap()
                .fact,
            NativeFact::Registrations {
                claim: claim.binding()
            }
        );
        assert_eq!(
            pinned
                .with_registrations(*id, 0, |registry| (
                    registry.increment_targets_sealed(),
                    registry.rows().to_vec()
                ))
                .unwrap(),
            Some((false, members.clone()))
        );
    }
    assert_eq!(
        pinned
            .with_claim(ClaimId::from_u128(2), 0, ClaimState::status)
            .unwrap(),
        Some(ClaimStatus::Posted)
    );
    core.release_native(&pinned).unwrap();
    drop(pinned);
}
