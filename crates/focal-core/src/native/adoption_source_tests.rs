//! Deliberately corrupt retained native indexes/targets after real owner
//! publications. No corrupted row is admitted through a participant command.
use super::*;
use focal_memory::{Change, Entry};

fn copy_actual_core(fixture: &Fixture) -> Core<NativeState> {
    let source = fixture.owner.committed();
    let mut core = Core::new_native(
        source.ledger(),
        RangeId(91_771),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 65_536,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 32,
            range: RangeConfig {
                max_batch_entries: 512,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    // Test-only snapshot copy: publish the retained rows at their exact source
    // prefix. Ordinary batches still advance exactly one position.
    core.state.rows = RangeStore::new(
        RangeId(91_771),
        source.sequence().0.checked_sub(1).unwrap(),
        core.limits.range,
        core.state.budget.clone(),
    )
    .unwrap();
    let changes: Vec<_> = source
        .source_view()
        .state
        .rows
        .entries()
        .map(|entry| {
            Change::Put(Entry::new(
                entry.key,
                prepare::copy(&entry.value).unwrap(),
                entry.heap_bytes,
            ))
        })
        .collect();
    let copied = core
        .state
        .rows
        .prepare_batch_with(
            source.sequence().0,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(copied).unwrap();
    core
}

fn corrupt(core: &mut Core<NativeState>, mut changes: Vec<Change<Key, Row>>) {
    changes.sort_unstable_by_key(|change| match change {
        Change::Put(entry) => entry.key,
        Change::Delete(key) => *key,
    });
    let changed = core
        .state
        .rows
        .prepare_batch_with(
            core.native_sequence().0 + 1,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(changed).unwrap();
}

fn assert_atomic_adoption_refusal(core: Core<NativeState>) {
    let mut fixture = from_core(core);
    let source = fixture.owner.committed();
    let before_claim = source.claim(CLAIM).unwrap().try_copy(1024 * 1024).unwrap();
    let before_registry = source
        .registrations(CLAIM)
        .unwrap()
        .try_copy(1024 * 1024)
        .unwrap();
    let before_states: Vec<_> = before_registry
        .rows()
        .iter()
        .map(|member| {
            let key = transactions::key_for_registered(CLAIM, *member);
            (key, *source.evaluation(key).unwrap())
        })
        .collect();
    let command = adoption(&fixture);
    refused(&mut fixture, ISSUER, command);
    assert_eq!(fixture.owner.committed().claim(CLAIM), Some(&before_claim));
    assert_eq!(
        fixture.owner.committed().registrations(CLAIM),
        Some(&before_registry)
    );
    for (key, state) in before_states {
        assert_eq!(fixture.owner.committed().evaluation(key), Some(&state));
    }
    assert!(
        fixture
            .owner
            .committed()
            .receipt(REPLACEMENT_RECEIPT)
            .is_none()
    );
}

fn assert_corrupt_cycle_reconstruction_refusal(core: Core<NativeState>, cycle: NativeCycleKey) {
    let stats = core.native_stats();
    let budget = core.state.budget.clone();
    let before_budget = budget.stats();
    let source = core.native_claim(CLAIM).unwrap();
    let claim = source.try_copy(source.retained_bytes().unwrap()).unwrap();
    let registry = core
        .native_registrations(CLAIM)
        .unwrap()
        .try_copy(1024 * 1024)
        .unwrap();
    let states: Vec<_> = registry
        .rows()
        .iter()
        .map(|member| {
            let key = transactions::key_for_registered(CLAIM, *member);
            (key, *core.native_evaluation(key).unwrap())
        })
        .collect();
    let deliveries: Vec<_> = core
        .state
        .rows
        .entries()
        .filter_map(|entry| match &entry.value {
            Row::DeliveryResult(row) => Some((entry.key, *row.get().unwrap())),
            _ => None,
        })
        .collect();
    let original = core.native_response(RESPONSE).unwrap();
    let response = original
        .try_copy(original.retained_bytes().unwrap())
        .unwrap();
    let original_cycle = match core.state.rows.get(&Key::Cycle(cycle)) {
        Some(Row::Cycle(row)) => (
            row.work_head,
            row.work_count,
            row.diagnostic_head,
            row.diagnostic_count,
            row.response,
        ),
        _ => panic!("original corrupt cycle"),
    };
    let refusal = NativeOwner::new(core).unwrap_err();
    assert!(matches!(
        refusal.error,
        NativeOwnerError::Native(NativeError::Contract(ContractError::InvalidManifest))
    ));
    assert_eq!(refusal.core.native_stats(), stats);
    assert_eq!(budget.stats(), before_budget);
    assert_eq!(refusal.core.native_claim(CLAIM), Some(&claim));
    assert_eq!(refusal.core.native_registrations(CLAIM), Some(&registry));
    assert_eq!(refusal.core.native_response(RESPONSE), Some(&response));
    for (key, state) in states {
        assert_eq!(refusal.core.native_evaluation(key), Some(&state));
    }
    for (key, result) in deliveries {
        assert_eq!(
            response_reads::as_delivery(refusal.core.state.rows.get(&key)),
            Some(&result)
        );
    }
    let Some(Row::Cycle(row)) = refusal.core.state.rows.get(&Key::Cycle(cycle)) else {
        panic!("retained corrupt cycle")
    };
    assert_eq!(
        (
            row.work_head,
            row.work_count,
            row.diagnostic_head,
            row.diagnostic_count,
            row.response
        ),
        original_cycle
    );
    assert!(
        refusal
            .core
            .state
            .rows
            .get(&Key::Receipt(REPLACEMENT_RECEIPT))
            .is_none()
    );
}

#[test]
fn adoption_rejects_corrupted_delivery_cycle_link_without_changing_terminal_receipt_result() {
    let mut fixture = Fixture::new();
    complete_response(&mut fixture, 900, 801);
    let identity = fixture
        .owner
        .committed()
        .response(RESPONSE)
        .unwrap()
        .identity();
    let key = NativeCycleKey {
        claim: CLAIM,
        receipt: identity.receipt.receipt,
        epoch: identity.receipt.epoch,
        cycle: identity.cycle,
    };
    // Prove this actual Received source accepts responsibility transfer before
    // independently substituting its native closed-cycle association.
    let mut healthy = from_core(copy_actual_core(&fixture));
    let command = adoption(&healthy);
    let NativeStaging::Prepared { candidate, .. } = healthy.stage(ISSUER, command).unwrap() else {
        panic!("fresh adoption")
    };
    healthy.owner.discard_from(candidate).unwrap();
    for replacement in [None, Some(TestamentId::from_u128(999))] {
        let mut core = copy_actual_core(&fixture);
        let mut cycle = match core.state.rows.get(&Key::Cycle(key)) {
            Some(Row::Cycle(cycle)) => *cycle,
            _ => panic!("actual cycle"),
        };
        cycle.response = replacement;
        corrupt(
            &mut core,
            vec![Change::Put(Entry::new(
                Key::Cycle(key),
                Row::Cycle(cycle),
                0,
            ))],
        );
        // Respondent reconstruction authenticates every actual response cycle
        // before accepting responsibility, so this corruption is now refused
        // before an adoption command can be admitted.
        assert_corrupt_cycle_reconstruction_refusal(core, key);
    }
}

#[test]
fn adoption_rejects_native_missing_target_substitution_when_actual_response_contains_the_slot() {
    let mut fixture = checked_slot_fixture_with_visits(ValidationMode::Required, 65_536);
    complete_response(&mut fixture, 900, 801);
    let mut core = copy_actual_core(&fixture);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let claim = view.claim(CLAIM).unwrap();
    let actual = view.owned_claim(CLAIM).unwrap().registrations().unwrap();
    let response = response_reads::as_response_record(view.get(Key::Response(RESPONSE)))
        .unwrap()
        .response();
    assert!(response.manifest().iter().any(|row| row.slot == 0));
    let mut registry = RegistrationSet::new(claim, actual.max_rows(), 1024 * 1024).unwrap();
    let mut substituted = None;
    let mut original_key = None;
    for member in actual.rows() {
        let key = transactions::key_for_registered(CLAIM, *member);
        let definition = view.definition(key.validation).unwrap();
        let target = match member.target() {
            validation::Target::Artifact { response, slot, .. } => {
                original_key = Some(key);
                validation::Target::MissingSlot { response, slot }
            }
            target => target,
        };
        let ready = validation::Evaluation::materialize(
            Principal::Actor(ISSUER),
            definition,
            validation::Materialization {
                binding: member.binding(),
                target,
                slot_name: match definition.target() {
                    validation::TargetDeclaration::WholeWorkSlot { name, .. } => Some(name),
                    _ => None,
                },
                generation: member.generation(),
                receipt: member.receipt(),
            },
        )
        .unwrap();
        registry.register(claim, &ready, 1024 * 1024).unwrap();
        if matches!(target, validation::Target::MissingSlot { .. }) {
            substituted = Some(ready.into_state());
        }
    }
    let state = substituted.unwrap();
    let next_key = EvaluationKey::of(CLAIM, &state);
    let claim = claim.try_copy(claim.copy_charge().unwrap()).unwrap();
    let claim_row = OwnedClaim::new(claim, registry).unwrap();
    let claim_heap = claim_row.heap_charge().unwrap();
    let evaluation = OwnedEvaluation::new(state).unwrap();
    let evaluation_heap = evaluation.heap_charge().unwrap();
    corrupt(
        &mut core,
        vec![
            Change::Put(Entry::new(
                Key::Claim(CLAIM),
                Row::Claim(claim_row),
                claim_heap,
            )),
            Change::Delete(Key::Evaluation(original_key.unwrap())),
            Change::Put(Entry::new(
                Key::Evaluation(next_key),
                Row::Evaluation(evaluation),
                evaluation_heap,
            )),
        ],
    );
    assert_atomic_adoption_refusal(core);
}
