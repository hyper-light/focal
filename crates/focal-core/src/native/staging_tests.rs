use super::*;
use focal_model::{
    Deadline, HandlerRef, ObjectId, ObjectRevision, ParticipantId, RequestEpoch, RequestId,
    SessionId, TenantId, TimerId, ValidationKind, ValidationMode, ValidationPhase, ValidatorId,
};

fn binding(id: u128) -> Binding {
    Binding {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        object: ObjectId::from_u128(id),
        content: ContentHash([31; 32]),
        revision: ObjectRevision(1),
    }
}

// Cheap payloads isolate descriptor staging from independent singleton charges.
fn extra(id: u128) -> Extra {
    let binding = binding(id);
    Extra {
        key: Key::Definition(ValidationId(binding.object.0)),
        row: Row::Meta(Meta {
            definitions: usize::try_from(id).unwrap(),
            ..Meta::default()
        }),
        heap: 0,
        fact: Some(NativeFact::Definition {
            binding,
            claim: ClaimId::from_u128(3),
            index: u32::try_from(id).unwrap(),
            intent: ContentHash([41; 32]),
        }),
    }
}

fn assert_extra(row: &Extra, id: u128) {
    let expected = extra(id);
    assert_eq!(row.key, expected.key);
    assert_eq!(row.fact, expected.fact);
    assert_eq!(row.heap, 0);
    match row.row {
        Row::Meta(meta) => assert_eq!(meta.definitions, usize::try_from(id).unwrap()),
        _ => panic!("staged payload changed"),
    }
}

#[test]
fn extras_empty_descriptor_set_has_no_reserved_heap() {
    let max = 4096;
    let extras = Extras::new(max, array::<Extra>(max).unwrap() * 2).unwrap();
    assert!(extras.rows.is_empty());
    assert_eq!(extras.rows.capacity(), 0);

    let mut disabled = Extras::new(0, 0).unwrap();
    assert!(matches!(
        disabled.push(extra(1)),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(disabled.rows.capacity(), 0);
}

#[test]
fn insufficient_replacement_peak_preserves_facts_across_refusal_and_retry() {
    // Enough for the final two-element buffer, but not for old and replacement
    // buffers to coexist. This must fail before the old buffer is moved out.
    let mut extras = Extras::new(2, array::<Extra>(2).unwrap()).unwrap();
    extras.push(extra(1)).unwrap();
    let pointer = extras.rows.as_ptr();
    let capacity = extras.rows.capacity();
    assert_eq!(capacity, 1);
    for _ in 0..2 {
        assert!(matches!(
            extras.push(extra(2)),
            Err(NativeError::Capacity(_))
        ));
        assert_eq!(extras.rows.as_ptr(), pointer);
        assert_eq!(extras.rows.capacity(), capacity);
        assert_eq!(extras.rows.len(), 1);
        assert_extra(&extras.rows[0], 1);
    }
    // Model a new owner reservation granting the missing transient headroom.
    extras.allowance = array::<Extra>(capacity).unwrap() + array::<Extra>(2).unwrap();
    extras.push(extra(2)).unwrap();
    assert_eq!(extras.rows.len(), 2);
    assert_extra(&extras.rows[0], 1);
    assert_extra(&extras.rows[1], 2);
    let pointer = extras.rows.as_ptr();
    assert!(matches!(
        extras.push(extra(3)),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(extras.rows.as_ptr(), pointer);
    assert_eq!(extras.rows.len(), 2);
    assert_extra(&extras.rows[0], 1);
    assert_extra(&extras.rows[1], 2);
}

#[test]
fn conflicting_descriptor_cannot_replace_fact_or_trigger_growth() {
    let mut extras = Extras::new(3, array::<Extra>(3).unwrap() * 2).unwrap();
    extras.push(extra(1)).unwrap();
    let pointer = extras.rows.as_ptr();
    let mut conflicting = extra(1);
    conflicting.fact = extra(2).fact;
    assert!(matches!(
        extras.push(conflicting),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
    assert_eq!(extras.rows.as_ptr(), pointer);
    assert_eq!(extras.rows.len(), 1);
    assert_extra(&extras.rows[0], 1);
    extras.push(extra(2)).unwrap();
    extras.push(extra(3)).unwrap();
    assert_extra(&extras.rows[0], 1);
    assert_extra(&extras.rows[1], 2);
    assert_extra(&extras.rows[2], 3);
    assert_eq!(extras.rows.capacity(), 3);
}

#[test]
fn scratch_refuses_missing_bytes_and_charges_actual_coexisting_capacities() {
    let original = 37;
    let requested = array::<Binding>(7).unwrap();
    let mut scratch = Scratch {
        used: original,
        max: original + requested - 1,
    };
    assert!(matches!(
        scratch.reserve::<Binding>(7),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(scratch.used, original);

    scratch.max = original + requested * 4;
    let first = scratch.reserve::<Binding>(7).unwrap();
    let second = scratch.reserve::<Binding>(7).unwrap();
    assert!(first.capacity() >= 7);
    assert!(second.capacity() >= 7);
    assert_eq!(
        scratch.used,
        original
            + array::<Binding>(first.capacity()).unwrap()
            + array::<Binding>(second.capacity()).unwrap()
    );
    let charged = scratch.used;
    scratch.max = charged;
    assert!(matches!(
        scratch.reserve::<Binding>(1),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(scratch.used, charged);
    assert!(first.is_empty());
    assert!(second.is_empty());
}

fn declaration() -> validation::Declaration {
    let actor = ParticipantId::from_u128(5);
    let handler = HandlerRef {
        id: ValidatorId::from_u128(7),
        version: ContentHash([7; 32]),
        agentic: false,
    };
    validation::Declaration::new(
        Principal::Actor(actor),
        validation::DeclarationSpec {
            binding: binding(4),
            claim: ClaimId::from_u128(3),
            issuer: actor,
            declaration_index: 0,
            kind: ValidationKind::Inspection,
            phase: ValidationPhase::Admission,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Admission,
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: actor,
                    definition: ContentHash([8; 32]),
                    required_policy: None,
                    handlers: &[validation::HandlerPolicy {
                        handler: &handler,
                        attempts: 1,
                        proof_schema: ContentHash([9; 32]),
                        diagnostic_schema: ContentHash([10; 32]),
                    }],
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(6),
                generation: 1,
                at: 100,
            },
        },
        validation::Limits {
            handlers: 1,
            attempts: 1,
            slot_bytes: 16,
        },
    )
    .unwrap()
}

#[test]
fn evaluation_requires_entire_singleton_charge_before_staging_any_fact() {
    let declaration = declaration();
    let state = validation::Evaluation::materialize(
        Principal::Actor(declaration.issuer()),
        &declaration,
        validation::Materialization {
            binding: declaration.binding(),
            target: validation::Target::Admission { claim: binding(3) },
            slot_name: None,
            generation: 1,
            receipt: None,
        },
    )
    .unwrap()
    .into_state();
    let mut extras = Extras::new(1, array::<Extra>(1).unwrap() * 2).unwrap();
    let mut scratch = Scratch {
        used: 19,
        max: 19 + OwnedEvaluation::container_charge() - 1,
    };
    for _ in 0..2 {
        assert!(matches!(
            extras.evaluation(
                ClaimId::from_u128(3),
                &declaration,
                None,
                state,
                &mut scratch
            ),
            Err(NativeError::Capacity(_))
        ));
        assert_eq!(scratch.used, 19);
        assert!(extras.rows.is_empty());
        assert_eq!(extras.rows.capacity(), 0);
    }
    scratch.max += 1;
    extras
        .evaluation(
            ClaimId::from_u128(3),
            &declaration,
            None,
            state,
            &mut scratch,
        )
        .unwrap();
    assert_eq!(scratch.used, scratch.max);
    assert_eq!(extras.rows.len(), 1);
    assert_eq!(extras.rows[0].heap, OwnedEvaluation::container_charge());
    match &extras.rows[0].row {
        Row::Evaluation(row) => assert_eq!(row.get(), Some(&state)),
        _ => panic!("evaluation singleton missing"),
    }
    assert!(matches!(
        extras.rows[0].fact,
        Some(NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Materialized,
            before: None,
            state: validation::State::Ready,
            attempt: None,
            fence: None,
            ..
        })
    ));
}

#[test]
fn impossible_capacities_fail_before_changing_scratch_or_allocating_rows() {
    let mut scratch = Scratch {
        used: 1,
        max: usize::MAX,
    };
    assert!(matches!(
        scratch.charge(usize::MAX),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(scratch.used, 1);
    assert!(matches!(
        scratch.reserve::<Extra>(usize::MAX),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(scratch.used, 1);
    assert!(matches!(
        Extras::new(usize::MAX, usize::MAX),
        Err(NativeError::Capacity(_))
    ));
    assert!(matches!(
        containers(usize::MAX),
        Err(NativeError::Capacity(_))
    ));
}

#[test]
fn owner_reserves_descriptors_and_containers_before_entering_transaction() {
    let budget = MemoryBudget::new(16 * 1024 * 1024, 0).unwrap();
    let limits = NativeLimits {
        range: RangeConfig {
            max_batch_entries: 16,
            ..RangeConfig::default()
        },
        plan_nodes: 4,
        preparation_bytes: 8192,
        ..NativeLimits::default()
    };
    let core = Core::new_native(binding(3).ledger, RangeId(83), limits, budget.clone()).unwrap();
    let initial = budget.stats();
    let descriptors = array::<Extra>(limits.range.max_batch_entries / 2).unwrap() * 2;
    let changes = array::<Change<Key, Row>>(limits.range.max_batch_entries).unwrap()
        + array::<claim_changes::History>(limits.plan_nodes).unwrap();
    let entire_charge = limits.preparation_bytes
        + descriptors
        + changes
        + containers(limits.plan_nodes).unwrap()
        + event_containers(limits.range.max_batch_entries).unwrap();
    // Leave every descriptor and all but one container byte available. No
    // candidate may enter semantic execution under this incomplete reservation.
    let pressure = budget
        .reserve(
            BudgetKind::Query,
            BudgetLane::Ordinary,
            initial.limit - initial.used - (entire_charge - 1),
        )
        .unwrap();
    let constrained = budget.stats();
    let request = RequestKey {
        principal: ParticipantId::from_u128(5),
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(99),
    };
    let input = || NativeInput {
        request,
        command: NativeCommand::Post {
            expected: binding(3),
        },
    };
    let context = NativeContext {
        principal: Principal::Actor(request.principal),
        logical_time: 0,
    };
    assert!(matches!(
        core.prepare_native(context, input(), &[]),
        Err(NativeError::Memory(MemoryError::Capacity { requested, available }))
            if requested == entire_charge && available == entire_charge - 1
    ));
    assert_eq!(budget.stats(), constrained);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_outcome(request).is_none());
    drop(pressure);
    // Adequate reservation reaches the real absent-target check. Its refusal
    // releases the entire temporary permit and retains no request outcome.
    assert!(matches!(
        core.prepare_native(context, input(), &[]),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
    assert_eq!(budget.stats(), initial);
    assert!(core.native_outcome(request).is_none());
}
