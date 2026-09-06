use super::*;
use crate::tests::{ISSUER, WORKER, input, ledger, new_claim, setup};

fn limits(workers: usize) -> EpochLimits {
    EpochLimits {
        max_commands: 32,
        max_workers: workers,
        max_edges: 1024,
        max_bytes: 64 * 1024 * 1024,
        max_trace_entries: 512,
        worker_stack_bytes: 512 * 1024,
    }
}
fn committed(
    base: &Core,
    inputs: Vec<AuthenticatedInput>,
) -> (Vec<PreparedMutation>, Core, Vec<ApplyResult>) {
    let mut oracle = base.clone();
    let mut commands = Vec::new();
    let mut results = Vec::new();
    for input in inputs {
        let prepared = oracle.prepare(&input).unwrap();
        results.push(
            oracle
                .apply_serial(next(oracle.sequence()).unwrap(), prepared.clone())
                .unwrap(),
        );
        commands.push(prepared);
    }
    (commands, oracle, results)
}
fn admissions(count: usize) -> Vec<AuthenticatedInput> {
    (0..count)
        .map(|index| {
            input(
                index as u128 + 1,
                ParticipantId::from_u128(index as u128 + 100),
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1),
                },
            )
        })
        .collect()
}
fn repeated_principal() -> Vec<AuthenticatedInput> {
    (1..=5)
        .map(|epoch| {
            let mut request = input(
                epoch as u128,
                ISSUER,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(epoch),
                },
            );
            request.request_epoch = RequestEpoch(epoch);
            request
        })
        .collect()
}
fn equal(actual: &Core, oracle: &Core) {
    assert_eq!(
        actual.normalized_bytes().unwrap(),
        oracle.normalized_bytes().unwrap()
    );
    assert_eq!(
        actual.encode_checkpoint().unwrap(),
        oracle.encode_checkpoint().unwrap()
    );
}
#[test]
fn independent_rows_execute_on_multiple_workers_and_publish_one_prefix() {
    let base = Core::new(ledger(), Limits::default());
    let (commands, oracle, expected) = committed(&base, admissions(8));
    for workers in [1, 2, 4, 8] {
        for reverse in [false, true] {
            let mut actual = base.clone();
            let before = actual.encode_checkpoint().unwrap();
            let plan = actual
                .plan_epoch(commands.clone(), limits(workers))
                .unwrap();
            let output = plan.execute_order(&actual, reverse).unwrap();
            assert_eq!(
                actual.encode_checkpoint().unwrap(),
                before,
                "execution cannot publish effects"
            );
            assert_eq!(output.report().max_parallel, workers);
            assert_eq!(
                output.report().dependency_edges,
                0,
                "derived row counts must not serialize independent insertions"
            );
            assert!(!output.report().serial_fallback);
            assert_eq!(output.results(), &expected);
            assert_eq!(actual.publish_epoch(output).unwrap(), expected);
            equal(&actual, &oracle);
        }
    }
}
#[test]
fn same_row_reads_select_greatest_previous_index() {
    let base = Core::new(ledger(), Limits::default());
    let mut inputs = repeated_principal();
    inputs.insert(2, admissions(1).remove(0));
    let (commands, oracle, expected) = committed(&base, inputs);
    let mut actual = base.clone();
    let output = actual
        .plan_epoch(commands, limits(4))
        .unwrap()
        .execute_order(&actual, true)
        .unwrap();
    assert!(output.report().dependency_edges > 0);
    assert!(!output.report().serial_fallback);
    assert!(output.report().max_parallel > 1);
    assert_eq!(actual.publish_epoch(output).unwrap(), expected);
    equal(&actual, &oracle);
    assert_eq!(actual.snapshot().epochs[&ISSUER].admitted.len(), 5);
}
#[test]
fn graph_predicates_lifecycle_receipts_and_deltas_match_serial_oracle() {
    let base = setup();
    let id = ClaimId::from_u128(700);
    let inputs = vec![
        input(
            1,
            ISSUER,
            Command::GenerateClaim {
                claim: new_claim(700),
            },
        ),
        input(2, ISSUER, Command::PostClaim { claim: id }),
        input(
            3,
            WORKER,
            Command::AcquireReceipt {
                claim: id,
                receipt: ReceiptId::from_u128(800),
                epoch: 1,
            },
        ),
        input(
            4,
            ISSUER,
            Command::CancelClaim {
                claim: id,
                reason: "retire".into(),
            },
        ),
    ];
    let (commands, oracle, expected) = committed(&base, inputs);
    for workers in [1, 4] {
        let mut actual = base.clone();
        let output = actual
            .plan_epoch(commands.clone(), limits(workers))
            .unwrap()
            .execute(&actual)
            .unwrap();
        assert!(!output.report().serial_fallback);
        assert_eq!(actual.publish_epoch(output).unwrap(), expected);
        equal(&actual, &oracle);
    }
}
#[test]
fn missing_read_or_write_declarations_discard_entire_speculation() {
    let base = Core::new(ledger(), Limits::default());
    let (commands, oracle, expected) = committed(&base, repeated_principal());
    for remove_reads in [false, true] {
        let mut actual = base.clone();
        let mut plan = actual.plan_epoch(commands.clone(), limits(4)).unwrap();
        for index in 0..plan.len() {
            let mut access = plan.accesses(index).unwrap().clone();
            if remove_reads {
                access.reads.clear();
            } else {
                access.writes.clear();
            }
            plan.declare(index, access).unwrap();
        }
        let output = plan.execute_order(&actual, true).unwrap();
        assert!(output.report().serial_fallback);
        assert_eq!(actual.publish_epoch(output).unwrap(), expected);
        equal(&actual, &oracle);
    }
}
#[test]
fn exhausted_tracking_runs_entire_epoch_serially() {
    let base = Core::new(ledger(), Limits::default());
    let (commands, oracle, expected) = committed(&base, admissions(4));
    let mut config = limits(4);
    config.max_trace_entries = 0;
    config.max_edges = 0;
    let mut actual = base.clone();
    let output = actual
        .plan_epoch(commands, config)
        .unwrap()
        .execute(&actual)
        .unwrap();
    assert!(output.report().serial_fallback);
    assert_eq!(output.report().max_parallel, 0);
    assert_eq!(actual.publish_epoch(output).unwrap(), expected);
    equal(&actual, &oracle);
}
#[test]
fn provenance_and_complete_output_are_checked_before_publication() {
    let base = Core::new(ledger(), Limits::default());
    let (commands, _, _) = committed(&base, admissions(3));
    let mut foreign = base.clone();
    foreign.limits.max_objects -= 1;
    let before = foreign.encode_checkpoint().unwrap();
    assert!(matches!(
        base.plan_epoch(commands.clone(), limits(2))
            .unwrap()
            .execute(&foreign),
        Err(EpochError::Provenance)
    ));
    let output = base
        .plan_epoch(commands.clone(), limits(2))
        .unwrap()
        .execute(&base)
        .unwrap();
    assert!(matches!(
        foreign.publish_epoch(output),
        Err(EpochError::Provenance)
    ));
    assert_eq!(foreign.encode_checkpoint().unwrap(), before);
    let mut output = base
        .plan_epoch(commands.clone(), limits(2))
        .unwrap()
        .execute(&base)
        .unwrap();
    output.versions[1].as_mut().unwrap().rows.epochs.clear();
    let mut actual = base.clone();
    assert!(matches!(
        actual.publish_epoch(output),
        Err(EpochError::Provenance)
    ));
    equal(&actual, &base);
    let mut stale = commands;
    stale[1].base = SessionSeq(42);
    assert!(matches!(
        base.plan_epoch(stale, limits(2)),
        Err(EpochError::Provenance)
    ));
}
#[test]
fn workspace_trace_declaration_and_dependency_capacity_fail_without_mutation() {
    let base = Core::new(ledger(), Limits::default());
    let (commands, _, _) = committed(&base, repeated_principal());
    let before = base.encode_checkpoint().unwrap();
    let mut config = limits(4);
    config.max_bytes = 1;
    assert!(matches!(
        base.plan_epoch(commands.clone(), config),
        Err(EpochError::Capacity(_))
    ));
    config = limits(4);
    config.max_commands = 1;
    assert!(matches!(
        base.plan_epoch(commands.clone(), config),
        Err(EpochError::Capacity(_))
    ));
    config = limits(4);
    config.max_edges = 0;
    assert!(matches!(
        base.plan_epoch(commands.clone(), config)
            .unwrap()
            .execute(&base),
        Err(EpochError::Capacity(_))
    ));
    let mut plan = base.plan_epoch(commands, limits(4)).unwrap();
    let mut declaration = plan.accesses(0).unwrap().clone();
    for id in 0..600 {
        declaration
            .reads
            .insert(AccessKey::Claim(ClaimId::from_u128(id)));
    }
    assert!(matches!(
        plan.declare(0, declaration),
        Err(EpochError::Capacity(_))
    ));
    assert_eq!(base.encode_checkpoint().unwrap(), before);
}
#[test]
fn count_reads_conflict_with_births_but_count_increments_do_not() {
    let make = |reads, writes| AccessFootprint {
        ledger: ledger(),
        base: SessionSeq(0),
        reads,
        writes,
        session_exclusive: false,
    };
    let writer = make(
        BTreeSet::new(),
        BTreeSet::from([AccessKey::Count(StateTable::Claims)]),
    );
    let reader = make(
        BTreeSet::from([AccessKey::Count(StateTable::Claims)]),
        BTreeSet::new(),
    );
    assert!(conflict(&writer, &reader));
    assert!(conflict(&reader, &writer));
    assert!(!conflict(&writer, &writer));
    let scan = make(
        BTreeSet::from([AccessKey::Scan(StateTable::Claims)]),
        BTreeSet::new(),
    );
    assert!(conflict(&writer, &scan));
}

#[test]
fn worker_unwind_and_partial_spawn_failure_join_and_never_publish() {
    let mut core = Core::new(ledger(), Limits::default());
    let (commands, oracle, expected) = committed(&core, admissions(4));
    let before = core.encode_checkpoint().unwrap();
    for fault in [TestFault::Panic(1), TestFault::Spawn(2)] {
        let mut plan = core.plan_epoch(commands.clone(), limits(4)).unwrap();
        plan.fault = Some(fault);
        assert!(matches!(plan.execute(&core), Err(EpochError::Worker(_))));
        assert_eq!(core.encode_checkpoint().unwrap(), before);
    }
    let output = core
        .plan_epoch(commands, limits(4))
        .unwrap()
        .execute(&core)
        .unwrap();
    assert_eq!(core.publish_epoch(output).unwrap(), expected);
    equal(&core, &oracle);
}

#[test]
fn singleton_executes_inline_without_stack_allowance_and_contains_unwind() {
    let mut core = Core::new(ledger(), Limits::default());
    let (commands, oracle, expected) = committed(&core, admissions(1));
    let config = EpochLimits {
        max_commands: 1,
        max_workers: 4,
        max_edges: 0,
        max_trace_entries: 64,
        worker_stack_bytes: 64 * 1024 * 1024,
        max_bytes: 128 * 1024,
    };
    let before = core.normalized_bytes().unwrap();
    let mut failed = core.plan_epoch(commands.clone(), config).unwrap();
    failed.fault = Some(TestFault::Panic(0));
    assert!(matches!(failed.execute(&core), Err(EpochError::Worker(_))));
    assert_eq!(core.normalized_bytes().unwrap(), before);
    let output = core
        .plan_epoch(commands, config)
        .unwrap()
        .execute(&core)
        .unwrap();
    assert_eq!(output.report().max_parallel, 1);
    assert_eq!(output.report().waves, 1);
    assert_eq!(core.publish_epoch(output).unwrap(), expected);
    equal(&core, &oracle);
}

#[test]
fn row_workspace_exhaustion_drops_partial_planning() {
    let core = Core::new(ledger(), Limits::default());
    let (commands, _, _) = committed(&core, admissions(2));
    let mut config = limits(1);
    let share = workspace(&commands, config).unwrap();
    // Keep all fixed reservations, leave one byte for each owned command draft.
    config.max_bytes -= share * commands.len() * 2;
    config.max_bytes += commands.len() * 2;
    assert_eq!(workspace(&commands, config).unwrap(), 1);
    let before = core.encode_checkpoint().unwrap();
    assert!(matches!(
        core.plan_epoch(commands, config),
        Err(EpochError::Capacity(_))
    ));
    assert_eq!(core.encode_checkpoint().unwrap(), before);
}
