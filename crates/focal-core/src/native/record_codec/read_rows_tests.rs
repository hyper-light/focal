use super::*;
use crate::native::report_tests as fixture;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::{RequestEpoch, RequestId, ValidationMode};

fn ledger() -> LedgerId {
    fixture::binding(1).ledger
}
fn claim(id: u128) -> ClaimId {
    ClaimId::from_u128(id)
}
fn monitor(id: u128) -> MonitorId {
    MonitorId::from_u128(id)
}
fn artifact(id: u128) -> ArtifactId {
    ArtifactId::from_u128(id)
}
fn testament(id: u128) -> TestamentId {
    TestamentId::from_u128(id)
}
fn cycle_key(epoch: u64) -> NativeCycleKey {
    NativeCycleKey {
        claim: claim(1),
        receipt: ReceiptId::from_u128(u128::from(epoch)),
        epoch,
        cycle: 1,
    }
}
fn outcome(invocation: NativeInvocation, operation: NativeOperation) -> NativeOutcome {
    NativeOutcome {
        ledger: ledger(),
        invocation,
        sequence: SessionSeq(9),
        logical_time: 0,
        operation,
        intent: ContentHash([9; 32]),
        created: 0,
        changed: 1,
        definitions: 0,
        evaluations: 0,
        artifacts: 0,
        results: 0,
        receipts: 0,
        responses: 0,
        result_testaments: 0,
        events: 1,
    }
}
fn encode(row: &Row) -> Vec<u8> {
    let mut size = CountingSink::new(usize::MAX, usize::MAX);
    rows::value(&mut size, row, ledger()).unwrap();
    let mut bytes = vec![0; size.len()];
    let mut sink = SliceSink::new(&mut bytes, usize::MAX);
    rows::value(&mut sink, row, ledger()).unwrap();
    sink.finish().unwrap();
    bytes
}
fn values() -> Vec<(Key, Row)> {
    let request = fixture::request(fixture::ISSUER, 1).into();
    vec![
        (
            Key::IncomingHead(claim(1)),
            Row::IncomingHead(incoming_graph::IncomingHead {
                head: None,
                count: 0,
            }),
        ),
        (
            Key::IncomingHead(claim(2)),
            Row::IncomingHead(incoming_graph::IncomingHead {
                head: Some(claim(1)),
                count: 1,
            }),
        ),
        (
            Key::IncomingLink(claim(1), claim(1)),
            Row::IncomingLink(incoming_graph::IncomingLink { next: None }),
        ),
        (
            Key::IncomingLink(claim(1), claim(2)),
            Row::IncomingLink(incoming_graph::IncomingLink {
                next: Some(claim(3)),
            }),
        ),
        (
            Key::Monitor(monitor(1)),
            Row::Monitor(monitor_index::MonitorAllocation {
                owner: fixture::binding(1),
                registered: SessionSeq(1),
                deadline: Deadline {
                    timer: TimerId::from_u128(1),
                    generation: 1,
                    at: 0,
                },
            }),
        ),
        (
            Key::MonitorHead(claim(1)),
            Row::MonitorHead(monitor_index::MonitorHead {
                head: None,
                count: 0,
            }),
        ),
        (
            Key::MonitorHead(claim(2)),
            Row::MonitorHead(monitor_index::MonitorHead {
                head: Some(monitor(1)),
                count: 1,
            }),
        ),
        (
            Key::MonitorLink(claim(1), monitor(1)),
            Row::MonitorLink(None),
        ),
        (
            Key::MonitorLink(claim(1), monitor(2)),
            Row::MonitorLink(Some(monitor_index::MonitorLink {
                owner: claim(2),
                registered: SessionSeq(1),
                stamp: SessionSeq(3),
                previous: Some(monitor(1)),
                next: Some(monitor(3)),
            })),
        ),
        (
            Key::Meta,
            Row::Meta(Meta {
                claims: 1,
                outcomes: 2,
                events: 3,
                definitions: 4,
                evaluations: 5,
                artifacts: 6,
                results: 7,
                receipts: 8,
                responses: 9,
                result_testaments: 10,
                monitors: 11,
                monitor_links: 12,
                creation_results: 13,
                legacy: 14,
                logical_time: u64::MAX,
            }),
        ),
        (
            Key::ArtifactIdentity(ContentHash([1; 32])),
            Row::ArtifactIdentity(artifact(1)),
        ),
        (
            Key::Receipt(ReceiptId::from_u128(1)),
            Row::Receipt(NativeReceipt {
                claim: claim(1),
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(1),
                    epoch: 1,
                },
                holder: fixture::SUBJECT,
                acquired: SessionSeq(1),
            }),
        ),
        (Key::Cycle(cycle_key(1)), Row::Cycle(NativeCycle::default())),
        (
            Key::Cycle(cycle_key(2)),
            Row::Cycle(NativeCycle {
                work_head: Some(artifact(1)),
                work_count: 1,
                diagnostic_head: Some(artifact(2)),
                diagnostic_count: 2,
                response: Some(testament(1)),
            }),
        ),
        (
            Key::RetiredCycleHead(claim(1)),
            Row::RetiredCycleHead(RetiredCycleHead {
                head: None,
                count: 0,
                work_count: 0,
            }),
        ),
        (
            Key::RetiredCycleHead(claim(1)),
            Row::RetiredCycleHead(RetiredCycleHead {
                head: Some(cycle_key(2)),
                count: 2,
                work_count: 0,
            }),
        ),
        (
            Key::RetiredCycle(cycle_key(1)),
            Row::RetiredCycle(RetiredCycle {
                holder: fixture::SUBJECT,
                next: None,
            }),
        ),
        (
            Key::RetiredCycle(cycle_key(2)),
            Row::RetiredCycle(RetiredCycle {
                holder: fixture::SUBJECT,
                next: Some(cycle_key(1)),
            }),
        ),
        (Key::WorkSlot(cycle_key(1), 0), Row::WorkSlot(artifact(1))),
        (
            Key::ClaimResultTestament(claim(1)),
            Row::ClaimResultTestament(testament(1)),
        ),
        (
            Key::Outcome(request),
            Row::Outcome(outcome(request, NativeOperation::Create)),
        ),
        (
            Key::ClaimIdentity(1, ContentHash([1; 32])),
            Row::ClaimIdentity(claim(1)),
        ),
        (
            Key::DefinitionIdentity(1, ContentHash([1; 32])),
            Row::DefinitionIdentity(ValidationId::from_u128(1)),
        ),
    ]
}

#[test]
fn every_fixed_family_roundtrips_exact_bytes_with_bounded_decoding_and_no_heap_build() {
    for (key, row) in values() {
        let bytes = encode(&row);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        let restored = read_fixed(key, &mut cursor, ledger()).unwrap().unwrap();
        let visits = cursor.visits_used();
        cursor.finish().unwrap();
        assert_eq!(encode(&restored), bytes, "key {key:?}");
        let mut cursor = Cursor::new(&bytes, bytes.len(), visits).unwrap();
        assert!(read_fixed(key, &mut cursor, ledger()).unwrap().is_some());
        let mut cursor = Cursor::new(&bytes, bytes.len(), visits - 1).unwrap();
        assert!(matches!(
            read_fixed(key, &mut cursor, ledger()),
            Err(NativeError::Contract(ContractError::Capacity))
        ));
        for length in 0..bytes.len() {
            let mut cursor = Cursor::new(&bytes[..length], bytes.len(), usize::MAX).unwrap();
            assert!(
                read_fixed(key, &mut cursor, ledger()).is_err(),
                "key {key:?}, length {length}"
            );
        }
    }
    for key in [
        Key::Claim(claim(1)),
        Key::Definition(ValidationId::from_u128(1)),
        Key::Event(SessionSeq(1), 0),
        Key::Response(testament(1)),
        Key::End,
    ] {
        let mut cursor = Cursor::new(&[255], 1, 0).unwrap();
        assert!(read_fixed(key, &mut cursor, ledger()).unwrap().is_none());
        assert_eq!((cursor.offset(), cursor.visits_used()), (0, 0));
    }
}

#[test]
fn malformed_heads_cycles_receipts_keys_and_invocation_namespaces_refuse() {
    let request = fixture::request(fixture::ISSUER, 1).into();
    let invalid_rows = [
        (
            Key::IncomingHead(claim(1)),
            Row::IncomingHead(incoming_graph::IncomingHead {
                head: Some(claim(2)),
                count: 0,
            }),
        ),
        (
            Key::IncomingLink(claim(1), claim(2)),
            Row::IncomingLink(incoming_graph::IncomingLink {
                next: Some(claim(2)),
            }),
        ),
        (
            Key::MonitorLink(claim(1), monitor(2)),
            Row::MonitorLink(Some(monitor_index::MonitorLink {
                owner: claim(2),
                registered: SessionSeq(3),
                stamp: SessionSeq(2),
                previous: None,
                next: None,
            })),
        ),
        (
            Key::Cycle(cycle_key(1)),
            Row::Cycle(NativeCycle {
                work_count: 1,
                ..NativeCycle::default()
            }),
        ),
        (
            Key::RetiredCycleHead(claim(2)),
            Row::RetiredCycleHead(RetiredCycleHead {
                head: Some(cycle_key(1)),
                count: 1,
                work_count: 0,
            }),
        ),
        (
            Key::RetiredCycle(cycle_key(1)),
            Row::RetiredCycle(RetiredCycle {
                holder: fixture::SUBJECT,
                next: Some(cycle_key(1)),
            }),
        ),
        (
            Key::Receipt(ReceiptId::from_u128(2)),
            Row::Receipt(NativeReceipt {
                claim: claim(1),
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(1),
                    epoch: 1,
                },
                holder: fixture::SUBJECT,
                acquired: SessionSeq(1),
            }),
        ),
        (
            Key::ClaimIdentity(0, ContentHash([1; 32])),
            Row::ClaimIdentity(claim(1)),
        ),
        (
            Key::ArtifactIdentity(ContentHash([0; 32])),
            Row::ArtifactIdentity(artifact(1)),
        ),
        (
            Key::Outcome(request),
            Row::Outcome(outcome(request, NativeOperation::ClaimDeadline)),
        ),
    ];
    for (key, row) in invalid_rows {
        let bytes = encode(&row);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert!(
            read_fixed(key, &mut cursor, ledger()).is_err(),
            "key {key:?}"
        );
    }
    for (key, row) in values() {
        let bytes = encode(&row);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        let mut foreign = ledger();
        foreign.session = focal_model::SessionId::from_u128(999);
        if matches!(key, Key::Outcome(_) | Key::Monitor(_)) {
            assert!(matches!(
                read_fixed(key, &mut cursor, foreign),
                Err(NativeError::Contract(ContractError::WrongLedger))
            ));
        }
    }
    let bytes = [2]; // Unknown optional-link tag is never treated as None.
    let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
    assert!(
        read_fixed(
            Key::MonitorLink(claim(1), monitor(1)),
            &mut cursor,
            ledger()
        )
        .is_err()
    );
}

#[test]
fn all_outcome_namespaces_and_large_scalar_counters_preserve_their_complete_widths() {
    let deadline = NativeDeadlineKey {
        evaluation: fixture::key(0),
        timer: TimerId::from_u128(1),
        generation: 1,
    };
    let claim_deadline = NativeClaimDeadlineKey {
        claim: claim(1),
        timer: TimerId::from_u128(1),
        generation: 1,
    };
    let monitor_deadline = NativeMonitorDeadlineKey {
        claim: claim(1),
        monitor: monitor(1),
        timer: TimerId::from_u128(1),
        generation: 1,
    };
    for (invocation, operation) in [
        (
            fixture::request(fixture::ISSUER, 1).into(),
            NativeOperation::Create,
        ),
        (deadline.into(), NativeOperation::EvaluationDeadline),
        (claim_deadline.into(), NativeOperation::ClaimDeadline),
        (monitor_deadline.into(), NativeOperation::MonitorDeadline),
    ] {
        let expected = outcome(invocation, operation);
        let bytes = encode(&Row::Outcome(expected));
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        let Some(Row::Outcome(actual)) =
            read_fixed(Key::Outcome(invocation), &mut cursor, ledger()).unwrap()
        else {
            panic!("expected an outcome");
        };
        assert_eq!(actual, expected);
    }
    #[cfg(target_pointer_width = "64")]
    {
        let large = usize::try_from(u64::from(u32::MAX) + 19).unwrap();
        let row = Row::IncomingHead(incoming_graph::IncomingHead {
            head: Some(claim(1)),
            count: large,
        });
        let bytes = encode(&row);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        let Some(Row::IncomingHead(actual)) =
            read_fixed(Key::IncomingHead(claim(2)), &mut cursor, ledger()).unwrap()
        else {
            panic!("expected head");
        };
        assert_eq!(actual.count, large);
    }
    let zero_request = NativeInvocation::Request(RequestKey {
        principal: fixture::ISSUER,
        epoch: RequestEpoch(0),
        id: RequestId::from_u128(1),
    });
    let bytes = encode(&Row::Outcome(outcome(
        zero_request,
        NativeOperation::Create,
    )));
    let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
    assert!(read_fixed(Key::Outcome(zero_request), &mut cursor, ledger()).is_err());
}

#[test]
fn actual_native_events_prepare_without_allocation_then_build_under_exact_precharges() {
    let mut core = fixture::core();
    let prepared = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 1),
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ));
    core.publish_native(prepared).unwrap();
    let budget = &core.state.budget;
    for entry in core
        .state
        .rows
        .entries()
        .filter(|entry| matches!(entry.key, Key::Event(..)))
    {
        let bytes = encode(&entry.value);
        let before = budget.stats();
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        let plan = EventPlan::read(entry.key, &mut cursor, ledger()).unwrap();
        let reads = cursor.visits_used();
        cursor.finish().unwrap();
        assert_eq!(budget.stats(), before);
        let heap = plan.heap_bytes();
        let work = plan.build_visits();
        assert!(matches!(
            plan.build(heap - 1, work),
            Err(NativeError::Contract(ContractError::Capacity))
        ));
        assert_eq!(budget.stats(), before);
        let mut cursor = Cursor::new(&bytes, bytes.len(), reads).unwrap();
        let plan = EventPlan::read(entry.key, &mut cursor, ledger()).unwrap();
        assert!(matches!(
            plan.build(heap, work - 1),
            Err(NativeError::Contract(ContractError::Capacity))
        ));
        let mut cursor = Cursor::new(&bytes, bytes.len(), reads).unwrap();
        let plan = EventPlan::read(entry.key, &mut cursor, ledger()).unwrap();
        let permit = budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, heap)
            .unwrap();
        let (restored, actual) = plan.build(heap, work).unwrap();
        assert_eq!(actual, heap);
        assert_eq!(encode(&restored), bytes);
        drop(restored);
        drop(permit);
        assert_eq!(budget.stats(), before);
        let mut cursor = Cursor::new(&bytes, bytes.len(), reads - 1).unwrap();
        assert!(matches!(
            EventPlan::read(entry.key, &mut cursor, ledger()),
            Err(NativeError::Contract(ContractError::Capacity))
        ));
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert!(EventPlan::read(Key::Event(SessionSeq(900), 0), &mut cursor, ledger()).is_err());
        let mut foreign = ledger();
        foreign.session = focal_model::SessionId::from_u128(999);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert!(EventPlan::read(entry.key, &mut cursor, foreign).is_err());
        for length in 0..bytes.len() {
            let mut cursor = Cursor::new(&bytes[..length], bytes.len(), usize::MAX).unwrap();
            assert!(EventPlan::read(entry.key, &mut cursor, ledger()).is_err());
        }
    }
}
